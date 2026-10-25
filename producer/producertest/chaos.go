package producertest

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
)

const (
	// verbTimeout is the deadline the harness gives Start, HotReload and Traffic.
	verbTimeout = time.Second
	// stopTimeout is the deadline it gives Stop, long enough to drain once the bus is back.
	stopTimeout = 5 * time.Second
	// stalledStop is the deadline of a Stop under a stalled publisher.
	stalledStop = 300 * time.Millisecond
	// slack is how far past its deadline a device verb may return before it counts as blocked.
	slack = time.Second
	// settleTimeout bounds the wait for the last capture's stop on the bus.
	settleTimeout = 10 * time.Second
)

var (
	errBusDown = errors.New("producertest: bus down")
	errFault   = errors.New("producertest: injected fault")
)

// Device is an integrator's wiring of the SDK, which RunChaos drives:
// its verbs are the device's own, and each maps onto the SDK as the integrator maps it (producer-sdk.md §4.7).
type Device interface {
	// Start opens the device, which calls Begin.
	Start(ctx context.Context) error
	// HotReload changes a configured value of the device, which calls Rotate with the changed descriptor.
	HotReload(ctx context.Context) error
	// Stop stops the device, which calls End; it never fails.
	Stop(ctx context.Context)
	// Traffic makes the device exchange at least n messages with its peer, which the SDK's observers see.
	Traffic(ctx context.Context, n int) error
}

// NewDevice builds a device whose producer publishes through bus (producer.WithBus) with tun as its Config.Tunables.
type NewDevice func(t *testing.T, bus *Bus, tun producer.Tunables) Device

// RunChaos runs the chaos scenarios against an integrator's device, each on a fresh bus and device:
// the bus going down and up, HotReload under traffic, Stop under a stalled publisher, and injected publish faults.
// Every device verb must return within its deadline whatever the bus does,
// and what reaches the bus must pass CheckCaptures.
func RunChaos(t *testing.T, newDevice NewDevice) {
	t.Helper()
	runScenarios(t, newDevice, func(t *testing.T) testing.TB { return t })
}

func runScenarios(t *testing.T, newDevice NewDevice, report func(t *testing.T) testing.TB) {
	t.Helper()
	scenarios := []struct {
		name string
		tun  producer.Tunables
		run  func(c *chaos)
	}{
		// A small lane and horizon make the outage drop records within the test, as seq gaps.
		{"bus down and up", producer.Tunables{OrdinaryLaneRecords: 64, RetryHorizon: 200 * time.Millisecond}, busDownAndUp},
		{"hot reload under traffic", producer.Tunables{}, hotReloadUnderTraffic},
		{"stop under a stalled publisher", producer.Tunables{}, stopUnderAStalledPublisher},
		{"injected faults", producer.Tunables{}, injectedFaults},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			bus := NewBus()
			sc.run(&chaos{t: report(t), bus: bus, d: newDevice(t, bus, sc.tun)})
		})
	}
}

// CheckCaptures checks what a producer published, as a consumer reads it:
// every message decodes; every capture with a message has its start on the bus;
// every start but the first names as its previous capture one whose start is also on the bus;
// and a capture's stop, if any, is its last seq.
// It does not check for gaps, which a drop leaves by design,
// nor for repeated seqs, which the bus deduplicates by message id.
func CheckCaptures(msgs []producer.Msg) error {
	caps, err := decode(msgs)
	if err != nil {
		return err
	}
	var errs []error
	roots := 0
	for id, c := range caps {
		switch {
		case c.desc == nil:
			errs = append(errs, fmt.Errorf("capture %v has records but no start", id))
		case c.desc.PreviousCaptureID == nil:
			roots++
		case caps[*c.desc.PreviousCaptureID] == nil || caps[*c.desc.PreviousCaptureID].desc == nil:
			errs = append(errs, fmt.Errorf("capture %v names a previous capture %v whose start is not on the bus", id, *c.desc.PreviousCaptureID))
		default:
		}
		if c.stopped && c.stopSeq != c.maxSeq {
			errs = append(errs, fmt.Errorf("capture %v has seq %d after its stop at %d", id, c.maxSeq, c.stopSeq))
		}
	}
	if roots > 1 {
		errs = append(errs, fmt.Errorf("%d captures name no previous capture, want one", roots))
	}

	return errors.Join(errs...)
}

// chaos reports through Error and Errorf only, so a scenario's failures can be recorded instead of failing a test.
type chaos struct {
	t   testing.TB
	bus *Bus
	d   Device
}

// capture is what the bus holds of one capture.
type capture struct {
	desc    *tracepack.PackMeta
	frames  int // data and control records
	maxSeq  uint64
	stopped bool
	stopSeq uint64
}

func busDownAndUp(c *chaos) {
	c.verb("Start", verbTimeout, c.d.Start)
	c.trafficSeen(20)
	failed := make(chan struct{})
	var once sync.Once
	c.bus.FailPublish(func(*producer.Msg) error {
		once.Do(func() { close(failed) })
		return errBusDown
	})
	c.traffic(200)
	c.verb("HotReload", verbTimeout, c.d.HotReload)
	c.traffic(20)
	c.wait(failed, "no publish met the outage")
	c.bus.FailPublish(nil)
	c.traffic(20)
	c.stop(stopTimeout)
	c.settle(2)
}

func hotReloadUnderTraffic(c *chaos) {
	c.verb("Start", verbTimeout, c.d.Start)
	c.trafficSeen(20)
	for range 5 {
		var wg sync.WaitGroup
		wg.Go(func() { c.traffic(50) })
		c.verb("HotReload", verbTimeout, c.d.HotReload)
		wg.Wait()
	}
	c.stop(stopTimeout)
	c.settle(6)
}

func stopUnderAStalledPublisher(c *chaos) {
	c.bus.HoldAsync(true)
	c.verb("Start", verbTimeout, c.d.Start)
	c.traffic(20)
	// Records go out asynchronously, so the publisher is stalled once one is held.
	ctx, cancel := context.WithTimeout(c.t.Context(), settleTimeout)
	defer cancel()
	if err := c.bus.waitHeld(ctx); err != nil {
		c.t.Errorf("the traffic never reached the bus: %v", err)
	}
	c.stop(stalledStop)
	c.bus.HoldAsync(false)
	c.bus.ReleaseAsync(nil)
	c.check(c.bus.Messages())
}

func injectedFaults(c *chaos) {
	var n atomic.Int64
	faulted := make(chan struct{})
	c.bus.FailPublish(func(*producer.Msg) error {
		if n.Add(1) == 3 {
			close(faulted)
		}
		if n.Load()%3 == 0 {
			return errFault
		}

		return nil
	})
	c.bus.FailPutsUncertain(func(string) bool { return n.Load()%2 == 0 })
	c.verb("Start", verbTimeout, c.d.Start)
	c.traffic(100)
	c.verb("HotReload", verbTimeout, c.d.HotReload)
	c.traffic(100)
	c.wait(faulted, "no publish met an injected fault")
	c.bus.FailPublish(nil)
	c.bus.FailPutsUncertain(nil)
	c.trafficSeen(20)
	c.stop(stopTimeout)
	c.settle(2)
}

// verb runs one device verb with deadline d and fails the test if it errs or returns more than slack past d.
func (c *chaos) verb(name string, d time.Duration, f func(ctx context.Context) error) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), d)
	defer cancel()
	start := time.Now()
	err := f(ctx)
	if took := time.Since(start); took > d+slack {
		c.t.Errorf("%s took %v with a %v deadline: the device waited on the producer", name, took, d)
	}
	if err != nil {
		c.t.Errorf("%s: %v", name, err)
	}
}

func (c *chaos) traffic(n int) {
	c.t.Helper()
	c.verb(fmt.Sprintf("Traffic(%d)", n), verbTimeout, func(ctx context.Context) error { return c.d.Traffic(ctx, n) })
}

// trafficSeen runs Traffic(n) and waits until at least n more frames are on the bus.
// It tells the frames apart by count only: a device whose other frames reach the bus meanwhile in such number
// can stand in for a Traffic that does nothing.
func (c *chaos) trafficSeen(n int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), settleTimeout)
	defer cancel()
	msgs := c.bus.Messages()
	before := frames(msgs)
	c.traffic(n)
	for got := frames(msgs); got < before+n; got = frames(msgs) {
		var err error
		if msgs, err = c.bus.WaitMessages(ctx, len(msgs)+1); err != nil {
			c.t.Errorf("Traffic(%d) put %d frames on the bus, want at least %d", n, got-before, n)
			return
		}
	}
}

// wait waits for done, reporting what if it does not come.
func (c *chaos) wait(done <-chan struct{}, what string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), settleTimeout)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
		c.t.Error(what)
	}
}

// frames counts the data and control records among msgs, ignoring any that do not decode.
func frames(msgs []producer.Msg) int {
	n := 0
	for i := range msgs {
		if caps, err := decode(msgs[i : i+1]); err == nil {
			for _, cp := range caps {
				n += cp.frames
			}
		}
	}

	return n
}

func (c *chaos) stop(d time.Duration) {
	c.t.Helper()
	c.verb("Stop", d, func(ctx context.Context) error {
		c.d.Stop(ctx)
		return nil
	})
}

// settle waits until the last capture's stop is on the bus, then checks everything published:
// the device's captures, one per Start or HotReload, and its traffic among them.
func (c *chaos) settle(captures int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), settleTimeout)
	defer cancel()
	msgs := c.bus.Messages()
	var caps map[tracepack.UUID]*capture
	for {
		var err error
		if caps, err = decode(msgs); err != nil {
			c.t.Error(err)
			return
		}
		if lastStopped(caps) {
			break
		}
		if msgs, err = c.bus.WaitMessages(ctx, len(msgs)+1); err != nil {
			c.t.Errorf("the last capture's stop never reached the bus: %v", err)
			return
		}
	}
	c.check(msgs)
	started, frames := 0, 0
	for _, cp := range caps {
		if cp.desc != nil {
			started++
		}
		frames += cp.frames
	}
	if started != captures {
		c.t.Errorf("%d captures on the bus, want %d: one per Start and HotReload", started, captures)
	}
	if frames == 0 {
		c.t.Errorf("no traffic on the bus")
	}
}

func (c *chaos) check(msgs []producer.Msg) {
	c.t.Helper()
	if err := CheckCaptures(msgs); err != nil {
		c.t.Error(err)
	}
}

// lastStopped reports whether every capture no other start names as previous has its stop on the bus.
func lastStopped(caps map[tracepack.UUID]*capture) bool {
	named := make(map[tracepack.UUID]bool, len(caps))
	for _, c := range caps {
		if c.desc != nil && c.desc.PreviousCaptureID != nil {
			named[*c.desc.PreviousCaptureID] = true
		}
	}
	tips := 0
	for id, c := range caps {
		if c.desc != nil && !named[id] {
			if !c.stopped {
				return false
			}
			tips++
		}
	}

	return tips > 0
}

func decode(msgs []producer.Msg) (map[tracepack.UUID]*capture, error) {
	caps := make(map[tracepack.UUID]*capture)
	for i := range msgs {
		m := &msgs[i]
		br, err := tracepack.CheckBusMessage(tracepack.BusEnvelope{
			Layout:        m.Header[tracepack.BusHeaderLayout],
			MsgID:         m.MsgID,
			Descriptor:    m.Header[tracepack.BusHeaderDescriptor],
			PayloadRef:    m.Header[tracepack.BusHeaderPayloadRef],
			PayloadDigest: m.Header[tracepack.BusHeaderPayloadDigest],
		}, m.Data)
		if err != nil {
			return nil, fmt.Errorf("message %s: %w", m.MsgID, err)
		}
		c := caps[br.CaptureID]
		if c == nil {
			c = &capture{}
			caps[br.CaptureID] = c
		}
		r := &br.Record
		c.maxSeq = max(c.maxSeq, r.Seq)
		if r.Kind == tracepack.KindData || r.Kind == tracepack.KindControl {
			c.frames++
		}
		desc := br.Descriptor
		if desc == nil && br.ClaimCheck && m.Header[tracepack.BusHeaderDescriptor] != "" {
			// A claim-check start's descriptor is checked with its payload, which the check does not fetch.
			if desc, err = base64.StdEncoding.DecodeString(m.Header[tracepack.BusHeaderDescriptor]); err != nil {
				return nil, fmt.Errorf("message %s: descriptor header: %w", m.MsgID, err)
			}
		}
		if desc != nil {
			if c.desc, err = tracepack.DecodeDescriptor(desc); err != nil {
				return nil, fmt.Errorf("message %s: descriptor: %w", m.MsgID, err)
			}
		}
		if r.Kind != tracepack.KindTransportEvent || br.ClaimCheck {
			continue
		}
		ev, err := tracepack.UnmarshalTransportEvent(r.Payload)
		if err != nil {
			return nil, fmt.Errorf("message %s: event: %w", m.MsgID, err)
		}
		if ev.Event == tracepack.EventCaptureBoundary && ev.BoundaryKind != nil && *ev.BoundaryKind == tracepack.BoundaryKindStop {
			c.stopped, c.stopSeq = true, r.Seq
		}
	}

	return caps, nil
}
