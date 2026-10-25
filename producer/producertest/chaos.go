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

// RunChaos runs phase 0's chaos scenarios against an integrator's device, each on a fresh bus and device:
// the bus going down and up, HotReload under traffic, Stop under a stalled publisher, and injected publish faults.
// Every device verb must return within its deadline whatever the bus does,
// and what reaches the bus must pass CheckCaptures.
func RunChaos(t *testing.T, newDevice NewDevice) {
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
			sc.run(&chaos{t: t, bus: bus, d: newDevice(t, bus, sc.tun)})
		})
	}
}

// CheckCaptures checks what a producer published, as a consumer reads it:
// every message decodes; every capture with a message has its start at seq 0;
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

type chaos struct {
	t   *testing.T
	bus *Bus
	d   Device
}

// capture is what the bus holds of one capture.
type capture struct {
	desc    *tracepack.PackMeta
	maxSeq  uint64
	stopped bool
	stopSeq uint64
}

func busDownAndUp(c *chaos) {
	c.verb("Start", verbTimeout, c.d.Start)
	c.traffic(20)
	c.bus.FailPublish(func(*producer.Msg) error { return errBusDown })
	c.traffic(200)
	c.verb("HotReload", verbTimeout, c.d.HotReload)
	c.traffic(20)
	c.bus.FailPublish(nil)
	c.traffic(20)
	c.stop(stopTimeout)
	c.settle()
}

func hotReloadUnderTraffic(c *chaos) {
	c.verb("Start", verbTimeout, c.d.Start)
	for range 5 {
		var wg sync.WaitGroup
		wg.Go(func() { c.traffic(50) })
		c.verb("HotReload", verbTimeout, c.d.HotReload)
		wg.Wait()
	}
	c.stop(stopTimeout)
	c.settle()
}

func stopUnderAStalledPublisher(c *chaos) {
	c.bus.HoldAsync(true)
	c.verb("Start", verbTimeout, c.d.Start)
	c.traffic(20)
	c.stop(stalledStop)
	c.bus.HoldAsync(false)
	c.bus.ReleaseAsync(nil)
	c.check(c.bus.Messages())
}

func injectedFaults(c *chaos) {
	var n atomic.Int64
	c.bus.FailPublish(func(*producer.Msg) error {
		if n.Add(1)%3 == 0 {
			return errFault
		}

		return nil
	})
	c.bus.FailPutsUncertain(func(string) bool { return n.Load()%2 == 0 })
	c.verb("Start", verbTimeout, c.d.Start)
	c.traffic(100)
	c.verb("HotReload", verbTimeout, c.d.HotReload)
	c.traffic(100)
	c.bus.FailPublish(nil)
	c.bus.FailPutsUncertain(nil)
	c.stop(stopTimeout)
	c.settle()
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

func (c *chaos) stop(d time.Duration) {
	c.t.Helper()
	c.verb("Stop", d, func(ctx context.Context) error {
		c.d.Stop(ctx)
		return nil
	})
}

// settle waits until the last capture's stop is on the bus, then checks everything published.
func (c *chaos) settle() {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), settleTimeout)
	defer cancel()
	msgs := c.bus.Messages()
	for {
		caps, err := decode(msgs)
		if err != nil {
			c.t.Fatal(err)
		}
		if lastStopped(caps) {
			break
		}
		if msgs, err = c.bus.WaitMessages(ctx, len(msgs)+1); err != nil {
			c.t.Fatalf("the last capture's stop never reached the bus: %v", err)
		}
	}
	c.check(msgs)
}

func (c *chaos) check(msgs []producer.Msg) {
	c.t.Helper()
	if err := CheckCaptures(msgs); err != nil {
		c.t.Error(err)
	}
}

// lastStopped reports whether the capture no other start names as previous has its stop on the bus.
func lastStopped(caps map[tracepack.UUID]*capture) bool {
	named := make(map[tracepack.UUID]bool, len(caps))
	for _, c := range caps {
		if c.desc != nil && c.desc.PreviousCaptureID != nil {
			named[*c.desc.PreviousCaptureID] = true
		}
	}
	for id, c := range caps {
		if c.desc != nil && !named[id] {
			return c.stopped
		}
	}

	return false
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
