package sim

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// fakeTimers is a clock for the waiter's timers that a test moves by hand.
type fakeTimers struct {
	mu  sync.Mutex
	now time.Duration
	ts  []*fakeTimer
}

type fakeTimer struct {
	at   time.Duration
	f    func()
	done bool
}

func (c *fakeTimers) after(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now + d, f: f}
	c.ts = append(c.ts, t)

	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		stopped := !t.done
		t.done = true

		return stopped
	}
}

func (c *fakeTimers) expire(d time.Duration) {
	c.mu.Lock()
	c.now += d
	var due []func()
	for _, t := range c.ts {
		if !t.done && t.at <= c.now {
			t.done = true
			due = append(due, t.f)
		}
	}
	c.mu.Unlock()
	for _, f := range due {
		f()
	}
}

func TestWaiterConforms(t *testing.T) {
	t.Parallel()
	clk := &fakeTimers{}
	w := newWaiter()
	w.after = clk.after
	err := producertest.CheckTimeoutSource(producertest.TimeoutSourceHarness{
		Source:  w,
		Reply:   func(t producer.Token) { w.reply(t) },
		Cancel:  func(t producer.Token) { w.cancel(t) },
		Expire:  clk.expire,
		Quiesce: func() {}, // the fake timers fire synchronously
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testOptions() Options {
	return Options{ToolID: "SIM-TOOL", DeviceName: "eqp", SessionID: 1, T3: 50 * time.Millisecond, WithholdEvery: 5}
}

func newTestLoopback(t *testing.T, cfg producer.Config, opts ...producer.Option) *Loopback {
	t.Helper()
	return newTestLoopbackWith(t, cfg, testOptions(), opts...)
}

func newTestLoopbackWith(t *testing.T, cfg producer.Config, lo Options, opts ...producer.Option) *Loopback {
	t.Helper()
	cfg.SubjectPrefix, cfg.ClaimBucket = "tpk", "tpk-objects"
	p, err := producer.New(cfg, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l, err := NewLoopback(p, lo)
	if err != nil {
		t.Fatalf("NewLoopback: %v", err)
	}
	t.Cleanup(func() {
		_ = l.Close()
		_ = p.Close()
	})

	return l
}

func TestLoopbackChaos(t *testing.T) {
	t.Parallel()
	producertest.RunChaos(t, func(t *testing.T, bus *producertest.Bus, tun producer.Tunables) producertest.Device {
		return newTestLoopback(t, producer.Config{Tunables: tun}, producer.WithBus(bus))
	})
}

// The equipment withholds every fifth reply, so exactly those requests time out and the capture holds their T3,
// in the epoch of the socket they went out on; a request that got its reply never records one.
func TestLoopbackRecordsT3(t *testing.T) {
	t.Parallel()
	const n = 200
	bus := producertest.NewBus()
	opts := testOptions()
	opts.T3 = time.Second // far above a localhost reply, so an answered request's wait never times out
	l := newTestLoopbackWith(t, producer.Config{}, opts, producer.WithBus(bus))
	if err := l.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := l.Traffic(t.Context(), n); err != nil {
		t.Fatal(err)
	}

	t3 := map[uint32]tracepack.TransportEvent{}
	var dataEpoch, t3Epoch uint32
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for i := 1; len(t3) < n/5; i++ {
		msgs, err := bus.WaitMessages(ctx, i)
		if err != nil {
			t.Fatalf("%d T3 events on the bus: %v", len(t3), err)
		}
		br := decode(t, &msgs[i-1])
		r := &br.Record
		if r.Kind == tracepack.KindData {
			dataEpoch = r.Epoch
		}
		if r.Kind != tracepack.KindTransportEvent {
			continue
		}
		ev, err := tracepack.UnmarshalTransportEvent(r.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Event == tracepack.EventTimerExpiry && ev.Timer != nil && *ev.Timer == tracepack.TimerT3 {
			sb := binary.BigEndian.Uint32(ev.PrimarySystemBytes[:])
			if sb%5 != 0 {
				t.Errorf("T3 for System Bytes %d, which the equipment answered", sb)
			}
			t3[sb], t3Epoch = *ev, r.Epoch
		}
	}
	for _, ev := range t3 {
		if ev.Detail == nil || *ev.Detail != "effective_timeout_ms=1000" || *ev.PrimaryStream != 1 || *ev.PrimaryFunction != 3 {
			t.Errorf("T3 event = %+v", ev)
		}
	}
	if t3Epoch != dataEpoch || t3Epoch == 0 {
		t.Errorf("T3 in epoch %d, frames in epoch %d", t3Epoch, dataEpoch)
	}
	l.Stop(t.Context())
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.inflight) != 0 {
		t.Errorf("%d requests still in flight after Stop", len(l.inflight))
	}
}

func decode(t *testing.T, m *producer.Msg) tracepack.BusRecord {
	t.Helper()
	br, err := tracepack.CheckBusMessage(tracepack.BusEnvelope{
		Layout: m.Header[tracepack.BusHeaderLayout], MsgID: m.MsgID, Descriptor: m.Header[tracepack.BusHeaderDescriptor],
	}, m.Data)
	if err != nil {
		t.Fatalf("CheckBusMessage(%s): %v", m.MsgID, err)
	}

	return br
}
