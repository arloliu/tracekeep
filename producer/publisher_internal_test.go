package producer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// recBus is the package's own minimal Bus: it records accepted message ids; objects are unsupported.
type recBus struct {
	mu   sync.Mutex
	ids  []string
	seen chan struct{}
}

type doneFuture struct {
	ok  chan Ack
	err chan error
}

func (f doneFuture) Ok() <-chan Ack    { return f.ok }
func (f doneFuture) Err() <-chan error { return f.err }

func newRecBus() *recBus { return &recBus{seen: make(chan struct{}, 1024)} }

func (b *recBus) MaxPayload() int { return 1 << 20 }

func (b *recBus) Publish(_ context.Context, m *Msg) (Ack, error) {
	b.mu.Lock()
	b.ids = append(b.ids, m.MsgID)
	b.mu.Unlock()
	b.seen <- struct{}{}

	return Ack{}, nil
}

func (b *recBus) PublishAsync(m *Msg) (AckFuture, error) {
	f := doneFuture{ok: make(chan Ack, 1), err: make(chan error, 1)}
	ack, err := b.Publish(context.Background(), m)
	f.ok <- ack

	return f, err
}

func (b *recBus) Objects(context.Context, string) (Objects, error) {
	return nil, errors.New("recBus: no objects")
}

func (b *recBus) published() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.ids...)
}

func (b *recBus) waitFor(t *testing.T, n int) []string {
	t.Helper()
	for len(b.published()) < n {
		select {
		case <-b.seen:
		case <-time.After(10 * time.Second):
			t.Fatalf("published %v, want %d messages", b.published(), n)
		}
	}

	return b.published()
}

// A clock step reserved after the publisher's reservation snapshot must still hold back every record above it.
func TestLateReservationHoldsBackLaterRecords(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	var (
		cur    atomic.Pointer[core.Capture]
		armed  atomic.Bool
		passes = make(chan struct{}, 64)
		stepAt atomic.Uint64
		p      *Producer
	)
	hooks := publisherHooks{
		afterSnapshot: func() {
			c := cur.Load()
			if c == nil || !armed.CompareAndSwap(true, false) {
				return
			}
			// Callback A allocates a clock step and its record, then stalls before queuing the step;
			// callback B's record above them is queued meanwhile.
			wall, mono := core.Now()
			a, _ := c.Allocate(wall+3*int64(time.Second), mono, 1)
			stepAt.Store(a.StepSeq)
			wall, mono = core.Now()
			p.rec.Observe(&core.Observation{Wall: wall + 3*int64(time.Second), Mono: mono, Socket: 1, Kind: tracepack.KindData, Payload: []byte{1}})
		},
		afterPass: func() {
			select {
			case passes <- struct{}{}:
			default:
			}
		},
	}
	var err error
	p, err = New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, WithBus(bus), func(o *options) { o.hooks = hooks })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	cur.Store(c)
	bus.waitFor(t, 1) // the start
	armed.Store(true)
	wall, mono := core.Now()
	p.rec.Observe(&core.Observation{Wall: wall, Mono: mono, Socket: 1, Kind: tracepack.KindData, Payload: []byte{1}}) // seq 1 wakes the publisher
	for range 5 {                                                                                                     // passes after the late reservation
		<-passes
	}
	for _, id := range bus.published() {
		if id == c.ID.String()+".4" {
			t.Fatalf("published %s while the clock step at %d is unqueued: %v", id, stepAt.Load(), bus.published())
		}
	}

	step := stepAt.Load()
	p.rec.PushReserved(core.Entry{Cap: c, Kind: core.EntryClockStep, Seq: step, Drift: 3 * int64(time.Second)})
	c.Retire(step)
	ids := bus.waitFor(t, 4) // start, the waking record, the step, and the record above it
	stepIdx, laterIdx := -1, -1
	for i, id := range ids {
		switch id {
		case c.ID.String() + ".2":
			stepIdx = i
		case c.ID.String() + ".4":
			laterIdx = i
		default:
		}
	}
	if stepIdx < 0 || laterIdx < 0 || laterIdx < stepIdx {
		t.Errorf("published %v: want the step (seq 2) before the record above it (seq 4)", ids)
	}
}

// A reservation that is never queued faults the capture even when nothing else happens.
func TestIdleUnqueuedReservationFaults(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Tunables: Tunables{ReservationTimeout: 50 * time.Millisecond}}, WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	bus.waitFor(t, 1)
	wall, mono := core.Now()
	c.Allocate(wall+3*int64(time.Second), mono, 1) // a step reserved and never queued: a callback panicked

	deadline := time.After(10 * time.Second)
	for p.rec.Current() == c {
		select {
		case <-bus.seen: // the fault rotation publishes the successor's start
		case <-deadline:
			t.Fatalf("the capture was not faulted and rotated after its reservation timed out")
		}
	}
	if !c.Faulted() {
		t.Errorf("the capture with the stale reservation is not faulted")
	}
}

func TestSeqLimitRotates(t *testing.T) {
	t.Parallel()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, func(o *options) { o.noPublisher, o.rotateAt = true, 2 })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	first := p.rec.Current()
	nextReserved(t, p)
	for range 2 {
		wall, mono := core.Now()
		p.rec.Observe(&core.Observation{Wall: wall, Mono: mono, Socket: 1, Kind: tracepack.KindData, Payload: []byte{1}})
	}

	e := nextReserved(t, p)
	if e.Kind != core.EntryStart || e.Cap == first {
		t.Fatalf("reserved entry %+v, want the successor's start", e)
	}
	if prev := previousOf(t, e.Cap); prev == nil || *prev != first.ID {
		t.Errorf("successor's previous_capture_id = %v, want %s", prev, first.ID)
	}
	if e := nextReserved(t, p); e.Kind != core.EntryStop || e.Cap != first {
		t.Errorf("reserved entry %+v, want the old capture's stop: a clean rotation, not a fault", e)
	}
}
