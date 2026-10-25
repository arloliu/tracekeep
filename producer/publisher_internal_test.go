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

// doneFuture is an asynchronous publish that resolved before PublishAsync returned.
type doneFuture struct{ ack Ack }

func (f doneFuture) Poll() (Ack, bool, error) { return f.ack, true, nil }

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
	ack, err := b.Publish(context.Background(), m)

	return doneFuture{ack: ack}, err
}

func (b *recBus) CheckConn() []Problem { return nil }

func (b *recBus) StreamInfo(context.Context, string) (StreamInfo, error) { return StreamInfo{}, nil }

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

// A record of a capture whose drain ended is dropped even when a late callback queues it afterwards.
func TestLateEnqueueAfterDrainIsDropped(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	if rep := p.End(t.Context()); !rep.Drained {
		t.Fatalf("End = %+v", rep)
	}
	if !c.Terminated() {
		t.Fatalf("a drained capture is not terminated")
	}
	before := len(bus.published())

	p.rec.PushReserved(core.Entry{Cap: c, Kind: core.EntryClockStep, Seq: 99})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil { // its start is published after the late entry was taken in
		t.Fatal(err)
	}
	ids := bus.waitFor(t, before+1)
	for _, id := range ids[before:] {
		if id == c.ID.String()+".99" {
			t.Errorf("published %s of a capture whose drain ended", id)
		}
	}
}

// An attempt is judged by the present: a record whose horizon has passed is dropped without an attempt.
func TestSendOrdinaryUsesThePresent(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Tunables: Tunables{RetryHorizon: 50 * time.Millisecond}},
		func(o *options) { o.noPublisher = true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	pb := newPublisher(p, bus)
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	st := &capState{cap: c, subject: c.Subject}
	first := time.Now().Add(-time.Second) // its first attempt was a second ago, far past H
	pd := &pending{e: core.Entry{Cap: c, Kind: core.EntryObserved, Seq: 1, Payload: []byte{1}}, first: first}

	pb.sendOrdinary(st, pd)
	if got := bus.published(); len(got) != 0 {
		t.Errorf("published %v past the retry horizon", got)
	}
	if pb.dropped[core.DropPublishFailed] != 1 {
		t.Errorf("dropped{publish_failed} = %d, want 1", pb.dropped[core.DropPublishFailed])
	}
}

// No attempt at a capture starts once End's deadline for it has passed, whatever the pass's clock says.
func TestSendOrdinaryStopsAtEndDeadline(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, func(o *options) { o.noPublisher = true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	pb := newPublisher(p, bus)
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	c.SetEndBy(time.Now().Add(-time.Millisecond))
	st := &capState{cap: c, subject: c.Subject}
	pd := &pending{e: core.Entry{Cap: c, Kind: core.EntryObserved, Seq: 1, Payload: []byte{1}}}

	if pb.sendOrdinary(st, pd) {
		t.Errorf("sendOrdinary let the record leave the held list past End's deadline")
	}
	if got := bus.published(); len(got) != 0 {
		t.Errorf("published %v past End's deadline", got)
	}

	// The same holds for a reserved record at the head of the capture.
	pb.epoch = 1 // the entry below was taken in before the last snapshot
	st.reserved = []*pending{{e: core.Entry{Cap: c, Kind: core.EntryStop, Seq: 2}}}
	pb.progress(st, time.Now().Add(-time.Hour))
	if got := bus.published(); len(got) != 0 {
		t.Errorf("published %v past End's deadline", got)
	}
}

// slowBus delays MaxPayload, which building a message calls, past End's deadline.
type slowBus struct {
	*recBus
	delay time.Duration
}

func (b slowBus) MaxPayload() int {
	time.Sleep(b.delay) // injects the delay of the scenario: building the message crosses End's deadline
	return b.recBus.MaxPayload()
}

// A deadline crossed while building a message stops the attempt, ordinary or reserved.
func TestDeadlineCrossedWhileBuilding(t *testing.T) {
	t.Parallel()
	bus := slowBus{recBus: newRecBus(), delay: 50 * time.Millisecond}
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, func(o *options) { o.noPublisher = true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	pb := newPublisher(p, bus)
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	c.SetEndBy(time.Now().Add(20 * time.Millisecond))
	st := &capState{cap: c, subject: c.Subject}

	pb.sendOrdinary(st, &pending{e: core.Entry{Cap: c, Kind: core.EntryObserved, Seq: 1, Payload: []byte{1}}})
	pb.publishReserved(st, &pending{e: core.Entry{Cap: c, Kind: core.EntryStop, Seq: 2}}, time.Now())
	if got := bus.published(); len(got) != 0 {
		t.Errorf("published %v after End's deadline passed while building", got)
	}
}

// A capture with a record a callback allocated but has not sent stays on End's list across later rotations.
func TestAllocatedButUnsentCaptureStaysOnEndsList(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	a := p.rec.Current()
	wall, mono := core.Now()
	a.Allocate(wall, mono, 1) // a callback allocated seq 1 and has not sent it

	sw := "v2"
	d := testDescriptor()
	d.HostSoftware = &sw
	if err := p.Rotate(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	bus.waitFor(t, 3) // A's start, A's stop, B's start: the publisher has nothing else of A
	sw = "v3"
	if err := p.Rotate(t.Context(), d); err != nil { // prunes settled captures
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	p.End(ctx)
	if _, stamped := a.EndBy(); !stamped {
		t.Errorf("End did not stop a capture whose allocated record was still unsent")
	}
}
