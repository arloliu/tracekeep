package producer

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

func newTestProducer(t *testing.T, tun Tunables) *Producer {
	t.Helper()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects", Tunables: tun}, func(o *options) { o.noPublisher = true })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	return p
}

func testDescriptor() CaptureDescriptor {
	return CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}
}

// nextReserved waits for the next reserved-lane entry, failing after a generous bound.
func nextReserved(t *testing.T, p *Producer) core.Entry {
	t.Helper()
	select {
	case e := <-p.rec.Reserved():
		return e
	case <-time.After(10 * time.Second):
		t.Fatalf("no reserved entry")
		return core.Entry{}
	}
}

func previousOf(t *testing.T, c *core.Capture) *tracepack.UUID {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(c.Descriptor)
	if err != nil {
		t.Fatalf("descriptor header: %v", err)
	}
	m, err := tracepack.DecodeDescriptor(b)
	if err != nil {
		t.Fatalf("DecodeDescriptor: %v", err)
	}

	return m.PreviousCaptureID
}

func TestBeginIsIdempotent(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})

	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	first := p.rec.Current()
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatalf("second Begin: %v", err)
	}
	if p.rec.Current() != first {
		t.Errorf("a second Begin opened another capture")
	}
	if e := nextReserved(t, p); e.Kind != core.EntryStart || e.Cap != first {
		t.Errorf("reserved entry %+v, want the start", e)
	}
	if len(p.rec.Reserved()) != 0 {
		t.Errorf("a second Begin queued another start")
	}
	if previousOf(t, first) != nil {
		t.Errorf("the first capture of the process names a previous capture")
	}
}

func TestBeginRefusesAnInvalidDescriptor(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})

	if err := p.Begin(t.Context(), CaptureDescriptor{ToolID: "T1"}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Errorf("Begin = %v, want ErrInvalidDescriptor", err)
	}
	if p.rec.Current() != nil {
		t.Errorf("an invalid descriptor opened a capture")
	}
}

func TestRotate(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})

	if err := p.Rotate(t.Context(), testDescriptor()); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Rotate before Begin = %v, want ErrNotStarted", err)
	}
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	first := p.rec.Current()

	if err := p.Rotate(t.Context(), testDescriptor()); err != nil {
		t.Fatalf("Rotate unchanged: %v", err)
	}
	if p.rec.Current() != first {
		t.Errorf("an unchanged descriptor rotated the capture")
	}

	changed := testDescriptor()
	sw := "eqp-hub 1.2.3"
	changed.HostSoftware = &sw
	if err := p.Rotate(t.Context(), changed); err != nil {
		t.Fatalf("Rotate changed: %v", err)
	}
	second := p.rec.Current()
	if second == first || second == nil {
		t.Fatalf("a changed descriptor did not rotate")
	}
	if prev := previousOf(t, second); prev == nil || *prev != first.ID {
		t.Errorf("previous_capture_id = %v, want %s", prev, first.ID)
	}
	if second.ID.String() <= first.ID.String() {
		t.Errorf("capture ids do not sort by creation: %s then %s", first.ID, second.ID)
	}

	nextReserved(t, p) // first's start
	nextReserved(t, p) // second's start
	if e := nextReserved(t, p); e.Kind != core.EntryStop || e.Cap != first {
		t.Errorf("reserved entry %+v, want the old capture's stop", e)
	}
}

// A sampling rule is a descriptor value: a changed rule rotates the capture, and an equal one, its defaults filled in, does not.
func TestRotateOnASamplingRuleChange(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		rule    *SamplingRule
		rotates bool
	}{
		{&SamplingRule{}, true},
		{&SamplingRule{KeepEveryN: 60, KeepInterval: time.Minute}, false},
		{&SamplingRule{KeepEveryN: 30}, true},
		{nil, true},
	}
	for _, s := range steps {
		before := p.rec.Current()
		d := testDescriptor()
		d.Thinning = s.rule
		if err := p.Rotate(t.Context(), d); err != nil {
			t.Fatalf("Rotate(%+v): %v", s.rule, err)
		}
		if rotated := p.rec.Current() != before; rotated != s.rotates {
			t.Errorf("Rotate(%+v) rotated = %v, want %v", s.rule, rotated, s.rotates)
		}
	}
}

func TestEnd(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})

	if rep := p.End(t.Context()); rep != (Report{}) {
		t.Errorf("End with no capture = %+v, want the zero report", rep)
	}
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	first := p.rec.Current()

	rep := p.End(t.Context())
	if rep.CaptureID != first.ID || !rep.StopQueued || rep.Abandoned {
		t.Errorf("End = %+v, want the capture's stop queued", rep)
	}
	if p.rec.Current() != nil {
		t.Errorf("End left a current capture")
	}
	if err := p.Rotate(t.Context(), testDescriptor()); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Rotate after End = %v, want ErrNotStarted", err)
	}

	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	if prev := previousOf(t, p.rec.Current()); prev == nil || *prev != first.ID {
		t.Errorf("Begin after End: previous_capture_id = %v, want the ended capture %s", prev, first.ID)
	}
}

func TestFaultRotates(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{FaultBackoff: 200 * time.Millisecond})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	first := p.rec.Current()
	nextReserved(t, p)

	first.Fault()
	p.rec.Observe(&core.Observation{Socket: 1, Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Payload: []byte{1}})

	e := nextReserved(t, p)
	if e.Kind != core.EntryStart || e.Cap == first {
		t.Fatalf("after a fault: reserved entry %+v, want a fresh capture's start", e)
	}
	second := e.Cap
	// The start is queued before its capture is installed: wait for the fault rotation to finish,
	// or the next fault could signal the old capture and be discarded as stale.
	if !p.lockLife(time.Now().Add(10 * time.Second)) {
		t.Fatal("the fault rotation kept the lifecycle lock")
	}
	p.unlockLife()
	if prev := previousOf(t, second); prev == nil || *prev != first.ID {
		t.Errorf("previous_capture_id = %v, want the faulted capture", prev)
	}
	if len(p.rec.Reserved()) != 0 {
		t.Errorf("the faulted capture got a stop")
	}

	// The fresh capture faults again within a minute: rotation waits for the backoff.
	faultedAt := time.Now()
	second.Fault()
	p.rec.Observe(&core.Observation{Socket: 1, Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Payload: []byte{1}})
	e = nextReserved(t, p)
	if e.Kind != core.EntryStart || e.Cap == second {
		t.Fatalf("after a second fault: reserved entry %+v, want a fresh capture's start", e)
	}
	if waited := time.Since(faultedAt); waited < 200*time.Millisecond {
		t.Errorf("rotation after a repeated fault came after %v, want at least the 200ms backoff", waited)
	}
}

func TestFaultAfterEndDoesNotRotate(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	p.End(t.Context())

	p.handleFault(c) // a signal that raced the stop
	if p.rec.Current() != nil {
		t.Errorf("a stale fault signal installed a capture after End")
	}
}

func TestNewUUIDv7(t *testing.T) {
	t.Parallel()
	var g uuidV7
	prev := g.next()
	for range 1000 {
		u := g.next()
		if u[6]>>4 != 7 || u[8]>>6 != 2 {
			t.Fatalf("%s: version %d variant %d, want 7 and RFC 9562", u, u[6]>>4, u[8]>>6)
		}
		if u.String() <= prev.String() {
			t.Fatalf("%s does not sort after %s", u, prev)
		}
		prev = u
	}
}

// A fault rotation that cannot complete starts the backoff too, so a persistent fault cannot spin.
func TestFailedFaultRotationBacksOff(t *testing.T) {
	t.Parallel()
	const backoff = 150 * time.Millisecond
	p := newTestProducer(t, Tunables{ReservedLaneRecords: 1, FaultBackoff: backoff})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil { // its start fills the one-slot reserved lane
		t.Fatal(err)
	}
	c := p.rec.Current()
	c.Fault() // no observation signals it, so only the calls below handle it

	p.handleFault(c) // fails for want of room in the reserved lane
	if p.rec.Current() != c {
		t.Fatalf("a rotation succeeded with a full reserved lane")
	}
	start := time.Now()
	p.handleFault(c) // the same capture faults on: this attempt waits for the backoff
	if waited := time.Since(start); waited < backoff {
		t.Errorf("second attempt after %v, want at least the %v backoff", waited, backoff)
	}
}

// End returns by its deadline when another lifecycle call holds the lock, and changes nothing.
func TestEndKeepsItsDeadlineUnderContention(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	p.lifeSem <- struct{}{} // another lifecycle call holds the lock
	defer func() { <-p.lifeSem }()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	rep := p.End(ctx)
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("End took %v with a 100ms deadline", took)
	}
	if !rep.Contended || p.rec.Current() != c {
		t.Errorf("End = %+v, current changed: %v; want contended and nothing changed", rep, p.rec.Current() != c)
	}
}

// A contended End leaves the lifecycle as it found it: rotations still run.
func TestContendedEndChangesNothing(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	p.lifeSem <- struct{}{}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if rep := p.End(ctx); !rep.Contended {
		t.Fatalf("End = %+v, want contended", rep)
	}
	<-p.lifeSem

	d := testDescriptor()
	sw := "v2"
	d.HostSoftware = &sw
	if err := p.Rotate(t.Context(), d); err != nil {
		t.Errorf("Rotate after a contended End = %v, want a rotation", err)
	}
}
