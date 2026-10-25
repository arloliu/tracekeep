package core

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

const (
	origin = int64(1_800_000_000) * int64(time.Second)
	eps    = int64(time.Second)
)

func newTestCapture(t *testing.T, reservedCap int) (*Capture, *atomic.Uint64) {
	t.Helper()
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{
		ID:          tracepack.UUID{1},
		Generation:  0,
		Lifecycle:   &gen,
		Tolerance:   eps,
		ReservedCap: reservedCap,
		OriginWall:  origin,
		OriginMono:  0,
	})

	return c, &gen
}

func TestAllocateSeqAndEpoch(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 8)

	// seq 0 is the start's; observations follow it.
	cases := []struct {
		socket    uint64
		wantSeq   uint64
		wantEpoch uint32
	}{
		{socket: 5, wantSeq: 1, wantEpoch: 1},
		{socket: 5, wantSeq: 2, wantEpoch: 1},
		{socket: 9, wantSeq: 3, wantEpoch: 2}, // a new socket takes the next epoch
		{socket: 5, wantSeq: 4, wantEpoch: 1},
		{socket: 0, wantSeq: 5, wantEpoch: 0}, // no socket: epoch unknown
	}
	for i, tc := range cases {
		a, st := c.Allocate(origin+int64(i), int64(i), tc.socket)
		if st != Allocated {
			t.Fatalf("observation %d: status %v, want Allocated", i, st)
		}
		if a.Seq != tc.wantSeq || a.Epoch != tc.wantEpoch {
			t.Errorf("observation %d: seq %d epoch %d, want seq %d epoch %d", i, a.Seq, a.Epoch, tc.wantSeq, tc.wantEpoch)
		}
		if a.Step {
			t.Errorf("observation %d: unexpected clock step", i)
		}
	}
}

func TestAllocateClockStep(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 8)
	c.Retire(0) // the start is queued

	// Within ε of the anchor: no step.
	if a, _ := c.Allocate(origin+10+eps, 10, 1); a.Step {
		t.Fatalf("drift of exactly ε stepped")
	}
	// The wall clock jumps two seconds ahead of the monotonic clock.
	a, st := c.Allocate(origin+20+3*eps, 20, 1)
	if st != Allocated || !a.Step {
		t.Fatalf("status %v step %v, want a clock step", st, a.Step)
	}
	if a.StepDrift != 3*eps {
		t.Errorf("step drift %d, want %d", a.StepDrift, 3*eps)
	}
	if a.StepSeq != 2 || a.Seq != 3 {
		t.Errorf("step seq %d record seq %d, want 2 and 3: the step precedes the record", a.StepSeq, a.Seq)
	}
	if got, ok := c.MinReserved(); !ok || got != 2 {
		t.Errorf("MinReserved = %d, %v; want the step's seq 2 reserved", got, ok)
	}
	c.Retire(a.StepSeq)
	if _, ok := c.MinReserved(); ok {
		t.Errorf("reservation still held after Retire")
	}
	// The anchor moved: the same offset no longer steps.
	if a, _ := c.Allocate(origin+30+3*eps, 30, 1); a.Step {
		t.Errorf("stepped again under the new anchor")
	}
	// A backward step steps too.
	if a, _ := c.Allocate(origin+40, 40, 1); !a.Step {
		t.Errorf("a backward wall step of 3ε did not step")
	}
}

func TestAllocateOrderingUncertain(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 8)

	if a, _ := c.Allocate(origin+100, 100, 1); a.OrderingUncertain {
		t.Fatalf("first observation marked ordering-uncertain")
	}
	// Observed earlier than the record allocated before it.
	if a, _ := c.Allocate(origin+50, 50, 1); !a.OrderingUncertain {
		t.Errorf("an observation older than its predecessor was not marked ordering-uncertain")
	}
	if a, _ := c.Allocate(origin+100, 100, 1); a.OrderingUncertain {
		t.Errorf("an observation at its predecessor's time was marked ordering-uncertain")
	}
}

func TestAllocateClosedFaultedAbandoned(t *testing.T) {
	t.Parallel()

	c, _ := newTestCapture(t, 8)
	c.Fault()
	if _, st := c.Allocate(origin, 0, 1); st != Faulted {
		t.Errorf("faulted capture: status %v, want Faulted", st)
	}

	c, gen := newTestCapture(t, 8)
	gen.Add(1)
	if _, st := c.Allocate(origin, 0, 1); st != Abandoned {
		t.Errorf("capture of an older generation: status %v, want Abandoned", st)
	}
}

func TestSocketTableExhaustionFaults(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 8)

	for s := range uint64(MaxSockets) {
		if _, st := c.Allocate(origin, 0, s+1); st != Allocated {
			t.Fatalf("socket %d: status %v", s+1, st)
		}
	}
	if _, st := c.Allocate(origin, 0, MaxSockets+1); st != Faulted {
		t.Fatalf("socket %d: status %v, want Faulted", MaxSockets+1, st)
	}
	if _, st := c.Allocate(origin, 0, 1); st != Faulted {
		t.Errorf("after exhaustion: status %v, want the capture to stay faulted", st)
	}
}

func TestReservationExhaustionFaults(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 2) // the start's reservation plus one

	// Two steps without retiring: the second has no room.
	if a, st := c.Allocate(origin+3*eps, 0, 1); st != Allocated || !a.Step {
		t.Fatalf("first step: status %v step %v", st, a.Step)
	}
	if _, st := c.Allocate(origin, 1, 1); st != Faulted {
		t.Errorf("second step with a full reservation set: status %v, want Faulted", st)
	}
}

func TestStartIsReservedSeqZero(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 8)

	if got, ok := c.MinReserved(); !ok || got != 0 {
		t.Fatalf("MinReserved = %d, %v; want the start's seq 0", got, ok)
	}
	if got, ok := c.OldestReservation(); !ok || got != 0 {
		t.Errorf("OldestReservation = %d, %v; want mono 0", got, ok)
	}
}

func TestAllocateDoesNotAllocate(t *testing.T) {
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: eps, ReservedCap: 8, OriginWall: origin})
	var mono int64
	allocs := testing.AllocsPerRun(1000, func() {
		mono++
		c.Allocate(origin+mono, mono, uint64(mono%3)+1)
	})
	if allocs != 0 {
		t.Errorf("Allocate allocates %v times per call, want 0", allocs)
	}
}

// End's deadline keeps its monotonic reading, so a wall-clock step cannot move it.
func TestEndByKeepsTheMonotonicReading(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 8)
	dl := time.Now().Add(time.Second)
	c.SetEndBy(dl)
	got, ok := c.EndBy()
	// time.Time.String prints the monotonic reading as "m=..." only when the value carries one.
	if !ok || !got.Equal(dl) || !strings.Contains(got.String(), "m=") {
		t.Errorf("EndBy = %v, %v; want the stamped time with its monotonic reading", got, ok)
	}
}

// A record counts as queued from its allocation, before its callback sends it.
func TestAllocationCountsAsQueued(t *testing.T) {
	t.Parallel()
	c, _ := newTestCapture(t, 8)
	if _, st := c.Allocate(origin+3*eps, 0, 1); st != Allocated { // a clock step and its record
		t.Fatal(st)
	}
	if got := c.Queued(); got != 2 {
		t.Errorf("Queued = %d after allocating a step and a record, want 2", got)
	}
}
