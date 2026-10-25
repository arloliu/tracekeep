package core

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

type testClock struct{ wall, mono atomic.Int64 }

func (c *testClock) now() (int64, int64) { return c.wall.Load(), c.mono.Load() }

func newRotateRecorder(t *testing.T) (*Recorder, *atomic.Uint64, *testClock) {
	t.Helper()
	r := NewRecorder(RecorderParams{ReservedCap: 16, OrdinaryRecords: 16, OrdinaryBytes: 1 << 20})
	var gen atomic.Uint64
	clk := &testClock{}
	clk.wall.Store(origin)

	return r, &gen, clk
}

func prepared(gen *atomic.Uint64, clk *testClock, id byte) *Capture {
	w, m := clk.now()
	return NewCapture(CaptureParams{ID: tracepack.UUID{id}, Generation: gen.Load(), Lifecycle: gen, Tolerance: eps, ReservedCap: 16, OriginWall: w, OriginMono: m})
}

func drainReserved(r *Recorder) []Entry {
	var out []Entry
	for {
		select {
		case e := <-r.Reserved():
			out = append(out, e)
		default:
			return out
		}
	}
}

func never() bool { return false }

func TestRotateInstallsAndStops(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)

	a := prepared(gen, clk, 1)
	if res := r.Rotate(a, time.Now().Add(time.Second), never, clk.now); !res.Installed {
		t.Fatalf("first rotation: %+v", res)
	}
	r.Observe(frameObs(1, []byte{1})) // seq 1 of a

	clk.mono.Store(10)
	clk.wall.Store(origin + 10)
	b := prepared(gen, clk, 2)
	res := r.Rotate(b, time.Now().Add(time.Second), never, clk.now)
	if !res.Installed || res.Old != a || !res.StopQueued {
		t.Fatalf("second rotation: %+v", res)
	}
	if r.Current() != b {
		t.Errorf("current capture is not the new one")
	}
	if a.StartState() != StartInstalled || b.StartState() != StartInstalled {
		t.Errorf("start states %v, %v; want both installed", a.StartState(), b.StartState())
	}

	got := drainReserved(r)
	want := []struct {
		cap  *Capture
		kind EntryKind
		seq  uint64
	}{{a, EntryStart, 0}, {b, EntryStart, 0}, {a, EntryStop, 2}}
	if len(got) != len(want) {
		t.Fatalf("reserved lane holds %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].Cap != w.cap || got[i].Kind != w.kind || got[i].Seq != w.seq {
			t.Errorf("reserved[%d] = %v seq %d of %v, want %v seq %d of %v", i, got[i].Kind, got[i].Seq, got[i].Cap.ID, w.kind, w.seq, w.cap.ID)
		}
	}
	if _, ok := a.MinReserved(); ok {
		t.Errorf("the old capture still holds a reservation")
	}

	// A callback after the rotation lands in the new capture.
	r.Observe(frameObs(11, []byte{1}))
	popOrdinary(t, r)
	if e := popOrdinary(t, r); e.Cap != b || e.Seq != 1 {
		t.Errorf("post-rotation record: seq %d of %v, want seq 1 of the new capture", e.Seq, e.Cap.ID)
	}
}

func TestRotateClockStepBeforeStop(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	drainReserved(r)

	clk.wall.Store(origin + 3*eps) // the wall clock stepped; mono did not move
	res := r.Rotate(nil, time.Now().Add(time.Second), never, clk.now)
	if !res.Installed || !res.StopQueued {
		t.Fatalf("rotation to nil: %+v", res)
	}
	got := drainReserved(r)
	if len(got) != 2 || got[0].Kind != EntryClockStep || got[0].Seq != 1 || got[1].Kind != EntryStop || got[1].Seq != 2 {
		t.Fatalf("reserved lane = %+v, want the clock step at 1 then the stop at 2", got)
	}
	if r.Current() != nil {
		t.Errorf("rotation to nil left a current capture")
	}
}

func TestRotateAbandonsWhenTheOldLockIsHeld(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	drainReserved(r)

	a.mu.Lock() // a callback holds the old capture's lock past the deadline
	b := prepared(gen, clk, 2)
	res := r.Rotate(b, time.Now().Add(20*time.Millisecond), never, clk.now)
	a.mu.Unlock()

	if res.Installed || !res.Abandoned {
		t.Fatalf("rotation under a held lock: %+v, want abandoned", res)
	}
	if r.Current() != a {
		t.Errorf("an abandoned rotation changed the current capture")
	}
	if b.StartState() != StartDiscarded {
		t.Errorf("prepared start state %v, want discarded", b.StartState())
	}
	if got := drainReserved(r); len(got) != 1 || got[0].Cap != b || got[0].Kind != EntryStart {
		t.Errorf("reserved lane = %+v, want only the discarded start the publisher will skip", got)
	}
	if _, st := a.Allocate(origin, 1, 1); st != Allocated {
		t.Errorf("the old capture is not open after an abandoned rotation: %v", st)
	}
}

func TestRotateAbortsWhenStopping(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)

	a.mu.Lock()
	var stopping atomic.Bool
	done := make(chan RotateResult)
	go func() {
		done <- r.Rotate(prepared(gen, clk, 2), time.Now().Add(time.Hour), stopping.Load, clk.now)
	}()
	stopping.Store(true)
	res := <-done // returns long before the hour-long deadline
	a.mu.Unlock()

	if res.Installed || !res.Abandoned {
		t.Errorf("rotation while stopping: %+v, want abandoned", res)
	}
}

func TestRotateFaultedOldGetsNoStop(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	drainReserved(r)
	a.Fault()

	res := r.Rotate(prepared(gen, clk, 2), time.Now().Add(time.Second), never, clk.now)
	if !res.Installed || res.StopQueued {
		t.Fatalf("rotation from a faulted capture: %+v, want installed without a stop", res)
	}
	if got := drainReserved(r); len(got) != 1 || got[0].Kind != EntryStart {
		t.Errorf("reserved lane = %+v, want only the new start", got)
	}
	if _, st := a.Allocate(origin, 1, 1); st != Faulted {
		t.Errorf("faulted capture reads %v after the rotation, want Faulted", st)
	}
}

// A rotation needs room for the old capture's clock step and stop before it commits; without it, it installs nothing.
func TestRotateWithoutRoomForTheBoundariesAbandons(t *testing.T) {
	t.Parallel()
	r := NewRecorder(RecorderParams{ReservedCap: 2, OrdinaryRecords: 4, OrdinaryBytes: 1 << 10})
	var gen atomic.Uint64
	clk := &testClock{}
	clk.wall.Store(origin)
	a := prepared(&gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now) // a's start takes one of the two places

	res := r.Rotate(nil, time.Now().Add(time.Second), never, clk.now)
	if res.Installed || !res.Abandoned {
		t.Fatalf("rotation without room for the stop: %+v, want abandoned", res)
	}
	if r.Current() != a {
		t.Errorf("an abandoned rotation changed the current capture")
	}
	if _, st := a.Allocate(origin, 1, 1); st != Allocated {
		t.Errorf("the old capture reads %v, want still open", st)
	}
}

func TestRotateAfterTheDeadlineAbandons(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	b := prepared(gen, clk, 1)
	res := r.Rotate(b, time.Now().Add(-time.Millisecond), never, clk.now)
	if res.Installed || !res.Abandoned || b.StartState() != StartDiscarded || r.Current() != nil {
		t.Errorf("rotation past its deadline: %+v, start %v; want nothing installed", res, b.StartState())
	}
	cancelled := func() bool { return true }
	if res := r.Rotate(prepared(gen, clk, 2), time.Now().Add(time.Hour), cancelled, clk.now); res.Installed {
		t.Errorf("a cancelled rotation was installed")
	}
}

// The reserved lane's bound counts what the publisher holds: a popped but unaccepted record keeps its place.
func TestReservedBoundCountsHeldRecords(t *testing.T) {
	t.Parallel()
	r := NewRecorder(RecorderParams{ReservedCap: 2, OrdinaryRecords: 4, OrdinaryBytes: 1 << 10})
	for i := range 2 {
		if !r.PushReserved(Entry{Kind: EntryClockStep, Seq: uint64(i)}) {
			t.Fatalf("push %d refused", i)
		}
	}
	<-r.Reserved()
	<-r.Reserved() // popped, not yet accepted
	if r.PushReserved(Entry{Kind: EntryClockStep, Seq: 2}) {
		t.Fatalf("a push past the bound was accepted while the publisher holds two records")
	}
	r.ReleaseReserved()
	if !r.PushReserved(Entry{Kind: EntryClockStep, Seq: 2}) {
		t.Errorf("a released place was not reusable")
	}
}

func TestSeqLimitAsksForRotation(t *testing.T) {
	t.Parallel()
	r := NewRecorder(RecorderParams{ReservedCap: 8, OrdinaryRecords: 8, OrdinaryBytes: 1 << 10})
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: eps, ReservedCap: 8, OriginWall: origin, RotateAt: 2})
	c.Retire(0)
	r.cur.Store(c)

	r.Observe(frameObs(1, []byte{1})) // seq 1
	select {
	case <-r.Rotations():
		t.Fatalf("rotation asked before RotateAt")
	default:
	}
	r.Observe(frameObs(2, []byte{1})) // seq 2
	r.Observe(frameObs(3, []byte{1})) // seq 3: asked only once
	select {
	case got := <-r.Rotations():
		if got != c {
			t.Errorf("rotation asked for another capture")
		}
	default:
		t.Fatalf("no rotation asked at RotateAt")
	}
	if len(r.Rotations()) != 0 {
		t.Errorf("rotation asked more than once")
	}
}

func TestAbandonAdvancesTheGeneration(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)

	r.Abandon(gen)
	if r.Current() != nil {
		t.Errorf("Abandon left a current capture")
	}
	if !a.Abandoned() {
		t.Errorf("capture of the old generation does not read as abandoned")
	}
	if _, st := a.Allocate(origin, 1, 1); st != Abandoned {
		t.Errorf("allocation after abandonment: %v, want Abandoned", st)
	}
}

func TestObserveRacingARotationEndsOnTheNewCapture(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)

	// A callback loaded a's pointer and waits for a's lock while the rotation holds it.
	held, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	r.hooks[hookLoaded] = func() {
		if first.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Observe(frameObs(1, []byte{1}))
	}()
	<-held
	b := prepared(gen, clk, 2)
	r.Rotate(b, time.Now().Add(time.Second), never, clk.now)
	close(release)
	<-done

	if e := popOrdinary(t, r); e.Cap != b || e.Seq != 1 {
		t.Errorf("the racing record landed at seq %d of %v, want seq 1 of the new capture", e.Seq, e.Cap.ID)
	}
}

// A capture closed by a rotation and faulted afterwards still sends callbacks to its successor.
func TestFaultAfterRotationKeepsClosed(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	b := prepared(gen, clk, 2)
	r.Rotate(b, time.Now().Add(time.Second), never, clk.now)

	a.Fault() // e.g. a step push that failed after the rotation
	if _, st := a.Allocate(origin, 1, 1); st != Closed {
		t.Errorf("rotated-then-faulted capture reads %v, want Closed", st)
	}
}

// A panic before allocation whose fault loaded a capture that a rotation then closed faults the successor instead:
// the loss has no seq, so only a fault window can show it.
func TestPanicFaultRacingARotationFaultsTheSuccessor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		panic func(r *Recorder)
	}{
		{"before Observe", func(r *Recorder) {
			defer r.Recover()
			panic("injected")
		}},
		{"inside Observe", func(r *Recorder) {
			r.hooks[hookBeforeAllocate] = func() { panic("injected") }
			r.Observe(frameObs(1, []byte{1}))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, gen, clk := newRotateRecorder(t)
			a := prepared(gen, clk, 1)
			r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
			b := prepared(gen, clk, 2)
			var once atomic.Bool
			r.hooks[hookFaultLoaded] = func() {
				if once.CompareAndSwap(false, true) {
					r.Rotate(b, time.Now().Add(time.Second), never, clk.now)
				}
			}

			tt.panic(r)

			if a.Faulted() || !b.Faulted() {
				t.Errorf("faulted: old %v, successor %v; want only the successor", a.Faulted(), b.Faulted())
			}
			if got := r.Counters().Panics; got != 1 {
				t.Errorf("panics = %d, want 1", got)
			}
		})
	}
}

// A callback that loaded a faulted capture and panics after a rotation installed its successor leaves the successor healthy:
// the loss is inside the faulted capture's window.
func TestPanicAfterLoadingAFaultedCaptureKeepsTheSuccessor(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	a.Fault()
	b := prepared(gen, clk, 2)
	r.hooks[hookLoaded] = func() {
		r.Rotate(b, time.Now().Add(time.Second), never, clk.now)
		panic("injected")
	}

	r.Observe(frameObs(1, []byte{1}))

	if !a.Faulted() || b.Faulted() {
		t.Errorf("faulted: old %v, successor %v; want only the old", a.Faulted(), b.Faulted())
	}
	select {
	case got := <-r.Faults():
		if got != a {
			t.Errorf("fault signal for the successor")
		}
	default:
		t.Errorf("no fault signal")
	}
}
