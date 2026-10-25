package core

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

func newTestRecorder(t *testing.T, p RecorderParams) (*Recorder, *Capture) {
	t.Helper()
	if p.ReservedCap == 0 {
		p.ReservedCap = 8
	}
	if p.OrdinaryRecords == 0 {
		p.OrdinaryRecords = 8
	}
	if p.OrdinaryBytes == 0 {
		p.OrdinaryBytes = 1 << 20
	}
	if p.Pending.Unawaited == 0 {
		p.Pending = PendingParams{Grace: 5 * time.Second, Unawaited: 30 * time.Second}
	}
	r := NewRecorder(p)
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: eps, ReservedCap: p.ReservedCap, OriginWall: origin})
	c.Retire(0) // the start is queued
	r.cur.Store(c)

	return r, c
}

func frameObs(mono int64, payload []byte) *Observation {
	return &Observation{
		Wall:    origin + mono,
		Mono:    mono,
		Socket:  1,
		Kind:    tracepack.KindData,
		Dir:     tracepack.DirEquipmentToHost,
		Payload: payload,
	}
}

func popOrdinary(t *testing.T, r *Recorder) Entry {
	t.Helper()
	select {
	case e := <-r.Ordinary():
		r.ReleaseOrdinary(&e)
		return e
	default:
		t.Fatalf("ordinary lane is empty")
		return Entry{}
	}
}

func TestObserveQueuesACopy(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{})

	frame := []byte{0, 0, 0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	r.Observe(frameObs(5, frame))
	frame[4] = 0xff // go-secs reuses the borrowed frame after the callback

	e := popOrdinary(t, r)
	if e.Cap != c || e.Kind != EntryObserved || e.Seq != 1 || e.Epoch != 1 || e.Mono != 5 || e.Wall != origin+5 {
		t.Errorf("entry = %+v", e)
	}
	if e.Payload[4] != 1 {
		t.Errorf("payload aliases the borrowed frame")
	}
	if got := r.Counters().Observed[tracepack.KindData][tracepack.DirEquipmentToHost]; got != 1 {
		t.Errorf("observed{data,equipment-to-host} = %d, want 1", got)
	}
}

func TestObserveClockStepGoesFirstToTheReservedLane(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{})

	o := frameObs(5, []byte{1})
	o.Wall += 3 * eps
	r.Observe(o)

	select {
	case e := <-r.Reserved():
		if e.Kind != EntryClockStep || e.Seq != 1 || e.Drift != 3*eps || e.Cap != c {
			t.Errorf("reserved entry = %+v, want the clock step at seq 1", e)
		}
	default:
		t.Fatalf("no clock step in the reserved lane")
	}
	if e := popOrdinary(t, r); e.Seq != 2 {
		t.Errorf("ordinary seq %d, want 2", e.Seq)
	}
	if _, ok := c.MinReserved(); ok {
		t.Errorf("the queued step is still reserved")
	}
}

func TestObserveOrdinaryLaneBudgets(t *testing.T) {
	t.Parallel()

	t.Run("count", func(t *testing.T) {
		t.Parallel()
		r, _ := newTestRecorder(t, RecorderParams{OrdinaryRecords: 2})
		for i := range 3 {
			r.Observe(frameObs(int64(i), []byte{1, 2}))
		}
		if got := r.Counters().Dropped[DropLaneFull]; got != 1 {
			t.Errorf("dropped{lane_full} = %d, want 1", got)
		}
		popOrdinary(t, r)
		r.Observe(frameObs(9, []byte{1}))
		popOrdinary(t, r)
		// The dropped record kept its seq: a gap at 3.
		if e := popOrdinary(t, r); e.Seq != 4 {
			t.Errorf("seq after the drop = %d, want 4", e.Seq)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		t.Parallel()
		r, _ := newTestRecorder(t, RecorderParams{OrdinaryBytes: 10})
		r.Observe(frameObs(1, make([]byte, 6)))
		r.Observe(frameObs(2, make([]byte, 6)))
		c := r.Counters()
		if c.Dropped[DropLaneFull] != 1 || c.DroppedBytes != 6 {
			t.Errorf("dropped = %d, bytes %d; want 1 and 6", c.Dropped[DropLaneFull], c.DroppedBytes)
		}
		popOrdinary(t, r)
		r.Observe(frameObs(3, make([]byte, 10)))
		if e := popOrdinary(t, r); e.Seq != 3 {
			t.Errorf("released budget not reusable: seq %d", e.Seq)
		}
	})
}

func TestObserveAfterStop(t *testing.T) {
	t.Parallel()
	r := NewRecorder(RecorderParams{ReservedCap: 8, OrdinaryRecords: 8, OrdinaryBytes: 1 << 20})

	r.Observe(frameObs(1, []byte{1}))
	if got := r.Counters().AfterStop; got != 1 {
		t.Errorf("after_stop = %d, want 1", got)
	}
}

func TestObserveFaultedCapture(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{})
	c.Fault()

	r.Observe(frameObs(1, []byte{1}))
	if got := r.Counters().Dropped[DropCaptureFault]; got != 1 {
		t.Errorf("dropped{capture_fault} = %d, want 1", got)
	}
	if len(r.Ordinary()) != 0 {
		t.Errorf("a faulted capture queued a record")
	}
}

func TestObserveReservedLaneFullFaults(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{ReservedCap: 1})
	r.PushReserved(Entry{Cap: c, Kind: EntryStart}) // the lane's one slot is taken

	o := frameObs(5, []byte{1})
	o.Wall += 3 * eps
	r.Observe(o)

	if !c.Faulted() {
		t.Fatalf("a clock step that found the reserved lane full did not fault the capture")
	}
	select {
	case got := <-r.Faults():
		if got != c {
			t.Errorf("fault signal for %v, want the capture", got.ID)
		}
	default:
		t.Errorf("no fault signal to the lifecycle goroutine")
	}
}

func TestObservePanicBeforeAllocationFaults(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{})
	r.hooks[hookBeforeAllocate] = func() { panic("injected") }

	r.Observe(frameObs(1, []byte{1}))
	if !c.Faulted() {
		t.Errorf("a panic before allocation did not fault the capture")
	}
	if r.Counters().Panics != 1 {
		t.Errorf("panics = %d, want 1", r.Counters().Panics)
	}
	select {
	case got := <-r.Faults():
		if got != c {
			t.Errorf("fault signal for another capture")
		}
	default:
		t.Errorf("no fault signal after a panic before allocation")
	}
}

func TestObservePanicAfterAllocationLeavesAGap(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{})
	r.hooks[hookAfterUnlock] = func() { panic("injected") }

	r.Observe(frameObs(1, []byte{1}))
	if c.Faulted() {
		t.Errorf("a panic after allocation faulted the capture")
	}
	r.hooks[hookAfterUnlock] = nil
	r.Observe(frameObs(2, []byte{1}))
	if e := popOrdinary(t, r); e.Seq != 2 {
		t.Errorf("seq = %d, want 2 after the gap at 1", e.Seq)
	}
}

// A record allocated after a clock step cannot overtake the step:
// while the step's callback is held between its unlock and its enqueue,
// the step's seq stays reserved, so the dependency barrier holds back the later record.
func TestObserveStepStaysReservedUntilQueued(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{})

	held, release := make(chan struct{}), make(chan struct{})
	// Only the first callback is held; sync.Once would block the second one behind it.
	var first atomic.Bool
	r.hooks[hookAfterUnlock] = func() {
		if first.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		o := frameObs(5, []byte{1})
		o.Wall += 3 * eps
		r.Observe(o) // the step takes seq 1, the record seq 2
	}()
	<-held

	o := frameObs(6, []byte{2})
	o.Wall += 3 * eps // under the step's new anchor
	r.Observe(o)      // seq 3, queued before the step
	if got, ok := c.MinReserved(); !ok || got != 1 {
		t.Errorf("MinReserved = %d, %v while the step is unqueued; want 1", got, ok)
	}
	if e := popOrdinary(t, r); e.Seq != 3 {
		t.Errorf("first queued seq %d, want 3", e.Seq)
	}

	close(release)
	<-done
	if _, ok := c.MinReserved(); ok {
		t.Errorf("step still reserved after its enqueue")
	}
	if e := <-r.Reserved(); e.Seq != 1 {
		t.Errorf("reserved seq %d, want 1", e.Seq)
	}
}

func TestObserveDoesNotAllocateBeyondTheCopy(t *testing.T) {
	r, _ := newTestRecorder(t, RecorderParams{OrdinaryRecords: 4096})
	payload := make([]byte, 64)
	var mono int64
	allocs := testing.AllocsPerRun(1000, func() {
		mono++
		r.Observe(frameObs(mono, payload))
		e := <-r.Ordinary()
		r.ReleaseOrdinary(&e)
	})
	if allocs > 1 {
		t.Errorf("Observe allocates %v times per call, want only the frame copy", allocs)
	}
}

func TestObserveEventAllocatesNothing(t *testing.T) {
	r, _ := newTestRecorder(t, RecorderParams{OrdinaryRecords: 4096})
	ev := Event{Kind: tracepack.EventSocketClose, Cause: tracepack.CausePeerClose, CauseRaw: "go-secs:CauseIOError"}
	var mono int64
	var e Entry
	allocs := testing.AllocsPerRun(1000, func() {
		mono++
		r.Observe(&Observation{
			Wall: origin + mono, Mono: mono, Socket: 1,
			Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable,
			Event: ev,
		})
		e = <-r.Ordinary()
		r.ReleaseOrdinary(&e)
	})
	if allocs != 0 {
		t.Errorf("Observe allocates %v times per event, want 0", allocs)
	}
	if e.Event != ev || e.RecKind != tracepack.KindTransportEvent || len(e.Payload) != 0 {
		t.Errorf("entry = %+v", e)
	}
	if count, bytes := r.OrdinaryDepth(); count != 0 || bytes != 0 {
		t.Errorf("ordinary lane depth = %d records, %d bytes after release, want 0", count, bytes)
	}
}
