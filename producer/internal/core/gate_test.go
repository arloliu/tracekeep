package core

import (
	"sync/atomic"
	"testing"
	"time"
)

// holdAt holds the first callback that reaches hook id until release is closed; held is closed once it is held.
func holdAt(r *Recorder, id int) (held, release chan struct{}) {
	held, release = make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	r.hooks[id] = func() {
		if first.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
	}

	return held, release
}

// observeAsync runs Observe on its own goroutine and returns a channel closed when it returns.
func observeAsync(r *Recorder, o *Observation) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Observe(o)
	}()

	return done
}

// Two callbacks step the clock in opposite directions, the first held between its unlock and its enqueue:
// each step takes a reserved seq under the lock, so the first step's seq stays the lowest reserved
// and no record under either anchor can be published before the step it follows.
func TestOpposingClockSteps(t *testing.T) {
	t.Parallel()
	r, c := newTestRecorder(t, RecorderParams{})
	held, release := holdAt(r, hookAfterUnlock)

	ahead := frameObs(5, []byte{1})
	ahead.Wall += 3 * eps
	done := observeAsync(r, ahead) // a forward step at seq 1, its record at seq 2
	<-held

	back := frameObs(6, []byte{2}) // the wall back on the old anchor: a backward step against the new one
	r.Observe(back)                // a backward step at seq 3, its record at seq 4
	if got, ok := c.MinReserved(); !ok || got != 1 {
		t.Errorf("MinReserved = %d, %v with the forward step unqueued; want 1", got, ok)
	}
	step := <-r.Reserved()
	if step.Kind != EntryClockStep || step.Seq != 3 || step.Drift >= 0 {
		t.Errorf("second callback's reserved entry = %+v, want a backward step at seq 3", step)
	}
	if e := popOrdinary(t, r); e.Seq != 4 {
		t.Errorf("second callback's record at seq %d, want 4", e.Seq)
	}

	close(release)
	<-done
	step = <-r.Reserved()
	if step.Kind != EntryClockStep || step.Seq != 1 || step.Drift <= 0 {
		t.Errorf("first callback's reserved entry = %+v, want a forward step at seq 1", step)
	}
	if _, ok := c.MinReserved(); ok {
		t.Errorf("a step is still reserved after both were queued")
	}
}

// A callback that loaded A while rotations ran A→B and then B→C retries until it finds the open capture:
// its record is the first of C.
func TestCallbackHeldAcrossTwoRotations(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	b, c := prepared(gen, clk, 2), prepared(gen, clk, 3)

	// Each time the callback loads a pointer, the next rotation runs before it takes that capture's lock.
	var loads atomic.Int32
	r.hooks[hookLoaded] = func() {
		switch loads.Add(1) {
		case 1:
			r.Rotate(b, time.Now().Add(time.Second), never, clk.now)
		case 2:
			r.Rotate(c, time.Now().Add(time.Second), never, clk.now)
		default:
		}
	}
	r.Observe(frameObs(1, []byte{1}))

	if e := popOrdinary(t, r); e.Cap != c || e.Seq != 1 {
		t.Errorf("record at seq %d of %v, want seq 1 of C", e.Seq, e.Cap.ID)
	}
	if n := loads.Load(); n != 3 {
		t.Errorf("the callback loaded the pointer %d times, want 3", n)
	}
}

// A callback that allocated in A and is held before its enqueue while A→B→C rotates
// queues its record in A, below A's stop, which stays A's last seq.
func TestAllocatedCallbackHeldAcrossTwoRotations(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	drainReserved(r)
	held, release := holdAt(r, hookAfterUnlock)
	queued := a.Queued()

	done := observeAsync(r, frameObs(1, []byte{1}))
	<-held
	// Counted as queued from its allocation, so End waits for it.
	if n := a.Queued(); n != queued+1 {
		t.Errorf("A counts %d queued while the record is held, want %d", n, queued+1)
	}
	b, c := prepared(gen, clk, 2), prepared(gen, clk, 3)
	r.Rotate(b, time.Now().Add(time.Second), never, clk.now)
	r.Rotate(c, time.Now().Add(time.Second), never, clk.now)
	close(release)
	<-done

	if e := popOrdinary(t, r); e.Cap != a || e.Seq != 1 {
		t.Errorf("record at seq %d of %v, want seq 1 of A", e.Seq, e.Cap.ID)
	}
	var aStop Entry
	for _, e := range drainReserved(r) {
		if e.Cap == a && e.Kind == EntryStop {
			aStop = e
		}
	}
	if aStop.Seq != 2 {
		t.Errorf("A's stop at seq %d, want 2, after the held record", aStop.Seq)
	}
}

// Stop rotates to nil after a callback loaded the pointer: the callback finds the capture closed,
// reloads nil and counts the observation after the stop; the stop stays the capture's last seq.
func TestStopAfterACallbackLoadedThePointer(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	drainReserved(r)
	held, release := holdAt(r, hookLoaded)

	done := observeAsync(r, frameObs(1, []byte{1}))
	<-held
	if res := r.Rotate(nil, time.Now().Add(time.Second), never, clk.now); !res.StopQueued {
		t.Fatalf("stop rotation: %+v", res)
	}
	close(release)
	<-done

	if n := r.Counters().AfterStop; n != 1 {
		t.Errorf("after_stop = %d, want 1", n)
	}
	if n := len(r.Ordinary()); n != 0 {
		t.Errorf("%d records queued after the stop, want none", n)
	}
	if stop := drainReserved(r); len(stop) != 1 || stop[0].Kind != EntryStop || stop[0].Seq != 1 {
		t.Errorf("reserved after the stop = %+v, want A's stop at seq 1", stop)
	}
}

// A callback that loaded A before Stop abandoned it resumes after a later Start installed B:
// it finds A's generation passed, drops its observation as abandoned and allocates nothing in either capture.
func TestCallbackOfAnAbandonedCaptureAfterALaterStart(t *testing.T) {
	t.Parallel()
	r, gen, clk := newRotateRecorder(t)
	a := prepared(gen, clk, 1)
	r.Rotate(a, time.Now().Add(time.Second), never, clk.now)
	drainReserved(r)
	held, release := holdAt(r, hookLoaded)

	done := observeAsync(r, frameObs(1, []byte{1}))
	<-held
	if res := r.Rotate(nil, time.Now().Add(-time.Second), never, clk.now); !res.Abandoned {
		t.Fatalf("stop past its deadline: %+v, want abandoned", res)
	}
	r.Abandon(gen)
	b := prepared(gen, clk, 2)
	if res := r.Rotate(b, time.Now().Add(time.Second), never, clk.now); !res.Installed {
		t.Fatalf("later start: %+v", res)
	}
	close(release)
	<-done

	if n := r.Counters().Dropped[DropAbandoned]; n != 1 {
		t.Errorf("dropped{abandoned} = %d, want 1", n)
	}
	if n := len(r.Ordinary()); n != 0 {
		t.Errorf("%d records queued, want none", n)
	}
	if al, st := b.Allocate(origin+2, 2, 1); st != Allocated || al.Seq != 1 {
		t.Errorf("B's next allocation = seq %d, %v; want seq 1", al.Seq, st)
	}
}
