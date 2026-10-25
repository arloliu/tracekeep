package producer_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// endSlack is how far past its deadline End may return.
const endSlack = time.Second

// holdAt holds the first goroutine that reaches the recorder's hook id until release is closed;
// held is closed once it is held.
func holdAt(rec *core.Recorder, id int) (held, release chan struct{}) {
	held, release = make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	rec.SetHook(id, func() {
		if first.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
	})

	return held, release
}

// observeHeld runs one observation on its own goroutine, held inside the capture's critical section;
// close release to let it finish, then wait on done.
func (h *harness) observeHeld() (release, done chan struct{}) {
	h.t.Helper()
	held, release := holdAt(h.rec, core.HookLocked)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		h.observe(frame(4))
	}()
	<-held

	return release, finished
}

// endBy runs End with a deadline d from now and fails the test if it returns more than endSlack past it.
func (h *harness) endBy(d time.Duration) producer.Report {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), d)
	defer cancel()
	start := time.Now()
	rep := h.p.End(ctx)
	if took := time.Since(start); took > d+endSlack {
		h.t.Errorf("End took %v with a %v deadline", took, d)
	}

	return rep
}

// A callback holds the capture's lock while End runs: End abandons the capture by its deadline,
// and the record the callback then queues is dropped as abandoned, never published.
func TestEndAbandonsACaptureACallbackHolds(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.begin()
	h.wait(1)
	release, done := h.observeHeld()

	rep := h.endBy(200 * time.Millisecond)
	if !rep.Abandoned || rep.StopQueued || h.rec.Current() != nil {
		t.Errorf("End = %+v with capture %v, want it abandoned without its stop", rep, h.rec.Current())
	}
	close(release)
	<-done
	_ = h.p.Close()

	if got := h.m.get("records_dropped_total{reason=abandoned}"); got != 1 {
		t.Errorf("dropped{abandoned} = %v, want the held record", got)
	}
	if n := len(h.bus.Messages()); n != 1 {
		t.Errorf("%d messages on the bus, want only the start", n)
	}
}

// A HotReload waits in its TryLock loop on a capture lock a callback holds when Stop arrives:
// the rotation abandons as soon as End announces itself, its start is never published,
// and End then abandons the capture by its own deadline.
func TestHotReloadBlockedOnACaptureLockWhenStopArrives(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.begin()
	h.wait(1)
	release, done := h.observeHeld()

	trying, retry := holdAt(h.rec, core.HookTryLockFailed)
	const rotateDeadline = 5 * time.Second
	rotated := make(chan error, 1)
	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rotateDeadline)
		defer cancel()
		rotated <- h.p.Rotate(ctx, producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp-reloaded"})
	}()
	<-trying // the rotation holds the lifecycle lock and loops on the capture lock
	ended := make(chan producer.Report, 1)
	go func() { ended <- h.endBy(300 * time.Millisecond) }()
	close(retry)

	// Only End's announcement can end the loop before the rotation's own deadline: the callback still holds the lock.
	if err := <-rotated; !errors.Is(err, producer.ErrRotationAbandoned) {
		t.Errorf("Rotate = %v, want ErrRotationAbandoned", err)
	}
	if took := time.Since(start); took >= rotateDeadline {
		t.Errorf("the rotation ran to its own deadline (%v): it never saw End", took)
	}
	rep := <-ended
	if !rep.Abandoned {
		t.Errorf("End = %+v, want the capture abandoned", rep)
	}
	close(release)
	<-done
	_ = h.p.Close()

	if n := len(h.bus.Messages()); n != 1 {
		t.Errorf("%d messages on the bus, want only the first start", n)
	}
}
