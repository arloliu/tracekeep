package core

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a monotonic clock a test moves by hand.
type fakeClock struct {
	mu sync.Mutex
	ns int64
}

func (c *fakeClock) now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ns
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ns += int64(d)
}

func newTestPending(t *testing.T, maxKeys int) (*PendingTable, *fakeClock) {
	t.Helper()
	clk := &fakeClock{}
	pt := NewPendingTable(PendingParams{Grace: 5 * time.Second, Unawaited: 30 * time.Second, MaxKeys: maxKeys, Now: clk.now})

	return pt, clk
}

func ignored(pt *PendingTable, r IgnoredReason) uint64 { return pt.Counters().Ignored[r] }

func TestPendingReplySealsTheEntry(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")

	e.Sent()
	e.Awaiting(time.Minute)
	e.Replied()
	if e.State() != PendingCompleted {
		t.Fatalf("state %v, want completed", e.State())
	}
	// Every later report, of either kind, is a counted no-op.
	e.TimedOut(time.Minute)
	e.SendFailed()
	e.Awaiting(time.Minute)
	if e.State() != PendingCompleted {
		t.Fatalf("state %v after late reports, want completed", e.State())
	}
	if n := ignored(pt, IgnoredSealed); n != 3 {
		t.Fatalf("sealed no-ops %d, want 3", n)
	}
	if o := e.Outcome(); o != OutcomeReplied {
		t.Fatalf("outcome %v, want replied", o)
	}
}

func TestPendingProgressIsFirstWins(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")

	e.Sent()
	e.SendFailed()
	e.SubmissionFailed()
	if e.State() != PendingLive {
		t.Fatalf("state %v, want live: a failure after Sent is not the first progress", e.State())
	}
	if n := ignored(pt, IgnoredDuplicateProgress); n != 2 {
		t.Fatalf("duplicate progress %d, want 2", n)
	}
}

func TestPendingFailureRemovesALiveEntry(t *testing.T) {
	t.Parallel()
	for _, fail := range []func(*PendingEntry){(*PendingEntry).SendFailed, (*PendingEntry).SubmissionFailed} {
		pt, _ := newTestPending(t, 0)
		e := pt.Register(1, 100, "c")

		fail(e)
		if e.State() != PendingRemoved {
			t.Fatalf("state %v, want removed", e.State())
		}
		// A waiter that times out after a failed send emits nothing: the entry is gone.
		e.TimedOut(time.Minute)
		if n := ignored(pt, IgnoredRemoved); n != 1 {
			t.Fatalf("removed no-ops %d, want 1", n)
		}
		if n := pt.Counters().Live; n != 0 {
			t.Fatalf("live entries %d, want 0", n)
		}
		// The key stays in the ambiguity set.
		if e2 := pt.Register(1, 100, "c"); !e2.Ambiguous() {
			t.Fatalf("a re-registered key after a failure is not ambiguous")
		}
	}
}

func TestPendingSecondAwaitingIsIgnored(t *testing.T) {
	t.Parallel()
	pt, clk := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")

	e.Awaiting(10 * time.Second)
	e.Awaiting(time.Hour)
	if n := ignored(pt, IgnoredDuplicateAwaiting); n != 1 {
		t.Fatalf("duplicate awaiting %d, want 1", n)
	}
	// The first wait's expiry holds: 10 s plus the 5 s grace.
	clk.advance(15 * time.Second)
	pt.Sweep()
	if e.State() != PendingExpired {
		t.Fatalf("state %v, want expired at the first wait's expiry", e.State())
	}
}

func TestPendingExpiryFollowsTheWait(t *testing.T) {
	t.Parallel()
	pt, clk := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")

	clk.advance(10 * time.Second)
	e.Awaiting(time.Minute) // expires at 10 s + 60 s + 5 s
	clk.advance(64 * time.Second)
	pt.Sweep()
	if e.State() != PendingLive {
		t.Fatalf("state %v at 74 s, want live", e.State())
	}
	clk.advance(time.Second)
	// Expiry is no outcome: a report at the deadline finds the entry expired even before a sweep.
	e.TimedOut(time.Minute)
	if e.State() != PendingExpired {
		t.Fatalf("state %v at 75 s, want expired", e.State())
	}
	if n := ignored(pt, IgnoredExpired); n != 1 {
		t.Fatalf("expired no-ops %d, want 1", n)
	}
}

func TestPendingUnawaitedEntryExpires(t *testing.T) {
	t.Parallel()
	pt, clk := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")
	e.Sent()

	clk.advance(30*time.Second - 1)
	pt.Sweep()
	if n := pt.Counters().Live; n != 1 {
		t.Fatalf("live entries %d before 30 s, want 1", n)
	}
	clk.advance(1)
	pt.Sweep()
	if e.State() != PendingExpired || pt.Counters().Live != 0 {
		t.Fatalf("state %v, live %d at 30 s; want expired and none live", e.State(), pt.Counters().Live)
	}
}

func TestPendingCompletedEntryIsKeptUntilExpiry(t *testing.T) {
	t.Parallel()
	pt, clk := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")
	e.Awaiting(time.Second)
	e.Replied()

	pt.Sweep()
	if got := pt.Lookup(1, 100); got != e {
		t.Fatalf("a completed entry is not found before its expiry")
	}
	clk.advance(6 * time.Second)
	pt.Sweep()
	if e.State() != PendingExpired {
		t.Fatalf("state %v, want expired", e.State())
	}
	if got := pt.Lookup(1, 100); got != nil {
		t.Fatalf("an expired entry is still found")
	}
}

func TestPendingReusedKeyIsAmbiguous(t *testing.T) {
	t.Parallel()
	pt, clk := newTestPending(t, 0)
	a := pt.Register(1, 100, "c")
	other := pt.Register(2, 100, "c")
	b := pt.Register(1, 100, "c")

	if !a.Ambiguous() || !b.Ambiguous() {
		t.Fatalf("ambiguous a=%v b=%v, want both", a.Ambiguous(), b.Ambiguous())
	}
	if other.Ambiguous() {
		t.Fatalf("another session's key is ambiguous")
	}
	if got := pt.Lookup(1, 100); got != b {
		t.Fatalf("lookup finds an older entry")
	}
	// Over the process's lifetime: a key reused long after its entry expired is still ambiguous.
	clk.advance(time.Hour)
	pt.Sweep()
	if c := pt.Register(1, 100, "c"); !c.Ambiguous() {
		t.Fatalf("a key reused after expiry is not ambiguous")
	}
	if n := pt.Counters().Ambiguous; n != 3 {
		t.Fatalf("ambiguous count %d, want 3", n)
	}
}

func TestPendingKeySetCap(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 2)
	pt.Register(1, 1, "c")
	pt.Register(1, 2, "c")
	if pt.Counters().Capped {
		t.Fatalf("capped at the cap")
	}
	e := pt.Register(1, 3, "c")
	if !pt.Counters().Capped {
		t.Fatalf("not capped past the cap")
	}
	// Past the cap a live entry of the same key is still found.
	if f := pt.Register(1, 3, "c"); !f.Ambiguous() || !e.Ambiguous() {
		t.Fatalf("a reused live key past the cap is not ambiguous")
	}
}

func TestPendingCompetingOutcomesSealOnce(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")
	e.Awaiting(time.Minute)

	reports := []func(){
		e.Replied, func() { e.TimedOut(time.Minute) }, e.Cancelled,
		e.Replied, func() { e.TimedOut(time.Minute) }, e.Cancelled,
		e.SendFailed, e.Sent,
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, r := range reports {
		wg.Go(func() {
			<-start
			r()
		})
	}
	close(start)
	wg.Wait()

	if e.State() != PendingCompleted && e.State() != PendingRemoved {
		t.Fatalf("state %v, want completed or removed", e.State())
	}
	c := pt.Counters()
	var total uint64
	for _, n := range c.Ignored {
		total += n
	}
	// One report changed the entry's state; at most one progress report may also have been taken first.
	if total != uint64(len(reports)-1) && total != uint64(len(reports)-2) {
		t.Fatalf("ignored %d of %d reports", total, len(reports))
	}
}

func TestPendingReportsDoNotAllocate(t *testing.T) {
	pt, _ := newTestPending(t, 0)
	e := pt.Register(1, 100, "c")
	n := testing.AllocsPerRun(100, func() {
		e.Sent()
		e.Awaiting(time.Minute)
		e.Replied()
		e.TimedOut(time.Minute)
	})
	if n != 0 {
		t.Fatalf("reports allocate %v times", n)
	}
}
