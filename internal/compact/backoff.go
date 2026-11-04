package compact

import (
	"cmp"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// DefaultMergeAttempts is compact.merge_attempts' default, the merges a cycle attempts when MergeAttempts is zero.
const DefaultMergeAttempts = 1024

// schedule is the merge pass's memory across cycles (compactor.md §8.1): the failing scopes' backoffs and the scopes the last cycle's cap left.
// It is this process's alone, kept across its own lease renewals and reacquisitions,
// and dropped by a take whose fence token is not the last one's plus one, since another process held the lease between them.
// It is best effort: forgetting it allows extra attempts and nothing else.
type schedule struct {
	// cycle counts the merge passes; a backoff is measured in them.
	cycle int64
	// cooling holds the scopes whose last attempt failed, at most those a cycle met: forget drops the rest.
	cooling map[catalog.Scope]*cooling
	// carried are the scheduled scopes the last cycle's cap left, in their order.
	carried []catalog.Scope
	// fence is the fence token of this process's last take.
	fence int64
}

// cooling is a failing scope's backoff, keyed by the revision its failed snapshot read.
type cooling struct {
	revision int64
	failures int
	// due is the first cycle that attempts the scope again.
	due int64
	// requests are the operators' requests attempted for the scope at this revision, which never bypass its wait again.
	requests map[tracepack.UUID]bool
}

// mergeCycle is one cycle's merges: the attempts it has left, the scopes it attempted, each at most once,
// and the scopes it met, a candidate or the scope of a pending merge request, whose backoffs it keeps.
type mergeCycle struct {
	left      int
	attempted map[catalog.Scope]bool
	met       map[catalog.Scope]bool
}

// newCycle starts a cycle of merges, the operators' requests and the scheduled scopes together.
func (r *Compactor) newCycle() *mergeCycle {
	r.sched.cycle++

	return &mergeCycle{left: cmp.Or(r.MergeAttempts, DefaultMergeAttempts), attempted: map[catalog.Scope]bool{}, met: map[catalog.Scope]bool{}}
}

// forget drops the backoffs of the scopes cyc did not meet, after both its passes completed:
// a scope neither a candidate nor requested, evicted by retention, touched by a conflict or with its request ended,
// would otherwise stay for the process's life. A scope met again later starts over at one cycle.
func (r *Compactor) forget(cyc *mergeCycle) {
	for s := range r.sched.cooling {
		if !cyc.met[s] {
			delete(r.sched.cooling, s)
		}
	}
}

// took records a take of the lease, dropping the schedule when another process held the lease since this process's last take.
func (r *Compactor) took(l catalog.Lease) {
	if l.Fence != r.sched.fence+1 {
		r.sched = schedule{}
	}
	r.sched.fence = l.Fence
}

// waiting reports whether s, at its current revision, waits out a failure's backoff this cycle;
// a backoff of another revision is forgotten, since work registered after the failed snapshot makes the scope due at once.
func (r *Compactor) waiting(s catalog.Scope, revision int64) *cooling {
	c := r.sched.cooling[s]
	if c == nil {
		return nil
	}
	if c.revision != revision {
		delete(r.sched.cooling, s)
		return nil
	}
	if r.sched.cycle >= c.due {
		return nil
	}

	return c
}

// failed records a failed attempt of s, whose snapshot read revision, for request when it is not nil:
// the wait doubles with each failure of the same revision, from one cycle to the cycles of an hour.
func (r *Compactor) failed(s catalog.Scope, revision int64, request *tracepack.UUID) {
	c := r.sched.cooling[s]
	if c == nil || c.revision != revision {
		c = &cooling{revision: revision, requests: map[tracepack.UUID]bool{}}
		if r.sched.cooling == nil {
			r.sched.cooling = map[catalog.Scope]*cooling{}
		}
		r.sched.cooling[s] = c
	}
	c.failures++
	c.due = r.sched.cycle + min(int64(1)<<min(c.failures-1, 32), r.hourCycles())
	if request != nil {
		c.requests[*request] = true
	}
}

// hourCycles is the cycles of an hour at Interval, at least one; one without an interval.
func (r *Compactor) hourCycles() int64 {
	if r.Interval <= 0 {
		return 1
	}

	return max(1, int64(time.Hour/r.Interval))
}
