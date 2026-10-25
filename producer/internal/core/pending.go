package core

import (
	"sync"
	"sync/atomic"
	"time"
)

// PendingState is a pending request's state (docs/specs/tracekeep/producer.md §4.6 *Send-path token protocol*, T-31).
type PendingState uint8

const (
	// PendingLive is a registered request no outcome has ended.
	PendingLive PendingState = iota
	// PendingCompleted is a request sealed by its first terminal outcome, kept until its expiry so a late callback finds it.
	PendingCompleted
	// PendingRemoved is a request whose send failed before any outcome.
	PendingRemoved
	// PendingExpired is a request past its expiry; expiry is no outcome.
	PendingExpired
)

// Outcome is a request's terminal outcome, reported by the waiter.
type Outcome uint8

const (
	// OutcomeNone is no terminal outcome yet.
	OutcomeNone Outcome = iota
	// OutcomeReplied is a reply.
	OutcomeReplied
	// OutcomeTimedOut is the waiter's timeout, the only source of a forwarded request's T3.
	OutcomeTimedOut
	// OutcomeCancelled is a wait that ended any other way.
	OutcomeCancelled
)

// IgnoredReason names why a report on a token changed nothing (operations.md, pending_reports_ignored_total).
type IgnoredReason uint8

const (
	// IgnoredDuplicateProgress is a progress report after the token's first.
	IgnoredDuplicateProgress IgnoredReason = iota
	// IgnoredDuplicateAwaiting is an Awaiting after the token's first.
	IgnoredDuplicateAwaiting
	// IgnoredSealed is any report after the first terminal outcome.
	IgnoredSealed
	// IgnoredRemoved is any report after a failure removed the entry.
	IgnoredRemoved
	// IgnoredExpired is any report after the entry expired.
	IgnoredExpired
	// NumIgnoredReasons counts the reasons.
	NumIgnoredReasons
)

type progress uint8

const (
	progressNone progress = iota
	progressSent
	progressFailed
)

// PendingParams configures NewPendingTable.
type PendingParams struct {
	// Grace is the reporting grace past an awaited entry's wait.
	Grace time.Duration
	// Unawaited is how long after its registration an entry no wait was opened on expires.
	Unawaited time.Duration
	// MaxKeys caps the ambiguity set; zero leaves it unbounded.
	MaxKeys int
	// Now reads the monotonic clock in nanoseconds; nil takes the process's.
	Now func() int64
}

// PendingCounters is a snapshot of the table's counts; the publisher reports it to Metrics.
type PendingCounters struct {
	// Live counts the entries neither removed nor expired.
	Live int
	// Ignored counts the no-op reports by reason.
	Ignored [NumIgnoredReasons]uint64
	// Ambiguous counts the entries marked ambiguous.
	Ambiguous uint64
	// Capped reports that the ambiguity set reached its cap: keys registered since are not remembered past their entry.
	Capped bool
}

// PendingTable is the pending-request table of producer.md §4.6:
// every forwarded W-bit primary is registered in it before it is sent, and its token reports the request's fate.
// Its calls are safe from any goroutine and never block on anything external;
// a report takes the table's lock, which no call holds across anything but map and field updates.
type PendingTable struct {
	grace, unawaited int64
	maxKeys          int
	now              func() int64

	// latest maps a key to its newest entry neither removed nor expired, for the observers to find without the lock.
	latest sync.Map // uint64 → *PendingEntry

	mu sync.Mutex
	// Guarded by mu.
	seen   map[uint64]struct{} // every key ever registered, up to maxKeys
	live   map[*PendingEntry]struct{}
	capped bool

	ignored   [NumIgnoredReasons]atomic.Uint64
	ambiguous atomic.Uint64
}

// PendingEntry is one registration and the token that reports its fate.
type PendingEntry struct {
	t           *PendingTable
	key         uint64
	SessionID   uint16
	SystemBytes uint32
	Channel     string

	ambiguous atomic.Bool

	// Guarded by t.mu.
	state    PendingState
	progress progress
	awaited  bool
	outcome  Outcome
	expires  int64
}

// NewPendingTable builds an empty table.
func NewPendingTable(p PendingParams) *PendingTable {
	now := p.Now
	if now == nil {
		now = func() int64 { return Mono(time.Now()) }
	}

	return &PendingTable{
		grace:     int64(p.Grace),
		unawaited: int64(p.Unawaited),
		maxKeys:   p.MaxKeys,
		now:       now,
		seen:      make(map[uint64]struct{}),
		live:      make(map[*PendingEntry]struct{}),
	}
}

// Register records a forwarded W-bit primary before it is sent and returns its entry, whose methods are its token.
// A key registered before in this process marks the new entry, and the key's newest entry still kept, ambiguous.
func (t *PendingTable) Register(sessionID uint16, systemBytes uint32, channel string) *PendingEntry {
	k := key(sessionID, systemBytes)
	e := &PendingEntry{t: t, key: k, SessionID: sessionID, SystemBytes: systemBytes, Channel: channel}
	t.mu.Lock()
	defer t.mu.Unlock()
	e.expires = t.now() + t.unawaited
	reused := false
	if _, ok := t.seen[k]; ok {
		reused = true
	} else if t.maxKeys == 0 || len(t.seen) < t.maxKeys {
		t.seen[k] = struct{}{}
	} else {
		t.capped = true
	}
	if old, ok := t.latest.Load(k); ok {
		reused = true
		if oe, ok := old.(*PendingEntry); ok {
			t.markAmbiguous(oe)
		}
	}
	if reused {
		t.markAmbiguous(e)
	}
	t.live[e] = struct{}{}
	t.latest.Store(k, e)

	return e
}

// Lookup returns the newest entry of a key that is neither removed nor expired, or nil; it takes no lock.
func (t *PendingTable) Lookup(sessionID uint16, systemBytes uint32) *PendingEntry {
	v, _ := t.latest.Load(key(sessionID, systemBytes))
	e, _ := v.(*PendingEntry)

	return e
}

// Sweep expires every entry past its expiry.
func (t *PendingTable) Sweep() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	for e := range t.live {
		if now >= e.expires {
			t.dropLocked(e, PendingExpired)
		}
	}
}

// Counters returns a snapshot of the table's counts.
func (t *PendingTable) Counters() PendingCounters {
	var c PendingCounters
	t.mu.Lock()
	c.Live, c.Capped = len(t.live), t.capped
	t.mu.Unlock()
	for i := range t.ignored {
		c.Ignored[i] = t.ignored[i].Load()
	}
	c.Ambiguous = t.ambiguous.Load()

	return c
}

// Sent reports that the forwarding call returned without error.
func (e *PendingEntry) Sent() { e.reportProgress(false) }

// SendFailed reports that the forwarding call returned an error; it removes a live entry.
func (e *PendingEntry) SendFailed() { e.reportProgress(true) }

// SubmissionFailed reports that the request never reached the forwarding call; it removes a live entry.
func (e *PendingEntry) SubmissionFailed() { e.reportProgress(true) }

// Awaiting reports that the waiter opened the wait with timeout; the entry then expires at the wait's end plus the grace.
func (e *PendingEntry) Awaiting(timeout time.Duration) {
	t := e.t
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if !e.liveLocked(now) {
		return
	}
	if e.awaited {
		t.ignored[IgnoredDuplicateAwaiting].Add(1)
		return
	}
	e.awaited = true
	e.expires = now + int64(timeout) + t.grace
}

// Replied reports a reply.
func (e *PendingEntry) Replied() { e.reportOutcome(OutcomeReplied) }

// TimedOut reports the waiter's timeout, with the effective timeout of that wait.
func (e *PendingEntry) TimedOut(_ time.Duration) { e.reportOutcome(OutcomeTimedOut) }

// Cancelled reports a wait that ended any other way.
func (e *PendingEntry) Cancelled() { e.reportOutcome(OutcomeCancelled) }

// State returns the entry's state.
func (e *PendingEntry) State() PendingState {
	e.t.mu.Lock()
	defer e.t.mu.Unlock()

	return e.state
}

// Outcome returns the entry's terminal outcome, OutcomeNone before one.
func (e *PendingEntry) Outcome() Outcome {
	e.t.mu.Lock()
	defer e.t.mu.Unlock()

	return e.outcome
}

// Ambiguous reports whether the entry's key was registered more than once; an ambiguous entry never thins and emits no T3.
func (e *PendingEntry) Ambiguous() bool { return e.ambiguous.Load() }

func key(sessionID uint16, systemBytes uint32) uint64 {
	return uint64(sessionID)<<32 | uint64(systemBytes)
}

func (e *PendingEntry) reportProgress(failed bool) {
	t := e.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if !e.liveLocked(t.now()) {
		return
	}
	if e.progress != progressNone {
		t.ignored[IgnoredDuplicateProgress].Add(1)
		return
	}
	if !failed {
		e.progress = progressSent
		return
	}
	e.progress = progressFailed
	t.dropLocked(e, PendingRemoved)
}

func (e *PendingEntry) reportOutcome(o Outcome) {
	t := e.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if !e.liveLocked(t.now()) {
		return
	}
	e.state, e.outcome = PendingCompleted, o
}

// liveLocked reports whether e still takes a report, expiring it first if its time has come;
// otherwise it counts the report as ignored.
func (e *PendingEntry) liveLocked(now int64) bool {
	t := e.t
	if (e.state == PendingLive || e.state == PendingCompleted) && now >= e.expires {
		t.dropLocked(e, PendingExpired)
	}
	switch e.state {
	case PendingLive:
		return true
	case PendingCompleted:
		t.ignored[IgnoredSealed].Add(1)
	case PendingRemoved:
		t.ignored[IgnoredRemoved].Add(1)
	case PendingExpired:
		t.ignored[IgnoredExpired].Add(1)
	default:
		panic("core: unknown pending state")
	}

	return false
}

func (t *PendingTable) dropLocked(e *PendingEntry, s PendingState) {
	e.state = s
	delete(t.live, e)
	t.latest.CompareAndDelete(e.key, e)
}

func (t *PendingTable) markAmbiguous(e *PendingEntry) {
	if !e.ambiguous.Swap(true) {
		t.ambiguous.Add(1)
	}
}
