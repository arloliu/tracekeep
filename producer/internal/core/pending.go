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

// TimeoutDropReason names why a forwarded request's timeout recorded no T3 (operations.md, pending_timeouts_dropped_total).
type TimeoutDropReason uint8

const (
	// TimeoutUnbound is a request whose outbound frame was never observed.
	TimeoutUnbound TimeoutDropReason = iota
	// TimeoutAmbiguous is a request whose key was reused, or whose socket changed between registration and send.
	TimeoutAmbiguous
	// TimeoutUnattributed is a request whose socket the capture current at the timeout never saw.
	TimeoutUnattributed
	// NumTimeoutDropReasons counts the reasons.
	NumTimeoutDropReasons
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
	// TimeoutsDropped counts the forwarded timeouts that recorded no T3, by reason.
	TimeoutsDropped [NumTimeoutDropReasons]uint64
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

	// socket is the connection's newest socket, which a registration compares its send's socket against.
	socket atomic.Uint64
	// emit records a forwarded request's T3; the recorder sets it.
	emit func(e *PendingEntry, effective time.Duration)

	ignored         [NumIgnoredReasons]atomic.Uint64
	ambiguous       atomic.Uint64
	timeoutsDropped [NumTimeoutDropReasons]atomic.Uint64
}

// PendingEntry is one registration and the token that reports its fate.
type PendingEntry struct {
	t           *PendingTable
	key         uint64
	SessionID   uint16
	SystemBytes uint32
	Channel     string

	ambiguous atomic.Bool
	// regSocket is the connection's newest socket at registration;
	// bound is the socket the outbound frame went out on, 0 until it is observed, and hdr that frame's stream and function.
	regSocket uint64
	bound     atomic.Uint64
	hdr       atomic.Uint32

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
	e := &PendingEntry{t: t, key: k, SessionID: sessionID, SystemBytes: systemBytes, Channel: channel, regSocket: t.socket.Load()}
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

// SetSocket records the connection's newest socket; the socket observer calls it at every connect or accept.
func (t *PendingTable) SetSocket(socket uint64) { t.socket.Store(socket) }

// Bind binds the newest entry of a key to the socket its outbound frame went out on, from the wire observer;
// it takes no lock and allocates nothing.
// Only the first frame binds, and a socket other than the newest at registration marks the entry ambiguous:
// the connection was replaced between registration and send.
func (t *PendingTable) Bind(sessionID uint16, systemBytes uint32, stream, function uint8, socket uint64) {
	e := t.Lookup(sessionID, systemBytes)
	if e == nil || socket == 0 {
		return
	}
	e.hdr.Store(uint32(stream)<<8 | uint32(function))
	if e.bound.CompareAndSwap(0, socket) && socket != e.regSocket {
		t.markAmbiguous(e)
	}
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
	for i := range t.timeoutsDropped {
		c.TimeoutsDropped[i] = t.timeoutsDropped[i].Load()
	}

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
// As the first terminal outcome of an unambiguous entry bound to a socket it records T3 in the current capture;
// otherwise it is counted and records nothing.
func (e *PendingEntry) TimedOut(effective time.Duration) {
	if !e.reportOutcome(OutcomeTimedOut) {
		return
	}
	t := e.t
	switch {
	case e.bound.Load() == 0:
		t.timeoutsDropped[TimeoutUnbound].Add(1)
	case e.ambiguous.Load():
		t.timeoutsDropped[TimeoutAmbiguous].Add(1)
	case t.emit != nil:
		t.emit(e, effective)
	default:
	}
}

// Cancelled reports a wait that ended any other way.
func (e *PendingEntry) Cancelled() { e.reportOutcome(OutcomeCancelled) }

// Socket returns the socket the entry's outbound frame went out on, 0 before it is observed.
func (e *PendingEntry) Socket() uint64 { return e.bound.Load() }

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

// reportOutcome seals a live entry with o and reports whether it did.
func (e *PendingEntry) reportOutcome(o Outcome) bool {
	t := e.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if !e.liveLocked(t.now()) {
		return false
	}
	e.state, e.outcome = PendingCompleted, o

	return true
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
