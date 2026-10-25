package core

import (
	"sync/atomic"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// Drop reasons the recorder counts (docs/specs/tracekeep/operations.md, records_dropped_total).
const (
	DropLaneFull DropReason = iota
	DropBusDown
	DropPublishFailed
	DropClaimFailed
	DropCaptureFault
	DropAbandoned
	NumDropReasons
)

// Test hooks: points where a barrier-controlled test holds a callback.
const (
	hookBeforeAllocate = iota
	hookLoaded         // the callback holds a capture pointer and has not taken its lock
	hookAfterUnlock
	hookFaultLoaded // a panic's fault holds a capture pointer and has not taken its lock
	// HookLocked holds a callback inside the capture's critical section, past its closed, faulted and generation checks.
	HookLocked
	// HookTryLockFailed holds a rotation once its first attempt at the old capture's lock failed.
	HookTryLockFailed
	numHooks
)

// faultSignals bounds the queued fault signals; the lifecycle goroutine re-checks the current capture anyway.
const faultSignals = 8

// DropReason names why a record was dropped.
type DropReason uint8

// EntryKind says what a lane entry is.
type EntryKind uint8

const (
	// EntryObserved is an observation: a frame or a transport event, its payload copied.
	EntryObserved EntryKind = iota + 1
	// EntryStart is a capture's start, which the publisher materializes from the capture.
	EntryStart
	// EntryStop is a capture's stop.
	EntryStop
	// EntryClockStep is a clock step; Drift holds the drift that caused it.
	EntryClockStep
)

// Event is a transport event as plain fields:
// the publisher encodes it with tracepack, so the callback that reports it allocates nothing for it.
// Kind decides which other fields the encoding carries.
type Event struct {
	Kind tracepack.Event
	// Prev and Cur are a state transition's states.
	Prev, Cur tracepack.State
	// Cause and CauseRaw are a state transition's or socket close's cause; CauseRaw is empty when there is none.
	Cause    tracepack.Cause
	CauseRaw string
	// Timer is the timer a timer expiry names, or the one that drove a state transition.
	Timer tracepack.Timer
	// Role is a socket connect's or accept's role.
	Role tracepack.SocketRole
	// SessionID, Stream, Function and SystemBytes name the primary a timer expiry ended the wait of.
	SessionID        uint16
	Stream, Function uint8
	SystemBytes      uint32
	// Timeout is the effective timeout of a forwarded request's T3, which the publisher writes into detail;
	// zero for go-secs's own T3.
	Timeout time.Duration
}

// Observation is one go-secs event as the observers hand it to Observe.
type Observation struct {
	// Wall and Mono are the event's clock reading in nanoseconds.
	Wall, Mono int64
	// Socket is go-secs's socket identity; 0 for none.
	Socket uint64
	// Kind, Dir and Fidelity are the record's.
	Kind     tracepack.Kind
	Dir      tracepack.Dir
	Fidelity tracepack.Fidelity
	// Payload is the frame, borrowed for the call only; a transport event has none.
	Payload []byte
	// Event is the transport event of an observation of kind transport-event.
	Event Event
	// Mapped requires Socket to be in the current capture's socket map already, as a forwarded request's T3 does:
	// a socket the capture never saw is not numbered, and the observation is dropped as unattributed.
	Mapped bool
}

// Entry is one record in a lane, waiting for the publisher.
// Every entry names its capture, so a queued record never takes another capture's identity.
type Entry struct {
	Cap      *Capture
	Kind     EntryKind
	Seq      uint64
	Wall     int64
	Mono     int64
	Epoch    uint32
	RecKind  tracepack.Kind
	Dir      tracepack.Dir
	Fidelity tracepack.Fidelity
	Quality  tracepack.Quality
	Drift    int64
	Payload  []byte
	Event    Event
}

// Counters is a snapshot of what the observers counted; the publisher reports it to Metrics.
type Counters struct {
	Observed     [256][4]uint64 // by kind and dir
	Dropped      [NumDropReasons]uint64
	DroppedBytes uint64
	AfterStop    uint64
	Panics       uint64
}

// RecorderParams sizes the lanes.
type RecorderParams struct {
	// ReservedCap bounds the reserved lane by count.
	ReservedCap int
	// OrdinaryRecords and OrdinaryBytes bound the ordinary lane.
	OrdinaryRecords int
	OrdinaryBytes   int64
	// Pending configures the pending-request table.
	Pending PendingParams
}

// Recorder holds the current capture and the two lanes the observer callbacks feed
// (docs/specs/tracekeep/producer.md §4.2 *Observer callback*, *Lanes*).
type Recorder struct {
	cur atomic.Pointer[Capture]

	// Pending is the send path's pending-request table (producer.md §4.6).
	Pending *PendingTable

	// The reserved lane holds start, stop and clock-step records and never drops: a full one faults the capture.
	reserved chan Entry
	// The ordinary lane is bounded by count and bytes, reserved before the copy, and drops the newest arrival.
	ordinary  chan Entry
	ordCount  atomic.Int64
	ordBytes  atomic.Int64
	maxCount  int64
	maxBytes  int64
	faults    chan *Capture
	rotations chan *Capture
	busDown   atomic.Bool
	// resCount counts reserved records from their push until the publisher accepts or drops them,
	// so the reserved lane's bound holds for what the publisher holds too.
	resCount  atomic.Int64
	resCap    int64
	observed  [256][4]atomic.Uint64
	dropped   [NumDropReasons]atomic.Uint64
	dropBytes atomic.Uint64
	afterStop atomic.Uint64
	panics    atomic.Uint64

	hooks [numHooks]func()
}

// NewRecorder builds a recorder with no current capture.
func NewRecorder(p RecorderParams) *Recorder {
	r := &Recorder{
		reserved:  make(chan Entry, p.ReservedCap),
		ordinary:  make(chan Entry, p.OrdinaryRecords),
		maxCount:  int64(p.OrdinaryRecords),
		maxBytes:  p.OrdinaryBytes,
		faults:    make(chan *Capture, faultSignals),
		rotations: make(chan *Capture, faultSignals),
		resCap:    int64(p.ReservedCap),
	}
	r.Pending = NewPendingTable(p.Pending)
	r.Pending.emit = r.forwardedT3

	return r
}

// Observe records one observation: the observer callback of producer.md §4.2, steps 1–8.
// It never blocks on anything but another callback inside a capture's critical section.
func (r *Recorder) Observe(o *Observation) {
	var c *Capture
	allocated := false
	// owed is what Allocate counted as queued that this callback has not sent yet.
	var owed int64
	defer func() {
		if owed > 0 {
			c.queued.Add(-owed)
		}
		if p := recover(); p != nil {
			r.panics.Add(1)
			// Before allocation the loss has no seq to show it, so the fault window of the current capture bounds it.
			if !allocated {
				r.faultFrom(c)
			}
		}
	}()
	r.observed[o.Kind][o.Dir&3].Add(1)
	r.hook(hookBeforeAllocate)

	a, ok := r.allocate(o, &c)
	if !ok {
		return
	}
	allocated = true
	owed = 1
	if a.Step {
		owed = 2
	}
	if a.RotateDue {
		select {
		case r.rotations <- c:
		default:
		}
	}
	r.hook(hookAfterUnlock)

	var q tracepack.Quality
	if a.OrderingUncertain {
		q |= tracepack.QualityOrderingUncertain
	}
	if a.Step {
		// Queued whatever becomes of the ordinary record, and retired only once queued,
		// so no later record is published before it.
		step := Entry{Cap: c, Kind: EntryClockStep, Seq: a.StepSeq, Wall: o.Wall, Mono: o.Mono, Drift: a.StepDrift}
		if !r.pushCounted(step) {
			c.Fault()
			r.SignalFault(c)
			r.drop(DropCaptureFault, int64(len(o.Payload)))

			return
		}
		owed--
		c.Retire(a.StepSeq)
	}

	n := int64(len(o.Payload))
	if !r.reserveOrdinary(n) {
		r.drop(r.fullReason(), n)
		return
	}
	e := Entry{
		Cap: c, Kind: EntryObserved, Seq: a.Seq, Wall: o.Wall, Mono: o.Mono, Epoch: a.Epoch,
		RecKind: o.Kind, Dir: o.Dir, Fidelity: o.Fidelity, Quality: q,
		Event: o.Event,
	}
	if n > 0 {
		e.Payload = append(make([]byte, 0, n), o.Payload...)
	}
	select {
	case r.ordinary <- e:
		owed--
	default:
		r.returnBudget(n)
		r.drop(r.fullReason(), n)
	}
}

// Recover, deferred at an observer's entry, contains a panic raised before the observer reached Observe:
// it is counted and faults the current capture, since a loss with no seq is otherwise invisible.
func (r *Recorder) Recover() {
	if p := recover(); p != nil { //nolint:revive // Recover is only ever deferred, where recover stops the panic
		r.panics.Add(1)
		r.faultFrom(nil)
	}
}

// forwardedT3 records a forwarded request's T3 in the current capture, at the epoch its socket map holds for the request's socket
// (producer.md §4.6 *Send-path token protocol*); a socket the capture never saw is counted as unattributed instead.
func (r *Recorder) forwardedT3(e *PendingEntry, effective time.Duration) {
	wall, mono := Now()
	hdr := e.hdr.Load()
	r.Observe(&Observation{
		Wall: wall, Mono: mono, Socket: e.bound.Load(), Mapped: true,
		Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable,
		Event: Event{
			Kind: tracepack.EventTimerExpiry, Timer: tracepack.TimerT3,
			SessionID: e.SessionID, Stream: uint8(hdr >> 8), Function: uint8(hdr), SystemBytes: e.SystemBytes, //nolint:gosec // hdr holds two bytes
			Timeout: effective,
		},
	})
}

// SetBusDown records whether the publisher finds the bus unreachable, which names why a full lane drops.
func (r *Recorder) SetBusDown(down bool) { r.busDown.Store(down) }

// OrdinaryDepth returns the ordinary lane's reserved count and bytes, the records the publisher still holds included.
func (r *Recorder) OrdinaryDepth() (count, bytes int64) { return r.ordCount.Load(), r.ordBytes.Load() }

// PushReserved queues e in the reserved lane without blocking;
// false when the lane's bound, which counts the reserved records the publisher holds, is reached.
func (r *Recorder) PushReserved(e Entry) bool {
	if e.Cap != nil {
		e.Cap.queued.Add(1)
	}
	if !r.pushCounted(e) {
		if e.Cap != nil {
			e.Cap.queued.Add(-1)
		}

		return false
	}

	return true
}

// pushCounted is PushReserved for an entry whose capture already counts it as queued.
func (r *Recorder) pushCounted(e Entry) bool {
	if !r.takeReserved(1) {
		return false
	}
	if !r.sendReserved(e) {
		r.ReleaseReserved()
		return false
	}

	return true
}

// ReleaseReserved returns one reserved record's place in the lane's bound, once the publisher accepted or dropped it.
func (r *Recorder) ReleaseReserved() { r.resCount.Add(-1) }

// Rotations carries captures whose seq neared its end, for the lifecycle goroutine to rotate (G5-231).
func (r *Recorder) Rotations() <-chan *Capture { return r.rotations }

// Reserved is the reserved lane, which only the publisher reads.
func (r *Recorder) Reserved() <-chan Entry { return r.reserved }

// Ordinary is the ordinary lane, which only the publisher reads.
func (r *Recorder) Ordinary() <-chan Entry { return r.ordinary }

// ReleaseOrdinary returns a dequeued ordinary entry's budget.
func (r *Recorder) ReleaseOrdinary(e *Entry) { r.returnBudget(int64(len(e.Payload))) }

// Faults carries the captures the observers put in the fault state, for the lifecycle goroutine.
func (r *Recorder) Faults() <-chan *Capture { return r.faults }

// Counters returns a snapshot of the observers' counts.
func (r *Recorder) Counters() Counters {
	var s Counters
	for k := range r.observed {
		for d := range r.observed[k] {
			s.Observed[k][d] = r.observed[k][d].Load()
		}
	}
	for i := range r.dropped {
		s.Dropped[i] = r.dropped[i].Load()
	}
	s.DroppedBytes = r.dropBytes.Load()
	s.AfterStop = r.afterStop.Load()
	s.Panics = r.panics.Load()

	return s
}

// allocate runs steps 1–4: it finds an open capture and allocates in it, or counts why it could not.
// A rotation installs the new capture before it closes the old, so the loop ends within as many rounds as rotations in progress.
func (r *Recorder) allocate(o *Observation, cp **Capture) (Allocation, bool) {
	for {
		c := r.cur.Load()
		*cp = c
		if c == nil {
			r.afterStop.Add(1)
			return Allocation{}, false
		}
		r.hook(hookLoaded)
		a, st := c.allocateIn(o.Wall, o.Mono, o.Socket, o.Mapped, r.hooks[HookLocked])
		switch st {
		case Allocated:
			return a, true
		case Closed:
			continue
		case Faulted:
			r.SignalFault(c)
			r.drop(DropCaptureFault, int64(len(o.Payload)))

			return a, false
		case Abandoned:
			r.drop(DropAbandoned, int64(len(o.Payload)))
			return a, false
		case Unmapped:
			// Counted observed above, its loss is counted here and not as a dropped record: it never had a seq.
			r.Pending.timeoutsDropped[TimeoutUnattributed].Add(1)
			return a, false
		default:
			panic("core: unknown allocation status")
		}
	}
}

func (r *Recorder) reserveOrdinary(n int64) bool {
	if r.ordCount.Add(1) > r.maxCount {
		r.ordCount.Add(-1)
		return false
	}
	if r.ordBytes.Add(n) > r.maxBytes {
		r.ordBytes.Add(-n)
		r.ordCount.Add(-1)

		return false
	}

	return true
}

func (r *Recorder) takeReserved(n int64) bool {
	if r.resCount.Add(n) > r.resCap {
		r.resCount.Add(-n)
		return false
	}

	return true
}

// sendReserved puts e in the channel; the bound keeps the channel from filling, so this fails only on a bug.
func (r *Recorder) sendReserved(e Entry) bool {
	select {
	case r.reserved <- e:
		return true
	default:
		return false
	}
}

func (r *Recorder) fullReason() DropReason {
	if r.busDown.Load() {
		return DropBusDown
	}

	return DropLaneFull
}

func (r *Recorder) returnBudget(n int64) {
	r.ordBytes.Add(-n)
	r.ordCount.Add(-1)
}

func (r *Recorder) drop(reason DropReason, n int64) {
	r.dropped[reason].Add(1)
	r.dropBytes.Add(uint64(n)) //nolint:gosec // a payload length is never negative
}

// faultFrom faults c, or the current capture when c is nil, for a loss that has no seq to show it.
// A capture a rotation closed meanwhile is passed over for its successor, as allocate does,
// so the loss always falls in a fault window; the loop ends within as many rounds as rotations in progress.
// A capture already faulted ends it: the loss is inside that capture's fault window.
func (r *Recorder) faultFrom(c *Capture) {
	if c == nil {
		c = r.cur.Load()
	}
	for c != nil {
		r.hook(hookFaultLoaded)
		if c.FaultOpen() {
			r.SignalFault(c)
			return
		}
		c = r.cur.Load()
	}
}

// SignalFault tells the lifecycle goroutine that c faulted, without blocking.
func (r *Recorder) SignalFault(c *Capture) {
	select {
	case r.faults <- c:
	default:
	}
}

// SetHook sets the function a test holds a goroutine in at hook id, before any callback runs;
// the module's tests outside this package set HookLocked and HookTryLockFailed here.
func (r *Recorder) SetHook(id int, f func()) { r.hooks[id] = f }

func (r *Recorder) hook(id int) {
	if h := r.hooks[id]; h != nil {
		h()
	}
}
