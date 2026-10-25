package core

import (
	"sync/atomic"

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
	// Payload is the frame or the transport event's payload, borrowed for the call only.
	Payload []byte
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
}

// Recorder holds the current capture and the two lanes the observer callbacks feed
// (docs/specs/tracekeep/producer.md §4.2 *Observer callback*, *Lanes*).
type Recorder struct {
	cur atomic.Pointer[Capture]

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
	return &Recorder{
		reserved:  make(chan Entry, p.ReservedCap),
		ordinary:  make(chan Entry, p.OrdinaryRecords),
		maxCount:  int64(p.OrdinaryRecords),
		maxBytes:  p.OrdinaryBytes,
		faults:    make(chan *Capture, faultSignals),
		rotations: make(chan *Capture, faultSignals),
		resCap:    int64(p.ReservedCap),
	}
}

// Observe records one observation: the observer callback of producer.md §4.2, steps 1–8.
// It never blocks on anything but another callback inside a capture's critical section.
func (r *Recorder) Observe(o *Observation) {
	var c *Capture
	allocated := false
	defer func() {
		if p := recover(); p != nil {
			r.panics.Add(1)
			// Before allocation the loss has no seq to show it, so the fault window of the current capture bounds it.
			if !allocated && c == nil {
				c = r.cur.Load()
			}
			if !allocated && c != nil {
				c.Fault()
				r.SignalFault(c)
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
		if !r.PushReserved(step) {
			c.Fault()
			r.SignalFault(c)
			r.drop(DropCaptureFault, int64(len(o.Payload)))

			return
		}
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
		Payload: append(make([]byte, 0, n), o.Payload...),
	}
	select {
	case r.ordinary <- e:
	default:
		r.returnBudget(n)
		r.drop(r.fullReason(), n)
	}
}

// SetBusDown records whether the publisher finds the bus unreachable, which names why a full lane drops.
func (r *Recorder) SetBusDown(down bool) { r.busDown.Store(down) }

// OrdinaryDepth returns the ordinary lane's reserved count and bytes, the records the publisher still holds included.
func (r *Recorder) OrdinaryDepth() (count, bytes int64) { return r.ordCount.Load(), r.ordBytes.Load() }

// PushReserved queues e in the reserved lane without blocking;
// false when the lane's bound, which counts the reserved records the publisher holds, is reached.
func (r *Recorder) PushReserved(e Entry) bool {
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
		a, st := c.Allocate(o.Wall, o.Mono, o.Socket)
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

// SignalFault tells the lifecycle goroutine that c faulted, without blocking.
func (r *Recorder) SignalFault(c *Capture) {
	select {
	case r.faults <- c:
	default:
	}
}

func (r *Recorder) hook(id int) {
	if h := r.hooks[id]; h != nil {
		h()
	}
}
