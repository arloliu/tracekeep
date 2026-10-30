package core

import (
	"cmp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// MaxSockets is how many sockets one capture numbers; the next one faults the capture.
const MaxSockets = 1024

// LastOrdinarySeq is the last seq an observation takes (G5-231);
// the seqs above it are left for the clock step and stop of a rotation, and 2^63-1 is the consumer's.
const LastOrdinarySeq = 1<<63 - 4

// DefaultRotateAt leaves the rotation a margin of seqs to take place in before LastOrdinarySeq.
const DefaultRotateAt = LastOrdinarySeq - 1<<20

// Status is the outcome of Allocate.
type Status uint8

const (
	// Allocated means the observation took a seq in the capture.
	Allocated Status = iota
	// Closed means a rotation closed the capture: reload the current capture and retry.
	Closed
	// Faulted means the capture is in the fault state, or Allocate just put it there: count the drop and return.
	Faulted
	// Abandoned means the lifecycle generation advanced past the capture: return without allocating.
	Abandoned
	// Unmapped means the observation required a socket the capture never saw: return without allocating.
	Unmapped
)

// Allocation is what Allocate decided for one observation.
type Allocation struct {
	// Seq is the observation's seq.
	Seq uint64
	// Epoch numbers the observation's socket within the capture in order of first sight; 0 for no socket.
	Epoch uint32
	// Step reports that the observation's clock reading drifted past ε from the anchor:
	// StepSeq is the reserved seq of the clock-step record that precedes the observation,
	// which the caller pushes into the reserved lane and then retires.
	// StepDrift is the drift that caused the step, in nanoseconds.
	Step      bool
	StepSeq   uint64
	StepDrift int64
	// OrderingUncertain reports an observation older than the one allocated before it in this capture.
	OrderingUncertain bool
	// RotateDue reports, once per capture, that its seq passed the point where the producer rotates (G5-231).
	RotateDue bool
}

// CaptureParams configures NewCapture.
type CaptureParams struct {
	// ID is the capture_id.
	ID tracepack.UUID
	// Generation is the lifecycle generation the capture is created in.
	Generation uint64
	// Lifecycle is the producer's current lifecycle generation, which only abandonment advances.
	Lifecycle *atomic.Uint64
	// Tolerance is ε in nanoseconds.
	Tolerance int64
	// ReservedCap bounds the reservation set: the reserved lane's size.
	ReservedCap int
	// OriginWall and OriginMono are the clock reading of the capture's start, its first anchor.
	OriginWall, OriginMono int64
	// RotateAt is the seq from which the capture asks to be rotated; zero takes DefaultRotateAt.
	RotateAt uint64
	// Sampling is the capture's sampling rule; nil, it thins nothing.
	Sampling *Sampling
}

// Capture is one capture: its identity and the state the observer callbacks allocate under its lock
// (docs/specs/tracekeep/producer.md §4.2 *Capture object*).
//
// The critical section holds no I/O, no allocation, no logging and no call into go-secs:
// every structure it touches is preallocated here, and exhausting one faults the capture.
type Capture struct {
	// ID is the capture_id.
	ID tracepack.UUID
	// Generation is the lifecycle generation the capture was created in.
	Generation uint64
	// OriginWall and OriginMono are the clock reading of the capture's start; a record's mono_ns is measured from OriginMono.
	OriginWall, OriginMono int64
	// Descriptor is the start's Tpk-Capture-Descriptor value and Subject the capture's subject,
	// both set before the capture is rotated in.
	Descriptor string
	Subject    string

	start atomic.Uint32
	// terminated is set once End's drain of the capture finished: nothing of it is published afterwards.
	terminated atomic.Bool
	done       chan struct{}
	doneOnce   sync.Once
	// endBy is End's deadline for publishing the capture, kept with its monotonic reading; nil until End stamps it.
	endBy atomic.Pointer[time.Time]
	// queued counts the capture's records from their allocation, under the lock, until the publisher takes them in,
	// so a capture with a record a callback has not sent yet never looks finished.
	queued atomic.Int64
	// dropped counts what the publisher gave up of the capture when it terminated it.
	dropped atomic.Uint64
	// settled is set when the publisher, the capture no longer current, had nothing of it left.
	settled atomic.Bool

	lifecycle *atomic.Uint64
	tolerance int64
	rotateAt  uint64
	// sampler is nil when the capture thins nothing.
	sampler *sampler

	mu sync.Mutex
	// Guarded by mu.
	nextSeq    uint64
	anchorWall int64
	anchorMono int64
	lastMono   int64
	closed     bool
	faulted    bool
	// rotateAsked records that the capture asked for its seq-limit rotation.
	rotateAsked bool
	// faultedFlag mirrors faulted for readers that must not wait on the lock, such as the lifecycle goroutine.
	faultedFlag  atomic.Bool
	sockets      [MaxSockets]uint64
	nSockets     int
	reservations []reservation
}

// reservation is a seq allocated for the reserved lane and not yet queued, with the monotonic reading it was taken at.
type reservation struct {
	seq  uint64
	mono int64
}

// NewCapture builds a capture whose start takes seq 0, reserved:
// the caller pushes the start into the reserved lane and then retires seq 0.
func NewCapture(p CaptureParams) *Capture {
	c := &Capture{
		ID:           p.ID,
		Generation:   p.Generation,
		OriginWall:   p.OriginWall,
		OriginMono:   p.OriginMono,
		lifecycle:    p.Lifecycle,
		tolerance:    p.Tolerance,
		rotateAt:     cmp.Or(p.RotateAt, DefaultRotateAt),
		nextSeq:      1,
		anchorWall:   p.OriginWall,
		anchorMono:   p.OriginMono,
		lastMono:     p.OriginMono,
		reservations: make([]reservation, 1, max(p.ReservedCap, 1)),
		done:         make(chan struct{}),
	}
	c.reservations[0] = reservation{seq: 0, mono: p.OriginMono}
	if p.Sampling != nil {
		rule := *p.Sampling
		rule.KeepEveryN = max(rule.KeepEveryN, 1)
		c.sampler = &sampler{rule: rule}
	}

	return c
}

// Allocate gives one observation its seq, epoch and clock decision under the capture's lock (steps 1–4 of the callback).
// wall and mono are the observation's clock reading, in nanoseconds;
// socket is go-secs's socket identity, 0 for none.
func (c *Capture) Allocate(wall, mono int64, socket uint64) (Allocation, Status) {
	return c.allocateIn(wall, mono, socket, false, nil)
}

// allocateIn is Allocate; with mapped, a socket missing from the socket map yields Unmapped instead of a new epoch.
// locked, when set, is a test's hook inside the critical section.
func (c *Capture) allocateIn(wall, mono int64, socket uint64, mapped bool, locked func()) (Allocation, Status) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var a Allocation
	if c.faulted {
		return a, Faulted
	}
	if c.closed {
		return a, Closed
	}
	if c.lifecycle.Load() != c.Generation {
		return a, Abandoned
	}
	if locked != nil {
		locked()
	}

	if socket != 0 {
		if mapped && !c.knownLocked(socket) {
			return a, Unmapped
		}
		epoch, ok := c.epochLocked(socket)
		if !ok {
			c.faultLocked()
			return a, Faulted
		}
		a.Epoch = epoch
	}

	if drift := c.driftLocked(wall, mono); drift > c.tolerance || drift < -c.tolerance {
		if len(c.reservations) == cap(c.reservations) || c.nextSeq > LastOrdinarySeq {
			c.faultLocked()
			return a, Faulted
		}
		a.Step, a.StepSeq, a.StepDrift = true, c.nextSeq, drift
		c.reservations = append(c.reservations, reservation{seq: c.nextSeq, mono: mono})
		c.nextSeq++
		c.anchorWall, c.anchorMono = wall, mono
	}

	// Past RotateAt the capture asks once for a rotation; only if that never comes does the seq space end in a fault.
	if c.nextSeq > LastOrdinarySeq {
		c.faultLocked()
		return a, Faulted
	}
	a.Seq = c.nextSeq
	c.nextSeq++
	c.queued.Add(1)
	if a.Step {
		c.queued.Add(1)
	}
	if a.Seq >= c.rotateAt && !c.rotateAsked {
		c.rotateAsked, a.RotateDue = true, true
	}
	a.OrderingUncertain = mono < c.lastMono
	c.lastMono = max(c.lastMono, mono)

	return a, Allocated
}

// Retire removes seq from the reservation set once its record is in the reserved lane.
func (c *Capture) Retire(seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, r := range c.reservations {
		if r.seq == seq {
			last := len(c.reservations) - 1
			c.reservations[i] = c.reservations[last]
			c.reservations = c.reservations[:last]

			return
		}
	}
}

// MinReserved returns the lowest reserved seq, which the dependency barrier keeps everything above unpublished.
func (c *Capture) MinReserved() (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reservations) == 0 {
		return 0, false
	}
	low := c.reservations[0].seq
	for _, r := range c.reservations[1:] {
		low = min(low, r.seq)
	}

	return low, true
}

// OldestReservation returns the monotonic reading of the oldest unqueued reservation,
// which faults the capture once it is older than the reservation timeout.
func (c *Capture) OldestReservation() (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reservations) == 0 {
		return 0, false
	}
	oldest := c.reservations[0].mono
	for _, r := range c.reservations[1:] {
		oldest = min(oldest, r.mono)
	}

	return oldest, true
}

// Fault puts the capture in the fault state: closed and faulted, so no callback allocates in it again.
func (c *Capture) Fault() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.faultLocked()
}

// FaultOpen faults the capture unless a rotation already closed it,
// which it reports as false: the caller then faults the successor the rotation installed.
func (c *Capture) FaultOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed && !c.faulted {
		return false
	}
	c.faultLocked()

	return true
}

// Terminate marks the capture finished for the publisher: a record of it queued later is dropped, never published.
// It closes Done.
func (c *Capture) Terminate() {
	c.terminated.Store(true)
	c.doneOnce.Do(func() { close(c.done) })
}

// Done is closed once the capture is terminated.
func (c *Capture) Done() <-chan struct{} { return c.done }

// SetEndBy stamps End's deadline for publishing the capture.
func (c *Capture) SetEndBy(t time.Time) { c.endBy.Store(&t) }

// EndBy returns End's deadline for the capture, false until End stamps it.
func (c *Capture) EndBy() (time.Time, bool) {
	if t := c.endBy.Load(); t != nil {
		return *t, true
	}

	return time.Time{}, false
}

// Settle records that the publisher finished with the capture; Settled reports it.
func (c *Capture) Settle() { c.settled.Store(true) }

// Settled reports whether Settle was called.
func (c *Capture) Settled() bool { return c.settled.Load() }

// Queued returns how many of the capture's records are in the lanes.
func (c *Capture) Queued() int64 { return c.queued.Load() }

// Dequeued records that the publisher took one of the capture's records in.
func (c *Capture) Dequeued() { c.queued.Add(-1) }

// AddDropped counts records of the capture the publisher gave up at its termination; Dropped reads the count.
func (c *Capture) AddDropped(n uint64) { c.dropped.Add(n) }

// Dropped returns what AddDropped counted.
func (c *Capture) Dropped() uint64 { return c.dropped.Load() }

// Terminated reports whether Terminate was called.
func (c *Capture) Terminated() bool { return c.terminated.Load() }

// Faulted reports whether the capture is in the fault state; it never waits on the capture's lock.
func (c *Capture) Faulted() bool { return c.faultedFlag.Load() }

// faultLocked faults an open capture.
// A capture a rotation already closed is left closed:
// callbacks that loaded it must retry on its successor, not drop, and its stop or its absence is already decided.
func (c *Capture) faultLocked() {
	if c.closed {
		return
	}
	c.closed, c.faulted = true, true
	c.faultedFlag.Store(true)
}

// epochLocked returns the epoch of socket, numbering a socket seen for the first time;
// false when the socket table is full.
func (c *Capture) epochLocked(socket uint64) (uint32, bool) {
	// Recent sockets are the likely ones, so the scan starts from the newest.
	for i := c.nSockets - 1; i >= 0; i-- {
		if c.sockets[i] == socket {
			return uint32(i + 1), true //nolint:gosec // i < MaxSockets
		}
	}
	if c.nSockets == MaxSockets {
		return 0, false
	}
	c.sockets[c.nSockets] = socket
	c.nSockets++

	return uint32(c.nSockets), true //nolint:gosec // nSockets ≤ MaxSockets
}

// knownLocked reports whether socket is in the capture's socket map.
func (c *Capture) knownLocked(socket uint64) bool {
	for i := c.nSockets - 1; i >= 0; i-- {
		if c.sockets[i] == socket {
			return true
		}
	}

	return false
}

// driftLocked returns the reading's drift from the anchor ([SEM §4]): (wall − anchor wall) − (mono − anchor mono).
func (c *Capture) driftLocked(wall, mono int64) int64 {
	return (wall - c.anchorWall) - (mono - c.anchorMono)
}
