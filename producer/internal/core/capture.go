package core

import (
	"sync"
	"sync/atomic"

	"github.com/arloliu/go-secs/tracepack"
)

// MaxSockets is how many sockets one capture numbers; the next one faults the capture.
const MaxSockets = 1024

// LastOrdinarySeq is the last seq an observation takes (G5-231);
// the seqs above it are left for the clock step and stop of a rotation, and 2^63-1 is the consumer's.
const LastOrdinarySeq = 1<<63 - 4

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

	lifecycle *atomic.Uint64
	tolerance int64

	mu sync.Mutex
	// Guarded by mu.
	nextSeq      uint64
	anchorWall   int64
	anchorMono   int64
	lastMono     int64
	closed       bool
	faulted      bool
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
		nextSeq:      1,
		anchorWall:   p.OriginWall,
		anchorMono:   p.OriginMono,
		lastMono:     p.OriginMono,
		reservations: make([]reservation, 1, max(p.ReservedCap, 1)),
	}
	c.reservations[0] = reservation{seq: 0, mono: p.OriginMono}

	return c
}

// Allocate gives one observation its seq, epoch and clock decision under the capture's lock (steps 1–4 of the callback).
// wall and mono are the observation's clock reading, in nanoseconds;
// socket is go-secs's socket identity, 0 for none.
func (c *Capture) Allocate(wall, mono int64, socket uint64) (Allocation, Status) {
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

	if socket != 0 {
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

	// shortcut: the seq space ends in a fault, not a rotation; it lasts 2^63 records, so no capture reaches it.
	if c.nextSeq > LastOrdinarySeq {
		c.faultLocked()
		return a, Faulted
	}
	a.Seq = c.nextSeq
	c.nextSeq++
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

// Faulted reports whether the capture is in the fault state.
func (c *Capture) Faulted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.faulted
}

// faultLocked faults an open capture.
// A capture a rotation already closed is left closed:
// callbacks that loaded it must retry on its successor, not drop, and its stop or its absence is already decided.
func (c *Capture) faultLocked() {
	if c.closed {
		return
	}
	c.closed, c.faulted = true, true
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

// driftLocked returns the reading's drift from the anchor ([SEM §4]): (wall − anchor wall) − (mono − anchor mono).
func (c *Capture) driftLocked(wall, mono int64) int64 {
	return (wall - c.anchorWall) - (mono - c.anchorMono)
}
