package core

import (
	"sync/atomic"
	"time"
)

// StartState is the terminal state of a prepared capture's start, recorded on the capture, never inferred from the pointer.
type StartState uint32

const (
	// StartPending means the start is queued and its capture not yet installed: the publisher waits.
	StartPending StartState = iota
	// StartInstalled means the capture was installed: the publisher publishes the start.
	StartInstalled
	// StartDiscarded means the rotation was abandoned before its commit point: the publisher skips the start.
	StartDiscarded
)

// tryLockPause is the pause between attempts on a capture lock a callback holds.
const tryLockPause = 50 * time.Microsecond

// boundarySlots is what a rotation holds in the reserved lane's bound for the old capture's clock step and stop.
const boundarySlots = 2

// RotateResult reports what a rotation did.
type RotateResult struct {
	// Installed reports that the rotation passed its commit point.
	Installed bool
	// Abandoned reports that it gave up before the commit point and installed nothing.
	Abandoned bool
	// Old is the capture it replaced; nil when none was current.
	Old *Capture
	// StopQueued reports that the old capture's stop is in the reserved lane.
	StopQueued bool
}

// Current returns the current capture, nil when none is open.
func (r *Recorder) Current() *Capture { return r.cur.Load() }

// Rotate replaces the current capture with next, nil at a stop (docs/specs/tracekeep/producer.md §4.2 *Rotation*).
// It runs on the lifecycle goroutine under the lifecycle lock.
//
// Before anything is installed it queues next's start and takes two places in the reserved lane's bound
// for the old capture's clock step and stop, so a rotation that commits never loses a boundary;
// without room it abandons.
// The old capture's lock is taken by a TryLock loop that gives up at deadline or when abort reports true,
// and both are checked once more before the commit point, so an abandoned rotation installs nothing and discards next's start.
// With the old lock held it installs next (the commit point), closes the old capture
// and queues its clock step, when the anchor rule needs one, and its stop, which take its last seqs;
// nothing is acquired after the commit point.
// A faulted old capture gets neither: it stays faulted and the consumer closes it unclean.
// now reads the clock for the stop.
func (r *Recorder) Rotate(next *Capture, deadline time.Time, abort func() bool, now func() (wall, mono int64)) RotateResult {
	old := r.cur.Load()
	discard := func() RotateResult {
		if next != nil {
			next.start.Store(uint32(StartDiscarded))
		}

		return RotateResult{Abandoned: true, Old: old}
	}
	if next != nil {
		// Nothing else can hold next's lock before it is installed; seq 0 is its start's.
		if !r.PushReserved(Entry{Cap: next, Kind: EntryStart, Seq: 0, Wall: next.OriginWall, Mono: next.OriginMono}) {
			return discard()
		}
		next.Retire(0)
	}

	credit := int64(0)
	if old != nil {
		if !r.takeReserved(boundarySlots) {
			return discard()
		}
		credit = boundarySlots
		if !old.tryLockUntil(deadline, abort) {
			r.resCount.Add(-credit)
			return discard()
		}
	}
	if abort() || !time.Now().Before(deadline) {
		if old != nil {
			old.mu.Unlock()
		}
		r.resCount.Add(-credit)

		return discard()
	}

	if old != nil {
		// Counted as queued before the commit point, so the old capture never looks finished before its stop is sent.
		old.queued.Add(credit)
	}
	r.cur.Store(next)
	if next != nil {
		next.start.Store(uint32(StartInstalled))
	}
	res := RotateResult{Installed: true, Old: old}
	if old != nil {
		var used int64
		res.StopQueued, used = r.closeLocked(old, now)
		old.mu.Unlock()
		r.resCount.Add(used - credit)
		old.queued.Add(used - credit)
	}

	return res
}

// Abandon gives up the current capture without its lock or its stop, as End does past its deadline:
// the generation advances for good, so no callback allocates in the old capture again
// and the publisher drops what it still queued.
func (r *Recorder) Abandon(lifecycle *atomic.Uint64) {
	lifecycle.Add(1)
	r.cur.Store(nil)
}

// StartState returns the state of the capture's start.
func (c *Capture) StartState() StartState { return StartState(c.start.Load()) }

// Abandoned reports whether the lifecycle generation moved past the capture.
func (c *Capture) Abandoned() bool { return c.lifecycle.Load() != c.Generation }

// closeLocked closes old, whose lock the caller holds, and queues its clock step and stop
// into the two places Rotate took in the reserved lane's bound; it returns how many it used.
// The seqs are taken, queued and complete under the lock, so no one observes them reserved and they are not reserved.
func (r *Recorder) closeLocked(old *Capture, now func() (wall, mono int64)) (bool, int64) {
	if old.faulted {
		return false, 0
	}
	old.closed = true
	if old.lifecycle.Load() != old.Generation {
		return false, 0
	}
	var used int64
	wall, mono := now()
	if drift := old.driftLocked(wall, mono); drift > old.tolerance || drift < -old.tolerance {
		if r.sendReserved(Entry{Cap: old, Kind: EntryClockStep, Seq: old.nextSeq, Wall: wall, Mono: mono, Drift: drift}) {
			used++
		}
		old.nextSeq++
		old.anchorWall, old.anchorMono = wall, mono
	}
	stopped := r.sendReserved(Entry{Cap: old, Kind: EntryStop, Seq: old.nextSeq, Wall: wall, Mono: mono})
	old.nextSeq++
	if stopped {
		used++
	}

	return stopped, used
}

// tryLockUntil takes c's lock unless deadline passes or abort reports true first.
func (c *Capture) tryLockUntil(deadline time.Time, abort func() bool) bool {
	for {
		if c.mu.TryLock() {
			return true
		}
		if abort() || !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(tryLockPause)
	}
}
