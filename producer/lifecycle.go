package producer

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// defaultLifecycleTimeout bounds a lifecycle call whose context has no deadline.
const defaultLifecycleTimeout = 5 * time.Second

// faultRepeatWindow is how soon a fresh capture may fault again before rotation pauses for the backoff.
const faultRepeatWindow = time.Minute

// Rotation reasons (capture_rotations_total).
const (
	reasonStart     = "start"
	reasonHotReload = "hotreload"
	reasonFault     = "fault"
	reasonStop      = "stop"
	reasonSeqLimit  = "seq_limit"
)

var (
	// ErrNotStarted reports a Rotate with no capture open: before Begin or after End.
	ErrNotStarted = errors.New("producer: no capture open")
	// ErrRotationAbandoned reports a rotation that gave up before its commit point:
	// an observer held the old capture's lock past the deadline or the context's end, End began,
	// or the reserved lane had no room for the new start.
	// The previous capture stays current.
	ErrRotationAbandoned = errors.New("producer: rotation abandoned")
)

// Report is what End did; it is never an error the device must act on.
type Report struct {
	// CaptureID is the capture End closed; zero when none was open.
	CaptureID tracepack.UUID
	// StopQueued reports that the capture's stop was queued for publication.
	StopQueued bool
	// Abandoned reports that End passed its deadline and gave the capture up without its stop;
	// the consumer closes it unclean.
	Abandoned bool
	// Drained reports that every capture End stopped was published, or given up, before the deadline;
	// Dropped counts the records given up.
	Drained bool
	Dropped uint64
	// Contended reports that another lifecycle call held the lifecycle lock past the deadline: End did nothing.
	Contended bool
}

// uuidV7 generates capture ids as UUIDv7 (RFC 9562 §5.7) that sort by creation,
// within a millisecond by a 12-bit counter in rand_a (RFC 9562 §6.2, method 1).
type uuidV7 struct {
	lastMs  uint64
	counter uint16
}

// Begin opens a capture of the device d describes: the device's Start.
// It is idempotent: with a capture open it does nothing.
// It never waits for the bus.
//
// Returns:
//   - error: an error wrapping ErrInvalidDescriptor, or ErrRotationAbandoned.
func (p *Producer) Begin(ctx context.Context, d CaptureDescriptor) error {
	if err := d.Validate(); err != nil {
		return err
	}
	if !p.lockLife(deadline(ctx)) {
		return ErrRotationAbandoned
	}
	defer p.unlockLife()
	p.stopping.Store(false)
	if p.rec.Current() != nil {
		return nil
	}

	return p.rotateLocked(ctx, &d, p.last, reasonStart)
}

// Rotate rotates to a new capture for the changed descriptor d: the device's HotReload when any descriptor value changed.
// A descriptor equal to the current one rotates nothing.
//
// Returns:
//   - error: an error wrapping ErrInvalidDescriptor, ErrNotStarted, or ErrRotationAbandoned.
func (p *Producer) Rotate(ctx context.Context, d CaptureDescriptor) error {
	if err := d.Validate(); err != nil {
		return err
	}
	if !p.lockLife(deadline(ctx)) {
		return ErrRotationAbandoned
	}
	defer p.unlockLife()
	cur := p.rec.Current()
	if cur == nil {
		return ErrNotStarted
	}
	key, err := p.descriptorKey(&d)
	if err != nil {
		return err
	}
	if bytes.Equal(key, p.descKey) {
		return nil
	}
	id := cur.ID

	return p.rotateLocked(ctx, &d, &id, reasonHotReload)
}

// End closes the open capture: the device's Stop.
// It rotates to no capture within ctx's deadline, and past it abandons the capture without its stop;
// then it gives the publisher until the same deadline to publish what is queued of the capture, its stop included,
// and of every capture a rotation closed since the previous End; what is left then is dropped and counted.
// It never returns an error, and returns by the deadline.
func (p *Producer) End(ctx context.Context) Report {
	dl := deadline(ctx)
	// Announced before waiting for the lock, so a rotation inside it abandons; withdrawn if End gives up.
	p.ending.Add(1)
	if !p.lockLife(dl) {
		p.ending.Add(-1)
		return Report{Contended: true}
	}
	p.stopping.Store(true)
	p.ending.Add(-1)
	cur := p.rec.Current()
	if cur == nil {
		p.unlockLife()
		return Report{}
	}
	res := p.rec.Rotate(nil, dl, func() bool { return ctx.Err() != nil }, p.now)
	rep := Report{CaptureID: cur.ID, StopQueued: res.StopQueued}
	if res.Abandoned {
		p.rec.Abandon(&p.gen)
		rep.Abandoned = true
		p.emit(func() { p.log.Warn("producer: stop deadline passed, capture abandoned", "capture_id", cur.ID.String()) })
	}
	targets := p.retired
	targets = append(targets, cur)
	p.retired = nil
	for _, c := range targets {
		c.SetEndBy(dl)
	}
	id := cur.ID
	p.last = &id
	p.emit(func() { p.metrics.Add("capture_rotations_total", map[string]string{"reason": reasonStop}, 1) })
	p.setCurrentMetric(nil, nil)
	// The publisher is waited for outside the lifecycle lock, so a second End or a Begin is not held behind it.
	p.unlockLife()
	if p.pub == nil {
		return rep
	}
	p.pub.end(targets)
	rep.Drained, rep.Dropped = waitEnded(targets, dl)

	return rep
}

// waitEnded waits until every capture is terminated or dl passes,
// and returns whether all were, and every loss counted on them so far.
func waitEnded(caps []*core.Capture, dl time.Time) (bool, uint64) {
	t := time.NewTimer(time.Until(dl))
	defer t.Stop()
	all := true
	for _, c := range caps {
		select {
		case <-c.Done():
			continue
		case <-t.C:
			all = false
		}

		break
	}
	var dropped uint64
	for _, c := range caps {
		dropped += c.Dropped()
	}

	return all, dropped
}

// lockLife takes the lifecycle lock, giving up at dl.
func (p *Producer) lockLife(dl time.Time) bool {
	select {
	case p.lifeSem <- struct{}{}:
		return true
	default:
	}
	t := time.NewTimer(time.Until(dl))
	defer t.Stop()
	select {
	case p.lifeSem <- struct{}{}:
		return true
	case <-t.C:
		return false
	}
}

func (p *Producer) unlockLife() { <-p.lifeSem }

// rotateLocked builds a capture for d and rotates to it; the caller holds the lifecycle lock.
func (p *Producer) rotateLocked(ctx context.Context, d *CaptureDescriptor, previous *tracepack.UUID, reason string) error {
	key, err := p.descriptorKey(d)
	if err != nil {
		return err
	}
	wall, mono := p.now()
	id := p.ids.next()
	desc, err := encodeStartDescriptor(d, previous, time.Unix(0, wall), p.tun.ClockStepTolerance, p.recorder)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDescriptor, err)
	}
	c := core.NewCapture(core.CaptureParams{
		ID:          id,
		Generation:  p.gen.Load(),
		Lifecycle:   &p.gen,
		Tolerance:   int64(p.tun.ClockStepTolerance),
		ReservedCap: p.tun.ReservedLaneRecords,
		OriginWall:  wall,
		OriginMono:  mono,
		RotateAt:    p.rotateAt,
		Sampling:    sampling(d.Thinning),
	})
	c.Descriptor = base64.StdEncoding.EncodeToString(desc)
	if c.Subject, err = tracepack.BusSubject(p.prefix, d.ToolID, id); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDescriptor, err)
	}
	if err := p.checkStartFits(c); err != nil {
		return err
	}

	res := p.rec.Rotate(c, deadline(ctx), func() bool { return p.stoppingNow() || ctx.Err() != nil }, p.now)
	if res.Abandoned {
		return ErrRotationAbandoned
	}
	p.desc, p.descKey, p.last = *d, key, &id
	// A settled capture has nothing left for End to wait for.
	p.retired = slices.DeleteFunc(p.retired, (*core.Capture).Settled)
	if res.Old != nil {
		p.retired = append(p.retired, res.Old)
	}
	p.setCurrentMetric(c, d)
	p.emit(func() {
		p.metrics.Add("capture_rotations_total", map[string]string{"reason": reason}, 1)
		p.log.Info("producer: capture rotated", "reason", reason, "capture_id", id.String())
	})

	return nil
}

// checkStartFits refuses a descriptor whose start message's header block exceeds tracepack's 4096-byte bound,
// which no max_payload lifts.
func (p *Producer) checkStartFits(c *core.Capture) error {
	msgID, err := tracepack.BusMsgID(c.ID, 0)
	if err != nil {
		return err
	}
	m := Msg{
		Header: map[string]string{
			tracepack.BusHeaderLayout:     "1",
			tracepack.BusHeaderDescriptor: c.Descriptor,
		},
		MsgID:        msgID,
		ExpectStream: p.stream,
	}
	if n := headerBlockLen(&m); n > tracepack.BusHeaderBlockLimit {
		return fmt.Errorf("%w: the start's header block is %d bytes, above %d", ErrInvalidDescriptor, n, tracepack.BusHeaderBlockLimit)
	}

	return nil
}

// descriptorKey encodes what a descriptor decides, without the origin and previous capture a rotation fills in,
// so two descriptors that would publish the same values compare equal.
func (p *Producer) descriptorKey(d *CaptureDescriptor) ([]byte, error) {
	b, err := encodeStartDescriptor(d, nil, time.Unix(0, 0), p.tun.ClockStepTolerance, p.recorder)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDescriptor, err)
	}

	return b, nil
}

// setCurrentMetric replaces the current_capture info gauge, deleting the previous label set.
func (p *Producer) setCurrentMetric(c *core.Capture, d *CaptureDescriptor) {
	if prev := p.metricsCur; prev != nil {
		p.emit(func() { p.metrics.Delete("current_capture", prev) })
		p.metricsCur = nil
	}
	if c == nil {
		return
	}
	cur := map[string]string{
		"capture_id":           c.ID.String(),
		"tool_id":              d.ToolID,
		"recorder_instance_id": d.RecorderInstanceID().String(),
	}
	p.metricsCur = cur
	p.emit(func() { p.metrics.Set("current_capture", cur, 1) })
}

// emit hands a log line or a measurement to the sink goroutine without waiting:
// the lifecycle path never calls an integrator's Logger or Metrics, so neither can delay a bounded End.
// When the sink falls behind, the event is dropped.
func (p *Producer) emit(f func()) {
	select {
	case p.events <- f:
	default:
	}
}

// sinkLoop runs the events the lifecycle path emitted.
func (p *Producer) sinkLoop() {
	for {
		select {
		case <-p.quit:
			return
		case f := <-p.events:
			f()
		}
	}
}

// faultLoop rotates away from each capture the observers fault, on the producer's own goroutine.
func (p *Producer) faultLoop() {
	for {
		select {
		case <-p.quit:
			return
		case c := <-p.rec.Faults():
			p.handleFault(c)
		case c := <-p.rec.Rotations():
			p.handleSeqLimit(c)
		}
	}
}

// handleFault rotates from the faulted capture c to a fresh one, without a stop for c.
// A signal whose capture is no longer current, or of an older generation, is stale and discarded,
// so a fault rotation prepared before an End or a Rotate never installs a capture after them.
// A fresh capture that faults again within a minute waits for the backoff first, outside both locks.
func (p *Producer) handleFault(c *core.Capture) {
	if !p.lockLife(time.Now().Add(defaultLifecycleTimeout)) {
		return
	}
	if !p.faultCurrentLocked(c) {
		p.unlockLife()
		return
	}
	_, mono := p.now()
	repeat := c.ID == p.faultFresh && time.Duration(mono-p.faultedAt) < faultRepeatWindow
	p.unlockLife()
	if p.faultGap != nil {
		p.faultGap()
	}

	if repeat {
		select {
		case <-time.After(p.tun.FaultBackoff):
		case <-p.quit:
			return
		}
	}

	if !p.lockLife(time.Now().Add(defaultLifecycleTimeout)) {
		return
	}
	defer p.unlockLife()
	if !p.faultCurrentLocked(c) {
		return
	}
	p.emit(func() { p.log.Error("producer: capture fault, rotating", "capture_id", c.ID.String()) })
	ctx, cancel := context.WithTimeout(context.Background(), defaultLifecycleTimeout)
	defer cancel()
	d := p.desc
	id := c.ID
	err := p.rotateLocked(ctx, &d, &id, reasonFault)
	// The backoff covers a failed rotation too: the faulted capture stays current and keeps signalling,
	// so without it a rotation that cannot complete (a full reserved lane) would spin.
	_, p.faultedAt = p.now()
	if err != nil {
		p.emit(func() { p.log.Warn("producer: fault rotation failed", "capture_id", c.ID.String(), "error", err) })
		p.faultFresh = c.ID

		return
	}
	p.faultFresh = *p.last
}

// handleSeqLimit rotates away from a capture whose seq neared its end, with the same descriptor (G5-231).
func (p *Producer) handleSeqLimit(c *core.Capture) {
	if !p.lockLife(time.Now().Add(defaultLifecycleTimeout)) {
		return
	}
	defer p.unlockLife()
	if p.stoppingNow() || p.rec.Current() != c || c.Faulted() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultLifecycleTimeout)
	defer cancel()
	d := p.desc
	id := c.ID
	if err := p.rotateLocked(ctx, &d, &id, reasonSeqLimit); err != nil {
		p.emit(func() { p.log.Error("producer: seq-limit rotation failed", "capture_id", id.String(), "error", err) })
	}
}

// stoppingNow reports a completed stop, or an End waiting for the lifecycle lock.
func (p *Producer) stoppingNow() bool { return p.stopping.Load() || p.ending.Load() > 0 }

func (p *Producer) faultCurrentLocked(c *core.Capture) bool {
	return !p.stoppingNow() && !c.Abandoned() && p.rec.Current() == c && c.Faulted()
}

// next returns a capture id that sorts after every id this generator returned before.
func (g *uuidV7) next() tracepack.UUID {
	ms := uint64(time.Now().UnixMilli()) //nolint:gosec // the clock is past 1970
	if ms <= g.lastMs {
		g.counter++
		if g.counter > 0x0fff {
			g.lastMs++
			g.counter = 0
		}
		ms = g.lastMs
	} else {
		g.lastMs, g.counter = ms, 0
	}

	var u tracepack.UUID
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], ms<<16)
	copy(u[:6], ts[:6])
	binary.BigEndian.PutUint16(u[6:8], 0x7000|g.counter)
	_, _ = rand.Read(u[8:]) // crypto/rand.Read never fails (Go 1.24)
	u[8] = u[8]&0x3f | 0x80

	return u
}

func deadline(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}

	return time.Now().Add(defaultLifecycleTimeout)
}
