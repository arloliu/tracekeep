package producer

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
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
)

var (
	// ErrNotStarted reports a Rotate with no capture open: before Begin or after End.
	ErrNotStarted = errors.New("producer: no capture open")
	// ErrRotationAbandoned reports a rotation that gave up before its commit point:
	// an observer held the old capture's lock past the deadline, or End began.
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
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
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
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
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
// it never returns an error.
func (p *Producer) End(ctx context.Context) Report {
	p.stopping.Store(true)
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()

	cur := p.rec.Current()
	if cur == nil {
		return Report{}
	}
	res := p.rec.Rotate(nil, deadline(ctx), func() bool { return false }, p.now)
	rep := Report{CaptureID: cur.ID, StopQueued: res.StopQueued}
	if res.Abandoned {
		p.rec.Abandon(&p.gen)
		rep.Abandoned = true
		p.log.Warn("producer: stop deadline passed, capture abandoned", "capture_id", cur.ID.String())
	}
	id := cur.ID
	p.last = &id
	p.metrics.Add("capture_rotations_total", map[string]string{"reason": reasonStop}, 1)
	p.setCurrentMetric(nil, nil)

	return rep
}

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
	})
	c.Descriptor = base64.StdEncoding.EncodeToString(desc)

	res := p.rec.Rotate(c, deadline(ctx), p.stopping.Load, p.now)
	if res.Abandoned {
		return ErrRotationAbandoned
	}
	p.desc, p.descKey, p.last = *d, key, &id
	p.metrics.Add("capture_rotations_total", map[string]string{"reason": reason}, 1)
	p.setCurrentMetric(c, d)
	p.log.Info("producer: capture rotated", "reason", reason, "capture_id", id.String())

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
	if p.metricsCur != nil {
		p.metrics.Delete("current_capture", p.metricsCur)
		p.metricsCur = nil
	}
	if c == nil {
		return
	}
	p.metricsCur = map[string]string{
		"capture_id":           c.ID.String(),
		"tool_id":              d.ToolID,
		"recorder_instance_id": d.RecorderInstanceID().String(),
	}
	p.metrics.Set("current_capture", p.metricsCur, 1)
}

// faultLoop rotates away from each capture the observers fault, on the producer's own goroutine.
func (p *Producer) faultLoop() {
	for {
		select {
		case <-p.quit:
			return
		case c := <-p.rec.Faults():
			p.handleFault(c)
		}
	}
}

// handleFault rotates from the faulted capture c to a fresh one, without a stop for c.
// A signal whose capture is no longer current, or of an older generation, is stale and discarded,
// so a fault rotation prepared before an End or a Rotate never installs a capture after them.
// A fresh capture that faults again within a minute waits for the backoff first, outside both locks.
func (p *Producer) handleFault(c *core.Capture) {
	p.lifeMu.Lock()
	if !p.faultCurrentLocked(c) {
		p.lifeMu.Unlock()
		return
	}
	wall, _ := p.now()
	repeat := c.ID == p.faultFresh && time.Duration(wall-p.faultedAt) < faultRepeatWindow
	p.lifeMu.Unlock()

	if repeat {
		select {
		case <-time.After(p.tun.FaultBackoff):
		case <-p.quit:
			return
		}
	}

	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if !p.faultCurrentLocked(c) {
		return
	}
	p.log.Error("producer: capture fault, rotating", "capture_id", c.ID.String())
	ctx, cancel := context.WithTimeout(context.Background(), defaultLifecycleTimeout)
	defer cancel()
	d := p.desc
	id := c.ID
	if err := p.rotateLocked(ctx, &d, &id, reasonFault); err != nil {
		p.log.Warn("producer: fault rotation failed", "capture_id", c.ID.String(), "error", err)
		return
	}
	p.faultedAt, _ = p.now()
	p.faultFresh = *p.last
}

func (p *Producer) faultCurrentLocked(c *core.Capture) bool {
	return !p.stopping.Load() && !c.Abandoned() && p.rec.Current() == c && c.Faulted()
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
