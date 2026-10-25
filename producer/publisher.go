package producer

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

const (
	// pollInterval is how often the publisher looks at in-flight publishes and due retries while it has work.
	pollInterval = 5 * time.Millisecond
	// retryBase and retryCap bound the backoff between attempts at one record.
	retryBase = 100 * time.Millisecond
	retryCap  = 5 * time.Second
	// metricsInterval is how often counters are reported to Metrics.
	metricsInterval = time.Second
)

// dropReasons labels records_dropped_total.
var dropReasons = [core.NumDropReasons]string{
	core.DropLaneFull:      "lane_full",
	core.DropBusDown:       "bus_down",
	core.DropPublishFailed: "publish_failed",
	core.DropClaimFailed:   "claim_failed",
	core.DropCaptureFault:  "capture_fault",
	core.DropAbandoned:     "abandoned",
}

// errClaimConflict reports a claim object of the same name holding another record: a producer fault.
var errClaimConflict = errors.New("producer: claim object holds another record")

// publisher is the producer's one publishing goroutine, the only place that waits
// (docs/specs/tracekeep/producer.md §4.2 *Publisher goroutine*, *Dependency barrier*, *start publication*, *Large records*).
type publisher struct {
	p       *Producer
	bus     Bus
	objects Objects
	caps    map[*core.Capture]*capState
	flights []*flight
	drainCh chan drainReq
	drain   *drainReq

	// Counted on this goroutine and reported with the observers' counts.
	published  uint64
	clockSteps uint64
	dropped    [core.NumDropReasons]uint64
	dropBytes  uint64
	claimPuts  map[string]uint64
	reported   reported
	busDown    bool
	lastReport time.Time
}

// capState is what the publisher holds of one capture.
type capState struct {
	cap     *core.Capture
	subject string
	// fresh marks a capture first seen in this pass, whose reservations were not read before the lanes were drained.
	fresh bool
	// reserved holds its popped, unaccepted reserved records in seq order; the head is published synchronously.
	reserved []*pending
	retryAt  time.Time
	fails    int
	// held holds ordinary records the barrier or a retry keeps back.
	held     []*pending
	inflight int
	// minRes is the capture's lowest reserved seq, read before the lanes are drained.
	minRes    uint64
	hasMinRes bool
	// lostFrom is set when a reservation was never queued: no seq from it on can be published.
	lostFrom uint64
	lost     bool
}

// pending is one record on its way to the bus.
type pending struct {
	e     core.Entry
	msg   *message
	first time.Time
	next  time.Time
	fails int
	// claimed reports that its claim object is in the bucket, complete and verified.
	claimed bool
}

type flight struct {
	st  *capState
	pd  *pending
	fut AckFuture
}

type drainReq struct {
	deadline time.Time
	reply    chan uint64
}

// reported holds the counts already sent to Metrics, so each report adds the difference.
type reported struct {
	core      core.Counters
	published uint64
	steps     uint64
	dropped   [core.NumDropReasons]uint64
	dropBytes uint64
	claimPuts map[string]uint64
}

func newPublisher(p *Producer, bus Bus) *publisher {
	return &publisher{
		p:         p,
		bus:       bus,
		caps:      make(map[*core.Capture]*capState),
		drainCh:   make(chan drainReq, 1),
		claimPuts: make(map[string]uint64),
		reported:  reported{claimPuts: make(map[string]uint64)},
	}
}

func (pb *publisher) run(quit <-chan struct{}) {
	rec := pb.p.rec
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()
	for {
		pb.pass()
		pb.finishDrain()
		pb.report(false)

		var wake <-chan time.Time
		if pb.busy() {
			timer.Reset(pollInterval)
			wake = timer.C
		}
		select {
		case <-quit:
			pb.report(true)
			return
		case e := <-rec.Reserved():
			pb.add(e)
		case e := <-rec.Ordinary():
			pb.add(e)
		case d := <-pb.drainCh:
			pb.drain = &d
		case <-wake:
		}
	}
}

// pass makes all the progress it can: read reservations, drain the lanes, publish what the barrier allows.
// Reservations are read before the lanes are drained, so a record queued and retired in between is drained, not missed.
func (pb *publisher) pass() {
	now := time.Now()
	_, mono := pb.p.now()
	for _, st := range pb.caps {
		st.fresh = false
		st.minRes, st.hasMinRes = st.cap.MinReserved()
		pb.checkReservations(st, mono)
	}
	pb.drainLanes()
	pb.pollFlights(now)

	for c, st := range pb.caps {
		if st.fresh {
			continue
		}
		if c.Abandoned() {
			pb.dropAll(st, core.DropAbandoned)
		} else {
			pb.progress(st, now)
		}
		if st.empty() && pb.p.rec.Current() != c {
			delete(pb.caps, c)
		}
	}
}

// progress publishes st's reserved head while it may, then every held record the barrier allows and that is due.
func (pb *publisher) progress(st *capState, now time.Time) {
	for len(st.reserved) > 0 {
		head := st.reserved[0]
		if head.e.Kind == core.EntryStart {
			state := st.cap.StartState()
			if state == core.StartPending {
				return
			}
			if state == core.StartDiscarded {
				pb.dropAll(st, core.DropAbandoned)
				return
			}
		}
		if !st.mayPublish(head.e.Seq) || now.Before(st.retryAt) || !pb.publishReserved(st, head, now) {
			break
		}
		st.reserved = st.reserved[1:]
	}

	kept := st.held[:0]
	for _, pd := range st.held {
		switch {
		case st.lost && pd.e.Seq >= st.lostFrom:
			pb.dropEntry(&pd.e, core.DropCaptureFault)
		case !st.mayPublish(pd.e.Seq) || now.Before(pd.next) || !pb.sendOrdinary(st, pd, now):
			kept = append(kept, pd)
		default:
		}
	}
	clear(st.held[len(kept):])
	st.held = kept
}

// publishReserved publishes a reserved record synchronously; false leaves it for a retry after a backoff.
// A reserved record is never dropped for the bus: it is retried until it is accepted.
func (pb *publisher) publishReserved(st *capState, pd *pending, now time.Time) bool {
	if pd.msg == nil {
		m, err := pb.p.buildMessage(&pd.e, st.subject, pb.bus.MaxPayload())
		if err != nil {
			// A reserved record the publisher cannot materialize faults the capture (producer.md §4.2 *Lanes*).
			pb.p.log.Error("producer: reserved record cannot be built", "capture_id", st.cap.ID.String(), "seq", pd.e.Seq, "error", err)
			pb.loseFrom(st, pd.e.Seq)

			return false
		}
		pd.msg = m
	}
	if pd.msg.claim != nil && !pd.claimed {
		if err := pb.putClaim(pd.msg.claim); err != nil {
			pb.retryReserved(st, now, err)
			return false
		}
		pd.claimed = true
	}

	ctx, cancel := context.WithDeadline(context.Background(), pb.attemptDeadline(now))
	defer cancel()
	start := time.Now()
	_, err := pb.bus.Publish(ctx, &pd.msg.msg)
	if err != nil {
		pb.retryReserved(st, now, err)
		return false
	}
	pb.setBusDown(false)
	pb.p.metrics.Observe("publish_ack_seconds", nil, time.Since(start).Seconds())
	st.fails, st.retryAt = 0, time.Time{}
	pb.published++
	if pd.e.Kind == core.EntryClockStep {
		pb.clockSteps++
	}

	return true
}

func (pb *publisher) retryReserved(st *capState, now time.Time, err error) {
	if st.fails == 0 {
		pb.p.log.Warn("producer: reserved record not accepted, retrying", "capture_id", st.cap.ID.String(), "error", err)
	}
	pb.failed(err)
	st.fails++
	st.retryAt = now.Add(backoff(st.fails))
}

// sendOrdinary sends an ordinary record asynchronously, putting its claim object first;
// false keeps it for a retry, and a record past the retry horizon H is dropped as a seq gap.
func (pb *publisher) sendOrdinary(st *capState, pd *pending, now time.Time) bool {
	if pd.first.IsZero() {
		pd.first = now
	}
	if pd.msg == nil {
		m, err := pb.p.buildMessage(&pd.e, st.subject, pb.bus.MaxPayload())
		if err != nil {
			pb.p.log.Error("producer: record cannot be built", "capture_id", st.cap.ID.String(), "seq", pd.e.Seq, "error", err)
			pb.dropEntry(&pd.e, core.DropPublishFailed)

			return true
		}
		pd.msg = m
	}
	if pd.msg.claim != nil && !pd.claimed {
		if err := pb.putClaim(pd.msg.claim); err != nil {
			if errors.Is(err, errClaimConflict) {
				pb.p.log.Error("producer: claim-conflict", "capture_id", st.cap.ID.String(), "seq", pd.e.Seq)
				pb.dropEntry(&pd.e, core.DropClaimFailed)

				return true
			}

			return pb.retryOrDrop(pd, now, core.DropClaimFailed)
		}
		pd.claimed = true
	}
	fut, err := pb.bus.PublishAsync(&pd.msg.msg)
	if err != nil {
		pb.failed(err)
		return pb.retryOrDrop(pd, now, core.DropPublishFailed)
	}
	pb.flights = append(pb.flights, &flight{st: st, pd: pd, fut: fut})
	st.inflight++

	return true
}

// retryOrDrop schedules another attempt at pd, or drops it once H has passed since its first attempt.
// It reports whether pd left the held list.
func (pb *publisher) retryOrDrop(pd *pending, now time.Time, reason core.DropReason) bool {
	if now.Sub(pd.first) >= pb.p.tun.RetryHorizon {
		pb.dropEntry(&pd.e, reason)
		return true
	}
	pd.fails++
	pd.next = now.Add(backoff(pd.fails))

	return false
}

// putClaim makes sure the claim object holds exactly c's payload, never overwriting another version:
// an existing object with the same digest and record header is reused only after a full read-back,
// since an uncertain earlier put can leave metadata without every chunk.
func (pb *publisher) putClaim(c *claimObject) error {
	ctx, cancel := context.WithDeadline(context.Background(), pb.attemptDeadline(time.Now()))
	defer cancel()
	if pb.objects == nil {
		objs, err := pb.bus.Objects(ctx, pb.p.bucket)
		if err != nil {
			pb.claimPuts["error"]++
			return err
		}
		pb.objects = objs
	}

	info, err := pb.objects.GetInfo(ctx, c.meta.Name)
	switch {
	case errors.Is(err, ErrObjectNotFound):
	case err != nil:
		pb.claimPuts["error"]++
		return err
	default:
		if info.Digest != c.digest ||
			info.Metadata[ObjectMetaRecordHeader] != c.meta.Metadata[ObjectMetaRecordHeader] ||
			info.Metadata[ObjectMetaTSUTCNs] != c.meta.Metadata[ObjectMetaTSUTCNs] {
			pb.claimPuts["conflict"]++
			return errClaimConflict
		}
		data, err := pb.objects.Get(ctx, c.meta.Name)
		if err == nil && len(data) == len(c.payload) && tracepack.BusPayloadDigest(data) == c.digest {
			pb.claimPuts["reused"]++
			return nil
		}
		if err := pb.objects.Delete(ctx, c.meta.Name); err != nil {
			pb.claimPuts["error"]++
			return err
		}
	}
	if _, err := pb.objects.Put(ctx, c.meta, c.payload); err != nil {
		pb.claimPuts["error"]++
		return err
	}
	pb.claimPuts["put"]++

	return nil
}

// pollFlights collects finished asynchronous publishes.
func (pb *publisher) pollFlights(now time.Time) {
	kept := pb.flights[:0]
	for _, f := range pb.flights {
		select {
		case <-f.fut.Ok():
			f.st.inflight--
			pb.setBusDown(false)
			pb.published++
			pb.p.rec.ReleaseOrdinary(&f.pd.e)
		case err := <-f.fut.Err():
			f.st.inflight--
			pb.failed(err)
			if !pb.retryOrDrop(f.pd, now, core.DropPublishFailed) {
				f.st.held = append(f.st.held, f.pd)
			}
		default:
			kept = append(kept, f)
		}
	}
	clear(pb.flights[len(kept):])
	pb.flights = kept
}

// checkReservations faults a capture whose reservation stayed unqueued past the bound:
// its seq can never be published, so nothing from it on is (producer.md §4.2 *Lanes*, fault handling).
func (pb *publisher) checkReservations(st *capState, mono int64) {
	oldest, ok := st.cap.OldestReservation()
	if !ok || st.lost || time.Duration(mono-oldest) < pb.p.tun.ReservationTimeout {
		return
	}
	pb.p.log.Error("producer: reservation never queued, capture faulted", "capture_id", st.cap.ID.String())
	low, _ := st.cap.MinReserved()
	pb.loseFrom(st, low)
}

// loseFrom faults st's capture and gives up every record from seq on.
func (pb *publisher) loseFrom(st *capState, seq uint64) {
	st.cap.Fault()
	pb.p.rec.SignalFault(st.cap)
	st.lost, st.lostFrom = true, seq
	kept := st.reserved[:0]
	for _, pd := range st.reserved {
		if pd.e.Seq >= seq {
			pb.dropEntry(&pd.e, core.DropCaptureFault)
		} else {
			kept = append(kept, pd)
		}
	}
	st.reserved = kept
}

func (pb *publisher) drainLanes() {
	rec := pb.p.rec
	for {
		select {
		case e := <-rec.Reserved():
			pb.add(e)
		case e := <-rec.Ordinary():
			pb.add(e)
		default:
			return
		}
	}
}

// add takes one lane entry into its capture's state.
func (pb *publisher) add(e core.Entry) {
	st := pb.caps[e.Cap]
	if st == nil {
		st = &capState{cap: e.Cap, subject: e.Cap.Subject, fresh: true}
		pb.caps[e.Cap] = st
	}
	pd := &pending{e: e}
	switch {
	case st.lost && e.Seq >= st.lostFrom:
		pb.dropEntry(&e, core.DropCaptureFault)
	case e.Kind == core.EntryObserved:
		st.held = append(st.held, pd)
	default:
		i, _ := slices.BinarySearchFunc(st.reserved, e.Seq, func(p *pending, seq uint64) int {
			return compareSeq(p.e.Seq, seq)
		})
		st.reserved = slices.Insert(st.reserved, i, pd)
	}
}

// finishDrain answers End's drain once nothing is left, or at its deadline after dropping what is left.
func (pb *publisher) finishDrain() {
	if pb.drain == nil {
		return
	}
	if !pb.idle() && time.Now().Before(pb.drain.deadline) {
		return
	}
	pb.drainLanes()
	var left uint64
	for c, st := range pb.caps {
		left += uint64(len(st.reserved) + len(st.held))
		pb.dropAll(st, core.DropPublishFailed)
		if st.empty() {
			delete(pb.caps, c)
		}
	}
	pb.drain.reply <- left
	pb.drain = nil
}

func (pb *publisher) dropAll(st *capState, reason core.DropReason) {
	for _, pd := range st.reserved {
		pb.dropEntry(&pd.e, reason)
	}
	for _, pd := range st.held {
		pb.dropEntry(&pd.e, reason)
	}
	st.reserved, st.held = nil, nil
}

func (pb *publisher) dropEntry(e *core.Entry, reason core.DropReason) {
	pb.dropped[reason]++
	pb.dropBytes += uint64(len(e.Payload))
	if e.Kind == core.EntryObserved {
		pb.p.rec.ReleaseOrdinary(e)
	}
}

// failed records a failed publish: the bus counts as down until a publish succeeds.
func (pb *publisher) failed(err error) {
	if !pb.busDown {
		pb.p.log.Warn("producer: traffic bus unreachable", "error", err)
	}
	pb.setBusDown(true)
}

func (pb *publisher) setBusDown(down bool) {
	if down == pb.busDown {
		return
	}
	pb.busDown = down
	pb.p.rec.SetBusDown(down)
	if !down {
		pb.p.log.Info("producer: traffic bus reachable")
	}
}

// attemptDeadline is τ from now, cut short by a drain's deadline.
func (pb *publisher) attemptDeadline(now time.Time) time.Time {
	d := now.Add(pb.p.tun.PublishTimeout)
	if pb.drain != nil && pb.drain.deadline.Before(d) {
		d = pb.drain.deadline
	}

	return d
}

func (pb *publisher) busy() bool {
	if len(pb.flights) > 0 {
		return true
	}
	for _, st := range pb.caps {
		if len(st.reserved) > 0 || len(st.held) > 0 {
			return true
		}
	}

	return false
}

func (pb *publisher) idle() bool {
	rec := pb.p.rec
	return !pb.busy() && len(rec.Reserved()) == 0 && len(rec.Ordinary()) == 0
}

// report sends the counts gathered since the last report to Metrics, at most once per interval unless forced.
func (pb *publisher) report(force bool) {
	now := time.Now()
	if !force && now.Sub(pb.lastReport) < metricsInterval {
		return
	}
	pb.lastReport = now
	m, rec := pb.p.metrics, pb.p.rec
	c := rec.Counters()
	last := &pb.reported

	for k := range c.Observed {
		for d := range c.Observed[k] {
			if delta := c.Observed[k][d] - last.core.Observed[k][d]; delta > 0 {
				m.Add("records_observed_total", map[string]string{"kind": tracepack.Kind(k).String(), "dir": tracepack.Dir(d).String()}, float64(delta))
			}
		}
	}
	for i := range core.NumDropReasons {
		total := c.Dropped[i] + pb.dropped[i]
		if delta := total - last.core.Dropped[i] - last.dropped[i]; delta > 0 {
			m.Add("records_dropped_total", map[string]string{"reason": dropReasons[i]}, float64(delta))
		}
	}
	if delta := c.AfterStop - last.core.AfterStop; delta > 0 {
		m.Add("records_dropped_total", map[string]string{"reason": "after_stop"}, float64(delta))
	}
	if delta := c.DroppedBytes + pb.dropBytes - last.core.DroppedBytes - last.dropBytes; delta > 0 {
		m.Add("dropped_bytes_total", nil, float64(delta))
	}
	if delta := pb.published - last.published; delta > 0 {
		m.Add("records_published_total", nil, float64(delta))
	}
	if delta := pb.clockSteps - last.steps; delta > 0 {
		m.Add("clock_steps_total", nil, float64(delta))
	}
	for outcome, n := range pb.claimPuts {
		if delta := n - last.claimPuts[outcome]; delta > 0 {
			m.Add("claim_puts_total", map[string]string{"outcome": outcome}, float64(delta))
		}
		last.claimPuts[outcome] = n
	}
	depth, bytes := rec.OrdinaryDepth()
	m.Set("ring_depth", nil, float64(depth))
	m.Set("ring_bytes", nil, float64(bytes))
	m.Set("reserved_lane_depth", nil, float64(len(rec.Reserved())))
	connected := 1.0
	if pb.busDown {
		connected = 0
	}
	m.Set("bus_connected", nil, connected)

	last.core, last.published, last.steps, last.dropped, last.dropBytes = c, pb.published, pb.clockSteps, pb.dropped, pb.dropBytes
}

// mayPublish is the dependency barrier: seq may be published only when no reserved seq below it is unaccepted,
// neither queued here nor still reserved in the capture, and no lost seq is at or below it.
func (st *capState) mayPublish(seq uint64) bool {
	if st.lost && seq >= st.lostFrom {
		return false
	}
	if st.hasMinRes && st.minRes < seq {
		return false
	}

	return len(st.reserved) == 0 || st.reserved[0].e.Seq >= seq
}

func (st *capState) empty() bool {
	return len(st.reserved) == 0 && len(st.held) == 0 && st.inflight == 0
}

func backoff(fails int) time.Duration {
	d := retryBase << min(fails-1, 6)
	return min(d, retryCap)
}

func compareSeq(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
