package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// Nak delays: a deferral backs off from the first to the last as the message is redelivered (ingest.md §6.1).
const (
	firstNakDelay = 5 * time.Second
	lastNakDelay  = time.Minute
)

// suppressedNakDelay outwaits the parking stream's duplicate window, which suppressed a copy's publish and stored nothing (parking.md *Confirmation*).
const suppressedNakDelay = busconfig.DuplicateWindow + firstNakDelay

// writeFailureLimit is how many times the Writer refuses one batch before the refusal counts as deterministic.
const writeFailureLimit = 3

// Run's backoff after a failed step: a catalog or bus outage is retried, never spun on.
const (
	firstStepBackoff = 100 * time.Millisecond
	lastStepBackoff  = 5 * time.Second
)

// ReasonIncarnationChanged is the alert of a traffic stream that is no longer the incarnation the catalog records (parking.md *incarnations*).
const ReasonIncarnationChanged = "bus-incarnation-changed"

// Ingester is one ingest replica: it holds a membership in the activation gate,
// pulls the traffic stream, decides each delivery, batches and flushes (rollouts.md §6.4, ingest.md §6).
// It is driven by Step, which Run calls in a loop; Step starts no goroutine of its own,
// and Run runs one more, which keeps in progress the held messages that wait past a routine delivery's life.
type Ingester struct {
	Executor *Executor
	Batcher  *Batcher
	Flusher  *Flusher
	// Coordinator parks the deferrals that cannot clear by themselves and acknowledges every resolved version.
	Coordinator *Coordinator
	// Park enables parking (parking.enabled); without it an indefinite deferral is nak'd like any other,
	// and the consumer's acknowledgement budget may fill. It stops new moves only: a parked consumer works either way.
	Park bool
	// RedeliveryDelay is the parked consumer's nak delay (parking.redelivery_delay), in place of the backoff.
	RedeliveryDelay time.Duration
	Consumer        jetstream.Consumer
	// Stream is the stream the consumer pulls, the traffic stream's or the parking stream's,
	// checked against Incarnation, the catalog's, with every renewal.
	Stream      jetstream.Stream
	Incarnation catalog.Incarnation
	Log         *slog.Logger
	// Replica names this replica's membership.
	Replica string
	// Lease is the membership lease, renewed before every fetch; a fetch waits at most what remains of it.
	Lease time.Duration
	// FetchBatch bounds the messages of one fetch.
	FetchBatch int
	// MaxWait bounds how long a fetch waits for FetchBatch messages; it never waits past what remains of the lease.
	MaxWait time.Duration
	// Heartbeat is the keep's tick: while Run runs, every tick keeps in progress each batch not yet flushing
	// whose deliveries have gone AckWait less FlushTimeout and two ticks since their fetch or their last in-progress ack (ingest.md §6.2),
	// so each has a flush's budget and a tick of its ack wait left when its flush starts.
	Heartbeat time.Duration
	// AckWait is the consumer's ack wait, which the keep holds a waiting batch's deliveries within; zero keeps every waiting batch at every tick.
	AckWait time.Duration
	// FlushTimeout is a batch's flush budget (ingest.flush_timeout): a flush not done within it is given up, its messages nak'd (ingest.md §6.2).
	// Zero gives no bound.
	FlushTimeout time.Duration
	// StopTimeout is the budget the batches still due share once Run's context ends (ingest.stop_timeout); zero gives none.
	StopTimeout time.Duration
	// Now is the clock the batches and the lease are judged by.
	Now func() time.Time
	// Metrics records the role's metrics; nil records none.
	Metrics *metrics.Ingest
	// Timed, a test seam for the load harness, is told the wall time of each delivery's disposition and batching
	// and of each batch's flush; nil in production, where no clock is read for it.
	Timed func(stage string, took time.Duration)
	// PassSettled, a test seam for the load harness, is called once every batch a flush pass took is settled,
	// acknowledged or nak'd, with the pass's batches and its failure; an empty pass calls nothing. Nil in production.
	PassSettled func(at time.Time, batches []*Batch, err error)

	// writeFailures counts, by record version, the flushes the Writer refused holding it: a record refused three times is deterministic,
	// whichever batch held it, since a live capture's redelivered records join fresh ones and the metadata budget splits a key's batches.
	writeFailures map[Version]int
	// mu guards the batcher, flushing and inFlight against the heartbeat.
	mu sync.Mutex
	// flushing are the batches a flush took from the batcher and has not started, which the heartbeat keeps like the batcher's.
	flushing []*Batch
	// inFlight is the batch a flush is writing, held but never kept: its budget ends before its deliveries' ack wait does.
	// The heartbeat reads none of its entries, which the flush sorts.
	inFlight *Batch
	// fetched is when the fetch being disposed of started; only the stepping goroutine reads or writes it.
	fetched time.Time

	membership catalog.Membership
	// leaseEnd is when this replica stops trusting its lease: the renewal's start plus the lease,
	// before the database's own end, which the renewal set later.
	leaseEnd time.Time
}

// Join registers the membership: refused unless this replica executes every configuration not yet retired.
func (g *Ingester) Join(ctx context.Context) error {
	ids := make([]int64, 0, len(g.Executor.Configs))
	for id := range g.Executor.Configs {
		ids = append(ids, id)
	}
	start := g.Now()
	m, err := g.Executor.Catalog.Register(ctx, g.Replica, ids, g.Lease)
	if err != nil {
		return err
	}
	g.membership, g.leaseEnd = m, start.Add(g.Lease)

	return nil
}

// Run steps until ctx ends, then flushes what it holds within the stop budget, StopTimeout from ctx's end,
// and returns the failure of any flush the stop overlapped: a stop whose batches were not all written failed;
// a stream that is no longer its incarnation ends it at once, the held deliveries abandoned unacknowledged.
func (g *Ingester) Run(ctx context.Context) error {
	if g.Heartbeat <= 0 {
		return errors.New("ingest: no heartbeat")
	}
	stopping, release := stopBudget(ctx, g.StopTimeout)
	defer release()
	beat, stop := context.WithCancel(context.WithoutCancel(ctx))
	ticker := time.NewTicker(g.Heartbeat)
	beating := make(chan struct{})
	go func() {
		defer close(beating)
		g.heartbeats(beat, ticker.C)
	}()
	defer func() { // no progress is sent once Run returned
		stop()
		ticker.Stop()
		<-beating
	}()

	var backoff time.Duration
	var stopErr error
	for ctx.Err() == nil {
		err := g.stepFlushing(ctx, stopping)
		switch {
		case errors.Is(err, catalog.ErrIncarnationChanged):
			return err
		case errors.Is(err, errStopFlush):
			stopErr = err
		case err != nil:
			g.Log.Warn("ingest: step failed", "error", err)
			backoff = min(max(2*backoff, firstStepBackoff), lastStepBackoff)
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
			case <-t.C:
			}
			t.Stop()
		default:
			backoff = 0
		}
	}

	return errors.Join(stopErr, g.flush(stopping, g.take(g.Batcher.All)))
}

// errStopFlush marks the failure of a flush a stop overlapped, which fails the stop.
var errStopFlush = errors.New("ingest: a flush during the stop failed")

// stopBudget is a context that outlives ctx by budget, the one budget every flush still due at a stop shares (ingest.md §6.2),
// and its release; with no budget it ends only at its release.
func stopBudget(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	out, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if budget <= 0 {
		return out, cancel
	}
	var mu sync.Mutex
	var timer *time.Timer
	armed := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		timer = time.AfterFunc(budget, cancel)
	})

	return out, func() {
		armed()
		mu.Lock()
		if timer != nil {
			timer.Stop()
		}
		mu.Unlock()
		cancel()
	}
}

// Step renews the membership, fetches one batch of messages within the lease, disposes of each,
// and flushes the batches that are due, one its byte bound closes before the fetch's next delivery.
// It returns the renewal's and the fetch's errors, a pull the server ended with an error included,
// after disposing of what it delivered; a failed flush's batch is nak'd and logged,
// and its error is returned too once ctx has ended, the flush then being part of a stop.
func (g *Ingester) Step(ctx context.Context) error {
	return g.stepFlushing(ctx, ctx)
}

// stepFlushing is Step flushing the due batches under flushing, which a stop does not end before its budget:
// a batch due when the stop comes is flushed within the budget, never given up at once and written again.
func (g *Ingester) stepFlushing(ctx, flushing context.Context) error {
	if err := g.renew(ctx); err != nil {
		if errors.Is(err, catalog.ErrIncarnationChanged) {
			g.Log.Error("ingest: alert", "reason", ReasonIncarnationChanged, "error", err)
			g.mu.Lock()
			g.Batcher.All() // abandoned unacknowledged (parking.md *incarnations*)
			g.mu.Unlock()
		}

		return err
	}
	var fetchErr, flushErr error
	flushDue := func(remove func() []*Batch) {
		if err := g.flush(flushing, g.take(remove)); err != nil {
			g.Log.Warn("ingest: flush failed", "error", err)
			flushErr = errors.Join(flushErr, err)
		}
	}
	g.fetched = g.Now()
	if wait := min(g.MaxWait, g.leaseEnd.Sub(g.fetched)/2); wait >= time.Millisecond { // otherwise no lease is left to fetch within
		batch, err := g.Consumer.Fetch(g.FetchBatch, jetstream.FetchMaxWait(wait))
		if err != nil {
			return fmt.Errorf("ingest: fetch: %w", err)
		}
		generation := g.membership.Generation
		for m := range batch.Messages() {
			g.handle(ctx, m, generation)
			if len(g.Batcher.closed) > 0 { // only this goroutine changes the batcher, so its read needs no lock
				flushDue(g.Batcher.Closed) // before the next delivery is batched, so no fetch is held whole
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) && !errors.Is(err, context.DeadlineExceeded) {
			fetchErr = fmt.Errorf("ingest: fetch ended: %w", err)
		}
	}
	now := g.Now()
	flushDue(func() []*Batch { return g.Batcher.Due(now) })
	if flushErr != nil && ctx.Err() != nil {
		return errors.Join(fetchErr, fmt.Errorf("%w: %w", errStopFlush, flushErr))
	}

	return fetchErr
}

// take removes batches from the batcher with remove and hands them to the heartbeat in one step,
// so no tick finds them in neither place.
func (g *Ingester) take(remove func() []*Batch) []*Batch {
	g.mu.Lock()
	defer g.mu.Unlock()
	batches := remove()
	g.flushing = batches

	return batches
}

// heartbeats keeps the waiting messages in progress at every tick until ctx ends.
func (g *Ingester) heartbeats(ctx context.Context, tick <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			g.beat()
		}
	}
}

// beat keeps in progress the messages of each batch not yet flushing, the batcher's and those a flush took and has not started,
// once they have gone AckWait less FlushTimeout and two ticks since their fetch or their last in-progress ack:
// a routine delivery is acknowledged before then and is never kept, and a batch whose flush started is never kept.
// One racing its message's acknowledgement is harmless: the server ignores progress on a message no longer pending.
// The deferrals of the waiting messages, write failures redelivered into a batch, are kept while they are held.
func (g *Ingester) beat() {
	now := g.Now()
	after := g.AckWait - g.FlushTimeout - 2*g.Heartbeat
	var held, kept []Entry
	visit := func(b *Batch) {
		held = append(held, b.Entries...)
		if now.Sub(b.kept) >= after {
			kept = append(kept, b.Entries...)
			b.kept = now
		}
	}
	g.mu.Lock()
	g.Batcher.Held(visit)
	for _, b := range g.flushing {
		visit(b)
	}
	g.mu.Unlock()
	for _, e := range kept {
		_ = e.Msg.InProgress()
	}
	ids := make([]string, len(held))
	for i, e := range held {
		ids[i] = e.D.deferralID()
	}
	g.Metrics.InProgress(len(kept))
	g.Metrics.Held(ids)
}

// renew checks the stream against its incarnation, then renews the membership, or registers again once it lapsed.
func (g *Ingester) renew(ctx context.Context) error {
	if err := g.checkIncarnation(ctx); err != nil {
		return err
	}
	start := g.Now()
	err := g.Executor.Catalog.Renew(ctx, g.membership, g.Lease)
	if errors.Is(err, catalog.ErrMembershipLapsed) {
		g.Log.Warn("ingest: membership lapsed, registering again", "replica", g.Replica)
		return g.Join(ctx)
	}
	if err != nil {
		return err
	}
	g.leaseEnd = start.Add(g.Lease)

	return nil
}

// checkIncarnation verifies that the stream is still the incarnation the consumer was named for,
// its creation time and a last sequence not below one observed (parking.md *incarnations*),
// then spends the traffic stream's orders its first sequence passed.
func (g *Ingester) checkIncarnation(ctx context.Context) error {
	inc, err := bus.Incarnate(ctx, g.Executor.Catalog, g.Incarnation.Role, g.Stream)
	if err != nil {
		return err
	}
	if inc.ID != g.Incarnation.ID {
		return fmt.Errorf("%w: incarnation %d, the consumer is named for %d", catalog.ErrIncarnationChanged, inc.ID, g.Incarnation.ID)
	}
	if inc.Role != catalog.RoleTraffic {
		return nil
	}
	// Incarnate read the stream; an order whose cut-off its first sequence passed covers no message that can still exist.
	first := int64(g.Stream.CachedInfo().State.FirstSeq) //nolint:gosec // a stream sequence fits
	_, err = g.Executor.Catalog.SpendOrders(ctx, inc.ID, first)

	return err
}

// handle disposes of one delivery pulled under membership generation:
// a message received after the lease lapsed, or under an earlier membership, is nak'd untouched (rollouts.md §6.4).
func (g *Ingester) handle(ctx context.Context, m jetstream.Msg, generation int64) {
	if !g.Now().Before(g.leaseEnd) || generation != g.membership.Generation {
		_ = m.NakWithDelay(firstNakDelay)
		return
	}
	msg, err := readMessage(m)
	if err != nil {
		_ = m.NakWithDelay(firstNakDelay)
		return
	}
	msg.Incarnation, msg.Parked, msg.Fetched = g.Incarnation.ID, g.Incarnation.Role == catalog.RoleParking, g.fetched
	if msg.Parked {
		if err := g.recordCopy(ctx, msg); err != nil {
			g.Log.Warn("ingest: copy row failed", "msg_id", msg.Env.MsgID, "error", err)
			_ = m.NakWithDelay(g.delay(msg.Delivered))

			return
		}
	}
	disposed := timing(g.Timed, "dispose")
	d, dec, err := g.Executor.Dispose(ctx, msg)
	disposed()
	if err != nil {
		g.Log.Warn("ingest: disposition failed", "msg_id", msg.Env.MsgID, "error", err)
		_ = m.NakWithDelay(g.delay(msg.Delivered))

		return
	}
	g.act(ctx, Entry{D: d, Msg: m}, dec, msg.Delivered)
}

// recordCopy inserts or finds the copy row of the parked copy msg before anything disposes of it, so a redelivery finds its own row,
// and advances a version still parking, whose original's process stopped between the publish and the advance (parking.md *Two tables*).
// A message without a version is left to its disposition, which terminates it as malformed.
func (g *Ingester) recordCopy(ctx context.Context, msg Message) error {
	d := &Delivery{Msg: msg}
	d.Raw, d.Facts = ReadRaw(&d.Msg.Env, d.Msg.Body, 0)
	if !d.Facts.LayoutOK || !d.Facts.MsgIDOK {
		return nil
	}
	state, err := g.Executor.Catalog.RecordCopy(ctx, d.identity())
	if err != nil || state != catalog.VersionParking {
		return err
	}
	_, err = g.Coordinator.Advance(ctx, d)

	return err
}

// delay is the nak delay of a message delivered so many times: the parked consumer's redelivery delay, or the backoff.
func (g *Ingester) delay(delivered uint64) time.Duration {
	if g.Incarnation.Role == catalog.RoleParking {
		return g.RedeliveryDelay
	}

	return nakDelay(delivered)
}

// alert logs a disposition that raises an alert: its reason, and for a masking failure its incident and why,
// an error that names keys, references and seqs and never material or content.
func (g *Ingester) alert(d *Delivery, dec Decision) {
	if !dec.Alert {
		return
	}
	attrs := []any{"reason", dec.Reason, "msg_id", d.Msg.Env.MsgID}
	if d.MaskErr != nil {
		attrs = append(attrs, "incident", d.Incident, "error", d.MaskErr.Error())
	}
	g.Log.Warn("ingest: alert", attrs...)
}

// act carries out a final disposition.
func (g *Ingester) act(ctx context.Context, e Entry, dec Decision, delivered uint64) {
	g.alert(e.D, dec)
	g.Metrics.Disposed(dec.Action.String(), delivered > 1)
	switch dec.Action {
	case ActBatch: // settled once its batch is registered: a batch the Writer refuses keeps it deferred
		g.Metrics.Held([]string{e.D.deferralID()}) // kept from now, the heartbeat keeping it while held
		batched := timing(g.Timed, "batch")
		g.mu.Lock()
		g.Batcher.Add(e, g.Now())
		g.mu.Unlock()
		batched()
	case ActAck:
		g.resolved(e)
		g.acknowledge(ctx, e)
	case ActPark: // park records whether the message stays deferred
		g.park(ctx, e, dec.Reason, delivered)
	case ActTerminate:
		g.resolved(e)
		if e.D.Msg.Parked && dec.Reason == ReasonBeyondRetention {
			// A parked message is never terminated for its age (G5-267): its version waits for an operator as evicted-hour.
			g.evict(ctx, e)
			return
		}
		if dec.Audit {
			if err := g.Executor.AuditTermination(ctx, e.D, dec.Reason); err != nil {
				// No audit row, no termination: the message is decided again at its redelivery.
				_ = e.Msg.NakWithDelay(g.delay(delivered))
				return
			}
		}
		_ = e.Msg.Term()
		if dec.Reason == ReasonBeyondRetention {
			g.Metrics.Lost(e.D.ToolID)
		}
	case ActNak:
		g.deferMsg(e, dec.Reason, g.delay(delivered))
	default:
		panic("ingest: unknown action")
	}
}

// deferMsg naks e's message for redelivery after delay, noting it deferred for reason.
func (g *Ingester) deferMsg(e Entry, reason string, delay time.Duration) {
	g.Metrics.Deferred(e.D.deferralID(), reason, delay)
	_ = e.Msg.NakWithDelay(delay)
}

// acknowledge acknowledges a message of a resolved version through the coordinator, which records it under a termination.
func (g *Ingester) acknowledge(ctx context.Context, e Entry) {
	if err := g.Coordinator.Acknowledge(ctx, e.D, e.Msg); err != nil {
		// Unacknowledged: the message returns and is acknowledged again under the same resolution.
		g.Log.Warn("ingest: ack failed", "msg_id", e.D.Msg.Env.MsgID, "error", err)
	}
}

// evict moves the version of a parked copy whose hour retention evicted from parked to evicted-hour, and returns the copy:
// it is neither staged nor terminated for its age, and only an operator ends it.
func (g *Ingester) evict(ctx context.Context, e Entry) {
	if _, err := g.Executor.Catalog.EvictParked(ctx, e.D.versionKey()); err != nil {
		g.Log.Warn("ingest: eviction of a parked version failed", "msg_id", e.D.Msg.Env.MsgID, "error", err)
	}
	g.Log.Warn("ingest: alert", "reason", ReasonBeyondRetention, "msg_id", e.D.Msg.Env.MsgID, "parked", true)
	_ = e.Msg.NakWithDelay(g.RedeliveryDelay)
}

// park moves an indefinite deferral to the parking stream and acts on what the coordinator found:
// a version past the horizon is terminated, a resolved one acknowledged, an unconfirmed copy's original nak'd to be parked again.
// Without parking, and for a copy already parked, the deferral is nak'd like any other.
func (g *Ingester) park(ctx context.Context, e Entry, reason string, delivered uint64) {
	id := e.D.Msg.Env.MsgID
	if !g.Park || e.D.Msg.Parked {
		g.deferMsg(e, reason, g.delay(delivered))
		return
	}
	p, err := g.Coordinator.Park(ctx, e, reason, HourOf(g.Executor.Horizon()))
	switch {
	case err != nil:
		cause := slog.Any("error", err)
		if p.Confirmation == Unconfirmed {
			cause = publishCause(err)
		}
		g.Log.Warn("ingest: parking failed", "msg_id", id, cause)
		g.deferMsg(e, reason, g.delay(delivered))
	case p.Begun.BeyondRetention:
		g.act(ctx, e, Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}, delivered)
	case p.Begun.State.Resolved():
		g.resolved(e)
		g.acknowledge(ctx, e)
	case p.Confirmation == Suppressed:
		g.deferMsg(e, reason, suppressedNakDelay)
	case p.Confirmation != Confirmed:
		g.deferMsg(e, reason, g.delay(delivered))
	default: // parked: the original was acknowledged
		g.resolved(e)
	}
}

// publishCause is an unconfirmed publish's cause as fields built here, never the error's text,
// which can hold the server's description, a URL or a credential:
// a JetStream API error's code and error code, a context error, else the class publish-failed.
func publishCause(err error) slog.Attr {
	var api *jetstream.APIError
	switch {
	case errors.As(err, &api):
		return slog.Group("publish", "code", api.Code, "error_code", api.ErrorCode)
	case errors.Is(err, context.Canceled):
		return slog.Group("publish", "error", context.Canceled)
	case errors.Is(err, context.DeadlineExceeded):
		return slog.Group("publish", "error", context.DeadlineExceeded)
	default:
		return slog.Group("publish", "class", "publish-failed")
	}
}

// flush flushes each batch, the heartbeat keeping those it has not started as it keeps the batcher's
// (batches taken from the batcher by take, which the heartbeat already holds);
// a batch that fails, or is not done within its budget, is nak'd, but for the messages it acknowledged, and written again at its redelivery.
func (g *Ingester) flush(ctx context.Context, batches []*Batch) error {
	defer g.hold(nil, nil)
	var errs []error
	for i, b := range batches {
		g.hold(b, batches[i+1:]) // b is never kept: its budget ends before its ack wait
		out, err := g.flushBatch(ctx, b)
		g.hold(nil, batches[i+1:])
		var refused *refusalError
		switch {
		case err != nil:
			errs = append(errs, err)
			g.settled(b.Entries[:out.Acked])
			if errors.As(err, &refused) && g.refused(b.Entries[out.Acked:]) >= writeFailureLimit {
				// Deterministic: the messages stay deferred for an operator (ingest.md §6.1 `write-failure`), parked when parking is on.
				g.Log.Warn("ingest: alert", "reason", ReasonWriteFailure, "capture", b.Key.Capture.String(), "hour", b.Key.Hour, "error", err)
				for _, e := range b.Entries[out.Acked:] {
					if g.Park {
						g.park(ctx, e, ReasonWriteFailure, 0)
					} else {
						g.deferMsg(e, ReasonWriteFailure, lastNakDelay)
					}
				}

				continue
			}
			for _, e := range b.Entries[out.Acked:] {
				_ = e.Msg.NakWithDelay(g.delay(0))
			}
		case out.Terminated:
			// Not a write failure: the redelivery is decided again and finds the termination (T-32).
			for _, e := range b.Entries {
				_ = e.Msg.NakWithDelay(g.delay(0))
			}
		case out.BeyondHorizon:
			for _, e := range b.Entries {
				g.act(ctx, e, Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}, 0)
			}
		default: // registered and acknowledged
			g.settled(b.Entries)
		}
	}
	err := errors.Join(errs...)
	if g.PassSettled != nil && len(batches) > 0 {
		g.PassSettled(g.Now(), batches, err)
	}

	return err
}

// flushBatch flushes b within its budget: a flush that has not registered and acknowledged by then is given up (ingest.md §6.2).
func (g *Ingester) flushBatch(ctx context.Context, b *Batch) (Flushed, error) {
	flushed := timing(g.Timed, "flush")
	defer flushed()
	if g.FlushTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.FlushTimeout)
		defer cancel()
	}

	return g.Flusher.Flush(ctx, b)
}

// hold records the batch a flush is writing and hands the batches it has not started to the heartbeat.
func (g *Ingester) hold(inFlight *Batch, flushing []*Batch) {
	g.mu.Lock()
	g.inFlight, g.flushing = inFlight, flushing
	g.mu.Unlock()
}

// settled resolves the entries a flush acknowledged.
func (g *Ingester) settled(entries []Entry) {
	for _, e := range entries {
		g.resolved(e)
	}
}

// resolved forgets e's deferral and its refusals: its message is acknowledged, terminated or parked, so no write of it is pending here.
func (g *Ingester) resolved(e Entry) {
	g.Metrics.Settled(e.D.deferralID())
	delete(g.writeFailures, e.D.Raw.Version)
}

// refused counts one more refusal by the Writer of each record version of entries and returns the highest count among them.
func (g *Ingester) refused(entries []Entry) int {
	if g.writeFailures == nil {
		g.writeFailures = map[Version]int{}
	}
	counted := make(map[Version]bool, len(entries))
	most := 0
	for _, e := range entries {
		v := e.D.Raw.Version
		if counted[v] { // equal copies are one record
			continue
		}
		counted[v] = true
		g.writeFailures[v]++
		most = max(most, g.writeFailures[v])
	}

	return most
}

// timing starts timing stage for seam, a test seam told each stage's wall time, and returns what reports it;
// with no seam it reads no clock.
func timing(seam func(stage string, took time.Duration), stage string) (done func()) {
	if seam == nil {
		return func() {}
	}
	start := time.Now()

	return func() { seam(stage, time.Since(start)) }
}

// readMessage reads a delivery's headers, body, stream sequence and delivery count.
func readMessage(m jetstream.Msg) (Message, error) {
	md, err := m.Metadata()
	if err != nil {
		return Message{}, err
	}
	h := m.Headers()
	env := tpbus.Envelope{
		Layout: h.Get(tpbus.HeaderLayout), MsgID: h.Get(tpbus.HeaderMsgID), Descriptor: h.Get(tpbus.HeaderDescriptor),
		PayloadRef: h.Get(tpbus.HeaderPayloadRef), PayloadDigest: h.Get(tpbus.HeaderPayloadDigest),
	}

	msg := Message{Env: env, Body: m.Data(), StreamSeq: int64(md.Sequence.Stream), Delivered: md.NumDelivered, Subject: m.Subject(), Header: h} //nolint:gosec // a stream sequence fits

	return msg, nil
}

// nakDelay backs off from five seconds to a minute as the message is redelivered.
func nakDelay(delivered uint64) time.Duration {
	d := firstNakDelay
	for i := uint64(1); i < delivered && d < lastNakDelay; i++ {
		d *= 2
	}

	return min(d, lastNakDelay)
}

// ObjectClaims reads claim objects from the claim bucket.
type ObjectClaims struct{ Store jetstream.ObjectStore }

// Get implements ClaimStore: it reads at most size+1 bytes, so an object longer than declared is found without being held,
// and a read whose ctx has no deadline is bounded by the bucket's own timeout.
func (o ObjectClaims) Get(ctx context.Context, name string, size int64) ([]byte, error) {
	r, err := o.Store.Get(ctx, name)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrClaimNotFound, name)
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	b := make([]byte, size+1)
	n, err := io.ReadFull(r, b)
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) { // the object's end, before size+1 bytes
		err = nil
	}
	if err != nil {
		return nil, err
	}

	return b[:n], nil
}
