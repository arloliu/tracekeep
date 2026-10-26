package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
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
// and Run runs one more, which keeps the held messages in progress.
type Ingester struct {
	Executor *Executor
	Batcher  *Batcher
	Flusher  *Flusher
	// Coordinator parks the deferrals that cannot clear by themselves and acknowledges every resolved version.
	Coordinator *Coordinator
	// Park enables parking (parking.enabled); without it an indefinite deferral is nak'd like any other,
	// and the consumer's acknowledgement budget may fill.
	Park     bool
	Consumer jetstream.Consumer
	// Stream is the traffic stream, checked against the catalog's incarnation with every renewal.
	Stream jetstream.Stream
	Log    *slog.Logger
	// Replica names this replica's membership.
	Replica string
	// Lease is the membership lease, renewed before every fetch; a fetch waits at most what remains of it.
	Lease time.Duration
	// FetchBatch bounds the messages of one fetch.
	FetchBatch int
	// MaxWait bounds how long a fetch waits for FetchBatch messages; it never waits past what remains of the lease.
	MaxWait time.Duration
	// Heartbeat is the consumer's ack wait over three: while Run runs, every held message,
	// those of a flush in progress included, is kept in progress this often (architecture.md §3.1).
	Heartbeat time.Duration
	// Now is the clock the batches and the lease are judged by.
	Now func() time.Time

	// writeFailures counts, by batch, the flushes the Writer refused: the same batch refused three times is deterministic.
	writeFailures map[string]int
	// mu guards the batcher and flushing against the heartbeat.
	mu sync.Mutex
	// flushing are the messages of the batches a flush took from the batcher and has not finished,
	// copied out, since a flush sorts its batch's entries.
	flushing []Msg

	membership catalog.Membership
	// leaseEnd is when this replica stops trusting its lease: the renewal's start plus the lease,
	// before the database's own end, which the renewal set later.
	leaseEnd time.Time
}

// ConfigContent is a processing configuration as the catalog stores it: the same configuration gives the same bytes on every replica.
type ConfigContent struct {
	Classifier  string `json:"classifier"`
	MaxFrameLen uint64 `json:"max_frame_len"`
}

// Activate records the configurations this binary executes and returns them by id, for the executor and the flusher.
func Activate(ctx context.Context, cat *catalog.Catalog, configs []Config) (map[int64]Config, error) {
	byID := make(map[int64]Config, len(configs))
	for _, c := range configs {
		content, err := json.Marshal(ConfigContent{Classifier: c.Name, MaxFrameLen: c.MaxFrameLen})
		if err != nil {
			return nil, err
		}
		id, err := cat.ActivateConfig(ctx, content)
		if err != nil {
			return nil, err
		}
		byID[id] = c
	}

	return byID, nil
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

// Run steps until ctx ends, then flushes what it holds;
// a stream that is no longer its incarnation ends it at once, the held deliveries abandoned unacknowledged.
func (g *Ingester) Run(ctx context.Context) error {
	if g.Heartbeat <= 0 {
		return errors.New("ingest: no heartbeat")
	}
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
	for ctx.Err() == nil {
		err := g.Step(ctx)
		switch {
		case errors.Is(err, catalog.ErrIncarnationChanged):
			return err
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

	return g.flush(context.WithoutCancel(ctx), g.take(g.Batcher.All))
}

// Step renews the membership, fetches one batch of messages within the lease, disposes of each,
// keeps the held ones in progress and flushes the batches that are due.
// It returns the renewal's and the fetch's errors, a pull the server ended with an error included,
// after disposing of what it delivered; a failed flush's batch is nak'd and logged.
func (g *Ingester) Step(ctx context.Context) error {
	if err := g.renew(ctx); err != nil {
		if errors.Is(err, catalog.ErrIncarnationChanged) {
			g.Log.Error("ingest: alert", "reason", ReasonIncarnationChanged, "error", err)
			g.mu.Lock()
			g.Batcher.All() // abandoned unacknowledged (parking.md *incarnations*)
			g.mu.Unlock()
		}

		return err
	}
	var fetchErr error
	if wait := min(g.MaxWait, g.leaseEnd.Sub(g.Now())/2); wait >= time.Millisecond { // otherwise no lease is left to fetch within
		batch, err := g.Consumer.Fetch(g.FetchBatch, jetstream.FetchMaxWait(wait))
		if err != nil {
			return fmt.Errorf("ingest: fetch: %w", err)
		}
		generation := g.membership.Generation
		for m := range batch.Messages() {
			g.handle(ctx, m, generation)
		}
		if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) && !errors.Is(err, context.DeadlineExceeded) {
			fetchErr = fmt.Errorf("ingest: fetch ended: %w", err)
		}
	}
	now := g.Now()
	due := g.take(func() []*Batch { return g.Batcher.Due(now) })
	if err := g.flush(ctx, due); err != nil {
		g.Log.Warn("ingest: flush failed", "error", err)
	}

	return fetchErr
}

// take removes batches from the batcher with remove and hands their messages to the heartbeat in one step,
// so no tick finds them in neither place.
func (g *Ingester) take(remove func() []*Batch) []*Batch {
	g.mu.Lock()
	defer g.mu.Unlock()
	batches := remove()
	g.flushing = messages(batches)

	return batches
}

// messages are the messages of batches' entries.
func messages(batches []*Batch) []Msg {
	var msgs []Msg
	for _, b := range batches {
		for _, e := range b.Entries {
			msgs = append(msgs, e.Msg)
		}
	}

	return msgs
}

// heartbeats keeps the held messages in progress at every tick until ctx ends.
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

// beat keeps every held message in progress: those the batcher holds and those of the batches a flush has not finished.
// One racing its message's acknowledgement is harmless: the server ignores progress on a message no longer pending.
func (g *Ingester) beat() {
	g.mu.Lock()
	var msgs []Msg
	g.Batcher.Held(func(m Msg) { msgs = append(msgs, m) })
	msgs = append(msgs, g.flushing...)
	g.mu.Unlock()
	for _, m := range msgs {
		_ = m.InProgress()
	}
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

// checkIncarnation verifies that the traffic stream is still the incarnation the consumer was named for:
// its creation time, and a last sequence not below one observed (parking.md *incarnations*).
func (g *Ingester) checkIncarnation(ctx context.Context) error {
	inc, err := bus.Incarnate(ctx, g.Executor.Catalog, catalog.RoleTraffic, g.Stream)
	if err != nil {
		return err
	}
	if inc.ID != g.Executor.Incarnation {
		return fmt.Errorf("%w: incarnation %d, the consumer is named for %d", catalog.ErrIncarnationChanged, inc.ID, g.Executor.Incarnation)
	}

	return nil
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
	d, dec, err := g.Executor.Dispose(ctx, msg)
	if err != nil {
		g.Log.Warn("ingest: disposition failed", "msg_id", msg.Env.MsgID, "error", err)
		_ = m.NakWithDelay(nakDelay(msg.Delivered))

		return
	}
	g.act(ctx, Entry{D: d, Msg: m}, dec, msg.Delivered)
}

// act carries out a final disposition.
func (g *Ingester) act(ctx context.Context, e Entry, dec Decision, delivered uint64) {
	if dec.Alert {
		g.Log.Warn("ingest: alert", "reason", dec.Reason, "msg_id", e.D.Msg.Env.MsgID)
	}
	switch dec.Action {
	case ActBatch:
		g.mu.Lock()
		g.Batcher.Add(e, g.Now())
		g.mu.Unlock()
	case ActAck:
		g.acknowledge(ctx, e, dec.Reason == ReasonTerminated)
	case ActPark:
		g.park(ctx, e, dec.Reason, delivered)
	case ActTerminate:
		if dec.Audit {
			if err := g.Executor.AuditTermination(ctx, e.D, dec.Reason); err != nil {
				// No audit row, no termination: the message is decided again at its redelivery.
				_ = e.Msg.NakWithDelay(nakDelay(delivered))
				return
			}
		}
		_ = e.Msg.Term()
	case ActNak:
		_ = e.Msg.NakWithDelay(nakDelay(delivered))
	default:
		panic("ingest: unknown action")
	}
}

// acknowledge acknowledges a message of a resolved version through the coordinator, under the termination when terminated.
func (g *Ingester) acknowledge(ctx context.Context, e Entry, terminated bool) {
	if err := g.Coordinator.Acknowledge(ctx, e.D, e.Msg, terminated); err != nil {
		// Unacknowledged: the message returns and is acknowledged again under the same resolution.
		g.Log.Warn("ingest: ack failed", "msg_id", e.D.Msg.Env.MsgID, "error", err)
	}
}

// park moves an indefinite deferral to the parking stream and acts on what the coordinator found:
// a version past the horizon is terminated, a resolved one acknowledged, an unconfirmed copy's original nak'd to be parked again.
// Without parking, the deferral is nak'd like any other.
func (g *Ingester) park(ctx context.Context, e Entry, reason string, delivered uint64) {
	if !g.Park {
		_ = e.Msg.NakWithDelay(nakDelay(delivered))
		return
	}
	p, err := g.Coordinator.Park(ctx, e, reason, HourOf(g.Executor.Horizon()))
	switch {
	case err != nil:
		g.Log.Warn("ingest: parking failed", "msg_id", e.D.Msg.Env.MsgID, "error", err)
		_ = e.Msg.NakWithDelay(nakDelay(delivered))
	case p.Begun.BeyondRetention:
		g.act(ctx, e, Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}, delivered)
	case p.Begun.State.Resolved():
		g.acknowledge(ctx, e, p.Begun.State == catalog.VersionTerminated)
	case p.Confirmation == Suppressed:
		_ = e.Msg.NakWithDelay(suppressedNakDelay)
	case p.Confirmation != Confirmed:
		_ = e.Msg.NakWithDelay(nakDelay(delivered))
	default: // parked: the original was acknowledged
	}
}

// flush flushes each batch, the heartbeat keeping those it has not finished in progress
// (batches taken from the batcher by take, whose messages the heartbeat already holds);
// a batch that fails is nak'd, but for the messages it acknowledged, and written again at its redelivery.
func (g *Ingester) flush(ctx context.Context, batches []*Batch) error {
	defer func() {
		g.mu.Lock()
		g.flushing = nil
		g.mu.Unlock()
	}()
	var errs []error
	for i, b := range batches {
		msgs := messages(batches[i:])
		g.mu.Lock()
		g.flushing = msgs
		g.mu.Unlock()
		out, err := g.Flusher.Flush(ctx, b)
		var refused *refusalError
		switch {
		case err != nil:
			errs = append(errs, err)
			if errors.As(err, &refused) && g.refused(refused.batch) >= writeFailureLimit {
				// Deterministic: the messages stay deferred for an operator (ingest.md §6.1 `write-failure`), parked when parking is on.
				g.Log.Warn("ingest: alert", "reason", ReasonWriteFailure, "capture", b.Key.Capture.String(), "hour", b.Key.Hour, "error", err)
				for _, e := range b.Entries[out.Acked:] {
					if g.Park {
						g.park(ctx, e, ReasonWriteFailure, 0)
					} else {
						_ = e.Msg.NakWithDelay(lastNakDelay)
					}
				}

				continue
			}
			for _, e := range b.Entries[out.Acked:] {
				_ = e.Msg.NakWithDelay(firstNakDelay)
			}
		case out.Terminated:
			// Not a write failure: the redelivery is decided again and finds the termination (T-32).
			for _, e := range b.Entries {
				_ = e.Msg.NakWithDelay(firstNakDelay)
			}
		case out.BeyondHorizon:
			for _, e := range b.Entries {
				g.act(ctx, e, Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}, 0)
			}
		default: // registered and acknowledged
			delete(g.writeFailures, batchName(b))
		}
	}

	return errors.Join(errs...)
}

// refused counts one more refusal of the batch named batch by the Writer and returns the count.
func (g *Ingester) refused(batch string) int {
	if g.writeFailures == nil {
		g.writeFailures = map[string]int{}
	}
	g.writeFailures[batch]++

	return g.writeFailures[batch]
}

// readMessage reads a delivery's headers, body, stream sequence and delivery count.
func readMessage(m jetstream.Msg) (Message, error) {
	md, err := m.Metadata()
	if err != nil {
		return Message{}, err
	}
	h := m.Headers()
	env := tracepack.BusEnvelope{
		Layout: h.Get(tracepack.BusHeaderLayout), MsgID: h.Get(tracepack.BusHeaderMsgID), Descriptor: h.Get(tracepack.BusHeaderDescriptor),
		PayloadRef: h.Get(tracepack.BusHeaderPayloadRef), PayloadDigest: h.Get(tracepack.BusHeaderPayloadDigest),
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

// Get implements ClaimStore.
func (o ObjectClaims) Get(ctx context.Context, name string) ([]byte, error) {
	b, err := o.Store.GetBytes(ctx, name)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrClaimNotFound, name)
	}

	return b, err
}
