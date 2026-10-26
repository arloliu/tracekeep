package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// Nak delays: a deferral backs off from the first to the last as the message is redelivered (ingest.md §6.1).
const (
	firstNakDelay = 5 * time.Second
	lastNakDelay  = time.Minute
)

// Ingester is one ingest replica: it holds a membership in the activation gate,
// pulls the traffic stream, decides each delivery, batches and flushes (rollouts.md §6.4, ingest.md §6).
// It is driven by Step, which Run calls in a loop; Step starts no goroutine of its own.
type Ingester struct {
	Executor *Executor
	Batcher  *Batcher
	Flusher  *Flusher
	Consumer jetstream.Consumer
	Log      *slog.Logger
	// Replica names this replica's membership.
	Replica string
	// Lease is the membership lease, renewed before every fetch; a fetch waits at most what remains of it.
	Lease time.Duration
	// FetchBatch bounds the messages of one fetch.
	FetchBatch int
	// MaxWait bounds how long a fetch waits for FetchBatch messages; it never waits past what remains of the lease.
	MaxWait time.Duration
	// Now is the clock the batches and the lease are judged by.
	Now func() time.Time

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

// Run steps until ctx ends, then flushes what it holds.
func (g *Ingester) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := g.Step(ctx); err != nil {
			g.Log.Warn("ingest: step failed", "error", err)
		}
	}

	return g.flush(context.WithoutCancel(ctx), g.Batcher.All())
}

// Step renews the membership, fetches one batch of messages within the lease, disposes of each,
// keeps the held ones in progress and flushes the batches that are due.
func (g *Ingester) Step(ctx context.Context) error {
	if err := g.renew(ctx); err != nil {
		return err
	}
	wait := min(g.MaxWait, g.leaseEnd.Sub(g.Now())/2)
	batch, err := g.Consumer.Fetch(g.FetchBatch, jetstream.FetchMaxWait(max(wait, time.Millisecond)))
	if err != nil {
		return fmt.Errorf("ingest: fetch: %w", err)
	}
	generation := g.membership.Generation
	for m := range batch.Messages() {
		g.handle(ctx, m, generation)
	}
	if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) && !errors.Is(err, context.DeadlineExceeded) {
		g.Log.Warn("ingest: fetch ended", "error", err)
	}
	g.Batcher.Held(func(m Msg) { _ = m.InProgress() })

	return g.flush(ctx, g.Batcher.Due(g.Now()))
}

// renew renews the membership, or registers again once it lapsed.
func (g *Ingester) renew(ctx context.Context) error {
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

// handle disposes of one delivery pulled under membership generation:
// a message received after the lease lapsed, or under an earlier membership, is nak'd untouched (rollouts.md §6.4).
func (g *Ingester) handle(ctx context.Context, m jetstream.Msg, generation int64) {
	if !g.Now().Before(g.leaseEnd) || generation != g.membership.Generation {
		_ = m.NakWithDelay(firstNakDelay)
		return
	}
	msg, delivered, err := readMessage(m)
	if err != nil {
		_ = m.NakWithDelay(firstNakDelay)
		return
	}
	d, dec, err := g.Executor.Dispose(ctx, msg)
	if err != nil {
		g.Log.Warn("ingest: disposition failed", "msg_id", msg.Env.MsgID, "error", err)
		_ = m.NakWithDelay(nakDelay(delivered))

		return
	}
	g.act(ctx, Entry{D: d, Msg: m}, dec, delivered)
}

// act carries out a final disposition.
func (g *Ingester) act(ctx context.Context, e Entry, dec Decision, delivered uint64) {
	if dec.Alert {
		g.Log.Warn("ingest: alert", "reason", dec.Reason, "msg_id", e.D.Msg.Env.MsgID)
	}
	switch dec.Action {
	case ActBatch:
		g.Batcher.Add(e, g.Now())
	case ActAck:
		if err := ack(ctx, e); err != nil {
			g.Log.Warn("ingest: ack failed", "msg_id", e.D.Msg.Env.MsgID, "error", err)
		}
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

// flush flushes each batch; a batch that fails is nak'd whole and written again at its redelivery.
func (g *Ingester) flush(ctx context.Context, batches []*Batch) error {
	var errs []error
	for _, b := range batches {
		out, err := g.Flusher.Flush(ctx, b)
		switch {
		case err != nil:
			errs = append(errs, err)
			for _, e := range b.Entries {
				_ = e.Msg.NakWithDelay(firstNakDelay)
			}
		case out.BeyondHorizon:
			for _, e := range b.Entries {
				g.act(ctx, e, Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}, 0)
			}
		default: // registered and acknowledged
		}
	}

	return errors.Join(errs...)
}

// readMessage reads a delivery's headers, body and stream sequence.
func readMessage(m jetstream.Msg) (Message, uint64, error) {
	md, err := m.Metadata()
	if err != nil {
		return Message{}, 0, err
	}
	h := m.Headers()
	env := tracepack.BusEnvelope{
		Layout: h.Get(tracepack.BusHeaderLayout), MsgID: h.Get(tracepack.BusHeaderMsgID), Descriptor: h.Get(tracepack.BusHeaderDescriptor),
		PayloadRef: h.Get(tracepack.BusHeaderPayloadRef), PayloadDigest: h.Get(tracepack.BusHeaderPayloadDigest),
	}

	return Message{Env: env, Body: m.Data(), StreamSeq: int64(md.Sequence.Stream)}, md.NumDelivered, nil //nolint:gosec // a stream sequence fits
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
