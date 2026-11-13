package logfeed

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/metrics"
)

// releaseTimeout bounds giving a lease back, so a stalled catalog never holds a stopping stage.
const releaseTimeout = 5 * time.Second

// Stage is the log stage in the ingest role (logfeed.md §15.4, §15.5): it holds its share of the lease groups among the live replicas,
// taking free ones and giving back those past its share as replicas join, and runs each held group's converter on its group stream's consumer,
// re-created at every take. A group whose lease is lost stops at once; one whose converter gives up returns everything it holds and its lease,
// which any replica takes again after a pause.
type Stage struct {
	Catalog *catalog.Catalog
	// JS opens group n's stream, bus.FeedStreamName's.
	JS jetstream.JetStream
	// Conn is the streams' connection, which a held delivery's acknowledgement publishes on; nil keeps each delivery whole until acknowledged.
	Conn    *nats.Conn
	Feed    bus.Feed
	Replica string
	// LeaseTTL is a group lease's lifetime, renewed every third of it.
	LeaseTTL time.Duration
	// Base holds every group converter's settings; its lease, group and state are not used.
	Base *Converter
	// FetchBatch bounds one fetch; Pause is how long a group given up waits before this replica takes it again.
	FetchBatch int
	Pause      time.Duration
	Log        *slog.Logger

	mu sync.Mutex
	// held are the groups this replica holds; workers every group goroutine still running, held or not, which a stop waits for.
	held    map[int]*running
	workers map[*running]struct{}
	givenUp map[int]time.Time
	// balanced is the last balance's error, nil once one succeeded; running reports Run in progress;
	// starting are the held groups whose consumer is not fetching yet; failed is the last give-up's error, at failedAt.
	balanced    error
	running     bool
	starting    map[int]bool
	failed      error
	failedAt    time.Time
	failedGroup int
}

type running struct {
	group  int
	lease  catalog.Lease
	cancel context.CancelFunc
	done   chan struct{}
}

// Run balances the groups every third of the lease lifetime until ctx ends, then stops every group and waits for each.
func (s *Stage) Run(ctx context.Context) error {
	s.mu.Lock()
	s.held, s.workers, s.givenUp, s.starting = map[int]*running{}, map[*running]struct{}{}, map[int]time.Time{}, map[int]bool{}
	s.running, s.balanced = true, errors.New("logfeed: not balanced yet")
	s.mu.Unlock()
	tick := time.NewTicker(s.LeaseTTL / 3)
	defer tick.Stop()
	defer s.stopAll()
	for {
		s.balance(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// Ready is nil while Run balances the groups, every group it holds fetches from its consumer,
// and no group's converter gave up within the pause; a metrics.ReadyError names the group streams that are not ready.
func (s *Stage) Ready(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case !s.running:
		return errors.New("logfeed: the log stage is not running")
	case s.balanced != nil:
		return s.balanced
	case s.failed != nil && time.Since(s.failedAt) < s.Pause:
		return &metrics.ReadyError{Parts: []string{bus.FeedStreamName(s.Feed, s.failedGroup)}, Err: fmt.Errorf("logfeed: a lease group gave up: %w", s.failed)}
	}
	var starting []string
	for n := range s.held {
		if s.starting[n] {
			starting = append(starting, bus.FeedStreamName(s.Feed, n))
		}
	}
	if len(starting) > 0 {
		slices.Sort(starting)
		return &metrics.ReadyError{Parts: starting, Err: fmt.Errorf("logfeed: lease groups starting on %v", starting)}
	}

	return nil
}

// Held is the groups this replica holds now.
func (s *Stage) Held() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.held))
	for n := range s.held {
		out = append(out, n)
	}

	return out
}

// balance renews each held lease, gives back the groups past the replica's share, and takes free ones up to it,
// starting from a group of its own so replicas spread over the groups.
func (s *Stage) balance(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for n, r := range s.held {
		if err := s.Catalog.RenewLease(ctx, r.lease, s.LeaseTTL); err != nil {
			// Lost, or its worker ended and gave it back: the converter stops at once and acknowledges nothing more (§15.4).
			r.cancel()
			delete(s.held, n)
		}
	}
	live, err := s.Catalog.LiveReplicas(ctx)
	if err != nil {
		s.balanced = fmt.Errorf("logfeed: live replicas: %w", err)
		return
	}
	s.balanced = nil
	share := (s.Feed.Groups + max(live, 1) - 1) / max(live, 1)
	order := s.order()
	// Past the share: the groups furthest from the replica's own start go back, for a replica that joined to take.
	for i := len(order) - 1; i >= 0 && len(s.held) > share; i-- {
		if r, ok := s.held[order[i]]; ok {
			r.cancel()
			delete(s.held, order[i])
		}
	}
	for _, n := range order {
		if len(s.held) >= share {
			break
		}
		if _, ok := s.held[n]; ok || time.Since(s.givenUp[n]) < s.Pause {
			continue
		}
		l, ok, err := s.Catalog.TakeLease(ctx, catalog.LogGroupLease(n), s.Replica, s.LeaseTTL)
		if err != nil || !ok {
			continue
		}
		gctx, cancel := context.WithCancel(ctx)
		r := &running{group: n, lease: l, cancel: cancel, done: make(chan struct{})}
		s.held[n], s.workers[r], s.starting[n] = r, struct{}{}, true
		go s.runGroup(gctx, r)
	}
}

// order is every group, from the replica's own start.
func (s *Stage) order() []int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s.Replica))
	start := int(h.Sum32() % uint32(s.Feed.Groups)) //nolint:gosec // groups are few
	out := make([]int, s.Feed.Groups)
	for i := range out {
		out[i] = (start + i) % s.Feed.Groups
	}

	return out
}

func (s *Stage) stopAll() {
	s.mu.Lock()
	ws := make([]*running, 0, len(s.workers))
	for r := range s.workers {
		r.cancel()
		ws = append(ws, r)
	}
	s.held, s.running = map[int]*running{}, false
	s.mu.Unlock()
	for _, r := range ws {
		<-r.done
	}
}

func (s *Stage) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.DiscardHandler)
	}

	return s.Log
}

// runGroup runs r's group converter until ctx ends or the converter gives up (§15.5 *Taking a lease*):
// the group's open captures restored, the group's consumer deleted and re-created, so its deliveries come from the oldest message
// no one acknowledged, and a heartbeat beside it. Whatever ends it, it returns what it holds and the lease.
func (s *Stage) runGroup(ctx context.Context, r *running) {
	defer func() {
		s.mu.Lock()
		delete(s.workers, r)
		s.mu.Unlock()
		close(r.done)
	}()
	conv := s.Base.clone(r.lease, r.group)
	beat, stopBeat := context.WithCancel(ctx)
	var hb sync.WaitGroup
	hb.Go(func() { heartbeat(beat, conv) })
	err := s.convert(ctx, r.group, conv)
	stopBeat()
	hb.Wait()
	conv.Abandon()
	givenUp := err != nil && ctx.Err() == nil
	s.mu.Lock()
	delete(s.starting, r.group)
	if givenUp {
		s.log().Warn("logfeed: a lease group's converter gave up", "group", r.group, "error", err)
		// Before the release, so no balance takes the group back within the pause.
		s.givenUp[r.group], s.failedAt, s.failedGroup = time.Now(), time.Now(), r.group
		s.failed = fmt.Errorf("lease group %d on stream %s: %w", r.group, bus.FeedStreamName(s.Feed, r.group), err)
	}
	s.mu.Unlock()
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if err := s.Catalog.ReleaseLease(rctx, r.lease); err != nil && !errors.Is(err, catalog.ErrLeaseLost) {
		s.log().Warn("logfeed: release a lease group", "group", r.group, "error", err)
	}
}

// heartbeat keeps conv's held deliveries in progress every third of ack_wait until ctx ends, whatever conv's own goroutine is doing.
func heartbeat(ctx context.Context, conv *Converter) {
	every := conv.AckWait / 3
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			conv.Heartbeat()
		}
	}
}

// convert restores the group's open captures, re-creates its consumer and feeds the deliveries to conv,
// ticking it every tenth of the reorder window, until ctx ends or a step fails.
func (s *Stage) convert(ctx context.Context, n int, conv *Converter) error {
	opens, err := s.Catalog.OpenLogCaptures(ctx, n, s.Feed.Groups, n == 0)
	if err != nil {
		return err
	}
	if err := conv.Load(opens); err != nil {
		return err
	}
	stream := bus.FeedStreamName(s.Feed, n)
	if err := s.JS.DeleteConsumer(ctx, stream, bus.FeedConsumer(n)); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return err
	}
	cons, err := s.JS.CreateOrUpdateConsumer(ctx, stream, bus.FeedConsumerConfig(s.Feed, n))
	if err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.starting, n)
	s.mu.Unlock()
	tick := max(conv.Reorder/10, 50*time.Millisecond)
	for ctx.Err() == nil {
		// The server ends each pull at tick, and every message it sent is read: a pull cut short by the client's context
		// would drop what was already on its way, redelivered only after ack_wait and then stale (logfeed.md §15.5).
		batch, err := cons.Fetch(s.FetchBatch, jetstream.FetchMaxWait(tick))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}
		for m := range batch.Messages() {
			var d Msg = m
			if s.Conn != nil {
				d = delivery{Msg: m, nc: s.Conn}
			}
			if err := conv.Receive(ctx, d, conv.Now()); err != nil {
				return err
			}
		}
		berr := batch.Error()
		if berr != nil && !errors.Is(berr, jetstream.ErrNoMessages) && !errors.Is(berr, nats.ErrTimeout) && ctx.Err() == nil {
			return berr
		}
		if err := conv.Tick(ctx, conv.Now()); err != nil {
			return err
		}
	}

	return nil
}

// delivery is a feed message whose held acknowledgement keeps only its reply subject, not its payload (logfeed.md §15.7 *Held entries keep no payload*).
type delivery struct {
	jetstream.Msg
	nc *nats.Conn
}

func (d delivery) Handle() Acker { return replyAck{subject: d.Reply(), nc: d.nc} }

// replyAck acknowledges a delivery by its reply subject, as jetstream.Msg does: each is one publish of JetStream's acknowledgement.
type replyAck struct {
	subject string
	nc      *nats.Conn
}

func (a replyAck) Ack() error        { return a.nc.Publish(a.subject, []byte("+ACK")) }
func (a replyAck) Nak() error        { return a.nc.Publish(a.subject, []byte("-NAK")) }
func (a replyAck) InProgress() error { return a.nc.Publish(a.subject, []byte("+WPI")) }
