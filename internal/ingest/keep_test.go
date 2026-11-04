package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// watchedConsumer hands out its consumer's deliveries wrapped, so a test sees, by subject, the in-progress acknowledgements sent,
// the redeliveries received and each acknowledgement's age since its ack wait last started, its fetch or its last in-progress ack.
type watchedConsumer struct {
	jetstream.Consumer

	mu          sync.Mutex
	inProgress  map[string]int
	redelivered int
	oldest      time.Duration
	acked       map[uint64]bool
}

func watch(c jetstream.Consumer) *watchedConsumer {
	return &watchedConsumer{Consumer: c, inProgress: map[string]int{}, acked: map[uint64]bool{}}
}

func (c *watchedConsumer) Fetch(n int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	b, err := c.Consumer.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	out := make(chan jetstream.Msg)
	go func() {
		defer close(out)
		for m := range b.Messages() {
			if md, err := m.Metadata(); err == nil && md.NumDelivered > 1 {
				c.mu.Lock()
				c.redelivered++
				c.mu.Unlock()
			}
			out <- &watchedMsg{Msg: m, c: c, since: time.Now()}
		}
	}()

	return watchedBatch{ch: out, b: b}, nil
}

// counts are the in-progress acknowledgements sent, by subject, the redeliveries and the oldest acknowledgement's age.
func (c *watchedConsumer) counts() (map[string]int, int, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ip := make(map[string]int, len(c.inProgress))
	maps.Copy(ip, c.inProgress)

	return ip, c.redelivered, c.oldest
}

type watchedBatch struct {
	ch <-chan jetstream.Msg
	b  jetstream.MessageBatch
}

func (w watchedBatch) Messages() <-chan jetstream.Msg { return w.ch }
func (w watchedBatch) Error() error                   { return w.b.Error() }

type watchedMsg struct {
	jetstream.Msg
	c     *watchedConsumer
	since time.Time // guarded by c.mu
}

func (m *watchedMsg) InProgress() error {
	m.c.mu.Lock()
	m.c.inProgress[m.Subject()]++
	m.since = time.Now()
	m.c.mu.Unlock()

	return m.Msg.InProgress()
}

func (m *watchedMsg) acked() {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	m.c.oldest = max(m.c.oldest, time.Since(m.since))
	if md, err := m.Metadata(); err == nil {
		m.c.acked[md.Sequence.Stream] = true
	}
}

func (m *watchedMsg) Ack() error {
	m.acked()
	return m.Msg.Ack()
}

func (m *watchedMsg) DoubleAck(ctx context.Context) error {
	m.acked()
	return m.Msg.DoubleAck(ctx)
}

// slowPut delays every put, within the put's context.
type slowPut struct {
	store.ObjectStore
	delay time.Duration
}

func (s slowPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	t := time.NewTimer(s.delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}

	return s.ObjectStore.Put(ctx, key, r, size)
}

// keepTimed runs r on the wall clock at scaled timing: the consumer's ack wait, a batch's flush interval and fetch wait,
// a flush's budget and the heartbeat's tick, the consumer's deliveries watched.
func keepTimed(t *testing.T, r *replica, ackWait, flush, fetchWait, budget, tick time.Duration) *watchedConsumer {
	t.Helper()
	cons, err := bus.Consumer(t.Context(), r.stream, r.g.Incarnation, bus.ConsumerConfig{AckWait: ackWait, MaxAckPending: 1000})
	if err != nil {
		t.Fatal(err)
	}
	w := watch(cons)
	g := r.g
	g.Consumer, g.Now, g.MaxWait, g.Heartbeat = w, time.Now, fetchWait, tick
	g.Batcher = NewBatcher(flush, 1<<20)
	setBudgets(g, ackWait, budget)
	if err := g.Join(t.Context()); err != nil { // the lease again, on the wall clock
		t.Fatal(err)
	}

	return w
}

// setBudgets gives g the consumer's ack wait and a flush's own budget.
func setBudgets(g *Ingester, ackWait, budget time.Duration) {
	g.AckWait, g.FlushTimeout = ackWait, budget
}

// running runs g until the returned stop is called, which returns Run's error.
func running(t *testing.T, g *Ingester) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()

	return func() error { cancel(); return <-done }
}

// Over several flush periods at a healthy store, every delivery is acknowledged within the consumer's ack wait
// with no in-progress acknowledgement and no redelivery: the ack wait exceeds a routine delivery's life,
// the flush interval, a fetch's wait and a flush's budget (architecture.md §3.1, ingest.md §6.2).
func TestRoutineDeliveriesNeedNoProgress(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		const ackWait, flush = 2600 * time.Millisecond, time.Second
		w := keepTimed(t, r, ackWait, flush, 100*time.Millisecond, 500*time.Millisecond, 250*time.Millisecond)
		captures := []tracepack.UUID{{0x4a, 1}, {0x4a, 2}}
		for _, capture := range captures {
			r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		}
		stop := running(t, r.g)
		const records = 80
		for i := range records {
			seq := uint64(i/len(captures) + 1) //nolint:gosec // a small index
			r.publish(t, inline(t, captures[i%len(captures)], dataRecord(seq, now+int64(seq)), nil), nil)
			time.Sleep(5 * flush / records) // the scenario rate: five flush periods of traffic
		}
		if left := r.settled(t); left != 0 {
			t.Errorf("%d messages left on the stream, want every one acknowledged", left)
		}
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		ip, redelivered, oldest := w.counts()
		if len(ip) != 0 || redelivered != 0 {
			t.Errorf("in-progress acknowledgements %v and %d redeliveries, want none of either", ip, redelivered)
		}
		if oldest == 0 || oldest >= ackWait {
			t.Errorf("the oldest acknowledgement came %s after its delivery, want within the ack wait %s", oldest, ackWait)
		}
	})
}

// Behind a slow store, a pass of due batches outlasts a routine delivery's life:
// the batches still queued are kept in progress, so none is redelivered, and every acknowledgement comes within its ack wait;
// a batch whose flush started is never kept, so the pass's first batch, flushed within that life, is never kept at all.
func TestQueuedBatchesKeptBehindASlowStore(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		const ackWait, budget, put = 3 * time.Second, time.Second, 900 * time.Millisecond
		w := keepTimed(t, r, ackWait, time.Second, 100*time.Millisecond, budget, 250*time.Millisecond)
		r.g.Flusher.Store = slowPut{ObjectStore: r.st, delay: put}
		subjects := map[tracepack.UUID]string{}
		for i := range 4 {
			capture := tracepack.UUID{0x4b, byte(i)}
			for _, m := range traffic(t, capture) {
				r.publish(t, m, nil)
			}
			subjects[capture] = subjectOf(t, capture)
		}
		stop := running(t, r.g)
		if left := r.settled(t); left != 0 {
			t.Errorf("%d messages left on the stream, want every one acknowledged", left)
		}
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		ip, redelivered, oldest := w.counts()
		if redelivered != 0 || oldest >= ackWait {
			t.Errorf("%d redeliveries, the oldest acknowledgement %s after its ack wait started; want none, within %s", redelivered, oldest, ackWait)
		}
		never := 0
		for _, s := range subjects {
			if ip[s] == 0 {
				never++
			}
		}
		if never != 1 || len(ip) == 0 {
			t.Errorf("in-progress acknowledgements by subject %v: want the queued batches kept and only the first flushed never", ip)
		}
	})
}

// Local work that outlasts both the flush's budget and the ack wait is still waited for, and its batch is never kept:
// its deliveries are redelivered, to another replica here, written again and acknowledged only after that registration;
// the stalled flush owns its working file until its work returns, registers nothing, and its late naks lose nothing.
func TestLocalWorkOutlastingBothBudgets(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		const ackWait = time.Second
		w := keepTimed(t, r, ackWait, 0, 100*time.Millisecond, 300*time.Millisecond, 100*time.Millisecond)
		g1 := r.g
		writing, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		releaseOnce := func() { once.Do(func() { close(release) }) }
		t.Cleanup(releaseOnce) // a failed test still lets the stalled flush return
		g1.Flusher.hooks[hookWrite] = func() error {
			close(writing)
			<-release // local work outlasting both budgets
			return nil
		}
		settled := make(chan error, 1)
		g1.PassSettled = func(_ time.Time, _ []*Batch, err error) { settled <- err }

		f2 := *g1.Flusher
		f2.WorkDir, f2.hooks = t.TempDir(), [numFlushHooks]func() error{}
		registered := false
		f2.hooks[hookAfterRegister] = func() error { registered = true; return nil }
		f2.hooks[hookAfterAck] = func() error {
			if !registered {
				t.Error("a redelivery acknowledged before its registration")
			}
			return nil
		}
		g2 := &Ingester{
			Executor: g1.Executor, Batcher: NewBatcher(0, 1<<20), Flusher: &f2, Consumer: w, Stream: g1.Stream, Log: slog.New(slog.DiscardHandler),
			Coordinator: g1.Coordinator, Park: true, Incarnation: g1.Incarnation, Replica: "r2", Lease: time.Minute, FetchBatch: 100,
			MaxWait: 100 * time.Millisecond, Heartbeat: g1.Heartbeat, Now: time.Now,
		}
		setBudgets(g2, ackWait, 300*time.Millisecond)
		if err := g2.Join(ctx); err != nil {
			t.Fatal(err)
		}
		capture := tracepack.UUID{0x4c}
		msgs := traffic(t, capture)
		for _, m := range msgs {
			r.publish(t, m, nil)
		}
		stop := running(t, g1)
		<-writing
		deadline := time.Now().Add(ackWait + 5*time.Second)
		for !registered && time.Now().Before(deadline) {
			if err := g2.Step(ctx); err != nil {
				t.Fatal(err)
			}
		}
		_, redelivered, _ := w.counts()
		if !registered || redelivered != len(msgs) {
			t.Fatalf("registered %v after %d redeliveries, want the %d deliveries of the stalled flush redelivered and registered", registered, redelivered, len(msgs))
		}
		files, err := filepath.Glob(filepath.Join(g1.Flusher.WorkDir, workFilePattern))
		if err != nil || len(files) != 1 {
			t.Errorf("the stalled flush's working files %v, %v; want its one file owned until its work returns", files, err)
		}
		releaseOnce()
		if err := <-settled; !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the stalled flush = %v, want its budget's deadline", err)
		}
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if files, _ := filepath.Glob(filepath.Join(g1.Flusher.WorkDir, workFilePattern)); len(files) != 0 {
			t.Errorf("working files %v left once the stalled flush returned", files)
		}
		if keys := storedKeys(t, r.st); len(keys) != 1 {
			t.Errorf("packs %q, want the redelivery's one segment and nothing of the stalled flush", keys)
		}
		if ip, _, _ := w.counts(); len(ip) != 0 {
			t.Errorf("in-progress acknowledgements %v, want none: neither flush's batch is kept", ip)
		}
		if left := r.settled(t); left != 0 {
			t.Errorf("%d messages left on the stream after the stalled flush's naks, want none", left)
		}
	})
}

// subjectOf is capture's subject on the traffic stream.
func subjectOf(t *testing.T, capture tracepack.UUID) string {
	t.Helper()
	s, err := tpbus.Subject(prefix, "T1", capture)
	if err != nil {
		t.Fatal(err)
	}

	return s
}
