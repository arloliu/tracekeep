package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

// guard ends a double's wait that no context ended, so a missing bound fails the test instead of hanging it.
const guard = 5 * time.Second

// bound is the ack wait or stop budget the tests give a flush; only its end matters, never its length.
const bound = 100 * time.Millisecond

var errNotEnded = errors.New("not ended by its context")

// stallingPut is a store whose puts never answer: each returns only once its context ends.
// A durable one makes the object first, so what is lost is the answer.
type stallingPut struct {
	store.ObjectStore
	durable bool

	mu          sync.Mutex
	puts, ended int
	deletes     int
}

func (s *stallingPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if s.durable {
		if err := s.ObjectStore.Put(context.WithoutCancel(ctx), key, r, size); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.puts++
	s.mu.Unlock()
	t := time.NewTimer(guard)
	defer t.Stop()
	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.ended++
		s.mu.Unlock()
		return ctx.Err()
	case <-t.C:
		return errNotEnded
	}
}

func (s *stallingPut) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deletes++
	s.mu.Unlock()

	return s.ObjectStore.Delete(ctx, key)
}

func (s *stallingPut) counts() (puts, ended, deletes int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.puts, s.ended, s.deletes
}

// stallingAck is a message whose confirmed acknowledgement never answers, returning only once its context ends;
// entered, when set, is told the acknowledgement is waiting.
type stallingAck struct {
	*msg
	entered func()
}

func (m stallingAck) DoubleAck(ctx context.Context) error {
	if m.entered != nil {
		m.entered()
	}
	t := time.NewTimer(guard)
	defer t.Stop()
	select {
	case <-ctx.Done():
		_ = m.do("double-ack ended")
		return ctx.Err()
	case <-t.C:
		return errNotEnded
	}
}

// expiring is a context whose deadline the test passes by calling expire: Done closes and Err is the deadline's.
// It ends a flush at the call the test stalls, however long the work before that call takes on the engine.
type expiring struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func newExpiring(parent context.Context) *expiring {
	return &expiring{Context: parent, done: make(chan struct{})}
}

func (e *expiring) expire()               { e.once.Do(func() { close(e.done) }) }
func (e *expiring) Done() <-chan struct{} { return e.done }

func (e *expiring) Err() error {
	select {
	case <-e.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

// add disposes m, which must be batched, and batches it with the handle h.
func (p *pipeline) add(t *testing.T, m Message, h Msg) {
	t.Helper()
	d, dec := dispose(t, p.x, m)
	if dec.Action != ActBatch {
		t.Fatalf("seq %d disposed %+v, want batched", d.Raw.Version.Seq, dec)
	}
	p.b.Add(Entry{D: d, Msg: h}, time.Unix(0, now))
}

// lockCapture holds the capture's row in a transaction of its own on the catalog's writer, so every write of the capture waits,
// on SQLite for the one write connection, on MariaDB for the row; release ends it.
func lockCapture(t *testing.T, c *catalog.Catalog, capture tracepack.UUID) (release func()) {
	t.Helper()
	w, _ := c.Pools()
	tx, err := w.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE captures SET data_revision = data_revision WHERE capture_id = ?`, capture[:]); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _ = tx.Rollback() }) }
	timer := time.AfterFunc(guard, release) // the guard: a registration no context ends goes on after it
	t.Cleanup(func() { timer.Stop() })
	t.Cleanup(release)

	return release
}

// A flush whose remote call never answers is given up at its deadline, each call ended by its context in turn:
// the store's put, a put made durable whose answer is lost, the catalog's registration at the ack wait,
// and a confirmed acknowledgement at a deadline passed once it waits.
// Whatever the outcome, nothing is acknowledged before its registration, a message not acknowledged is nak'd,
// and an object the flush may have put is kept for the reconciler, never deleted by the flush (ingest.md §6.2).
func TestFlushRemoteCallsEndWithTheirContext(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		nak := []string{"nak " + firstNakDelay.String()}
		for _, tc := range []struct {
			name string
			// stall sets the stall up once the batch is held, and returns the store the flush puts to when it is not p's.
			stall func(t *testing.T, p *pipeline, capture tracepack.UUID) *stallingPut
			// claimCheck acknowledges by DoubleAck, and the second message's never answers.
			claimCheck bool
			objects    int
			registered bool
			want       [][]string // each message's disposition, in seq order
		}{
			{
				name:  "put",
				stall: func(_ *testing.T, p *pipeline, _ tracepack.UUID) *stallingPut { return &stallingPut{ObjectStore: p.st} },
				want:  [][]string{nak, nak, nak, nak},
			},
			{
				name: "durable put whose answer is lost",
				stall: func(_ *testing.T, p *pipeline, _ tracepack.UUID) *stallingPut {
					return &stallingPut{ObjectStore: p.st, durable: true}
				},
				objects: 1, want: [][]string{nak, nak, nak, nak},
			},
			{
				name: "registration",
				stall: func(t *testing.T, p *pipeline, capture tracepack.UUID) *stallingPut {
					lockCapture(t, p.cat, capture)
					return nil
				},
				objects: 1, want: [][]string{nak, nak, nak, nak},
			},
			{
				name:       "confirmed acknowledgement",
				claimCheck: true, objects: 1, registered: true,
				want: [][]string{{"double-ack"}, append([]string{"double-ack ended"}, nak...), nak, nak},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				p := newPipeline(t, c)
				g := p.ingester(slog.New(slog.DiscardHandler))
				g.AckWait = bound
				ctx := t.Context()
				var exp *expiring
				capture := tracepack.UUID{0xc1, byte(len(tc.name))}
				msgs := traffic(t, capture)
				if tc.claimCheck {
					msgs = confirmedTraffic(t, p.cl, capture)
					// The deadline passes once the acknowledgement waits, so the registration before it,
					// slower on some engines than any fixed bound, is never what it ends.
					g.AckWait = time.Hour
					exp = newExpiring(ctx)
					ctx = exp
				}
				hs := make([]*msg, len(msgs))
				for i, m := range msgs {
					hs[i] = &msg{}
					var h Msg = hs[i]
					if tc.claimCheck && i == 1 {
						h = stallingAck{msg: hs[i], entered: exp.expire}
					}
					p.add(t, m, h)
				}
				var st *stallingPut
				if tc.stall != nil {
					if st = tc.stall(t, p, capture); st != nil {
						p.f.Store = st
					}
				}

				err := g.flush(ctx, p.b.All())
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("flush = %v, want the deadline", err)
				}
				if st != nil {
					if puts, ended, deletes := st.counts(); puts != 1 || ended != 1 || deletes != 0 {
						t.Errorf("puts %d, ended by their context %d, deletes %d; want the put ended by its context and nothing deleted", puts, ended, deletes)
					}
				}
				for i, h := range hs {
					if got := h.done(); !slices.Equal(got, tc.want[i]) {
						t.Errorf("message %d: %v, want %v", i, got, tc.want[i])
					}
				}
				if ks := storedKeys(t, p.st); len(ks) != tc.objects {
					t.Errorf("objects %v, want %d kept for the reconciler", ks, tc.objects)
				}
				if got, err := c.Capture(t.Context(), capture); err != nil || got.HasHighestSeq != tc.registered {
					t.Errorf("capture %+v, %v; want registered %v", got, err, tc.registered)
				}
			})
		}
	})
}

// A registration whose commit succeeded but whose answer is lost fails the flush at the catalog's call:
// the segment stays registered and its object kept, nothing is acknowledged,
// and the messages are nak'd and written again at their redelivery, the merge deduplicating them.
func TestFlushCommitWhoseAnswerIsLost(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		g.AckWait = time.Hour
		p.f.registerPack = func(ctx context.Context, pk *catalog.Pack, r catalog.Registration) (catalog.PackOutcome, int, error) {
			if out, _, err := c.RegisterPackGaps(ctx, pk, r); err != nil || out != catalog.Registered {
				t.Errorf("registration = %v, %v; want it committed", out, err)
			}

			return 0, 0, fmt.Errorf("catalog: register pack %s: %w", pk.ID, context.DeadlineExceeded)
		}
		capture := tracepack.UUID{0xc1, 0x10}
		hs := p.deliver(t, confirmedTraffic(t, p.cl, capture))
		if err := g.flush(t.Context(), p.b.All()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("flush = %v, want the lost answer", err)
		}
		for i, h := range hs {
			if got := h.done(); !slices.Equal(got, []string{"nak " + firstNakDelay.String()}) {
				t.Errorf("message %d: %v, want nak'd", i, got)
			}
		}
		if ks := storedKeys(t, p.st); len(ks) != 1 {
			t.Errorf("objects %v, want the registered segment kept", ks)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || !got.HasHighestSeq {
			t.Errorf("capture %+v, %v; want the segment registered", got, err)
		}
	})
}

// stallLog signals each flush stall it is told of.
type stallLog struct {
	slog.Handler
	stalled chan struct{}
}

func (h stallLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "stalled") {
		select {
		case h.stalled <- struct{}{}:
		default:
		}
	}

	return nil
}

func (h stallLog) Enabled(context.Context, slog.Level) bool { return true }

// Local file work is not interrupted: a working file's write that outlasts the ack wait is reported stalled while it runs,
// the flush waits for it, and only then gives the batch up, its messages nak'd and nothing put.
func TestFlushWaitsForStalledLocalWork(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		g.AckWait = bound
		s := metrics.NewServer()
		m, err := metrics.NewIngest(s, nil)
		if err != nil {
			t.Fatal(err)
		}
		stalled := make(chan struct{}, 1)
		p.f.Metrics, p.f.Log = m, slog.New(stallLog{Handler: slog.DiscardHandler, stalled: stalled})
		var returned bool
		p.f.hooks[hookWrite] = func() error { // the write, blocked inside the working file's abstraction until its stall is reported
			p.f.hooks[hookWrite] = nil
			timer := time.NewTimer(guard)
			defer timer.Stop()
			select {
			case <-stalled:
			case <-timer.C:
			}
			returned = true

			return nil
		}
		hs := p.deliver(t, traffic(t, tracepack.UUID{0xc1, 0x11}))

		err = g.flush(t.Context(), p.b.All())
		if !returned || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("flush = %v, the write returned %v; want the flush given up after the write returned", err, returned)
		}
		if body := scrape(t, s); !strings.Contains(body, "tracekeep_ingest_flush_stalls_total 1") {
			t.Errorf("no stall counted:\n%s", grep(body, "flush_stalls"))
		}
		for i, h := range hs {
			if got := h.done(); !slices.Equal(got, []string{"nak " + firstNakDelay.String()}) {
				t.Errorf("message %d: %v, want nak'd", i, got)
			}
		}
		if ks := storedKeys(t, p.st); len(ks) != 0 {
			t.Errorf("objects %v, want none put", ks)
		}
	})
}

// slowSource is a store whose puts read their source slowly: the first read waits until wait yields.
type slowSource struct {
	store.ObjectStore
	wait <-chan struct{}
}

func (s slowSource) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	return s.ObjectStore.Put(ctx, key, &waitingReader{ReadSeeker: r, wait: s.wait}, size)
}

type waitingReader struct {
	io.ReadSeeker
	wait   <-chan struct{}
	waited bool
}

func (r *waitingReader) Read(p []byte) (int, error) {
	if !r.waited {
		r.waited = true
		timer := time.NewTimer(guard)
		defer timer.Stop()
		select {
		case <-r.wait:
		case <-timer.C:
		}
	}

	return r.ReadSeeker.Read(p)
}

// A filesystem put is local file work too: one that outlasts the ack wait mid-copy is reported stalled,
// finishes, its object published, and only then is the batch given up, its messages nak'd and the object kept for the reconciler.
func TestFlushWaitsForAStalledFilesystemPut(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		g.AckWait = bound
		s := metrics.NewServer()
		m, err := metrics.NewIngest(s, nil)
		if err != nil {
			t.Fatal(err)
		}
		stalled := make(chan struct{}, 1)
		p.f.Metrics, p.f.Log = m, slog.New(stallLog{Handler: slog.DiscardHandler, stalled: stalled})
		fs, err := store.NewFS(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fs.Close() })
		p.f.Store, p.f.LocalStore = slowSource{ObjectStore: fs, wait: stalled}, true
		hs := p.deliver(t, traffic(t, tracepack.UUID{0xc1, 0x15}))

		err = g.flush(t.Context(), p.b.All())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("flush = %v, want the batch given up at the ack wait", err)
		}
		if body := scrape(t, s); !strings.Contains(body, "tracekeep_ingest_flush_stalls_total 1") {
			t.Errorf("no stall counted:\n%s", grep(body, "flush_stalls"))
		}
		if ks := storedKeys(t, fs); len(ks) != 1 {
			t.Errorf("objects %v, want the put finished and its object kept", ks)
		}
		for i, h := range hs {
			if got := h.done(); !slices.Equal(got, []string{"nak " + firstNakDelay.String()}) {
				t.Errorf("message %d: %v, want nak'd", i, got)
			}
		}
	})
}

// grep is the lines of body that hold s.
func grep(body, s string) string {
	var out []string
	for l := range strings.Lines(body) {
		if strings.Contains(l, s) {
			out = append(out, l)
		}
	}

	return strings.Join(out, "")
}

// beatingMsg is a message whose nak is followed by a heartbeat tick, as one can be during a flush's naks.
type beatingMsg struct {
	*msg
	beat func()
}

func (m beatingMsg) NakWithDelay(d time.Duration) error {
	err := m.msg.NakWithDelay(d)
	m.beat()

	return err
}

// A batch given up is no longer kept in progress: a tick during its naks sends no progress for its messages,
// which would hold them for another ack wait.
func TestGivenUpBatchLeavesTheHeartbeat(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		g.AckWait = bound
		msgs := traffic(t, tracepack.UUID{0xc1, 0x12})
		hs := make([]*msg, len(msgs))
		for i, m := range msgs {
			hs[i] = &msg{}
			p.add(t, m, beatingMsg{msg: hs[i], beat: g.beat})
		}
		p.f.Store = &stallingPut{ObjectStore: p.st}
		if err := g.flush(t.Context(), g.take(p.b.All)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("flush = %v, want the ack wait's deadline", err)
		}
		for i, h := range hs {
			if got := h.done(); slices.Contains(got, "in-progress") {
				t.Errorf("message %d: %v, want no progress once given up", i, got)
			}
		}
	})
}

// On a stop, the batches still due share one budget: once a put that never answers has spent it,
// the next batch is given up without a put of its own, every message is left to redelivery and nothing is registered.
func TestStopSharesOneBudget(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		st := &stallingPut{ObjectStore: r.st}
		r.g.Flusher.Store = st
		r.g.AckWait, r.g.StopTimeout = time.Hour, bound
		captures := []tracepack.UUID{{0xc1, 0x13}, {0xc1, 0x14}}
		for _, capture := range captures {
			r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
			r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		}
		if err := r.g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // the stop: every batch is still due

		if err := r.g.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want the stop's budget spent", err)
		}
		if puts, ended, deletes := st.counts(); puts != 1 || ended != 1 || deletes != 0 {
			t.Errorf("puts %d, ended by their context %d, deletes %d; want one put, ended by the shared budget", puts, ended, deletes)
		}
		if n := r.pending(t); n != 4 {
			t.Errorf("the stream holds %d messages, want all 4 left to redelivery", n)
		}
		for _, capture := range captures {
			if got, err := c.Capture(t.Context(), capture); err != nil || got.HasHighestSeq {
				t.Errorf("capture %+v, %v; want nothing registered", got, err)
			}
		}
	})
}

// stopOnFetch is a consumer whose every fetch is followed by a stop, as a signal arriving while one waits.
type stopOnFetch struct {
	jetstream.Consumer
	stop context.CancelFunc
}

func (s stopOnFetch) Fetch(n int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	defer s.stop()

	return s.Consumer.Fetch(n, opts...)
}

// A stop between a fetch and the flush of the batches then due hands them to the bounded final flush:
// they are registered and acknowledged, never nak'd under the stop's ended context and written again.
func TestStopBetweenAFetchAndTheDueBatches(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		r.g.AckWait, r.g.StopTimeout = time.Minute, time.Minute
		capture := tracepack.UUID{0xc9, 1}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		r.step(t)
		r.clk.flushInterval() // the batch is due at the next step
		ctx, cancel := context.WithCancel(t.Context())
		r.g.Consumer = stopOnFetch{Consumer: r.g.Consumer, stop: cancel}

		if err := r.g.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || got.HighestSeq != 1 {
			t.Errorf("capture %+v, %v; want the due batch registered", got, err)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the stream holds %d messages, want both acknowledged", n)
		}
	})
}

// A stop right after a fetch whose due batch then spends the stop's budget is a failed stop:
// Run reports it, the batch's messages left to redelivery and nothing registered.
func TestStopRightAfterAFetchThatSpendsTheBudget(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		st := &stallingPut{ObjectStore: r.st}
		r.g.Flusher.Store = st
		// The fetch's wait is short and the budget long beside it, so it is the put that spends the budget.
		r.g.AckWait, r.g.StopTimeout, r.g.MaxWait = time.Hour, time.Second, time.Millisecond
		capture := tracepack.UUID{0xc9, 2}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		r.step(t)
		r.clk.flushInterval() // the batch is due at the next step
		ctx, cancel := context.WithCancel(t.Context())
		r.g.Consumer = stopOnFetch{Consumer: r.g.Consumer, stop: cancel}

		if err := r.g.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want the stop's budget spent", err)
		}
		if puts, ended, deletes := st.counts(); puts != 1 || ended != 1 || deletes != 0 {
			t.Errorf("puts %d, ended by their context %d, deletes %d; want one put, ended by the stop's budget", puts, ended, deletes)
		}
		if n := r.pending(t); n != 2 {
			t.Errorf("the stream holds %d messages, want both left to redelivery", n)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || got.HasHighestSeq {
			t.Errorf("capture %+v, %v; want nothing registered", got, err)
		}
	})
}
