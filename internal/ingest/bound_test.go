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
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

// guard ends a double's wait that no context ended, so a missing bound fails the test instead of hanging it.
const guard = 5 * time.Second

// bound is the ack wait, the stop budget or the wait after a put the tests give a flush; only its end matters, never its length.
const bound = 100 * time.Millisecond

var errNotEnded = errors.New("not ended by its context")

// stallingPut is a store whose puts never answer: each returns only once its context ends.
// A durable one makes the object first, so what is lost is the answer.
type stallingPut struct {
	store.ObjectStore
	durable bool
	// entered, when set, is told the put is waiting.
	entered func()

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
	if s.entered != nil {
		s.entered()
	}
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

// afterPut is a store that calls after once a put succeeded.
type afterPut struct {
	store.ObjectStore
	after func()
}

func (s afterPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	err := s.ObjectStore.Put(ctx, key, r, size)
	if err == nil {
		s.after()
	}

	return err
}

// stallingDelete is a store whose deletes never answer: each tells entered it waits, then returns only once its context ends.
type stallingDelete struct {
	store.ObjectStore
	entered func()
}

func (s stallingDelete) Delete(ctx context.Context, _ string) error {
	s.entered()
	t := time.NewTimer(guard)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return errNotEnded
	}
}

// holdReads takes the catalog's one read connection, its read pool cut to one, so every read waits for it until its context ends;
// release, the guard or the test's end gives it back and restores the pool.
func holdReads(t *testing.T, c *catalog.Catalog) (release func()) {
	t.Helper()
	_, r := c.Pools()
	limit := r.Stats().MaxOpenConnections
	r.SetMaxOpenConns(1)
	conn, err := r.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() {
		once.Do(func() {
			_ = conn.Close()
			r.SetMaxOpenConns(limit)
		})
	}
	timer := time.AfterFunc(guard, release) // the guard: a read no context ends goes on after it
	t.Cleanup(func() { timer.Stop() })
	t.Cleanup(release)

	return release
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
// the catalog's read of the capture, the store's put, a put made durable whose answer is lost, the catalog's registration,
// the deletion of a segment whose hour passed the horizon before its registration,
// and a confirmed acknowledgement at a deadline passed once it waits.
// Whatever the outcome, nothing is acknowledged before its registration, a message not acknowledged is nak'd,
// and an object the flush may have put is kept for the reconciler, never deleted by the flush (ingest.md §6.2).
func TestFlushRemoteCallsEndWithTheirContext(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		nak := []string{"nak " + firstNakDelay.String()}
		var reads func() // the held read connection, given back once the flush returned
		for _, tc := range []struct {
			name string
			// stall sets the stall up once the batch is held, passing exp's deadline once the stalled call waits,
			// and returns the store the flush puts to when it is not p's.
			stall func(t *testing.T, p *pipeline, capture tracepack.UUID, exp *expiring) *stallingPut
			// claimCheck acknowledges by DoubleAck, and the second message's never answers.
			claimCheck bool
			objects    int
			registered bool
			want       [][]string // each message's disposition, in seq order
		}{
			{
				name: "capture read",
				stall: func(t *testing.T, p *pipeline, _ tracepack.UUID, exp *expiring) *stallingPut {
					reads = holdReads(t, p.cat)
					// The read is the flush's first call, so the deadline passing a bound from here passes while it waits.
					time.AfterFunc(bound, exp.expire)
					return nil
				},
				want: [][]string{nak, nak, nak, nak},
			},
			{
				name: "put",
				stall: func(_ *testing.T, p *pipeline, _ tracepack.UUID, exp *expiring) *stallingPut {
					return &stallingPut{ObjectStore: p.st, entered: exp.expire}
				},
				want: [][]string{nak, nak, nak, nak},
			},
			{
				name: "durable put whose answer is lost",
				stall: func(_ *testing.T, p *pipeline, _ tracepack.UUID, exp *expiring) *stallingPut {
					return &stallingPut{ObjectStore: p.st, durable: true, entered: exp.expire}
				},
				objects: 1, want: [][]string{nak, nak, nak, nak},
			},
			{
				name: "registration",
				stall: func(t *testing.T, p *pipeline, capture tracepack.UUID, exp *expiring) *stallingPut {
					lockCapture(t, p.cat, capture)
					// The deadline passes a bound after the put, while the registration waits for the capture.
					p.f.Store = afterPut{ObjectStore: p.st, after: func() { time.AfterFunc(bound, exp.expire) }}
					return nil
				},
				objects: 1, want: [][]string{nak, nak, nak, nak},
			},
			{
				// The registration tombstoned the key; the object it could not delete is the orphan pass's.
				name: "beyond-horizon delete",
				stall: func(_ *testing.T, p *pipeline, _ tracepack.UUID, exp *expiring) *stallingPut {
					p.f.HorizonHour = func() int64 { return HourOf(now) + 1 }
					p.f.Store = stallingDelete{ObjectStore: p.st, entered: exp.expire}
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
				// The deadline passes once the stalled call waits, so the work before it,
				// slower on some engines and runners than any fixed bound, is never what it ends.
				g.AckWait = time.Hour
				exp := newExpiring(t.Context())
				var ctx context.Context = exp
				capture := tracepack.UUID{0xc1, byte(len(tc.name))}
				msgs := traffic(t, capture)
				if tc.claimCheck {
					msgs = confirmedTraffic(t, p.cl, capture)
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
					if st = tc.stall(t, p, capture, exp); st != nil {
						p.f.Store = st
					}
				}

				flushed := time.Now()
				err := g.flush(ctx, p.b.All())
				if took := time.Since(flushed); took >= guard {
					t.Errorf("flush returned after %v, want the stalled call ended by its deadline, not the guard", took)
				}
				if reads != nil {
					reads()
					reads = nil
				}
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

// The ack wait bounds a flush's registration, with no deadline above it: the registration's context ends at the ack wait from the flush's start.
func TestAckWaitBoundsTheRegistration(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		g.AckWait = time.Hour
		capture := tracepack.UUID{0xc1, 0x17}
		p.deliver(t, traffic(t, capture))
		var deadline time.Time
		var bounded bool
		p.f.registerPack = func(ctx context.Context, pk *catalog.Pack, r catalog.Registration) (catalog.PackOutcome, int, error) {
			deadline, bounded = ctx.Deadline()
			return c.RegisterPackGaps(ctx, pk, r)
		}

		began := time.Now()
		err := g.flush(t.Context(), p.b.All())
		if err != nil {
			t.Fatalf("flush = %v", err)
		}
		if start := deadline.Add(-g.AckWait); !bounded || start.Before(began) || start.After(time.Now()) {
			t.Errorf("the registration's deadline %v (set %v) is not the ack wait from the flush's start", deadline, bounded)
		}
	})
}

// A flush whose registration never answers is given up at its deadline, here passing once the registration is entered:
// every message nak'd, the put object kept for the reconciler and nothing registered.
func TestStalledRegistrationGivenUp(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		g.AckWait = time.Hour // the deadline is the test's, on cue
		exp := newExpiring(t.Context())
		capture := tracepack.UUID{0xc1, 0x16}
		hs := p.deliver(t, traffic(t, capture))
		release := lockCapture(t, p.cat, capture)
		p.f.registerPack = func(ctx context.Context, pk *catalog.Pack, r catalog.Registration) (catalog.PackOutcome, int, error) {
			time.AfterFunc(bound, exp.expire)
			return c.RegisterPackGaps(ctx, pk, r)
		}

		err := g.flush(exp, p.b.All())
		release()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("flush = %v, want the deadline", err)
		}
		for i, h := range hs {
			if got := h.done(); !slices.Equal(got, []string{"nak " + firstNakDelay.String()}) {
				t.Errorf("message %d: %v, want nak'd", i, got)
			}
		}
		if ks := storedKeys(t, p.st); len(ks) != 1 {
			t.Errorf("objects %v, want the put object kept for the reconciler", ks)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || got.HasHighestSeq {
			t.Errorf("capture %+v, %v; want nothing registered", got, err)
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
		g.AckWait = time.Hour
		exp := newExpiring(t.Context()) // the ack wait's deadline, passed once the write waits
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
			exp.expire()
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

		err = g.flush(exp, p.b.All())
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

// slowSource is a store whose puts read their source slowly: the first read tells entered, then waits until wait yields.
type slowSource struct {
	store.ObjectStore
	entered func()
	wait    <-chan struct{}
}

func (s slowSource) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	return s.ObjectStore.Put(ctx, key, &waitingReader{ReadSeeker: r, entered: s.entered, wait: s.wait}, size)
}

type waitingReader struct {
	io.ReadSeeker
	entered func()
	wait    <-chan struct{}
	waited  bool
}

func (r *waitingReader) Read(p []byte) (int, error) {
	if !r.waited {
		r.waited = true
		r.entered()
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
		g.AckWait = time.Hour
		exp := newExpiring(t.Context()) // the ack wait's deadline, passed once the put reads its source
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
		p.f.Store, p.f.LocalStore = slowSource{ObjectStore: fs, entered: exp.expire, wait: stalled}, true
		hs := p.deliver(t, traffic(t, tracepack.UUID{0xc1, 0x15}))

		err = g.flush(exp, p.b.All())
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
		// The budget starts once the put waits, so the work before it, slower on some engines and runners than any fixed bound,
		// is never what spends it.
		start, started := context.WithCancel(t.Context())
		r.g.stopStart = start
		st := &stallingPut{ObjectStore: r.st, entered: started}
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

// On a stop, a registration that never answers is ended by the stop's budget, which starts once it waits:
// Run returns within the budget, the put object kept for the reconciler, nothing registered and every message left to redelivery.
func TestStopEndsAStalledRegistration(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		start, started := context.WithCancel(t.Context())
		r.g.stopStart = start
		r.g.AckWait, r.g.StopTimeout = time.Hour, bound
		capture := tracepack.UUID{0xc1, 0x17}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		if err := r.g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		release := lockCapture(t, c, capture) // after the step, whose renewal writes; the stop renews nothing
		var waiting time.Time
		r.g.Flusher.registerPack = func(ctx context.Context, pk *catalog.Pack, reg catalog.Registration) (catalog.PackOutcome, int, error) {
			waiting = time.Now()
			started()
			return c.RegisterPackGaps(ctx, pk, reg)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // the stop: the batch is still due

		err := r.g.Run(ctx)
		release()
		if waiting.IsZero() {
			t.Fatalf("Run = %v before the registration", err)
		}
		if took := time.Since(waiting); took >= guard {
			t.Errorf("Run returned %v after the registration waited, want it ended by the stop's budget", took)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want the stop's budget spent", err)
		}
		if n := r.pending(t); n != 2 {
			t.Errorf("the stream holds %d messages, want both left to redelivery", n)
		}
		if ks := storedKeys(t, r.st); len(ks) != 1 {
			t.Errorf("objects %v, want the put object kept for the reconciler", ks)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || got.HasHighestSeq {
			t.Errorf("capture %+v, %v; want nothing registered", got, err)
		}
	})
}

// On a stop, local file work is not interrupted either: a working file's write that outlasts the stop's budget is reported stalled and counted,
// and Run waits for it, returning only once the test releases the write, with the stop's failure and every message left to redelivery.
func TestStopWaitsForStalledLocalWork(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		start, started := context.WithCancel(t.Context())
		r.g.stopStart = start
		r.g.AckWait, r.g.StopTimeout = time.Hour, bound
		s := metrics.NewServer()
		m, err := metrics.NewIngest(s, nil)
		if err != nil {
			t.Fatal(err)
		}
		stalled := make(chan struct{}, 1)
		r.g.Flusher.Metrics, r.g.Flusher.Log = m, slog.New(stallLog{Handler: slog.DiscardHandler, stalled: stalled})
		released := make(chan struct{})
		var returned atomic.Bool
		r.g.Flusher.hooks[hookWrite] = func() error { // the write, held inside the working file's abstraction until the test releases it
			started()
			timer := time.NewTimer(guard)
			defer timer.Stop()
			select {
			case <-released:
			case <-timer.C:
			}
			returned.Store(true)

			return nil
		}
		capture := tracepack.UUID{0xc1, 0x18}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		if err := r.g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // the stop: the batch is still due
		done := make(chan error, 1)
		go func() { done <- r.g.Run(ctx) }()

		timer := time.NewTimer(guard)
		defer timer.Stop()
		select {
		case <-stalled:
		case <-timer.C:
			t.Fatal("no stall reported once the stop's budget was spent")
		}
		// Nothing signals a wait that goes on, so a bound past the spent budget is what Run must outlast while the write is held.
		held := time.NewTimer(bound)
		defer held.Stop()
		select {
		case err := <-done:
			t.Fatalf("Run = %v while its write is held, want it waiting for the write", err)
		case <-held.C:
		}
		close(released)
		err = <-done
		if !returned.Load() {
			t.Fatal("Run returned before the write it started")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want the stop's failure", err)
		}
		if body := scrape(t, s); !strings.Contains(body, "tracekeep_ingest_flush_stalls_total 1") {
			t.Errorf("no stall counted:\n%s", grep(body, "flush_stalls"))
		}
		if n := r.pending(t); n != 2 {
			t.Errorf("the stream holds %d messages, want both left to redelivery", n)
		}
		if ks := storedKeys(t, r.st); len(ks) != 0 {
			t.Errorf("objects %v, want none put after the budget was spent", ks)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || got.HasHighestSeq {
			t.Errorf("capture %+v, %v; want nothing registered", got, err)
		}
	})
}

// watchedMsg is a delivered message whose naks are recorded and whose confirmed acknowledgement doubleAck replaces when set.
type watchedMsg struct {
	Msg
	seq       uint64
	doubleAck func(ctx context.Context) error
	naked     bool
}

func (m *watchedMsg) DoubleAck(ctx context.Context) error {
	if m.doubleAck != nil {
		return m.doubleAck(ctx)
	}

	return m.Msg.DoubleAck(ctx)
}

func (m *watchedMsg) NakWithDelay(d time.Duration) error {
	m.naked = true

	return m.Msg.NakWithDelay(d)
}

// A parked copy's acknowledgement that never answers ends its flush at each of its three boundaries (parking.md §6.1),
// after the copies before it completed: those stay acknowledged, their rows acked, and the copy and the rest are nak'd.
// Stalled in PrepareAck, the copy is not acknowledged; stalled in the confirmed acknowledgement, it is not confirmed;
// stalled in AckCopy once the server confirmed it, the flush counts it unacknowledged and its row stays waiting,
// and the copy reconciliation marks the row acked from the stream, which no longer holds the copy, with no redelivery.
func TestParkedAcknowledgementStalls(t *testing.T) {
	t.Parallel()
	const done = 2 // copies acknowledged before the stall, in seq order
	for i, tc := range []struct {
		name string
		// arm stalls s.stalled's acknowledgement at its boundary, passing s.exp's deadline once the stalled call waits.
		arm func(t *testing.T, s *ackStall)
		// confirmed reports the server confirmed the stalled copy's acknowledgement.
		confirmed bool
	}{
		{
			name: "prepare",
			arm: func(t *testing.T, s *ackStall) {
				acked := 0
				s.pg.Flusher.hooks[hookAfterAck] = func() error {
					if acked++; acked == done {
						s.release = lockCapture(t, s.c, s.capture)
						time.AfterFunc(bound, s.exp.expire) // while the next copy's PrepareAck waits for the capture
					}

					return nil
				}
				s.stalled.doubleAck = func(context.Context) error {
					t.Error("the stalled copy's acknowledgement was sent")
					return nil
				}
			},
		},
		{
			name: "double-ack",
			arm: func(_ *testing.T, s *ackStall) {
				s.stalled.doubleAck = func(ctx context.Context) error {
					s.exp.expire()
					timer := time.NewTimer(guard)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-timer.C:
						return errNotEnded
					}
				}
			},
		},
		{
			name: "ack-copy",
			arm: func(t *testing.T, s *ackStall) {
				s.stalled.doubleAck = func(ctx context.Context) error {
					if err := s.stalled.Msg.DoubleAck(ctx); err != nil {
						return err
					}
					s.release = lockCapture(t, s.c, s.capture)
					time.AfterFunc(bound, s.exp.expire) // while AckCopy waits for the capture

					return nil
				}
			},
			confirmed: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				r := newReplica(t, c)
				s := &ackStall{c: c, capture: tracepack.UUID{0xc2, byte(i)}, exp: newExpiring(t.Context())}
				parkTraffic(t, r, s.capture)
				s.pg = r.parkedConsumer(t)
				batches, ws := watchedBatch(t, s.pg)
				s.stalled = ws[done]
				tc.arm(t, s)

				err := s.pg.flush(s.exp, batches)
				if s.release != nil {
					s.release()
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("flush = %v, want the deadline", err)
				}
				for j, w := range ws {
					if w.naked != (j >= done) {
						t.Errorf("copy of seq %d nak'd %v; want the first %d acknowledged, the stalled copy and the rest nak'd", w.seq, w.naked, done)
					}
				}
				want := []string{"acked", "acked", "waiting", "waiting"}
				if got := copyStates(versions(t, c, s.capture), s.pg.Incarnation.ID); !slices.Equal(got, want) {
					t.Errorf("copy rows %v, want %v", got, want)
				}
				held := uint64(len(ws) - done)
				if tc.confirmed {
					held-- // the work-queue stream removed the confirmed copy
				}
				if n := msgs(t, r.parked(t)); n != held {
					t.Errorf("the parking stream holds %d copies, want %d", n, held)
				}
				if !tc.confirmed {
					return
				}
				rc := &compact.Compactor{Catalog: c, Log: slog.New(slog.DiscardHandler)}
				if n, err := rc.Copies(t.Context(), retention(t, c), catalog.RoleParking, r.parked(t)); err != nil || n != 1 {
					t.Fatalf("Copies = %d, %v; want the confirmed copy's row marked", n, err)
				}
				want[done] = "acked"
				if got := copyStates(versions(t, c, s.capture), s.pg.Incarnation.ID); !slices.Equal(got, want) {
					t.Errorf("copy rows after the reconciliation %v, want %v", got, want)
				}
				if n := msgs(t, r.parked(t)); n != held {
					t.Errorf("the parking stream holds %d copies after the reconciliation, want %d: the confirmed copy is not redelivered", n, held)
				}
			})
		})
	}
}

// ackStall is one stalled acknowledgement of a parked copy: the parked consumer flushing it, the copy, the deadline the stall passes,
// and the release of a capture row the stall holds.
type ackStall struct {
	c       *catalog.Catalog
	capture tracepack.UUID
	pg      *Ingester
	stalled *watchedMsg
	exp     *expiring
	release func()
}

// watchedBatch fetches and batches the parked copies with pg, not yet due, takes the batches for a flush
// and returns them with each copy's message watched, in seq order.
func watchedBatch(t *testing.T, pg *Ingester) ([]*Batch, []*watchedMsg) {
	t.Helper()
	if err := pg.Step(t.Context()); err != nil {
		t.Fatal(err)
	}
	batches := pg.take(pg.Batcher.All)
	var ws []*watchedMsg
	for _, b := range batches {
		for j := range b.Entries {
			w := &watchedMsg{Msg: b.Entries[j].Msg, seq: b.Entries[j].D.Raw.Version.Seq}
			b.Entries[j].Msg = w
			ws = append(ws, w)
		}
	}
	if len(ws) != 4 {
		t.Fatalf("%d copies batched, want 4", len(ws))
	}
	slices.SortFunc(ws, func(a, b *watchedMsg) int { return cmpSeq(a.seq, b.seq) })

	return batches, ws
}

// parkTraffic parks capture's traffic: its four versions parked write-failure, the Writer refusing their batch three times.
func parkTraffic(t *testing.T, r *replica, capture tracepack.UUID) {
	t.Helper()
	r.g.Flusher.hooks[hookWrite] = func() error { return fmt.Errorf("%w: %w", tracepack.ErrRefused, tracepack.ErrInvalidRecord) }
	for _, m := range traffic(t, capture) {
		r.publish(t, m, nil)
	}
	r.step(t)
	for _, b := range r.g.Batcher.batches {
		for range writeFailureLimit - 1 {
			r.g.refused(b.Entries)
		}
	}
	r.clk.flushInterval()
	r.step(t)
	if vs := versions(t, r.cat, capture); len(vs) != 4 {
		t.Fatalf("%d versions parked, want 4", len(vs))
	}
	r.g.Flusher.hooks[hookWrite] = nil // the Writer is fixed
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
		if n := r.settled(t, 0); n != 0 {
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
		start, started := context.WithCancel(t.Context())
		r.g.stopStart = start
		st := &stallingPut{ObjectStore: r.st, entered: started}
		r.g.Flusher.Store = st
		r.g.AckWait, r.g.StopTimeout = time.Hour, bound
		capture := tracepack.UUID{0xc9, 2}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		r.step(t) // under the default fetch wait, so both messages are held before the stop
		if n := len(r.g.Batcher.batches); n != 1 {
			t.Fatalf("%d batches held after the first step, want 1", n)
		}
		r.clk.flushInterval() // the batch is due at the next step
		// The stop's fetch waits briefly, and the budget starts once the put waits, so it is the put that spends the budget.
		r.g.MaxWait = time.Millisecond
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
