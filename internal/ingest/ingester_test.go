package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

const prefix = "tk"

// clock is a settable clock; each reading advances it by tick.
type clock struct {
	mu   sync.Mutex
	t    time.Time
	tick time.Duration
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.t
	c.t = c.t.Add(c.tick)

	return t
}

// flushInterval advances the clock by one flush interval.
func (c *clock) flushInterval() { c.mu.Lock(); c.t = c.t.Add(time.Second); c.mu.Unlock() }

// replica is one ingest replica on its own bus, with its stream, catalog and store.
type replica struct {
	g      *Ingester
	js     jetstream.JetStream
	stream jetstream.Stream
	claims jetstream.ObjectStore
	st     *store.Mem
	clk    *clock
	cat    *catalog.Catalog
}

func newReplica(t *testing.T, c *catalog.Catalog) *replica {
	t.Helper()
	ctx := t.Context()
	nc, err := nats.Connect(testbus.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := bus.Ensure(ctx, js, bus.Config{SubjectPrefix: prefix, ClaimBucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	inc, err := bus.Incarnate(ctx, c, catalog.RoleTraffic, s)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := bus.Consumer(ctx, s, inc, bus.ConsumerConfig{AckWait: 30 * time.Second, MaxAckPending: 1000})
	if err != nil {
		t.Fatal(err)
	}
	obj, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		t.Fatal(err)
	}
	configs, err := Activate(ctx, c, []Config{testConfig()})
	if err != nil {
		t.Fatal(err)
	}
	clk := &clock{t: time.Unix(0, now)}
	x := &Executor{Catalog: c, Claims: ObjectClaims{Store: obj}, Horizon: func() int64 { return now - int64(24*time.Hour) }, Configs: configs}
	st := store.NewMem()
	f := &Flusher{
		Catalog: c, Store: st, WorkDir: t.TempDir(), Configs: configs, Interval: time.Second,
		HorizonHour: func() int64 { return HourOf(x.Horizon()) }, Writer: "tracekeep-test", Now: clk.now,
	}
	g := &Ingester{
		Executor: x, Batcher: NewBatcher(time.Second, 1<<20), Flusher: f, Consumer: cons, Stream: s, Log: slog.New(slog.DiscardHandler),
		Coordinator: &Coordinator{Catalog: c, Bus: js, ParkPrefix: busconfig.DefaultParkPrefix}, Park: true, Incarnation: inc,
		Replica: "r1", Lease: time.Minute, FetchBatch: 100, MaxWait: 200 * time.Millisecond, Heartbeat: 10 * time.Second, Now: clk.now,
	}
	if err := g.Join(ctx); err != nil {
		t.Fatal(err)
	}

	return &replica{g: g, js: js, stream: s, claims: obj, st: st, clk: clk, cat: c}
}

// publish publishes m on its capture's subject, putting a claim-check record's payload in the bucket first.
func (r *replica) publish(t *testing.T, m Message, claim []byte) {
	t.Helper()
	subject, err := tracepack.BusSubject(prefix, "T1", m.Raw().Version.Capture)
	if err != nil {
		t.Fatal(err)
	}
	if claim != nil {
		if _, err := r.claims.PutBytes(t.Context(), m.Env.MsgID, claim); err != nil {
			t.Fatal(err)
		}
	}
	nm := nats.NewMsg(subject)
	nm.Data = m.Body
	set := func(k, v string) {
		if v != "" {
			nm.Header.Set(k, v)
		}
	}
	set(tracepack.BusHeaderLayout, m.Env.Layout)
	set(tracepack.BusHeaderMsgID, m.Env.MsgID)
	set(tracepack.BusHeaderDescriptor, m.Env.Descriptor)
	set(tracepack.BusHeaderPayloadRef, m.Env.PayloadRef)
	set(tracepack.BusHeaderPayloadDigest, m.Env.PayloadDigest)
	if _, err := r.js.PublishMsg(t.Context(), nm); err != nil {
		t.Fatal(err)
	}
}

// Raw reads the message's raw facts, for a test that needs its version.
func (m Message) Raw() Raw {
	r, _ := ReadRaw(&m.Env, m.Body, 0)

	return r
}

func (r *replica) pending(t *testing.T) uint64 {
	t.Helper()
	info, err := r.stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return info.State.Msgs
}

// A capture's start and records travel the whole way: pulled, admitted, batched, written, registered and acknowledged,
// the stream then holding none of them.
func TestIngesterEndToEnd(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		capture := tracepack.UUID{1}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		for seq := uint64(1); seq <= 3; seq++ {
			r.publish(t, inline(t, capture, dataRecord(seq, now+int64(seq)), nil), nil)
		}
		big := dataRecord(4, now+4)
		cl := newClaims()
		m := claimCheck(t, cl, capture, big, nil)
		r.publish(t, m, cl.objs[m.Env.MsgID])

		if err := r.g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval() // the flush interval passes
		if err := r.g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		got, err := c.Capture(t.Context(), capture)
		if err != nil || got.State != catalog.StateOpen || got.HighestSeq != 4 {
			t.Fatalf("capture %+v, %v; want open with highest seq 4", got, err)
		}
		ks := keys(t, r.st)
		if len(ks) != 1 {
			t.Fatalf("objects %v, want one segment", ks)
		}
		if n := len(records(t, r.st, ks[0])); n != 5 {
			t.Errorf("segment holds %d records, want 5", n)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the stream still holds %d messages, want none", n)
		}
	})
}

// A message received after the lease lapsed, or pulled under an earlier membership, is nak'd untouched (rollouts.md §6.4).
func TestIngesterLapsedLeaseTouchesNothing(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		for i, tc := range []struct {
			name  string
			setup func() int64
		}{
			{"after the lease", func() int64 { r.g.leaseEnd = r.clk.now(); return r.g.membership.Generation }},
			{"under an earlier membership", func() int64 { r.g.leaseEnd = r.clk.now().Add(time.Minute); return r.g.membership.Generation - 1 }},
		} {
			capture := tracepack.UUID{2, byte(i)}
			r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
			batch, err := r.g.Consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			generation := tc.setup()
			handled := 0
			for m := range batch.Messages() {
				handled++
				r.g.handle(t.Context(), m, generation)
			}
			if handled != 1 {
				t.Fatalf("%s: %d messages handled, want the start (%v)", tc.name, handled, batch.Error())
			}
			if _, err := c.Capture(t.Context(), capture); !errors.Is(err, catalog.ErrUnknownCapture) {
				t.Errorf("%s: the start was processed: %v", tc.name, err)
			}
		}
	})
}

// A renewal that leaves no lease to fetch within fetches nothing.
func TestIngesterNoLeaseNoFetch(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		r.publish(t, inline(t, tracepack.UUID{3}, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.clk.tick = r.g.Lease // every reading of the clock takes a whole lease
		if err := r.g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		info, err := r.g.Consumer.Info(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if info.Delivered.Consumer != 0 {
			t.Errorf("%d messages fetched without a lease", info.Delivered.Consumer)
		}
	})
}

// A stream that is no longer its incarnation stops the replica at its renewal with bus-incarnation-changed,
// its held deliveries abandoned unacknowledged and nothing flushed (parking.md *incarnations*).
func TestIngesterStopsOnAnotherIncarnation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		capture := tracepack.UUID{4}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		if err := r.g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		if n := len(r.g.Batcher.batches); n != 1 {
			t.Fatalf("%d batches held, want the start's", n)
		}
		// A restore of an older snapshot: the stream's last sequence falls below one observed.
		if err := c.ObserveLastSeq(t.Context(), r.g.Incarnation.ID, 1000); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval()                                           // the batch is due
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second) // a Run that does not stop fails, never hangs
		defer cancel()
		if err := r.g.Run(ctx); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Fatalf("Run = %v, want ErrIncarnationChanged", err)
		}
		if n := len(r.g.Batcher.batches); n != 0 {
			t.Errorf("%d batches still held", n)
		}
		if ks := keys(t, r.st); len(ks) != 0 {
			t.Errorf("objects %v, want nothing flushed", ks)
		}
		if n := r.pending(t); n != 1 {
			t.Errorf("the stream holds %d messages, want the start unacknowledged", n)
		}
	})
}

// stuck is a held message whose first in-progress blocks until released; nakked closes at its first nak.
type stuck struct {
	msg
	entered, release, nakked chan struct{}
	once, nakOnce            sync.Once
}

func (m *stuck) NakWithDelay(d time.Duration) error {
	m.nakOnce.Do(func() { close(m.nakked) })

	return m.msg.NakWithDelay(d)
}

// alerted is a log handler that closes its channel at the first alert.
type alerted struct {
	slog.Handler
	once sync.Once
	ch   chan struct{}
}

func (a *alerted) Enabled(context.Context, slog.Level) bool { return true }

func (a *alerted) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "ingest: alert" {
		a.once.Do(func() { close(a.ch) })
	}

	return a.Handler.Handle(ctx, r)
}

func (m *stuck) InProgress() error {
	m.once.Do(func() {
		close(m.entered)
		<-m.release
	})

	return m.msg.InProgress()
}

// Run returns only once its heartbeat stopped, so no progress is sent after it: at its end and on bus-incarnation-changed.
func TestRunJoinsItsHeartbeat(t *testing.T) {
	t.Parallel()
	for _, exit := range []string{"cancelled", "incarnation-changed"} {
		t.Run(exit, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				r := newReplica(t, c)
				r.g.Heartbeat = time.Millisecond
				h := &stuck{entered: make(chan struct{}), release: make(chan struct{}), nakked: make(chan struct{})}
				alert := &alerted{Handler: slog.DiscardHandler, ch: make(chan struct{})}
				r.g.Log = slog.New(alert)
				d := &Delivery{Raw: Raw{Version: Version{Capture: tracepack.UUID{9}}, TS: now}}
				r.g.Batcher.Add(Entry{D: d, Msg: h}, r.clk.now())
				ctx, cancel := context.WithCancel(t.Context())
				returned := make(chan error, 1)
				go func() { returned <- r.g.Run(ctx) }()
				<-h.entered // a beat is under way
				// Run reaches its end while the beat is stuck: the final flush's nak, or the incarnation alert.
				if exit == "cancelled" {
					cancel()
					<-h.nakked // the held batch is not executable here, so its final flush naks it
				} else {
					if err := c.ObserveLastSeq(t.Context(), r.g.Incarnation.ID, 1000); err != nil {
						t.Fatal(err)
					}
					<-alert.ch
				}
				// All Run has left is to join its heartbeat: without the join it would return at once.
				select {
				case err := <-returned:
					close(h.release)
					t.Fatalf("%s: Run returned %v while its heartbeat was beating", exit, err)
				case <-time.After(100 * time.Millisecond):
				}
				close(h.release)
				err := <-returned
				if exit == "incarnation-changed" && !errors.Is(err, catalog.ErrIncarnationChanged) {
					t.Errorf("Run = %v, want ErrIncarnationChanged", err)
				}
				cancel()
			})
		})
	}
}

// failedPull is a pull the server accepted and then ended with an error.
type failedPull struct {
	jetstream.Consumer
	err error
}

func (f failedPull) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return endedBatch{f.err}, nil
}

type endedBatch struct{ err error }

func (b endedBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg)
	close(ch)

	return ch
}

func (b endedBatch) Error() error { return b.err }

// A pull the server ends with an error is the step's error, so Run backs off rather than pulling again at once.
func TestIngesterReturnsAFailedPull(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		ended := errors.New("consumer deleted")
		r.g.Consumer = failedPull{Consumer: r.g.Consumer, err: ended}
		if err := r.g.Step(t.Context()); !errors.Is(err, ended) {
			t.Errorf("Step = %v, want the pull's error", err)
		}
		r.g.Consumer = failedPull{Consumer: r.g.Consumer, err: jetstream.ErrNoMessages}
		if err := r.g.Step(t.Context()); err != nil {
			t.Errorf("Step = %v after a pull that found nothing, want nil", err)
		}
	})
}

// A replica that does not execute every configuration not yet retired cannot join (rollouts.md §6.4).
func TestIngesterJoinNeedsEveryConfiguration(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		if _, err := c.ActivateConfig(t.Context(), []byte(`{"classifier":"newer","max_frame_len":1}`)); err != nil {
			t.Fatal(err)
		}
		r.g.Replica = "r2"
		if err := r.g.Join(t.Context()); !errors.Is(err, catalog.ErrConfigNotExecuted) {
			t.Errorf("Join = %v, want ErrConfigNotExecuted", err)
		}
	})
}

// The nak delay backs off from five seconds to a minute.
func TestNakDelay(t *testing.T) {
	t.Parallel()
	for delivered, want := range map[uint64]time.Duration{1: 5 * time.Second, 2: 10 * time.Second, 4: 40 * time.Second, 5: time.Minute, 50: time.Minute} {
		if got := nakDelay(delivered); got != want {
			t.Errorf("nakDelay(%d) = %v, want %v", delivered, got, want)
		}
	}
}
