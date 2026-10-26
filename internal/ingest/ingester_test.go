package ingest

import (
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
)

const prefix = "tk"

// clock is a settable clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *clock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

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
	s, err := bus.Ensure(ctx, js, bus.Config{SubjectPrefix: prefix, ClaimBucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	inc, err := bus.Incarnate(ctx, c, s)
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
	x := &Executor{Catalog: c, Claims: ObjectClaims{Store: obj}, Incarnation: inc.ID, Horizon: func() int64 { return now - int64(24*time.Hour) }, Configs: configs}
	st := store.NewMem()
	f := &Flusher{
		Catalog: c, Store: st, WorkDir: t.TempDir(), Configs: configs, Interval: time.Second,
		HorizonHour: func() int64 { return HourOf(x.Horizon()) }, Writer: "tracekeep-test", Now: clk.now,
	}
	g := &Ingester{
		Executor: x, Batcher: NewBatcher(time.Second, 1<<20), Flusher: f, Consumer: cons, Log: slog.New(slog.DiscardHandler),
		Replica: "r1", Lease: time.Minute, FetchBatch: 100, MaxWait: 200 * time.Millisecond, Now: clk.now,
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
		r.clk.advance(time.Second) // the flush interval passes
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
		capture := tracepack.UUID{2}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		for _, tc := range []struct {
			name  string
			setup func() int64
		}{
			{"after the lease", func() int64 { r.g.leaseEnd = r.clk.now(); return r.g.membership.Generation }},
			{"under an earlier membership", func() int64 { r.g.leaseEnd = r.clk.now().Add(time.Minute); return r.g.membership.Generation - 1 }},
		} {
			batch, err := r.g.Consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			generation := tc.setup()
			for m := range batch.Messages() {
				r.g.handle(t.Context(), m, generation)
			}
			if _, err := c.Capture(t.Context(), capture); !errors.Is(err, catalog.ErrUnknownCapture) {
				t.Errorf("%s: the start was processed: %v", tc.name, err)
			}
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
