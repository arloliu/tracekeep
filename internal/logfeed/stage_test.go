package logfeed

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// stageEnv is a feed stream of 4 groups on an embedded bus over c, and a builder of replicas' stages sharing one store.
type stageEnv struct {
	t    *testing.T
	c    *catalog.Catalog
	js   jetstream.JetStream
	s    jetstream.Stream
	feed bus.Feed
	cfg  int64
	st   store.ObjectStore
}

func newStageEnv(t *testing.T, c *catalog.Catalog) *stageEnv {
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
	feed := bus.Feed{Stream: "TRACEKEEP_LOGFEED", Prefix: "tklog", Groups: 4, MaxBytes: 64 << 20, Duplicates: 2 * time.Minute,
		Replicas: 1, AckWait: 10 * time.Second, MaxAckPending: 10_000}
	s, err := bus.EnsureFeed(ctx, js, feed, busconfig.StreamConfig("TRACEKEEP_TRAFFIC", "tk"), busconfig.ParkedStreamConfig("TRACEKEEP_PARKED", "tkpark", 1))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := c.ActivateConfig(ctx, []byte(`{"classifier":"go-secs/v2.8.0","max_frame_len":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SyncLogGroups(ctx, feed.Groups); err != nil {
		t.Fatal(err)
	}

	return &stageEnv{t: t, c: c, js: js, s: s, feed: feed, cfg: cfg, st: store.NewMem()}
}

// stage registers replica's membership and returns its stage, whose files go idle after idle.
func (e *stageEnv) stage(replica string, idle time.Duration) *Stage {
	e.t.Helper()
	if _, err := e.c.Register(e.t.Context(), replica, []int64{e.cfg}, time.Hour); err != nil {
		e.t.Fatal(err)
	}

	return &Stage{
		Catalog: e.c, Stream: e.s, Feed: e.feed, Replica: replica, LeaseTTL: 900 * time.Millisecond, FetchBatch: 100, Pause: time.Second,
		Base: &Converter{
			Catalog: e.c, Store: e.st, WorkDir: e.t.TempDir(), Writer: "tracekeep-test", Configs: map[int64]ingest.Config{e.cfg: {Name: "go-secs/v2.8.0"}},
			Devices: Devices{Main: "eqp"}, Flush: 300 * time.Millisecond, Reorder: 100 * time.Millisecond, Idle: idle, AckWait: e.feed.AckWait,
			Pairing: 3, HorizonHour: func() int64 { return 0 }, Now: time.Now,
		},
	}
}

// publish publishes 3 unclassified entries for each of n equipment, their offsets from round's, logged now:
// a replica taking a group over restores a file's last arrival from its last record's time, which a fixed time would leave idle.
func (e *stageEnv) publish(n, round int) {
	e.t.Helper()
	taipei, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		e.t.Fatal(err)
	}
	ts := time.Now().In(taipei).Format("2006-01-02 15:04:05.000")
	for eq := range n {
		f := &fixture{t: e.t, offset: int64(round) * 1000}
		for range 3 {
			raw := f.raw(map[string]any{"equipment_id": fmt.Sprintf("EQ%d", eq), "kind": KindUnclassified, "text": "line", "ts": ts})
			if _, err := e.js.Publish(e.t.Context(), fmt.Sprintf("tklog.EQ%d", eq), raw); err != nil {
				e.t.Fatal(err)
			}
		}
	}
}

// eventually waits up to 20 s for ok.
func (e *stageEnv) eventually(what string, ok func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			e.t.Fatalf("%s: not within 20 s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (e *stageEnv) drained() bool {
	info, err := e.s.Info(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}

	return info.State.Msgs == 0
}

func run(ctx context.Context, wg *sync.WaitGroup, s *Stage) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	wg.Go(func() { _ = s.Run(ctx) })

	return cancel
}

// Two replicas share the lease groups and convert a feed of many equipment; when one stops, the other takes its groups,
// each re-creating the group's consumer, and every entry ends converted and acknowledged.
func TestStageSharesAndTakesOver(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		a, b := e.stage("a", time.Hour), e.stage("b", time.Hour)
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		stopA := run(ctx, &wg, a)
		run(ctx, &wg, b)
		e.publish(8, 0)
		e.eventually("two replicas drain the feed", e.drained)
		if len(a.Held())+len(b.Held()) != e.feed.Groups || len(a.Held()) == 0 || len(b.Held()) == 0 {
			t.Errorf("groups held: a %v, b %v; want the four shared", a.Held(), b.Held())
		}
		stopA()
		// The stopped replica leaves, as its ingest does; a replica that dies instead lapses with its membership.
		if err := c.Leave(ctx, "a"); err != nil {
			t.Fatal(err)
		}
		e.publish(8, 1)
		e.eventually("b drains the feed alone", e.drained)
		e.eventually("b holds every group", func() bool { return len(b.Held()) == e.feed.Groups })
		checkCaptures(t, c, 8, 6)
	})
}

// A replica that starts alone takes every group and gives back its share's excess once another joins.
func TestStageRebalancesForAJoiner(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		a := e.stage("a", time.Hour)
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, a)
		e.eventually("a takes every group", func() bool { return len(a.Held()) == e.feed.Groups })
		e.eventually("a ready", func() bool { return a.Ready(ctx) == nil })
		b := e.stage("b", time.Hour)
		run(ctx, &wg, b)
		e.eventually("the groups shared two and two", func() bool { return len(a.Held()) == 2 && len(b.Held()) == 2 })
		e.publish(8, 0)
		e.eventually("the feed drained", e.drained)
		checkCaptures(t, c, 8, 3)
	})
}

// A replica taking a group restores its open captures: a file of it goes idle and stops though no entry of it arrives.
func TestStageRestoresOpenCaptures(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		a := e.stage("a", time.Hour)
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		stopA := run(ctx, &wg, a)
		e.publish(1, 0)
		e.eventually("a drains the feed", e.drained)
		e.eventually("a registered EQ0's entries", func() bool {
			cp, ok, err := c.LogCaptureOf(ctx, catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "EQ0", File: "host-20261107.log"}, Device: "eqp"})
			return err == nil && ok && cp.HasHighestSeq
		})
		stopA()
		if err := c.Leave(ctx, "a"); err != nil {
			t.Fatal(err)
		}
		b := e.stage("b", time.Second) // b's files go idle after a second
		run(ctx, &wg, b)
		key := catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "EQ0", File: "host-20261107.log"}, Device: "eqp"}
		e.eventually("b stops EQ0's idle file", func() bool {
			cp, _, err := c.LogCaptureOf(ctx, key)
			return err == nil && cp.State == catalog.StateStopped
		})
	})
}

// checkCaptures checks that each of n equipment's capture holds its start and its entries up to seq highest.
func checkCaptures(t *testing.T, c *catalog.Catalog, n int, highest int64) {
	t.Helper()
	for eq := range n {
		key := catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: fmt.Sprintf("EQ%d", eq), File: "host-20261107.log"}, Device: "eqp"}
		cp, ok, err := c.LogCaptureOf(t.Context(), key)
		if err != nil || !ok || !cp.HasHighestSeq || cp.HighestSeq != highest {
			t.Errorf("EQ%d's capture = %+v, %v, %v; want its entries up to seq %d", eq, cp.Capture, ok, err, highest)
		}
	}
}

// ackPending is the entries the feed's group consumers delivered and hold unacknowledged.
func (e *stageEnv) ackPending() int {
	n := 0
	l := e.s.ListConsumers(e.t.Context())
	for ci := range l.Info() {
		n += ci.NumAckPending
	}
	if err := l.Err(); err != nil {
		e.t.Fatal(err)
	}

	return n
}

// A replica stopped holding every group's entries unflushed is taken over (logfeed.md §15.5 *Taking a lease*):
// the replica taking the groups gets those older entries again with a newer backlog, and converts each once.
func TestGateTakeoverWithHeldEntriesAndBacklog(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		a := e.stage("a", time.Hour)
		a.Base.Flush = time.Hour // a holds what it receives
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		stopA := run(ctx, &wg, a)
		e.eventually("a takes every group", func() bool { return len(a.Held()) == e.feed.Groups })
		e.publish(8, 0)
		e.eventually("a holds the entries", func() bool { return e.ackPending() == 24 })
		stopA()
		if err := c.Leave(ctx, "a"); err != nil {
			t.Fatal(err)
		}
		e.publish(8, 1)
		b := e.stage("b", time.Hour)
		run(ctx, &wg, b)
		e.eventually("b drains the older entries and the backlog", e.drained)
		checkCaptures(t, c, 8, 6)
	})
}

// A publish retried under its message id is stored once, and converted once (logfeed.md §15.3).
func TestGateRetriedPublish(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		f := &fixture{t: e.t}
		raw := f.raw(map[string]any{"equipment_id": "EQ0", "kind": KindUnclassified, "text": "line", "ts": time.Now().UTC().Format("2006-01-02 15:04:05.000"), "tz": "UTC"})
		for range 2 {
			if _, err := e.js.Publish(ctx, "tklog.EQ0", raw, jetstream.WithMsgID("feed-a/EQ0/host-20261107.log/0")); err != nil {
				t.Fatal(err)
			}
		}
		if info, err := e.s.Info(ctx); err != nil || info.State.Msgs != 1 {
			t.Fatalf("stream state %+v, %v; want the retried publish stored once", info.State, err)
		}
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, e.stage("a", time.Hour))
		e.eventually("the feed drained", e.drained)
		checkCaptures(t, c, 1, 1)
	})
}
