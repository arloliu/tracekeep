package logfeed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// stageEnv is a feed stream of 4 groups on an embedded bus over c, and a builder of replicas' stages sharing one store.
type stageEnv struct {
	t    *testing.T
	c    *catalog.Catalog
	js   jetstream.JetStream
	feed bus.Feed
	cfg  int64
	st   store.ObjectStore
}

func newStageEnv(t *testing.T, c *catalog.Catalog) *stageEnv {
	t.Helper()
	ctx := t.Context()
	feed := bus.Feed{Stream: "TRACEKEEP_LOGFEED", Prefix: "tklog", Groups: 4, MaxBytes: 64 << 20, Duplicates: 2 * time.Minute,
		Replicas: 1, AckWait: 10 * time.Second, MaxAckPending: 10_000}
	srv := testbus.StartServer(t, testbus.Options{})
	srv.MapFeed(feed.Prefix, feed.Groups)
	nc, err := nats.Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureFeed(ctx, js, feed, 0, busconfig.StreamConfig("TRACEKEEP_TRAFFIC", "tk"), busconfig.ParkedStreamConfig("TRACEKEEP_PARKED", "tkpark", 1)); err != nil {
		t.Fatal(err)
	}
	cfg, err := c.ActivateConfig(ctx, []byte(`{"classifier":"go-secs/v2.8.0","max_frame_len":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SyncLogGroups(ctx, feed.Groups); err != nil {
		t.Fatal(err)
	}

	return &stageEnv{t: t, c: c, js: js, feed: feed, cfg: cfg, st: store.NewMem()}
}

// stage registers replica's membership and returns its stage, whose files go idle after idle.
func (e *stageEnv) stage(replica string, idle time.Duration) *Stage {
	e.t.Helper()
	if _, err := e.c.Register(e.t.Context(), replica, []int64{e.cfg}, time.Hour); err != nil {
		e.t.Fatal(err)
	}

	return &Stage{
		Catalog: e.c, JS: e.js, Conn: e.js.Conn(), Feed: e.feed, Replica: replica, LeaseTTL: 900 * time.Millisecond, FetchBatch: 100, Pause: time.Second,
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
		for k := range 3 {
			raw := f.raw(map[string]any{"equipment_id": fmt.Sprintf("EQ%d", eq), "kind": KindUnclassified, "text": fmt.Sprintf("r%d-%d", round, k), "ts": ts})
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
	msgs, _, err := bus.FeedBacklog(e.t.Context(), e.js, e.feed)
	if err != nil {
		e.t.Fatal(err)
	}

	return msgs == 0
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
		e.converted(8, 2, 3)
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
		e.converted(8, 1, 3)
	})
}

// A group whose stream cannot be read gives up, and readiness names the group's stream until the pause has passed.
func TestStageReadinessNamesTheStream(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		lost := bus.FeedStreamName(e.feed, 2)
		if err := e.js.DeleteStream(ctx, lost); err != nil {
			t.Fatal(err)
		}
		a := e.stage("a", time.Hour)
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, a)
		e.eventually("readiness names the lost stream", func() bool {
			var re *metrics.ReadyError
			err := a.Ready(ctx)
			return errors.As(err, &re) && strings.Contains(err.Error(), "gave up") && slices.Equal(re.Parts, []string{lost})
		})
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

// converted checks each of n equipment's capture holds its start, then the first perRound entries of each round publish published, in order, each once,
// and its file's watermark the last one's offset.
func (e *stageEnv) converted(n, rounds, perRound int) {
	e.t.Helper()
	ctx := e.t.Context()
	h := &harness{t: e.t, c: e.c, store: e.st}
	for eq := range n {
		file := catalog.LogFile{Source: "feed-a", EquipmentID: fmt.Sprintf("EQ%d", eq), File: "host-20261107.log"}
		cp, ok, err := e.c.LogCaptureOf(ctx, catalog.LogKey{LogFile: file, Device: "eqp"})
		if err != nil || !ok {
			e.t.Errorf("EQ%d's capture = %v, %v", eq, ok, err)
			continue
		}
		var got []string
		want := make([]string, 0, rounds*perRound)
		for r := range rounds {
			for k := range perRound {
				want = append(want, fmt.Sprintf("r%d-%d", r, k))
			}
		}
		for _, r := range h.records(cp.ID) {
			if a, err := tracepack.UnmarshalAnnotation(r.Payload); r.Kind == tracepack.KindAnnotation && err == nil && a.Text != nil {
				got = append(got, *a.Text)
			}
		}
		if !slices.Equal(got, want) || cp.HighestSeq != int64(len(want)) {
			e.t.Errorf("EQ%d's entries %v, highest_seq %d; want %v", eq, got, cp.HighestSeq, want)
		}
		w := int64(rounds-1)*1000 + int64(perRound-1)*100
		if st, err := e.c.LogFileState(ctx, file); err != nil || st.Watermark != w {
			e.t.Errorf("EQ%d's file state = %+v, %v; want watermark %d", eq, st, err, w)
		}
	}
}

// A replica that died holding every group's deliveries unacknowledged is taken over (logfeed.md §15.5 *Taking a lease*):
// the replica taking the groups once their leases lapse re-creates each consumer, so the held entries come back at once, ahead of the newer backlog,
// not after ack_wait; each is converted once, and the dead holder's acknowledgements arriving late change nothing.
func TestGateTakeoverWithHeldEntriesAndBacklog(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		for n := range e.feed.Groups {
			if _, ok, err := c.TakeLease(ctx, catalog.LogGroupLease(n), "dead", 900*time.Millisecond); err != nil || !ok {
				t.Fatal(ok, err)
			}
		}
		e.publish(8, 0)
		var held []jetstream.Msg
		for n := range e.feed.Groups {
			cons, err := e.js.Consumer(ctx, bus.FeedStreamName(e.feed, n), bus.FeedConsumer(n))
			if err != nil {
				t.Fatal(err)
			}
			batch, err := cons.Fetch(100, jetstream.FetchMaxWait(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			for m := range batch.Messages() {
				held = append(held, m)
			}
		}
		if len(held) != 24 {
			t.Fatalf("the dead holder holds %d entries, want 24", len(held))
		}
		e.publish(8, 1)
		start := time.Now()
		b := e.stage("b", time.Hour)
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, b)
		e.eventually("b drains the held entries and the backlog", e.drained)
		if took := time.Since(start); took >= e.feed.AckWait {
			t.Errorf("the held entries came back after %v, not before ack_wait %v: the consumer was not re-created", took, e.feed.AckWait)
		}
		for _, m := range held {
			_ = m.Ack()
		}
		e.converted(8, 2, 3)
	})
}

// A publish retried under its message id is stored once, and converted once (logfeed.md §15.3).
func TestGateRetriedPublish(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c)
		f := &fixture{t: e.t}
		raw := f.raw(map[string]any{"equipment_id": "EQ0", "kind": KindUnclassified, "text": "r0-0", "ts": time.Now().UTC().Format("2006-01-02 15:04:05.000"), "tz": "UTC"})
		for range 2 {
			if _, err := e.js.Publish(ctx, "tklog.EQ0", raw, jetstream.WithMsgID("feed-a/EQ0/host-20261107.log/0")); err != nil {
				t.Fatal(err)
			}
		}
		if msgs, _, err := bus.FeedBacklog(ctx, e.js, e.feed); err != nil || msgs != 1 {
			t.Fatalf("the feed holds %d, %v; want the retried publish stored once", msgs, err)
		}
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, e.stage("a", time.Hour))
		e.eventually("the feed drained", e.drained)
		e.converted(1, 1, 1)
	})
}
