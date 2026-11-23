package logfeed

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
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

// stageEnv is a feed of 4 group streams on an embedded bus over c, and a builder of replicas' stages sharing one store.
type stageEnv struct {
	t    *testing.T
	c    *catalog.Catalog
	js   jetstream.JetStream
	feed bus.Feed
	cfg  int64
	st   store.ObjectStore
	// servers is the cluster's, when the bus is one.
	servers []*server.Server
}

// newStageEnv starts the bus, one server or, clustered, three, each mapping the feed's subjects,
// the group streams placed on the three in turn by their tags as migrate places them (logfeed.md §15.4).
func newStageEnv(t *testing.T, c *catalog.Catalog, clustered bool) *stageEnv {
	t.Helper()
	ctx := t.Context()
	feed := bus.Feed{Stream: "TRACEKEEP_LOGFEED", Prefix: "tklog", Groups: 4, MaxBytes: 64 << 20, Duplicates: 2 * time.Minute,
		Replicas: 1, AckWait: 10 * time.Second, MaxAckPending: 10_000}
	var url string
	var srvs []*server.Server
	if clustered {
		srvs = testbus.StartClusterServers(t, 3)
		urls := make([]string, len(srvs))
		for i, s := range srvs {
			if err := testbus.MapFeed(s, feed.Prefix, feed.Groups); err != nil {
				t.Fatal(err)
			}
			urls[i], feed.PlacementTags = s.ClientURL(), append(feed.PlacementTags, s.Name())
		}
		url = strings.Join(urls, ",")
	} else {
		srv := testbus.StartServer(t, testbus.Options{})
		srv.MapFeed(feed.Prefix, feed.Groups)
		url = srv.URL()
	}
	nc, err := nats.Connect(url)
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
	// A stream just created on a cluster elects its leader after the creation returns.
	for n := range feed.Groups {
		s, err := js.Stream(ctx, bus.FeedStreamName(feed, n))
		if err != nil {
			t.Fatal(err)
		}
		testbus.AwaitStream(t, js, s)
	}
	cfg, err := c.ActivateConfig(ctx, []byte(`{"classifier":"go-secs/v2.8.0","max_frame_len":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SyncLogGroups(ctx, feed.Groups); err != nil {
		t.Fatal(err)
	}

	return &stageEnv{t: t, c: c, js: js, feed: feed, cfg: cfg, st: store.NewMem(), servers: srvs}
}

// layouts runs f on one server and on a three-node cluster, each on both engines.
func layouts(t *testing.T, f func(t *testing.T, e *stageEnv)) {
	for _, clustered := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "clustered"}[clustered], func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) { f(t, newStageEnv(t, c, clustered)) })
		})
	}
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
	eqs := make([]int, n)
	for i := range eqs {
		eqs[i] = i
	}
	e.publishOf(eqs, round)
}

// publishOf is publish for the equipment eqs.
func (e *stageEnv) publishOf(eqs []int, round int) {
	e.t.Helper()
	taipei, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		e.t.Fatal(err)
	}
	ts := time.Now().In(taipei).Format("2006-01-02 15:04:05.000")
	for _, eq := range eqs {
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
	layouts(t, func(t *testing.T, e *stageEnv) {
		ctx, c := t.Context(), e.c
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
		e := newStageEnv(t, c, false)
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
		e := newStageEnv(t, c, false)
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

// A stage whose check found a group stream unable to answer balances the groups as one whose check passed, beside a replica with no check,
// the two holding every group, and retries the check until it passes; a refusal from the check ends Run (logfeed.md §15.4).
func TestStageRunsWhileTheCheckFails(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c, false)
		var back atomic.Bool
		var checks atomic.Int32
		a, b := e.stage("a", time.Hour), e.stage("b", time.Hour)
		a.Check = func(context.Context) error {
			checks.Add(1)
			if back.Load() {
				return nil
			}

			return fmt.Errorf("%w: a test stream", bus.ErrFeedUnavailable)
		}
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, a)
		run(ctx, &wg, b)
		e.eventually("a and b hold every group between them, each its share, started", func() bool {
			ha, hb := a.Held(), b.Held()
			return len(ha) == e.feed.Groups/2 && len(hb) == e.feed.Groups/2 && a.Ready(ctx) == nil && b.Ready(ctx) == nil
		})
		e.eventually("a retried its check", func() bool { return checks.Load() > 2 })
		back.Store(true)
		e.eventually("a's check passed", func() bool { n := checks.Load(); time.Sleep(a.LeaseTTL); return checks.Load() == n })

		r := e.stage("r", time.Hour)
		r.Check = func(context.Context) error { return fmt.Errorf("%w: a test mapping", bus.ErrFeedMapping) }
		if err := r.Run(ctx); !errors.Is(err, bus.ErrFeedMapping) {
			t.Errorf("Run under a refusing check = %v, want ErrFeedMapping", err)
		}
	})
}

// A replica taking a group restores its open captures: a file of it goes idle and stops though no entry of it arrives.
func TestStageRestoresOpenCaptures(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c, false)
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
	e.convertedOf(slices.Collect(func(yield func(int) bool) {
		for eq := range n {
			if !yield(eq) {
				return
			}
		}
	}), rounds, perRound)
}

// convertedOf is converted for the equipment eqs.
func (e *stageEnv) convertedOf(eqs []int, rounds, perRound int) {
	e.t.Helper()
	ctx := e.t.Context()
	h := &harness{t: e.t, c: e.c, store: e.st}
	for _, eq := range eqs {
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
	layouts(t, func(t *testing.T, e *stageEnv) {
		ctx, c := t.Context(), e.c
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
	layouts(t, func(t *testing.T, e *stageEnv) {
		ctx := t.Context()
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

// lossyDialer dials the stage's connection; lose makes its current connection read and discard everything the server sends,
// as a server's slow-consumer closure loses what it had sent and the client had not read, until drop closes it and the client reconnects.
type lossyDialer struct {
	mu   sync.Mutex
	conn *lossyConn
}

type lossyConn struct {
	net.Conn
	lost atomic.Bool
}

func (d *lossyDialer) Dial(network, address string) (net.Conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conn = &lossyConn{Conn: c}

	return d.conn, nil
}

func (d *lossyDialer) current() *lossyConn {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.conn
}

func (c *lossyConn) Read(b []byte) (int, error) {
	for {
		n, err := c.Conn.Read(b)
		if err != nil || !c.lost.Load() {
			return n, err
		}
	}
}

// A drop of the stage's connection loses the deliveries in flight (T-69): every group gives up, readiness names each stream,
// and the groups taken again re-create their consumers, so the lost entries come back at once, ahead of the newer ones,
// not after ack_wait and stale; each entry is converted once.
func TestGateConnectionDropLosesNoEntry(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c, false)
		d := &lossyDialer{}
		nc, err := nats.Connect(e.js.Conn().ConnectedUrl(), nats.SetCustomDialer(d), nats.ReconnectWait(10*time.Millisecond), nats.MaxReconnects(-1))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		a := e.stage("a", time.Hour)
		a.JS, a.Conn, a.Pause = js, nc, 2*time.Second
		srv := metrics.NewServer()
		if a.Base.Metrics, err = metrics.NewLogfeed(srv); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, a)
		e.eventually("a ready on every group", func() bool { return len(a.Held()) == e.feed.Groups && a.Ready(ctx) == nil })
		created := map[int]time.Time{}
		pending := func() int {
			sum := 0
			for n := range e.feed.Groups {
				cons, err := e.js.Consumer(ctx, bus.FeedStreamName(e.feed, n), bus.FeedConsumer(n))
				if err != nil {
					t.Fatal(err)
				}
				info, err := cons.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := created[n]; !ok {
					created[n] = info.Created
				}
				sum += info.NumAckPending
			}

			return sum
		}
		pending()
		d.current().lost.Store(true)
		e.publish(8, 0)
		e.eventually("the server sent every entry to the lossy connection", func() bool { return pending() == 24 })
		_ = d.current().Close()
		e.eventually("readiness names every group's stream", func() bool {
			var re *metrics.ReadyError
			err := a.Ready(ctx)
			return errors.As(err, &re) && strings.Contains(err.Error(), "gave up") && len(re.Parts) == e.feed.Groups
		})
		e.publish(8, 1)
		// No time bound: a lost entry back through ack_wait instead of a re-created consumer would be stale, which the scrape below refuses.
		e.eventually("a drains the lost entries and the newer ones", e.drained)
		e.eventually("a holds every group again", func() bool { return len(a.Held()) == e.feed.Groups })
		for n := range e.feed.Groups {
			cons, err := e.js.Consumer(ctx, bus.FeedStreamName(e.feed, n), bus.FeedConsumer(n))
			if err != nil {
				t.Fatal(err)
			}
			if info := cons.CachedInfo(); !info.Created.After(created[n]) {
				t.Errorf("group %d's consumer created %v, not after the drop's %v", n, info.Created, created[n])
			}
		}
		e.converted(8, 2, 3)
		if got := scrape(t, srv); strings.Contains(got, `outcome="stale"`) {
			t.Errorf("entries stale after the drop:\n%s", got)
		}
	})
}

// A fetch's byte bound is at least the largest delivery the server sends, so a pull never ends before a message larger than the group's share of the budget.
func TestStageFetchBytesTakeTheLargestDelivery(t *testing.T) {
	t.Parallel()
	srv := testbus.StartServer(t, testbus.Options{})
	nc, err := nats.Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	s := &Stage{JS: js, Feed: bus.Feed{Groups: 32}}
	if got, want := s.fetchBytes(), int(nc.MaxPayload())+deliverySlack; got != want || got*s.Feed.Groups >= 64<<20 {
		t.Errorf("fetchBytes() = %d at G = 32; want %d, the 32 fetches within the server's 64 MiB pending", got, want)
	}
}

// A delivery larger than a fetch's bytes is never sent by a bounded pull, and the client does not report it:
// after a bounded fetch delivered nothing, the stage takes one message unbounded, so a group whose every entry exceeds the bound still converts.
func TestStageFetchesADeliveryPastTheBound(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		e := newStageEnv(t, c, false)
		e.publish(4, 0)
		a := e.stage("a", time.Hour)
		a.fetchMax = 64
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(t.Context(), &wg, a)
		e.eventually("the feed drained", e.drained)
		e.converted(4, 1, 3)
	})
}

// The generation check fails once the connection reconnected, whatever its status since, and holds on the generation read after it.
func TestStageSameConnAfterReconnect(t *testing.T) {
	t.Parallel()
	srv := testbus.StartServer(t, testbus.Options{})
	nc, err := nats.Connect(srv.URL(), nats.ReconnectWait(10*time.Millisecond), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	s := &Stage{Conn: nc}
	gen := s.reconnects()
	if err := s.sameConn(gen); err != nil {
		t.Fatalf("sameConn before a reconnect = %v", err)
	}
	if err := nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for nc.Status() != nats.CONNECTED || s.reconnects() == gen {
		if time.Now().After(deadline) {
			t.Fatalf("not reconnected within 10 s: %v, %d reconnects", nc.Status(), s.reconnects())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.sameConn(gen); !errors.Is(err, errReconnected) {
		t.Errorf("sameConn on the generation before the reconnect = %v, want errReconnected", err)
	}
	if err := s.sameConn(s.reconnects()); err != nil {
		t.Errorf("sameConn on the generation after it = %v", err)
	}
}

// A server lost with a group's stream of one replica stalls that group alone (logfeed.md §15.4, T-68):
// the replica gives the group up and readiness names its stream, while the other groups' streams, on the other servers, are converted.
// A lost server's restart redelivering what it held rests on the bus, probed in the feed stream a group plan's step 1.
func TestStageServerLost(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := newStageEnv(t, c, true)
		e.publish(8, 0)
		// The group placed on tb-1, its stream's equipment, and the others'.
		lost := slices.Index(e.feed.PlacementTags, "tb-1")
		var gone, live []int
		for n := range e.feed.Groups {
			s, err := e.js.Stream(ctx, bus.FeedStreamName(e.feed, n))
			if err != nil {
				t.Fatal(err)
			}
			info, err := s.Info(ctx, jetstream.WithSubjectFilter(">"))
			if err != nil {
				t.Fatal(err)
			}
			for subject := range info.State.Subjects {
				var eq int
				if _, err := fmt.Sscanf(subject, fmt.Sprintf("tklog.%d.EQ%%d", n), &eq); err != nil {
					t.Fatal(subject, err)
				}
				if n == lost {
					gone = append(gone, eq)
				} else {
					live = append(live, eq)
				}
			}
		}
		if len(gone) == 0 || len(live) == 0 {
			t.Fatalf("equipment of the lost group %v, of the others %v: want some of each", gone, live)
		}
		a := e.stage("a", time.Hour)
		var wg sync.WaitGroup
		t.Cleanup(wg.Wait)
		run(ctx, &wg, a)
		e.eventually("a holds every group, started", func() bool { return len(a.Held()) == e.feed.Groups && a.Ready(ctx) == nil })
		e.eventually("the feed drained", e.drained)
		// The server lost while the stage runs; the client's connection, if to it, reconnects to another before the next round.
		e.servers[lost].Shutdown()
		e.eventually("the client connected to a live server", func() bool {
			nc := e.js.Conn()
			return nc.IsConnected() && nc.ConnectedServerName() != e.servers[lost].Name()
		})
		e.publishOf(live, 1)
		lostStream := bus.FeedStreamName(e.feed, lost)
		e.eventually("the lost group given up, its lease released, and readiness naming its stream", func() bool {
			var re *metrics.ReadyError
			return errors.As(a.Ready(ctx), &re) && slices.Equal(re.Parts, []string{lostStream}) && !slices.Contains(a.Held(), lost)
		})
		e.eventually("the other groups' streams drained", func() bool {
			for _, st := range bus.FeedStates(ctx, e.js, e.feed) {
				if st.Group != lost && (st.Err != nil || st.Messages != 0) {
					return false
				}
			}

			return true
		})
		e.convertedOf(live, 2, 3)
		e.convertedOf(gone, 1, 3)
	})
}
