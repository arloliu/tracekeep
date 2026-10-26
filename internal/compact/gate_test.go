package compact

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// The sole parked copy is acknowledged at exactly LastSeq, the process crashes before the catalog update and nothing is published after;
// after a restart of the bus and of the process, the reconciler marks its row acked from the stream's answer alone.
func TestSoleCopyReconciledAfterARestart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		srv := testbus.StartServer(t, testbus.Options{})
		js := connect(t, srv.URL())
		_, parked, err := bus.Ensure(ctx, js, bus.Config{SubjectPrefix: "tk", ClaimBucket: "claims"})
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, c, catalog.RoleParking, parked)
		if err != nil {
			t.Fatal(err)
		}
		cons, err := parked.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "t", AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatal(err)
		}
		b := &streams{js: js, parked: parked, inc: inc, cons: cons}
		capture := newID(t)
		d := b.copyOf(t, c, capture, 1)
		if _, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil {
			t.Fatal(err)
		}
		b.acknowledge(t) // DoubleAck confirmed; the process dies before AckCopy

		srv.Stop()
		srv.Restart()
		js = connect(t, srv.URL())
		cfg := bus.Config{SubjectPrefix: "tk", ClaimBucket: "claims"}
		_, parked, err = bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if last := parked.CachedInfo().State.LastSeq; last != uint64(d.StreamSeq) { //nolint:gosec // a stream sequence fits
			t.Fatalf("LastSeq %d, want the acknowledged copy's %d", last, d.StreamSeq)
		}
		r := &Reconciler{
			Catalog: c, Store: store.NewMem(), HorizonHour: func() int64 { return hour - 24 }, Log: slog.New(slog.DiscardHandler),
			JS: js, Bus: cfg, Holder: "restarted", LeaseTTL: time.Hour, Interval: time.Minute,
		}
		step(t, r)
		if got := copyState(t, c, d); got != "acked" {
			t.Errorf("copy %s, want acked after the restart", got)
		}
	})
}

// A handle created while AllowDirect was on refreshes itself before reading,
// so its GetMsg goes to the leader-served API, asserted on the request subject, never to a direct get.
func TestHandleCreatedWithAllowDirectReadsThroughTheAPI(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		url := testbus.Start(t)
		js := connect(t, url)
		_, parked, err := bus.Ensure(ctx, js, bus.Config{SubjectPrefix: "tk", ClaimBucket: "claims"})
		if err != nil {
			t.Fatal(err)
		}
		name := parked.CachedInfo().Config.Name
		setDirect(t, js, name, true)
		stale, err := js.Stream(ctx, name) // its cached configuration says AllowDirect
		if err != nil || !stale.CachedInfo().Config.AllowDirect {
			t.Fatalf("handle = %v, %v; want one created with AllowDirect on", stale, err)
		}
		setDirect(t, js, name, false)

		inc, err := bus.Incarnate(ctx, c, catalog.RoleParking, parked)
		if err != nil {
			t.Fatal(err)
		}
		cons, err := parked.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "t", AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatal(err)
		}
		b := &streams{js: js, parked: parked, inc: inc, cons: cons}
		capture := newID(t)
		d := b.copyOf(t, c, capture, 1) // held: the read must find it
		if _, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil {
			t.Fatal(err)
		}

		watch, err := nats.Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(watch.Close)
		api, err := watch.SubscribeSync("$JS.API.STREAM.MSG.GET." + name)
		if err != nil {
			t.Fatal(err)
		}
		direct, err := watch.SubscribeSync("$JS.API.DIRECT.GET.>")
		if err != nil {
			t.Fatal(err)
		}
		if err := watch.Flush(); err != nil {
			t.Fatal(err)
		}
		if n, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, stale); err != nil || n != 0 {
			t.Fatalf("Copies = %d, %v; want the held copy left", n, err)
		}
		if _, err := api.NextMsg(5 * time.Second); err != nil {
			t.Errorf("no request on the API's subject: %v", err)
		}
		if n, _, _ := direct.Pending(); n != 0 {
			t.Errorf("%d direct gets", n)
		}
		if got := copyState(t, c, d); got != "waiting" {
			t.Errorf("copy %s, want waiting", got)
		}
	})
}

// On a clustered bus, the reconciler connected to a server that is not the stream's leader reads through the leader:
// a copy the leader still holds is never marked, whatever a replica has received, and an acknowledged one is.
func TestCopiesFromAFollower(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		urls := strings.Split(testbus.StartCluster(t, 3), ",")
		js := connect(t, strings.Join(urls, ","))
		cfg := busconfig.ParkedStreamConfig(busconfig.DefaultParkedStream, busconfig.DefaultParkPrefix, 3)
		s, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		testbus.AwaitStream(t, js, s)
		leader := s.CachedInfo().Cluster.Leader
		var follower string
		for i, u := range urls {
			if leader != "tb-"+string(rune('0'+i)) {
				follower = u
				break
			}
		}
		fjs := connect(t, follower)
		fs, err := fjs.Stream(ctx, cfg.Name)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, c, catalog.RoleParking, fs)
		if err != nil {
			t.Fatal(err)
		}
		cons, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "t", AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatal(err)
		}
		b := &streams{js: js, parked: s, inc: inc, cons: cons}
		capture := newID(t)
		acked := b.copyOf(t, c, capture, 1)
		b.acknowledge(t)
		held := b.copyOf(t, c, capture, 2)
		if _, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil {
			t.Fatal(err)
		}
		if n, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, fs); err != nil || n != 1 {
			t.Fatalf("Copies through a follower = %d, %v; want the acknowledged copy only", n, err)
		}
		if a, h := copyState(t, c, acked), copyState(t, c, held); a != "acked" || h != "waiting" {
			t.Errorf("copies %s and %s, want acked and waiting", a, h)
		}
	})
}

// An adoption that meets ingest's own registration of the same pack, committed between the listing and the adoption, does nothing:
// the registration that committed first won (ingest.md §6.3).
func TestAdoptionAfterIngestRegistered(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		capture := f.admit(t)
		b := segment(t, capture, []uint64{1, 2}, false)
		f.put(t, b.key, b.data)
		f.pass(t)
		p := &catalog.Pack{
			ID: b.id, Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleSegment, Key: b.key, Size: int64(len(b.data)),
			SeqStart: 1, FormatMinor: 1, Ranges: []catalog.Range{{SeqFrom: 1, SeqTo: 2}},
		}
		ev := &catalog.Evidence{Deliveries: []catalog.Identity{{Version: catalog.VersionKey{Capture: capture, Seq: 1}, Incarnation: 1, StreamSeq: 2}}}
		// The hold point: the reconciler has read the key unknown and opens the object; ingest's registration commits now.
		f.r.Store = &opening{ObjectStore: f.s, at: b.key, hook: func() {
			if out, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: ev}); err != nil || out != catalog.Registered {
				t.Errorf("ingest's registration = %v, %v", out, err)
			}
		}}
		rep := f.pass(t)
		if len(rep.Adopted)+len(rep.Deleted)+len(rep.Quarantined)+len(rep.Unadmitted) != 0 {
			t.Errorf("pass = %+v, want nothing done", rep)
		}
		if got, err := c.KeyStanding(ctx, b.key); err != nil || got != catalog.KeyRegistered || !f.present(t, b.key) {
			t.Errorf("standing %v, %v; want ingest's registration kept with its object", got, err)
		}
	})
}

// opening runs hook once, when the object under at is opened.
type opening struct {
	store.ObjectStore
	at   string
	hook func()
}

func (o *opening) Open(ctx context.Context, key string) (store.Object, error) {
	if key == o.at && o.hook != nil {
		o.hook()
		o.hook = nil
	}

	return o.ObjectStore.Open(ctx, key)
}

func connect(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	return js
}

func setDirect(t *testing.T, js jetstream.JetStream, name string, on bool) {
	t.Helper()
	s, err := js.Stream(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	cfg := s.CachedInfo().Config
	cfg.AllowDirect = on
	if _, err := js.UpdateStream(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
}
