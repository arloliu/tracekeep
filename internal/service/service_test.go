package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/migrations"
)

// load is the configuration of a test: the bus at url, an fs store under a temporary directory, everything else defaulted.
func load(t *testing.T, url string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	yaml := "catalog:\n  dsn: " + filepath.Join(dir, "catalog.db") + "\nbus:\n  url: " + url +
		"\n  subject_prefix: tk\n  claim_bucket: claims\nstore:\n  kind: fs\n  dir: " + dir + "\nreplica: r1\n"
	path := filepath.Join(dir, "tracekeep.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	return c
}

// env is an environment over the catalog c, a fresh embedded bus and an in-memory store, migrated.
func env(t *testing.T, c *catalog.Catalog) *Env {
	t.Helper()

	return envOn(t, c, testbus.Start(t), nil)
}

// feedEnv is env with the feed enabled in groups lease groups, the bus mapping its subjects as the servers' configuration does.
func feedEnv(t *testing.T, c *catalog.Catalog, groups int) *Env {
	t.Helper()
	srv := testbus.StartServer(t, testbus.Options{})

	return envOn(t, c, srv.URL(), func(e *Env) {
		l := &e.Config.Logfeed
		// A small limit: JetStream reserves a stream's max_bytes, and a test server's storage is far below the default 40 GiB.
		l.Enabled, l.LeaseGroups, l.MaxBytes = true, groups, 64<<20
		srv.MapFeed(l.Prefix, l.LeaseGroups)
	})
}

// envOn is env on the bus at url, adjusted by adjust before migrate unless it is nil.
func envOn(t *testing.T, c *catalog.Catalog, url string, adjust func(*Env)) *Env {
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
	// A test binary carries no dependency versions, so its classifier names none, as a dev build's.
	e := &Env{Config: load(t, url), Log: slog.New(slog.DiscardHandler), Catalog: c, NC: nc, JS: js, Store: store.NewMem(), AllowUnversioned: true, Dev: true}
	if adjust != nil {
		adjust(e)
	}
	if err := Migrate(t.Context(), e); err != nil {
		t.Fatal(err)
	}

	return e
}

// migrate ensures the streams and the bucket and records both incarnations; it can run again.
func TestMigrate(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := env(t, c)
		if err := Migrate(ctx, e); err != nil {
			t.Fatalf("Migrate again = %v", err)
		}
		for _, role := range []string{catalog.RoleTraffic, catalog.RoleParking} {
			s, err := e.JS.Stream(ctx, bus.StreamName(e.Bus(), role))
			if err != nil {
				t.Fatalf("%s stream: %v", role, err)
			}
			if _, err := c.Incarnate(ctx, role, s.CachedInfo().Config.Name, s.CachedInfo().Created.UnixNano()); err != nil {
				t.Errorf("%s incarnation: %v", role, err)
			}
		}
		if _, err := e.JS.ObjectStore(ctx, "claims"); err != nil {
			t.Errorf("claim bucket: %v", err)
		}
	})
}

// With the feed enabled, migrate also ensures the group streams, their consumers and the groups' lease rows, and can run again;
// it refuses a change of G while a group stream holds a message.
func TestMigrateFeed(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := feedEnv(t, c, 4)
		if err := Migrate(ctx, e); err != nil {
			t.Fatalf("Migrate again with the feed = %v", err)
		}
		if _, err := e.JS.Consumer(ctx, bus.FeedStreamName(e.Feed(), 3), bus.FeedConsumer(3)); err != nil {
			t.Errorf("group 3's consumer: %v", err)
		}
		old, ok, err := c.TakeLease(ctx, catalog.LogGroupLease(1), "r", time.Minute)
		if err != nil || !ok {
			t.Fatalf("group 1's lease = %v, %v; want its row", ok, err)
		}
		if _, err := e.JS.Publish(ctx, "tklog.T1", []byte("{}")); err != nil {
			t.Fatal(err)
		}
		e.Config.Logfeed.LeaseGroups = 2
		if err := Migrate(ctx, e); !errors.Is(err, bus.ErrFeedNotDrained) {
			t.Fatalf("Migrate changing G over a message = %v, want ErrFeedNotDrained", err)
		}
		for n := range 4 {
			s, err := e.JS.Stream(ctx, bus.FeedStreamName(e.Feed(), n))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Purge(ctx); err != nil {
				t.Fatal(err)
			}
		}

		// A change of G whose streams changed and whose catalog did not, as a migrate failing between the two leaves it, completes on the next run.
		tc := func(role string) jetstream.StreamConfig {
			s, err := e.JS.Stream(ctx, bus.StreamName(e.Bus(), role))
			if err != nil {
				t.Fatal(err)
			}

			return s.CachedInfo().Config
		}
		if err := bus.EnsureFeed(ctx, e.JS, e.Feed(), 4, tc(catalog.RoleTraffic), tc(catalog.RoleParking)); err != nil {
			t.Fatal(err)
		}
		if err := Migrate(ctx, e); err != nil {
			t.Fatalf("Migrate after the change of G = %v", err)
		}
		if err := c.RenewLease(ctx, old, time.Minute); !errors.Is(err, catalog.ErrLeaseLost) {
			t.Errorf("the old G's holder renewing = %v, want ErrLeaseLost", err)
		}
		if _, ok, err := c.TakeLease(ctx, catalog.LogGroupLease(3), "r", time.Minute); !errors.Is(err, catalog.ErrUnknownLease) || ok {
			t.Errorf("a retired group's lease = %v, %v; want ErrUnknownLease", ok, err)
		}
		if _, err := e.JS.Stream(ctx, bus.FeedStreamName(e.Feed(), 3)); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("a retired group's stream = %v, want none", err)
		}
	})
}

// With the feed enabled, ingest runs the log stage: entries published on the feed's subject, which the servers' mapping routes to its group's stream,
// become a log capture.
func TestIngestConvertsTheFeed(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := feedEnv(t, c, 2)
		l := &e.Config.Logfeed
		l.ReorderWindow, l.FlushInterval = 100*time.Millisecond, 200*time.Millisecond
		run, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- Ingest(run, e) }()
		defer func() {
			stop()
			<-done //nolint:errcheck // the role's end, its result not checked here
		}()
		entry := `{"v":1,"equipment_id":"T1","source":"feed-a","tz":"UTC","file":"host.log","offset":%d,"ts":"2026-11-07 08:00:0%d","kind":"unclassified","text":"line"}`
		for i := range 3 {
			if _, err := e.JS.Publish(ctx, "tklog.T1", fmt.Appendf(nil, entry, 100*i, i)); err != nil {
				t.Fatal(err)
			}
		}
		key := catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host.log"}, Device: "eqp"}
		deadline := time.Now().Add(20 * time.Second)
		for {
			cp, ok, err := c.LogCaptureOf(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if ok && cp.HasHighestSeq && cp.HighestSeq == 3 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the feed's capture = %+v, %v after 20 s; want its start and 3 entries registered", cp.Capture, ok)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

// With the feed enabled, a role refuses to start until migrate has made the feed's stream.
func TestRolesNeedTheFeed(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		e := env(t, c)
		e.Config.Logfeed.Enabled = true
		if err := Compact(t.Context(), e); err == nil || !strings.Contains(err.Error(), "feed stream") {
			t.Errorf("compact without the feed's stream = %v, want refused", err)
		}
	})
}

// Migrate, ingest and compact refuse to start against a traffic stream that allows direct gets, or a stream the catalog does not record.
func TestRolesRefuseToStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := env(t, c)
		name := bus.StreamName(e.Bus(), catalog.RoleTraffic)
		setDirect(t, e.JS, name, true)
		for role, run := range map[string]func(context.Context, *Env) error{"migrate": Migrate, "ingest": Ingest, "compact": Compact} {
			if err := run(ctx, e); !errors.Is(err, ErrAllowDirect) {
				t.Errorf("%s with direct gets = %v, want ErrAllowDirect", role, err)
			}
		}
		setDirect(t, e.JS, name, false)

		s, err := e.JS.Stream(ctx, bus.StreamName(e.Bus(), catalog.RoleParking))
		if err != nil {
			t.Fatal(err)
		}
		cfg := s.CachedInfo().Config
		if err := e.JS.DeleteStream(ctx, cfg.Name); err != nil {
			t.Fatal(err)
		}
		if _, err := e.JS.CreateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		for role, run := range map[string]func(context.Context, *Env) error{"migrate": Migrate, "ingest": Ingest, "compact": Compact} {
			if err := run(ctx, e); !errors.Is(err, catalog.ErrIncarnationChanged) {
				t.Errorf("%s on a recreated stream = %v, want ErrIncarnationChanged", role, err)
			}
		}
	})
}

// Ingest joins the traffic and the parked consumers, each with its membership and its durable consumer named for its incarnation,
// and stops cleanly when its context ends.
func TestIngestJoinsBothConsumers(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := env(t, c)
		gs, err := ingesters(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		if len(gs) != 2 || gs[0].Replica != "r1" || gs[1].Replica != "r1/parked" || gs[1].RedeliveryDelay != e.Config.Parking.RedeliveryDelay {
			t.Fatalf("ingesters %+v, want the traffic and the parked consumer", gs)
		}
		for _, g := range gs {
			if _, err := g.Stream.Consumer(ctx, bus.ConsumerName(g.Incarnation)); err != nil {
				t.Errorf("%s: consumer %s: %v", g.Replica, bus.ConsumerName(g.Incarnation), err)
			}
		}
		stopped, cancel := context.WithCancel(ctx)
		cancel()
		if err := run(stopped, e, gs, nil); err != nil {
			t.Errorf("run stopped = %v", err)
		}
		// Both memberships left: a configuration neither executes is bindable, which a live member lacking it would prevent.
		newer, err := c.ActivateConfig(ctx, []byte(`{"classifier":"newer","max_frame_len":0}`))
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.Admit(ctx, &catalog.Identity{Version: catalog.VersionKey{Capture: tracepack.UUID{5}}, Incarnation: 1, StreamSeq: 1},
			&catalog.Descriptor{Bytes: []byte("d"), EquipmentID: "T1", RecorderInstanceID: tracepack.UUID{6}, Device: "eqp", OriginNS: 1})
		if err != nil || res.ConfigID != newer {
			t.Errorf("Admit after the stop = %+v, %v; want the newer configuration bound, no member left", res, err)
		}
	})
}

// The ingest role takes its budgets and its store's kind from the configuration:
// each ingester its ack wait, its heartbeat a third of it, its stop budget, and a flusher told the store is local for an fs store.
func TestIngestWiresItsConfiguration(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := env(t, c)
		// Values apart from the defaults, so a field left unset or set from another key shows.
		e.Config.Ingest.AckWait, e.Config.Ingest.StopTimeout = 5*time.Minute+7*time.Second, 17*time.Second
		gs, err := ingesters(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range gs {
			e.leave(g.Replica)
		}
		if len(gs) != 2 {
			t.Fatalf("%d ingesters, want the traffic and the parked one", len(gs))
		}
		for _, g := range gs {
			if g.AckWait != e.Config.Ingest.AckWait || g.Heartbeat != e.Config.Ingest.AckWait/3 || g.StopTimeout != e.Config.Ingest.StopTimeout {
				t.Errorf("%s: ack wait %s, heartbeat %s, stop budget %s; want %s, a third of it, %s",
					g.Replica, g.AckWait, g.Heartbeat, g.StopTimeout, e.Config.Ingest.AckWait, e.Config.Ingest.StopTimeout)
			}
			if !g.Flusher.LocalStore {
				t.Errorf("%s: the flusher does not know the fs store is local", g.Replica)
			}
		}
	})
}

// Open opens the catalog, the bus and the fs store a configuration names.
func TestOpen(t *testing.T) {
	t.Parallel()
	cfg := load(t, testbus.Start(t))
	db, err := sql.Open("sqlite", catalog.SQLiteURI(cfg.Catalog.DSN))
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(t.Context(), db, catalog.EngineSQLite); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	e, err := Open(t.Context(), cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.Store.(*store.FS); !ok || !e.NC.IsConnected() {
		t.Errorf("env %+v, want an fs store and a connected bus", e)
	}
	if err := e.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
}

// An fs store whose directory is missing fails Open with its error; the cleanup closes no store it did not open.
func TestOpenMissingStore(t *testing.T) {
	t.Parallel()
	cfg := load(t, testbus.Start(t))
	db, err := sql.Open("sqlite", catalog.SQLiteURI(cfg.Catalog.DSN))
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(t.Context(), db, catalog.EngineSQLite); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	cfg.Store.Dir = filepath.Join(cfg.Store.Dir, "missing")
	if _, err := Open(t.Context(), cfg, slog.New(slog.DiscardHandler)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open = %v, want the missing directory", err)
	}
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

// A stream replaced under all ends it with an error rather than leaving a role running; which role meets the change first,
// and whether at its start, is not controlled here: TestTogetherCancelsTheOthers is the schedule for the cancellation.
func TestAllStopsWithOneRole(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		e := env(t, c)
		done := make(chan error, 1)
		go func() { done <- All(t.Context(), e) }()
		s, err := e.JS.Stream(t.Context(), bus.StreamName(e.Bus(), catalog.RoleTraffic))
		if err != nil {
			t.Fatal(err)
		}
		cfg := s.CachedInfo().Config
		if err := e.JS.DeleteStream(t.Context(), cfg.Name); err != nil {
			t.Fatal(err)
		}
		if _, err := e.JS.CreateStream(t.Context(), cfg); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err == nil {
			t.Error("All = nil after its traffic stream was replaced, want the role's error")
		}
	})
}

// together cancels every other role when one fails, and returns that role's error (barrier-controlled):
// both roles are running before one fails, and the other ends only by its context's cancellation.
func TestTogetherCancelsTheOthers(t *testing.T) {
	t.Parallel()
	errFail := errors.New("role failed")
	running := make(chan struct{})
	cancelled := make(chan struct{})
	err := together(t.Context(),
		func(ctx context.Context) error {
			<-running // the other role is running
			return errFail
		},
		func(ctx context.Context) error {
			close(running)
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		})
	if !errors.Is(err, errFail) {
		t.Errorf("together = %v, want the failed role's error", err)
	}
	select {
	case <-cancelled:
	default:
		t.Error("the running role was not cancelled")
	}
}

// An observed ingest role is not ready before it starts, nor after a refused start; once joined it is ready while both consumers are on the bus,
// and not once one is gone or the role has stopped.
func TestObservedIngestReadiness(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		e := env(t, c)
		e.Config.Metrics.Listen = "" // nothing served: the handler is read directly
		if _, err := e.Observe(ctx, "ingest"); err != nil {
			t.Fatal(err)
		}
		h := e.Metrics.Handler()
		readyz := func() (int, string) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil))
			return rec.Code, rec.Body.String()
		}
		if code, body := readyz(); code != http.StatusServiceUnavailable || !strings.Contains(body, "ingest") || strings.Contains(body, errStarting.Error()) {
			t.Fatalf("/readyz before the role starts = %d %q, want 503 naming ingest, not its error", code, body)
		}
		traffic := bus.StreamName(e.Bus(), catalog.RoleTraffic)
		setDirect(t, e.JS, traffic, true)
		if err := Ingest(ctx, e); !errors.Is(err, ErrAllowDirect) {
			t.Fatalf("Ingest = %v, want ErrAllowDirect", err)
		}
		if code, body := readyz(); code != http.StatusServiceUnavailable || !strings.Contains(body, "ingest") || strings.Contains(body, errStopped.Error()) {
			t.Errorf("/readyz after a refused start = %d %q, want 503 naming ingest, not its error", code, body)
		}
		setDirect(t, e.JS, traffic, false)
		joined := make(chan struct{})
		e.joined = func() { close(joined) }
		rctx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- Ingest(rctx, e) }()
		<-joined
		if code, body := readyz(); code != http.StatusOK {
			t.Fatalf("/readyz once joined = %d %q, want 200", code, body)
		}
		parked, err := e.JS.Stream(ctx, bus.StreamName(e.Bus(), catalog.RoleParking))
		if err != nil {
			t.Fatal(err)
		}
		for name := range parked.ConsumerNames(ctx).Name() {
			if err := parked.DeleteConsumer(ctx, name); err != nil {
				t.Fatal(err)
			}
		}
		if code, body := readyz(); code != http.StatusServiceUnavailable {
			t.Errorf("/readyz with the parked consumer gone = %d %q, want 503", code, body)
		}
		stop()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Ingest = %v at its context's end", err)
		}
		if code, body := readyz(); code != http.StatusServiceUnavailable || !strings.Contains(body, "ingest") || strings.Contains(body, errStopped.Error()) {
			t.Errorf("/readyz after the role ended = %d %q, want 503 naming ingest, not its error", code, body)
		}
	})
}

// Observe fails when it cannot bind metrics.listen, rather than leaving the process running unobserved;
// bound, it serves until Env.Close.
func TestObserveBindsAtStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		e := env(t, c)
		e.Config.Metrics.Listen = "127.0.0.1:0"
		ctx, err := e.Observe(t.Context(), "ingest")
		if err != nil {
			t.Fatal(err)
		}
		cfg := *e.Config
		cfg.Metrics.Listen = e.Metrics.Addr().String()
		taken := &Env{Config: &cfg, Catalog: c, Log: e.Log}
		if _, err := taken.Observe(t.Context()); err == nil {
			t.Error("Observe on a bound address = nil, want its error")
		}
		if err := e.Metrics.Close(); err != nil || ServeFailed(ctx) != nil {
			t.Errorf("Close = %v, ServeFailed = %v; want an orderly stop", err, ServeFailed(ctx))
		}
	})
}

// The metrics server Observe builds serves Go's profiles exactly when metrics.pprof is on.
func TestObservePprof(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		for on, want := range map[bool]int{false: http.StatusNotFound, true: http.StatusOK} {
			cfg := &config.Config{Metrics: config.Metrics{Pprof: on}} // nothing served: the handler is read directly
			e := &Env{Config: cfg, Catalog: c, Log: slog.New(slog.DiscardHandler)}
			if _, err := e.Observe(t.Context()); err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			e.Metrics.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/debug/pprof/", nil))
			if rec.Code != want {
				t.Errorf("metrics.pprof %v: /debug/pprof/ = %d, want %d", on, rec.Code, want)
			}
		}
	})
}

// Each role of each replica works in a directory of its own, so the files one removes at start are only the ones it left:
// another replica's, and the other role's, stay.
func TestOwnWorkDir(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	mine, err := ownWorkDir(base, "compact", "pod/a")
	if err != nil {
		t.Fatal(err)
	}
	// pod_a reads as pod/a once both are made safe for a file name; the digest keeps them apart.
	for _, other := range [][2]string{{"compact", "pod-b"}, {"compact", "pod_a"}, {"ingest", "pod/a"}} {
		dir, err := ownWorkDir(base, other[0], other[1])
		if err != nil {
			t.Fatal(err)
		}
		if dir == mine || filepath.Dir(dir) != base {
			t.Fatalf("%v's directory %s, mine %s", other, dir, mine)
		}
		live := filepath.Join(dir, "archive-live.tpk")
		if err := os.WriteFile(live, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		left := filepath.Join(mine, "archive-left.tpk")
		if err := os.WriteFile(left, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := compact.CleanWorkDir(mine); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(left); !os.IsNotExist(err) {
			t.Error("my abandoned file stays")
		}
		if _, err := os.Stat(live); err != nil {
			t.Errorf("%v's live file was removed", other)
		}
	}
	if again, err := ownWorkDir(base, "compact", "pod/a"); err != nil || again != mine {
		t.Errorf("a restart's directory = %s, %v; want %s", again, err, mine)
	}
}
