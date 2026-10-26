package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
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
	url := testbus.Start(t)
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
	e := &Env{Config: load(t, url), Log: slog.New(slog.DiscardHandler), Catalog: c, NC: nc, JS: js, Store: store.NewMem(), AllowUnversioned: true}
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
		if err := run(stopped, e, gs); err != nil {
			t.Errorf("run stopped = %v", err)
		}
		// Both memberships left: a configuration neither executes is bindable, which a live member lacking it would prevent.
		newer, err := c.ActivateConfig(ctx, []byte(`{"classifier":"newer","max_frame_len":0}`))
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.Admit(ctx, &catalog.Identity{Version: catalog.VersionKey{Capture: tracepack.UUID{5}}, Incarnation: 1, StreamSeq: 1},
			&catalog.Descriptor{Bytes: []byte("d"), ToolID: "T1", RecorderInstanceID: tracepack.UUID{6}, Device: "eqp", OriginNS: 1})
		if err != nil || res.ConfigID != newer {
			t.Errorf("Admit after the stop = %+v, %v; want the newer configuration bound, no member left", res, err)
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
