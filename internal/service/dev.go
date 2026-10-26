package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"golang.org/x/sync/errgroup"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/sim"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/migrations"
	"github.com/arloliu/tracekeep/producer"
)

// simStop bounds the simulator's device stop, the SDK's End included.
const simStop = 10 * time.Second

// DevOptions adjusts tracekeep dev.
type DevOptions struct {
	// Dir keeps the bus's storage, the catalog and the packs across runs.
	Dir string
	// Port is the embedded bus's client port, 0 for a free one.
	Port int
	// Flush is ingest's flush interval, short enough to watch a capture arrive.
	Flush time.Duration
	// Rate is the simulator's forwarded requests per second; 0 runs no simulator.
	Rate float64
	Log  *slog.Logger
}

// Dev is tracekeep dev: an embedded bus, a SQLite catalog and an fs store under one directory,
// ingest and compact, and the loopback simulator publishing to the bus, in one process (docs/ROADMAP.md phase 1).
type Dev struct {
	Env  *Env
	bus  *server.Server
	opts DevOptions
}

// NewDev starts the embedded bus, creates the catalog when the directory has none, opens the environment and migrates the bus.
func NewDev(ctx context.Context, o DevOptions) (*Dev, error) {
	for _, d := range []string{"bus", "packs"} {
		if err := os.MkdirAll(filepath.Join(o.Dir, d), 0o750); err != nil {
			return nil, fmt.Errorf("dev: %w", err)
		}
	}
	srv, err := testbus.Serve(filepath.Join(o.Dir, "bus"), o.Port)
	if err != nil {
		return nil, err
	}
	d := &Dev{bus: srv, opts: o}
	cfg, err := config.Dev(o.Dir, srv.ClientURL(), o.Flush)
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	if err := createCatalog(ctx, cfg.Catalog.DSN); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	if d.Env, err = Open(ctx, cfg, o.Log); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	// A dev build is often a workspace build, whose classifier names no version.
	d.Env.AllowUnversioned = true
	if err := Migrate(ctx, d.Env); err != nil {
		return nil, errors.Join(err, d.Close())
	}

	return d, nil
}

// createCatalog applies the embedded migrations to a SQLite catalog the file does not hold yet.
func createCatalog(ctx context.Context, path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	db, err := sql.Open("sqlite", catalog.SQLiteURI(path))
	if err != nil {
		return fmt.Errorf("dev: catalog: %w", err)
	}
	defer db.Close()
	if err := migrations.Apply(ctx, db, catalog.EngineSQLite); err != nil {
		return fmt.Errorf("dev: catalog: %w", err)
	}

	return nil
}

// URL is the embedded bus's client URL.
func (d *Dev) URL() string { return d.bus.ClientURL() }

// Run runs ingest, compact and the simulator until ctx ends.
func (d *Dev) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return All(gctx, d.Env) })
	if d.opts.Rate > 0 {
		g.Go(func() error { return d.simulate(gctx) })
	}

	return g.Wait()
}

// simulate runs the loopback simulator on the embedded bus until ctx ends.
func (d *Dev) simulate(ctx context.Context) error {
	c := d.Env.Config
	p, err := producer.New(producer.Config{
		Servers: []string{d.URL()}, SubjectPrefix: c.Bus.SubjectPrefix, ClaimBucket: c.Bus.ClaimBucket,
	}, producer.WithLogger(d.Env.Log.With("role", "sim")))
	if err != nil {
		return err
	}
	defer p.Close()
	l, err := sim.NewLoopback(p, sim.Options{ToolID: "SIM-TOOL", DeviceName: "eqp", SessionID: 1, T3: 45 * time.Second})
	if err != nil {
		return err
	}
	if err := l.Start(ctx); err != nil {
		return errors.Join(err, l.Close())
	}
	sim.Forward(ctx, l, d.opts.Rate, d.Env.Log.With("role", "sim"))
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), simStop)
	defer cancel()
	l.Stop(sctx)

	return nil
}

// Close closes the environment and shuts the embedded bus down.
func (d *Dev) Close() error {
	var err error
	if d.Env != nil {
		err = d.Env.Close()
	}
	d.bus.Shutdown()
	d.bus.WaitForShutdown()

	return err
}
