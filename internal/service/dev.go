package service

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"golang.org/x/sync/errgroup"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/keys"
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
	// API is where the machine API listens, over TLS; empty takes 127.0.0.1:8080.
	// IdP is where the test IdP listens; empty takes 127.0.0.1:8444.
	// Each names localhost, 127.0.0.1 or ::1, and port 0 takes a free port (api.md §10.3 *Development*).
	API, IDP string
	// PublicURL is the https URL the browser reaches the API at, such as Vite's server; empty is the API's own address.
	PublicURL string
	Log       *slog.Logger
}

// Dev is tracekeep dev: an embedded bus, a SQLite catalog and an fs store under one directory,
// ingest, compact and the machine API, and the loopback simulator publishing to the bus, in one process (docs/ROADMAP.md phase 1).
type Dev struct {
	Env  *Env
	bus  *server.Server
	opts DevOptions
	// api is the API's listener, bound before the configuration; idp serves the test IdP.
	api net.Listener
	idp *http.Server
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
	if err := d.devAPI(cfg); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	if err := MigrateCatalog(ctx, &cfg.Catalog); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	if d.Env, err = Open(ctx, cfg, o.Log); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	d.Env.apiListener = d.api
	// A dev build is often a workspace build, whose classifier names no version.
	d.Env.AllowUnversioned, d.Env.Dev = true, true
	if err := Migrate(ctx, d.Env); err != nil {
		return nil, errors.Join(err, d.Close())
	}

	return d, nil
}

// Dev's credentials, under its directory: the static token its API admits and its download key.
const (
	DevTokenFile = "api-token"
	devKeyFile   = "download-key"
	devKeyID     = "dev-download-1"
)

// devAPI configures dev's machine API in cfg: TLS on o.API, people's sign-in at the test IdP on o.IDP, both bound first,
// a static token admitting pack reads for machine clients, and a download key, each generated once under o.Dir with mode 0600 and kept across runs.
func (d *Dev) devAPI(cfg *config.Config) error {
	o := d.opts
	var apiAddr, idpAddr string
	var idpLn net.Listener
	var err error
	if d.api, apiAddr, err = devListen("api", cmp.Or(o.API, "127.0.0.1:8080")); err != nil {
		return err
	}
	if idpLn, idpAddr, err = devListen("idp", cmp.Or(o.IDP, "127.0.0.1:8444")); err != nil {
		return err
	}
	cfg.API.Listen = d.api.Addr().String()
	token := filepath.Join(o.Dir, DevTokenFile)
	if err := generate(token, func() []byte { return []byte(rand.Text() + rand.Text()) }); err != nil {
		return err
	}
	b, err := os.ReadFile(token)
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	cfg.Auth.Static = config.Static{Enabled: true, Tokens: []config.StaticToken{
		{Subject: "dev", TokenRef: "file://" + token, Token: string(b), Roles: []string{auth.RolePackReader, auth.RoleViewer}},
	}}
	key := filepath.Join(o.Dir, devKeyFile)
	if err := generate(key, func() []byte { return []byte(base64.StdEncoding.EncodeToString([]byte(rand.Text() + rand.Text()))) }); err != nil {
		return err
	}
	cfg.Keys = config.Keys{
		Declared: map[string]config.KeyDeclaration{devKeyID: {Provider: keys.ProviderFile, Ref: key}},
		Download: config.KeyPurpose{Issuing: devKeyID, Accepted: []string{devKeyID}},
	}
	if err := d.devSignIn(cfg, apiAddr, idpLn, idpAddr); err != nil {
		return errors.Join(err, idpLn.Close())
	}

	return nil
}

// generate writes what content returns to a new file at path, mode 0600, unless one is there.
func generate(path string, content func() []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	_, err = f.Write(content())

	return errors.Join(err, f.Close())
}

// MigrateCatalog applies the catalog's pending migrations (catalog.md §7.4), creating a SQLite catalog's file when there is none.
// A SQLite catalog is migrated under the writer's lock the service holds, so it is refused beside a running service (ErrSecondWriter);
// a MariaDB one under the migrator's named lock (migrations.ApplyDir).
func MigrateCatalog(ctx context.Context, c *config.Catalog) error {
	if c.Engine != catalog.EngineSQLite {
		db, err := sql.Open("mysql", c.DSN)
		if err != nil {
			return fmt.Errorf("service: migrate the catalog: %w", err)
		}
		defer db.Close()
		if err := migrations.Apply(ctx, db, c.Engine); err != nil {
			return fmt.Errorf("service: migrate the catalog: %w", err)
		}

		return nil
	}
	f, err := os.OpenFile(c.DSN, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("service: migrate the catalog: %w", err)
	}
	_ = f.Close()
	held, err := catalog.Open(ctx, catalog.Config{Engine: c.Engine, DSN: c.DSN})
	if err != nil {
		return fmt.Errorf("service: migrate the catalog: %w", err)
	}
	write, _ := held.Pools()
	err = migrations.Apply(ctx, write, c.Engine)
	if errors.Is(err, migrations.ErrOtherTool) {
		err = fmt.Errorf("%w; a tracekeep dev directory made before phase 2.7 holds no revisions: start dev with a fresh --data", err)
	}

	return errors.Join(err, held.Close())
}

// URL is the embedded bus's client URL.
func (d *Dev) URL() string { return d.bus.ClientURL() }

// Run runs ingest, compact, the machine API and the simulator until ctx ends.
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
	err = errors.Join(err, d.closeIDP())
	if d.api != nil {
		_ = d.api.Close() // closed already when the api role ran
	}
	d.bus.Shutdown()
	d.bus.WaitForShutdown()

	return err
}
