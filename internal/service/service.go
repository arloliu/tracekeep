// Package service wires the roles of the tracekeep binary (architecture.md §3.1) from the configuration:
// migrate, ingest, compact, api and all, each over the catalog, the bus and the object store the configuration names.
package service

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// writerName names this service in every segment's metadata.
const writerName = "tracekeep"

// ErrAllowDirect refuses a traffic stream that allows direct gets: a read could be served by a replica behind the leader (T-12).
var ErrAllowDirect = errors.New("service: the traffic stream allows direct gets")

// Env is what every role runs over: the configuration, the catalog, the bus and the store.
type Env struct {
	Config  *config.Config
	Log     *slog.Logger
	Catalog *catalog.Catalog
	NC      *nats.Conn
	JS      jetstream.JetStream
	Store   store.ObjectStore
	// AllowUnversioned accepts a build whose classifier names no version, and Dev admits the env key provider; only dev sets them.
	AllowUnversioned, Dev bool
	// Now is the clock; nil takes time.Now.
	Now func() time.Time
	// Metrics is the process's metrics and health server; nil records and serves none.
	Metrics *metrics.Server

	// keys is the process's one keys module, which every role shares, so a key is resolved and bound once per process.
	keysMu sync.Mutex
	keys   *keys.Keys

	// joined, when not nil, runs once Ingest's consumers joined and its readiness is registered; a test's barrier.
	joined func()
	// apiAddr is the api role's bound address; apiBound, when not nil, runs once it is bound, a test's barrier;
	// apiShutdown, when not zero, replaces the api role's shutdown bound.
	apiAddr     atomic.Value
	apiBound    func()
	apiShutdown time.Duration
	// listening is closed once the api role's listener is bound.
	listening     chan struct{}
	listeningOnce sync.Once
}

// refreshInterval is how often ingest's metrics read the catalog and the bus.
const refreshInterval = 30 * time.Second

// Readiness of a role, before it runs and after it ends.
var (
	errStarting = errors.New("service: the role is starting")
	errStopped  = errors.New("service: the role has stopped")
)

// Observe registers the catalog's metrics and readiness on a new metrics server, each of roles not ready until it has started,
// and serves it on metrics.listen until Close, failing when it cannot bind; with no listen address it serves nothing, the metrics still recorded.
// The roles run under the context it returns, which serving that ends before Close cancels with its error (metrics.ErrServe).
func (e *Env) Observe(ctx context.Context, roles ...string) (context.Context, error) {
	e.Metrics = metrics.NewServer()
	if err := metrics.RegisterCatalog(e.Metrics, e.Catalog); err != nil {
		return ctx, err
	}
	for _, role := range roles {
		e.ready(role, failing(errStarting))
	}
	addr := e.Config.Metrics.Listen
	if addr == "" {
		return ctx, nil
	}
	sctx, cancel := context.WithCancelCause(ctx)
	if err := e.Metrics.Start(ctx, addr, cancel); err != nil {
		cancel(err)
		return ctx, err
	}

	return sctx, nil
}

// ServeFailed is the error serving the metrics ended with, which cancelled ctx, a context Observe returned; nil otherwise.
func ServeFailed(ctx context.Context) error {
	if err := context.Cause(ctx); errors.Is(err, metrics.ErrServe) {
		return err
	}

	return nil
}

// ready registers role's readiness check, when the process is observed.
func (e *Env) ready(role string, check func(context.Context) error) {
	if e.Metrics != nil {
		e.Metrics.Ready(role, check)
	}
}

func failing(err error) func(context.Context) error {
	return func(context.Context) error { return err }
}

// Open opens the catalog, the bus connection and the store cfg names.
func Open(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Env, error) {
	e := &Env{Config: cfg, Log: log, listening: make(chan struct{})}
	var err error
	if e.Catalog, err = catalog.Open(ctx, catalog.Config{Engine: cfg.Catalog.Engine, DSN: cfg.Catalog.DSN}); err != nil {
		return nil, err
	}
	if e.NC, err = connect(&cfg.Bus); err != nil {
		return nil, errors.Join(err, e.Close())
	}
	if e.JS, err = jetstream.New(e.NC); err != nil {
		return nil, errors.Join(err, e.Close())
	}
	if e.Store, err = openStore(&cfg.Store); err != nil {
		return nil, errors.Join(err, e.Close())
	}

	return e, nil
}

// Close closes what Open opened.
func (e *Env) Close() error {
	var errs []error
	if e.Metrics != nil {
		errs = append(errs, e.Metrics.Close())
	}
	if c, ok := e.Store.(interface{ Close() error }); ok {
		errs = append(errs, c.Close())
	}
	if e.NC != nil {
		e.NC.Close()
	}
	if e.Catalog != nil {
		errs = append(errs, e.Catalog.Close())
	}

	return errors.Join(errs...)
}

func connect(b *config.Bus) (*nats.Conn, error) {
	opts := []nats.Option{nats.Name("tracekeep"), nats.MaxReconnects(-1)}
	switch {
	case b.CredsFile != "":
		opts = append(opts, nats.UserCredentials(b.CredsFile))
	case b.User != "":
		opts = append(opts, nats.UserInfo(b.User, b.Password))
	default:
		// No credentials: a bus that admits the connection as it is.
	}
	if b.CAFile != "" {
		opts = append(opts, nats.RootCAs(b.CAFile))
	}
	if b.CertFile != "" {
		opts = append(opts, nats.ClientCert(b.CertFile, b.KeyFile))
	}
	nc, err := nats.Connect(b.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("service: connect the bus: %w", err)
	}

	return nc, nil
}

func openStore(s *config.Store) (store.ObjectStore, error) {
	if s.Kind == "fs" {
		return store.NewFS(s.Dir)
	}

	return s3store.New(s3store.Config{
		Endpoint: s.S3.Endpoint, Region: s.S3.Region, Bucket: s.S3.Bucket, AccessKeyID: s.S3.AccessKeyID, SecretAccessKey: s.S3.SecretAccessKey,
	})
}

func (e *Env) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}

	return time.Now()
}

// Bus names the bus's streams and prefixes.
func (e *Env) Bus() bus.Config {
	b := &e.Config.Bus

	return bus.Config{Stream: b.Stream, SubjectPrefix: b.SubjectPrefix, ParkedStream: b.ParkedStream, ParkPrefix: b.ParkPrefix, ClaimBucket: b.ClaimBucket}
}

// horizon is the retention horizon, in UTC nanoseconds.
func (e *Env) horizon() int64 {
	return e.now().Add(-e.Config.Retention).UnixNano()
}

// Migrate is the bus part of tracekeep migrate: the traffic and parking streams and the claim bucket ensured,
// and each stream's incarnation recorded in the catalog (parking.md *incarnations*), which MigrateCatalog has migrated before.
func Migrate(ctx context.Context, e *Env) error {
	traffic, parked, err := bus.Ensure(ctx, e.JS, e.Bus())
	if err != nil {
		return err
	}
	// Ensure leaves an existing traffic stream as it is; one that allows direct gets is refused before either incarnation is recorded.
	if tc := traffic.CachedInfo().Config; tc.AllowDirect {
		return fmt.Errorf("%w: %s", ErrAllowDirect, tc.Name)
	}
	for role, s := range map[string]jetstream.Stream{catalog.RoleTraffic: traffic, catalog.RoleParking: parked} {
		if _, err := bus.Incarnate(ctx, e.Catalog, role, s); err != nil {
			return err
		}
	}

	return nil
}

// streams opens the traffic and parking streams and refuses either when it allows direct gets or is not the catalog's incarnation.
func (e *Env) streams(ctx context.Context) (map[string]jetstream.Stream, map[string]catalog.Incarnation, error) {
	ss := map[string]jetstream.Stream{}
	incs := map[string]catalog.Incarnation{}
	for _, role := range []string{catalog.RoleTraffic, catalog.RoleParking} {
		s, err := e.JS.Stream(ctx, bus.StreamName(e.Bus(), role))
		if err != nil {
			return nil, nil, fmt.Errorf("service: %s stream: %w", role, err)
		}
		ss[role] = s
	}
	tc, pc := ss[catalog.RoleTraffic].CachedInfo().Config, ss[catalog.RoleParking].CachedInfo().Config
	if tc.AllowDirect {
		return nil, nil, fmt.Errorf("%w: %s", ErrAllowDirect, tc.Name)
	}
	if err := bus.CheckParked(tc, pc); err != nil {
		return nil, nil, err
	}
	for role, s := range ss {
		inc, err := bus.Incarnate(ctx, e.Catalog, role, s)
		if err != nil {
			return nil, nil, err
		}
		incs[role] = inc
	}

	return ss, incs, nil
}

// Ingest runs the ingest role until ctx ends: the traffic stream's consumer and the parking stream's,
// sharing the executor, the flusher and the parking coordinator, each with its own membership (ingest.md §6, parking.md).
// Ingest is ready once both consumers joined, while both are still on the bus.
func Ingest(ctx context.Context, e *Env) error {
	e.ready("ingest", failing(errStarting))
	defer e.ready("ingest", failing(errStopped))
	gs, err := ingesters(ctx, e)
	if err != nil {
		return err
	}
	states := make([]busState, len(gs))
	for i, g := range gs {
		states[i] = e.busState(g)
	}
	e.ready("ingest", func(ctx context.Context) error {
		for _, s := range states {
			if _, err := s.State(ctx); err != nil {
				return err
			}
		}

		return nil
	})
	if e.joined != nil {
		e.joined()
	}
	if gs[0].Metrics != nil {
		rctx, cancel := context.WithCancel(ctx)
		refreshed := make(chan struct{})
		go func() {
			defer close(refreshed)
			e.refresh(rctx, gs[0].Metrics, states[0])
		}()
		defer func() {
			cancel()
			<-refreshed
		}()
	}

	return run(ctx, e, gs)
}

// refresh reads ingest's catalog and bus gauges every refreshInterval until ctx ends, each pass bounded by the interval.
func (e *Env) refresh(ctx context.Context, m *metrics.Ingest, traffic busState) {
	t := time.NewTicker(refreshInterval)
	defer t.Stop()
	for {
		pctx, cancel := context.WithTimeout(ctx, refreshInterval)
		if err := m.Refresh(pctx, e.Catalog, traffic); err != nil && ctx.Err() == nil {
			e.Log.Warn("metrics: refresh", "error", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// busState reads a consumer's and its stream's backlog on handles of its own, never the ingester's.
type busState struct {
	js       jetstream.JetStream
	stream   string
	consumer string
}

func (e *Env) busState(g *ingest.Ingester) busState {
	return busState{js: e.JS, stream: g.Stream.CachedInfo().Config.Name, consumer: bus.ConsumerName(g.Incarnation)}
}

func (b busState) State(ctx context.Context) (metrics.BusState, error) {
	s, err := b.js.Stream(ctx, b.stream)
	if err != nil {
		return metrics.BusState{}, err
	}
	c, err := s.Consumer(ctx, b.consumer)
	if err != nil {
		return metrics.BusState{}, err
	}
	ci, err := c.Info(ctx)
	if err != nil {
		return metrics.BusState{}, err
	}
	st := s.CachedInfo().State

	return metrics.BusState{AckPending: uint64(ci.NumAckPending), Messages: st.Msgs, Bytes: st.Bytes}, nil //nolint:gosec // a count is positive
}

// run runs the joined ingesters until ctx ends, and ends their memberships.
func run(ctx context.Context, e *Env, gs []*ingest.Ingester) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, ig := range gs {
		g.Go(func() error {
			defer e.leave(ig.Replica)

			return ig.Run(gctx)
		})
	}

	return g.Wait()
}

// leaveTimeout bounds ending a membership, so a stalled catalog never holds a stopping process.
const leaveTimeout = 5 * time.Second

// leave ends replica's membership; a lapsed lease would end it too, later.
func (e *Env) leave(replica string) {
	ctx, cancel := context.WithTimeout(context.Background(), leaveTimeout)
	defer cancel()
	if err := e.Catalog.Leave(ctx, replica); err != nil {
		e.Log.Warn("ingest: leave", "replica", replica, "error", err)
	}
}

// ingesters builds and joins the traffic and parked ingesters; a failure leaves the memberships it joined.
func ingesters(ctx context.Context, e *Env) (_ []*ingest.Ingester, err error) {
	var gs []*ingest.Ingester
	defer func() {
		if err != nil {
			for _, g := range gs {
				e.leave(g.Replica)
			}
		}
	}()
	c := e.Config
	ss, incs, err := e.streams(ctx)
	if err != nil {
		return nil, err
	}
	builtin, err := ingest.Builtin(c.Ingest.MaxFrameLen, e.AllowUnversioned)
	if err != nil {
		return nil, err
	}
	// Every unretired configuration of this classifier is executed, so a changed frame limit never locks the replica out;
	// another classifier's refuses the start before anything is activated.
	executable, err := ingest.Executable(ctx, e.Catalog, builtin)
	if err != nil {
		return nil, err
	}
	configs, err := ingest.Activate(ctx, e.Catalog, executable)
	if err != nil {
		return nil, err
	}
	claims, err := e.JS.ObjectStore(ctx, c.Bus.ClaimBucket)
	if err != nil {
		return nil, fmt.Errorf("service: claim bucket: %w", err)
	}
	x := &ingest.Executor{Catalog: e.Catalog, Claims: ingest.ObjectClaims{Store: claims}, Horizon: e.horizon, Configs: configs}
	f := &ingest.Flusher{
		Catalog: e.Catalog, Store: e.Store, WorkDir: c.Ingest.WorkDir, KeyPrefix: c.Store.KeyPrefix, Configs: configs,
		Interval: c.Ingest.FlushInterval, HorizonHour: func() int64 { return ingest.HourOf(e.horizon()) }, Writer: writerName, Now: e.now,
	}
	dir, err := ownWorkDir(f.WorkDir, "ingest", c.Replica)
	if err != nil {
		return nil, err
	}
	// At start a role removes the working files it left behind (storage.md §5.1).
	if err := ingest.CleanWorkDir(dir); err != nil {
		return nil, err
	}
	f.WorkDir = dir
	var m *metrics.Ingest
	if e.Metrics != nil {
		if m, err = metrics.NewIngest(e.Metrics, e.now); err != nil {
			return nil, err
		}
	}
	f.Metrics = m
	k := &ingest.Coordinator{Catalog: e.Catalog, Bus: e.JS, ParkPrefix: cmp.Or(c.Bus.ParkPrefix, busconfig.DefaultParkPrefix)}
	for _, role := range []string{catalog.RoleTraffic, catalog.RoleParking} {
		cons, err := bus.Consumer(ctx, ss[role], incs[role], bus.ConsumerConfig{AckWait: c.Ingest.AckWait, MaxAckPending: c.Ingest.MaxAckPending})
		if err != nil {
			return nil, err
		}
		g := &ingest.Ingester{
			Executor: x, Flusher: f, Coordinator: k, Batcher: ingest.NewBatcher(c.Ingest.FlushInterval, int(c.Ingest.BatchMaxBytes.Int64())),
			Park: c.Parking.Enabled, Consumer: cons, Stream: ss[role], Incarnation: incs[role], Log: e.Log.With("role", role),
			Replica: c.Replica, Lease: c.Ingest.Lease, FetchBatch: c.Ingest.FetchBatch, MaxWait: c.Ingest.MaxWait,
			Heartbeat: c.Ingest.AckWait / 3, Now: e.now, Metrics: m,
		}
		if role == catalog.RoleParking {
			g.Replica, g.RedeliveryDelay = c.Replica+"/parked", c.Parking.RedeliveryDelay
		}
		if err := g.Join(ctx); err != nil {
			return nil, err
		}
		gs = append(gs, g)
	}

	return gs, nil
}

// Compact runs the compact role until ctx ends: its reconciler (storage.md §5.4) and its merges (compactor.md §8.1), under the compactor lease.
func Compact(ctx context.Context, e *Env) error {
	e.ready("compact", failing(errStarting))
	defer e.ready("compact", failing(errStopped))
	if _, _, err := e.streams(ctx); err != nil {
		return err
	}
	c := e.Config
	r := &compact.Compactor{
		Catalog: e.Catalog, Store: e.Store, KeyPrefix: c.Store.KeyPrefix, HorizonHour: func() int64 { return ingest.HourOf(e.horizon()) },
		Log: e.Log.With("role", "compact"), JS: e.JS, Bus: e.Bus(), Holder: c.Replica, LeaseTTL: c.Compact.LeaseTTL, Interval: c.Compact.Interval,
		WorkDir: c.Ingest.WorkDir, Writer: writerName, QuietPeriod: c.Compact.QuietPeriod, Now: e.now,
		Inactivity: c.Compact.Inactivity, FlushInterval: c.Ingest.FlushInterval, Retention: c.Retention, DeleteDelay: c.Compact.DeleteDelay,
	}
	claims, err := e.JS.ObjectStore(ctx, c.Bus.ClaimBucket)
	if err != nil {
		return err
	}
	r.Claims = compact.ObjectBucket{Store: claims}
	dir, err := ownWorkDir(r.WorkDir, "compact", c.Replica)
	if err != nil {
		return err
	}
	if err := compact.CleanWorkDir(dir); err != nil {
		return err
	}
	r.WorkDir = dir
	if e.Metrics != nil {
		var err error
		if r.Metrics, err = metrics.NewCompact(e.Metrics); err != nil {
			return err
		}
		e.Metrics.Ready("compact", r.Ready)
	}
	err = r.Run(ctx)
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}

	return err
}

// All runs every role of this phase in one process, for a standalone host: ingest, compact and api.
func All(ctx context.Context, e *Env) error {
	return together(ctx,
		func(ctx context.Context) error { return Ingest(ctx, e) },
		func(ctx context.Context) error { return Compact(ctx, e) },
		func(ctx context.Context) error { return API(ctx, e) })
}

// together runs roles until each returns; the first to fail cancels the others, and its error is returned.
func together(ctx context.Context, roles ...func(context.Context) error) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, role := range roles {
		g.Go(func() error { return role(gctx) })
	}

	return g.Wait()
}

// ownWorkDir is the working directory of role in this replica, under base (the system's temporary directory when empty):
// a directory of its own, named for the role and the replica, so what a role removes at start is only what it left behind,
// never another process's (storage.md §5.1). The replica's readable part may alias another's, so a digest of the whole name follows it.
func ownWorkDir(base, role, replica string) (string, error) {
	if base == "" {
		base = os.TempDir()
	}
	name := strings.Map(func(r rune) rune {
		if r == '-' || r == '.' || r == '_' || ('0' <= r && r <= '9') || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') {
			return r
		}

		return '_'
	}, replica)
	sum := sha256.Sum256([]byte(replica))
	dir := filepath.Join(base, "tracekeep-"+role+"-"+name+"-"+hex.EncodeToString(sum[:8]))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("service: working directory: %w", err)
	}

	return dir, nil
}
