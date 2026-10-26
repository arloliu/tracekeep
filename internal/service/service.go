// Package service wires the roles of the tracekeep binary (architecture.md §3.1) from the configuration:
// migrate, ingest, compact and all, each over the catalog, the bus and the object store the configuration names.
package service

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/ingest"
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
	// AllowUnversioned accepts a build whose classifier names no version; only dev sets it.
	AllowUnversioned bool
	// Now is the clock; nil takes time.Now.
	Now func() time.Time
}

// Open opens the catalog, the bus connection and the store cfg names.
func Open(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Env, error) {
	e := &Env{Config: cfg, Log: log}
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
// and each stream's incarnation recorded (parking.md *incarnations*).
// The catalog's schema is applied with the Atlas CLI (T-12).
func Migrate(ctx context.Context, e *Env) error {
	traffic, parked, err := bus.Ensure(ctx, e.JS, e.Bus())
	if err != nil {
		return err
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
func Ingest(ctx context.Context, e *Env) error {
	gs, err := ingesters(ctx, e)
	if err != nil {
		return err
	}

	return run(ctx, e, gs)
}

// run runs the joined ingesters until ctx ends, and ends their memberships.
func run(ctx context.Context, e *Env, gs []*ingest.Ingester) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, ig := range gs {
		g.Go(func() error {
			defer func() {
				// The membership ends with the process; a lapsed lease would end it too, later.
				if err := e.Catalog.Leave(context.WithoutCancel(ctx), ig.Replica); err != nil {
					e.Log.Warn("ingest: leave", "replica", ig.Replica, "error", err)
				}
			}()

			return ig.Run(gctx)
		})
	}

	return g.Wait()
}

// ingesters builds and joins the traffic and parked ingesters.
func ingesters(ctx context.Context, e *Env) ([]*ingest.Ingester, error) {
	c := e.Config
	ss, incs, err := e.streams(ctx)
	if err != nil {
		return nil, err
	}
	builtin, err := ingest.Builtin(c.Ingest.MaxFrameLen, e.AllowUnversioned)
	if err != nil {
		return nil, err
	}
	configs, err := ingest.Activate(ctx, e.Catalog, []ingest.Config{builtin})
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
	if f.WorkDir == "" {
		f.WorkDir = os.TempDir()
	}
	k := &ingest.Coordinator{Catalog: e.Catalog, Bus: e.JS, ParkPrefix: cmp.Or(c.Bus.ParkPrefix, busconfig.DefaultParkPrefix)}
	var gs []*ingest.Ingester
	for _, role := range []string{catalog.RoleTraffic, catalog.RoleParking} {
		cons, err := bus.Consumer(ctx, ss[role], incs[role], bus.ConsumerConfig{AckWait: c.Ingest.AckWait, MaxAckPending: c.Ingest.MaxAckPending})
		if err != nil {
			return nil, err
		}
		g := &ingest.Ingester{
			Executor: x, Flusher: f, Coordinator: k, Batcher: ingest.NewBatcher(c.Ingest.FlushInterval, int(c.Ingest.BatchMaxBytes.Int64())),
			Park: c.Parking.Enabled, Consumer: cons, Stream: ss[role], Incarnation: incs[role], Log: e.Log.With("role", role),
			Replica: c.Replica, Lease: c.Ingest.Lease, FetchBatch: c.Ingest.FetchBatch, MaxWait: c.Ingest.MaxWait,
			Heartbeat: c.Ingest.AckWait / 3, Now: e.now,
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

// Compact runs the compact role until ctx ends; for now its reconciler, under the compactor lease (storage.md §5.4).
func Compact(ctx context.Context, e *Env) error {
	if _, _, err := e.streams(ctx); err != nil {
		return err
	}
	c := e.Config
	r := &compact.Reconciler{
		Catalog: e.Catalog, Store: e.Store, KeyPrefix: c.Store.KeyPrefix, HorizonHour: func() int64 { return ingest.HourOf(e.horizon()) },
		Log: e.Log.With("role", "compact"), JS: e.JS, Bus: e.Bus(), Holder: c.Replica, LeaseTTL: c.Compact.LeaseTTL, Interval: c.Compact.Interval,
	}
	err := r.Run(ctx)
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}

	return err
}

// All runs every role of this phase in one process, for a standalone host: ingest and compact.
func All(ctx context.Context, e *Env) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return Ingest(gctx, e) })
	g.Go(func() error { return Compact(gctx, e) })

	return g.Wait()
}
