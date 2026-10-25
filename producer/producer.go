package producer

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/internal/core"
	"github.com/arloliu/tracekeep/producer/internal/link"
)

const (
	modulePath = "github.com/arloliu/tracekeep/producer"
	goSecsPath = "github.com/arloliu/go-secs/v2"
	// sinkEvents bounds the lifecycle events waiting for the sink goroutine.
	sinkEvents = 256
)

// ErrNoBus reports a producer built without a publisher, which has no bus to check.
var ErrNoBus = errors.New("producer: no bus")

// Option adjusts New.
type Option func(*options)

// Producer records one device's traffic as captures on the traffic bus
// (docs/specs/tracekeep/producer.md, producer-sdk.md §4.7).
// Its lifecycle calls, Begin, Rotate and End, are the device's Start, HotReload and Stop.
type Producer struct {
	tun    Tunables
	prefix string
	stream string
	bucket string
	pub    *publisher
	// conn is the default bus, on the connection New dialed or WithConn brought, which Close cleans up; nil with WithBus.
	conn     *natsBus
	rec      *core.Recorder
	log      Logger
	metrics  Metrics
	recorder string
	now      func() (wall, mono int64)

	// lifeSem is the lifecycle lock, which serializes every rotation, whatever its cause;
	// a one-slot semaphore, so a caller can give up at its deadline.
	lifeSem chan struct{}
	// gen is the lifecycle generation; only abandonment advances it.
	gen atomic.Uint64
	// stopping is set by a completed End until the next Begin;
	// ending counts Ends waiting for the lifecycle lock, so a rotation inside it abandons.
	stopping atomic.Bool
	ending   atomic.Int32
	// Guarded by the lifecycle lock.
	desc       CaptureDescriptor
	descKey    []byte
	last       *tracepack.UUID
	retired    []*core.Capture // captures rotations closed since the last End
	ids        uuidV7
	faultedAt  int64          // when the last fault rotation ran, mono ns
	faultFresh tracepack.UUID // the capture it installed, or the faulted one when it failed
	metricsCur map[string]string
	// rotateAt overrides the seq-limit rotation point; the package's tests lower it.
	rotateAt uint64

	events    chan func()
	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type options struct {
	log     Logger
	metrics Metrics
	bus     Bus
	conn    *nats.Conn
	// noPublisher builds a producer without a bus or a publisher, for the package's own lifecycle tests.
	noPublisher bool
	// hooks and rotateAt are the package's test seams.
	hooks    publisherHooks
	rotateAt uint64
}

func init() {
	link.Recorder = func(p any) *core.Recorder {
		if pp, ok := p.(*Producer); ok {
			return pp.rec
		}

		return nil
	}
}

// New builds a producer from cfg; it opens no capture until Begin.
// By default it dials a dedicated connection to cfg.Servers and returns without waiting for it:
// an unreachable cluster is retried in the background, and the bus counts as down until it answers (producer.md §4.3).
//
// Returns:
//   - *Producer: the producer; Close releases it.
//   - error: an error wrapping ErrInvalidConfig.
func New(cfg Config, opts ...Option) (*Producer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	o := options{log: nopLogger{}, metrics: nopMetrics{}}
	for _, opt := range opts {
		opt(&o)
	}
	tun := cfg.Tunables.withDefaults()
	var nb *natsBus
	if o.bus == nil && !o.noPublisher {
		var err error
		if nb, err = newNATSBus(&cfg, &tun, o.conn); err != nil {
			return nil, err
		}
		o.bus = nb
	} else if o.conn != nil {
		return nil, fmt.Errorf("%w: WithConn and WithBus both given", ErrInvalidConfig)
	}
	stream := cfg.Stream
	if stream == "" {
		stream = busconfig.DefaultTrafficStream
	}
	p := &Producer{
		tun:    tun,
		prefix: cfg.SubjectPrefix,
		stream: stream,
		bucket: cfg.ClaimBucket,
		rec: core.NewRecorder(core.RecorderParams{
			ReservedCap:     tun.ReservedLaneRecords,
			OrdinaryRecords: tun.OrdinaryLaneRecords,
			OrdinaryBytes:   tun.OrdinaryLaneBytes,
		}),
		log:      o.log,
		metrics:  o.metrics,
		recorder: recorderName(),
		now:      core.Now,
		quit:     make(chan struct{}),
		events:   make(chan func(), sinkEvents),
		lifeSem:  make(chan struct{}, 1),
		rotateAt: o.rotateAt,
	}
	p.conn = nb
	if nb != nil {
		// nats.go calls back on its own goroutine; the sink keeps a slow Logger from stalling it.
		nb.log = sinkLogger{p}
		if nb.owned {
			go nb.dial(&p.tun) // not joined: Close closes the connection whenever the dial returns
		}
	}
	p.wg.Go(p.faultLoop)
	p.wg.Go(p.sinkLoop)
	if !o.noPublisher {
		p.pub = newPublisher(p, o.bus)
		p.pub.hooks = o.hooks
		p.wg.Go(func() { p.pub.run(p.quit) })
	}
	if nb != nil {
		p.wg.Go(func() { p.preflightOnConnect(nb) })
	}

	return p, nil
}

// WithConn publishes over nc, a connection the producer built itself, instead of one the SDK dials:
// for a producer with its own TLS loader, credential rotation, dialer or connection event hooks.
// New refuses nc when its reconnect buffer is enabled (nats.ReconnectBufSize(-1) disables it);
// Preflight reports one without TLS (tracekeep-decisions.md T-35).
// Close leaves nc open.
func WithConn(nc *nats.Conn) Option {
	return func(o *options) { o.conn = nc }
}

// WithBus publishes through b instead of a connection the SDK dials; producertest injects its in-memory bus this way.
// The publication policy covers this route like every other.
func WithBus(b Bus) Option {
	return func(o *options) { o.bus = b }
}

// WithLogger sends the producer's log lines to l.
func WithLogger(l Logger) Option {
	return func(o *options) { o.log = l }
}

// WithMetrics sends the producer's measurements to m.
func WithMetrics(m Metrics) Option {
	return func(o *options) { o.metrics = m }
}

// Close stops the producer's goroutines and closes the connection New dialed;
// call it after End, whose drain it does not wait for.
func (p *Producer) Close() error {
	p.closeOnce.Do(func() { close(p.quit) })
	p.wg.Wait()
	if p.conn != nil {
		p.conn.close()
	}

	return nil
}

// preflightOnConnect runs Preflight once the connection first comes up and logs what it finds,
// so a misconfigured bus is reported at startup without delaying Begin (docs/specs/tracekeep/producer-sdk.md §4.7).
func (p *Producer) preflightOnConnect(nb *natsBus) {
	nc := nb.waitUp(p.quit)
	if nc == nil {
		return
	}
	up := nc.StatusChanged(nats.CONNECTED)
	defer nc.RemoveStatusListener(up)
	if !nc.IsConnected() {
		select {
		case <-up:
		case <-p.quit:
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.tun.PublishTimeout)
	defer cancel()
	go func() {
		select {
		case <-p.quit:
			cancel()
		case <-ctx.Done():
		}
	}()
	problems, err := p.Preflight(ctx)
	log := sinkLogger{p}
	if err != nil {
		log.Warn("producer: preflight could not finish", "error", err)
	}
	for _, pr := range problems {
		log.Warn("producer: preflight", "check", pr.Check, "problem", pr.Detail)
	}
}

// sinkLogger hands each line to the producer's sink goroutine without waiting,
// so the SDK's own goroutines and nats.go's callbacks never wait on the integrator's Logger,
// and no line reaches it after Close.
type sinkLogger struct{ p *Producer }

func (l sinkLogger) Info(msg string, args ...any)  { l.p.emit(func() { l.p.log.Info(msg, args...) }) }
func (l sinkLogger) Warn(msg string, args ...any)  { l.p.emit(func() { l.p.log.Warn(msg, args...) }) }
func (l sinkLogger) Error(msg string, args ...any) { l.p.emit(func() { l.p.log.Error(msg, args...) }) }

// recorderName names the SDK and the go-secs that delivers its observations (the descriptor's recorder).
func recorderName() string {
	sdk, goSecs := "(devel)", "(unknown)"
	if bi, ok := debug.ReadBuildInfo(); ok {
		if bi.Main.Path == modulePath {
			sdk = bi.Main.Version
		}
		for _, m := range bi.Deps {
			switch m.Path {
			case modulePath:
				sdk = m.Version
			case goSecsPath:
				goSecs = m.Version
			default:
			}
		}
	}

	return "tracekeep-producer/" + sdk + " go-secs/" + goSecs
}
