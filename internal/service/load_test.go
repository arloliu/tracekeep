package service

// The load harness of the write-connection case: tracekeep dev without its simulator, under K simulated equipment on its bus,
// each its own producer and loopback; it sweeps the rate each K sustains, offers half the highest at the largest K,
// and measures PackURL under that load. loadjudge_test.go judges what it records.
//
// It runs only when TRACEKEEP_LOAD is set, alone, without the race detector, which slows every path it measures,
// and with a timeout of its own, since the full sweep takes hours:
//
//	TRACEKEEP_LOAD=baseline go test ./internal/service/ -run '^TestLoad$' -count 1 -v -timeout 4h
//
// TRACEKEEP_LOAD names the run:
//   - baseline records the numbers: the sweep, the offered rate, and a PackURL measurement of at least 1,000 audited successes,
//     which must be a valid record; its gate verdict is reported and need not pass;
//   - gate is the write-connection gate: the same measurement must also pass the gate (1,000 audited successes, p99 within 2 s);
//   - smoke checks the harness itself in a few minutes: two equipment at a low rate over short windows, no call count required;
//   - ceiling profiles where ingest stops keeping up: K = 10 and 100 at 1,600 and 3,200 fwd/s under dev's 50,000 pending slots,
//     then 3,200 fwd/s with max_ack_pending raised to 200,000, past the records a 10 s flush holds, each point with its CPU,
//     allocation, block and mutex profiles, each stage's wall time, the catalog's SQL statements a message, the consumer's
//     pending and redelivered counts and the process's CPU (about 30 minutes);
//   - claimcheck runs K = 10 with 10% of its records, then all of them, above the inline limit (max_payload − 4096),
//     so they travel by claim-check, for acknowledgement's share of a flush (about 10 minutes,
//     writing some 20 GB of claim objects and packs under the test's temporary directory, which must be on a disk, not memory);
//   - production runs ingest's defaults scaled by 1/30 (T-61): flush 10 s and ack_wait 14 s (the heartbeat every ack_wait/3), at 1,600 records a second over K = 100,
//     with max_ack_pending 20,000 and then 1,667 as its control, each after a warm-up of a flush plus ack_wait,
//     judged over at least six completed drain cycles spanning at least three heartbeat periods (a few minutes both);
//     it fails unless both are judged with their offered rate held, no drop and no failed pass, the first sustained and the control reaching its budget;
//   - production-real runs the defaults themselves, flush 5 m and ack_wait 7 m, max_ack_pending 600,000 and 50,000 (about 1.5 hours), for a phase gate;
//   - recovery measures a recovery backlog's drain (loadrecovery_test.go): K = 100 at 800 fwd/s on dev's 10 s flush, max_ack_pending 600,000 on both consumers,
//     one more equipment's capture rejected by a refused descriptor and its start and M data records parked, then released and drained,
//     M from TRACEKEEP_LOAD_RECOVERY_M (1000, 10000 or 100000); an unknown condition runs the episode again, at most three episodes;
//   - feed measures the host log feed's stream (loadfeed_test.go, docs/plans/host-log-feed.md step 1) on a three-node cluster beside production's traffic:
//     2,000 equipment at 3,200 entries a second through 32 partitioned group consumers at ingest's times scaled by 1/30, then its controls (about 10 minutes);
//     feed-real runs the same at ingest's own times, for the plan's numbers (about an hour);
//   - ceiling-smoke, claimcheck-smoke, production-smoke, recovery-smoke and feed-smoke check those five in a few minutes, at a low rate over short spans,
//     recovery-smoke parking 200 records unless TRACEKEEP_LOAD_RECOVERY_M names M.
//
// TRACEKEEP_LOAD_OFFERED, forwards a second, skips the sweep and offers that rate at the largest K, as the gate does with the rate a baseline recorded.
// TRACEKEEP_LOAD_ENGINE, sqlite (the default) or mariadb, is the catalog's engine; mariadb runs dev on a database of its own on the server
// TRACEKEEP_MARIADB_DSN names, which it then requires, and reports the SQL counts, read on SQLite only, as unavailable.
// TRACEKEEP_LOAD_PROFILES is the directory the ceiling's and production's profiles are written to, a new temporary one when unset.
// The harness profiles in-process (runtime/pprof), so it runs without -cpuprofile, and every profile is the whole process's,
// the embedded bus server, the producers and the loopbacks included: ingest's share is go tool pprof -top -focus 'tracekeep/internal/(ingest|catalog)',
// the bus server's -focus 'nats-server/v2/server'.
// The results print (-v) as markdown tables for the plan's Baselines.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectrpc "connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/sim"
	"github.com/arloliu/tracekeep/producer"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// loadSpec shapes a run of the harness.
type loadSpec struct {
	// flush is dev's flush interval; warmup precedes a point's windows.
	flush, warmup time.Duration
	// window is a window's duration; windows, a sweep point's windows; final, the offered rate's.
	window         time.Duration
	windows, final int
	// calls are the audited PackURL successes a measurement runs until, its windows extended as needed.
	calls int
	// equipment are the sweep's K, each swept from start forwards a second, doubling, to at most top.
	equipment  []int
	start, top float64
	// ackWait and maxAckPending, when not zero, replace dev's consumer configuration, which the harness asserts took effect.
	ackWait       time.Duration
	maxAckPending int
	// probed installs the ingester's test seams and reads the consumer every sample, for the ceiling's, claim-check and production modes.
	probed bool
	// recovery installs the recovery drain's seams (loadrecovery_test.go) on a probed harness; redelivery, when not zero,
	// replaces the parked consumer's nak delay.
	recovery   bool
	redelivery time.Duration
	// bus, when not empty, is the bus dev uses instead of its own, as the feed mode shares a cluster with it (loadfeed_test.go).
	bus string
}

var (
	// fullLoad is point 27's sweep, at dev's default flush interval.
	fullLoad = loadSpec{
		flush: 10 * time.Second, warmup: time.Minute, window: 10 * time.Second, windows: 12, final: 60, calls: gateCalls,
		equipment: []int{1, 10, 100}, start: 100, top: 102_400,
	}
	// smokeLoad checks the harness: one point of two equipment, then the offered rate and PackURL, over short windows.
	smokeLoad = loadSpec{
		flush: time.Second, warmup: 5 * time.Second, window: 5 * time.Second, windows: 6, final: 6,
		equipment: []int{2}, start: 20, top: 20,
	}
)

const (
	// loadT3 is every simulated equipment's T3, as dev's simulator's.
	loadT3 = 45 * time.Second
	// quiesceLimit bounds the wait between points for the queues and the backlog to empty.
	quiesceLimit = 10 * time.Minute
	// packURLPace and packURLDeadline pace the PackURL calls and bound each attempt; packURLRetries bounds a superseded call's retries.
	packURLPace     = 100 * time.Millisecond
	packURLDeadline = 10 * time.Second
	packURLRetries  = 3
)

// equipmentMetrics is a producer's Metrics: the gauges, the drops and the claim objects put that the harness reads.
type equipmentMetrics struct {
	mu                              sync.Mutex
	ring, reserved, dropped, claims float64
	// data and other are the records observed, data records and the rest (the connection's control frames and events), the start apart.
	data, other float64
}

var _ producer.Metrics = (*equipmentMetrics)(nil)

func (m *equipmentMetrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case name == "records_dropped_total":
		m.dropped += delta
	case name == "claim_puts_total" && labels["outcome"] == "put":
		m.claims += delta
	case name == "records_observed_total" && labels["kind"] == "data":
		m.data += delta
	case name == "records_observed_total":
		m.other += delta
	default:
	}
}

func (m *equipmentMetrics) Set(name string, _ map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch name {
	case "ring_depth":
		m.ring = value
	case "reserved_lane_depth":
		m.reserved = value
	default:
	}
}

func (*equipmentMetrics) Observe(string, map[string]string, float64) {}
func (*equipmentMetrics) Delete(string, map[string]string)           {}

func (m *equipmentMetrics) read() (ring, reserved, dropped float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.ring, m.reserved, m.dropped
}

// loadEquipment is one simulated equipment, the forwards the harness made through it counted.
type loadEquipment struct {
	id   string
	p    *producer.Producer
	l    *sim.Loopback
	m    *equipmentMetrics
	made atomic.Uint64
	// forwards counts the forwards attempted, which choose the claim-check ones; claims, the claim-check forwards' system bytes.
	forwards, claims atomic.Uint32
}

// loadHarness is tracekeep dev without its simulator, with what the harness reads of it.
type loadHarness struct {
	t      *testing.T
	d      *Dev
	spec   loadSpec
	put    *putting
	stream jetstream.Stream
	token  string
	report []string
	// cons is the traffic consumer, read every sample when the spec is probed, parked the parked one; probe gathers the ingester's seams,
	// and rec, in the recovery mode, releases the rejected capture.
	cons, parked jetstream.Consumer
	// engine is the catalog's engine, TRACEKEEP_LOAD_ENGINE's.
	engine string
	probe  *loadProbe
	rec    *recovery
	// claimEvery makes every n-th forward a claim-check one of claimItem bytes, 0 none.
	claimEvery uint32
	claimItem  []byte
	// begin and end, when set, run at a measurement's first and last sample; samples are its samples.
	begin, end func()
	samples    []loadSample
}

// newLoadHarness starts dev with no simulator, observed so its ingest metrics are recorded, its retention recorded so a pack has a deadline.
func newLoadHarness(t *testing.T, spec loadSpec) *loadHarness {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	opts := DevOptions{Dir: dir, Flush: spec.flush, API: "127.0.0.1:0", IDP: "127.0.0.1:0", Log: slog.New(slog.DiscardHandler), bus: spec.bus}
	engine := loadEngine(t)
	if engine == catalog.EngineMariaDB {
		dsn, _ := catalogtest.MariaDBEmpty(t)
		opts.catalog = &config.Catalog{Engine: engine, DSN: dsn}
	}
	d, err := NewDev(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if got := d.Env.Config.Catalog.Engine; got != engine {
		t.Fatalf("dev's catalog is %s, want %s", got, engine)
	}
	h := &loadHarness{t: t, d: d, spec: spec, engine: engine, put: &putting{ObjectStore: d.Env.Store, put: make(chan string, 64)}}
	d.Env.Store = h.put
	if spec.ackWait > 0 {
		d.Env.Config.Ingest.AckWait = spec.ackWait
	}
	if spec.maxAckPending > 0 {
		d.Env.Config.Ingest.MaxAckPending = spec.maxAckPending
	}
	if spec.redelivery > 0 {
		d.Env.Config.Parking.RedeliveryDelay = spec.redelivery
	}
	if spec.probed {
		h.probe = &loadProbe{stages: map[string]*stageTotal{}}
		d.Env.timed, d.Env.passSettled = h.probe.timed, h.probe.settled
	}
	if spec.recovery {
		h.rec = newRecovery(d.Env, h.probe)
	}
	bound, joined := make(chan struct{}), make(chan struct{})
	d.Env.apiBound = func() { close(bound) }
	d.Env.joined = func() { close(joined) }
	octx, err := d.Env.Observe(ctx, "ingest", "compact", "api")
	if err != nil {
		t.Fatal(err)
	}
	seedRetention(t, d.Env)
	run, stop := context.WithCancel(octx)
	done := make(chan error, 1)
	go func() { done <- d.Run(run) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the run's end, its result not checked here
	})
	for _, ready := range []chan struct{}{bound, joined} {
		select {
		case <-ready:
		case err := <-done:
			t.Fatalf("dev ended before its API was bound and its ingest joined: %v", err)
		}
	}
	if h.stream, err = d.Env.JS.Stream(ctx, bus.StreamName(d.Env.Bus(), catalog.RoleTraffic)); err != nil {
		t.Fatal(err)
	}
	if spec.probed {
		h.consumer()
	}
	b, err := os.ReadFile(filepath.Join(dir, DevTokenFile))
	if err != nil {
		t.Fatal(err)
	}
	h.token = string(b)

	return h
}

// Two simulated equipment on dev's bus, each its own producer and loopback, are admitted as two captures, one of each piece of equipment.
func TestLoadEquipmentAdmittedAsTwoCaptures(t *testing.T) {
	t.Parallel()
	h := newLoadHarness(t, loadSpec{flush: 200 * time.Millisecond})
	equipment := h.startEquipment(2)
	defer h.stopEquipment(equipment)
	for _, lt := range equipment {
		if err := lt.l.Traffic(t.Context(), 3); err != nil {
			t.Fatal(err)
		}
	}
	// A segment's key names its equipment; each piece of equipment's first registered segment admitted its capture.
	waiting := map[string]bool{equipment[0].id: true, equipment[1].id: true}
	for len(waiting) > 0 {
		key := <-h.put.put
		for id := range waiting {
			if strings.Contains(key, "/"+id+"/") {
				delete(waiting, id)
			}
		}
	}
	now := time.Now()
	seen := map[tracepack.UUID]bool{}
	for _, lt := range equipment {
		cs, err := h.d.Env.Catalog.ListCaptures(t.Context(), catalog.Selector{EquipmentID: lt.id, From: now.Add(-time.Hour).UnixNano(), To: now.Add(time.Hour).UnixNano()},
			h.d.Env.Config.Compact.QuietPeriod)
		if err != nil {
			t.Fatal(err)
		}
		if len(cs) != 1 || seen[cs[0].ID] {
			t.Fatalf("equipment %s's captures %+v, want one of its own", lt.id, cs)
		}
		seen[cs[0].ID] = true
	}
}

// TestLoad runs the harness: the sweep, the offered rate, and the PackURL measurement under it.
func TestLoad(t *testing.T) {
	mode := os.Getenv("TRACEKEEP_LOAD")
	if mode == "" {
		t.Skip("TRACEKEEP_LOAD is not set: the load harness runs alone, without -race; see load_test.go")
	}
	if run, ok := loadModes[strings.TrimSuffix(mode, "-smoke")]; ok {
		run(t, strings.HasSuffix(mode, "-smoke"))
		return
	}
	if mode != "baseline" && mode != "gate" && mode != "smoke" {
		t.Fatalf("TRACEKEEP_LOAD=%q: want baseline, gate, smoke, ceiling, claimcheck, production, recovery or feed, the last five with -smoke or without", mode)
	}
	spec := fullLoad
	if mode == "smoke" {
		spec = smokeLoad
	}
	var offered float64
	if v := os.Getenv("TRACEKEEP_LOAD_OFFERED"); v != "" {
		var err error
		if offered, err = strconv.ParseFloat(v, 64); err != nil || offered <= 0 {
			t.Fatalf("TRACEKEEP_LOAD_OFFERED=%q: want forwards a second", v)
		}
	}
	h := newLoadHarness(t, spec)
	// Printed however the run ends, so a sweep cut short still reports its points.
	t.Cleanup(func() { t.Log("\n" + strings.Join(h.report, "\n")) })
	h.log(fmt.Sprintf("%s: flush %s, warm-up %s, %d windows of %s a point, %d at the offered rate", mode, spec.flush, spec.warmup, spec.windows, spec.window, spec.final),
		"", "| K | offered fwd/s | made fwd/s | published rec/s | dropped | backlog slope | ring slope | reserved slope | bound | sustained |",
		"|---|---|---|---|---|---|---|---|---|---|")
	var equipment []*loadEquipment
	if offered == 0 {
		equipment, offered = h.sweep()
		k := len(equipment)
		v := h.measure(equipment, offered, spec.final, nil)
		h.log("", fmt.Sprintf("Offered rate: %.0f fwd/s at K = %d over %d windows", offered, k, spec.final))
		h.row(k, v)
		if !v.sustained {
			t.Errorf("the offered rate %.0f at K = %d is not sustained: %s", offered, k, v.why)
		}
	} else {
		equipment = h.startEquipment(spec.equipment[len(spec.equipment)-1])
	}
	defer h.stopEquipment(equipment)
	r := h.measurePackURL(equipment, offered)
	gate := r.gate()
	h.log(fmt.Sprintf("gate (%d audited successes, p99 within %s): %s", gateCalls, gateP99, verdict(gate)))
	switch err := r.accept(); {
	case mode == "gate" && gate != nil:
		t.Errorf("PackURL fails the gate: %v", gate)
	case err != nil:
		t.Errorf("PackURL is no valid record: %v", err)
	default:
	}
}

// verdict reads err as a verdict.
func verdict(err error) string {
	if err != nil {
		return "fail: " + err.Error()
	}

	return "pass"
}

// sweep runs each K of the spec at doubling rates while sustained, and returns the largest K's equipment, still running,
// with the offered rate: half the highest rate sustained there.
func (h *loadHarness) sweep() ([]*loadEquipment, float64) {
	spec := h.spec
	var equipment []*loadEquipment
	var highest float64
	summary := make([]string, 0, 3+len(spec.equipment))
	summary = append(summary, "", "| K | highest sustained fwd/s |", "|---|---|")
	for _, k := range spec.equipment {
		h.stopEquipment(equipment)
		equipment = h.startEquipment(k)
		highest = 0
		for rate := spec.start; rate <= spec.top; rate *= 2 {
			v := h.measure(equipment, rate, spec.windows, nil)
			h.row(k, v)
			if !v.sustained {
				break
			}
			highest = rate
		}
		summary = append(summary, fmt.Sprintf("| %d | %.0f |", k, highest))
	}
	h.log(summary...)
	if highest == 0 {
		h.stopEquipment(equipment)
		h.t.Fatalf("no rate sustained at K = %d", len(equipment))
	}

	return equipment, highest / 2
}

func (h *loadHarness) log(lines ...string) { h.report = append(h.report, lines...) }

func (h *loadHarness) row(k int, v loadVerdict) {
	h.log(fmt.Sprintf("| %d | %.0f | %.1f | %.1f | %d | %.2f | %.2f | %.2f | %.2f | %v %s |",
		k, v.offered, v.made, v.published, v.dropped, v.backlogSlope, v.ringSlope, v.reservedSlope, v.bound, v.sustained, v.why))
}

// startEquipment starts k simulated equipment, each its own producer and loopback with its own equipment id, dev's subject prefix and claim bucket,
// an explicit T3, and plain traffic: no withholding, process program or thinning.
func (h *loadHarness) startEquipment(k int) []*loadEquipment {
	h.t.Helper()
	equipment := make([]*loadEquipment, k)
	g, ctx := errgroup.WithContext(h.t.Context())
	g.SetLimit(8) // a start's admission waits on SQLite's one write connection
	for i := range equipment {
		g.Go(func() error {
			return startEquipment(ctx, h.d.Env, &equipment[i], fmt.Sprintf("LOAD-K%d-%03d", k, i), "eqp")
		})
	}
	if err := g.Wait(); err != nil {
		h.stopEquipment(equipment)
		h.t.Fatal(err)
	}

	return equipment
}

// startEquipment starts one simulated equipment on e's bus into *lt, set once its producer exists so a failed start is still stopped,
// its descriptor naming device.
func startEquipment(ctx context.Context, e *Env, lt **loadEquipment, id, device string) error {
	c := e.Config
	t := &loadEquipment{id: id, m: &equipmentMetrics{}}
	cfg := producer.Config{Servers: []string{e.NC.ConnectedUrl()}, SubjectPrefix: c.Bus.SubjectPrefix, ClaimBucket: c.Bus.ClaimBucket, Source: "test-source"}
	var err error
	if t.p, err = producer.New(cfg, producer.WithMetrics(t.m), producer.WithLogger(slog.New(slog.DiscardHandler))); err != nil {
		return err
	}
	*lt = t
	if t.l, err = sim.NewLoopback(t.p, sim.Options{EquipmentID: id, Device: device, SessionID: 1, T3: loadT3}); err != nil {
		return err
	}

	return t.l.Start(ctx)
}

// stopEquipment ends each piece of equipment's capture and closes its producer.
func (h *loadHarness) stopEquipment(equipment []*loadEquipment) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.t.Context()), simStop)
	defer cancel()
	for _, lt := range equipment {
		if lt == nil {
			continue
		}
		if lt.l != nil {
			lt.l.Stop(ctx)
		}
		_ = lt.p.Close()
	}
}

// drive forwards through each piece of equipment at its share of rate until the returned stop, which waits for the forwards in flight.
func (h *loadHarness) drive(ctx context.Context, equipment []*loadEquipment, rate float64) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	every := time.Duration(float64(len(equipment)) / rate * float64(time.Second))
	var wg sync.WaitGroup
	for _, lt := range equipment {
		wg.Go(func() {
			tick := time.NewTicker(every)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if h.forward(ctx, lt) == nil {
						lt.made.Add(1)
					}
				}
			}
		})
	}

	return func() {
		cancel()
		wg.Wait()
	}
}

// sample reads the stream's state, as busState does, and the simulated equipment's counters and gauges.
func (h *loadHarness) sample(equipment []*loadEquipment) loadSample {
	s, err := h.read(equipment)
	if err != nil {
		h.t.Fatal(err)
	}

	return s
}

// read is sample returning its error, for a goroutine other than the test's.
func (h *loadHarness) read(equipment []*loadEquipment) (loadSample, error) {
	info, err := h.stream.Info(h.t.Context())
	if err != nil {
		return loadSample{}, err
	}
	s := loadSample{at: time.Now(), published: info.State.LastSeq, backlog: info.State.Msgs}
	if h.cons != nil {
		ci, err := h.cons.Info(h.t.Context())
		if err != nil {
			return loadSample{}, err
		}
		s.ackPending, s.redelivered = uint64(ci.NumAckPending), uint64(ci.NumRedelivered) //nolint:gosec // counts
	}
	for _, lt := range equipment {
		ring, reserved, dropped := lt.m.read()
		s.made += lt.made.Load()
		s.ring, s.reserved, s.dropped = s.ring+ring, s.reserved+reserved, s.dropped+uint64(dropped)
	}

	return s, nil
}

// measure runs one point: rate forwards a second through equipment, a warm-up, then at least windows of samples.
// during, when not nil, runs from the first sample; its context ends once the least windows have passed,
// and sampling goes on, by whole windows, past its last attempt's end (collectFor), so the judged span holds every attempt.
// Then the forwards stop and the queues and the backlog empty.
func (h *loadHarness) measure(equipment []*loadEquipment, rate float64, windows int, during func(context.Context)) loadVerdict {
	ctx := h.t.Context()
	origin := h.sample(equipment)
	stop := h.drive(ctx, equipment, rate)
	select {
	case <-ctx.Done():
		h.t.Fatal(ctx.Err())
	case <-time.After(h.spec.warmup):
	}
	tick := time.NewTicker(loadTick)
	defer tick.Stop()
	samples := []loadSample{h.sample(equipment)}
	if h.begin != nil {
		h.begin()
	}
	t0 := samples[0].at
	wctx, end := context.WithCancel(ctx)
	defer end()
	ended := make(chan time.Time, 1) // when during's last attempt ended
	if during == nil {
		ended <- t0
	} else {
		go func() {
			during(wctx)
			ended <- time.Now()
		}()
	}
	// Sampling stops at a sample at or past the endpoint collectFor gives once the last attempt has ended,
	// which is past that end too.
	least, need := time.Duration(windows)*h.spec.window, time.Duration(0)
	var last time.Time
	for need == 0 || samples[len(samples)-1].at.Sub(t0) < need {
		<-tick.C
		s := h.sample(equipment)
		samples = append(samples, s)
		if s.at.Sub(t0) >= least {
			end()
		}
		select {
		case last = <-ended:
			need = collectFor(windows, h.spec.window, last.Sub(t0))
		default:
		}
	}
	if h.end != nil {
		h.end()
	}
	h.samples = samples
	stop()
	h.quiesce(equipment)

	return judgeLoad(rate, origin, samples, h.spec.window, last)
}

// quiesce waits for every producer's queues, then the stream's backlog, to empty.
func (h *loadHarness) quiesce(equipment []*loadEquipment) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	limit := time.After(quiesceLimit)
	for {
		s := h.sample(equipment)
		if s.ring == 0 && s.reserved == 0 && s.backlog == 0 {
			return
		}
		select {
		case <-limit:
			h.t.Fatalf("the queues and the backlog did not empty within %s: %+v", quiesceLimit, s)
		case <-tick.C:
		}
	}
}

// measurePackURL runs a point at rate with the PackURL calls on the first equipment's newest pack, over its windows extended until the spec's audited successes,
// counting the flushes they spanned from that equipment's segments: a flush registers one segment of a capture an hour.
func (h *loadHarness) measurePackURL(equipment []*loadEquipment, rate float64) packURLResult {
	var r packURLResult
	eq := equipment[0].id
	cl := tracekeepv1connect.NewMachineServiceClient(trusting(h.t, filepath.Join(h.d.opts.Dir, DevCertFile)), h.d.Env.Config.API.PublicURL)
	r.ingest = h.measure(equipment, rate, h.spec.windows, func(ctx context.Context) {
		began, before := time.Now(), h.segments(eq)
		h.packURLs(ctx, cl, eq, &r)
		hours := int(time.Now().Truncate(time.Hour).Sub(began.Truncate(time.Hour)) / time.Hour)
		r.flushes = int(h.segments(eq)-before) - hours
	})
	minima := make([]string, len(r.ingest.backlogMinima))
	for i, m := range r.ingest.backlogMinima {
		minima[i] = fmt.Sprintf("%.0f", m)
	}
	h.log("", fmt.Sprintf("PackURL at %.0f fwd/s, K = %d:", rate, len(equipment)), "",
		"| p50 | p99 | made | succeeded | superseded | failed | flushes | made fwd/s | published rec/s | backlog minima | sustained |",
		"|---|---|---|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %s | %s | %d | %d | %d | %d | %d | %.1f | %.1f | %s | %v %s |",
			percentile(r.latencies, 0.5), percentile(r.latencies, 0.99), r.made, r.succeeded, r.superseded, r.failed, r.flushes,
			r.ingest.made, r.ingest.published, strings.Join(minima, " "), r.ingest.sustained, r.ingest.why))
	if r.failed > 0 {
		h.log(fmt.Sprintf("first failure after %s: %s", r.took, r.failure))
	}

	return r
}

// segments is the ingester's count of equipment's registered segments.
func (h *loadHarness) segments(equipment string) float64 {
	mfs, err := h.d.Env.Metrics.Registry().Gather()
	if err != nil {
		h.t.Error(err) // called off the test's goroutine
	}
	for _, mf := range mfs {
		if mf.GetName() != "tracekeep_ingest_segments_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "equipment_id" && l.GetValue() == equipment {
					return m.GetCounter().GetValue()
				}
			}
		}
	}

	return 0
}

// newestPack is a pack and its capture.
type newestPack struct{ id, capture string }

// packURLs calls PackURL until ctx ends, one call at a time, ten a second, on equipment's newest pack listed at most a second earlier:
// a superseded attempt lists again and retries, at most three times.
func (h *loadHarness) packURLs(ctx context.Context, cl tracekeepv1connect.MachineServiceClient, equipment string, r *packURLResult) {
	tick := time.NewTicker(packURLPace)
	defer tick.Stop()
	audited := map[string]int{}
	var pack newestPack
	var listed time.Time
	for {
		<-tick.C
		// Past the least windows, the calls go on to the spec's audited successes, unless the record already fails.
		if ctx.Err() != nil && (r.made == 0 || r.succeeded >= h.spec.calls || r.failed > 0 || r.superseded*20 > r.made) {
			return
		}
		for attempt := 0; attempt <= packURLRetries; attempt++ {
			if attempt > 0 || time.Since(listed) >= time.Second {
				var err error
				began := time.Now()
				if pack, err = h.newestPack(ctx, cl, equipment); err != nil {
					r.record(packAttempt{began: began, listing: true, why: "listing: " + err.Error()})
					return
				}
				listed = time.Now()
			}
			if pack.id == "" || !h.call(ctx, cl, pack, audited, r) {
				break
			}
		}
	}
}

// call makes one PackURL attempt on pack, its own deadline outliving ctx, and records it; it reports a superseded refusal.
func (h *loadHarness) call(ctx context.Context, cl tracekeepv1connect.MachineServiceClient, pack newestPack, audited map[string]int, r *packURLResult) bool {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), packURLDeadline)
	defer cancel()
	req := connectrpc.NewRequest(&tkv1.PackURLRequest{PackId: pack.id})
	req.Header().Set("Authorization", "Bearer "+h.token)
	a := packAttempt{began: time.Now()}
	resp, err := cl.PackURL(actx, req)
	a.took = time.Since(a.began)
	switch {
	case connectrpc.CodeOf(err) == connectrpc.CodeFailedPrecondition:
		a.superseded = true
		r.record(a)
		return true
	case err != nil:
		a.why = connectrpc.CodeOf(err).String() + ": " + err.Error()
		r.record(a)
		return false
	}
	// The row names the pack and the URL's expiry; two URLs of one pack in one second share it, so rows are counted against calls.
	suffix := " expiring " + resp.Msg.GetExpires().AsTime().UTC().Format(time.RFC3339)
	audited[pack.id+suffix]++
	n, err := h.auditRows(ctx, pack, suffix)
	if err != nil || n < audited[pack.id+suffix] {
		a.why = fmt.Sprintf("pack %s: %d audit rows for %d URLs, %v", pack.id, n, audited[pack.id+suffix], err)
	}
	r.record(a)

	return false
}

// record counts an attempt: a listing's failure is no call made, a superseded call neither succeeded nor failed,
// and a call with a failure's why failed, the first one's why and RPC time kept.
func (r *packURLResult) record(a packAttempt) {
	r.attempts = append(r.attempts, a)
	if !a.listing {
		r.made++
	}
	switch {
	case a.superseded:
		r.superseded++
	case a.why != "":
		if r.failed == 0 {
			r.failure, r.took = a.why, a.took
		}
		r.failed++
	default:
		r.succeeded++
		r.latencies = append(r.latencies, a.took)
	}
}

// within is r over the attempts begun from from to to, both included.
func (r *packURLResult) within(from, to time.Time) packURLResult {
	var w packURLResult
	for _, a := range r.attempts {
		if !a.began.Before(from) && !a.began.After(to) {
			w.record(a)
		}
	}

	return w
}

// auditRows counts the pack-url rows of pack's capture naming pack and ending in suffix.
func (h *loadHarness) auditRows(ctx context.Context, pack newestPack, suffix string) (int, error) {
	capture, err := tracepack.ParseUUID(pack.capture)
	if err != nil {
		return 0, err
	}
	rows, err := h.d.Env.Catalog.AuditEntries(context.WithoutCancel(ctx), capture)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range rows {
		if e.Action == "pack-url" && strings.HasPrefix(e.Detail, "pack "+pack.id+",") && strings.HasSuffix(e.Detail, suffix) {
			n++
		}
	}

	return n, nil
}

// newestPack lists equipment's packs and returns the one whose last range ends latest, empty for none.
func (h *loadHarness) newestPack(ctx context.Context, cl tracekeepv1connect.MachineServiceClient, equipment string) (newestPack, error) {
	now := time.Now()
	req := connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: &tkv1.Selector{
		EquipmentId: equipment, From: timestamppb.New(now.Add(-2 * time.Hour)), To: timestamppb.New(now.Add(time.Hour)),
	}})
	req.Header().Set("Authorization", "Bearer "+h.token)
	resp, err := cl.ListPacks(context.WithoutCancel(ctx), req)
	if err != nil {
		return newestPack{}, err
	}
	var out newestPack
	var latest time.Time
	for _, hour := range resp.Msg.GetHours() {
		for _, s := range hour.GetScopes() {
			for _, p := range s.GetPacks() {
				if rs := p.GetRanges(); len(rs) > 0 && rs[len(rs)-1].GetTo().AsTime().After(latest) {
					out, latest = newestPack{id: p.GetPackId(), capture: s.GetCaptureId()}, rs[len(rs)-1].GetTo().AsTime()
				}
			}
		}
	}

	return out, nil
}

// loadEngine is TRACEKEEP_LOAD_ENGINE's catalog engine, sqlite when unset; mariadb requires TRACEKEEP_MARIADB_DSN.
func loadEngine(t *testing.T) string {
	t.Helper()
	switch e := os.Getenv("TRACEKEEP_LOAD_ENGINE"); e {
	case "", catalog.EngineSQLite:
		return catalog.EngineSQLite
	case catalog.EngineMariaDB:
		if os.Getenv(catalogtest.EnvMariaDB) == "" {
			t.Fatalf("TRACEKEEP_LOAD_ENGINE=mariadb needs %s", catalogtest.EnvMariaDB)
		}
		return e
	default:
		t.Fatalf("TRACEKEEP_LOAD_ENGINE=%q: want sqlite or mariadb", e)
		return ""
	}
}
