package service

// The load harness's measuring modes, ceiling, claimcheck and production (load_test.go's header):
// each point reads the ingester's test seams, the catalog's statements, the consumer and the process's CPU over its measured span.

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"
	"github.com/nats-io/nats.go/jetstream"
	"modernc.org/sqlite"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/ingest"
)

// loadModes are TRACEKEEP_LOAD's measuring modes, each run at full size or, smoke, as a few minutes' check of itself.
var loadModes = map[string]func(t *testing.T, smoke bool){
	"ceiling": loadCeiling, "claimcheck": loadClaimCheck, "production": loadProduction, "recovery": loadRecovery,
}

const (
	// blockRate and mutexFraction are the ceiling's block and mutex profile rates: blocking sampled about once every millisecond blocked,
	// coarse enough not to slow the bus server's many short waits, and one contention event in 100.
	blockRate     = 1_000_000
	mutexFraction = 100
	// leastCycles are the completed drain cycles a production-timing run is judged over.
	leastCycles = 6
)

// stages are the ingest stages the Timed seam reports, in a delivery's order; flush is a batch's whole flush.
var stages = []string{"dispose", "batch", "write", "put", "register", "acknowledge", "flush"}

// loadCeiling runs K = 10 and 100 at 1,600 and 3,200 fwd/s under dev's pending slots, then 3,200 fwd/s with the slots raised past the held records,
// each budget on a dev of its own, since a consumer's budget is set when ingest starts.
func loadCeiling(t *testing.T, smoke bool) {
	spec := loadSpec{flush: 10 * time.Second, warmup: time.Minute, window: 10 * time.Second, windows: 12, probed: true}
	tools, rates, raised := []int{10, 100}, []float64{1600, 3200}, 200_000
	if smoke {
		spec = loadSpec{flush: time.Second, warmup: 5 * time.Second, window: 5 * time.Second, windows: 2, probed: true}
		tools, rates, raised = []int{2}, []float64{20, 40}, 2_000
	}
	dir := profileDir(t)
	countSQL(t)
	runtime.SetBlockProfileRate(blockRate)
	runtime.SetMutexProfileFraction(mutexFraction)
	t.Cleanup(func() {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
	})
	for _, budget := range []int{0, raised} {
		points, name := rates, fmt.Sprintf("max_ack_pending=%d", budget)
		if budget == 0 {
			name = "max_ack_pending=dev's"
		} else {
			points = rates[len(rates)-1:]
		}
		t.Run(name, func(t *testing.T) {
			spec.maxAckPending = budget
			h := newReportingHarness(t, spec)
			h.log(fmt.Sprintf("ceiling: flush %s, warm-up %s, %d windows of %s a point; profiles in %s", spec.flush, spec.warmup, spec.windows, spec.window, dir))
			for _, k := range tools {
				ts := h.startTools(k)
				for _, rate := range points {
					h.point(ts, rate, dir)
				}
				h.stopTools(ts)
			}
		})
	}
}

// loadClaimCheck runs K = 10 with every tenth forward's records above the inline limit, then every forward's,
// at rates giving the same claim-check bytes a second: about 40 MiB of claim objects at dev's 1 MiB max_payload.
func loadClaimCheck(t *testing.T, smoke bool) {
	spec := loadSpec{flush: 10 * time.Second, warmup: time.Minute, window: 10 * time.Second, windows: 12, probed: true}
	k := 10
	points := []struct {
		every uint32
		rate  float64
	}{{10, 200}, {1, 20}}
	if smoke {
		spec = loadSpec{flush: time.Second, warmup: 5 * time.Second, window: 5 * time.Second, windows: 2, probed: true}
		k, points[0].rate, points[1].rate = 2, 20, 4
	}
	countSQL(t)
	h := newReportingHarness(t, spec)
	// An item of max_payload bytes makes a record above max_payload − 4096 whatever its framing; random, so no pack compresses it away.
	h.claimItem = make([]byte, h.d.Env.NC.MaxPayload())
	_, _ = rand.Read(h.claimItem)
	h.log(fmt.Sprintf("claimcheck: flush %s, warm-up %s, %d windows of %s a point, claim-check items of %d bytes",
		spec.flush, spec.warmup, spec.windows, spec.window, len(h.claimItem)))
	tools := h.startTools(k)
	defer h.stopTools(tools)
	for _, p := range points {
		h.claimEvery = p.every
		h.point(tools, p.rate, "")
	}
}

// loadProduction runs ingest's flush and ack_wait defaults at 1,600 records a second over K = 100, with max_ack_pending sized past the held records
// and then dev's 50,000 as its control, each on a dev of its own, judged over completed drain cycles.
func loadProduction(t *testing.T, smoke bool) {
	spec := loadSpec{flush: 5 * time.Minute, ackWait: 7 * time.Minute, probed: true}
	k, rate, budgets := 100, 800.0, []int{600_000, 50_000}
	if smoke {
		spec.flush, spec.ackWait = 5*time.Second, 9*time.Second
		k, rate, budgets = 2, 20, []int{2_000, 50}
	}
	dir := profileDir(t)
	countSQL(t)
	for _, budget := range budgets {
		t.Run(fmt.Sprintf("max_ack_pending=%d", budget), func(t *testing.T) {
			spec.maxAckPending = budget
			h := newReportingHarness(t, spec)
			h.log(fmt.Sprintf("production: flush %s, ack_wait %s, %.0f fwd/s over K = %d; profiles in %s", spec.flush, spec.ackWait, rate, k, dir))
			tools := h.startTools(k)
			defer h.stopTools(tools)
			h.cycles(tools, rate, dir)
		})
	}
}

// newReportingHarness is newLoadHarness printing its report when t ends.
func newReportingHarness(t *testing.T, spec loadSpec) *loadHarness {
	t.Helper()
	h := newLoadHarness(t, spec)
	t.Cleanup(func() { t.Log("\n" + strings.Join(h.report, "\n")) })

	return h
}

// profileDir is TRACEKEEP_LOAD_PROFILES, or a new temporary directory, which outlives the run.
func profileDir(t *testing.T) string {
	t.Helper()
	if d := os.Getenv("TRACEKEEP_LOAD_PROFILES"); d != "" {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
		return d
	}
	d, err := os.MkdirTemp("", "tracekeep-load-")
	if err != nil {
		t.Fatal(err)
	}

	return d
}

// consumer finds the traffic and parked consumers ingest created and asserts that each one's effective configuration is the one dev was given.
func (h *loadHarness) consumer() {
	ctx := h.t.Context()
	parking, err := h.d.Env.JS.Stream(ctx, bus.StreamName(h.d.Env.Bus(), catalog.RoleParking))
	if err != nil {
		h.t.Fatal(err)
	}
	h.cons, h.parked = h.effective(h.stream, "tk-ingest-"), h.effective(parking, "tk-parked-") // bus.ConsumerName's
}

// effective finds s's consumer named with prefix and asserts its effective configuration.
func (h *loadHarness) effective(s jetstream.Stream, prefix string) jetstream.Consumer {
	ctx := h.t.Context()
	names := s.ConsumerNames(ctx)
	var name string
	for n := range names.Name() {
		if strings.HasPrefix(n, prefix) {
			name = n
		}
	}
	if err := names.Err(); err != nil {
		h.t.Fatal(err)
	}
	cons, err := s.Consumer(ctx, name)
	if err != nil {
		h.t.Fatal(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	c := h.d.Env.Config.Ingest
	if err := effective(name, info.Config, c); err != nil {
		h.t.Fatal(err)
	}
	h.log(fmt.Sprintf("effective consumer %s: ack_wait %s, max_ack_pending %d, the heartbeat every %s", name, info.Config.AckWait, info.Config.MaxAckPending, c.AckWait/3))

	return cons
}

// forward relays one request through lt: every claimEvery-th a claim-check one, the rest as the loopback's Forward does.
func (h *loadHarness) forward(ctx context.Context, lt *loadTool) error {
	if h.claimEvery == 0 || lt.forwards.Add(1)%h.claimEvery != 0 {
		return lt.l.Forward(ctx)
	}

	return lt.forwardClaim(ctx, h.claimItem)
}

// forwardClaim relays an S1F3 holding item through the replay path, whose equipment echoes it,
// so its primary and its reply each travel by claim-check; its system bytes, the high bit set, never meet Forward's.
func (lt *loadTool) forwardClaim(ctx context.Context, item []byte) error {
	var sys [4]byte
	binary.BigEndian.PutUint32(sys[:], 1<<31|lt.claims.Add(1))
	msg, err := hsms.NewDataMessage(1, 3, true, 1, sys, secs2.B(item))
	if err != nil {
		return err
	}

	return lt.l.Replay(ctx, tracepack.DirHostToEquipment, msg.ToBytes())
}

// stageTotal is a stage's calls and their summed wall time.
type stageTotal struct {
	n   int
	sum time.Duration
}

// loadProbe gathers what the ingester's test seams report: each stage's wall time, each settled flush pass,
// and, while aging, the age of each delivery a settled pass acknowledged.
type loadProbe struct {
	mu     sync.Mutex
	stages map[string]*stageTotal
	passes []settledPass
	aging  bool
	ages   []time.Duration
	wg     sync.WaitGroup
	// firsts, once since is set (sinceAt), are each stage's first report begun at or after it;
	// events, every report of an acknowledgement stage (ackStages) begun at or after it, so they grow with the parked cohort alone.
	since  time.Time
	firsts map[string]stageSpan
	events []stageEvent
}

// ackStages are the parked consumer's acknowledgement stages, done or failed: the drain's acknowledgement time, which the decision reads.
var ackStages = []string{
	"parked/prepare-ack", "parked/prepare-ack/failed", "parked/double-ack", "parked/double-ack/failed", "parked/ack-copy", "parked/ack-copy/failed",
}

// stageSpan is when one report of a stage began and ended.
type stageSpan struct{ start, end time.Time }

// stageEvent is one report of a stage: when it began and its wall time.
type stageEvent struct {
	stage string
	start time.Time
	took  time.Duration
}

func (p *loadProbe) timed(stage string, took time.Duration) { p.record(stage, time.Now(), took) }

// record counts a report of stage that ended at end.
func (p *loadProbe) record(stage string, end time.Time, took time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stages[stage]
	if s == nil {
		s = &stageTotal{}
		p.stages[stage] = s
	}
	s.n++
	s.sum += took
	start := end.Add(-took)
	if p.firsts == nil || start.Before(p.since) {
		return
	}
	if _, ok := p.firsts[stage]; !ok {
		p.firsts[stage] = stageSpan{start: start, end: end}
	}
	if slices.Contains(ackStages, stage) {
		p.events = append(p.events, stageEvent{stage: stage, start: start, took: took})
	}
}

// within is each acknowledgement stage's reports begun from from to to, both included, of those begun since sinceAt.
func (p *loadProbe) within(from, to time.Time) map[string]stageTotal {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]stageTotal{}
	for _, e := range p.events {
		if e.start.Before(from) || e.start.After(to) {
			continue
		}
		s := out[e.stage]
		s.n++
		s.sum += e.took
		out[e.stage] = s
	}

	return out
}

// sinceAt starts the firsts at at.
func (p *loadProbe) sinceAt(at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.since, p.firsts = at, map[string]stageSpan{}
}

// first is stage's first report begun since sinceAt, false for none.
func (p *loadProbe) first(stage string) (stageSpan, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.firsts[stage]

	return s, ok
}

// count is stage's reports so far.
func (p *loadProbe) count(stage string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.stages[stage]; s != nil {
		return s.n
	}

	return 0
}

// settled records a pass. A delivery's age is the pass's settlement less the time the stream stored its message,
// read off the ingester's goroutine: the age at its acknowledgement or a little more, and, for a message that waited in the stream
// or was redelivered, more than the age of the delivery acknowledged.
func (p *loadProbe) settled(at time.Time, batches []*ingest.Batch, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.passes = append(p.passes, settledPass{at: at, failed: err != nil})
	if !p.aging || err != nil {
		return
	}
	p.wg.Go(func() {
		var ages []time.Duration
		for _, b := range batches {
			for _, e := range b.Entries {
				if m, ok := e.Msg.(jetstream.Msg); ok {
					if md, err := m.Metadata(); err == nil {
						ages = append(ages, at.Sub(md.Timestamp))
					}
				}
			}
		}
		p.mu.Lock()
		p.ages = append(p.ages, ages...)
		p.mu.Unlock()
	})
}

// age starts or stops reading the ages, a stop waiting for those under way.
func (p *loadProbe) age(on bool) {
	p.mu.Lock()
	p.aging = on
	p.mu.Unlock()
	if !on {
		p.wg.Wait()
	}
}

// Each read copies only its own part, under the lock the ingester's seams take: the ages, which grow large, are read once, at the end.
func (p *loadProbe) readStages() map[string]stageTotal {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := make(map[string]stageTotal, len(p.stages))
	for k, v := range p.stages {
		st[k] = *v
	}

	return st
}

func (p *loadProbe) readPasses() []settledPass {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.passes)
}

func (p *loadProbe) readAges() []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.ages)
}

// sqlStatements are the catalog's SQL statements the process prepared since countSQL, by kind, read through an authorizer on every SQLite connection:
// SQLite authorizes a statement as it prepares it, which the driver does at every execution.
// A SELECT counts each SELECT it holds, subqueries and compound parts included; an INSERT or DELETE once;
// an UPDATE once for its run of columns, so two consecutive UPDATEs of one table count once.
var sqlStatements struct {
	once                 sync.Once
	reads, writes, trans atomic.Uint64
}

// countSQL installs the counting authorizer on the SQLite connections opened from now on, once a process.
// It is process-wide and stays: TestLoad calls it before dev opens its catalog, never a test sharing the process with others.
func countSQL(t *testing.T) {
	t.Helper()
	sqlStatements.once.Do(func() {
		sqlite.RegisterConnectionHook(func(c sqlite.ExecQuerierContext, _ string) error {
			a, ok := c.(sqlite.AuthorizerRegisterer)
			if !ok {
				return errors.New("load: the SQLite connection takes no authorizer")
			}
			var prev sqlite.AuthorizerActionCode
			var prevTable string

			return a.RegisterAuthorizer(func(action sqlite.AuthorizerActionCode, table, _, _, _ string) sqlite.AuthorizerReturnCode {
				switch action { //nolint:exhaustive // only a statement's own action counts
				case sqlite.AuthSelect:
					sqlStatements.reads.Add(1)
				case sqlite.AuthInsert, sqlite.AuthDelete:
					sqlStatements.writes.Add(1)
				case sqlite.AuthUpdate:
					if prev != sqlite.AuthUpdate || prevTable != table {
						sqlStatements.writes.Add(1)
					}
				case sqlite.AuthTransaction:
					sqlStatements.trans.Add(1)
				default:
				}
				if action != sqlite.AuthRead && action != sqlite.AuthFunction { // which an UPDATE's columns interleave
					prev, prevTable = action, table
				}

				return sqlite.AuthorizerOK
			})
		})
	})
}

// loadMark is what a measured span is reckoned from: the probe's stages, the catalog's statements, the process's CPU,
// ingest's counters and the claim objects the tools put, and when.
type loadMark struct {
	at                                time.Time
	stages                            map[string]stageTotal
	reads, writes, trans              uint64
	cpu                               time.Duration
	disposed, redelivered, inProgress float64
	claims                            float64
}

func (h *loadHarness) mark(tools []*loadTool) loadMark {
	m := loadMark{
		at: time.Now(), cpu: processCPU(h.t),
		reads: sqlStatements.reads.Load(), writes: sqlStatements.writes.Load(), trans: sqlStatements.trans.Load(),
		disposed: h.counter("tracekeep_ingest_messages_total"), redelivered: h.counter("tracekeep_ingest_redelivered_total"),
		inProgress: h.counter("tracekeep_ingest_in_progress_acks_total"),
	}
	m.stages = h.probe.readStages()
	for _, lt := range tools {
		lt.m.mu.Lock()
		m.claims += lt.m.claims
		lt.m.mu.Unlock()
	}

	return m
}

// counter sums every series of the counter family name in dev's registry.
func (h *loadHarness) counter(name string) float64 {
	mfs, err := h.d.Env.Metrics.Registry().Gather()
	if err != nil {
		h.t.Fatal(err)
	}
	var sum float64
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				sum += m.GetCounter().GetValue()
			}
		}
	}

	return sum
}

// processCPU is the process's user and system CPU so far.
func processCPU(t *testing.T) time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatal(err)
	}

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// profiler writes a measured span's profiles under dir, named for it: CPU over the span,
// and the allocation, block and mutex profiles at its start and its end, cumulative since the process started,
// so the span's own are its end's less its start's (go tool pprof -diff_base <start> <end>). An empty dir writes none.
type profiler struct {
	dir, name string
	cpu       *os.File
}

func (p *profiler) start(t *testing.T) {
	if p.dir == "" {
		return
	}
	f, err := os.Create(filepath.Join(p.dir, p.name+"-cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	p.cpu = f
	p.write(t, "start")
}

func (p *profiler) stop(t *testing.T) {
	if p.cpu == nil {
		return
	}
	pprof.StopCPUProfile()
	if err := p.cpu.Close(); err != nil {
		t.Fatal(err)
	}
	p.write(t, "end")
}

func (p *profiler) write(t *testing.T, when string) {
	for _, kind := range []string{"allocs", "block", "mutex"} {
		f, err := os.Create(filepath.Join(p.dir, p.name+"-"+kind+"-"+when+".pprof"))
		if err != nil {
			t.Fatal(err)
		}
		err = pprof.Lookup(kind).WriteTo(f, 0)
		if err = errors.Join(err, f.Close()); err != nil {
			t.Fatal(err)
		}
	}
}

// point measures rate through tools as a sweep point is measured and judged, reporting beside its verdict, over its windows,
// each stage's wall time, the catalog's statements, the consumer's pending and redelivered counts, the process's CPU,
// the share of its records that travelled by claim-check and acknowledgement's share of a flush, its profiles written under dir.
func (h *loadHarness) point(tools []*loadTool, rate float64, dir string) {
	budget := h.d.Env.Config.Ingest.MaxAckPending
	prof := &profiler{dir: dir, name: fmt.Sprintf("ceiling-K%d-%.0f-pending%d", len(tools), rate, budget)}
	var from, to loadMark
	h.begin = func() {
		from = h.mark(tools)
		prof.start(h.t)
	}
	h.end = func() {
		prof.stop(h.t)
		to = h.mark(tools)
	}
	defer func() { h.begin, h.end = nil, nil }()
	v := h.measure(tools, rate, h.spec.windows, nil)
	first, last := h.samples[0], h.samples[len(h.samples)-1]
	published := float64(last.published - first.published)
	label := fmt.Sprintf("K = %d at %.0f fwd/s, max_ack_pending %d", len(tools), rate, budget)
	if h.claimEvery > 0 {
		label += fmt.Sprintf(", one forward in %d by claim-check", h.claimEvery)
	}
	h.log("", label+":", "",
		"| K | offered fwd/s | made fwd/s | published rec/s | dropped | backlog slope | ring slope | reserved slope | bound | sustained |",
		"|---|---|---|---|---|---|---|---|---|---|")
	h.row(len(tools), v)
	h.spanRows(from, to, h.samples)
	st := stageDelta(from, to)
	h.log("", "| claim-check share of records | acknowledge share of flush |", "|---|---|",
		fmt.Sprintf("| %.1f%% | %.1f%% |", 100*(to.claims-from.claims)/published, 100*st["acknowledge"].sum.Seconds()/st["flush"].sum.Seconds()))
	if dir != "" {
		h.log("", "profiles: "+filepath.Join(dir, prof.name+"-*.pprof"))
	}
}

// spanRows reports a measured span's statements, consumer, counters and CPU, and its stages.
func (h *loadHarness) spanRows(from, to loadMark, samples []loadSample) {
	msgs := to.disposed - from.disposed
	var ackPending, redelivered uint64
	for _, s := range samples {
		ackPending, redelivered = max(ackPending, s.ackPending), max(redelivered, s.redelivered)
	}
	per := func(n uint64) float64 { return float64(n) / msgs }
	h.log("", "| messages | SQL reads/msg | SQL writes/msg | SQL BEGIN/COMMIT/msg | max ack pending | max redelivered pending | redeliveries | in-progress acks | process CPU cores |",
		"|---|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %.3f | %.3f | %.3f | %d | %d | %.0f | %.0f | %.2f |", msgs,
			per(to.reads-from.reads), per(to.writes-from.writes), per(to.trans-from.trans), ackPending, redelivered,
			to.redelivered-from.redelivered, to.inProgress-from.inProgress, (to.cpu-from.cpu).Seconds()/to.at.Sub(from.at).Seconds()))
	st := stageDelta(from, to)
	header, sep, row := "|", "|", "|"
	for _, name := range stages {
		s := st[name]
		mean := time.Duration(0)
		if s.n > 0 {
			mean = s.sum / time.Duration(s.n)
		}
		header, sep, row = header+" "+name+" s (calls, mean) |", sep+"---|", row+fmt.Sprintf(" %.2f (%d, %s) |", s.sum.Seconds(), s.n, mean)
	}
	h.log("", header, sep, row)
}

// stageDelta is each stage's calls and time between two marks.
func stageDelta(from, to loadMark) map[string]stageTotal {
	out := make(map[string]stageTotal, len(to.stages))
	for k, v := range to.stages {
		out[k] = stageTotal{n: v.n - from.stages[k].n, sum: v.sum - from.stages[k].sum}
	}

	return out
}

// cycles runs rate through tools at ingest's own flush and ack_wait: a warm-up of a flush plus ack_wait,
// then samples every second until at least leastCycles completed drain cycles span at least three heartbeat periods,
// or a pass fails, or the span reaches leastCycles+2 flushes plus ack_waits; it judges them and reports the run's counts.
func (h *loadHarness) cycles(tools []*loadTool, rate float64, dir string) {
	ctx := h.t.Context()
	c := h.d.Env.Config.Ingest
	origin := h.sample(tools)
	stop := h.drive(ctx, tools, rate)
	defer stop()
	select {
	case <-ctx.Done():
		h.t.Fatal(ctx.Err())
	case <-time.After(c.FlushInterval + c.AckWait):
	}
	prof := &profiler{dir: dir, name: fmt.Sprintf("production-pending%d", c.MaxAckPending)}
	from := h.mark(tools)
	prof.start(h.t)
	h.probe.age(true)
	samples := []loadSample{h.sample(tools)}
	limit := time.Duration(leastCycles+2) * (c.FlushInterval + c.AckWait)
	tick := time.NewTicker(loadTick)
	defer tick.Stop()
	var v cycleVerdict
	failed := 0
	for failed == 0 {
		<-tick.C
		samples = append(samples, h.sample(tools))
		passes := h.probe.readPasses()
		for _, p := range passes {
			if p.failed {
				failed++
			}
		}
		v = judgeCycles(rate, origin, samples, passes, leastCycles)
		if span := samples[len(samples)-1].at.Sub(samples[0].at); (len(v.minima) >= leastCycles && span >= c.AckWait) || span >= limit {
			break
		}
	}
	h.probe.age(false)
	prof.stop(h.t)
	to := h.mark(tools)
	passes, ages := h.probe.readPasses(), h.probe.readAges()
	if failed > 0 {
		h.t.Errorf("%d flush passes failed", failed)
	}
	minima := make([]string, len(v.minima))
	for i, m := range v.minima {
		minima[i] = fmt.Sprintf("%.0f@%.0fs", m.backlog, m.at.Sub(samples[0].at).Seconds())
	}
	h.log("", fmt.Sprintf("max_ack_pending %d, %d passes settled, %d of them failed:", c.MaxAckPending, len(passes), failed), "",
		"| offered fwd/s | made fwd/s | published rec/s | dropped | cycles | backlog minima (records@s) | backlog slope /s | ring slope /s | reserved slope /s | bound /s | sustained |",
		"|---|---|---|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %.1f | %.1f | %d | %d | %s | %.2f | %.2f | %.2f | %.2f | %v %s |", v.offered, v.made, v.published, v.dropped, len(v.minima),
			strings.Join(minima, " "), v.backlogSlope, v.ringSlope, v.reservedSlope, v.bound, v.sustained, v.why))
	h.spanRows(from, to, samples)
	msgs := to.disposed - from.disposed
	h.log("", "| messages received | in-progress acks | in-progress acks / received | ages acknowledged | age p50 | age p90 | age p99 | age max |",
		"|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %.0f | %.3f | %d | %s | %s | %s | %s |", msgs, to.inProgress-from.inProgress, (to.inProgress-from.inProgress)/msgs,
			len(ages), percentile(ages, 0.5).Round(time.Millisecond), percentile(ages, 0.9).Round(time.Millisecond),
			percentile(ages, 0.99).Round(time.Millisecond), percentile(ages, 1).Round(time.Millisecond)))
	if prof.cpu != nil {
		h.log("", "the bus server's CPU: go tool pprof -top -focus 'nats-server/v2/server' "+filepath.Join(dir, prof.name+"-cpu.pprof"))
	}
}
