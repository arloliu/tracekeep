package service

// The load harness's recovery mode (load_test.go's header): a recovery backlog's drain beside the offered rate.
// One more tool's capture is rejected by a refused descriptor and its start and M data records parked capture-rejected;
// once the cohort is parked by identity, a release switches the descriptor acceptance the ingester consults
// while the parked consumer is held at a step's start, reads the monotonic clock and lets the consumer go;
// the drain runs to every version registered and every copy's row acked, read from the catalog by identity.
// loaddrain_test.go judges what it records.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

const (
	// blockerTool is the rejected capture's tool; blockerEvery paces its forwards, two records each.
	blockerTool  = "LOAD-PARKED"
	blockerEvery = time.Millisecond
	// cohortPoll paces the catalog reads of the cohort, which grow with M, off the sampling;
	// cohortIdle bounds the time between two reads of the drain's cohort whatever the probe counts.
	cohortPoll = time.Second
	cohortIdle = 30 * time.Second
)

// recoveryStages are the stages the recovery mode reports for each consumer, in a delivery's order.
var recoveryStages = []string{"copy", "dispose", "admit", "batch", "flush", "write", "put", "register", "acknowledge", "prepare-ack", "double-ack", "ack-copy"}

// recovery is a rejected capture's release on an Env: the seams it installs before ingest starts, the cohort's identity, and the probe it reads.
type recovery struct {
	e        *Env
	probe    *loadProbe
	released atomic.Bool
	// mu guards the hold: armed, the parked consumer stops at its next step's start, closing arrived, until open closes.
	mu            sync.Mutex
	armed         bool
	arrived, open chan struct{}
	// n, the cohort's versions, capture and incs are known once it is parked.
	n       int
	capture tracepack.UUID
	incs    map[string]catalog.Incarnation
	parking jetstream.Stream
}

// newRecovery installs the release's seams on e: the descriptor acceptance, refusing a blank device until the release, and the parked consumer's hold.
// The probe must be e's timed seam.
func newRecovery(e *Env, probe *loadProbe) *recovery {
	r := &recovery{e: e, probe: probe}
	e.accepts = func(d []byte) bool { return r.released.Load() || ingest.AcceptedDescriptor(d) }
	e.stepping = r.step

	return r
}

// step is the parked consumer's Stepping: held while armed.
func (r *recovery) step(ctx context.Context) {
	r.mu.Lock()
	armed, arrived, open := r.armed, r.arrived, r.open
	if armed {
		r.armed = false // one step holds; letGo opens it
		close(arrived)
	}
	r.mu.Unlock()
	if !armed {
		return
	}
	select {
	case <-open:
	case <-ctx.Done():
	}
}

// barrier waits, polling every poll for at most limit, until the blocker's capture is parked by identity (cohortParked):
// its start, its connection's control frames and events it observed and its m data records, each a version.
// The originals' acknowledgements the probe counts start each catalog read.
func (r *recovery) barrier(ctx context.Context, blocker *toolMetrics, m int, poll, limit time.Duration) error {
	ss, incs, err := r.e.streams(ctx)
	if err != nil {
		return err
	}
	r.incs, r.parking = incs, ss[catalog.RoleParking]
	tick := time.NewTicker(poll)
	defer tick.Stop()
	deadline := time.After(limit)
	why := "no original acknowledged"
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("the cohort was not parked within %s: %s", limit, why)
		case <-tick.C:
		}
		blocker.mu.Lock()
		data, other := blocker.data, blocker.other
		blocker.mu.Unlock()
		if int(data) != m {
			why = fmt.Sprintf("%.0f of %d data records observed", data, m)
			continue
		}
		if r.n = 1 + int(other) + m; r.probe.count("ack-copy") < r.n {
			why = fmt.Sprintf("%d of %d originals acknowledged", r.probe.count("ack-copy"), r.n)
			continue
		}
		vs, err := r.e.Catalog.ParkedVersions(ctx, nil)
		if err != nil {
			return err
		}
		info, err := r.parking.Info(ctx, jetstream.WithSubjectFilter(">"))
		if err != nil {
			return err
		}
		if why = cohortParked(vs, info.State.Subjects, r.n, incs[catalog.RoleTraffic].ID); why == "" {
			r.capture = vs[0].Version.Capture
			return nil
		}
	}
}

// release holds the parked consumer at its next step's start, waiting at most limit for it, switches the descriptor acceptance,
// reads the clock and starts the probe's firsts there; it returns the clock and what lets the consumer go.
func (r *recovery) release(ctx context.Context, limit time.Duration) (time.Time, func(), error) {
	r.mu.Lock()
	r.armed, r.arrived, r.open = true, make(chan struct{}), make(chan struct{})
	arrived, open := r.arrived, r.open
	r.mu.Unlock()
	select {
	case <-arrived:
	case <-ctx.Done():
		return time.Time{}, nil, ctx.Err()
	case <-time.After(limit):
		return time.Time{}, nil, fmt.Errorf("the parked consumer did not reach a step's start within %s", limit)
	}
	r.released.Store(true)
	clock := time.Now()
	r.probe.sinceAt(clock)

	return clock, func() { close(open) }, nil
}

// drained reads the cohort from the catalog once: empty when the drain is complete (cohortDrained).
func (r *recovery) drained(ctx context.Context) (string, error) {
	vs, err := r.e.Catalog.ParkedVersions(ctx, &r.capture)
	if err != nil {
		return "", err
	}

	return cohortDrained(vs, r.n, r.incs[catalog.RoleParking].ID)
}

// drainEnd is a drain's completion, the return of the first catalog read finding the cohort complete, or why it cannot complete.
type drainEnd struct {
	at  time.Time
	err error
}

// watch reads the cohort from the catalog, which alone decides its completion, and sends the completion, or a state the drain cannot leave;
// why holds the last read's reason.
// It reads every poll once the probe's acknowledgements of copies, their bookkeeping done or failed, cover the cohort, and every idle regardless:
// a copy the broker confirmed without its bookkeeping, or whose confirmation's answer was lost, is completed by the reconciler,
// which times no acknowledgement. The completion is late by at most a poll and a read, inside the drain's clock.
func (r *recovery) watch(ctx context.Context, poll, idle time.Duration, why *atomic.Value) <-chan drainEnd {
	out := make(chan drainEnd, 1)
	go func() {
		tick := time.NewTicker(poll)
		defer tick.Stop()
		var read time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if n := r.probe.count("parked/ack-copy") + r.probe.count("parked/ack-copy/failed"); n < r.n && time.Since(read) < idle {
				why.Store(fmt.Sprintf("%d of %d copies' acknowledgements timed", n, r.n))
				continue
			}
			w, err := r.drained(ctx)
			read = time.Now()
			if err != nil {
				if ctx.Err() == nil {
					out <- drainEnd{err: err}
				}
				return
			}
			if w == "" {
				out <- drainEnd{at: read}
				return
			}
			why.Store(w)
		}
	}()

	return out
}

// loadRecovery runs the recovery drain's episodes, each on a dev of its own, until no condition is unknown or three ran,
// and reports the decision.
func loadRecovery(t *testing.T, smoke bool) {
	// The ack wait is production's default, not dev's minute: a parked flush acknowledging tens of thousands of copies
	// at about 0.6 ms each could outlast a minute, be given up and redeliver, which would measure dev's setting, not the drain.
	spec := loadSpec{flush: 10 * time.Second, warmup: time.Minute, ackWait: 7 * time.Minute, maxAckPending: recoveryPending, probed: true, recovery: true}
	k, rate, def, limit := 100, 800.0, 0, 2*time.Hour
	if smoke {
		spec.flush, spec.warmup, spec.ackWait, spec.redelivery = time.Second, 5*time.Second, 0, 8*time.Second
		k, rate, def, limit = 2, 20, 200, 3*time.Minute
	}
	m, err := recoveryM(os.Getenv("TRACEKEEP_LOAD_RECOVERY_M"), def)
	if err == nil && m == 0 {
		err = errors.New("TRACEKEEP_LOAD_RECOVERY_M is not set: want 1000, 10000 or 100000")
	}
	if err != nil {
		t.Fatal(err)
	}
	var eps []drainEpisode
	for i := 1; i <= drainEpisodes; i++ {
		t.Run(fmt.Sprintf("episode=%d", i), func(t *testing.T) { eps = append(eps, recoveryEpisode(t, spec, k, rate, m, limit)) })
		if t.Failed() || !decide(eps).rerun() {
			break
		}
	}
	d := decide(eps)
	note := ""
	if m != 100_000 {
		note = " (the decision is read at M of 100,000)"
	}
	t.Log("\n" + strings.Join([]string{
		fmt.Sprintf("recovery decision at M = %d over %d episodes%s:", m, len(eps), note), "",
		"| traffic not sustained | PackURL p99 past 2 s | acknowledgement ≥ 20% of a drain > 5 min | p99 (calls) | outcome |",
		"|---|---|---|---|---|",
		fmt.Sprintf("| %s | %s | %s | %s (%d) | %s |", d.traffic, d.p99, d.ack, p99Text(d), d.calls, d.outcome()),
	}, "\n"))
}

// p99Text is the pooled p99, or what keeps it unread.
func p99Text(d drainDecision) string {
	switch {
	case d.p99 == unknown:
		return "unread"
	case d.p99v == failedCall:
		return "failed"
	default:
		return d.p99v.Round(time.Millisecond).String()
	}
}

// recoveryEpisode runs one episode on a dev of its own: the offered rate, a warm-up, the cohort parked, a settling warm-up,
// the release, and the drain to completion, sampled every second, with PackURL paced through it.
func recoveryEpisode(t *testing.T, spec loadSpec, k int, rate float64, m int, limit time.Duration) drainEpisode {
	h := newReportingHarness(t, spec)
	ctx := t.Context()
	c := h.d.Env.Config
	h.log(fmt.Sprintf("recovery: flush %s, ack_wait %s, parked redelivery %s, %.0f fwd/s over K = %d, M = %d; warm-up %s",
		c.Ingest.FlushInterval, c.Ingest.AckWait, c.Parking.RedeliveryDelay, rate, k, m, spec.warmup))
	tools := h.startTools(k)
	defer h.stopTools(tools)
	var blocker *loadTool
	err := startTool(ctx, h.d.Env, &blocker, blockerTool, " ") // a blank device: refused
	defer h.stopTools([]*loadTool{blocker})
	if err != nil {
		t.Fatal(err)
	}
	stop := h.drive(ctx, tools, rate)
	defer stop()
	wait(t, spec.warmup)

	began := time.Now()
	if err := park(ctx, blocker, m); err != nil {
		t.Fatal(err)
	}
	if err := h.rec.barrier(ctx, blocker.m, m, cohortPoll, limit); err != nil {
		_, _, dropped := blocker.m.read()
		t.Fatalf("%v (the blocker dropped %.0f records)", err, dropped)
	}
	parked := time.Since(began)
	wait(t, spec.warmup) // the traffic's backlog from the parking subsides

	cl := tracekeepv1connect.NewMachineServiceClient(trusting(t, filepath.Join(h.d.opts.Dir, DevCertFile)), c.API.PublicURL)
	clock, letGo, err := h.rec.release(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	samples := []loadSample{h.sample(tools)} // the release's
	from := h.mark(tools)
	auditN, auditSum := h.histogram("tracekeep_catalog_audit_wait_seconds")
	cctx, endCalls := context.WithCancel(ctx)
	var calls packURLResult
	called := make(chan struct{})
	go func() {
		defer close(called)
		h.packURLs(cctx, cl, tools[0].id, &calls)
	}()
	letGo()

	var why atomic.Value
	why.Store("no copy acknowledged")
	wctx, endWatch := context.WithCancel(ctx)
	defer endWatch()
	ended := h.rec.watch(wctx, cohortPoll, cohortIdle, &why)
	tick := time.NewTicker(loadTick)
	defer tick.Stop()
	deadline := time.After(limit)
	var parkedRedelivered uint64
	var end drainEnd
	for end.at.IsZero() {
		select {
		case end = <-ended:
			if end.err != nil {
				endCalls()
				t.Fatalf("the drain cannot complete: %v", end.err)
			}
		case <-deadline:
			endCalls()
			t.Fatalf("the drain did not complete within %s: %s", limit, why.Load())
		case <-tick.C:
		}
		samples = append(samples, h.sample(tools)) // the last, the completion's
		if ci, err := h.parked.Info(ctx); err == nil {
			parkedRedelivered = max(parkedRedelivered, uint64(ci.NumRedelivered)) //nolint:gosec // a count
		}
	}
	// The stages and counters at the completion's sample, before an attempt under way ends.
	to := h.mark(tools)
	auditN2, auditSum2 := h.histogram("tracekeep_catalog_audit_wait_seconds")
	endCalls()
	<-called

	st := stageDelta(from, to)
	ep := drainOf(rate, clock, end.at, samples, h.probe.readPasses(), &calls, st)
	h.recoveryRows(ep, m, parked, clock, samples, st, calls.within(clock, end.at))
	var trafficRedelivered uint64
	for _, s := range samples {
		trafficRedelivered = max(trafficRedelivered, s.redelivered)
	}
	mean := 0.0
	if n := auditN2 - auditN; n > 0 {
		mean = (auditSum2 - auditSum) / n
	}
	h.log("", "| audit waits | audit wait mean | traffic max redelivered pending | parked max redelivered pending | redeliveries, both consumers |", "|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %s | %d | %d | %.0f |", auditN2-auditN, time.Duration(mean*float64(time.Second)).Round(time.Microsecond),
			trafficRedelivered, parkedRedelivered, to.redelivered-from.redelivered))

	return ep
}

// drainOf is an episode's drain from the release's clock to its completion at end, read from what was observed within it alone:
// the traffic judged over the samples from the release's to the completion's, the first taken at or after end, and the passes between them,
// the PackURL attempts begun within the drain, and the parked consumer's acknowledgement in st, its stages' totals over the drain.
func drainOf(rate float64, clock, end time.Time, samples []loadSample, passes []settledPass, calls *packURLResult, st map[string]stageTotal) drainEpisode {
	if i := slices.IndexFunc(samples, func(s loadSample) bool { return !s.at.Before(end) }); i >= 0 {
		samples = samples[:i+1]
	}
	ep := drainEpisode{drain: end.Sub(clock), traffic: judgeDrain(rate, samples, passes)}
	in := calls.within(clock, end)
	ep.calls = append(ep.calls, in.latencies...)
	for range in.failed {
		ep.calls = append(ep.calls, failedCall)
	}
	for _, s := range []string{"prepare-ack", "double-ack", "ack-copy"} {
		ep.ack += st["parked/"+s].sum + st["parked/"+s+"/failed"].sum
	}

	return ep
}

// recoveryRows reports an episode: the drain and its marks, the traffic beside it, PackURL, and each consumer's stages within it.
func (h *loadHarness) recoveryRows(ep drainEpisode, m int, parked time.Duration, clock time.Time, samples []loadSample, st map[string]stageTotal, calls packURLResult) {
	after := func(stage string, end bool) string {
		s, ok := h.probe.first(stage)
		if !ok {
			return "none"
		}
		at := s.start
		if end {
			at = s.end
		}

		return at.Sub(clock).Round(time.Millisecond).String()
	}
	v := ep.traffic
	minima := make([]string, len(v.minima))
	for i, mn := range v.minima {
		minima[i] = fmt.Sprintf("%.0f@%.0fs", mn.backlog, mn.at.Sub(samples[0].at).Seconds())
	}
	h.log("", fmt.Sprintf("M = %d: parked by identity %s after the first forward; the release's backlog %d, its ack pending %d; completion is the first cohort read finding it complete, read every %s once every copy's acknowledgement is timed, every %s before", m, parked.Round(time.Second), samples[0].backlog, samples[0].ackPending, cohortPoll, cohortIdle), "",
		"| drain | first copy fetched after | admission committed after | traffic cycles | backlog minima (records@s) | made fwd/s | dropped | backlog slope /s | bound /s | judged | sustained |",
		"|---|---|---|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %s | %s | %s | %d | %s | %.1f | %d | %.2f | %.2f | %v | %v %s |", ep.drain.Round(time.Millisecond), after("parked/copy", false), after("parked/admit", true),
			len(v.minima), strings.Join(minima, " "), v.made, v.dropped, v.backlogSlope, v.bound, v.judged, v.sustained, v.why),
		"", "| PackURL calls | succeeded | failed | superseded | p50 | p99 | acknowledgement | its share of the drain |", "|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %d | %d | %d | %d | %s | %s | %s | %.1f%% |", len(ep.calls), calls.succeeded, calls.failed, calls.superseded,
			percentile(ep.calls, 0.5), percentile(ep.calls, 0.99), ep.ack.Round(time.Millisecond), 100*ep.ack.Seconds()/ep.drain.Seconds()))
	if calls.failed > 0 {
		h.log(fmt.Sprintf("first PackURL failure after %s: %s", calls.took, calls.failure))
	}
	header, sep := "| consumer |", "|---|"
	for _, s := range recoveryStages {
		header, sep = header+" "+s+" s (calls, mean; failed) |", sep+"---|"
	}
	h.log("", header, sep)
	for _, role := range []string{"traffic", "parked"} {
		row := "| " + role + " |"
		for _, s := range recoveryStages {
			name := s
			if role == "parked" {
				name = "parked/" + s
			}
			done, failed := st[name], st[name+"/failed"]
			mean := time.Duration(0)
			if done.n > 0 {
				mean = done.sum / time.Duration(done.n)
			}
			row += fmt.Sprintf(" %.2f (%d, %s; %d) |", (done.sum + failed.sum).Seconds(), done.n, mean, failed.n)
		}
		h.log(row)
	}
}

// histogram sums the count and sum of every series of the histogram family name in dev's registry.
func (h *loadHarness) histogram(name string) (n, sum float64) {
	mfs, err := h.d.Env.Metrics.Registry().Gather()
	if err != nil {
		h.t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				n, sum = n+float64(m.GetHistogram().GetSampleCount()), sum+m.GetHistogram().GetSampleSum()
			}
		}
	}

	return n, sum
}

// park forwards m/2 requests through lt, paced, so its capture publishes m data records, a primary and a reply each.
func park(ctx context.Context, lt *loadTool, m int) error {
	tick := time.NewTicker(blockerEvery)
	defer tick.Stop()
	for range m / 2 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		if err := lt.l.Forward(ctx); err != nil {
			return err
		}
	}

	return nil
}

// wait waits d, the scenario's own delay, or fails once the test's context ends.
func wait(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	case <-time.After(d):
	}
}

// A parked start held behind the release's barrier for a set interval: the parked consumer, held at a step's start once the acceptance is switched,
// admits nothing until it is let go, and the drain the clock reads, from the release to the first catalog read finding the cohort complete,
// holds the interval, the first copy fetched after it and the admission after that.
func TestRecoveryHoldsTheParkedStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		const m, hold, limit = 6, 300 * time.Millisecond, 30 * time.Second
		ctx := t.Context()
		r, probe := parkedCohort(t, c, m, limit, nil)

		clock, letGo, err := r.release(ctx, limit)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(hold) // the scenario's interval: the consumer held past the release
		if got, err := c.Capture(ctx, r.capture); err != nil || got.State != catalog.StateRejected {
			t.Fatalf("capture %+v, %v while the parked consumer is held; want still rejected", got, err)
		}
		letGo()
		var why atomic.Value
		var end drainEnd
		select {
		case end = <-r.watch(ctx, 50*time.Millisecond, 500*time.Millisecond, &why):
		case <-time.After(limit):
			t.Fatalf("no completion within %s: %v", limit, why.Load())
		}
		if end.err != nil {
			t.Fatal(end.err)
		}
		first, ok := probe.first("parked/copy")
		admitted, admittedOK := probe.first("parked/admit")
		if !ok || !admittedOK {
			t.Fatalf("first copy %v, admission %v since the release; want both", ok, admittedOK)
		}
		if first.start.Before(clock.Add(hold)) || admitted.end.Before(first.start) || end.at.Before(admitted.end) {
			t.Errorf("from the release: first copy %s, admission %s, completion %s; want the hold %s, then each after the one before",
				first.start.Sub(clock), admitted.end.Sub(clock), end.at.Sub(clock), hold)
		}
		if got, err := c.Capture(ctx, r.capture); err != nil || got.State != catalog.StateOpen {
			t.Errorf("capture %+v, %v; want admitted", got, err)
		}
	})
}

// parkedCohort runs ingest on a dev Env over c with a fast flush and the recovery's seams, the probe its timing seam,
// then parks a blocker's m data records and waits at most limit for the barrier.
// also, when set, is told every stage the probe is, on the ingester's goroutine.
func parkedCohort(t *testing.T, c *catalog.Catalog, m int, limit time.Duration, also func(stage string)) (*recovery, *loadProbe) {
	t.Helper()
	ctx := t.Context()
	e := env(t, c)
	e.Config.Ingest.FlushInterval, e.Config.Ingest.MaxWait, e.Config.Parking.RedeliveryDelay = 200*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond
	probe := &loadProbe{stages: map[string]*stageTotal{}}
	e.timed = func(stage string, took time.Duration) {
		probe.timed(stage, took)
		if also != nil {
			also(stage)
		}
	}
	r := newRecovery(e, probe)
	joined := make(chan struct{})
	e.joined = func() { close(joined) }
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- Ingest(run, e) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the run's end, its result not checked here
	})
	select {
	case <-joined:
	case err := <-done:
		t.Fatalf("ingest ended before it joined: %v", err)
	}
	var blocker *loadTool
	err := startTool(ctx, e, &blocker, blockerTool, " ")
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), simStop)
		defer cancel()
		if blocker != nil {
			blocker.l.Stop(sctx)
			_ = blocker.p.Close()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := park(ctx, blocker, m); err != nil {
		t.Fatal(err)
	}
	if err := r.barrier(ctx, blocker.m, m, 50*time.Millisecond, limit); err != nil {
		t.Fatal(err)
	}

	return r, probe
}

// lostBookkeeping fails the bookkeeping of the first parked copy the broker confirms once armed:
// the copy rows' table renamed away at its double acknowledgement and back at its AckCopy's failure, both on the ingester's goroutine.
// failed closes at that failure; others once want other copies are acknowledged.
type lostBookkeeping struct {
	t              *testing.T
	c              *catalog.Catalog
	armed, broken  atomic.Bool
	acked, want    atomic.Int64
	failed, others chan struct{}
}

// stage is the probe's also.
func (f *lostBookkeeping) stage(stage string) {
	switch {
	case stage == "parked/double-ack" && f.armed.CompareAndSwap(true, false):
		// The broker confirmed the copy: its AckCopy, next on this goroutine, finds no table.
		f.exec("ALTER TABLE parked_copies RENAME TO parked_copies_gone")
		f.broken.Store(true)
	case stage == "parked/ack-copy/failed" && f.broken.CompareAndSwap(true, false):
		f.exec("ALTER TABLE parked_copies_gone RENAME TO parked_copies")
		close(f.failed)
	case stage == "parked/ack-copy" && f.acked.Add(1) == f.want.Load():
		close(f.others)
	default:
	}
}

// exec runs stmt on the catalog's writer pool, which waits its turn with the ingesters' writes on SQLite's one write connection.
func (f *lostBookkeeping) exec(stmt string) {
	w, _ := f.c.Pools()
	if _, err := w.ExecContext(context.WithoutCancel(f.t.Context()), stmt); err != nil {
		f.t.Errorf("%s: %v", stmt, err) // off the test's goroutine
	}
}

// A copy the broker confirmed whose bookkeeping failed completes the drain once the reconciler marks its row acked, without a redelivery:
// no acknowledgement is timed for it again, so the catalog's rows read by identity end the drain, not a count of acknowledgements,
// and the completion is the first read finding the cohort complete, after the reconciliation; before it, the waiting row holds the drain open.
func TestRecoveryCompletesThroughReconciliation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		const m, limit = 6, 30 * time.Second
		ctx := t.Context()
		f := &lostBookkeeping{t: t, c: c, failed: make(chan struct{}), others: make(chan struct{})}
		r, _ := parkedCohort(t, c, m, limit, f.stage)
		f.want.Store(int64(r.n - 1))
		_, letGo, err := r.release(ctx, limit)
		if err != nil {
			t.Fatal(err)
		}
		f.armed.Store(true)
		letGo()
		for _, ch := range []chan struct{}{f.failed, f.others} {
			select {
			case <-ch:
			case <-time.After(limit):
				t.Fatalf("within %s: a confirmed copy's bookkeeping failed and restored %v, %d of the other %d copies acknowledged", limit, !f.broken.Load(), f.acked.Load(), r.n-1)
			}
		}
		var why atomic.Value
		ended := r.watch(ctx, 50*time.Millisecond, 500*time.Millisecond, &why)
		if w, err := r.drained(ctx); err != nil || !strings.Contains(w, "waiting") {
			t.Fatalf("before the reconciliation the cohort reads %q, %v; want a copy's row waiting", w, err)
		}
		lease, ok, err := c.TakeLease(ctx, "compactor/2", "test", time.Hour)
		if err != nil || !ok {
			t.Fatalf("TakeLease = %v, %v", ok, err)
		}
		select {
		case end := <-ended:
			t.Fatalf("completion %+v before the reconciliation", end)
		default:
		}
		rc := &compact.Compactor{Catalog: c, Log: slog.New(slog.DiscardHandler)}
		if n, err := rc.Copies(ctx, lease, catalog.RoleParking, r.parking); err != nil || n != 1 {
			t.Fatalf("Copies = %d, %v; want the confirmed copy's row marked", n, err)
		}
		reconciled := time.Now()
		select {
		case end := <-ended:
			if end.err != nil || end.at.Before(reconciled) {
				t.Errorf("completion %s after the reconciliation, %v; want it read after it", end.at.Sub(reconciled), end.err)
			}
		case <-time.After(limit):
			t.Fatalf("no completion within %s of the reconciliation: %v", limit, why.Load())
		}
	})
}
