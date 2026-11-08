package service

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	// loadTick is the harness's sampling period; a gap above two of them leaves a measurement unjudged.
	loadTick = time.Second
	// gateCalls and gateP99 are the gate's PackURL bar: this many audited successes, answered within gateP99 at the 99th percentile.
	gateCalls = 1000
	gateP99   = 2 * time.Second
)

// loadSample is one reading of the load harness, taken at at: counters since the harness started, and gauges.
type loadSample struct {
	at time.Time
	// made are the forwards the harness made; published, the traffic stream's last sequence; dropped, the producers' records_dropped_total.
	made, published, dropped uint64
	// backlog is the traffic stream's messages; ring and reserved, the producers' ordinary rings' and reserved lanes' depths summed.
	backlog        uint64
	ring, reserved float64
	// ackPending and redelivered are the traffic consumer's messages delivered and unacknowledged, and of those the redelivered ones,
	// read only when the harness is probed.
	ackPending, redelivered uint64
}

// loadVerdict is a sweep point judged.
type loadVerdict struct {
	// offered, made and published are forwards, forwards and records a second over the judged span, by its elapsed time.
	offered, made, published float64
	// dropped are the records the producers dropped since the point's origin.
	dropped uint64
	// backlogMinima are the windows' backlog minima; the slopes are a window's least-squares rise, bound their limit.
	backlogMinima                          []float64
	backlogSlope, ringSlope, reservedSlope float64
	bound                                  float64
	sustained                              bool
	// why names the first rule a point that is not sustained breaks.
	why string
}

// packURLResult is the PackURL measurement's record.
type packURLResult struct {
	ingest loadVerdict
	// flushes are the ingester's flushes the calls spanned, counted from its segments.
	flushes int
	// made are the attempts; succeeded, superseded and failed divide them.
	made, succeeded, superseded, failed int
	// latencies are the succeeded attempts' RPC times.
	latencies []time.Duration
	// failure is the first failed attempt's error, took its duration.
	failure string
	took    time.Duration
	// attempts are every attempt recorded, in order.
	attempts []packAttempt
}

// packAttempt is one PackURL attempt or the listing before it: when it began, its RPC time, and how it ended, a failure by its why.
type packAttempt struct {
	began               time.Time
	took                time.Duration
	listing, superseded bool
	why                 string
}

// judgeLoad judges a sweep point: offered forwards a second, the sample at its origin (before the warm-up),
// and its samples from the first window's start, cut into windows of a fixed duration by their timestamps;
// a trailing part shorter than a window is left out of the windows, its drops counted.
// ended is the last measured attempt's end, zero for none: the judged windows must hold a sample at or after it,
// or the point is unjudged; the span is never stretched past its whole windows to reach it.
func judgeLoad(offered float64, origin loadSample, samples []loadSample, window time.Duration, ended time.Time) loadVerdict {
	t0 := samples[0].at
	windows := int(samples[len(samples)-1].at.Sub(t0) / window)
	end := t0.Add(time.Duration(windows) * window)
	judged := samples
	for len(judged) > 1 && judged[len(judged)-1].at.After(end) {
		judged = judged[:len(judged)-1]
	}
	v := loadVerdict{offered: offered, dropped: samples[len(samples)-1].dropped - origin.dropped}
	if v.why = sampling(samples, judged, windows, ended); v.why != "" {
		return v
	}
	first, last := judged[0], judged[len(judged)-1]
	secs := last.at.Sub(first.at).Seconds()
	v.made, v.published = float64(last.made-first.made)/secs, float64(last.published-first.published)/secs
	ring, reserved := make([]float64, windows), make([]float64, windows)
	for w := range windows {
		from, to := t0.Add(time.Duration(w)*window), t0.Add(time.Duration(w+1)*window)
		lo := math.Inf(1)
		for _, s := range judged {
			if !s.at.Before(from) && !s.at.After(to) {
				lo = min(lo, float64(s.backlog))
				ring[w], reserved[w] = max(ring[w], s.ring), max(reserved[w], s.reserved)
			}
		}
		v.backlogMinima = append(v.backlogMinima, lo)
	}
	v.backlogSlope, v.ringSlope, v.reservedSlope = slope(v.backlogMinima), slope(ring), slope(reserved)
	// 0.5% of a window's mean published records: a rise of 1% a window fails.
	v.bound = 0.005 * v.published * window.Seconds()
	switch {
	case math.Abs(v.made-offered) > 0.02*offered:
		v.why = fmt.Sprintf("made %.1f/s against %.1f/s offered", v.made, offered)
	case v.dropped > 0:
		v.why = fmt.Sprintf("%d records dropped", v.dropped)
	case v.backlogSlope >= v.bound:
		v.why = fmt.Sprintf("backlog minima rise %.2f a window, the bound %.2f", v.backlogSlope, v.bound)
	case v.ringSlope >= v.bound:
		v.why = fmt.Sprintf("ring maxima rise %.2f a window, the bound %.2f", v.ringSlope, v.bound)
	case v.reservedSlope >= v.bound:
		v.why = fmt.Sprintf("reserved lane maxima rise %.2f a window, the bound %.2f", v.reservedSlope, v.bound)
	default:
		v.sustained = true
	}

	return v
}

// sampling names why samples cannot be judged: a gap above two sampling periods among all of them, which would also leave a window short of samples,
// no whole window in judged, or judged ending before ended, the last attempt's end; empty when they can.
func sampling(samples, judged []loadSample, windows int, ended time.Time) string {
	for i := 1; i < len(samples); i++ {
		if gap := samples[i].at.Sub(samples[i-1].at); gap > 2*loadTick {
			return fmt.Sprintf("sampling: a gap of %s after %s", gap, samples[i-1].at.Sub(samples[0].at))
		}
	}
	if windows < 1 {
		return "sampling: shorter than one window"
	}
	if last := judged[len(judged)-1].at; last.Before(ended) {
		return fmt.Sprintf("sampling: the last attempt ended %s after the judged span's last sample", ended.Sub(last))
	}

	return ""
}

// collectFor is how long a measurement samples from its first sample: at least least windows,
// and whole windows reaching two sampling periods past the last attempt's end, so that, with no gap above two periods,
// a sample lies between that end and the last whole window's end.
func collectFor(least int, window, callsEnded time.Duration) time.Duration {
	n := max(least, int(math.Ceil(float64(callsEnded+2*loadTick)/float64(window))))

	return time.Duration(n) * window
}

// slope is the least-squares slope of ys over their indexes.
func slope(ys []float64) float64 {
	xs := make([]float64, len(ys))
	for i := range xs {
		xs[i] = float64(i)
	}

	return fit(xs, ys)
}

// fit is the least-squares slope of ys over xs.
func fit(xs, ys []float64) float64 {
	n := float64(len(ys))
	if n < 2 {
		return 0
	}
	var sx, sy, sxx, sxy float64
	for i, y := range ys {
		x := xs[i]
		sx, sy, sxx, sxy = sx+x, sy+y, sxx+x*x, sxy+x*y
	}

	return (n*sxy - sx*sy) / (n*sxx - sx*sx)
}

// countSlope is the steepest least-squares rise that an error of a record in each value read at xs can make:
// a cycle's minimum is a count of records reconstructed to about one, and its maxima are counts too.
func countSlope(xs []float64) float64 {
	var mean float64
	for _, x := range xs {
		mean += x / float64(len(xs))
	}
	signs := make([]float64, len(xs))
	for i, x := range xs {
		signs[i] = float64(cmp.Compare(x, mean))
	}

	return fit(xs, signs)
}

// settledPass is a flush pass's completion, the ingester's PassSettled signal: when, and whether a batch of it failed.
type settledPass struct {
	at     time.Time
	failed bool
}

// cycleVerdict is a production-timing point judged over completed drain cycles.
type cycleVerdict struct {
	// offered, made and published are forwards, forwards and records a second from the first cycle's start to the last's end.
	offered, made, published float64
	dropped                  uint64
	// minima are the observed cycles' backlog minima, each at its flush's settling.
	minima []cycleMin
	// The slopes are records a second of rise through the cycles' backlog minima and queue maxima against elapsed time, bound their limit.
	backlogSlope, ringSlope, reservedSlope float64
	bound                                  float64
	// judged reports a verdict reached; one not judged, its sampling or its cycles too few, is neither sustained nor not.
	judged, sustained bool
	why               string
}

// cycleMin is a cycle's backlog minimum and when it was.
type cycleMin struct {
	at      time.Time
	backlog float64
}

// judgeCycles judges a point over completed drain cycles: a cycle runs from one flush's settled passes to the next's,
// so the samples before the first pass and after the last, an incomplete cycle each, are left out.
// A flush can settle in more than one pass, a few captures' batches opened a pass after the rest's and due that much later,
// so passes within half the flush interval of a flush's first are that flush's, and its last ends the cycle.
// A cycle's observation is its backlog minimum, the backlog when the flush that began it settled, and its queues' maxima.
// The minimum is read from the cycle's first sample, not taken as its least, which lags the settling by up to a sampling period
// and holds the records published meanwhile: at a flush of seconds, enough to swing the slope past the bound by itself.
// A cycle no sample fell in, or whose first sample follows the next flush's first pass, a pass within a sampling period of the flush's end,
// has none and is not counted.
// It is sustained as judgeLoad's windows are, the drift allowance a rate: made within 2% of offered, nothing dropped, no pass failed,
// and the least-squares rise of the minima, and of the queue maxima, against elapsed time each below 0.5% of the published rate a second,
// which is judgeLoad's 0.5% of a window's records a window, whatever the window, plus countSlope, a record's error in each:
// at a smoke's tens of records a second, a record over a few short cycles is past 0.5%.
// Fewer than least observed cycles leave it unjudged.
func judgeCycles(offered float64, origin loadSample, samples []loadSample, passes []settledPass, flush time.Duration, least int) cycleVerdict {
	return judgeSpan(offered, origin, samples, passes, flush, least, false)
}

// judgeSpan is judgeCycles, its rates read from the first and the last sample when fromEnds is set, from the cycles' own otherwise.
func judgeSpan(offered float64, origin loadSample, samples []loadSample, passes []settledPass, flush time.Duration, least int, fromEnds bool) cycleVerdict {
	v := cycleVerdict{offered: offered, dropped: samples[len(samples)-1].dropped - origin.dropped}
	if v.why = sampling(samples, samples, 1, time.Time{}); v.why != "" {
		return v
	}
	failed := 0
	for _, p := range passes {
		if p.failed { // a failure anywhere in the run, its warm-up included
			failed++
		}
	}
	ends := flushEnds(passes, samples[0].at, samples[len(samples)-1].at, flush)
	var xs, mins, ring, reserved []float64
	var first, last *loadSample
	for i := 1; i < len(ends); i++ {
		observed := false
		var r, q float64
		for j := range samples {
			s := &samples[j]
			if s.at.Before(ends[i-1]) || s.at.After(ends[i]) {
				continue
			}
			if first == nil {
				first = s
			}
			last, observed = s, true
			r, q = max(r, s.ring), max(q, s.reserved)
		}
		lo, settled := trough(samples, passes, ends[i-1])
		if !observed || !settled {
			continue
		}
		v.minima = append(v.minima, cycleMin{at: ends[i-1], backlog: lo})
		xs = append(xs, ends[i-1].Sub(samples[0].at).Seconds())
		mins, ring, reserved = append(mins, lo), append(ring, r), append(reserved, q)
	}
	if len(v.minima) < least {
		v.why = fmt.Sprintf("cycles: %d completed, want at least %d", len(v.minima), least)
		return v
	}
	if fromEnds {
		first, last = &samples[0], &samples[len(samples)-1]
	}
	secs := last.at.Sub(first.at).Seconds()
	v.made, v.published = float64(last.made-first.made)/secs, float64(last.published-first.published)/secs
	v.backlogSlope, v.ringSlope, v.reservedSlope = fit(xs, mins), fit(xs, ring), fit(xs, reserved)
	v.bound = 0.005*v.published + countSlope(xs)
	v.judged = true
	switch {
	case math.Abs(v.made-offered) > 0.02*offered:
		v.why = fmt.Sprintf("made %.1f/s against %.1f/s offered", v.made, offered)
	case v.dropped > 0:
		v.why = fmt.Sprintf("%d records dropped", v.dropped)
	case failed > 0:
		v.why = fmt.Sprintf("%d flush passes failed", failed)
	case v.backlogSlope >= v.bound:
		v.why = fmt.Sprintf("backlog minima rise %.2f/s, the bound %.2f/s", v.backlogSlope, v.bound)
	case v.ringSlope >= v.bound:
		v.why = fmt.Sprintf("ring maxima rise %.2f/s, the bound %.2f/s", v.ringSlope, v.bound)
	case v.reservedSlope >= v.bound:
		v.why = fmt.Sprintf("reserved lane maxima rise %.2f/s, the bound %.2f/s", v.reservedSlope, v.bound)
	default:
		v.sustained = true
	}

	return v
}

// trough is the backlog when a flush settled at end, a sample at or after it:
// acknowledgements come only in passes, so the first sample from end holds the flush's acknowledged records,
// and the records published by end are interpolated between it and the sample before, the traffic steady within a sampling period.
// A pass after end and by that sample, the next flush's, has acknowledged more, so the trough is not read.
func trough(samples []loadSample, passes []settledPass, end time.Time) (float64, bool) {
	j := slices.IndexFunc(samples, func(s loadSample) bool { return !s.at.Before(end) })
	s := samples[j]
	if slices.ContainsFunc(passes, func(p settledPass) bool { return p.at.After(end) && !p.at.After(s.at) }) {
		return 0, false
	}
	published := float64(s.published)
	if j > 0 && s.at.After(end) {
		a := samples[j-1]
		published = float64(a.published) + float64(s.published-a.published)*end.Sub(a.at).Seconds()/s.at.Sub(a.at).Seconds()
	}

	return published - (float64(s.published) - float64(s.backlog)), true
}

// flushEnds are the times the flushes settled from one sample to the other: a flush's last pass,
// passes within half the flush interval of a flush's first being that flush's.
// A flush whose half interval outlasts the samples may settle a later pass after them, so it ends no cycle.
func flushEnds(passes []settledPass, from, to time.Time, flush time.Duration) []time.Time {
	var ends []time.Time
	var first time.Time
	for _, p := range passes {
		switch {
		case p.at.Before(from) || p.at.After(to):
		case len(ends) > 0 && p.at.Sub(first) < flush/2:
			ends[len(ends)-1] = p.at
		default:
			ends, first = append(ends, p.at), p.at
		}
	}
	if len(ends) > 0 && to.Sub(first) < flush/2 {
		ends = ends[:len(ends)-1]
	}

	return ends
}

// accept passes a PackURL measurement as a valid record or names why it is not one:
// ingest sustained over the calls, ten flushes spanned, no failure, and at most 5% of the attempts superseded.
func (r packURLResult) accept() error {
	switch {
	case !r.ingest.sustained:
		return fmt.Errorf("ingest not sustained during the calls: %s", r.ingest.why)
	case r.flushes < 10:
		return fmt.Errorf("the calls spanned %d flushes, want at least 10", r.flushes)
	case r.failed > 0:
		return fmt.Errorf("%d of %d attempts failed, the first after %s: %s", r.failed, r.made, r.took, r.failure)
	case r.made == 0:
		return errors.New("no call made")
	case r.superseded*20 > r.made:
		return fmt.Errorf("%d of %d attempts superseded, more than 5%%", r.superseded, r.made)
	default:
		return nil
	}
}

// gate passes a PackURL measurement that meets the write-connection gate, or names why it does not:
// a valid record of at least 1,000 audited successes answered within 2 s at the 99th percentile.
func (r packURLResult) gate() error {
	if err := r.accept(); err != nil {
		return err
	}
	if r.succeeded < gateCalls {
		return fmt.Errorf("%d audited successes, want at least %d", r.succeeded, gateCalls)
	}
	if p99 := percentile(r.latencies, 0.99); p99 > gateP99 {
		return fmt.Errorf("p99 %s, above %s", p99, gateP99)
	}

	return nil
}

// percentile is the nearest-rank q-quantile of d, 0 for none.
func percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)

	return s[max(0, int(math.Ceil(q*float64(len(s))))-1)]
}

// synthetic builds a sweep point sampled every step over span from origin, after a minute's warm-up:
// made forwards a second of real time, two records published for each, and set filling each sample's gauges.
func synthetic(origin loadSample, span, step time.Duration, made float64, set func(i int, s *loadSample)) []loadSample {
	const warm = 60 * time.Second
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var out []loadSample
	for i := 0; time.Duration(i)*step <= span; i++ {
		at := time.Duration(i) * step
		n := uint64(math.Round((warm + at).Seconds() * made))
		s := loadSample{at: t0.Add(at), made: origin.made + n, published: origin.published + 2*n, dropped: origin.dropped}
		if set != nil {
			set(i, &s)
		}
		out = append(out, s)
	}

	return out
}

// The sustained rule on recorded series: made within 2% of offered, nothing dropped,
// and the windows' backlog minima and queue maxima each rising less than 0.5% of a window's records a window.
func TestJudgeLoad(t *testing.T) {
	t.Parallel()
	const windows, rate, window = 12, 100.0, 10 * time.Second // 2,000 records a window: the bound is 10 a window
	for name, tc := range map[string]struct {
		made      float64
		origin    loadSample
		set       func(i int, s *loadSample)
		sustained bool
		why       string
	}{
		"bounded sawtooth": {rate, loadSample{}, func(i int, s *loadSample) {
			s.backlog, s.ring, s.reserved = uint64(50+30*(i%5)), float64(20*(i%4)), float64(i%2)
		}, true, ""},
		// A period that does not divide the window: every window still holds a trough.
		"phase-shifted sawtooth": {rate, loadSample{}, func(i int, s *loadSample) {
			s.backlog, s.ring = uint64(200*((i+3)%7)), float64(10*((i+5)%7))
		}, true, ""},
		"rising": {rate, loadSample{}, func(i int, s *loadSample) { s.backlog = uint64(50 * i) }, false, "backlog"},
		// 1% of 2,000 records a window, a window: twice the bound.
		"rising 1% a window": {rate, loadSample{}, func(i int, s *loadSample) { s.backlog = uint64(2 * i) }, false, "backlog"},
		// A quarter of a percent a window stays below it.
		"rising 0.25% a window":             {rate, loadSample{}, func(i int, s *loadSample) { s.backlog = uint64(i / 2) }, true, ""},
		"made 3% short of the offered rate": {0.97 * rate, loadSample{}, nil, false, "made"},
		"ring growing while reserved records publish": {rate, loadSample{}, func(i int, s *loadSample) {
			s.ring, s.reserved = float64(3*i), float64(100-i/2)
		}, false, "ring"},
		"a drop since the origin": {rate, loadSample{dropped: 500}, func(i int, s *loadSample) {
			if i == windows*10 {
				s.dropped++
			}
		}, false, "dropped"},
	} {
		v := judgeLoad(rate, tc.origin, synthetic(tc.origin, windows*window, time.Second, tc.made, tc.set), window, time.Time{})
		if v.sustained != tc.sustained || !strings.Contains(v.why, tc.why) {
			t.Errorf("%s: sustained %v (%s), want %v naming %q; %+v", name, v.sustained, v.why, tc.sustained, tc.why, v)
		}
	}
}

// Two points whose producers' counters start from different origins are each judged on their own deltas.
func TestJudgeLoadOrigins(t *testing.T) {
	t.Parallel()
	const windows, rate, window = 12, 100.0, 10 * time.Second
	for _, origin := range []loadSample{{}, {made: 1_000_000, published: 3_000_000, dropped: 500}} {
		v := judgeLoad(rate, origin, synthetic(origin, windows*window, time.Second, rate, nil), window, time.Time{})
		if !v.sustained || v.made != rate || v.published != 2*rate || v.dropped != 0 || len(v.backlogMinima) != windows {
			t.Errorf("origin %+v: %+v, want sustained at %v made, %v published, none dropped, %d windows", origin, v, rate, 2*rate, windows)
		}
	}
}

// Rates come from the samples' elapsed time, not their count: samples taken late, each 1.1 s apart, still read the offered rate;
// a gap above two sampling periods leaves the point unjudged.
func TestJudgeLoadDelayedSamples(t *testing.T) {
	t.Parallel()
	const rate, window = 100.0, 10 * time.Second
	late := synthetic(loadSample{}, 120*time.Second, 1100*time.Millisecond, rate, nil)
	if v := judgeLoad(rate, loadSample{}, late, window, time.Time{}); !v.sustained || math.Abs(v.made-rate) > 1 || math.Abs(v.published-2*rate) > 2 || len(v.backlogMinima) != 11 {
		t.Errorf("late samples: %+v, want sustained at %v made over 11 whole windows", v, rate)
	}
	gapped := synthetic(loadSample{}, 120*time.Second, time.Second, rate, nil)
	gapped = slices.Delete(gapped, 50, 53) // four seconds between two samples
	if v := judgeLoad(rate, loadSample{}, gapped, window, time.Time{}); v.sustained || !strings.Contains(v.why, "sampling") {
		t.Errorf("a gap of four periods: %+v, want unjudged naming sampling", v)
	}
	if v := judgeLoad(rate, loadSample{}, synthetic(loadSample{}, 5*time.Second, time.Second, rate, nil), window, time.Time{}); v.sustained || !strings.Contains(v.why, "sampling") {
		t.Errorf("less than a window: %+v, want unjudged naming sampling", v)
	}
}

// A call still running at the last of the least windows extends the measurement by whole windows:
// two sampling periods past the last attempt's end, rounded up to a whole window:
// with no gap above two periods, a sample then lies between that end and the last whole window's.
func TestCollectFor(t *testing.T) {
	t.Parallel()
	const window = 10 * time.Second
	for _, tc := range []struct {
		ended, want time.Duration
	}{
		{0, 120 * time.Second}, {100 * time.Second, 120 * time.Second}, {118 * time.Second, 120 * time.Second},
		{118*time.Second + time.Millisecond, 130 * time.Second}, {125 * time.Second, 130 * time.Second},
		{129500 * time.Millisecond, 140 * time.Second}, {130 * time.Second, 140 * time.Second},
	} {
		if got := collectFor(12, window, tc.ended); got != tc.want {
			t.Errorf("calls ending at %s: sample for %s, want %s", tc.ended, got, tc.want)
		}
	}
}

// The judged span covers the last attempt: a sample at or after its end lies in the whole windows judged, or the point is unjudged.
// Gaps are checked over every sample collected, before the trailing part shorter than a window is left out.
func TestJudgeLoadCoversTheLastCall(t *testing.T) {
	t.Parallel()
	const rate, window = 100.0, 10 * time.Second
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := t0.Add
	through := func(span time.Duration) []loadSample { return synthetic(loadSample{}, span, time.Second, rate, nil) }
	for name, tc := range map[string]struct {
		samples   []loadSample
		ended     time.Time
		windows   int
		sustained bool
		why       string
	}{
		// Samples through 129 s, the call ending at 129.5 s, and a final sample delayed to 135 s: the 6 s gap is seen.
		"delayed final sample": {append(through(129*time.Second), synthetic(loadSample{}, 135*time.Second, 135*time.Second, rate, nil)[1]), at(129500 * time.Millisecond), 0, false, "gap"},
		// Through 129 s with no later sample: the whole windows end at 120 s, before the call did.
		"call ending past the whole windows": {through(129 * time.Second), at(129500 * time.Millisecond), 0, false, "last attempt"},
		// Collected as collectFor asks, to 140 s: 14 windows judged, the call inside them.
		"call ending off a boundary":      {through(140 * time.Second), at(129500 * time.Millisecond), 14, true, ""},
		"call ending on a boundary":       {through(130 * time.Second), at(130 * time.Second), 13, true, ""},
		"no call, the least windows":      {through(120 * time.Second), time.Time{}, 12, true, ""},
		"call ending at the first sample": {through(120 * time.Second), t0, 12, true, ""},
	} {
		v := judgeLoad(rate, loadSample{}, tc.samples, window, tc.ended)
		if v.sustained != tc.sustained || !strings.Contains(v.why, tc.why) || len(v.backlogMinima) != tc.windows {
			t.Errorf("%s: sustained %v (%s) over %d windows, want %v naming %q over %d", name, v.sustained, v.why, len(v.backlogMinima), tc.sustained, tc.why, tc.windows)
		}
	}
}

// A PackURL measurement is a valid record only over sustained ingest, across ten flushes, with no failure and at most 5% of its attempts superseded.
func TestPackURLAcceptance(t *testing.T) {
	t.Parallel()
	ok := packURLResult{ingest: loadVerdict{sustained: true}, flushes: 12, made: 1000, succeeded: 1000, latencies: fast(1000)}
	for name, tc := range map[string]struct {
		edit func(r *packURLResult)
		want string
	}{
		"passing":              {func(*packURLResult) {}, ""},
		"5% superseded":        {func(r *packURLResult) { r.succeeded, r.superseded = 950, 50 }, ""},
		"ingest not sustained": {func(r *packURLResult) { r.ingest = loadVerdict{why: "backlog"} }, "ingest"},
		"nine flushes":         {func(r *packURLResult) { r.flushes = 9 }, "flushes"},
		"one deadline": {func(r *packURLResult) {
			r.made, r.failed, r.failure, r.took = 1001, 1, "deadline_exceeded", 10*time.Second
		}, "deadline_exceeded"},
		"more than 5% superseded": {func(r *packURLResult) { r.succeeded, r.superseded = 949, 51 }, "superseded"},
		"no call":                 {func(r *packURLResult) { r.made, r.succeeded, r.latencies = 0, 0, nil }, "no call"},
	} {
		r := ok
		tc.edit(&r)
		err := r.accept()
		if (tc.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("%s: accept = %v, want %q", name, err, tc.want)
		}
	}
}

// The gate asks more of a valid record: 1,000 audited successes, answered within 2 s at the 99th percentile.
func TestPackURLGate(t *testing.T) {
	t.Parallel()
	slow := fast(1000)
	for i := range 20 { // 2% of the calls take 3 s: the 99th percentile is one of them
		slow[i] = 3 * time.Second
	}
	within := fast(1000)
	for i := range 10 { // 1% at 3 s: the 99th percentile stays fast
		within[i] = 3 * time.Second
	}
	for name, tc := range map[string]struct {
		r    packURLResult
		want string
	}{
		"950 successes and 50 superseded": {packURLResult{made: 1000, succeeded: 950, superseded: 50, latencies: fast(950)}, "audited successes"},
		"1,000 successes, p99 above 2 s":  {packURLResult{made: 1000, succeeded: 1000, latencies: slow}, "p99"},
		"1,000 successes within 2 s":      {packURLResult{made: 1000, succeeded: 1000, latencies: within}, ""},
		"invalid record":                  {packURLResult{made: 1000, succeeded: 1000, latencies: within, flushes: 9}, "flushes"},
	} {
		tc.r.ingest.sustained = true
		if tc.r.flushes == 0 {
			tc.r.flushes = 12
		}
		err := tc.r.gate()
		if (tc.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("%s: gate = %v, want %q", name, err, tc.want)
		}
	}
}

// fast is n latencies of 5 ms.
func fast(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = 5 * time.Millisecond
	}

	return out
}

// Latencies are reported by nearest rank.
func TestPercentile(t *testing.T) {
	t.Parallel()
	d := make([]time.Duration, 100)
	for i := range d {
		d[len(d)-1-i] = time.Duration(i+1) * time.Millisecond
	}
	if p50, p99 := percentile(d, 0.50), percentile(d, 0.99); p50 != 50*time.Millisecond || p99 != 99*time.Millisecond {
		t.Errorf("p50 %s, p99 %s; want 50ms and 99ms", p50, p99)
	}
	if p := percentile(nil, 0.5); p != 0 {
		t.Errorf("percentile of none = %s, want 0", p)
	}
}

// drainRun is a synthetic production-timing schedule: rate records a second over captures staggered evenly across one flush interval,
// one ingester taking every due batch in a pass, the pass completing upload(k) after it began, so a cycle lasts the flush plus its flush work.
// A capture's batch opens when the pass that took its last one completes, due a flush later, and holds the records published since that pass began;
// a pass drains them but for leak records a second since the previous pass, which stay unacknowledged and raise the floor after every drain.
type drainRun struct {
	rate     float64
	captures int
	flush    time.Duration
	upload   func(k int) time.Duration
	leak     float64
	// every is the sampling period, a second when zero.
	every time.Duration
}

// run returns the samples, every period over span from start, and every pass completed by then.
func (r drainRun) run(start, span time.Duration) ([]loadSample, []settledPass) {
	type capture struct{ opened, taken time.Duration }
	type drain struct {
		at time.Duration
		n  float64
	}
	cs := make([]capture, r.captures)
	for i := range cs {
		cs[i].opened = time.Duration(i) * r.flush / time.Duration(r.captures)
	}
	var drains []drain
	var free time.Duration // when the last pass completed
	for k := 0; ; k++ {
		due := cs[0].opened
		for _, c := range cs {
			due = min(due, c.opened)
		}
		begin := max(due+r.flush, free)
		if begin > start+span {
			break
		}
		end := begin + r.upload(k)
		n := -r.leak * (end - free).Seconds()
		for i, c := range cs {
			if c.opened+r.flush <= begin {
				n += r.rate / float64(r.captures) * (begin - c.taken).Seconds()
				cs[i] = capture{opened: end, taken: begin}
			}
		}
		drains, free = append(drains, drain{at: end, n: n}), end
	}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	passes := make([]settledPass, 0, len(drains))
	for _, d := range drains {
		passes = append(passes, settledPass{at: t0.Add(d.at)})
	}
	every := cmp.Or(r.every, time.Second)
	var samples []loadSample
	for at := start; at <= start+span; at += every {
		published, acked := r.rate*at.Seconds(), 0.0
		for _, d := range drains {
			if d.at <= at {
				acked += d.n
			}
		}
		samples = append(samples, loadSample{at: t0.Add(at), made: uint64(published / 2), published: uint64(published), backlog: uint64(published - acked)})
	}

	return samples, passes
}

// cycled is the uploads us in turn.
func cycled(us ...time.Duration) func(int) time.Duration {
	return func(k int) time.Duration { return us[k%len(us)] }
}

// Completed drain cycles judge the production-timing run: bounded cycles of a flush plus its upload pass,
// with variable flush delays, staggered captures and several starting phases;
// the same schedules with a floor rising after each drain at four times the allowance fail.
func TestJudgeCycles(t *testing.T) {
	t.Parallel()
	const rate, flush, ackWait = 1600.0, 5 * time.Minute, 7 * time.Minute
	for name, r := range map[string]drainRun{
		"one capture, a steady upload": {captures: 1, upload: cycled(10 * time.Second)},
		"variable flush delays":        {captures: 1, upload: cycled(8*time.Second, 11500*time.Millisecond, 9*time.Second, 12*time.Second, 9500*time.Millisecond)},
		"staggered captures":           {captures: 7, upload: cycled(2*time.Second, 3500*time.Millisecond, 2500*time.Millisecond)},
		// Captures closer than a pass: a pass takes several batches, the others wait for it.
		"staggered closer than a pass": {captures: 100, upload: cycled(4*time.Second, 6*time.Second, 5*time.Second)},
	} {
		r.rate, r.flush = rate, flush
		for _, phase := range []time.Duration{0, 37 * time.Second, 151 * time.Second, 263 * time.Second} {
			// The floor's rise as a share of the allowance, 0.5% of the published rate a second: below it sustained, four times it not.
			for _, rise := range []float64{0, 0.5, 4} {
				r.leak = rise * 0.005 * rate
				samples, passes := r.run(flush+ackWait+phase, 40*time.Minute)
				v := judgeCycles(rate/2, loadSample{}, samples, passes, r.flush, 6)
				if v.sustained != (rise < 1) || (rise > 1 && !strings.Contains(v.why, "backlog minima rise")) {
					t.Errorf("%s from %s, rising %v of the allowance: sustained %v (%s) over %d cycles, slope %.2f/s, the bound %.2f/s",
						name, phase, rise, v.sustained, v.why, len(v.minima), v.backlogSlope, v.bound)
				}
			}
		}
	}
}

// The cycles before the first pass and after the last are incomplete and left out; fewer than the least cycles leave the run unjudged;
// a made rate short of the offered one, and a failed pass, its warm-up's included, fail it.
func TestJudgeCyclesBounds(t *testing.T) {
	t.Parallel()
	const rate = 1600.0
	r := drainRun{rate: rate, captures: 1, flush: 5 * time.Minute, upload: cycled(10 * time.Second)}
	samples, passes := r.run(12*time.Minute, 40*time.Minute)
	whole := judgeCycles(rate/2, loadSample{}, samples, passes, r.flush, 6)
	if !whole.sustained {
		t.Fatalf("the steady schedule: %s", whole.why)
	}
	firstIn := 0
	for passes[firstIn].at.Before(samples[0].at) {
		firstIn++
	}
	// A trough after the last pass and before the first in the span, in no completed cycle, changes no minimum.
	troughs := slices.Clone(samples)
	for i := range troughs {
		if troughs[i].at.After(passes[len(passes)-1].at) || troughs[i].at.Before(passes[firstIn].at) {
			troughs[i].backlog = 0
		}
	}
	if v := judgeCycles(rate/2, loadSample{}, troughs, passes, r.flush, 6); !slices.Equal(v.minima, whole.minima) {
		t.Errorf("minima %v with troughs outside the cycles, want %v", v.minima, whole.minima)
	}
	if v := judgeCycles(rate/2, loadSample{}, samples, passes, r.flush, len(whole.minima)+1); v.sustained || !strings.Contains(v.why, "cycles") {
		t.Errorf("one cycle short: %+v, want unjudged naming cycles", v)
	}
	if v := judgeCycles(rate, loadSample{}, samples, passes, r.flush, 6); v.sustained || !strings.Contains(v.why, "made") {
		t.Errorf("half the offered rate made: %+v, want not sustained naming made", v)
	}
	dropping := slices.Clone(samples)
	for i := range dropping {
		dropping[i].dropped = 500 + uint64((i+1)/len(dropping)) // the last sample counts one drop past the origin's 500
	}
	if v := judgeCycles(rate/2, loadSample{dropped: 500}, dropping, passes, r.flush, 6); v.sustained || !strings.Contains(v.why, "1 records dropped") {
		t.Errorf("a drop since the origin: %+v, want not sustained naming it", v)
	}
	failed := slices.Clone(passes)
	failed[0].failed = true // before the span
	if v := judgeCycles(rate/2, loadSample{}, samples, failed, r.flush, 6); v.sustained || !strings.Contains(v.why, "failed") {
		t.Errorf("a failed pass: %+v, want not sustained naming it", v)
	}
}

// A flush whose batches settle in two passes a second apart, a few captures' batches opened a pass after the rest's, is one cycle:
// a cycle between the two would hold the flush's peak or its trough alone, and its minimum swing the slope.
// Wherever the samples end, between a flush's two passes included, every minimum is the backlog at a flush's last pass.
func TestJudgeCyclesSplitFlush(t *testing.T) {
	t.Parallel()
	const rate = 1600.0
	r := drainRun{rate: rate, captures: 1, flush: 10 * time.Second, upload: cycled(time.Second)}
	for _, rise := range []float64{0, 4} {
		r.leak = rise * 0.005 * rate
		for end := 72 * time.Second; end < 72*time.Second+r.flush+r.upload(0); end += 250 * time.Millisecond {
			samples, passes := r.run(time.Minute+500*time.Millisecond, end)
			whole := judgeCycles(rate/2, loadSample{}, samples, passes, r.flush, 5)
			if whole.sustained != (rise < 1) {
				t.Fatalf("rising %v of the allowance, one pass a flush, ending at %s: sustained %v (%s)", rise, end, whole.sustained, whole.why)
			}
			for _, apart := range []time.Duration{-time.Second, time.Second} { // the few captures' pass before the rest's, or after it
				split := slices.Clone(passes)
				for _, p := range passes {
					split = append(split, settledPass{at: p.at.Add(apart)})
				}
				slices.SortFunc(split, func(a, b settledPass) int { return a.at.Compare(b.at) })
				v := judgeCycles(rate/2, loadSample{}, samples, split, r.flush, 5)
				if troughs := settledTroughs(r, time.Minute+500*time.Millisecond, end, split, v.minima); troughs != len(v.minima) || len(v.minima) < len(whole.minima)-1 || v.sustained != whole.sustained {
					t.Errorf("rising %v of the allowance, a second pass %s apart, ending at %s: minima %v, sustained %v (%s); want the backlog at a flush's last pass, at least %d, sustained %v",
						rise, apart, end, v.minima, v.sustained, v.why, len(whole.minima)-1, whole.sustained)
				}
			}
		}
	}
}

// Passes that fall at another point between two samples every cycle, the flush's work a little longer than whole seconds, change no minimum:
// a cycle's least sample would hold the records published since the pass, a share of a sampling period changing every cycle,
// and swing the slope past the bound when nothing rises.
func TestJudgeCyclesSamplingPhase(t *testing.T) {
	t.Parallel()
	const rate = 1600.0
	start, span := time.Minute+100*time.Millisecond, 80*time.Second
	r := drainRun{rate: rate, captures: 1, flush: 10 * time.Second, upload: cycled(1150 * time.Millisecond)}
	for _, rise := range []float64{0, 4} {
		r.leak = rise * 0.005 * rate
		samples, passes := r.run(start, span)
		v := judgeCycles(rate/2, loadSample{}, samples, passes, r.flush, 6)
		if troughs := settledTroughs(r, start, span, passes, v.minima); v.sustained != (rise < 1) || troughs != len(v.minima) {
			t.Errorf("rising %v of the allowance: minima %v, %d of them the backlog at a flush's last pass, sustained %v (%s) at %.2f/s, the bound %.2f/s",
				rise, v.minima, troughs, v.sustained, v.why, v.backlogSlope, v.bound)
		}
	}
}

// settledTroughs counts the minima that are the backlog at a flush's last pass, read from r's run sampled every 10 ms:
// each at a pass no other follows within half a flush, its backlog the one sampled there.
func settledTroughs(r drainRun, start, span time.Duration, passes []settledPass, minima []cycleMin) int {
	r.every = 10 * time.Millisecond
	fine, _ := r.run(start, span)
	n := 0
	for _, m := range minima {
		p := slices.IndexFunc(passes, func(p settledPass) bool { return p.at.Equal(m.at) })
		s := slices.IndexFunc(fine, func(s loadSample) bool { return !s.at.Before(m.at) })
		if p >= 0 && (p == len(passes)-1 || passes[p+1].at.Sub(m.at) >= r.flush/2) && s >= 0 && math.Abs(float64(fine[s].backlog)-m.backlog) <= 1 {
			n++
		}
	}

	return n
}

// A trough is read at a flush's last pass between two samples, but not when the next flush's pass falls before the sample after it.
func TestTrough(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 1,000 records a second published, 1,000 acknowledged at 1.25 s.
	samples := []loadSample{{at: t0, published: 0, backlog: 0}, {at: t0.Add(time.Second), published: 1000, backlog: 1000}, {at: t0.Add(2 * time.Second), published: 2000, backlog: 1000}}
	end := t0.Add(1250 * time.Millisecond)
	if lo, ok := trough(samples, []settledPass{{at: end}}, end); !ok || lo != 250 {
		t.Errorf("at a pass between samples: %v, %v; want 250, true", lo, ok)
	}
	if lo, ok := trough(samples, []settledPass{{at: end}}, t0.Add(time.Second)); !ok || lo != 1000 {
		t.Errorf("at a sample: %v, %v; want 1000, true", lo, ok)
	}
	if _, ok := trough(samples, []settledPass{{at: t0.Add(1100 * time.Millisecond)}, {at: end}}, t0.Add(1100*time.Millisecond)); ok {
		t.Error("another pass before the sample after the flush's end: read, want not")
	}
}

// A record's error in each of five values two seconds apart can rise 0.3 a second: 4+2+0+2+4 seconds over 16+4+0+4+16 seconds squared.
func TestCountSlope(t *testing.T) {
	t.Parallel()
	if got := countSlope([]float64{0, 2, 4, 6, 8}); math.Abs(got-0.3) > 1e-9 {
		t.Errorf("countSlope = %v, want 0.3", got)
	}
	if got := countSlope([]float64{5}); got != 0 {
		t.Errorf("countSlope of one value = %v, want 0", got)
	}
}
