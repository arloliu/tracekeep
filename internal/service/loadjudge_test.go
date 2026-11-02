package service

import (
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
}

// judgeLoad judges a sweep point: offered forwards a second, the sample at its origin (before the warm-up),
// and its samples from the first window's start, cut into windows of a fixed duration by their timestamps;
// a trailing part shorter than a window is left out of the windows, its drops counted.
func judgeLoad(offered float64, origin loadSample, samples []loadSample, window time.Duration) loadVerdict {
	t0 := samples[0].at
	windows := int(samples[len(samples)-1].at.Sub(t0) / window)
	end := t0.Add(time.Duration(windows) * window)
	judged := samples
	for len(judged) > 1 && judged[len(judged)-1].at.After(end) {
		judged = judged[:len(judged)-1]
	}
	v := loadVerdict{offered: offered, dropped: samples[len(samples)-1].dropped - origin.dropped}
	if v.why = sampling(judged, windows); v.why != "" {
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

// sampling names why samples cannot be judged over windows: none whole, or a gap above two sampling periods,
// which would also leave a window short of samples; empty when they can.
func sampling(samples []loadSample, windows int) string {
	if windows < 1 {
		return "sampling: shorter than one window"
	}
	for i := 1; i < len(samples); i++ {
		if gap := samples[i].at.Sub(samples[i-1].at); gap > 2*loadTick {
			return fmt.Sprintf("sampling: a gap of %s after %s", gap, samples[i-1].at.Sub(samples[0].at))
		}
	}

	return ""
}

// windowsFor is the windows a measurement samples: at least least, and enough that the last whole window ends after its last call did.
func windowsFor(least int, window, callsEnded time.Duration) int {
	return max(least, int(math.Ceil(float64(callsEnded)/float64(window))))
}

// slope is the least-squares slope of ys over their indexes.
func slope(ys []float64) float64 {
	n := float64(len(ys))
	if n < 2 {
		return 0
	}
	var sx, sy, sxx, sxy float64
	for i, y := range ys {
		x := float64(i)
		sx, sy, sxx, sxy = sx+x, sy+y, sxx+x*x, sxy+x*y
	}

	return (n*sxy - sx*sy) / (n*sxx - sx*sx)
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
		v := judgeLoad(rate, tc.origin, synthetic(tc.origin, windows*window, time.Second, tc.made, tc.set), window)
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
		v := judgeLoad(rate, origin, synthetic(origin, windows*window, time.Second, rate, nil), window)
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
	if v := judgeLoad(rate, loadSample{}, late, window); !v.sustained || math.Abs(v.made-rate) > 1 || math.Abs(v.published-2*rate) > 2 || len(v.backlogMinima) != 11 {
		t.Errorf("late samples: %+v, want sustained at %v made over 11 whole windows", v, rate)
	}
	gapped := synthetic(loadSample{}, 120*time.Second, time.Second, rate, nil)
	gapped = slices.Delete(gapped, 50, 53) // four seconds between two samples
	if v := judgeLoad(rate, loadSample{}, gapped, window); v.sustained || !strings.Contains(v.why, "sampling") {
		t.Errorf("a gap of four periods: %+v, want unjudged naming sampling", v)
	}
	if v := judgeLoad(rate, loadSample{}, synthetic(loadSample{}, 5*time.Second, time.Second, rate, nil), window); v.sustained || !strings.Contains(v.why, "sampling") {
		t.Errorf("less than a window: %+v, want unjudged naming sampling", v)
	}
}

// A call still running at the last of the least windows extends the measurement by whole windows past its end,
// so the judged span holds every attempt.
func TestWindowsFor(t *testing.T) {
	t.Parallel()
	const window = 10 * time.Second
	for _, tc := range []struct {
		ended time.Duration
		want  int
	}{{100 * time.Second, 12}, {120 * time.Second, 12}, {120*time.Second + time.Millisecond, 13}, {125 * time.Second, 13}, {131 * time.Second, 14}} {
		if got := windowsFor(12, window, tc.ended); got != tc.want {
			t.Errorf("calls ending at %s: %d windows, want %d", tc.ended, got, tc.want)
		}
	}
	// Judged over the extended span, the windows are whole and end after the call.
	v := judgeLoad(100, loadSample{}, synthetic(loadSample{}, 130*time.Second, time.Second, 100, nil), window)
	if !v.sustained || len(v.backlogMinima) != 13 {
		t.Errorf("a span of 13 windows: %+v, want 13 sustained windows", v)
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
