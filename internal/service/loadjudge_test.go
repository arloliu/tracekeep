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

// loadSample is one second's reading of the load harness: counters since the harness started, and gauges.
type loadSample struct {
	// made are the forwards the harness made; published, the traffic stream's last sequence; dropped, the producers' records_dropped_total.
	made, published, dropped uint64
	// backlog is the traffic stream's messages; ring and reserved, the producers' ordinary rings' and reserved lanes' depths summed.
	backlog        uint64
	ring, reserved float64
}

// loadVerdict is a sweep point judged.
type loadVerdict struct {
	// offered, made and published are forwards, forwards and records a second over the windows.
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

// judgeLoad judges a sweep point: offered forwards a second, the sample at its origin (before the warm-up)
// and its windows' samples, one a second from the first window's start, perWindow a window.
func judgeLoad(offered float64, origin loadSample, samples []loadSample, perWindow int) loadVerdict {
	first, last := samples[0], samples[len(samples)-1]
	windows := (len(samples) - 1) / perWindow
	secs := float64(windows * perWindow)
	v := loadVerdict{
		offered: offered, made: float64(last.made-first.made) / secs, published: float64(last.published-first.published) / secs,
		dropped: last.dropped - origin.dropped,
	}
	ring, reserved := make([]float64, windows), make([]float64, windows)
	for w := range windows {
		span := samples[w*perWindow : (w+1)*perWindow+1]
		lo := float64(span[0].backlog)
		for _, s := range span {
			lo = min(lo, float64(s.backlog))
			ring[w], reserved[w] = max(ring[w], s.ring), max(reserved[w], s.reserved)
		}
		v.backlogMinima = append(v.backlogMinima, lo)
	}
	v.backlogSlope, v.ringSlope, v.reservedSlope = slope(v.backlogMinima), slope(ring), slope(reserved)
	// 0.5% of a window's mean published records: a rise of 1% a window fails.
	v.bound = 0.005 * float64(last.published-first.published) / float64(windows)
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

// accept passes a PackURL measurement or names why it fails.
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

// percentile is the nearest-rank q-quantile of d, 0 for none.
func percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)

	return s[max(0, int(math.Ceil(q*float64(len(s))))-1)]
}

// synthetic builds a sweep point of windows of perWindow one-second samples from origin, after a minute's warm-up:
// made forwards a second, two records published for each, and set filling each sample's gauges.
func synthetic(origin loadSample, windows, perWindow int, made float64, set func(i int, s *loadSample)) []loadSample {
	const warm = 60
	out := make([]loadSample, windows*perWindow+1)
	for i := range out {
		n := uint64((float64(warm + i)) * made)
		s := loadSample{made: origin.made + n, published: origin.published + 2*n, dropped: origin.dropped}
		if set != nil {
			set(i, &s)
		}
		out[i] = s
	}

	return out
}

// The sustained rule on recorded series: made within 2% of offered, nothing dropped,
// and the windows' backlog minima and queue maxima each rising less than 0.5% of a window's records a window.
func TestJudgeLoad(t *testing.T) {
	t.Parallel()
	const windows, per, rate = 12, 10, 100.0 // 2,000 records a window: the bound is 10 a window
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
			if i == windows*per {
				s.dropped++
			}
		}, false, "dropped"},
	} {
		v := judgeLoad(rate, tc.origin, synthetic(tc.origin, windows, per, tc.made, tc.set), per)
		if v.sustained != tc.sustained || !strings.Contains(v.why, tc.why) {
			t.Errorf("%s: sustained %v (%s), want %v naming %q; %+v", name, v.sustained, v.why, tc.sustained, tc.why, v)
		}
	}
}

// Two points whose producers' counters start from different origins are each judged on their own deltas.
func TestJudgeLoadOrigins(t *testing.T) {
	t.Parallel()
	const windows, per, rate = 12, 10, 100.0
	for _, origin := range []loadSample{{}, {made: 1_000_000, published: 3_000_000, dropped: 500}} {
		v := judgeLoad(rate, origin, synthetic(origin, windows, per, rate, nil), per)
		if !v.sustained || v.made != rate || v.published != 2*rate || v.dropped != 0 || len(v.backlogMinima) != windows {
			t.Errorf("origin %+v: %+v, want sustained at %v made, %v published, none dropped, %d windows", origin, v, rate, 2*rate, windows)
		}
	}
}

// A PackURL measurement passes only over sustained ingest, across ten flushes, with no failure and at most 5% of its attempts superseded.
func TestPackURLAcceptance(t *testing.T) {
	t.Parallel()
	fast := make([]time.Duration, 1000)
	for i := range fast {
		fast[i] = 5 * time.Millisecond
	}
	ok := packURLResult{ingest: loadVerdict{sustained: true}, flushes: 12, made: 1000, succeeded: 1000, latencies: fast}
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
