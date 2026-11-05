package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
)

// The recovery drain's judgement (load_test.go's recovery mode): the traffic beside a drain judged by completed flush cycles within it,
// the release's and completion's samples its ends; the decision's conditions, each true, false or unknown; and the cohort, read by identity.

const (
	// drainCycles are the completed cycles a drain's traffic is judged over; fewer leave its verdict unknown.
	drainCycles = 3
	// drainCalls are the PackURL calls a p99 is read over; fewer leave it unknown.
	drainCalls = 200
	// ackShare and compactorCycle: the acknowledgement's condition is its share of the drain, at least ackShare, in a drain longer than a compactor cycle.
	ackShare       = 0.2
	compactorCycle = 5 * time.Minute
	// drainEpisodes bound the episodes an unknown condition runs again; mergeCut is the drain a batched acknowledgement must cut to merge.
	drainEpisodes = 3
	mergeCut      = 0.2
	// failedCall is a failed or timed-out PackURL call's latency: past any bound.
	failedCall = time.Duration(math.MaxInt64)
)

// cond is one of the decision's conditions: true, false or unknown, an unknown never read as false.
type cond int8

const (
	unknown cond = iota
	condFalse
	condTrue
)

func (c cond) String() string { return [...]string{"unknown", "false", "true"}[c] }

// judgeDrain judges the traffic beside a drain: samples run from the release's sample to the completion's, both taken explicitly,
// and the cycles are the traffic's completed flush cycles between them, as judgeCycles reads them, at least drainCycles of them;
// the offered, made and published rates and the drops are counted from the release's and completion's samples, the slope through whole cycles.
// A drain with too few samples or cycles is not judged, never judged over a shorter span.
func judgeDrain(offered float64, samples []loadSample, passes []settledPass) cycleVerdict {
	if len(samples) < 2 {
		return cycleVerdict{offered: offered, why: fmt.Sprintf("sampling: %d samples, want the release's and the completion's at least", len(samples))}
	}

	return judgeSpan(offered, samples[0], samples, passes, drainCycles, true)
}

// drainEpisode is one episode's drain: its time from the release to completion, the traffic judged within it,
// its PackURL calls (a failed one as failedCall) and the parked consumer's acknowledgement time within it.
type drainEpisode struct {
	drain   time.Duration
	traffic cycleVerdict
	calls   []time.Duration
	ack     time.Duration
}

// drainDecision is the decision's conditions over the episodes run: the traffic not sustained, the PackURL p99 past gateP99,
// and the acknowledgement at least ackShare of a drain longer than compactorCycle.
type drainDecision struct {
	traffic, p99, ack cond
	// p99v is the pooled p99 when read; calls are the calls pooled.
	p99v  time.Duration
	calls int
}

// decide reads the conditions over eps.
// The traffic's condition reads each episode alone: true if any is judged not sustained, false if one is judged sustained and none not,
// unknown if none is judged. The p99 is pooled over every episode's calls, unknown under drainCalls.
// The acknowledgement's is true if an episode's drain, longer than compactorCycle, spent at least ackShare of it acknowledging.
func decide(eps []drainEpisode) drainDecision {
	d := drainDecision{traffic: unknown, p99: unknown, ack: condFalse}
	var calls []time.Duration
	for _, e := range eps {
		calls = append(calls, e.calls...)
		switch {
		case e.traffic.judged && !e.traffic.sustained:
			d.traffic = condTrue
		case e.traffic.judged && d.traffic == unknown:
			d.traffic = condFalse
		default:
		}
		if e.drain > compactorCycle && float64(e.ack) >= ackShare*float64(e.drain) {
			d.ack = condTrue
		}
	}
	if d.calls = len(calls); d.calls >= drainCalls {
		d.p99v, d.p99 = percentile(calls, 0.99), condFalse
		if d.p99v > gateP99 {
			d.p99 = condTrue
		}
	}

	return d
}

// outcome is the decision: admitted when any condition is true, closed when every one is false, unknown otherwise.
func (d drainDecision) outcome() string {
	cs := []cond{d.traffic, d.p99, d.ack}
	switch {
	case slices.Contains(cs, condTrue):
		return "admitted"
	case slices.Contains(cs, unknown):
		return "unknown"
	default:
		return "closed"
	}
}

// rerun reports an unknown condition, which runs the episode again, at most drainEpisodes in all.
func (d drainDecision) rerun() bool {
	return d.traffic == unknown || d.p99 == unknown || d.ack == unknown
}

// mergeRule reads a batched acknowledgement's merge from the base's episodes and the head's at the same M:
// true when head's mean drain is at least mergeCut shorter and its traffic judged sustained in every episode it ran,
// false when the cut falls short or an episode is judged not sustained, unknown when an episode is not judged.
func mergeRule(base, head []drainEpisode) cond {
	mean := func(eps []drainEpisode) float64 {
		var sum time.Duration
		for _, e := range eps {
			sum += e.drain
		}

		return float64(sum) / float64(len(eps))
	}
	if len(base) == 0 || len(head) == 0 || mean(head) > (1-mergeCut)*mean(base) {
		return condFalse
	}
	out := condTrue
	for _, e := range head {
		switch {
		case e.traffic.judged && !e.traffic.sustained:
			return condFalse
		case !e.traffic.judged:
			out = unknown
		default:
		}
	}

	return out
}

// versionSuffix is the end of a version's subject on the parking stream: its capture, seq and version token,
// the full hex SHA-256 of its 44 header bytes and its payload digest (parking.md).
func versionSuffix(v catalog.VersionKey) string {
	h := sha256.New()
	h.Write(v.Header[:])
	h.Write(v.Digest[:])

	return fmt.Sprintf("%s.%d.%s", v.Capture, v.Seq, hex.EncodeToString(h.Sum(nil)))
}

// subjectSuffix is a parking subject's last three tokens, its version's suffix.
func subjectSuffix(subject string) string {
	ts := strings.Split(subject, ".")

	return strings.Join(ts[max(0, len(ts)-3):], ".")
}

// cohortParked names why the cohort does not yet hold before the release, empty when it does:
// n versions parked, each original's row on the traffic incarnation acked, and the parking stream holding one copy on each version's subject, no other.
func cohortParked(vs []catalog.ParkedVersion, subjects map[string]uint64, n int, traffic int64) string {
	if len(vs) != n {
		return fmt.Sprintf("%d versions parked, want %d", len(vs), n)
	}
	copies := map[string]uint64{}
	for s, k := range subjects {
		copies[subjectSuffix(s)] += k
	}
	for _, v := range vs {
		if v.State != catalog.VersionParked {
			return fmt.Sprintf("seq %d is %s", v.Version.Seq, v.State)
		}
		if !slices.ContainsFunc(v.Copies, func(c catalog.ParkedCopy) bool { return c.Incarnation == traffic && c.State == "acked" }) {
			return fmt.Sprintf("seq %d: its original is not acknowledged", v.Version.Seq)
		}
		switch k := copies[versionSuffix(v.Version)]; k {
		case 0:
			return fmt.Sprintf("seq %d: no copy on its subject", v.Version.Seq)
		case 1:
		default:
			return fmt.Sprintf("seq %d: %d copies on its subject", v.Version.Seq, k)
		}
	}
	if len(copies) != n {
		return fmt.Sprintf("the parking stream holds %d subjects, want %d", len(copies), n)
	}

	return ""
}

// cohortDrained names why the drain is not complete, empty when it is: n versions staged, registered in a pack,
// each with its copy's row on the parked incarnation present and acked, read by identity, never by acked_at.
// err is a state the drain cannot leave: a lost row, a terminated or evicted version, more versions than n.
func cohortDrained(vs []catalog.ParkedVersion, n int, parked int64) (string, error) {
	if len(vs) > n {
		return "", fmt.Errorf("%d versions, want %d", len(vs), n)
	}
	for _, v := range vs {
		for _, c := range v.Copies {
			if c.State == "lost" {
				return "", fmt.Errorf("seq %d: the row of incarnation %d's stream seq %d is lost", v.Version.Seq, c.Incarnation, c.StreamSeq)
			}
		}
		if v.State == catalog.VersionTerminated || v.State == catalog.VersionEvictedHour {
			return "", fmt.Errorf("seq %d is %s", v.Version.Seq, v.State)
		}
	}
	if len(vs) < n {
		return fmt.Sprintf("%d versions, want %d", len(vs), n), nil
	}
	for _, v := range vs {
		if v.State != catalog.VersionStaged {
			return fmt.Sprintf("seq %d is %s", v.Version.Seq, v.State), nil
		}
		if !slices.ContainsFunc(v.Copies, func(c catalog.ParkedCopy) bool { return c.Incarnation == parked }) {
			return fmt.Sprintf("seq %d: its copy's row is missing", v.Version.Seq), nil
		}
		for _, c := range v.Copies {
			if c.State != "acked" {
				return fmt.Sprintf("seq %d: the row of incarnation %d's stream seq %d is %s", v.Version.Seq, c.Incarnation, c.StreamSeq, c.State), nil
			}
		}
	}

	return "", nil
}

// recoveryPending is max_ack_pending on both consumers in the recovery mode, as the production run had it:
// past the M + 1 copies the parked consumer holds at M of 100,000, and past the traffic a flush holds.
const recoveryPending = 600_000

// recoveryM reads M, the data records parked: a positive even count, a forward making two, whose cohort fits the pending budget.
func recoveryM(v string, def int) (int, error) {
	if v == "" {
		return def, nil
	}
	m, err := strconv.Atoi(v)
	if err != nil || m <= 0 || m%2 != 0 || m+1 > recoveryPending {
		return 0, fmt.Errorf("M %q: want a positive even count of data records, its cohort within %d pending", v, recoveryPending)
	}

	return m, nil
}

// effective names how a consumer's effective configuration differs from the one ingest was given, nil when it does not.
func effective(name string, got jetstream.ConsumerConfig, want config.Ingest) error {
	if got.AckWait != want.AckWait || got.MaxAckPending != want.MaxAckPending {
		return fmt.Errorf("consumer %s: ack_wait %s and max_ack_pending %d, want %s and %d", name, got.AckWait, got.MaxAckPending, want.AckWait, want.MaxAckPending)
	}

	return nil
}

// drainSchedule is a sustained traffic schedule beside a drain: dev's 10 s flush over one capture, a steady upload.
func drainSchedule(captures int, leak float64) drainRun {
	return drainRun{rate: 1600, captures: captures, flush: 10 * time.Second, upload: cycled(time.Second), leak: leak}
}

// The traffic beside a drain is judged over the drain's completed flush cycles:
// a bounded sawtooth of flushes is sustained at every starting phase, rising cycle floors are not, and too few cycles are not judged.
func TestJudgeDrain(t *testing.T) {
	t.Parallel()
	const rate = 1600.0
	for _, captures := range []int{1, 7} {
		for phase := time.Duration(0); phase < 10*time.Second; phase += time.Second { // every starting phase of a 10 s flush
			start := time.Minute + phase
			samples, passes := drainSchedule(captures, 0).run(start, 2*time.Minute)
			if v := judgeDrain(rate/2, samples, passes); !v.judged || !v.sustained {
				t.Errorf("a bounded sawtooth over %d captures from %s: judged %v, sustained %v (%s)", captures, phase, v.judged, v.sustained, v.why)
			}
			// Four times the allowance, 0.5% of the published rate a second.
			samples, passes = drainSchedule(captures, 4*0.005*rate).run(start, 2*time.Minute)
			if v := judgeDrain(rate/2, samples, passes); !v.judged || v.sustained || !strings.Contains(v.why, "backlog minima rise") {
				t.Errorf("rising floors over %d captures from %s: judged %v, sustained %v (%s)", captures, phase, v.judged, v.sustained, v.why)
			}
		}
	}
	// A drain of two flushes holds fewer than three completed cycles; one sample is too few to judge.
	samples, passes := drainSchedule(1, 0).run(time.Minute, 25*time.Second)
	if v := judgeDrain(rate/2, samples, passes); v.judged || v.sustained || !strings.Contains(v.why, "cycles") {
		t.Errorf("a drain of two flushes: judged %v, sustained %v (%s); want not judged, naming cycles", v.judged, v.sustained, v.why)
	}
	if v := judgeDrain(rate/2, samples[:1], passes); v.judged || v.sustained {
		t.Errorf("one sample: judged %v, sustained %v (%s); want not judged", v.judged, v.sustained, v.why)
	}
}

// The offered rate and the drops are counted from the release's and completion's samples, while the slope reads whole cycles:
// a drop or a stall in the part before the first completed pass or after the last fails the drain,
// which judgeCycles, reading its rates within the cycles, would pass; a backlog spike there changes no cycle's minimum.
func TestJudgeDrainEnds(t *testing.T) {
	t.Parallel()
	const rate = 1600.0
	samples, passes := drainSchedule(1, 0).run(time.Minute+3*time.Second, 2*time.Minute)
	firstPass := slices.IndexFunc(samples, func(s loadSample) bool {
		return !s.at.Before(passes[slices.IndexFunc(passes, func(p settledPass) bool { return !p.at.Before(samples[0].at) })].at)
	})
	lastPass := passes[len(passes)-1].at
	if firstPass < 3 || !lastPass.Before(samples[len(samples)-1].at) {
		t.Fatalf("the schedule leaves no partial cycle at its ends: first pass at sample %d, last at %s", firstPass, lastPass)
	}
	for name, tc := range map[string]struct {
		edit func(s []loadSample)
		why  string
	}{
		"steady":                     {func([]loadSample) {}, ""},
		"a drop after the last pass": {func(s []loadSample) { s[len(s)-1].dropped++ }, "1 records dropped"},
		// The forwards stall for the head's first seconds: 2.5% of the drain's span short of the offered rate.
		"a stall before the first pass": {func(s []loadSample) {
			for i := range s {
				s[i].made -= uint64(math.Min(float64(i), 3) * rate / 2)
			}
		}, "made"},
		"a backlog spike before the first pass": {func(s []loadSample) { s[1].backlog += 100_000 }, ""},
	} {
		s := slices.Clone(samples)
		tc.edit(s)
		v := judgeDrain(rate/2, s, passes)
		if !v.judged || v.sustained != (tc.why == "") || !strings.Contains(v.why, tc.why) {
			t.Errorf("%s: judged %v, sustained %v (%s); want naming %q", name, v.judged, v.sustained, v.why, tc.why)
		}
		if tc.why != "" {
			if w := judgeCycles(rate/2, s[0], s, passes, drainCycles); !w.sustained && !strings.Contains(w.why, "dropped") {
				t.Errorf("%s: judgeCycles, reading the cycles alone, not sustained (%s)", name, w.why)
			}
		}
	}
}

// episode is a drain episode of the schedule's traffic over span, with calls PackURL calls of latency each and ack of acknowledgement.
func episode(t *testing.T, r drainRun, span time.Duration, calls int, latency, ack time.Duration) drainEpisode {
	t.Helper()
	samples, passes := r.run(time.Minute, span)
	cs := make([]time.Duration, calls)
	for i := range cs {
		cs[i] = latency
	}

	return drainEpisode{drain: span, traffic: judgeDrain(r.rate/2, samples, passes), calls: cs, ack: ack}
}

// The decision's three states on synthetic episodes: any condition true admits, every one false closes, and an unknown,
// never read as false, runs the episode again; the traffic reads each episode alone, the p99 pools the episodes' calls.
func TestDrainDecision(t *testing.T) {
	t.Parallel()
	const rate = 1600.0
	steady, rising := drainSchedule(1, 0), drainSchedule(1, 4*0.005*rate)
	fast := 5 * time.Millisecond
	for name, tc := range map[string]struct {
		eps               []drainEpisode
		traffic, p99, ack cond
		outcome           string
		rerun             bool
	}{
		"every condition false": {[]drainEpisode{episode(t, steady, 6*time.Minute, 300, fast, time.Minute)}, condFalse, condFalse, condFalse, "closed", false},
		"unknown traffic with the rest passing": {[]drainEpisode{episode(t, steady, 25*time.Second, 300, fast, time.Second)},
			unknown, condFalse, condFalse, "unknown", true},
		"too few PackURL calls": {[]drainEpisode{episode(t, steady, 6*time.Minute, drainCalls-1, fast, time.Minute)}, condFalse, unknown, condFalse, "unknown", true},
		// Failed calls count as past 2 s, however fast the rest.
		"every call failing": {[]drainEpisode{episode(t, steady, 6*time.Minute, 300, failedCall, time.Minute)}, condFalse, condTrue, condFalse, "admitted", false},
		"the p99 pooled over episodes": {[]drainEpisode{episode(t, steady, 6*time.Minute, 150, fast, time.Minute), episode(t, steady, 6*time.Minute, 150, 3*time.Second, time.Minute)},
			condFalse, condTrue, condFalse, "admitted", false},
		// Each episode grows within its drain, and the backlog falls between them: judged each alone, each is not sustained.
		"three episodes growing within their drains": {[]drainEpisode{
			episode(t, rising, 3*time.Minute, 100, fast, 0), episode(t, rising, 3*time.Minute, 100, fast, 0), episode(t, rising, 3*time.Minute, 100, fast, 0),
		}, condTrue, condFalse, condFalse, "admitted", false},
		"a sustained episode beside an unjudged one": {[]drainEpisode{episode(t, steady, 6*time.Minute, 150, fast, 0), episode(t, steady, 25*time.Second, 150, fast, 0)},
			condFalse, condFalse, condFalse, "closed", false},
		"the acknowledgement a fifth of a drain past a compactor cycle": {[]drainEpisode{episode(t, steady, 6*time.Minute, 300, fast, 72*time.Second)},
			condFalse, condFalse, condTrue, "admitted", false},
		"the acknowledgement a fifth of a drain within a compactor cycle": {[]drainEpisode{episode(t, steady, 4*time.Minute, 300, fast, 48*time.Second)},
			condFalse, condFalse, condFalse, "closed", false},
	} {
		d := decide(tc.eps)
		if d.traffic != tc.traffic || d.p99 != tc.p99 || d.ack != tc.ack || d.outcome() != tc.outcome || d.rerun() != tc.rerun {
			t.Errorf("%s: traffic %s, p99 %s (%s over %d calls), ack %s, %s, rerun %v; want %s, %s, %s, %s, rerun %v",
				name, d.traffic, d.p99, d.p99v, d.calls, d.ack, d.outcome(), d.rerun(), tc.traffic, tc.p99, tc.ack, tc.outcome, tc.rerun)
		}
	}
}

// The batched acknowledgement merges only on a cut of a fifth with its traffic judged sustained in every episode it ran:
// a judged base whose head is too short to judge leaves the merge unknown.
func TestMergeRule(t *testing.T) {
	t.Parallel()
	const rate = 1600.0
	steady, rising := drainSchedule(1, 0), drainSchedule(1, 4*0.005*rate)
	base := []drainEpisode{episode(t, steady, 10*time.Minute, 0, 0, 0)}
	for name, tc := range map[string]struct {
		head []drainEpisode
		want cond
	}{
		"cut by a fifth, sustained":        {[]drainEpisode{episode(t, steady, 8*time.Minute, 0, 0, 0)}, condTrue},
		"cut short of a fifth":             {[]drainEpisode{episode(t, steady, 9*time.Minute, 0, 0, 0)}, condFalse},
		"cut, its traffic not sustained":   {[]drainEpisode{episode(t, rising, 5*time.Minute, 0, 0, 0)}, condFalse},
		"a head too short to judge":        {[]drainEpisode{episode(t, steady, 25*time.Second, 0, 0, 0)}, unknown},
		"a judged head beside an unjudged": {[]drainEpisode{episode(t, steady, 5*time.Minute, 0, 0, 0), episode(t, steady, 25*time.Second, 0, 0, 0)}, unknown},
	} {
		if !base[0].traffic.judged {
			t.Fatalf("the base is not judged: %s", base[0].traffic.why)
		}
		if got := mergeRule(base, tc.head); got != tc.want {
			t.Errorf("%s: merge %s, want %s", name, got, tc.want)
		}
	}
}

// cohort is m+1 versions of one capture in state, each with its original's row on the traffic incarnation 1 in orig,
// and, when copied is not empty, its copy's row on the parked incarnation 2 in copied; with the subjects of their copies, one each.
func cohort(m int, state catalog.VersionState, orig, copied string) ([]catalog.ParkedVersion, map[string]uint64) {
	vs := make([]catalog.ParkedVersion, m+1)
	subjects := map[string]uint64{}
	for i := range vs {
		v := catalog.VersionKey{Capture: tracepack.UUID{7}, Seq: uint64(i)} //nolint:gosec // a small index
		v.Digest[0] = byte(i)
		vs[i] = catalog.ParkedVersion{Version: v, State: state, Copies: []catalog.ParkedCopy{{Incarnation: 1, StreamSeq: int64(10 + i), State: orig}}}
		if copied != "" {
			vs[i].Copies = append(vs[i].Copies, catalog.ParkedCopy{Incarnation: 2, StreamSeq: int64(1 + i), State: copied})
		}
		subjects["tkpark.T7."+versionSuffix(v)] = 1
	}

	return vs, subjects
}

// Before the release the cohort holds by identity: m+1 versions parked, each original acknowledged, and the parking stream
// holding one copy on each version's own subject; copy rows are not required, and one made before the release changes nothing.
func TestCohortParked(t *testing.T) {
	t.Parallel()
	const m = 4
	for name, tc := range map[string]struct {
		edit func(vs []catalog.ParkedVersion, subjects map[string]uint64) []catalog.ParkedVersion
		why  string
	}{
		"held": {func(vs []catalog.ParkedVersion, _ map[string]uint64) []catalog.ParkedVersion { return vs }, ""},
		"a copy's row made before the release": {func(vs []catalog.ParkedVersion, _ map[string]uint64) []catalog.ParkedVersion {
			vs[2].Copies = append(vs[2].Copies, catalog.ParkedCopy{Incarnation: 2, StreamSeq: 3, State: "waiting"})
			return vs
		}, ""},
		"a version short": {func(vs []catalog.ParkedVersion, _ map[string]uint64) []catalog.ParkedVersion { return vs[:m] }, "versions"},
		"a version still parking": {func(vs []catalog.ParkedVersion, _ map[string]uint64) []catalog.ParkedVersion {
			vs[1].State = catalog.VersionParking
			return vs
		}, "parking"},
		"an original not acknowledged": {func(vs []catalog.ParkedVersion, _ map[string]uint64) []catalog.ParkedVersion {
			vs[3].Copies[0].State = "waiting"
			return vs
		}, "original"},
		"a copy missing from the stream": {func(vs []catalog.ParkedVersion, subjects map[string]uint64) []catalog.ParkedVersion {
			delete(subjects, "tkpark.T7."+versionSuffix(vs[0].Version))
			return vs
		}, "subject"},
		"a copy of another version": {func(vs []catalog.ParkedVersion, subjects map[string]uint64) []catalog.ParkedVersion {
			delete(subjects, "tkpark.T7."+versionSuffix(vs[0].Version))
			subjects["tkpark.T7."+tracepack.UUID{7}.String()+".0.00"] = 1
			return vs
		}, "subject"},
		"two copies of one version": {func(vs []catalog.ParkedVersion, subjects map[string]uint64) []catalog.ParkedVersion {
			subjects["tkpark.T7."+versionSuffix(vs[4].Version)] = 2
			return vs
		}, "copies"},
	} {
		vs, subjects := cohort(m, catalog.VersionParked, "acked", "")
		vs = tc.edit(vs, subjects)
		if why := cohortParked(vs, subjects, m+1, 1); (why == "") != (tc.why == "") || !strings.Contains(why, tc.why) {
			t.Errorf("%s: %q, want naming %q", name, why, tc.why)
		}
	}
}

// Completion is every version staged and every expected copy's row present and acked, by identity:
// a copy's row waiting or missing is not done, a lost one fails the drain, and acked_at is never the predicate.
func TestCohortDrained(t *testing.T) {
	t.Parallel()
	const m = 4
	for name, tc := range map[string]struct {
		edit func(vs []catalog.ParkedVersion) []catalog.ParkedVersion
		why  string
		lost bool
	}{
		"drained": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion { return vs }, "", false},
		"acked_at unset": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			for i := range vs {
				vs[i].AckedAt = 0
			}
			return vs
		}, "", false},
		"a copy's row waiting, acked_at set": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			vs[2].Copies[1].State = "waiting"
			return vs
		}, "waiting", false},
		"a copy's row missing": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			vs[0].Copies = vs[0].Copies[:1]
			return vs
		}, "missing", false},
		"a copy's row lost": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			vs[3].Copies[1].State = "lost"
			return vs
		}, "lost", true},
		"an original's row lost": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			vs[3].Copies[0].State = "lost"
			return vs
		}, "lost", true},
		"a version not yet staged": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			vs[1].State = catalog.VersionParked
			return vs
		}, "parked", false},
		"a version terminated": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			vs[1].State = catalog.VersionTerminated
			return vs
		}, "terminated", true},
		"a version short": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion { return vs[1:] }, "versions", false},
		"a version over": {func(vs []catalog.ParkedVersion) []catalog.ParkedVersion {
			return append(vs, vs[0])
		}, "versions", true},
	} {
		vs, _ := cohort(m, catalog.VersionStaged, "acked", "acked")
		for i := range vs {
			vs[i].AckedAt = 1
		}
		why, err := cohortDrained(tc.edit(vs), m+1, 2)
		text := why
		if err != nil {
			text = err.Error()
		}
		if (text == "") != (tc.why == "") || !strings.Contains(text, tc.why) || (err != nil) != tc.lost {
			t.Errorf("%s: %q, %v; want naming %q, failing %v", name, why, err, tc.why, tc.lost)
		}
	}
}

// M is a positive even count of data records whose cohort fits the raised pending budget;
// at M of 100,000 that budget is past the default 50,000, and a consumer left at the default is refused, on either consumer.
func TestRecoveryBudget(t *testing.T) {
	t.Parallel()
	for v, want := range map[string]int{"": 200, "1000": 1000, "100000": 100_000, "100001": 0, "0": 0, "-2": 0, "x": 0, "600000": 0} {
		got, err := recoveryM(v, 200)
		if got != want || (err == nil) != (want != 0) {
			t.Errorf("recoveryM(%q) = %d, %v; want %d", v, got, err, want)
		}
	}
	def := config.Ingest{AckWait: 7 * time.Minute, MaxAckPending: 50_000}
	if recoveryPending <= def.MaxAckPending || recoveryPending < 100_001 {
		t.Fatalf("the recovery budget %d is not past the default %d and a cohort of 100,001", recoveryPending, def.MaxAckPending)
	}
	raised := def
	raised.MaxAckPending = recoveryPending
	for _, name := range []string{"tk-ingest-1", "tk-parked-2"} {
		if err := effective(name, jetstream.ConsumerConfig{AckWait: def.AckWait, MaxAckPending: def.MaxAckPending}, raised); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s left at the default: %v, want refused", name, err)
		}
		if err := effective(name, jetstream.ConsumerConfig{AckWait: def.AckWait, MaxAckPending: recoveryPending}, raised); err != nil {
			t.Errorf("%s raised: %v", name, err)
		}
	}
}
