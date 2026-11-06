package compact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

var errPut = errors.New("the store refuses the put")

// puts is a store that counts its puts and refuses them while refuse is set, storing nothing then;
// a refused put comes after the merge read its inputs, so each is a failed attempt.
type puts struct {
	store.ObjectStore
	refuse atomic.Bool
	n      atomic.Int64
}

func (s *puts) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	s.n.Add(1)
	if s.refuse.Load() {
		return errPut
	}

	return s.ObjectStore.Put(ctx, key, r, size)
}

// refusing makes f's merges fail at their put, at the default interval, and returns the counting store.
func (f *fixture) refusing(t *testing.T) *puts {
	t.Helper()
	f.merging(t)
	f.r.Interval = 5 * time.Minute
	s := &puts{ObjectStore: f.s}
	s.refuse.Store(true)
	f.r.Store = s

	return s
}

// scope stages a segment of capture's hour hr holding seq and returns its scope.
func (f *fixture) scope(t *testing.T, capture tracepack.UUID, hr int64, seq uint64) catalog.Scope {
	t.Helper()
	f.stage(t, segmentOf(t, capture, hr, recordsAt(hr, seq), noAdjust))

	return catalog.Scope{Capture: capture, Hour: hr}
}

// requestAt requests a merge of capture's hour hr.
func (f *fixture) requestAt(t *testing.T, capture tracepack.UUID, hr int64) tracepack.UUID {
	t.Helper()
	id, err := f.c.RequestCompaction(t.Context(), catalog.RequestMerge, capture, &hr, "op")
	if err != nil {
		t.Fatal(err)
	}

	return id
}

// attempted runs f and returns the puts it made.
func (s *puts) attempted(f func()) int64 {
	before := s.n.Load()
	f()

	return s.n.Load() - before
}

// A failing merge waits one cycle, then two, four and eight, then an hour's twelve cycles at the default interval:
// a failure at cycle N is attempted again at N+1, never N+2. A cycle that skips it attempts nothing and counts no merge,
// but the merge task never succeeds while the scope fails or waits.
func TestMergeBackoffDoubles(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		s := f.refusing(t)
		m := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(m); err != nil {
			t.Fatal(err)
		}
		f.scope(t, f.admit(t), hour, 1)
		pass := func() {
			if err := f.r.mergePass(t.Context(), f.lease); err != nil {
				t.Fatal(err)
			}
		}
		var got []int
		for cycle := range 40 {
			if s.attempted(pass) == 1 {
				got = append(got, cycle)
			}
		}
		if want := []int{0, 1, 3, 7, 15, 27, 39}; !slices.Equal(got, want) {
			t.Errorf("attempted at cycles %v, want %v", got, want)
		}
		scraped := scrape(t, m)
		if want := fmt.Sprintf(`tracekeep_compact_merges_total{outcome="failed"} %d`, len(got)); !strings.Contains(scraped, want) {
			t.Errorf("/metrics lacks %s: a skipped scope counts no merge", want)
		}
		if strings.Contains(scraped, `tracekeep_compact_last_success_timestamp{task="merge"}`) {
			t.Errorf("the merge task succeeded while its scope failed or waited")
		}
	})
}

// Work registered after a failed snapshot makes the scope due at once, whether it came during the wait
// or between the snapshot and the failure: the failure is keyed by the revision its snapshot read.
func TestMergeBackoffEndsWithANewRevision(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		s := f.refusing(t)
		capture := f.admit(t)
		f.scope(t, capture, hour, 1)
		cycles := func(want ...int64) {
			t.Helper()
			for i, w := range want {
				if n := s.attempted(func() { f.merges(t) }); n != w {
					t.Fatalf("cycle %d of %v attempted %d, want %d", i, want, n, w)
				}
			}
		}
		cycles(1, 1, 0, 1, 0) // waiting four cycles from the third failure
		f.scope(t, capture, hour, 2)
		cycles(1, 1, 0, 1, 0, 0, 0) // a fresh revision's backoff, again waiting four from its third failure
		f.r.hooks[hookMergeSnapshot] = func() {
			f.r.hooks[hookMergeSnapshot] = nil
			f.scope(t, capture, hour, 3)
		}
		cycles(1, 1) // the fourth failure, of the snapshot's revision; the registration after it makes the next cycle attempt
	})
}

// A request for a scope MergeCandidates leaves out, unmergeable or not yet quiet, is attempted while it waits out a failure
// only when the scope's current revision moved: a registration during the wait makes it due, an unchanged revision keeps it suppressed,
// and a revision that cannot be read attempts nothing.
func TestMergeRequestBackoff(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unmergeable", "not yet quiet"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				s := f.refusing(t)
				capture := f.admit(t)
				if kind == "unmergeable" {
					s.refuse.Store(false)
					scope := f.overBudget(t, capture)
					if rep := f.merges(t); !slices.Equal(rep.Refused, []catalog.Scope{scope}) {
						t.Fatalf("Merges = %+v, want the scope refused", rep)
					}
					s.refuse.Store(true)
					f.r.Reader = tracepack.ReaderOptions{}
				} else {
					f.r.Now = func() time.Time { return time.Unix(0, hour*nsPerHour).Add(30 * time.Minute) }
					f.scope(t, capture, hour, 1)
				}
				if cands, err := c.MergeCandidates(t.Context(), floorHour(f.r.now().Add(-f.r.QuietPeriod).UnixNano())); err != nil || len(cands) != 0 {
					t.Fatalf("candidates %v, %v; want the scope left out", cands, err)
				}
				q := f.requestAt(t, capture, hour)
				cycles := func(want ...int) {
					t.Helper()
					for i, w := range want {
						// An attempt fails at its put, or before it once a plain segment joins the unmergeable scope's masked ones.
						rep, err := f.r.Requests(t.Context(), f.lease)
						if err != nil || rep.Pending != 1 || len(rep.Merges.Failed) != w {
							t.Fatalf("cycle %d of %v = %+v, %v; want %d attempted and failed, and the request pending", i, want, rep, err, w)
						}
					}
				}
				cycles(1, 1, 0, 1, 0)
				f.scope(t, capture, hour, 10)
				cycles(1, 1, 0) // the registration made it due; then its own backoff, unchanged, suppresses it
				catalogtest.ExecSQL(t, c, "ALTER TABLE scopes RENAME TO scopes_gone")
				blind := func() {
					if rep, err := f.r.Requests(t.Context(), f.lease); err != nil || len(rep.Merges.Failed)+len(rep.Merges.Merged) != 0 || rep.Pending != 1 {
						t.Errorf("Requests without the revision = %+v, %v; want nothing attempted and the request pending", rep, err)
					}
				}
				if n := s.attempted(blind); n != 0 {
					t.Errorf("a cycle that could not read the revision attempted %d merges", n)
				}
				catalogtest.ExecSQL(t, c, "ALTER TABLE scopes_gone RENAME TO scopes")
				cycles(1)
				if state, _ := f.status(t, q); state != "pending" {
					t.Errorf("request %s, want pending", state)
				}
			})
		})
	}
}

// Two pending requests and a new one each bypass a scope's wait once: one attempt a cycle for the scope,
// a request attempted for its failure suppressed until the revision moves.
func TestMergeRequestsBypassOnce(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		s := f.refusing(t)
		f.r.Now = func() time.Time { return time.Unix(0, hour*nsPerHour).Add(30 * time.Minute) }
		capture := f.admit(t)
		f.scope(t, capture, hour, 1)
		f.requestAt(t, capture, hour)
		f.requestAt(t, capture, hour)
		cycle := func() int64 {
			t.Helper()

			return s.attempted(func() {
				if _, err := f.r.Requests(t.Context(), f.lease); err != nil {
					t.Fatal(err)
				}
			})
		}
		got := make([]int64, 0, 6)
		for range 4 {
			got = append(got, cycle())
		}
		f.requestAt(t, capture, hour)
		got = append(got, cycle(), cycle())
		// The first request at cycles 0 and 1, its backoff not yet waiting; the second's bypass at 2; the third's at 4.
		if want := []int64{1, 1, 1, 0, 1, 0}; !slices.Equal(got, want) {
			t.Errorf("attempts a cycle %v, want %v", got, want)
		}
	})
}

// 1,025 candidates: a cycle attempts the default cap of 1,024, oldest hour first, and counts none for the scope it leaves,
// which leads the next cycle's scheduled scopes.
func TestMergeCap(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		f.merging(t)
		f.horizon = hour - 2000
		m := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(m); err != nil {
			t.Fatal(err)
		}
		capture := f.admit(t)
		const n = DefaultMergeAttempts + 1
		// Packs no object holds: every attempt fails at opening its input, after its snapshot.
		for i := range int64(n) {
			hr := hour - n + 1 + i
			id := newID(t)
			key, err := tracepack.SegmentKey("", equipment, capture, uint64(i), id) //nolint:gosec // a test's small seq
			if err != nil {
				t.Fatal(err)
			}
			at := hr*nsPerHour + i
			p := &catalog.Pack{
				ID: id, Capture: capture, EquipmentID: equipment, Hour: hr, Role: catalog.RoleSegment, Key: key, Size: 1, SeqStart: uint64(i), FormatMinor: 2, //nolint:gosec // a test's small seq
				Ranges: []catalog.Range{{SeqFrom: uint64(i), SeqTo: uint64(i), TSFrom: at, TSTo: at}}, //nolint:gosec // a test's small seq
			}
			if out, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: &catalog.Evidence{}, HorizonHour: f.horizon}); err != nil || out != catalog.Registered {
				t.Fatalf("RegisterPack = %v, %v", out, err)
			}
		}
		newest := catalog.Scope{Capture: capture, Hour: hour}
		rep := f.merges(t)
		if len(rep.Failed) != DefaultMergeAttempts || slices.Contains(rep.Failed, newest) || len(rep.Cooling)+len(rep.Merged)+len(rep.Stale) != 0 {
			t.Fatalf("first cycle = %d failed, newest among them %v; want the cap's %d, the newest left", len(rep.Failed), slices.Contains(rep.Failed, newest), DefaultMergeAttempts)
		}
		if err := f.r.mergePass(ctx, f.lease); err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf(`tracekeep_compact_merges_total{outcome="failed"} %d`, DefaultMergeAttempts); !strings.Contains(scrape(t, m), want) {
			t.Errorf("/metrics lacks %s: the scope the cap left counts no merge", want)
		}
		// The second cycle, through mergePass, was led by the scope the first left and left the next oldest, which leads the third.
		rep = f.merges(t)
		if left := (catalog.Scope{Capture: capture, Hour: hour - 1}); len(rep.Failed) != DefaultMergeAttempts || rep.Failed[0] != left {
			t.Errorf("third cycle = %d failed, first %v; want the cap's, led by %v which the second left", len(rep.Failed), rep.Failed, left)
		}
	})
}

// With a cap of two and a request every cycle, the request is attempted first, always,
// and the scheduled scopes the cap carries are still drained one a cycle, oldest first.
func TestMergeCapWithRequestsEveryCycle(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		f.r.MergeAttempts = 2
		scheduled, requested := f.admit(t), f.admit(t)
		want := make([]catalog.Scope, 0, 3)
		for i := range int64(3) {
			want = append(want, f.scope(t, scheduled, hour-2+i, uint64(i))) //nolint:gosec // a test's small seq
		}
		for cycle := range 3 {
			f.scope(t, requested, hour+1, uint64(cycle)) //nolint:gosec // a test's small seq
			q := f.requestAt(t, requested, hour+1)
			if err := f.r.mergePass(t.Context(), f.lease); err != nil {
				t.Fatal(err)
			}
			if state, _ := f.status(t, q); state != "done" {
				t.Errorf("cycle %d: request %s, want done", cycle, state)
			}
			for i, s := range want {
				snap, err := c.MergeSnapshot(t.Context(), s)
				if settled := snap.State == "settled"; err != nil || settled != (i <= cycle) {
					t.Errorf("cycle %d: scheduled scope %d %s, %v; want the first %d settled", cycle, i, snap.State, err, cycle+1)
				}
			}
		}
	})
}

// Older scopes found stale at every attempt never starve the rest: the scopes the cap left lead the next cycle.
func TestStaleScopesDoNotStarveTheRest(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		f.r.MergeAttempts = 2
		a, b, cc, d := f.admit(t), f.admit(t), f.admit(t), f.admit(t)
		older := []catalog.Scope{f.scope(t, a, hour-1, 0), f.scope(t, b, hour-1, 0)}
		newer := []catalog.Scope{f.scope(t, cc, hour, 0), f.scope(t, d, hour, 0)}
		seq := uint64(0)
		// Every put registers into both older scopes, so an older scope's commit is always stale.
		f.r.hooks[hookMergePut] = func() {
			seq++
			f.scope(t, a, hour-1, seq)
			f.scope(t, b, hour-1, seq)
		}
		if rep := f.merges(t); len(rep.Stale) != 2 || len(rep.Merged) != 0 {
			t.Fatalf("first cycle = %+v, want the older two stale", rep)
		}
		rep := f.merges(t)
		slices.SortFunc(rep.Merged, func(x, y catalog.Scope) int { return strings.Compare(x.Capture.String(), y.Capture.String()) })
		slices.SortFunc(newer, func(x, y catalog.Scope) int { return strings.Compare(x.Capture.String(), y.Capture.String()) })
		if !slices.Equal(rep.Merged, newer) {
			t.Errorf("second cycle merged %v, want the newer %v the first cycle's cap left; older %v", rep.Merged, newer, older)
		}
	})
}

// The backoff is this process's memory: kept across cycles at the default interval with the lease lapsing between them,
// and started over when another holder took and released the lease between two of its takes, or when the process restarts.
func TestMergeBackoffAcrossLeaseTakes(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		s := f.refusing(t)
		if err := c.ReleaseLease(ctx, f.lease); err != nil {
			t.Fatal(err)
		}
		f.r.Holder, f.r.LeaseTTL = "a", time.Minute
		f.scope(t, f.admit(t), hour, 1)
		// One cycle: the lease taken, the merge pass, then the lease lapses before the next, as at the defaults.
		cycle := func(r *Compactor) int64 {
			t.Helper()

			return s.attempted(func() {
				if !r.hold(ctx) {
					t.Fatal("the lease was not taken")
				}
				if err := r.mergePass(ctx, *r.lease); err != nil {
					t.Fatal(err)
				}
				if err := c.ReleaseLease(ctx, *r.lease); err != nil {
					t.Fatal(err)
				}
			})
		}
		cycles := func(r *Compactor, want ...int64) {
			t.Helper()
			got := make([]int64, 0, len(want))
			for range want {
				got = append(got, cycle(r))
			}
			if !slices.Equal(got, want) {
				t.Errorf("attempts a cycle %v, want %v", got, want)
			}
		}
		cycles(f.r, 1, 1, 0, 1, 0)
		other, ok, err := c.TakeLease(ctx, leaseName, "b", time.Minute)
		if err != nil || !ok {
			t.Fatalf("TakeLease = %v, %v", ok, err)
		}
		if err := c.ReleaseLease(ctx, other); err != nil {
			t.Fatal(err)
		}
		cycles(f.r, 1, 1, 0, 1, 0) // started over at one cycle
		restarted := &Compactor{
			Catalog: c, Store: s, HorizonHour: f.r.HorizonHour, Log: f.r.Log, Holder: "a", LeaseTTL: time.Minute, Interval: f.r.Interval,
			WorkDir: f.r.WorkDir, Writer: f.r.Writer, QuietPeriod: f.r.QuietPeriod, Now: f.r.Now,
		}
		cycles(restarted, 1, 1, 0)
	})
}

// The cap bounds merges alone: once a merge request spent it, a close request is still executed,
// and the close pass, deletion and retention still run, while the scheduled scope and a second merge request wait for a cycle with attempts left.
func TestMergeCapSpentBeforeTheOtherTasks(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		f.retaining(t)
		// The catalog's clock dates a merge, which D counts from, so the compactor's runs near it.
		now := time.Now()
		h := floorHour(now.UnixNano())
		f.closing(t, now)
		f.r.MergeAttempts, f.r.Retention, f.r.DeleteDelay, f.horizon = 1, 24*time.Hour, time.Hour, h-100
		evicted := f.scope(t, f.admit(t), h-48, 0)
		merged := f.admit(t)
		f.scope(t, merged, h-2, 0)
		f.scope(t, merged, h-2, 1)
		// Two scopes of the current hour, not yet quiet, each requested.
		requested, again := f.admit(t), f.admit(t)
		f.scope(t, requested, h, 0)
		f.scope(t, again, h, 0)
		first := f.requestAt(t, requested, h)
		second := f.requestAt(t, again, h)
		closing := f.described(t, newID(t), now.UnixNano())
		f.stage(t, segmentOf(t, closing, h, recordsAt(h, 0), noAdjust))
		closeReq, err := c.RequestCompaction(ctx, catalog.RequestClose, closing, nil, "op")
		if err != nil {
			t.Fatal(err)
		}
		cycle := func() (CloseReport, RetentionReport) {
			t.Helper()
			if err := f.r.mergePass(ctx, f.lease); err != nil {
				t.Fatal(err)
			}
			closes, err := f.r.Closes(ctx, f.lease)
			if err != nil {
				t.Fatal(err)
			}
			var ret RetentionReport
			if err := f.r.Deletions(ctx, f.lease, &ret); err != nil {
				t.Fatal(err)
			}
			if err := f.r.Retain(ctx, f.lease, &ret); err != nil {
				t.Fatal(err)
			}

			return closes, ret
		}
		closes, ret := cycle()
		for id, want := range map[tracepack.UUID]string{first: "done", second: "pending", closeReq: "done"} {
			if state, _ := f.status(t, id); state != want {
				t.Errorf("request %v = %s, want %s", id, state, want)
			}
		}
		if got := f.mustState(t, closing); got != catalog.StateClosed || closes.Published == 0 {
			t.Errorf("Closes = %+v, the requested capture %s; want its close published", closes, got)
		}
		if snap, err := c.MergeSnapshot(ctx, evicted); err != nil || len(snap.Inputs) != 0 || ret.Evicted == 0 {
			t.Errorf("retention = %+v, the old scope %+v, %v; want its hour evicted", ret, snap, err)
		}
		scheduled := catalog.Scope{Capture: merged, Hour: h - 2}
		if snap, err := c.MergeSnapshot(ctx, scheduled); err != nil || snap.State == "settled" {
			t.Errorf("the scheduled scope = %+v, %v; want it left by the spent cap", snap, err)
		}
		// Past D, the next cycle's cap goes to the second request, the scheduled scope still waiting, and deletion still runs.
		f.r.Now = func() time.Time { return now.Add(2 * time.Hour) }
		_, ret = cycle()
		if state, _ := f.status(t, second); state != "done" {
			t.Errorf("the second request = %s, want done", state)
		}
		if ret.Deleted == 0 {
			t.Errorf("deletion = %+v, want the first request's merged inputs deleted", ret)
		}
		if snap, err := c.MergeSnapshot(ctx, scheduled); err != nil || snap.State == "settled" {
			t.Errorf("the scheduled scope = %+v, %v; want it still left by the spent cap", snap, err)
		}
	})
}
