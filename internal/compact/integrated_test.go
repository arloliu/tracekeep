package compact

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
)

// reconcile runs, under lease, the passes of a cycle that need no bus, in the cycle's order:
// the orphan pass, the conflict pass, the merge pass, the close pass, deletion and retention.
// It returns the orphan pass's report, and the first error, which ends the cycle as it ends Step's.
func (f *fixture) reconcile(t *testing.T, lease catalog.Lease) (OrphanReport, error) {
	t.Helper()
	ctx := t.Context()
	rep, err := f.r.Orphans(ctx, lease)
	if err != nil {
		return rep, err
	}
	if _, err := f.r.Conflicts(ctx, lease); err != nil {
		return rep, err
	}
	if err := f.r.mergePass(ctx, lease); err != nil {
		return rep, err
	}
	if err := f.r.closePass(ctx, lease); err != nil {
		return rep, err
	}

	return rep, f.r.retentionPass(ctx, lease)
}

// reconciled runs reconcile under f's lease, failing the test on an error, and returns the merges it attempted, counted by s.
func (f *fixture) reconciled(t *testing.T, s *puts) int64 {
	t.Helper()

	return s.attempted(func() {
		if _, err := f.reconcile(t, f.lease); err != nil {
			t.Fatal(err)
		}
	})
}

// An orphan adopted into a scope waiting out a failure's backoff is a registration after the failed snapshot:
// the same cycle's merge pass, which follows the orphan pass, attempts the scope at once.
func TestOrphanAdoptionEndsABackoff(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		s := f.refusing(t)
		f.retaining(t)
		f.r.Now = func() time.Time { return time.Unix(0, (hour+2)*nsPerHour) }
		capture := f.admit(t)
		f.scope(t, capture, hour, 1)
		cycles := func(want ...int64) {
			t.Helper()
			for i, w := range want {
				if n := f.reconciled(t, s); n != w {
					t.Fatalf("cycle %d of %v attempted %d merges, want %d", i, want, n, w)
				}
			}
		}
		cycles(1, 1, 0, 1, 0) // waiting four cycles from the third failure
		orphan := segmentOf(t, capture, hour, recordsAt(hour, 7), noAdjust)
		f.put(t, orphan.key, orphan.data)
		cycles(0) // the orphan's first sighting; the scope still waits
		var rep OrphanReport
		adopting := func() {
			var err error
			if rep, err = f.reconcile(t, f.lease); err != nil {
				t.Fatal(err)
			}
		}
		if n := s.attempted(adopting); n != 1 || !slices.Equal(rep.Adopted, []string{orphan.key}) {
			t.Errorf("the cycle adopting the orphan = %v adopted, %d merges attempted; want it adopted and the scope attempted at once", rep.Adopted, n)
		}
	})
}

// A lease lost during a cycle's orphan pass ends the cycle before the merge pass: nothing of the chunk is acted on and no backoff moves.
// The process's next take, after another holder's, starts the backoff over, and the orphan pass under it settles what the lost one left.
func TestLeaseLostAcrossTheOrphanAndMergePasses(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		s := f.refusing(t)
		f.retaining(t)
		f.r.Now = func() time.Time { return time.Unix(0, (hour+2)*nsPerHour) }
		if err := c.ReleaseLease(ctx, f.lease); err != nil {
			t.Fatal(err)
		}
		f.r.Holder, f.r.LeaseTTL = "a", time.Hour
		capture := f.admit(t)
		f.scope(t, capture, hour, 1)
		cycle := func() (int64, error) {
			t.Helper()
			var err error
			n := s.attempted(func() {
				if !f.r.hold(ctx) {
					t.Fatal("the lease was not held")
				}
				_, err = f.reconcile(t, *f.r.lease)
			})

			return n, err
		}
		for i, want := range []int64{1, 1, 0} {
			if n, err := cycle(); err != nil || n != want {
				t.Fatalf("cycle %d attempted %d, %v; want %d", i, n, err, want)
			}
		}
		gone := segment(t, capture, []uint64{2}, false)
		tombstone(t, c, capture, gone)
		f.put(t, gone.key, gone.data)
		var other catalog.Lease
		f.r.hooks[hookStandings] = func() {
			f.r.hooks[hookStandings] = nil
			if err := c.ReleaseLease(ctx, *f.r.lease); err != nil {
				t.Fatal(err)
			}
			l, ok, err := c.TakeLease(ctx, leaseName, "b", time.Hour)
			if err != nil || !ok {
				t.Fatalf("TakeLease = %v, %v", ok, err)
			}
			other = l
		}
		if n, err := cycle(); !errors.Is(err, catalog.ErrLeaseLost) || n != 0 {
			t.Fatalf("the cycle losing its lease = %d attempted, %v; want none and ErrLeaseLost", n, err)
		}
		if !f.present(t, gone.key) {
			t.Errorf("the stale holder deleted the tombstoned key")
		}
		if err := c.ReleaseLease(ctx, other); err != nil {
			t.Fatal(err)
		}
		// The scope would wait two more cycles; another holder came between, so the backoff starts over.
		if n, err := cycle(); err != nil || n != 1 || f.present(t, gone.key) {
			t.Errorf("the next holder's cycle = %d attempted, %v, tombstoned key stored %v; want the scope attempted and the key deleted", n, err, f.present(t, gone.key))
		}
	})
}

// Retention running beside a failing scope: once retention evicts the scope's hour the merge pass neither attempts nor reports it,
// so the merge task succeeds again, and the orphan pass deletes a delayed object of the evicted hour.
func TestRetentionEvictsAFailingScope(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		s := f.refusing(t)
		f.retaining(t)
		f.r.Retention = 24 * time.Hour
		m := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(m); err != nil {
			t.Fatal(err)
		}
		at := func(h int64) { f.r.Now = func() time.Time { return time.Unix(0, h*nsPerHour) } }
		at(hour + 2)
		capture := f.admit(t)
		scope := f.scope(t, capture, hour, 1)
		for i, want := range []int64{1, 1, 0} {
			if n := f.reconciled(t, s); n != want {
				t.Fatalf("cycle %d attempted %d, want %d", i, n, want)
			}
		}
		if strings.Contains(scrape(t, m), `tracekeep_compact_last_success_timestamp{task="merge"}`) {
			t.Fatalf("the merge task succeeded while its scope failed or waited")
		}
		late := segmentOf(t, capture, hour, recordsAt(hour, 9), noAdjust)
		f.put(t, late.key, late.data)
		at(hour + 30)
		f.horizon = hour + 6
		// The merge pass precedes retention in a cycle, so the evicting cycle attempts the scope, now due, once more.
		if n := f.reconciled(t, s); n != 1 {
			t.Errorf("the evicting cycle attempted %d merges, want the due scope's one", n)
		}
		if snap, err := c.MergeSnapshot(t.Context(), scope); err != nil || len(snap.Inputs) != 0 {
			t.Errorf("the scope after retention = %+v, %v; want its hour evicted", snap, err)
		}
		for i := range 4 {
			if n := f.reconciled(t, s); n != 0 {
				t.Errorf("cycle %d after the eviction attempted %d merges, want none", i, n)
			}
		}
		if f.present(t, late.key) {
			t.Errorf("the delayed object of the evicted hour is still stored")
		}
		if !strings.Contains(scrape(t, m), `tracekeep_compact_last_success_timestamp{task="merge"}`) {
			t.Errorf("the merge task never succeeded once retention evicted its failing scope")
		}
	})
}
