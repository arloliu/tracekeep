package compact

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

// merging readies f's compactor for the merge pass, at a time when hour has ended and its quiet period passed.
func (f *fixture) merging(t *testing.T) {
	t.Helper()
	f.r.WorkDir, f.r.Writer, f.r.QuietPeriod = t.TempDir(), "tracekeep", 15*time.Minute
	f.r.Now = func() time.Time { return time.Unix(0, (hour+2)*nsPerHour) }
}

// stage puts b and registers it as ingest would, with (empty) evidence.
func (f *fixture) stage(t *testing.T, b built) *catalog.Pack {
	t.Helper()
	f.put(t, b.key, b.data)
	p, reason, err := f.r.read(t.Context(), store.ObjectInfo{Key: b.key, Size: int64(len(b.data))})
	if err != nil || reason != "" {
		t.Fatalf("read = %q, %v", reason, err)
	}
	out, err := f.c.RegisterPack(t.Context(), p, catalog.Registration{Evidence: &catalog.Evidence{}, HorizonHour: f.horizon})
	if err != nil || out != catalog.Registered {
		t.Fatalf("RegisterPack = %v, %v", out, err)
	}

	return p
}

func (f *fixture) merges(t *testing.T) MergeReport {
	t.Helper()
	rep, err := f.r.Merges(t.Context(), f.lease)
	if err != nil {
		t.Fatal(err)
	}

	return rep
}

// archives lists the archive keys in the store.
func (f *fixture) archives(t *testing.T) []string {
	t.Helper()
	var keys []string
	err := f.s.List(t.Context(), "archive/", func(info store.ObjectInfo) error {
		keys = append(keys, info.Key)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)

	return keys
}

func (f *fixture) candidates(t *testing.T) []catalog.Scope {
	t.Helper()
	got, err := f.c.MergeCandidates(t.Context(), hour+1)
	if err != nil {
		t.Fatal(err)
	}

	return got
}

func TestMergeSettlesAScope(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		f.stage(t, segment(t, capture, []uint64{0, 1, 2, 3, 4}, false))
		f.stage(t, segment(t, capture, []uint64{5, 6, 7, 8, 9}, false))
		scope := catalog.Scope{Capture: capture, Hour: hour}
		if rep := f.merges(t); !slices.Equal(rep.Merged, []catalog.Scope{scope}) {
			t.Fatalf("Merges = %+v, want the scope merged", rep)
		}
		keys := f.archives(t)
		if len(keys) != 1 {
			t.Fatalf("archives = %v, want one", keys)
		}
		if got, err := c.KeyStanding(t.Context(), keys[0]); err != nil || got != catalog.KeyRegistered {
			t.Errorf("the archive's standing = %v, %v; want registered", got, err)
		}
		if got := f.candidates(t); len(got) != 0 {
			t.Errorf("candidates after the merge = %v, want none", got)
		}
		snap, err := c.MergeSnapshot(t.Context(), scope)
		if err != nil || len(snap.Inputs) != 1 || snap.Inputs[0].Key != keys[0] || snap.Inputs[0].Role != catalog.RoleArchive {
			t.Fatalf("snapshot after the merge = %+v, %v; want the archive the one staged pack", snap, err)
		}
		// The archive holds every record once, and carries the scope's generation, the lease's epoch and the service's writer.
		obj, err := f.s.Open(t.Context(), keys[0])
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()
		rd, err := tracepack.Open(t.Context(), obj, obj.Size(), tracepack.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		m := rd.Header().Meta
		if m.ScopeGeneration == nil || *m.ScopeGeneration != uint64(snap.Revision) {
			t.Errorf("scope generation = %v, want the revision the commit gave the scope, %d", m.ScopeGeneration, snap.Revision)
		}
		if m.PublisherEpoch == nil || *m.PublisherEpoch != uint64(f.lease.Fence) || m.Writer != "tracekeep" {
			t.Errorf("publisher epoch %v, writer %q; want the fence %d and tracekeep", m.PublisherEpoch, m.Writer, f.lease.Fence)
		}
		p, reason, err := f.r.read(t.Context(), store.ObjectInfo{Key: keys[0], Size: obj.Size()})
		if err != nil || reason != "" || len(p.Ranges) != 1 || p.Ranges[0].SeqFrom != 0 || p.Ranges[0].SeqTo != 9 {
			t.Errorf("the archive's pack = %+v, %q, %v; want seqs 0-9", p, reason, err)
		}
	})
}

func TestMergeWaitsForTheQuietPeriod(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		f.r.Now = func() time.Time { return time.Unix(0, (hour+1)*nsPerHour).Add(10 * time.Minute) }
		f.stage(t, segment(t, f.admit(t), []uint64{0}, false))
		if rep := f.merges(t); len(rep.Merged) != 0 || len(f.archives(t)) != 0 {
			t.Errorf("Merges inside the quiet period = %+v, want nothing", rep)
		}
	})
}

// A registration between the snapshot and the commit fails the commit, in an open scope and in one a merge's later segment reopened
// (gate case 2); the archive is left to the reconciler, and the next pass merges the scope with the late segment.
func TestMergeIsStaleAfterARegistration(t *testing.T) {
	t.Parallel()
	for _, reopened := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "reopened"}[reopened], func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				f.merging(t)
				capture := f.admit(t)
				scope := catalog.Scope{Capture: capture, Hour: hour}
				f.stage(t, segment(t, capture, []uint64{0, 1}, false))
				last := uint64(3)
				if reopened {
					if rep := f.merges(t); !slices.Equal(rep.Merged, []catalog.Scope{scope}) {
						t.Fatalf("the first Merges = %+v", rep)
					}
					f.stage(t, segment(t, capture, []uint64{2, 3}, false))
					if snap, err := c.MergeSnapshot(t.Context(), scope); err != nil || snap.State != "reopened" {
						t.Fatalf("scope = %+v, %v; want reopened", snap, err)
					}
					last = 5
				}
				late := segment(t, capture, []uint64{last - 1, last}, false)
				f.r.hooks[hookMergePut] = func() {
					f.r.hooks[hookMergePut] = nil
					f.stage(t, late)
				}
				before := len(f.archives(t))
				rep := f.merges(t)
				if !slices.Equal(rep.Stale, []catalog.Scope{scope}) || len(rep.Merged) != 0 {
					t.Fatalf("Merges = %+v, want the scope stale", rep)
				}
				left := f.archives(t)
				if len(left) != before+1 {
					t.Fatalf("archives = %v, want the stale one left", left)
				}
				unknown := 0
				for _, k := range left {
					if got, _ := c.KeyStanding(t.Context(), k); got == catalog.KeyUnknown {
						unknown++
					}
				}
				if unknown != 1 {
					t.Errorf("%d archives of unknown standing, want the stale one's: the reconciler decides", unknown)
				}
				if rep := f.merges(t); !slices.Equal(rep.Merged, []catalog.Scope{scope}) {
					t.Fatalf("the next Merges = %+v, want the scope merged", rep)
				}
				snap, err := c.MergeSnapshot(t.Context(), scope)
				if err != nil || len(snap.Inputs) != 1 {
					t.Fatalf("snapshot = %+v, %v", snap, err)
				}
				p, _, err := f.r.read(t.Context(), store.ObjectInfo{Key: snap.Inputs[0].Key, Size: snap.Inputs[0].Size})
				if err != nil || len(p.Ranges) != 1 || p.Ranges[0].SeqFrom != 0 || p.Ranges[0].SeqTo != last {
					t.Errorf("the archive's pack = %+v, %v; want seqs 0-%d", p, err, last)
				}
			})
		})
	}
}

// A merge that fails leaves the scope as it was and puts no archive: a damaged input, and a store whose read fails.
func TestMergeFailureLeavesTheScope(t *testing.T) {
	t.Parallel()
	cases := map[string]func(t *testing.T, f *fixture, p *catalog.Pack){
		"a damaged input": func(t *testing.T, f *fixture, p *catalog.Pack) {
			t.Helper()
			if err := f.s.Delete(t.Context(), p.Key); err != nil {
				t.Fatal(err)
			}
			f.put(t, p.Key, bytes.Repeat([]byte{1}, int(p.Size)))
		},
		"a failed read": func(t *testing.T, f *fixture, p *catalog.Pack) {
			t.Helper()
			f.r.Store = failingOpen{f.s, p.Key}
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				f.merging(t)
				capture := f.admit(t)
				f.stage(t, segment(t, capture, []uint64{0, 1}, false))
				p := f.stage(t, segment(t, capture, []uint64{2, 3}, false))
				damage(t, f, p)
				scope := catalog.Scope{Capture: capture, Hour: hour}
				if rep := f.merges(t); !slices.Equal(rep.Failed, []catalog.Scope{scope}) || len(rep.Merged) != 0 {
					t.Fatalf("Merges = %+v, want the scope failed", rep)
				}
				if keys := f.archives(t); len(keys) != 0 {
					t.Errorf("archives = %v, want none", keys)
				}
				if got := f.candidates(t); !slices.Equal(got, []catalog.Scope{scope}) {
					t.Errorf("candidates = %v, want the scope still one", got)
				}
			})
		})
	}
}

// failingOpen is a store whose Open of one key fails.
type failingOpen struct {
	*store.Mem
	key string
}

func (s failingOpen) Open(ctx context.Context, key string) (store.Object, error) {
	if key == s.key {
		return nil, errors.New("open refused")
	}

	return s.Mem.Open(ctx, key)
}

// A worker that lost its lease commits nothing and ends the pass; its archive is left to the reconciler.
func TestMergeUnderALostLease(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		f.stage(t, segment(t, capture, []uint64{0, 1}, false))
		f.r.hooks[hookMergePut] = func() {
			if err := c.ReleaseLease(t.Context(), f.lease); err != nil {
				t.Error(err)
			}
			if _, ok, err := c.TakeLease(t.Context(), "compactor", "another", time.Hour); err != nil || !ok {
				t.Errorf("TakeLease = %v, %v", ok, err)
			}
		}
		if _, err := f.r.Merges(t.Context(), f.lease); !errors.Is(err, catalog.ErrLeaseLost) {
			t.Fatalf("Merges = %v, want ErrLeaseLost", err)
		}
		keys := f.archives(t)
		if len(keys) != 1 || !strings.Contains(keys[0], capture.String()) {
			t.Fatalf("archives = %v, want the put one left", keys)
		}
		if got, _ := c.KeyStanding(t.Context(), keys[0]); got != catalog.KeyUnknown {
			t.Errorf("standing = %v, want unknown", got)
		}
		if got := f.candidates(t); len(got) != 1 {
			t.Errorf("candidates = %v, want the scope unmerged", got)
		}
	})
}

// The merge pass counts its outcomes, times its cycle and reads the scope gauges.
func TestMergePassMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		s := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(s); err != nil {
			t.Fatal(err)
		}
		f.stage(t, segment(t, f.admit(t), []uint64{0, 1}, false))
		f.stage(t, segment(t, f.admit(t), []uint64{0, 1}, false))
		if err := f.r.mergePass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		got := scrape(t, s)
		for _, want := range []string{
			`tracekeep_compact_merges_total{outcome="merged"} 2`, `tracekeep_compact_cycle_seconds_count{task="merge"} 1`,
			`tracekeep_compact_last_success_timestamp{task="merge"}`, `tracekeep_compact_scopes{state="settled"} 2`,
			`tracekeep_compact_scopes{state="open"} 0`, "tracekeep_compact_conflicts_open 0",
			"tracekeep_compact_oldest_unmerged_scope_age_seconds 0",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("/metrics lacks %s", want)
			}
		}
	})
}

// A requested merge counts as a merge of the task, and a pass whose every attempt failed stamps no success.
func TestRequestedAndFailedMergeMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		s := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(s); err != nil {
			t.Fatal(err)
		}
		// Inside the quiet period: only the request merges.
		f.r.Now = func() time.Time { return time.Unix(0, (hour+1)*nsPerHour) }
		capture := f.admit(t)
		f.stage(t, segment(t, capture, []uint64{0, 1}, false))
		h := hour
		if _, err := c.RequestCompaction(t.Context(), catalog.RequestMerge, capture, &h, "op"); err != nil {
			t.Fatal(err)
		}
		if err := f.r.mergePass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		got := scrape(t, s)
		for _, want := range []string{`tracekeep_compact_merges_total{outcome="merged"} 1`, `tracekeep_compact_last_success_timestamp{task="merge"}`} {
			if !strings.Contains(got, want) {
				t.Errorf("after a requested merge /metrics lacks %s", want)
			}
		}

		g := metrics.NewServer()
		if f.r.Metrics, err = metrics.NewCompact(g); err != nil {
			t.Fatal(err)
		}
		f.r.Now = func() time.Time { return time.Unix(0, (hour+2)*nsPerHour) }
		p := f.stage(t, segment(t, f.admit(t), []uint64{0, 1}, false))
		f.r.Store = failingOpen{f.s, p.Key}
		if err := f.r.mergePass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		got = scrape(t, g)
		if !strings.Contains(got, `tracekeep_compact_merges_total{outcome="failed"} 1`) || !strings.Contains(got, `cycle_seconds_count{task="merge"} 1`) {
			t.Errorf("an all-failed pass is not counted:\n%s", got)
		}
		if strings.Contains(got, `last_success_timestamp{task="merge"}`) {
			t.Error("an all-failed pass stamped a success")
		}

		// A merge that fails on conflicting records failed too.
		k := metrics.NewServer()
		if f.r.Metrics, err = metrics.NewCompact(k); err != nil {
			t.Fatal(err)
		}
		f.r.Store = f.s
		if err := c.ReleaseLease(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		conflicting := f.admit(t)
		f.stage(t, segmentOf(t, conflicting, hour, recordsAt(hour, 0, 1, 2), noAdjust))
		changed := recordsAt(hour, 2, 3)
		changed[0].Payload = append([]byte(nil), changed[0].Payload...)
		changed[0].Payload[0] ^= 0xff
		b := segmentOf(t, conflicting, hour, changed, noAdjust)
		f.put(t, b.key, b.data)
		q, _, err := f.r.read(t.Context(), store.ObjectInfo{Key: b.key, Size: int64(len(b.data))})
		if err != nil {
			t.Fatal(err)
		}
		q.Ranges = []catalog.Range{{SeqFrom: 3, SeqTo: 3, TSFrom: q.Ranges[0].TSTo, TSTo: q.Ranges[0].TSTo}}
		if out, err := c.RegisterPack(t.Context(), q, catalog.Registration{Evidence: &catalog.Evidence{}, HorizonHour: f.horizon}); err != nil || out != catalog.Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		if f.lease, _, err = c.TakeLease(t.Context(), "compactor", "again", time.Hour); err != nil {
			t.Fatal(err)
		}
		// The scope of the failed read is merged by this pass too; it is the conflicting one that fails the task.
		if err := f.r.mergePass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		got = scrape(t, k)
		if !strings.Contains(got, `tracekeep_compact_merges_total{outcome="conflicted"} 1`) || strings.Contains(got, `last_success_timestamp{task="merge"}`) {
			t.Errorf("a pass with a conflicted merge stamped a success, or did not count it:\n%s", got)
		}
	})
}

// At start the compactor removes the merges' and closes' working files a crash left, and nothing else.
func TestCleanWorkDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, name := range []string{"archive-1.tpk", "close-1.tpk", "segment-1.tpk", "other"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := CleanWorkDir(dir); err != nil {
		t.Fatal(err)
	}
	left, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{filepath.Join(dir, "other"), filepath.Join(dir, "segment-1.tpk")}; !slices.Equal(left, want) {
		t.Errorf("left %v, want %v", left, want)
	}
}
