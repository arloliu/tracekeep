package compact

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// cycle runs what a compactor cycle runs on the catalog and the store: the orphan pass, the conflict pass and the merge pass.
func (f *fixture) cycle(t *testing.T) (ConflictReport, MergeReport) {
	t.Helper()
	f.pass(t)

	return f.conflicts(t), f.merges(t)
}

// settledArchive is the archive the settled scope's snapshot names, read back as a pack.
func (f *fixture) settledArchive(t *testing.T, scope catalog.Scope) *catalog.Pack {
	t.Helper()
	snap, err := f.c.MergeSnapshot(t.Context(), scope)
	if err != nil || snap.State != "settled" || len(snap.Inputs) != 1 || snap.Inputs[0].Role != catalog.RoleArchive {
		t.Fatalf("snapshot = %+v, %v; want a settled scope with its archive", snap, err)
	}
	p, reason, err := f.r.read(t.Context(), store.ObjectInfo{Key: snap.Inputs[0].Key, Size: snap.Inputs[0].Size})
	if err != nil || reason != "" {
		t.Fatalf("read the archive = %q, %v", reason, err)
	}

	return p
}

// recordCount reads the number of records the pack under key holds.
func recordCount(t *testing.T, f *fixture, key string) int {
	t.Helper()
	obj, err := f.s.Open(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	rd, err := tracepack.Open(t.Context(), obj, obj.Size(), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	if _, err := rd.Iterate(t.Context(), tracepack.Query{}, func(*tracepack.Item) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}

	return n
}

// The reconciler adopts the archive before the merge commits: the adopted rows stay and the commit adds nothing;
// the next cycle clears the adoption's suspects and merges the scope, the adopted archive an input, each record once.
func TestArchiveAdoptedBeforeTheCommit(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		f.stage(t, segment(t, capture, []uint64{0, 1, 2}, false))
		f.stage(t, segment(t, capture, []uint64{3, 4}, false))
		f.r.hooks[hookMergePut] = func() {
			f.r.hooks[hookMergePut] = nil
			f.pass(t)
			if rep := f.pass(t); len(rep.Adopted) != 1 {
				t.Errorf("the reconciler's pass = %+v, want the archive adopted", rep)
			}
		}
		scope := catalog.Scope{Capture: capture, Hour: hour}
		if rep := f.merges(t); !slices.Equal(rep.Stale, []catalog.Scope{scope}) {
			t.Fatalf("Merges = %+v, want stale", rep)
		}
		if conflicts, merges := f.cycle(t); conflicts.Cleared == 0 || !slices.Equal(merges.Merged, []catalog.Scope{scope}) {
			t.Fatalf("the next cycle = %+v, %+v; want the suspects cleared and the scope merged", conflicts, merges)
		}
		a := f.settledArchive(t, scope)
		if !slices.Equal(a.Ranges, []catalog.Range{{SeqFrom: 0, SeqTo: 4, TSFrom: hour * nsPerHour, TSTo: hour*nsPerHour + 4}}) || recordCount(t, f, a.Key) != 5 {
			t.Errorf("the archive = %+v, want seqs 0-4 once each", a.Ranges)
		}
	})
}

// After a merge commits, the reconciler finds its archive registered and changes nothing.
func TestReconcilerLeavesACommittedArchive(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		f.stage(t, segment(t, f.admit(t), []uint64{0, 1}, false))
		if rep := f.merges(t); len(rep.Merged) != 1 {
			t.Fatalf("Merges = %+v", rep)
		}
		for range 2 {
			if rep := f.pass(t); len(rep.Adopted)+len(rep.Waiting)+len(rep.Deleted)+len(rep.Quarantined) != 0 {
				t.Errorf("a pass after the merge = %+v, want nothing", rep)
			}
		}
	})
}

// A crash after the archive's put: a new lease holder adopts the orphan two passes later, clears its suspects
// and a fresh merge settles each record once; under a termination order the orphan is tombstoned instead and the inputs stay.
func TestCrashAfterTheArchivePut(t *testing.T) {
	t.Parallel()
	for _, terminated := range []bool{false, true} {
		catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
			f := newFixture(t, c)
			f.merging(t)
			capture := f.admit(t)
			in := f.stage(t, segment(t, capture, []uint64{0, 1, 2}, false))
			ctx, crash := context.WithCancel(t.Context())
			f.r.hooks[hookMergePut] = func() { crash() }
			if _, err := f.r.Merges(ctx, f.lease); !errors.Is(err, context.Canceled) {
				t.Fatalf("Merges = %v, want the crash", err)
			}
			f.r.hooks[hookMergePut] = nil
			orphan := f.archives(t)
			if len(orphan) != 1 {
				t.Fatalf("archives = %v, want the orphan", orphan)
			}
			// A new holder takes the lease.
			if err := c.ReleaseLease(t.Context(), f.lease); err != nil {
				t.Fatal(err)
			}
			l, ok, err := c.TakeLease(t.Context(), "compactor/2", "next", time.Hour)
			if err != nil || !ok {
				t.Fatalf("TakeLease = %v, %v", ok, err)
			}
			f.lease, f.r.unknown = l, nil
			if terminated {
				if _, err := c.TerminateCapture(t.Context(), capture, 1, 0, "op", "a1"); err != nil {
					t.Fatal(err)
				}
			}
			f.pass(t)
			rep := f.pass(t)
			if terminated {
				if !slices.Equal(rep.Tombstoned, orphan) || f.present(t, orphan[0]) {
					t.Fatalf("pass = %+v, want the orphan tombstoned and deleted", rep)
				}
				if got, _ := c.KeyStanding(t.Context(), in.Key); got != catalog.KeyRegistered {
					t.Errorf("the input's standing = %v, want still registered", got)
				}

				return
			}
			if !slices.Equal(rep.Adopted, orphan) {
				t.Fatalf("pass = %+v, want the orphan adopted", rep)
			}
			scope := catalog.Scope{Capture: capture, Hour: hour}
			if conflicts, merges := f.cycle(t); conflicts.Cleared == 0 || !slices.Equal(merges.Merged, []catalog.Scope{scope}) {
				t.Fatalf("the next cycle = %+v, %+v; want the scope merged", conflicts, merges)
			}
			if a := f.settledArchive(t, scope); recordCount(t, f, a.Key) != 3 {
				t.Errorf("the archive holds %d records, want 3", recordCount(t, f, a.Key))
			}
		})
	}
}

// A staged archive merges again with a late segment: ranges, endpoint times and losses kept.
func TestArchiveMergedAgain(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		two, ts := uint64(2), hour*nsPerHour+2
		f.stage(t, writeSegment(t, capture, []uint64{0, 1, 3}, func(m *tracepack.PackMeta) {
			m.Coverage = []tracepack.Coverage{{CaptureID: &capture, SeqFirst: &two, SeqLast: &two, TimeStart: &ts, TimeEnd: &ts}}
		}))
		scope := catalog.Scope{Capture: capture, Hour: hour}
		if rep := f.merges(t); len(rep.Merged) != 1 {
			t.Fatalf("Merges = %+v", rep)
		}
		f.stage(t, segment(t, capture, []uint64{5, 6}, false))
		if rep := f.merges(t); !slices.Equal(rep.Merged, []catalog.Scope{scope}) {
			t.Fatalf("Merges after a late segment = %+v", rep)
		}
		a := f.settledArchive(t, scope)
		at := func(seq int64) int64 { return hour*nsPerHour + seq }
		want := []catalog.Range{{SeqFrom: 0, SeqTo: 1, TSFrom: at(0), TSTo: at(1)}, {SeqFrom: 3, SeqTo: 3, TSFrom: at(3), TSTo: at(3)}, {SeqFrom: 5, SeqTo: 6, TSFrom: at(5), TSTo: at(6)}}
		if !slices.Equal(a.Ranges, want) {
			t.Errorf("ranges %+v, want %+v", a.Ranges, want)
		}
		if len(a.Losses) != 1 || *a.Losses[0].SeqFrom != 2 || *a.Losses[0].TSFrom != ts {
			t.Errorf("losses %+v, want seq 2's declared loss carried", a.Losses)
		}
	})
}

// writeSegment writes a segment of capture holding seqs, its metadata adjusted by adjust.
func writeSegment(t *testing.T, capture tracepack.UUID, seqs []uint64, adjust func(*tracepack.PackMeta)) built {
	t.Helper()

	return segmentOf(t, capture, hour, recordsAt(hour, seqs...), adjust)
}

// A put that fails leaves the scope as it was; one that became visible despite its error leaves its archive to the reconciler.
func TestMergePutFailure(t *testing.T) {
	t.Parallel()
	for _, visible := range []bool{false, true} {
		catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
			f := newFixture(t, c)
			f.merging(t)
			capture := f.admit(t)
			f.stage(t, segment(t, capture, []uint64{0, 1}, false))
			f.r.Store = failingPut{Mem: f.s, visible: visible}
			scope := catalog.Scope{Capture: capture, Hour: hour}
			if rep := f.merges(t); !slices.Equal(rep.Failed, []catalog.Scope{scope}) {
				t.Fatalf("Merges = %+v, want failed", rep)
			}
			if got := f.candidates(t); !slices.Equal(got, []catalog.Scope{scope}) {
				t.Errorf("candidates = %v, want the scope unchanged", got)
			}
			if keys := f.archives(t); len(keys) != map[bool]int{false: 0, true: 1}[visible] {
				t.Errorf("archives = %v", keys)
			} else if visible {
				if got, _ := c.KeyStanding(t.Context(), keys[0]); got != catalog.KeyUnknown {
					t.Errorf("the visible archive's standing = %v, want unknown", got)
				}
			}
		})
	}
}

// failingPut is a store whose puts fail, storing the object first when visible, its last byte flipped when garbled.
type failingPut struct {
	*store.Mem
	visible, garbled bool
}

func (s failingPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if s.visible {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if s.garbled {
			b[len(b)-1] ^= 0xff
		}
		if err := s.Mem.Put(ctx, key, bytes.NewReader(b), size); err != nil {
			return err
		}
	}

	return errors.New("put refused")
}
