package compact

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// maskedPerInput is how many records each masked input of BenchmarkMergeWrite masks,
// and maskedInputs how many inputs its masked rows merge: both fixed, so the archive's redaction entries,
// at most maskedInputs*maskedPerInput, keep its pack metadata far below the reader's budget (tracepack.DefaultMaxPackMetadataLen),
// over which the merge would fail with tracepack.ErrMergeLimit.
const (
	maskedPerInput = 100
	maskedInputs   = 12
)

// benchCatalog is a fresh catalog on SQLite: the compactor's baselines are local ones.
func benchCatalog(b *testing.B) *catalog.Catalog {
	b.Helper()

	return catalogtest.Open(b, catalog.EngineSQLite)
}

// freshFixture is a fixture on a fresh SQLite catalog of its own, for one iteration, with its teardown,
// which closes the catalog and deletes its database and the merges' working directory, so iterations leave nothing open behind them.
func freshFixture(b *testing.B) (*fixture, func()) {
	b.Helper()
	c, closeCatalog := catalogtest.OpenClosable(b, catalog.EngineSQLite)
	f := newFixture(b, c)

	return f, func() {
		closeCatalog()
		if f.r.WorkDir != "" {
			_ = os.RemoveAll(f.r.WorkDir)
		}
	}
}

// checkClosed fails b when more catalogs are open than before, the count catalogtest.Opened gave when its iterations began.
func checkClosed(b *testing.B, before int) {
	b.Helper()
	if n := catalogtest.Opened(); n != before {
		b.Fatalf("%d catalogs open after the iterations, %d before: an iteration's catalog was left open", n, before)
	}
}

// seqRun is n consecutive seqs from first.
func seqRun(first uint64, n int) []uint64 {
	seqs := make([]uint64, n)
	for i := range seqs {
		seqs[i] = first + uint64(i) //nolint:gosec // a benchmark's small index
	}

	return seqs
}

// masking masks the first count records of recs, each given a body to mask, and returns the adjustment that writes their redaction entries.
func masking(recs []tracepack.Record, count int) ([]tracepack.Record, func(*tracepack.PackMeta)) {
	for i := range count {
		recs = bodiedAt(recs, recs[i].Seq)
	}

	return recs, func(m *tracepack.PackMeta) {
		redacted(2)(m)
		entry := m.Redaction[0]
		m.Redaction = make([]tracepack.RedactionEntry, count)
		for i := range count {
			m.Redaction[i] = entry
			m.Redaction[i].Seq = recs[i].Seq
		}
	}
}

// BenchmarkMergeWrite is the merge writer: Compactor.write over a fixed snapshot of one scope's inputs, each a segment of disjoint seqs.
// Its starting state is the inputs staged and the scope's snapshot taken once; write never commits, so the snapshot stays valid.
// An iteration truncates the working file and merges every input into it; the time includes the archive's sync to disk,
// and the truncation, constant, is inside the timer.
// It asserts the merge found no conflict and wrote every input's records once.
// Its unmasked rows merge 2, 12 and 64 inputs (TRACEKEEP_BENCH_SCALE) of 1,000 and 10,000 records each;
// its masked rows merge maskedInputs inputs of 1,000 and 10,000 records, maskedPerInput of each masked.
func BenchmarkMergeWrite(b *testing.B) {
	type shape struct {
		inputs, records int
		masked          bool
	}
	sizes := benchscale.Sizes(b, 2, 12, 64)
	shapes := make([]shape, 0, 2*len(sizes)+2)
	for _, n := range sizes {
		shapes = append(shapes, shape{n, 1000, false}, shape{n, 10000, false})
	}
	shapes = append(shapes, shape{maskedInputs, 1000, true}, shape{maskedInputs, 10000, true})
	for _, s := range shapes {
		b.Run(fmt.Sprintf("inputs=%d/records=%d/masked=%t", s.inputs, s.records, s.masked), func(b *testing.B) {
			f := newFixture(b, benchCatalog(b))
			f.merging(b)
			capture := f.admit(b)
			for i := range s.inputs {
				recs := recordsAt(hour, seqRun(uint64(i*s.records), s.records)...) //nolint:gosec // a benchmark's small index
				adjust := noAdjust
				if s.masked {
					recs, adjust = masking(recs, maskedPerInput)
				}
				f.stage(b, segmentOf(b, capture, hour, recs, adjust))
			}
			snap, err := f.c.MergeSnapshot(b.Context(), catalog.Scope{Capture: capture, Hour: hour})
			if err != nil || !snap.Eligible || len(snap.Inputs) != s.inputs {
				b.Fatalf("MergeSnapshot = %d inputs, eligible %t, %v; want %d", len(snap.Inputs), snap.Eligible, err, s.inputs)
			}
			w, err := os.CreateTemp(f.r.WorkDir, workFilePattern)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			want := uint64(s.inputs * s.records) //nolint:gosec // a benchmark's small count
			b.ReportAllocs()
			for b.Loop() {
				if err := w.Truncate(0); err != nil {
					b.Fatal(err)
				}
				if _, err := w.Seek(0, io.SeekStart); err != nil {
					b.Fatal(err)
				}
				rep, found, err := f.r.write(b.Context(), w, f.lease, &snap)
				if err != nil || found != nil || rep.Inputs != s.inputs || rep.Records != want {
					b.Fatalf("write = %d inputs, %d records, %d conflicts, %v; want %d and %d", rep.Inputs, rep.Records, len(found), err, s.inputs, want)
				}
			}
		})
	}
}

// mergeBenchRecords is the records of each input of BenchmarkMergePass's scopes.
const mergeBenchRecords = 10

// seedScopes admits healthy+failing captures and stages two inputs in each one's scope;
// each failing scope's second input is overwritten with bytes of its size, so its merge fails every cycle until an operator acts.
func (f *fixture) seedScopes(b *testing.B, healthy, failing int) {
	b.Helper()
	for i := range healthy + failing {
		capture := f.admit(b)
		f.stage(b, segmentOf(b, capture, hour, recordsAt(hour, seqRun(0, mergeBenchRecords)...), noAdjust))
		p := f.stage(b, segmentOf(b, capture, hour, recordsAt(hour, seqRun(mergeBenchRecords, mergeBenchRecords)...), noAdjust))
		if i >= healthy {
			f.put(b, p.Key, bytes.Repeat([]byte{1}, int(p.Size)))
		}
	}
}

// mergeCycle runs one merge pass and asserts its outcome: merged scopes merged and failing ones failed, nothing else.
func (f *fixture) mergeCycle(b *testing.B, merged, failing int) {
	b.Helper()
	rep, err := f.r.Merges(b.Context(), f.lease)
	if err != nil || len(rep.Merged) != merged || len(rep.Failed) != failing || len(rep.Stale)+len(rep.Conflicted)+len(rep.Refused) != 0 {
		b.Fatalf("Merges = %d merged, %d failed, %d stale, %d conflicted, %d refused, %v; want %d merged and %d failed",
			len(rep.Merged), len(rep.Failed), len(rep.Stale), len(rep.Conflicted), len(rep.Refused), err, merged, failing)
	}
}

// BenchmarkMergePass is the scheduled merge pass, Compactor.Merges, with 0, 1 and 10 failing scopes, each with a damaged input (P17),
// beside 10, 100 and 1,000 healthy scopes (TRACEKEEP_BENCH_SCALE), every scope of two inputs of mergeBenchRecords records.
// Each row reports the scopes a cycle attempted, failed and merged, and the time of a cycle.
//   - fresh: the starting state is a fresh catalog and store seeded with the scopes, reseeded outside the timer before each iteration
//     and torn down outside it after, the catalog closed and deleted;
//     an iteration is one cycle, asserting every healthy scope merged and every failing one failed, and the run no catalog left open.
//   - cycles=10: the starting state is the same seed after one untimed cycle has merged the healthy scopes, so only the failures remain;
//     an iteration is ten cycles over the same failures, nothing reset, each cycle asserting every failing scope failed again and nothing merged.
func BenchmarkMergePass(b *testing.B) {
	for _, healthy := range benchscale.Sizes(b, 10, 100, 1000) {
		for _, failing := range []int{0, 1, 10} {
			b.Run(fmt.Sprintf("healthy=%d/failing=%d/fresh", healthy, failing), func(b *testing.B) {
				open := catalogtest.Opened()
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					f, teardown := freshFixture(b)
					f.merging(b)
					f.seedScopes(b, healthy, failing)
					b.StartTimer()
					f.mergeCycle(b, healthy, failing)
					b.StopTimer()
					teardown()
					b.StartTimer()
				}
				checkClosed(b, open)
				reportCycles(b, 1, healthy+failing, failing, healthy)
			})
			b.Run(fmt.Sprintf("healthy=%d/failing=%d/cycles=10", healthy, failing), func(b *testing.B) {
				f := newFixture(b, benchCatalog(b))
				f.merging(b)
				f.seedScopes(b, healthy, failing)
				f.mergeCycle(b, healthy, failing)
				b.ReportAllocs()
				for b.Loop() {
					for range 10 {
						f.mergeCycle(b, 0, failing)
					}
				}
				reportCycles(b, 10, failing, failing, 0)
			})
		}
	}
}

// reportCycles reports a cycle's scopes attempted, failed and merged, and its time, for iterations of cycles cycles each.
func reportCycles(b *testing.B, cycles, attempted, failed, merged int) {
	b.Helper()
	b.ReportMetric(float64(attempted), "attempted/cycle")
	b.ReportMetric(float64(failed), "failed/cycle")
	b.ReportMetric(float64(merged), "merged/cycle")
	b.ReportMetric(float64(b.Elapsed()/time.Duration(b.N*cycles)), "ns/cycle")
}
