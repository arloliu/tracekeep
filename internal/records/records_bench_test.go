package records

import (
	"fmt"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// benchReaders is the reader's bound on the packs a merge opens, max_readers' default; no cluster here exceeds it.
const benchReaders = 16

// BenchmarkRead reads an hour of n records in k packs, by Reader.Read over an fs store, the reader's baseline.
// Its starting state is the world serving one read set of k staged segments, written once outside the timer:
// disjoint packs each hold a run of consecutive seqs, k clusters of one pack;
// overlapping packs interleave their seqs (pack i holds every k-th seq from i), one cluster of k packs merged together, no seq held twice.
// An iteration reads the whole hour into a counting sink, writes nothing, and asserts n records and no incompleteness.
// Its sizes are 1,000, 72,000 and 360,000 records (TRACEKEEP_BENCH_SCALE); 1, 12 and 16 packs and both layouts run at every size.
func BenchmarkRead(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 72_000, 360_000) {
		for _, k := range []int{1, 12, benchReaders} {
			for _, overlap := range []bool{false, true} {
				if overlap && k == 1 {
					continue // one pack has nothing to overlap
				}
				layout := "disjoint"
				if overlap {
					layout = "overlapping"
				}
				b.Run(fmt.Sprintf("records=%d/packs=%d/%s", n, k, layout), func(b *testing.B) {
					w := newWorld(b)
					w.serve(benchPacks(b, w, n, k, overlap))
					r := w.reader(benchReaders)
					b.ReportAllocs()
					for b.Loop() {
						var s counting
						if err := r.Read(b.Context(), window(), &s); err != nil || s.records != n || s.incomplete != 0 {
							b.Fatalf("Read = %d records, %d incomplete, %v; want %d, 0", s.records, s.incomplete, err, n)
						}
					}
				})
			}
		}
	}
}

// benchPacks writes n data records, seqs 1 to n, as k staged segments of the test's hour, interleaved when overlap is set.
func benchPacks(b *testing.B, w *world, n, k int, overlap bool) []catalog.ReadPack {
	b.Helper()
	parts := make([][]tracepack.Record, k)
	for i := range n {
		p := i * k / n
		if overlap {
			p = i % k
		}
		seq := uint64(i + 1) //nolint:gosec // a positive index
		parts[p] = append(parts[p], data(seq, 6, 11))
	}
	packs := make([]catalog.ReadPack, 0, k)
	for _, recs := range parts {
		packs = append(packs, w.pack(b, catalog.RoleSegment, catalog.PackStaged, false, recs...))
	}

	return packs
}

// counting is a sink that counts the versions and incompletes it receives.
type counting struct{ records, incomplete int }

func (c *counting) Versions(rs []Record) error { c.records += len(rs); return nil }

func (c *counting) Incomplete(Incomplete) error { c.incomplete++; return nil }
