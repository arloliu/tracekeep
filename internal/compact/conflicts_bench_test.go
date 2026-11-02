package compact

import (
	"fmt"
	"testing"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// BenchmarkCompare is the conflict check, Compactor.compare, of one suspect (P15):
// two segments of one capture of n records each, the second starting halfway through the first, their n/2 overlapping copies equal,
// at n of 1,000, 10,000 and 100,000 (TRACEKEEP_BENCH_SCALE).
// Its starting state is both packs in the store and the suspect naming them; compare writes nothing, so nothing is reset.
// An iteration compares the two packs whole and asserts no difference found.
func BenchmarkCompare(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 10000, 100000) {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			f := newFixture(b, benchCatalog(b))
			capture := newID(b)
			half := uint64(n / 2) //nolint:gosec // a benchmark's small count
			var packs [2]catalog.ConflictPack
			for i, first := range []uint64{0, half} {
				p := segmentOf(b, capture, hour, recordsAt(hour, seqRun(first, n)...), noAdjust)
				f.put(b, p.key, p.data)
				packs[i] = catalog.ConflictPack{ID: p.id, Hour: hour, Key: p.key, Size: int64(len(p.data))}
			}
			s := catalog.Suspect{Capture: capture, SeqFrom: half, SeqTo: uint64(n) - 1, Pack: packs[1], Other: packs[0]} //nolint:gosec // a benchmark's small count
			b.ReportAllocs()
			for b.Loop() {
				differ, err := f.r.compare(b.Context(), &s)
				if err != nil || differ {
					b.Fatalf("compare = %t, %v; want equal copies", differ, err)
				}
			}
		})
	}
}
