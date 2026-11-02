package compact

import (
	"context"
	"fmt"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
)

// countingList is a store that counts the keys its listings return.
type countingList struct {
	store.ObjectStore
	listed int
}

func (s *countingList) List(ctx context.Context, prefix string, fn func(store.ObjectInfo) error) error {
	return s.ObjectStore.List(ctx, prefix, func(info store.ObjectInfo) error {
		s.listed++

		return fn(info)
	})
}

// BenchmarkOrphans is the orphan pass, Compactor.Orphans (P16), over 1,000, 10,000 and 100,000 objects (TRACEKEEP_BENCH_SCALE),
// each a segment of one seq of one admitted capture.
//   - steady: the starting state is every object registered, its stored bytes a placeholder the pass never reads;
//     an iteration lists them all and finds each registered, writing nothing, so nothing is reset;
//     it asserts every object listed and the report empty.
//   - adopting: before each iteration, outside the timer, a fresh catalog and store hold n new unknown segments,
//     sighted once by an untimed pass that finds them all waiting;
//     an iteration examines, verifies and adopts each, asserting every one adopted and nothing else reported.
func BenchmarkOrphans(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 10000, 100000) {
		b.Run(fmt.Sprintf("objects=%d/steady", n), func(b *testing.B) {
			f := newFixture(b, benchCatalog(b))
			capture := f.admit(b)
			for seq := range uint64(n) { //nolint:gosec // a benchmark's small count
				id := newID(b)
				key, err := tracepack.SegmentKey("", tool, capture, seq, id)
				if err != nil {
					b.Fatal(err)
				}
				at := hour*nsPerHour + int64(seq) //nolint:gosec // a benchmark's small seq
				p := &catalog.Pack{
					ID: id, Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleSegment, Key: key, Size: 1, SeqStart: seq, FormatMinor: 2,
					Ranges: []catalog.Range{{SeqFrom: seq, SeqTo: seq, TSFrom: at, TSTo: at}},
				}
				if out, err := f.c.RegisterPack(b.Context(), p, catalog.Registration{Evidence: &catalog.Evidence{}, HorizonHour: f.horizon}); err != nil || out != catalog.Registered {
					b.Fatalf("RegisterPack = %v, %v", out, err)
				}
				f.put(b, key, []byte{0})
			}
			s := &countingList{ObjectStore: f.s}
			f.r.Store = s
			b.ReportAllocs()
			for b.Loop() {
				s.listed = 0
				rep, err := f.r.Orphans(b.Context(), f.lease)
				if err != nil || s.listed != n || !emptyOrphans(&rep) {
					b.Fatalf("Orphans = %d reported over %d listed, %v; want nothing over %d", orphanCount(&rep), s.listed, err, n)
				}
			}
		})
		b.Run(fmt.Sprintf("objects=%d/adopting", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				f := newFixture(b, benchCatalog(b))
				capture := f.admit(b)
				for seq := range uint64(n) { //nolint:gosec // a benchmark's small count
					p := segmentOf(b, capture, hour, recordsAt(hour, seq), noAdjust)
					f.put(b, p.key, p.data)
				}
				if rep, err := f.r.Orphans(b.Context(), f.lease); err != nil || len(rep.Waiting) != n {
					b.Fatalf("the sighting pass = %d waiting, %v; want %d", len(rep.Waiting), err, n)
				}
				b.StartTimer()
				rep, err := f.r.Orphans(b.Context(), f.lease)
				adopted := len(rep.Adopted)
				rep.Adopted = nil
				if err != nil || adopted != n || !emptyOrphans(&rep) {
					b.Fatalf("Orphans = %d adopted of %d, %d others, %v", adopted, n, orphanCount(&rep), err)
				}
			}
		})
	}
}

// orphanCount is the keys an orphan pass reported, every outcome together.
func orphanCount(rep *OrphanReport) int {
	return len(rep.Adopted) + len(rep.Deleted) + len(rep.Tombstoned) + len(rep.Quarantined) + len(rep.Unadmitted) + len(rep.Waiting)
}

// emptyOrphans reports whether an orphan pass reported nothing.
func emptyOrphans(rep *OrphanReport) bool { return orphanCount(rep) == 0 }
