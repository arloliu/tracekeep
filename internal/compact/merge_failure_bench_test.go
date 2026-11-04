package compact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/store"
)

var errBenchPut = errors.New("compact bench: the store refuses every put")

// refusingPuts is a store whose every put fails, storing nothing; it counts the bytes its objects' reads return,
// which may run concurrently, and the bytes the puts it refused were handed.
type refusingPuts struct {
	store.ObjectStore
	read, refused atomic.Int64
}

type countedObject struct {
	store.Object
	s *refusingPuts
}

// BenchmarkMergeFailingPut is the merge pass, Compactor.Merges, over one scope whose merge fails only once its inputs are read and merged:
// the archive's put fails (P17), so the pass has paid for the whole merge and kept nothing.
// Its starting state is one admitted capture's scope of 2, 12 or 64 inputs (TRACEKEEP_BENCH_SCALE) of 1,000 and 10,000 records each, each a segment of disjoint seqs,
// staged before the store begins refusing puts.
// An iteration is one cycle, which leaves the catalog and the store as they were, so nothing is reset;
// it asserts the scope attempted and failed and nothing merged,
// and reports, besides the cycle's time, the bytes read from the store and the bytes of the archive refused, each an attempt.
func BenchmarkMergeFailingPut(b *testing.B) {
	for _, inputs := range benchscale.Sizes(b, 2, 12, 64) {
		for _, n := range []int{1000, 10000} {
			b.Run(fmt.Sprintf("inputs=%d/records=%d", inputs, n), func(b *testing.B) {
				f := newFixture(b, benchCatalog(b))
				f.merging(b)
				capture := f.admit(b)
				for i := range inputs {
					f.stage(b, segmentOf(b, capture, hour, recordsAt(hour, seqRun(uint64(i*n), n)...), noAdjust)) //nolint:gosec // a benchmark's small index
				}
				s := &refusingPuts{ObjectStore: f.s}
				f.r.Store = s
				b.ReportAllocs()
				for b.Loop() {
					f.mergeCycle(b, 0, 1)
				}
				reportCycles(b, 1, 1, 1, 0)
				b.ReportMetric(float64(s.read.Load())/float64(b.N), "read-bytes/attempt")
				b.ReportMetric(float64(s.refused.Load())/float64(b.N), "archive-bytes/attempt")
			})
		}
	}
}

func (s *refusingPuts) Put(_ context.Context, _ string, _ io.ReadSeeker, size int64) error {
	s.refused.Add(size)

	return errBenchPut
}

func (s *refusingPuts) Open(ctx context.Context, key string) (store.Object, error) {
	o, err := s.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}

	return &countedObject{Object: o, s: s}, nil
}

func (o *countedObject) ReadAt(p []byte, off int64) (int, error) {
	n, err := o.Object.ReadAt(p, off)
	o.s.read.Add(int64(n))

	return n, err
}
