package compact

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// scheduleCycles is BenchmarkMergeSchedule's fixed virtual schedule: two hours of cycles at the default interval.
const scheduleCycles = 24

// BenchmarkMergeSchedule is the merge pass, Compactor.Merges, over a fixed virtual schedule of scheduleCycles cycles at the default five-minute interval,
// the compactor's clock advanced by the interval each cycle (P17): it counts the merges attempted and the scopes drained, never a speed.
// Its starting state, seeded before each iteration outside the timer on a fresh catalog and store, is 10 healthy scopes and 1 or 10 failing ones,
// each of two inputs of mergeBenchRecords records, a failing scope's archive put refused after its inputs are read and merged.
// An iteration is the whole schedule, asserting every healthy scope merged and every attempt merged or failed, and the run no catalog left open;
// it reports the merges attempted, failed and merged a schedule.
func BenchmarkMergeSchedule(b *testing.B) {
	const healthy = 10
	for _, failing := range []int{1, 10} {
		b.Run(fmt.Sprintf("healthy=%d/failing=%d", healthy, failing), func(b *testing.B) {
			open := catalogtest.Opened()
			var attempted, failed, merged int
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				f, teardown := freshFixture(b)
				f.merging(b)
				f.r.Interval = 5 * time.Minute
				s := &refusingCaptures{ObjectStore: f.s, refused: map[string]bool{}}
				for i := range healthy + failing {
					capture := f.admit(b)
					f.stage(b, segmentOf(b, capture, hour, recordsAt(hour, seqRun(0, mergeBenchRecords)...), noAdjust))
					f.stage(b, segmentOf(b, capture, hour, recordsAt(hour, seqRun(mergeBenchRecords, mergeBenchRecords)...), noAdjust))
					if i >= healthy {
						s.refused[capture.String()] = true
					}
				}
				f.r.Store = s
				start := f.r.now()
				b.StartTimer()
				drained := 0
				for cycle := range scheduleCycles {
					f.r.Now = func() time.Time { return start.Add(time.Duration(cycle) * f.r.Interval) }
					rep, err := f.r.Merges(b.Context(), f.lease)
					if err != nil || len(rep.Stale)+len(rep.Conflicted)+len(rep.Refused) != 0 {
						b.Fatalf("Merges = %+v, %v; want only merges and failures", rep, err)
					}
					attempted += len(rep.Merged) + len(rep.Failed)
					failed += len(rep.Failed)
					drained += len(rep.Merged)
				}
				if drained != healthy {
					b.Fatalf("the schedule merged %d scopes, want the %d healthy", drained, healthy)
				}
				merged += drained
				b.StopTimer()
				teardown()
				b.StartTimer()
			}
			checkClosed(b, open)
			b.ReportMetric(float64(attempted)/float64(b.N), "attempted/schedule")
			b.ReportMetric(float64(failed)/float64(b.N), "failed/schedule")
			b.ReportMetric(float64(merged)/float64(b.N), "merged/schedule")
		})
	}
}

// refusingCaptures is a store that refuses the puts of the archives of the captures it names, storing nothing then.
type refusingCaptures struct {
	store.ObjectStore
	refused map[string]bool
}

func (s *refusingCaptures) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	for capture := range s.refused {
		if strings.Contains(key, capture) {
			return errBenchPut
		}
	}

	return s.ObjectStore.Put(ctx, key, r, size)
}
