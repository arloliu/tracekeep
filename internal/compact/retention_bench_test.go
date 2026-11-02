package compact

import (
	"fmt"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/bus"
)

// BenchmarkRetainClaimListing is a retention cycle, Compactor.Retain, dominated by its claim listing (P3):
// the claim bucket, in memory, listed and recorded in the catalog every cycle, over 1,000, 10,000 and 100,000 objects (TRACEKEEP_BENCH_SCALE),
// each the SDK's claim object of one seq of one admitted capture, in an hour well inside retention.
// Its starting state is every object already recorded by one untimed cycle; an iteration lists and records them again,
// evicting, releasing and pruning nothing, so nothing is reset.
// It asserts no row added, no unattributed object, nothing evicted or failed, and, after the loop, every object still recorded.
func BenchmarkRetainClaimListing(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 10000, 100000) {
		b.Run(fmt.Sprintf("objects=%d", n), func(b *testing.B) {
			f := newFixture(b, benchCatalog(b))
			bucket := newBucket()
			f.r.Claims, f.r.Bus, f.r.Retention, f.r.DeleteDelay = bucket, bus.Config{ClaimBucket: "claims"}, decade, time.Hour
			f.r.Now = func() time.Time { return time.Unix(0, (hour+2)*nsPerHour) }
			capture := f.admit(b)
			for seq := range uint64(n) { //nolint:gosec // a benchmark's small count
				bucket.put(sdkObject(b, capture, seq, hour*nsPerHour+int64(seq))) //nolint:gosec // a benchmark's small seq
			}
			var first RetentionReport
			if err := f.r.Retain(b.Context(), f.lease, &first); err != nil || first.Attributed != n || first.Unattributed != 0 {
				b.Fatalf("the recording cycle = %+v, %v; want %d attributed", first, err, n)
			}
			b.ReportAllocs()
			for b.Loop() {
				var rep RetentionReport
				if err := f.r.Retain(b.Context(), f.lease, &rep); err != nil || rep != (RetentionReport{}) {
					b.Fatalf("Retain = %+v, %v; want nothing added, evicted or failed", rep, err)
				}
			}
			if objs, err := f.c.ClaimObjects(b.Context()); err != nil || len(objs) != n {
				b.Fatalf("claim objects = %d, %v; want %d", len(objs), err, n)
			}
		})
	}
}
