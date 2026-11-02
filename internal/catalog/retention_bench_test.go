package catalog

import (
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/tracekeep/internal/benchscale"
)

// BenchmarkPruneEvents is the compactor's prune of the change cursor's seq prefix.
// Its starting state is 10,000, 100,000 or 1,000,000 scope events (TRACEKEEP_BENCH_SCALE), the i-th at time i+1.
// One iteration prunes at a time that makes the younger half the floor and asserts the older half pruned,
// then inserts that half again and lowers low_water back to 0 outside the timer.
func BenchmarkPruneEvents(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 10000, 100000, 1000000) {
		b.Run(fmt.Sprintf("events=%d", n), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				l := benchLease(b, c)
				capture := newID(b)
				first, _ := seedEvents(b, c, capture, 1, n, 1, 1)
				old := make([][]any, n/2)
				for i := range old {
					old[i] = []any{first + int64(i), capture[:], 1, "created", 1, 1 + int64(i)}
				}
				// Events at time n/2+1 and later are younger than the period at now, so the floor is the seq of the first of them.
				now := int64(n/2) + 1
				b.ReportAllocs()
				for b.Loop() {
					pruned, err := c.PruneEvents(b.Context(), l, 1, now)
					b.StopTimer()
					if err != nil || pruned != n/2 {
						b.Fatalf("PruneEvents = %d, %v; want %d", pruned, err, n/2)
					}
					seedTx(b, c, func(tx *sqlx.Tx) {
						insertRows(b, tx, "scope_events", []string{"seq", "capture_id", "hour", "event", "revision", "at"}, old)
						if _, err := tx.ExecContext(b.Context(), `UPDATE counters SET low_water = 0 WHERE name = 'scope_events'`); err != nil {
							b.Fatal(err)
						}
					})
					b.StartTimer()
				}
			})
		})
	}
}

// BenchmarkDeleteCandidates is the compactor's selection of the merged inputs it may delete.
// Its starting state is 1,000, 100,000 or 1,000,000 committed merges of one input each (TRACEKEEP_BENCH_SCALE) in one hour:
// half before a version recorded delete_at, deletable D after their time, and half deletable only far in the future.
// One iteration selects the candidates and asserts the first half selected. It writes nothing.
func BenchmarkDeleteCandidates(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 100000, 1000000) {
		b.Run(fmt.Sprintf("merges=%d", n), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				capture := seedCaptures(b, c, 1, 1)[0]
				// In chunks, so a large population is never held whole.
				for done := 0; done < n; done += 100000 {
					k := min(100000, n-done)
					seedMerges(b, c, capture, 1, k/2, 1, 0, 0)
					seedMerges(b, c, capture, 1, k-k/2, 1, 0, 1<<62)
				}
				b.ReportAllocs()
				for b.Loop() {
					if got, err := c.DeleteCandidates(b.Context(), int64(time.Hour), time.Hour); err != nil || len(got) != n/2 {
						b.Fatalf("DeleteCandidates = %d, %v; want %d", len(got), err, n/2)
					}
				}
			})
		})
	}
}

// evictedHour is the hour BenchmarkEvictHour seeds and evicts each iteration.
const evictedHour = 10

// BenchmarkEvictHour is retention's eviction of one hour of a capture holding many conflicts elsewhere.
// Its starting state is one open capture under a recorded retention, two packs of hour 1 with 10, 1,000 or 100,000 conflicts between them
// (TRACEKEEP_BENCH_SCALE). Before each iteration, outside the timer, the hour to evict is seeded the same way each time:
// two packs of one range each in evictedHour, their seqs just above hour 1's, a conflict between them and one with hour 1's first pack.
// One iteration evicts that hour and asserts it evicted with its two packs tombstoned and hour 1's conflicts kept;
// then, outside the timer, the hour's evicted ranges, its keys' tombstones and the conflict event it emitted are deleted,
// and every table it touches is asserted back to its starting count, which is logged.
func BenchmarkEvictHour(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 10, 1000, 100000) {
		b.Run(fmt.Sprintf("conflicts=%d", n), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				ctx := b.Context()
				l := benchLease(b, c)
				if _, err := c.RecordRetention(ctx, l, month, delay, 0); err != nil {
					b.Fatal(err)
				}
				capture := seedCaptures(b, c, 1, 1)[0]
				kept, seq0 := seedPacks(b, c, capture, packsSeed{hour0: 1, hours: 1, packs: 2, ranges: 1, span: 10})
				seedConflicts(b, c, capture, kept[0], kept[1], 0, n)
				now := hourNS(1<<20) + int64(month)
				start := tableCounts(b, c, evictTables...)
				var lastEvent int64
				if err := c.read.GetContext(ctx, &lastEvent, `SELECT COALESCE(MAX(seq), 0) FROM scope_events`); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					packs, next := seedPacks(b, c, capture, packsSeed{hour0: evictedHour, hours: 1, packs: 2, ranges: 1, span: 10, seq0: seq0})
					seedTx(b, c, func(tx *sqlx.Tx) {
						insertRows(b, tx, "conflicts", []string{"capture_id", "seq_from", "seq_to", "pack_id", "other_pack_id", "state"}, [][]any{
							{capture[:], int64(next) - 1, int64(next) - 1, packs[0][:], packs[1][:], "suspect"}, //nolint:gosec // a seeded seq is small
							{capture[:], int64(next) - 2, int64(next) - 2, packs[1][:], kept[0][:], "suspect"},  //nolint:gosec // a seeded seq is small
						})
					})
					b.StartTimer()
					ev, err := c.EvictHour(ctx, l, Scope{Capture: capture, Hour: evictedHour}, now)
					b.StopTimer()
					if err != nil || !ev.Evicted || len(ev.Packs) != 2 || ev.Tombstoned != 2 {
						b.Fatalf("EvictHour = %+v, %v; want hour %d evicted with its two packs", ev, err, evictedHour)
					}
					if got := count(b, c, `SELECT COUNT(*) FROM conflicts WHERE capture_id = ?`, capture[:]); got != n {
						b.Fatalf("%d conflicts left, want hour 1's %d", got, n)
					}
					seedTx(b, c, func(tx *sqlx.Tx) {
						for _, stmt := range []struct {
							q    string
							args []any
						}{
							{`DELETE FROM evicted_ranges WHERE capture_id = ? AND hour = ?`, []any{capture[:], evictedHour}},
							{`DELETE FROM tombstones WHERE object_key IN (?, ?)`, []any{ev.Packs[0], ev.Packs[1]}},
							{`DELETE FROM scope_events WHERE seq > ?`, []any{lastEvent}},
						} {
							if _, err := tx.ExecContext(ctx, stmt.q, stmt.args...); err != nil {
								b.Fatal(err)
							}
						}
					})
					if got := tableCounts(b, c, evictTables...); got != start {
						b.Fatalf("after the restore the tables hold %s, starting from %s", got, start)
					}
					b.StartTimer()
				}
				b.Logf("the tables an eviction touches: %s before and after each iteration", start)
			})
		})
	}
}

// evictTables are the tables an eviction writes.
var evictTables = []string{"packs", "pack_ranges", "scopes", "conflicts", "evicted_ranges", "tombstones", "scope_events"}
