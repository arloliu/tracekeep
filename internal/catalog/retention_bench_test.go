package catalog

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"

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
// due=10 holds as many merges, of which only ten are due, five of each kind, among merges not yet due:
// half before a version recorded delete_at, committed at the selection's time, and half deletable only far in the future.
// One iteration selects the candidates and asserts exactly the due merges' inputs selected, by pack. It writes nothing.
func BenchmarkDeleteCandidates(b *testing.B) {
	now := int64(time.Hour)
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
					if got, err := c.DeleteCandidates(b.Context(), now, time.Hour); err != nil || len(got) != n/2 {
						b.Fatalf("DeleteCandidates = %d, %v; want %d", len(got), err, n/2)
					}
				}
			})
		})
		b.Run(fmt.Sprintf("merges=%d/due=10", n), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				capture := seedCaptures(b, c, 1, 1)[0]
				for done := 10; done < n; done += 100000 {
					k := min(100000, n-done)
					seedMerges(b, c, capture, 1, k/2, 1, now, 0)
					seedMerges(b, c, capture, 1, k-k/2, 1, 0, 1<<62)
				}
				due := append(seedMerges(b, c, capture, 1, 5, 1, 0, 0), seedMerges(b, c, capture, 1, 5, 1, 0, now)...)
				want := mergeInputs(b, c, due)
				b.ReportAllocs()
				for b.Loop() {
					got, err := c.DeleteCandidates(b.Context(), now, time.Hour)
					packs := make([]string, 0, len(got))
					for _, d := range got {
						packs = append(packs, d.Pack.String())
					}
					slices.Sort(packs)
					if err != nil || !slices.Equal(packs, want) {
						b.Fatalf("DeleteCandidates = %v, %v; want the due merges' inputs %v", packs, err, want)
					}
				}
			})
		})
	}
}

// mergeInputs reads the input packs of merges, sorted as strings.
func mergeInputs(tb testing.TB, c *Catalog, merges []tracepack.UUID) []string {
	tb.Helper()
	q, args := inList(`SELECT pack_id FROM merge_inputs WHERE merge_id IN`, merges)
	var packs [][]byte
	if err := c.read.SelectContext(tb.Context(), &packs, q, args...); err != nil {
		tb.Fatal(err)
	}
	out := make([]string, 0, len(packs))
	for _, p := range packs {
		out = append(out, tracepack.UUID(p).String())
	}
	slices.Sort(out)

	return out
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

// chainHour is the hour BenchmarkEvictMerges evicts each iteration.
const chainHour = 100

// mergeChain is a merged hour's rows, which BenchmarkEvictMerges inserts again before each iteration:
// three segments merged into an archive, that archive and the third segment merged into a second, staged archive.
type mergeChain struct {
	merges, packs []tracepack.UUID
	keys          []string
	rows          map[string][][]any
}

// newMergeChain builds the chain of capture's hour: segments s1, s2 and s3 of seqs 0 to 9, 10 to 19 and 20 to 29, merged;
// merge m1 of s1 and s2 into archive a1 (0 to 19), merged too; merge m2 of a1 and s3 into archive a2 (0 to 29), staged; the hour's scope settled.
func newMergeChain(tb testing.TB, capture tracepack.UUID, hour int64) *mergeChain {
	tb.Helper()
	ch := &mergeChain{rows: map[string][][]any{}}
	pack := func(role, state string, from, to int64) tracepack.UUID {
		id := newID(tb)
		ch.packs, ch.keys = append(ch.packs, id), append(ch.keys, "staging/"+id.String())
		ch.rows["packs"] = append(ch.rows["packs"], []any{id[:], capture[:], "T1", hour, role, "staging/" + id.String(), 1 << 20, from, 2, state, false, 0})
		ch.rows["pack_ranges"] = append(ch.rows["pack_ranges"], []any{id[:], capture[:], from, to, hourNS(hour) + from, hourNS(hour) + to})

		return id
	}
	merge := func(out tracepack.UUID, inputs ...tracepack.UUID) {
		id := newID(tb)
		ch.merges = append(ch.merges, id)
		ch.rows["merges"] = append(ch.rows["merges"], []any{id[:], capture[:], hour, out[:], "committed", 0, 0})
		for _, in := range inputs {
			ch.rows["merge_inputs"] = append(ch.rows["merge_inputs"], []any{id[:], in[:]})
		}
	}
	s1, s2, s3 := pack(RoleSegment, "merged", 0, 9), pack(RoleSegment, "merged", 10, 19), pack(RoleSegment, "merged", 20, 29)
	a1, a2 := pack(RoleArchive, "merged", 0, 19), pack(RoleArchive, "staged", 0, 29)
	merge(a1, s1, s2)
	merge(a2, a1, s3)
	ch.rows["scopes"] = [][]any{{capture[:], hour, "settled", 2}}
	slices.Sort(ch.keys)

	return ch
}

// insert writes the chain's rows in one write transaction.
func (ch *mergeChain) insert(tb testing.TB, c *Catalog) {
	tb.Helper()
	cols := map[string][]string{
		"packs":        {"pack_id", "capture_id", "tool_id", "hour", "role", "object_key", "size", "seq_start", "format_minor", "state", "adopted", "registered_at"},
		"pack_ranges":  {"pack_id", "capture_id", "seq_from", "seq_to", "ts_from", "ts_to"},
		"merges":       {"merge_id", "capture_id", "hour", "output_pack_id", "outcome", "at", "delete_at"},
		"merge_inputs": {"merge_id", "pack_id"},
		"scopes":       {"capture_id", "hour", "state", "revision"},
	}
	seedTx(tb, c, func(tx *sqlx.Tx) {
		for _, table := range []string{"packs", "pack_ranges", "merges", "merge_inputs", "scopes"} {
			insertRows(tb, tx, table, cols[table], ch.rows[table])
		}
	})
}

// mergeRows reads every merge input as merge/pack and every merge as its id, sorted: the merges' identity.
func mergeRows(tb testing.TB, c *Catalog) []string {
	tb.Helper()
	var rows []struct {
		Merge []byte `db:"merge_id"`
		Pack  []byte `db:"pack_id"`
	}
	if err := c.read.SelectContext(tb.Context(), &rows, `SELECT merge_id, pack_id FROM merge_inputs`); err != nil {
		tb.Fatal(err)
	}
	var merges [][]byte
	if err := c.read.SelectContext(tb.Context(), &merges, `SELECT merge_id FROM merges`); err != nil {
		tb.Fatal(err)
	}
	out := make([]string, 0, len(rows)+len(merges))
	for _, r := range rows {
		out = append(out, tracepack.UUID(r.Merge).String()+"/"+tracepack.UUID(r.Pack).String())
	}
	for _, m := range merges {
		out = append(out, tracepack.UUID(m).String())
	}
	slices.Sort(out)

	return out
}

// BenchmarkEvictMerges is retention's eviction of one merged hour of a capture among many merges it leaves.
// Its starting state is two open captures under a recorded retention and 1,000, 100,000 or 1,000,000 committed merges of one input each
// (TRACEKEEP_BENCH_SCALE), none in the evicted hour of the first capture: half the first capture's, spread over hours 1 to 10,
// half the second's, in the evicted hour's number, chainHour.
// Before each iteration, outside the timer, the first capture's chainHour is inserted the same way each time, the same rows (newMergeChain):
// a chain of two merges whose inputs, segments and an archive, are in that hour.
// One iteration evicts that hour and asserts it evicted with its five packs tombstoned by key;
// then, outside the timer, it asserts by identity the chain's merges and their inputs deleted,
// and kept the first merge of each other population, each with its input;
// the hour's evicted ranges and its keys' tombstones are deleted, and every table it touches is asserted back to its starting count.
// After the loop every merge and merge input left is asserted, by identity, the seeded ones.
func BenchmarkEvictMerges(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 100000, 1000000) {
		b.Run(fmt.Sprintf("merges=%d", n), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				ctx := b.Context()
				l := benchLease(b, c)
				if _, err := c.RecordRetention(ctx, l, month, delay, 0); err != nil {
					b.Fatal(err)
				}
				captures := seedCaptures(b, c, 1, 2)
				var sentinels []tracepack.UUID
				for h := range int64(10) {
					ms := seedMerges(b, c, captures[0], 1+h, n/20, 1, 0, 0)
					if h == 0 {
						sentinels = append(sentinels, ms[0])
					}
				}
				// In chunks, so a large population is never held whole.
				for done := n / 2; done < n; done += 100000 {
					ms := seedMerges(b, c, captures[1], chainHour, min(100000, n-done), 1, 0, 0)
					if done == n/2 {
						sentinels = append(sentinels, ms[0])
					}
				}
				seeded := mergeRows(b, c)
				tables := append([]string{"merges", "merge_inputs"}, evictTables...)
				start := tableCounts(b, c, tables...)
				ch := newMergeChain(b, captures[0], chainHour)
				gone, goneArgs := inList(`SELECT COUNT(*) FROM merges WHERE merge_id IN`, ch.merges)
				goneInputs, goneInputsArgs := inList(`SELECT COUNT(*) FROM merge_inputs WHERE merge_id IN`, ch.merges)
				goneInputs, goneInputsArgs = inList(goneInputs+` OR pack_id IN`, ch.packs, goneInputsArgs...)
				kept, keptArgs := inList(`SELECT COUNT(*) FROM merges m JOIN merge_inputs i ON i.merge_id = m.merge_id WHERE m.merge_id IN`, sentinels)
				now := hourNS(1<<20) + int64(month)
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					ch.insert(b, c)
					b.StartTimer()
					ev, err := c.EvictHour(ctx, l, Scope{Capture: captures[0], Hour: chainHour}, now)
					b.StopTimer()
					keys := slices.Sorted(slices.Values(ev.Packs))
					if err != nil || !ev.Evicted || !slices.Equal(keys, ch.keys) || ev.Tombstoned != len(ch.keys) {
						b.Fatalf("EvictHour = %+v, %v; want hour %d evicted with its packs %v", ev, err, chainHour, ch.keys)
					}
					if got := count(b, c, gone, goneArgs...); got != 0 {
						b.Fatalf("%d of the chain's merges kept, want none", got)
					}
					if got := count(b, c, goneInputs, goneInputsArgs...); got != 0 {
						b.Fatalf("%d of the chain's merge inputs kept, want none", got)
					}
					if got := count(b, c, kept, keptArgs...); got != len(sentinels) {
						b.Fatalf("%d of the other merges' first inputs kept, want %d", got, len(sentinels))
					}
					seedTx(b, c, func(tx *sqlx.Tx) {
						if _, err := tx.ExecContext(ctx, `DELETE FROM evicted_ranges WHERE capture_id = ? AND hour = ?`, captures[0][:], chainHour); err != nil {
							b.Fatal(err)
						}
						for _, key := range ch.keys {
							if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE object_key = ?`, key); err != nil {
								b.Fatal(err)
							}
						}
					})
					if got := tableCounts(b, c, tables...); got != start {
						b.Fatalf("after the restore the tables hold %s, starting from %s", got, start)
					}
					b.StartTimer()
				}
				b.StopTimer()
				if got := mergeRows(b, c); !slices.Equal(got, seeded) {
					b.Fatalf("%d merges and inputs left, not the %d seeded", len(got), len(seeded))
				}
			})
		})
	}
}
