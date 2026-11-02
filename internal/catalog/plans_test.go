package catalog

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

// envExplain set to any value runs the plan harness's large seed.
const envExplain = "TRACEKEEP_EXPLAIN"

// planSeed sizes the plan harness's population of one capture: its hours of packs of ranges, and n rows of each other table it explains.
type planSeed struct {
	hours     int64
	packs     int
	ranges    int
	n, parked int
	evicted   int
	losses    int
}

var (
	smallPlans = planSeed{hours: 1, packs: 2, ranges: 2, n: 4, parked: 2, evicted: 1, losses: 1}
	largePlans = planSeed{hours: 200, packs: 100, ranges: 10, n: 100000, parked: 10000, evicted: 1000, losses: 1000}
)

// planned is one statement the harness explains, with its arguments.
// Every statement is explained on the write pool, since SQLite's read pool is query_only and some of them write.
type planned struct {
	name  string
	query string
	args  []any
}

// TestExplainPlans logs the plans of the statements whose predicates may lack an index (P4's five, the eviction's merges deletes),
// the registration's two probes, the completeness inputs and the capture's largest span, over a small seed on both engines:
// SQLite's EXPLAIN QUERY PLAN and MariaDB's EXPLAIN FORMAT=JSON, one block per statement. It asserts no plan.
func TestExplainPlans(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		explainAll(t, c, "small", seedPlans(t, c, smallPlans))
	})
}

// TestExplainPlansLarge logs the same plans over a large seed when TRACEKEEP_EXPLAIN is set:
// SQLite's before and after ANALYZE, and MariaDB's EXPLAIN and ANALYZE FORMAT=JSON, with the rows each step read, after ANALYZE TABLE.
func TestExplainPlansLarge(t *testing.T) {
	t.Parallel()
	if os.Getenv(envExplain) == "" {
		t.Skipf("%s not set: the large seed's plans are not recorded", envExplain)
	}
	engines(t, func(t *testing.T, c *Catalog) {
		qs := seedPlans(t, c, largePlans)
		if c.Engine() == EngineSQLite {
			explainAll(t, c, "large", qs)
			exec(t, c, `ANALYZE`)
			explainAll(t, c, "large, analyzed", qs)

			return
		}
		analyzeTables(t, c)
		explainAll(t, c, "large", qs)
		analyzeAll(t, c, qs)
	})
}

// seedPlans seeds s's population and returns the statements to explain, each with arguments that select from it:
// a capture with its packs, ranges, losses, parked versions, evicted ranges, merges of hour 2 and conflicts in its first two packs,
// scope events and audit rows, and a second capture closed with a crash gap.
func seedPlans(tb testing.TB, c *Catalog, s planSeed) []planned {
	tb.Helper()
	captures := seedCaptures(tb, c, 1, 2)
	capture := captures[0]
	packs, next := seedPacks(tb, c, capture, packsSeed{hour0: 1, hours: s.hours, packs: s.packs, ranges: s.ranges, span: 10, seq0: 1 << 20})
	seedConflicts(tb, c, capture, packs[0], packs[1], 1<<20, s.n)
	seedAudit(tb, c, s.n, captures, []string{"alice", "bob"}, 1)
	seedEvents(tb, c, capture, 1, s.n, 1, 1)
	seedMerges(tb, c, capture, 2, s.n, 1, 0, 0)
	parked := make([]uint64, s.parked)
	for i := range parked {
		parked[i] = next + uint64(i) //nolint:gosec // i is not negative
	}
	seedParked(tb, c, capture, s.hours+1, parked, string(VersionParked))
	evicted := make([]Range, s.evicted)
	for i := range evicted {
		from := uint64(i) * 10                                                                         //nolint:gosec // i is not negative
		evicted[i] = Range{SeqFrom: from, SeqTo: from + 9, TSFrom: int64(from), TSTo: int64(from) + 9} //nolint:gosec // a seeded seq is small
	}
	seedEvicted(tb, c, capture, 0, evicted)
	losses := make([]Loss, s.losses)
	for i := range losses {
		seq := uint64(1<<19) + uint64(i) //nolint:gosec // i is not negative
		losses[i] = Loss{SeqFrom: &seq, SeqTo: &seq}
	}
	seedLosses(tb, c, packs[0], losses)
	seedCloses(tb, c, captures[1:], 1, 1, 1)

	top := int64(next) - 1 //nolint:gosec // a seeded seq is small
	id, d := capture[:], int64(time.Hour)
	other := newID(tb)

	return []planned{
		{name: "auditByCaptureQuery", query: auditByCaptureQuery, args: []any{id}},
		{name: "auditByActorQuery", query: auditByActorQuery, args: []any{"alice"}},
		{name: "pruneFloorQuery", query: pruneFloorQuery, args: []any{int64(s.n / 2)}},
		{name: "deleteCandidatesQuery", query: deleteCandidatesQuery, args: []any{d, d + 1}},
		{name: "evictConflicts select", query: `SELECT pack_id, other_pack_id` + evictConflictsWhere, args: []any{id, int64(1), id, int64(1)}},
		{name: "evictConflicts delete", query: `DELETE` + evictConflictsWhere, args: []any{id, int64(1), id, int64(1)}},
		{name: "evictMergeInputsQuery", query: evictMergeInputsQuery, args: []any{id, int64(2)}},
		{name: "evictMergesQuery", query: evictMergesQuery, args: []any{id, int64(2)}},
		{name: "overlapQuery", query: overlapQuery, args: []any{id, other[:], top, top - 9}},
		{name: "heldSeqQuery", query: heldSeqQuery, args: []any{id, top, top}},
		{name: "frontierQuery", query: frontierQuery, args: []any{id}},
		{name: "retainedQuery", query: retainedQuery, args: []any{id}},
		{name: "evictedQuery", query: evictedQuery, args: []any{id}},
		{name: "parkedQuery", query: parkedQuery, args: []any{id}},
		{name: "lossesQuery", query: lossesQuery, args: []any{id}},
		{name: "crashGapsQuery", query: crashGapsQuery, args: []any{captures[1][:]}},
		{name: "conflictsQuery", query: conflictsQuery, args: []any{id}},
		{name: "maxSpanQuery", query: maxSpanQuery, args: []any{id}},
	}
}

// maxSpanQuery is the capture's largest stored span, which bounds its range probes from below.
const maxSpanQuery = `SELECT MAX(span) FROM pack_ranges WHERE capture_id = ?`

// The capture's largest span is one seek of pack_ranges_capture_span on both engines:
// SQLite searches that index, and MariaDB answers MAX from it while optimizing, reading no table.
// Only the index's name or MariaDB's "optimized away" is asserted, the rest of a plan's text varying by version.
func TestMaxSpanPlan(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		qs := seedPlans(t, c, smallPlans)
		q := qs[len(qs)-1]
		plan := explain(t, c, q)
		t.Logf("=== plan %s %s\n%s", c.Engine(), q.name, plan)
		if !strings.Contains(plan, "pack_ranges_capture_span") && !strings.Contains(plan, "optimized away") {
			t.Errorf("%s's plan of %s uses neither pack_ranges_capture_span nor an optimized-away MAX", c.Engine(), q.query)
		}
	})
}

// explainAll logs each statement's plan on c's write pool, which on SQLite is the connection a write explains on and sees ANALYZE's statistics.
func explainAll(t *testing.T, c *Catalog, tier string, qs []planned) {
	t.Helper()
	for _, q := range qs {
		t.Logf("=== plan %s (%s) %s\n%s", c.Engine(), tier, q.name, explain(t, c, q))
	}
}

// explain is q's plan on c's write pool: SQLite's EXPLAIN QUERY PLAN or MariaDB's EXPLAIN FORMAT=JSON.
func explain(t *testing.T, c *Catalog, q planned) string {
	t.Helper()
	var plan string
	var err error
	if c.Engine() == EngineSQLite {
		plan, err = sqlitePlan(t, c.write, q)
	} else {
		err = c.write.GetContext(t.Context(), &plan, `EXPLAIN FORMAT=JSON `+interpolate(t, q.query, q.args))
	}
	if err != nil {
		t.Fatalf("explain %s: %v", q.name, err)
	}

	return plan
}

// analyzeTables refreshes MariaDB's statistics of the tables the harness explains, which a bulk seed may leave behind.
func analyzeTables(t *testing.T, c *Catalog) {
	t.Helper()
	rows, err := c.write.QueryContext(t.Context(), `ANALYZE TABLE audit_log, scope_events, merges, merge_inputs, packs, pack_ranges, conflicts,
		parked, evicted_ranges, pack_losses, capture_closes, captures`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil || n == 0 {
		t.Fatalf("ANALYZE TABLE reported %d rows, %v", n, err)
	}
}

// analyzeAll runs each statement under MariaDB's ANALYZE FORMAT=JSON and logs the rows each step read,
// inside a transaction rolled back, so a statement that writes leaves the seed as it was.
func analyzeAll(t *testing.T, c *Catalog, qs []planned) {
	t.Helper()
	for _, q := range qs {
		tx, err := c.write.BeginTxx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var plan string
		err = tx.GetContext(t.Context(), &plan, `ANALYZE FORMAT=JSON `+interpolate(t, q.query, q.args))
		_ = tx.Rollback()
		if err != nil {
			t.Fatalf("analyze %s: %v", q.name, err)
		}
		t.Logf("=== analyze %s (large) %s\n%s", c.Engine(), q.name, plan)
	}
}

// sqlitePlan is SQLite's EXPLAIN QUERY PLAN of q as the shell prints it, one detail row a line indented under its parent.
func sqlitePlan(t *testing.T, db *sqlx.DB, q planned) (string, error) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `EXPLAIN QUERY PLAN `+q.query, q.args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	depth := map[int]int{}
	var b strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			return "", err
		}
		depth[id] = depth[parent] + 1
		fmt.Fprintf(&b, "%s%s\n", strings.Repeat("  ", depth[id]-1), detail)
	}

	return strings.TrimSuffix(b.String(), "\n"), rows.Err()
}

// interpolate writes args into query's placeholders as SQL literals, since an EXPLAIN is not prepared with them on every server:
// a byte slice as a hex literal, a string quoted, an integer in decimal. The statements it serves hold no "?" but their placeholders.
func interpolate(tb testing.TB, query string, args []any) string {
	tb.Helper()
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r != '?' {
			b.WriteRune(r)
			continue
		}
		if n == len(args) {
			tb.Fatalf("interpolate: more placeholders than the %d arguments", len(args))
		}
		switch v := args[n].(type) {
		case []byte:
			b.WriteString("X'" + hex.EncodeToString(v) + "'")
		case string:
			b.WriteString("'" + strings.ReplaceAll(v, "'", "''") + "'")
		case int64:
			b.WriteString(strconv.FormatInt(v, 10))
		default:
			tb.Fatalf("interpolate: argument %d of type %T", n, v)
		}
		n++
	}
	if n != len(args) {
		tb.Fatalf("interpolate: %d placeholders for %d arguments", n, len(args))
	}

	return b.String()
}

func TestInterpolate(t *testing.T) {
	t.Parallel()
	got := interpolate(t, `SELECT a FROM t WHERE b = ? AND c = ? AND d > ?`, []any{[]byte{0xab, 1}, "o'k", int64(-7)})
	if want := `SELECT a FROM t WHERE b = X'ab01' AND c = 'o''k' AND d > -7`; got != want {
		t.Errorf("interpolate = %s, want %s", got, want)
	}
}
