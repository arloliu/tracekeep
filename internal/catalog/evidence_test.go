package catalog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// perDelivery is registration as it ran before its evidence was read in sets, kept as the oracle the set-based path is held to:
// every delivery resolved and its version's state read one at a time, a statement for each range, loss, receipt, claim object and parked version.
func perDelivery(ctx context.Context, c *Catalog, p *Pack, r Registration) (PackOutcome, error) {
	var out PackOutcome
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		out = 0
		row, err := c.lockCapture(ctx, tx, p.Capture)
		if err != nil {
			return err
		}
		o, done, err := c.guards(ctx, tx, p, r, row)
		if !done && err == nil {
			o, done, err = perDeliveryGuard(ctx, c, tx, p, r.Evidence)
		}
		if done || err != nil {
			out = o
			if err == nil && o != BeyondHorizon && o != LostToTermination {
				err = errRollback
			}

			return err
		}
		if err := perRowPack(ctx, c, tx, p); err != nil {
			return err
		}
		scope, err := c.upsertScope(ctx, tx, p)
		if err != nil {
			return err
		}
		conflicts, err := c.recordConflicts(ctx, tx, p, scope.revision)
		if err != nil {
			return err
		}
		if err := perRowReceipts(ctx, c, tx, p, r.Evidence); err != nil {
			return err
		}
		if err := perDeliveryStage(ctx, c, tx, r.Evidence); err != nil {
			return err
		}
		if err := c.updateCapture(ctx, tx, p, row); err != nil {
			return err
		}
		out = Registered

		return c.emit(ctx, tx, append(scope.events, conflicts...))
	}))

	return out, err
}

func perDeliveryGuard(ctx context.Context, c *Catalog, tx *sqlx.Tx, p *Pack, e *Evidence) (PackOutcome, bool, error) {
	for i := range e.Deliveries {
		d := &e.Deliveries[i]
		res, _, err := resolveIn(ctx, tx, &d.Version, d.Incarnation, d.StreamSeq)
		if err != nil {
			return 0, true, err
		}
		out, reason := PackOutcome(0), ""
		switch res {
		case ResolvedTerminated, CoveredByOrder:
			out, reason = LostToTermination, "terminated"
		case Unresolved:
			state, err := readVersionState(ctx, tx, &d.Version)
			if err != nil && !errors.Is(err, ErrUnknownVersion) {
				return 0, true, err
			}
			if state == VersionEvictedHour {
				out, reason = BeyondHorizon, "beyond-retention"
			}
		case ResolvedStaged:
		default:
			return 0, true, fmt.Errorf("unknown resolution %d", res)
		}
		if out != 0 {
			_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO tombstones (object_key, reason, at) VALUES (?, ?, ?)`, p.Key, reason, c.now())
			return out, true, err
		}
	}

	return 0, false, nil
}

func perRowPack(ctx context.Context, c *Catalog, tx *sqlx.Tx, p *Pack) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO packs (pack_id, capture_id, tool_id, hour, role, object_key, size, seq_start, format_minor,
		state, adopted, registered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged', ?, ?)`,
		p.ID[:], p.Capture[:], p.ToolID, p.Hour, p.Role, p.Key, p.Size, int64(p.SeqStart), p.FormatMinor, false, c.now()) //nolint:gosec // a test seq fits
	if err != nil {
		return err
	}
	for _, rg := range p.Ranges {
		_, err := tx.ExecContext(ctx, `INSERT INTO pack_ranges (pack_id, capture_id, seq_from, seq_to, ts_from, ts_to) VALUES (?, ?, ?, ?, ?, ?)`,
			p.ID[:], p.Capture[:], int64(rg.SeqFrom), int64(rg.SeqTo), rg.TSFrom, rg.TSTo) //nolint:gosec // a test seq fits
		if err != nil {
			return err
		}
	}
	for i, l := range p.Losses {
		_, err := tx.ExecContext(ctx, `INSERT INTO pack_losses (pack_id, ord, seq_from, seq_to, ts_from, ts_to) VALUES (?, ?, ?, ?, ?, ?)`,
			p.ID[:], i, signed(l.SeqFrom), signed(l.SeqTo), l.TSFrom, l.TSTo)
		if err != nil {
			return err
		}
	}

	return nil
}

func perRowReceipts(ctx context.Context, c *Catalog, tx *sqlx.Tx, p *Pack, e *Evidence) error {
	for _, rc := range e.Receipts {
		_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO claim_records (capture_id, seq, header, payload_sha256, hour, first_pack_id)
			VALUES (?, ?, ?, ?, ?, ?)`, p.Capture[:], int64(rc.Seq), rc.Header[:], rc.PayloadSHA256[:], p.Hour, p.ID[:]) //nolint:gosec // a test seq fits
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, c.d.insertIgnore+` INTO claim_objects (object_key, capture_id, seq, hour, size, state)
			VALUES (?, ?, ?, ?, ?, 'live')`, rc.ObjectKey, p.Capture[:], int64(rc.Seq), p.Hour, rc.ObjectSize) //nolint:gosec // a test seq fits
		if err != nil {
			return err
		}
	}

	return nil
}

func perDeliveryStage(ctx context.Context, c *Catalog, tx *sqlx.Tx, e *Evidence) error {
	for _, d := range e.Deliveries {
		v := &d.Version
		_, err := tx.ExecContext(ctx, `UPDATE parked SET state = 'staged', resolved_at = ?
			WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ? AND state IN ('parking', 'parked')`,
			c.now(), v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:]) //nolint:gosec // a test seq fits
		if err != nil {
			return err
		}
	}

	return nil
}

// deliveryKind is a delivery of the evidence and what the catalog holds of it:
// its version (seq and header variant), its incarnation and stream seq, its version's parked state (none when empty),
// and the outcome of a registration whose evidence is that delivery alone.
// The capture of every scenario holds the range 0-9 and three termination orders:
// active on incarnation 1 cut off at 50, spent on incarnation 2 at 50, active on incarnation 3 at 20.
type deliveryKind struct {
	name        string
	seq         uint64
	variant     byte
	incarnation int64
	stream      int64
	parked      VersionState
	alone       PackOutcome
}

var deliveryKinds = []deliveryKind{
	{"plain", 20, 0, 1, 100, "", Registered},
	{"held, covered at the cut-off", 3, 0, 1, 50, "", Registered},
	{"unheld, covered below the cut-off", 30, 0, 1, 49, "", LostToTermination},
	{"above the cut-off", 31, 0, 1, 51, "", Registered},
	{"under a spent order", 32, 0, 2, 1, "", Registered},
	{"covered by another incarnation's order", 33, 0, 3, 20, "", LostToTermination},
	{"above another incarnation's cut-off", 34, 0, 3, 21, "", Registered},
	{"an incarnation with no order", 35, 0, 4, 1, "", Registered},
	{"staged, covered", 36, 0, 1, 1, VersionStaged, Registered},
	{"terminated", 37, 0, 1, 100, VersionTerminated, LostToTermination},
	{"parking", 38, 0, 1, 100, VersionParking, Registered},
	{"parking, delivered again", 38, 0, 1, 102, VersionParking, Registered},
	{"parked", 39, 0, 1, 100, VersionParked, Registered},
	{"parked, covered", 40, 0, 1, 10, VersionParked, LostToTermination},
	{"parked, covered and held", 4, 0, 1, 10, VersionParked, Registered},
	{"evicted hour", 41, 0, 1, 100, VersionEvictedHour, BeyondHorizon},
	{"evicted hour, covered and held", 6, 0, 1, 10, VersionEvictedHour, Registered},
	{"evicted hour, covered", 42, 0, 1, 10, VersionEvictedHour, LostToTermination},
	{"one version of a seq, parked", 43, 0, 1, 100, VersionParked, Registered},
	{"another version of that seq", 43, 1, 1, 101, "", Registered},
	{"the other version of a terminated one", 44, 1, 1, 100, "", Registered},
}

// undelivered are parked versions no delivery names: a terminated one beside a delivered version of its seq,
// and a parked one inside the evidence's seqs.
var undelivered = []deliveryKind{
	{"terminated, undelivered", 44, 0, 1, 0, VersionTerminated, 0},
	{"parked, undelivered", 25, 0, 1, 0, VersionParked, 0},
}

func (k deliveryKind) identity(capture tracepack.UUID) Identity {
	return Identity{
		Version:     VersionKey{Capture: capture, Seq: k.seq, Header: [44]byte{byte(k.seq), k.variant}, Digest: [32]byte{1}},
		Incarnation: k.incarnation, StreamSeq: k.stream, Hour: 2,
	}
}

// evidenceScenarios are the evidence the equivalence runs: each kind alone; every kind that registers, then each refusing kind last;
// every kind that registers, then each ordered pair of refusing kinds; and every kind in three seeded orders.
func evidenceScenarios() [][]deliveryKind {
	var ok, refusing []deliveryKind
	var out [][]deliveryKind
	for _, k := range deliveryKinds {
		out = append(out, []deliveryKind{k})
		if k.alone == Registered {
			ok = append(ok, k)
		} else {
			refusing = append(refusing, k)
		}
	}
	for _, r := range refusing {
		out = append(out, append(slices.Clone(ok), r))
	}
	for _, a := range refusing {
		for _, b := range refusing {
			if a.name != b.name {
				out = append(out, append(slices.Clone(ok), a, b))
			}
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 3 {
		all := slices.Clone(deliveryKinds)
		rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
		out = append(out, all)
	}

	return out
}

// evidenceCapture writes, identically on c, what every scenario's capture holds before its registration:
// a pack of hour 1 holding 0-9 with the receipt of seq 5, the three orders, and the parked versions kinds and undelivered name.
func evidenceCapture(t *testing.T, c *Catalog, capture, held tracepack.UUID, kinds []deliveryKind) {
	t.Helper()
	h := packOf(t, capture, 1, [2]uint64{0, 9})
	h.ID, h.Key = held, "staging/"+held.String()
	ev := &Evidence{Receipts: []Receipt{receiptOf(capture, deliveryKind{seq: 5})}}
	if out := register(t, c, h, Registration{Evidence: ev}); out != Registered {
		t.Fatalf("the capture's first pack = %v", out)
	}
	for i, o := range []struct {
		inc, cut int64
		state    string
	}{{1, 50, "active"}, {2, 50, "spent"}, {3, 20, "active"}} {
		exec(t, c, `INSERT INTO termination_orders (action_id, capture_id, incarnation_id, cut_off, state) VALUES (?, ?, ?, ?, ?)`,
			fmt.Sprintf("%s/%d", capture, i), capture[:], o.inc, o.cut, o.state)
	}
	seen := map[VersionKey]bool{}
	for _, k := range append(slices.Clone(kinds), undelivered...) {
		d := k.identity(capture)
		if k.parked == "" || seen[d.Version] {
			continue
		}
		seen[d.Version] = true
		park(t, c, &d, string(k.parked))
	}
}

// receiptOf is the receipt of k's version, its claim object named by its capture and seq.
func receiptOf(capture tracepack.UUID, k deliveryKind) Receipt {
	d := k.identity(capture)

	return Receipt{Seq: k.seq, Header: d.Version.Header, PayloadSHA256: d.Version.Digest, ObjectKey: fmt.Sprintf("claims/%s/%d", capture, k.seq), ObjectSize: 10}
}

// evidencePack is the pack of hour 2 holding 0-99 that registers kinds' deliveries,
// with a receipt of every even seq delivered, the receipt of seq 5 the first pack wrote, and one receipt twice.
func evidencePack(t *testing.T, capture, id tracepack.UUID, kinds []deliveryKind) (*Pack, *Evidence) {
	t.Helper()
	p := packOf(t, capture, 2, [2]uint64{0, 99})
	p.ID, p.Key = id, "staging/"+id.String()
	p.Losses = []Loss{{SeqFrom: ptr(uint64(100)), SeqTo: ptr(uint64(101))}, {}}
	ev := &Evidence{Receipts: []Receipt{receiptOf(capture, deliveryKind{seq: 5})}}
	for _, k := range kinds {
		ev.Deliveries = append(ev.Deliveries, k.identity(capture))
		if k.seq%2 == 0 {
			ev.Receipts = append(ev.Receipts, receiptOf(capture, k))
		}
	}
	ev.Receipts = append(ev.Receipts, ev.Receipts[len(ev.Receipts)-1])

	return p, ev
}

// equivalenceTables are the tables a registration writes or reads, dumped whole to compare two catalogs.
var equivalenceTables = []string{"captures", "packs", "pack_ranges", "pack_losses", "claim_records", "claim_objects", "parked",
	"conflicts", "scopes", "scope_events", "tombstones", "termination_orders", "counters"}

// dump is every row of c's tables, each a line of its columns, sorted.
func dump(t *testing.T, c *Catalog, tables ...string) []string {
	t.Helper()
	tabled := make([][]string, len(tables))
	for i, table := range tables {
		tabled[i] = dumpTable(t, c, table)
	}
	out := slices.Concat(tabled...)
	slices.Sort(out)

	return out
}

// dumpTable is every row of c's table, each a line of its columns.
func dumpTable(t *testing.T, c *Catalog, table string) []string {
	t.Helper()
	rows, err := c.write.QueryxContext(t.Context(), "SELECT * FROM "+table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.SliceScan()
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		b.WriteString(table)
		for _, v := range vals {
			if bs, ok := v.([]byte); ok {
				v = hex.EncodeToString(bs)
			}
			fmt.Fprintf(&b, " %v", v)
		}
		out = append(out, b.String())
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return out
}

// Reading the evidence in sets decides as reading it a delivery at a time did:
// over each kind of delivery alone, each refusal last, each ordered pair of refusals and shuffled evidence,
// a registration through RegisterPack and one through the per-delivery oracle, on two catalogs alike, return the same outcome
// and leave the same rows: the tombstone and its reason, the staged versions, the receipts naming their first pack.
func TestEvidenceInSetsMatchesPerDelivery(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, sets *Catalog) {
		per := newCatalog(t, sets.Engine())
		for _, c := range []*Catalog{sets, per} {
			c.now = func() int64 { return 1000 }
		}
		for i, kinds := range evidenceScenarios() {
			capture, held, id := newID(t), newID(t), newID(t)
			for _, c := range []*Catalog{sets, per} {
				evidenceCapture(t, c, capture, held, kinds)
			}
			p, ev := evidencePack(t, capture, id, kinds)
			got, err := sets.RegisterPack(t.Context(), p, Registration{Evidence: ev})
			if err != nil {
				t.Fatal(err)
			}
			want, err := perDelivery(t.Context(), per, p, Registration{Evidence: ev})
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Errorf("scenario %d (%s, ...): RegisterPack = %v, the per-delivery path %v", i, kinds[0].name, got, want)
			}
			if len(kinds) == 1 && got != kinds[0].alone {
				t.Errorf("%s alone = %v, want %v", kinds[0].name, got, kinds[0].alone)
			}
		}
		if a, b := dump(t, sets, equivalenceTables...), dump(t, per, equivalenceTables...); !slices.Equal(a, b) {
			for _, l := range diffLines(a, b) {
				t.Error(l)
			}
		}
	})
}

// diffLines lists the lines only one of two sorted dumps holds, at most twenty.
func diffLines(a, b []string) []string {
	var out []string
	for _, l := range a {
		if _, found := slices.BinarySearch(b, l); !found {
			out = append(out, "only in sets: "+l)
		}
	}
	for _, l := range b {
		if _, found := slices.BinarySearch(a, l); !found {
			out = append(out, "only per delivery: "+l)
		}
	}

	return out[:min(len(out), 20)]
}

// boundaryPack is a pack of n one-seq ranges from seq 1000, a seq apart, with n losses, and evidence of a delivery and a receipt of each ranged seq,
// every seventh delivery's version parked.
func boundaryPack(t *testing.T, capture tracepack.UUID, n int) (*Pack, *Evidence, []Identity) {
	t.Helper()
	p := packOf(t, capture, 1, [2]uint64{1000, 1000})
	p.Ranges, p.SeqStart = nil, 1000
	ev := &Evidence{}
	var parked []Identity
	for i := range n {
		seq := 1000 + 2*uint64(i)                                                                          //nolint:gosec // i is not negative
		p.Ranges = append(p.Ranges, Range{SeqFrom: seq, SeqTo: seq, TSFrom: int64(seq), TSTo: int64(seq)}) //nolint:gosec // a test seq fits
		p.Losses = append(p.Losses, Loss{SeqFrom: ptr(seq + 1), SeqTo: ptr(seq + 1)})
		k := deliveryKind{seq: seq, incarnation: 1, stream: int64(seq)} //nolint:gosec // a test seq fits
		d := k.identity(capture)
		ev.Deliveries = append(ev.Deliveries, d)
		ev.Receipts = append(ev.Receipts, receiptOf(capture, k))
		if i%7 == 0 {
			parked = append(parked, d)
		}
	}

	return p, ev, parked
}

// The multi-row statements split at 900 placeholders: n ranges, losses, receipts and claim objects of six columns each,
// at 149, 150 and 151 rows (894, 900 and 906 placeholders) and past two statements, write every row as the per-delivery path does.
func TestEvidenceAtTheStatementBoundary(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, sets *Catalog) {
		per := newCatalog(t, sets.Engine())
		for _, c := range []*Catalog{sets, per} {
			c.now = func() int64 { return 1000 }
		}
		for _, n := range []int{149, 150, 151, 301} {
			capture := newID(t)
			p, ev, parked := boundaryPack(t, capture, n)
			for _, c := range []*Catalog{sets, per} {
				for i := range parked {
					park(t, c, &parked[i], string(VersionParked))
				}
			}
			if out, err := sets.RegisterPack(t.Context(), p, Registration{Evidence: ev}); err != nil || out != Registered {
				t.Fatalf("%d rows: RegisterPack = %v, %v", n, out, err)
			}
			if out, err := perDelivery(t.Context(), per, p, Registration{Evidence: ev}); err != nil || out != Registered {
				t.Fatalf("%d rows: the per-delivery path = %v, %v", n, out, err)
			}
			for _, q := range []string{
				`SELECT COUNT(*) FROM pack_ranges WHERE capture_id = ?`,
				`SELECT COUNT(*) FROM claim_records WHERE capture_id = ?`,
				`SELECT COUNT(*) FROM claim_objects WHERE capture_id = ?`,
				`SELECT COUNT(*) FROM pack_losses l JOIN packs k ON k.pack_id = l.pack_id WHERE k.capture_id = ?`,
			} {
				if got := count(t, sets, q, capture[:]); got != n {
					t.Errorf("%d rows: %s = %d", n, q, got)
				}
			}
			if got := count(t, sets, `SELECT COUNT(*) FROM parked WHERE capture_id = ? AND state = 'staged'`, capture[:]); got != len(parked) {
				t.Errorf("%d rows: %d versions staged, want %d", n, got, len(parked))
			}
		}
		if a, b := dump(t, sets, equivalenceTables...), dump(t, per, equivalenceTables...); !slices.Equal(a, b) {
			for _, l := range diffLines(a, b) {
				t.Error(l)
			}
		}
	})
}

// A registration whose last multi-row statement fails, the last claim object of its second statement, writes nothing:
// no pack, range, loss, receipt or claim object of an earlier statement, no staged version, no scope.
func TestEvidenceFailingInTheLastStatementWritesNothing(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := newID(t)
		p, ev, parked := boundaryPack(t, capture, 151)
		for i := range parked {
			park(t, c, &parked[i], string(VersionParked))
		}
		last := &ev.Receipts[len(ev.Receipts)-1]
		last.ObjectKey = "claims/fail"
		if c.Engine() == EngineSQLite {
			exec(t, c, `CREATE TRIGGER fail_claim BEFORE INSERT ON claim_objects WHEN NEW.object_key = 'claims/fail' BEGIN SELECT RAISE(ABORT, 'injected'); END`)
		} else {
			exec(t, c, `CREATE TRIGGER fail_claim BEFORE INSERT ON claim_objects FOR EACH ROW
				BEGIN IF NEW.object_key = 'claims/fail' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected'; END IF; END`)
		}
		before := dump(t, c, equivalenceTables...)
		if out, err := c.RegisterPack(t.Context(), p, Registration{Evidence: ev}); err == nil {
			t.Fatalf("RegisterPack with a failing claim object = %v, want an error", out)
		}
		if after := dump(t, c, equivalenceTables...); !slices.Equal(before, after) {
			for _, l := range diffLines(after, before) {
				t.Error(l)
			}
		}
		var state sql.NullString
		if err := c.read.GetContext(t.Context(), &state, `SELECT state FROM parked WHERE capture_id = ? AND seq = 1000`, capture[:]); err != nil || state.String != string(VersionParked) {
			t.Errorf("the first parked version after the failure = %v, %v; want parked", state, err)
		}
	})
}

// statements records the statements an insert runs, with their arguments, and runs none.
type statements struct {
	queries []string
	args    [][]any
}

func (s *statements) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	s.queries, s.args = append(s.queries, query), append(s.args, args)

	return driver.RowsAffected(len(args) / 6), nil
}

// Every statement insertValues runs binds at most 900 placeholders, every one counted, as many as its arguments,
// a whole number of rows; together they bind every argument once, in order, in as few statements as that allows.
func TestInsertValuesWithin900Placeholders(t *testing.T) {
	t.Parallel()
	for _, cols := range []int{6, 7} {
		for _, rows := range []int{0, 1, 149, 150, 151, 300, 301} {
			args := make([]any, rows*cols)
			for i := range args {
				args[i] = i
			}
			var s statements
			if err := insertValues(t.Context(), &s, "INSERT INTO t (c)", cols, args); err != nil {
				t.Fatal(err)
			}
			var all []any
			for i, q := range s.queries {
				if n := strings.Count(q, "?"); n > maxParams || n != len(s.args[i]) || n%cols != 0 {
					t.Errorf("%d columns, %d rows: statement %d binds %d placeholders for %d arguments", cols, rows, i, n, len(s.args[i]))
				}
				all = append(all, s.args[i]...)
			}
			per := maxParams / cols * cols
			if want := (len(args) + per - 1) / per; len(s.queries) != want || !slices.Equal(all, args) {
				t.Errorf("%d columns, %d rows: %d statements binding %d arguments, want %d binding all %d in order", cols, rows, len(s.queries), len(all), want, len(args))
			}
		}
	}
}
