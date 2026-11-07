package catalog

import (
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/classify"

	"github.com/arloliu/tracekeep/internal/procconfig"
)

// The bulk seeders below write a benchmark's or a harness's population directly, in multi-row INSERTs inside one write transaction,
// where registering it through the domain transactions would take longer than the measurement.
// Each asserts what it seeded; the rows they write are those the domain transactions write, their descriptors and configurations decodable.

// seedVars bounds the arguments of one multi-row INSERT: under SQLite's 32,766 variables,
// and at most 1,000 rows of a few hundred bytes each, far under MariaDB's packet.
const seedVars = 30000

// seedTx runs f in one write transaction on c's write pool and commits it.
func seedTx(tb testing.TB, c *Catalog, f func(tx *sqlx.Tx)) {
	tb.Helper()
	tx, err := c.write.BeginTxx(tb.Context(), nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	f(tx)
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

// insertRows inserts rows into table's cols on tx, in multi-row INSERTs of at most seedVars arguments.
func insertRows(tb testing.TB, tx *sqlx.Tx, table string, cols []string, rows [][]any) {
	tb.Helper()
	per := min(1000, seedVars/len(cols))
	tuple := "(?" + strings.Repeat(", ?", len(cols)-1) + ")"
	for start := 0; start < len(rows); start += per {
		batch := rows[start:min(start+per, len(rows))]
		var q strings.Builder
		q.WriteString("INSERT INTO " + table + " (" + strings.Join(cols, ", ") + ") VALUES ")
		args := make([]any, 0, len(batch)*len(cols))
		for i, r := range batch {
			if i > 0 {
				q.WriteString(", ")
			}
			q.WriteString(tuple)
			args = append(args, r...)
		}
		if _, err := tx.ExecContext(tb.Context(), q.String(), args...); err != nil {
			tb.Fatalf("seed %s: %v", table, err)
		}
	}
}

// seedRows inserts rows into table's cols in one write transaction and asserts the table grew by len(rows).
func seedRows(tb testing.TB, c *Catalog, table string, cols []string, rows [][]any) {
	tb.Helper()
	before := count(tb, c, "SELECT COUNT(*) FROM "+table)
	seedTx(tb, c, func(tx *sqlx.Tx) { insertRows(tb, tx, table, cols, rows) })
	if got := count(tb, c, "SELECT COUNT(*) FROM "+table); got != before+len(rows) {
		tb.Fatalf("seeded %d rows into %s, which grew from %d to %d", len(rows), table, before, got)
	}
}

// seedConfig activates a decodable processing configuration and leases a member executing it, so admissions bind it; it returns its id.
func seedConfig(tb testing.TB, c *Catalog) int64 {
	tb.Helper()
	content := procconfig.Content{Classifier: classify.Name(), MaxFrameLen: 1 << 20}
	cfg := activate(tb, c, string(content.Encode()))
	if _, err := c.Register(tb.Context(), "r", []int64{cfg}, ttl); err != nil {
		tb.Fatal(err)
	}

	return cfg
}

// seedCaptures writes n open captures of equipment T1, the i-th on device eqp-(i mod devices) with its own recorder instance and origin 1+i,
// each with a decodable start descriptor and bound to a decodable configuration, and returns them in order.
func seedCaptures(tb testing.TB, c *Catalog, devices, n int) []tracepack.UUID {
	tb.Helper()
	infos := seedCaptureInfos(tb, c, devices, n)
	ids := make([]tracepack.UUID, n)
	for i := range infos {
		ids[i] = infos[i].ID
	}

	return ids
}

// seedCaptureInfos is seedCaptures, returning each capture as ListCaptures reports it before any registration.
func seedCaptureInfos(tb testing.TB, c *Catalog, devices, n int) []CaptureInfo {
	tb.Helper()
	const equipment = "T1"
	cfg := seedConfig(tb, c)
	infos := make([]CaptureInfo, n)
	rows := make([][]any, n)
	for i := range n {
		id := newID(tb)
		// Each device is recorded by two sources, so a listing groups series, not devices.
		// Each capture keeps a recorder instance of its own: the catalog never checks the derivation, which admission does,
		// and distinct instances spare the listings' oracles every overlap but the one varyListing makes.
		s := spec{instance: newID(tb), device: fmt.Sprintf("eqp-%d", i%devices), source: fmt.Sprintf("gw-%d", i/devices%2), origin: 1 + int64(i), equipmentID: equipment}
		rows[i] = []any{id[:], "open", descriptorBytes(tb, s), equipment, s.instance[:], s.device, s.source, s.origin, cfg, 1, 0}
		infos[i] = CaptureInfo{ID: id, State: StateOpen, Device: s.device, Source: s.source, RecorderInstanceID: s.instance, OriginNS: s.origin, ConfigID: &cfg,
			CaptureMethod: "raw-stream", Vantage: "host"} // a seeded row holds no method: read from its descriptor
	}
	seedRows(tb, c, "captures", []string{"capture_id", "state", "descriptor", "equipment_id", "recorder_instance_id", "device", "source", "origin_ns", "config_id", "data_revision", "created_at"}, rows)
	if got := count(tb, c, `SELECT COUNT(*) FROM captures WHERE equipment_id = ? AND state = 'open'`, equipment); got < n {
		tb.Fatalf("seeded %d open captures of %s, found %d", n, equipment, got)
	}

	return infos
}

// packsSeed shapes seedPacks' population of one capture.
type packsSeed struct {
	// hour0 is the first hour, an hour index; the packs fill hours hour0 to hour0+hours-1.
	hour0, hours int64
	// packs is the packs an hour, ranges the ranges a pack and span the seqs a range.
	packs, ranges int
	span          uint64
	// seq0 is the first seq; the ranges are adjacent, seq after seq, in the order of their hours and packs.
	seq0 uint64
	// deletedEvery makes every deletedEvery-th pack deleted, its ranges kept as a deleted pack's are; zero deletes none.
	deletedEvery int
}

// seedPacks writes s's segments of capture, of its equipment, their ranges and their hours' open scopes,
// raises the capture's highest seq to the last one, and returns the packs in order with the seq after the last.
// A range's records are at its hour's start plus the seq's offset in the hour, in nanoseconds.
// An hour of a capture is seeded once: its scope row is inserted, not upserted.
func seedPacks(tb testing.TB, c *Catalog, capture tracepack.UUID, s packsSeed) ([]tracepack.UUID, uint64) {
	tb.Helper()
	equipment := captureEquipment(tb, c, capture)
	var ids []tracepack.UUID
	var packs, ranges, scopes [][]any
	seq, live, deleted := s.seq0, 0, 0
	var lastTS int64
	for h := s.hour0; h < s.hour0+s.hours; h++ {
		first := seq
		scopes = append(scopes, []any{capture[:], h, "open", 1})
		for range s.packs {
			id := newID(tb)
			state := "staged"
			if s.deletedEvery > 0 && (len(ids)+1)%s.deletedEvery == 0 {
				state, deleted = "deleted", deleted+1
			} else {
				live++
			}
			ids = append(ids, id)
			packs = append(packs, []any{id[:], capture[:], equipment, h, RoleSegment, "staging/" + id.String(), 1 << 20, int64(seq), 2, state, false, 0}) //nolint:gosec // a seeded seq is small
			for range s.ranges {
				from, to := seq, seq+s.span-1
				tsFrom, tsTo := hourNS(h)+int64(from-first), hourNS(h)+int64(to-first)                  //nolint:gosec // a seeded seq is small
				ranges = append(ranges, []any{id[:], capture[:], int64(from), int64(to), tsFrom, tsTo}) //nolint:gosec // a seeded seq is small
				seq, lastTS = to+1, tsTo
			}
		}
	}
	seedTx(tb, c, func(tx *sqlx.Tx) {
		insertRows(tb, tx, "packs", []string{"pack_id", "capture_id", "equipment_id", "hour", "role", "object_key", "size", "seq_start", "format_minor", "state", "adopted", "registered_at"}, packs)
		insertRows(tb, tx, "pack_ranges", []string{"pack_id", "capture_id", "seq_from", "seq_to", "ts_from", "ts_to"}, ranges)
		insertRows(tb, tx, "scopes", []string{"capture_id", "hour", "state", "revision"}, scopes)
		if len(ranges) > 0 {
			_, err := tx.ExecContext(tb.Context(), `UPDATE captures SET highest_seq = ?, highest_seq_ts = ? WHERE capture_id = ? AND (highest_seq IS NULL OR highest_seq < ?)`,
				int64(seq-1), lastTS, capture[:], int64(seq-1)) //nolint:gosec // a seeded seq is small
			if err != nil {
				tb.Fatal(err)
			}
		}
	})
	hours := []any{capture[:], s.hour0, s.hour0 + s.hours}
	if got := count(tb, c, `SELECT COUNT(*) FROM packs WHERE capture_id = ? AND hour >= ? AND hour < ? AND state = 'staged'`, hours...); got != live {
		tb.Fatalf("seeded %d live packs, found %d", live, got)
	}
	if got := count(tb, c, `SELECT COUNT(*) FROM packs WHERE capture_id = ? AND hour >= ? AND hour < ? AND state = 'deleted'`, hours...); got != deleted {
		tb.Fatalf("seeded %d deleted packs, found %d", deleted, got)
	}
	if got := count(tb, c, `SELECT COUNT(*) FROM pack_ranges WHERE capture_id = ? AND seq_from >= ? AND seq_to < ?`, capture[:], int64(s.seq0), int64(seq)); got != len(ranges) { //nolint:gosec // a seeded seq is small
		tb.Fatalf("seeded %d ranges, found %d", len(ranges), got)
	}

	return ids, seq
}

// captureEquipment reads capture's equipment.
func captureEquipment(tb testing.TB, c *Catalog, capture tracepack.UUID) string {
	tb.Helper()
	var equipment string
	if err := c.read.GetContext(tb.Context(), &equipment, `SELECT equipment_id FROM captures WHERE capture_id = ?`, capture[:]); err != nil {
		tb.Fatal(err)
	}

	return equipment
}

// seedAudit writes n audit rows, the i-th at at0+i by actors[i mod len] about captures[i mod len], restores as the API writes them.
func seedAudit(tb testing.TB, c *Catalog, n int, captures []tracepack.UUID, actors []string, at0 int64) {
	tb.Helper()
	rows := make([][]any, n)
	for i := range n {
		id := newID(tb)
		rows[i] = []any{id[:], at0 + int64(i), actors[i%len(actors)], "restore", nil, captures[i%len(captures)][:], nil}
	}
	seedRows(tb, c, "audit_log", []string{"audit_id", "at", "actor", "action", "action_id", "capture_id", "detail"}, rows)
}

// seedEvents allocates n scope events of capture's hour from the counter, as emit does, the i-th at at0+i*step,
// and returns the first and last seq; the counter stays at the last, so a later emission follows them.
func seedEvents(tb testing.TB, c *Catalog, capture tracepack.UUID, hour int64, n int, at0, step int64) (first, last int64) {
	tb.Helper()
	seedTx(tb, c, func(tx *sqlx.Tx) {
		if _, err := tx.ExecContext(tb.Context(), `UPDATE counters SET value = value + ? WHERE name = 'scope_events'`, n); err != nil {
			tb.Fatal(err)
		}
		if err := tx.GetContext(tb.Context(), &last, `SELECT value FROM counters WHERE name = 'scope_events'`); err != nil {
			tb.Fatal(err)
		}
		first = last - int64(n) + 1
		rows := make([][]any, n)
		for i := range n {
			rows[i] = []any{first + int64(i), capture[:], hour, "created", 1, at0 + int64(i)*step}
		}
		insertRows(tb, tx, "scope_events", []string{"seq", "capture_id", "hour", "event", "revision", "at"}, rows)
	})
	if got := count(tb, c, `SELECT COUNT(*) FROM scope_events WHERE seq >= ? AND seq <= ?`, first, last); got != n {
		tb.Fatalf("seeded %d events, found %d", n, got)
	}

	return first, last
}

// seedMerges writes n committed merges of the admitted capture's hour at at, each deletable at deleteAt (0 for D after at),
// each with its staged archive and its inputs merged segments; it returns the merges.
func seedMerges(tb testing.TB, c *Catalog, capture tracepack.UUID, hour int64, n, inputs int, at, deleteAt int64) []tracepack.UUID {
	tb.Helper()
	equipment := captureEquipment(tb, c, capture)
	ids := make([]tracepack.UUID, n)
	packs, merges, links := make([][]any, 0, n*(1+inputs)), make([][]any, 0, n), make([][]any, 0, n*inputs)
	pack := func(role, state string) tracepack.UUID {
		id := newID(tb)
		packs = append(packs, []any{id[:], capture[:], equipment, hour, role, "staging/" + id.String(), 1 << 20, 0, 2, state, false, 0})

		return id
	}
	for i := range n {
		ids[i] = newID(tb)
		out := pack(RoleArchive, "staged")
		merges = append(merges, []any{ids[i][:], capture[:], hour, out[:], "committed", at, deleteAt})
		for range inputs {
			in := pack(RoleSegment, "merged")
			links = append(links, []any{ids[i][:], in[:]})
		}
	}
	seedTx(tb, c, func(tx *sqlx.Tx) {
		insertRows(tb, tx, "packs", []string{"pack_id", "capture_id", "equipment_id", "hour", "role", "object_key", "size", "seq_start", "format_minor", "state", "adopted", "registered_at"}, packs)
		insertRows(tb, tx, "merges", []string{"merge_id", "capture_id", "hour", "output_pack_id", "outcome", "at", "delete_at"}, merges)
		insertRows(tb, tx, "merge_inputs", []string{"merge_id", "pack_id"}, links)
	})
	if got := count(tb, c, `SELECT COUNT(*) FROM merges m JOIN merge_inputs i ON i.merge_id = m.merge_id WHERE m.capture_id = ? AND m.hour = ?`, capture[:], hour); got < n*inputs {
		tb.Fatalf("seeded %d merge inputs, found %d", n*inputs, got)
	}

	return ids
}

// seedConflicts writes n suspect conflicts of capture between packs a and b, the i-th over seq seq0+i.
func seedConflicts(tb testing.TB, c *Catalog, capture, a, b tracepack.UUID, seq0 uint64, n int) {
	tb.Helper()
	rows := make([][]any, n)
	for i := range n {
		seq := int64(seq0) + int64(i) //nolint:gosec // a seeded seq is small
		rows[i] = []any{capture[:], seq, seq, a[:], b[:], "suspect"}
	}
	seedRows(tb, c, "conflicts", []string{"capture_id", "seq_from", "seq_to", "pack_id", "other_pack_id", "state"}, rows)
}

// seedClaimObjects writes n claim object rows in state of capture's hour, seqs seq0 onwards, keyed in bucket,
// and returns them as a listing that records them again finds them.
func seedClaimObjects(tb testing.TB, c *Catalog, bucket string, capture tracepack.UUID, hour int64, seq0 uint64, n int, state string) []Listed {
	tb.Helper()
	listed := make([]Listed, n)
	rows := make([][]any, n)
	for i := range n {
		seq := seq0 + uint64(i) //nolint:gosec // i is not negative
		key := fmt.Sprintf("%s/%s.%d", bucket, capture, seq)
		listed[i] = Listed{Key: key, Size: 1 << 10, Attr: &ClaimObject{Key: key, Capture: capture, Seq: seq, Hour: hour, Size: 1 << 10, State: state}}
		rows[i] = []any{key, capture[:], int64(seq), hour, 1 << 10, state} //nolint:gosec // a seeded seq is small
	}
	seedRows(tb, c, "claim_objects", []string{"object_key", "capture_id", "seq", "hour", "size", "state"}, rows)

	return listed
}

// seedUnattributed writes n unattributed objects keyed in bucket claims, first seen at time 1, and returns them as a listing finds them.
func seedUnattributed(tb testing.TB, c *Catalog, n int) []Listed {
	tb.Helper()
	listed := make([]Listed, n)
	rows := make([][]any, n)
	for i := range n {
		key := "claims/foreign-" + rand.Text()
		listed[i] = Listed{Key: key, Size: 1 << 10}
		rows[i] = []any{key, 1 << 10, 1}
	}
	seedRows(tb, c, "unattributed_objects", []string{"object_key", "size", "seen_at"}, rows)

	return listed
}

// seedTombstones writes n tombstones for reason at at and returns their keys.
func seedTombstones(tb testing.TB, c *Catalog, n int, reason string, at int64) []string {
	tb.Helper()
	keys := make([]string, n)
	rows := make([][]any, n)
	for i := range n {
		id := newID(tb)
		keys[i] = "staging/" + id.String()
		rows[i] = []any{keys[i], reason, at}
	}
	seedRows(tb, c, "tombstones", []string{"object_key", "reason", "at"}, rows)

	return keys
}

// seedParked writes a parked version of each of capture's seqs in state, its record at its hour's start plus the seq, as parking would;
// a terminated one names the order a1.
func seedParked(tb testing.TB, c *Catalog, capture tracepack.UUID, hour int64, seqs []uint64, state string) {
	tb.Helper()
	by := sql.NullString{String: "a1", Valid: state == string(VersionTerminated)}
	rows := make([][]any, len(seqs))
	for i, seq := range seqs {
		var header [44]byte
		binary.LittleEndian.PutUint64(header[:8], seq)
		binary.LittleEndian.PutUint64(header[8:16], uint64(hourNS(hour))+seq) //nolint:gosec // a seeded time is positive
		digest := make([]byte, 32)
		_, _ = rand.Read(digest)
		rows[i] = []any{capture[:], int64(seq), header[:], digest, state, "unknown-capture", hour, by, 0} //nolint:gosec // a seeded seq is small
	}
	seedRows(tb, c, "parked", []string{"capture_id", "seq", "header", "payload_digest", "state", "reason", "hour", "terminated_by", "parked_at"}, rows)
}

// seedVersions writes a parked row of each delivery's version, exactly its key, in the state states names for it, as parking leaves it:
// parking or parked unresolved, staged resolved at time 1.
func seedVersions(tb testing.TB, c *Catalog, ds []Identity, states []VersionState) {
	tb.Helper()
	rows := make([][]any, len(ds))
	for i, d := range ds {
		v := &d.Version
		resolved := sql.NullInt64{Int64: 1, Valid: states[i] == VersionStaged}
		rows[i] = []any{v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:], string(states[i]), "unknown-capture", d.Hour, 0, resolved} //nolint:gosec // a seeded seq is small
	}
	seedRows(tb, c, "parked", []string{"capture_id", "seq", "header", "payload_digest", "state", "reason", "hour", "parked_at", "resolved_at"}, rows)
}

// seedEvicted writes capture's evicted ranges of hour, as an eviction leaves them.
func seedEvicted(tb testing.TB, c *Catalog, capture tracepack.UUID, hour int64, ranges []Range) {
	tb.Helper()
	rows := make([][]any, len(ranges))
	for i, r := range ranges {
		rows[i] = []any{capture[:], hour, int64(r.SeqFrom), int64(r.SeqTo), r.TSFrom, r.TSTo} //nolint:gosec // a seeded seq is small
	}
	seedRows(tb, c, "evicted_ranges", []string{"capture_id", "hour", "seq_from", "seq_to", "ts_from", "ts_to"}, rows)
}

// seedLosses writes the losses pack declares, in order.
func seedLosses(tb testing.TB, c *Catalog, pack tracepack.UUID, losses []Loss) {
	tb.Helper()
	rows := make([][]any, len(losses))
	for i, l := range losses {
		rows[i] = []any{pack[:], i, signed(l.SeqFrom), signed(l.SeqTo), l.TSFrom, l.TSTo}
	}
	seedRows(tb, c, "pack_losses", []string{"pack_id", "ord", "seq_from", "seq_to", "ts_from", "ts_to"}, rows)
}

// seedCloses closes each of captures by inactivity at at, its close published with the crash gap [gapStart, gapEnd]
// and a decodable pack meta, as the compactor leaves a capture whose recorder stopped without its stop record.
func seedCloses(tb testing.TB, c *Catalog, captures []tracepack.UUID, at, gapStart, gapEnd int64) {
	tb.Helper()
	meta := descriptorBytes(tb, spec{equipmentID: "T1", device: "eqp"})
	rows := make([][]any, len(captures))
	args := make([]any, 0, len(captures))
	for i, capture := range captures {
		pack := newID(tb)
		rows[i] = []any{capture[:], "inactivity", at, gapStart, gapEnd, pack[:], at, meta, "published"}
		args = append(args, capture[:])
	}
	seedTx(tb, c, func(tx *sqlx.Tx) {
		insertRows(tb, tx, "capture_closes", []string{"capture_id", "trigger_kind", "triggered_at", "gap_start", "gap_end", "pack_id", "writer_time", "pack_meta", "state"}, rows)
		in := "(?" + strings.Repeat(", ?", len(captures)-1) + ")"
		if _, err := tx.ExecContext(tb.Context(), `UPDATE captures SET state = 'closed' WHERE capture_id IN `+in, args...); err != nil {
			tb.Fatal(err)
		}
	})
	if got := count(tb, c, `SELECT COUNT(*) FROM capture_closes WHERE state = 'published' AND triggered_at = ?`, at); got < len(captures) {
		tb.Fatalf("seeded %d closes, found %d", len(captures), got)
	}
}

// TestSeeders checks each seeder's rows through the reads that serve them, so a population a benchmark measures is one the code reads.
func TestSeeders(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		captures := seedCaptures(t, c, 2, 3)
		capture := captures[0]
		got, err := c.Capture(ctx, capture)
		if err != nil || got.State != StateOpen || got.Device != "eqp-0" {
			t.Fatalf("Capture = %+v, %v", got, err)
		}
		if _, err := tracepack.DecodeDescriptor(got.Descriptor); err != nil {
			t.Fatalf("seeded descriptor: %v", err)
		}
		content, err := c.ConfigContent(ctx, got.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := procconfig.Decode(content); err != nil {
			t.Fatalf("seeded configuration: %v", err)
		}

		packs, next := seedPacks(t, c, capture, packsSeed{hour0: 1, hours: 2, packs: 2, ranges: 2, span: 10, deletedEvery: 4})
		if next != 80 || len(packs) != 4 {
			t.Fatalf("seedPacks = %d packs, next %d; want 4, 80", len(packs), next)
		}
		cp, err := c.Completeness(ctx, capture)
		// The fourth pack, seqs 60 to 79, is deleted: its seqs are a gap below the frontier.
		if err != nil || cp.Frontier != 79 || len(cp.Retained) != 1 || cp.Retained[0].SeqTo != 59 || len(cp.Gaps) != 1 || cp.Gaps[0].From != 60 {
			t.Fatalf("Completeness = %+v, %v; want 0 to 59 retained and 60 to 79 a gap", cp, err)
		}

		seedAudit(t, c, 4, captures[:2], []string{"alice", "bob"}, 1)
		if got, err := c.AuditEntries(ctx, capture); err != nil || len(got) != 2 {
			t.Fatalf("AuditEntries = %d, %v; want 2", len(got), err)
		}

		first, last := seedEvents(t, c, capture, 1, 5, 10, 10)
		register(t, c, segment(t, capture, 3, 100, 109), Registration{Evidence: &Evidence{}})
		if evs := events(t, c); len(evs) != int(last-first)+1+1 {
			t.Fatalf("events after a registration = %v; want the seeded ones and one more", evs)
		}

		merges := seedMerges(t, c, captures[1], 1, 2, 2, 5, 0)
		if got, err := c.DeleteCandidates(ctx, 5+int64(time.Hour), time.Hour); err != nil || len(got) != 2*len(merges) {
			t.Fatalf("DeleteCandidates = %d, %v; want %d", len(got), err, 2*len(merges))
		}
	})
}

// TestSeedersOfCompleteness checks the seeders of claim objects, tombstones and every completeness input through the reads that serve them.
func TestSeedersOfCompleteness(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		captures := seedCaptures(t, c, 1, 2)
		capture := captures[0]
		packs, _ := seedPacks(t, c, capture, packsSeed{hour0: 1, hours: 1, packs: 2, ranges: 1, span: 10})
		seedConflicts(t, c, capture, packs[0], packs[1], 0, 3)
		seedClaimObjects(t, c, "claims", capture, 1, 0, 3, "live")
		seedUnattributed(t, c, 2)
		if got, err := c.ClaimObjects(ctx, "live"); err != nil || len(got) != 3 {
			t.Fatalf("ClaimObjects = %d, %v; want 3", len(got), err)
		}
		if got, err := c.UnattributedObjects(ctx); err != nil || len(got) != 2 {
			t.Fatalf("UnattributedObjects = %d, %v; want 2", len(got), err)
		}
		seedTombstones(t, c, 2, "deleted", 1)

		seedParked(t, c, capture, 2, []uint64{200, 201}, "parked")
		seedEvicted(t, c, capture, 0, []Range{{SeqFrom: 300, SeqTo: 309, TSFrom: 1, TSTo: 10}})
		seedLosses(t, c, packs[0], []Loss{{SeqFrom: new(uint64(400)), SeqTo: new(uint64(401))}})
		seedCloses(t, c, captures[1:], 9, 1, 9)
		cp, err := c.Completeness(ctx, capture)
		if err != nil || len(cp.Deferred) != 2 || len(cp.Evicted) != 1 || len(cp.Losses) != 1 || len(cp.Conflicts) != 3 {
			t.Fatalf("Completeness = %+v, %v; want 2 deferred, 1 evicted run, 1 loss and 3 conflicts", cp, err)
		}
		if cp, err := c.Completeness(ctx, captures[1]); err != nil || len(cp.CrashGaps) != 1 {
			t.Fatalf("Completeness of the closed capture = %+v, %v; want its crash gap", cp, err)
		}

		vs := []Identity{{Version: VersionKey{Capture: capture, Seq: 210, Digest: [32]byte{1}}, Hour: 2}, {Version: VersionKey{Capture: capture, Seq: 211, Digest: [32]byte{1}}, Hour: 2}}
		seedVersions(t, c, vs, []VersionState{VersionParking, VersionStaged})
		for i, want := range []Resolution{Unresolved, ResolvedStaged} {
			if got, err := c.Resolve(ctx, &vs[i].Version, 1, 1); err != nil || got != want {
				t.Fatalf("Resolve of seeded version %d = %v, %v; want %v", i, got, err, want)
			}
		}
		if got, err := readVersionState(ctx, c.read, &vs[0].Version); err != nil || got != VersionParking {
			t.Fatalf("readVersionState of the seeded parking version = %q, %v", got, err)
		}
	})
}
