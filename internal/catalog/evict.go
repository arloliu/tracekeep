package catalog

import (
	"context"
	"fmt"
	"slices"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// copyRow is one parked copy as §8.4's predicate reads it: its stream incarnation and state.
type copyRow struct {
	Incarnation int64
	State       string
}

// versionCopies is one parked version of a (capture, seq) with its copies.
type versionCopies struct {
	State  VersionState
	Copies []copyRow
}

// objectReleased is §8.4's predicate, the one function both deletions of a claim object call under the capture row:
// the object of a (capture, seq) is no longer needed when every parked version of it is staged,
// or terminated with every copy of a current incarnation acknowledged; a copy of a retired incarnation never blocks.
// It decides only whether a parked version still needs the object; the caller checks the rest (compactor.md §8.4).
func objectReleased(versions []versionCopies, current map[int64]bool) bool {
	for _, v := range versions {
		if v.State == VersionStaged {
			continue
		}
		if v.State != VersionTerminated {
			return false // unresolved: parking, parked or evicted-hour
		}
		for _, cp := range v.Copies {
			if current[cp.Incarnation] && cp.State != "acked" {
				return false
			}
		}
	}

	return true
}

// released reads the parked versions of capture's seq with their copies and the current incarnations, and applies objectReleased.
func released(ctx context.Context, tx *sqlx.Tx, capture tracepack.UUID, seq int64) (bool, error) {
	var rows []struct {
		Header      []byte       `db:"header"`
		Digest      []byte       `db:"payload_digest"`
		State       VersionState `db:"state"`
		Incarnation *int64       `db:"incarnation_id"`
		CopyState   *string      `db:"copy_state"`
	}
	err := tx.SelectContext(ctx, &rows, `SELECT p.header, p.payload_digest, p.state, k.incarnation_id, k.state AS copy_state
		FROM parked p LEFT JOIN parked_copies k
		ON k.capture_id = p.capture_id AND k.seq = p.seq AND k.header = p.header AND k.payload_digest = p.payload_digest
		WHERE p.capture_id = ? AND p.seq = ? ORDER BY p.header, p.payload_digest`, capture[:], seq)
	if err != nil || len(rows) == 0 {
		return err == nil, err
	}
	var current []int64
	if err := tx.SelectContext(ctx, &current, `SELECT incarnation_id FROM bus_streams WHERE retired_at IS NULL`); err != nil {
		return false, err
	}
	cur := map[int64]bool{}
	for _, id := range current {
		cur[id] = true
	}
	var versions []versionCopies
	var lastKey string
	for _, r := range rows {
		if key := string(r.Header) + string(r.Digest); len(versions) == 0 || key != lastKey {
			versions, lastKey = append(versions, versionCopies{State: r.State}), key
		}
		if r.Incarnation != nil {
			v := &versions[len(versions)-1]
			v.Copies = append(v.Copies, copyRow{Incarnation: *r.Incarnation, State: *r.CopyState})
		}
	}

	return objectReleased(versions, cur), nil
}

// EvictCandidates returns the hours retention may evict at now: those of admitted captures past the eviction horizon
// that hold a scope, a pack, a live claim object or a parked version still parked; none before the first retention record.
func (c *Catalog) EvictCandidates(ctx context.Context, now int64) ([]Scope, error) {
	r, err := c.Retention(ctx)
	if err != nil || !r.Known {
		return nil, err
	}
	horizon := r.HorizonHour(now)
	var rows []struct {
		Capture []byte `db:"capture_id"`
		Hour    int64  `db:"hour"`
	}
	err = c.read.SelectContext(ctx, &rows, `SELECT h.capture_id, h.hour FROM (
			SELECT capture_id, hour FROM scopes WHERE hour < ?
			UNION SELECT capture_id, hour FROM packs WHERE hour < ?
			UNION SELECT capture_id, hour FROM claim_objects WHERE state = 'live' AND hour < ?
			UNION SELECT capture_id, hour FROM parked WHERE state = 'parked' AND hour < ?
		) h JOIN captures k ON k.capture_id = h.capture_id
		WHERE k.state IN ('open', 'stopped', 'closed') ORDER BY h.capture_id, h.hour`, horizon, horizon, horizon, horizon)
	if err != nil {
		return nil, fmt.Errorf("catalog: evict candidates: %w", err)
	}
	out := make([]Scope, 0, len(rows))
	for _, r := range rows {
		out = append(out, Scope{Capture: tracepack.UUID(r.Capture), Hour: r.Hour})
	}

	return out, nil
}

// Eviction is what EvictHour did: whether it evicted the hour, the pack and claim objects its caller deletes after the commit,
// and how many claim objects it kept as survivors.
type Eviction struct {
	Evicted bool
	Packs   []string
	Claims  []string
	// Tombstoned counts the keys this eviction tombstoned, a key a deletion tombstoned before not among them.
	Tombstoned int
	Survivors  int
}

// EvictHour evicts one hour of one admitted capture under the capture row and l (compactor.md §8.3, catalog.md §7.2 `EvictHour`):
// the hour past its deadline under the recorded retention at now, and no packs row of another hour naming one of its keys;
// then its ranges into evicted_ranges, coalesced within the hour, its packs with their ranges, losses, merges and conflicts,
// its receipts and its scope deleted, its keys tombstoned, its live claim objects released or, when §8.4's predicate holds false, kept as survivors,
// and its parked versions moved parked → evicted-hour; a conflict's other scope advances with a conflict event, last.
// A refusal writes nothing and returns Evicted false.
func (c *Catalog) EvictHour(ctx context.Context, l Lease, s Scope, now int64) (Eviction, error) {
	var ev Eviction
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		ev = Eviction{}
		row, err := c.lockCapture(ctx, tx, s.Capture)
		if err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		w, ok, err := readRetention(ctx, tx)
		if err != nil {
			return err
		}
		if !ok || !isAdmitted(row.State) || s.Hour >= w.retention().HorizonHour(now) {
			return errRollback
		}
		others, err := c.evictPacks(ctx, tx, s, now, &ev)
		if err != nil {
			return err
		}
		if err := c.evictClaims(ctx, tx, s, &ev); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE parked SET state = 'evicted-hour' WHERE capture_id = ? AND hour = ? AND state = 'parked'`, s.Capture[:], s.Hour)
		if err != nil {
			return err
		}
		ev.Evicted = true

		return c.touchScopes(ctx, tx, s.Capture, others)
	}))
	if err != nil {
		return Eviction{}, fmt.Errorf("catalog: evict %s hour %d: %w", s.Capture, s.Hour, err)
	}

	return ev, nil
}

// evictPacks deletes the hour's packs and the rows that name them, after copying their ranges into evicted_ranges,
// and returns the packs of other hours a deleted conflict named.
func (c *Catalog) evictPacks(ctx context.Context, tx *sqlx.Tx, s Scope, now int64, ev *Eviction) ([]tracepack.UUID, error) {
	if err := tx.SelectContext(ctx, &ev.Packs, `SELECT object_key FROM packs WHERE capture_id = ? AND hour = ?`, s.Capture[:], s.Hour); err != nil {
		return nil, err
	}
	for _, key := range ev.Packs {
		var n int
		err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM packs WHERE object_key = ? AND NOT (capture_id = ? AND hour = ?)`, key, s.Capture[:], s.Hour)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			return nil, errRollback // another hour's row names the key: an operator decides
		}
	}
	if err := c.evictRanges(ctx, tx, s); err != nil {
		return nil, err
	}
	others, err := evictConflicts(ctx, tx, s)
	if err != nil {
		return nil, err
	}
	for _, stmt := range []string{
		`DELETE FROM pack_ranges WHERE pack_id IN ` + hourPacks,
		`DELETE FROM pack_losses WHERE pack_id IN ` + hourPacks,
		`DELETE FROM merge_inputs WHERE pack_id IN ` + hourPacks,
		`DELETE FROM merge_inputs WHERE merge_id IN (SELECT merge_id FROM merges WHERE capture_id = ? AND hour = ?)`,
		`DELETE FROM merges WHERE capture_id = ? AND hour = ?`,
		`DELETE FROM packs WHERE capture_id = ? AND hour = ?`,
		`DELETE FROM claim_records WHERE capture_id = ? AND hour = ?`,
		`DELETE FROM scopes WHERE capture_id = ? AND hour = ?`,
	} {
		if _, err := tx.ExecContext(ctx, stmt, s.Capture[:], s.Hour); err != nil {
			return nil, err
		}
	}
	for _, key := range ev.Packs {
		res, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO tombstones (object_key, reason, at) VALUES (?, 'evicted', ?)`, key, now)
		if err != nil {
			return nil, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		ev.Tombstoned += int(n)
	}

	return others, nil
}

// hourPacks selects the packs of one (capture, hour), its two arguments.
const hourPacks = `(SELECT pack_id FROM packs WHERE capture_id = ? AND hour = ?)`

// evictConflicts deletes the conflicts naming a pack of the hour and returns the packs of other hours they named.
func evictConflicts(ctx context.Context, tx *sqlx.Tx, s Scope) ([]tracepack.UUID, error) {
	where := ` FROM conflicts WHERE pack_id IN ` + hourPacks + ` OR other_pack_id IN ` + hourPacks
	args := []any{s.Capture[:], s.Hour, s.Capture[:], s.Hour}
	var named []struct {
		A []byte `db:"pack_id"`
		B []byte `db:"other_pack_id"`
	}
	if err := tx.SelectContext(ctx, &named, `SELECT pack_id, other_pack_id`+where, args...); err != nil {
		return nil, err
	}
	var rows [][]byte
	if err := tx.SelectContext(ctx, &rows, `SELECT pack_id FROM packs WHERE capture_id = ? AND hour = ?`, s.Capture[:], s.Hour); err != nil {
		return nil, err
	}
	hour := make([]tracepack.UUID, 0, len(rows))
	for _, id := range rows {
		hour = append(hour, tracepack.UUID(id))
	}
	var others []tracepack.UUID
	for _, r := range named {
		for _, id := range []tracepack.UUID{tracepack.UUID(r.A), tracepack.UUID(r.B)} {
			if !slices.Contains(hour, id) && !slices.Contains(others, id) {
				others = append(others, id)
			}
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE`+where, args...)

	return others, err
}

// evictRanges replaces the hour's evicted_ranges with their union with the ranges of its packs not yet deleted, coalesced.
func (c *Catalog) evictRanges(ctx context.Context, tx *sqlx.Tx, s Scope) error {
	var rows []struct {
		From   int64 `db:"seq_from"`
		To     int64 `db:"seq_to"`
		TSFrom int64 `db:"ts_from"`
		TSTo   int64 `db:"ts_to"`
	}
	err := tx.SelectContext(ctx, &rows, `SELECT r.seq_from, r.seq_to, r.ts_from, r.ts_to FROM pack_ranges r JOIN packs p ON p.pack_id = r.pack_id
		WHERE p.capture_id = ? AND p.hour = ? AND p.state <> 'deleted'
		UNION ALL SELECT seq_from, seq_to, ts_from, ts_to FROM evicted_ranges WHERE capture_id = ? AND hour = ?`,
		s.Capture[:], s.Hour, s.Capture[:], s.Hour)
	if err != nil || len(rows) == 0 {
		return err
	}
	rs := make([]Range, 0, len(rows))
	for _, r := range rows {
		rs = append(rs, Range{SeqFrom: uint64(r.From), SeqTo: uint64(r.To), TSFrom: r.TSFrom, TSTo: r.TSTo}) //nolint:gosec // stored seqs are below 2^63
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM evicted_ranges WHERE capture_id = ? AND hour = ?`, s.Capture[:], s.Hour); err != nil {
		return err
	}
	for _, r := range coalesce(rs) {
		_, err := tx.ExecContext(ctx, `INSERT INTO evicted_ranges (capture_id, hour, seq_from, seq_to, ts_from, ts_to) VALUES (?, ?, ?, ?, ?, ?)`,
			s.Capture[:], s.Hour, int64(r.SeqFrom), int64(r.SeqTo), r.TSFrom, r.TSTo) //nolint:gosec // stored seqs are below 2^63
		if err != nil {
			return err
		}
	}

	return nil
}

// evictClaims releases the hour's live claim objects that §8.4's predicate frees and marks the others survivors of the hour.
func (c *Catalog) evictClaims(ctx context.Context, tx *sqlx.Tx, s Scope, ev *Eviction) error {
	var claims []struct {
		Key string `db:"object_key"`
		Seq int64  `db:"seq"`
	}
	err := tx.SelectContext(ctx, &claims, `SELECT object_key, seq FROM claim_objects WHERE capture_id = ? AND hour = ? AND state = 'live' ORDER BY object_key`,
		s.Capture[:], s.Hour)
	if err != nil {
		return err
	}
	for _, cl := range claims {
		ok, err := released(ctx, tx, s.Capture, cl.Seq)
		if err != nil {
			return err
		}
		if ok {
			if _, err := tx.ExecContext(ctx, `DELETE FROM claim_objects WHERE object_key = ?`, cl.Key); err != nil {
				return err
			}
			ev.Claims = append(ev.Claims, cl.Key)

			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE claim_objects SET state = 'survivor', evicted_hour = ? WHERE object_key = ?`, s.Hour, cl.Key); err != nil {
			return err
		}
		ev.Survivors++
	}

	return nil
}
