package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// ConflictPack is one of the two packs a conflicts row names, with what a comparison needs to read it.
type ConflictPack struct {
	ID   tracepack.UUID
	Hour int64
	Key  string
	Size int64
}

// Suspect is a conflicts row in state suspect: an overlap of two packs' seq runs found at registration,
// which the compactor compares (ingest.md §6.2).
type Suspect struct {
	Capture        tracepack.UUID
	SeqFrom, SeqTo uint64
	// Pack is the row's pack, the one whose registration found the overlap; Other is the pack it overlapped.
	Pack, Other ConflictPack
}

// Suspects lists every suspect with the packs it names. It is a read: RecordConflict judges again under the capture row.
func (c *Catalog) Suspects(ctx context.Context) ([]Suspect, error) {
	var rows []struct {
		Capture   []byte `db:"capture_id"`
		From      int64  `db:"seq_from"`
		To        int64  `db:"seq_to"`
		Pack      []byte `db:"pack_id"`
		PackHour  int64  `db:"pack_hour"`
		PackKey   string `db:"pack_key"`
		PackSize  int64  `db:"pack_size"`
		Other     []byte `db:"other_pack_id"`
		OtherHour int64  `db:"other_hour"`
		OtherKey  string `db:"other_key"`
		OtherSize int64  `db:"other_size"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT f.capture_id, f.seq_from, f.seq_to,
		f.pack_id, p.hour AS pack_hour, p.object_key AS pack_key, p.size AS pack_size,
		f.other_pack_id, o.hour AS other_hour, o.object_key AS other_key, o.size AS other_size
		FROM conflicts f JOIN packs p ON p.pack_id = f.pack_id JOIN packs o ON o.pack_id = f.other_pack_id
		WHERE f.state = 'suspect' ORDER BY f.capture_id, f.seq_from, f.seq_to, f.pack_id, f.other_pack_id`)
	if err != nil {
		return nil, fmt.Errorf("catalog: suspects: %w", err)
	}
	out := make([]Suspect, len(rows))
	for i, r := range rows {
		out[i] = Suspect{
			Capture: tracepack.UUID(r.Capture), SeqFrom: uint64(r.From), SeqTo: uint64(r.To), //nolint:gosec // seqs are stored below 2^63
			Pack:  ConflictPack{ID: tracepack.UUID(r.Pack), Hour: r.PackHour, Key: r.PackKey, Size: r.PackSize},
			Other: ConflictPack{ID: tracepack.UUID(r.Other), Hour: r.OtherHour, Key: r.OtherKey, Size: r.OtherSize},
		}
	}

	return out, nil
}

// ConflictOutcome is what recording a comparison did (catalog.md §7.2 `RecordConflict`).
type ConflictOutcome int

// The outcomes of recording a comparison.
const (
	// ConflictRecorded: the suspect was cleared, or confirmed.
	ConflictRecorded ConflictOutcome = iota + 1
	// ConflictChanged: the row no longer names exactly the packs compared as a suspect; nothing was written,
	// and the compactor compares again at its next cycle.
	ConflictChanged
)

// RecordConflict records the comparison of s under l: differ confirms the suspect, otherwise it is cleared,
// each only while the row still names exactly the packs compared and is still a suspect (T-32).
// Every scope it touches advances its revision and emits a conflict event, the counter last;
// another pair's row is never touched. A lost lease returns ErrLeaseLost.
func (c *Catalog) RecordConflict(ctx context.Context, l Lease, s Suspect, differ bool) (ConflictOutcome, error) {
	var out ConflictOutcome
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, s.Capture); err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		stmt := `DELETE FROM conflicts`
		if differ {
			stmt = `UPDATE conflicts SET state = 'confirmed'`
		}
		res, err := tx.ExecContext(ctx, stmt+` WHERE capture_id = ? AND seq_from = ? AND seq_to = ? AND pack_id = ? AND other_pack_id = ?
			AND state = 'suspect'`, s.Capture[:], int64(s.SeqFrom), int64(s.SeqTo), s.Pack.ID[:], s.Other.ID[:]) //nolint:gosec // stored seqs
		if err := affected(res, err, errStale); errors.Is(err, errStale) {
			out = ConflictChanged
			return errRollback
		} else if err != nil {
			return err
		}
		out = ConflictRecorded

		return c.touchScopes(ctx, tx, s.Capture, []tracepack.UUID{s.Pack.ID, s.Other.ID})
	}))
	if err != nil {
		return 0, fmt.Errorf("catalog: record conflict of %s at %d-%d: %w", s.Capture, s.SeqFrom, s.SeqTo, err)
	}

	return out, nil
}

// MergeConflict is a record a merge found in conflict: for each distinct version, the packs holding it.
type MergeConflict struct {
	Seq   uint64
	Packs [][]tracepack.UUID
}

// RecordMergeConflicts records under l the conflicts a merge of s failed on (ingest.md §6.2):
// for every pair of packs holding different versions of a seq, the pair's seqs are coalesced into runs (catalog.md §7.1);
// a suspect row of the pair that meets a run is confirmed in place, and a run no row of the pair meets is written confirmed;
// every scope the packs touch advances its revision and emits a conflict event, the counter last.
func (c *Catalog) RecordMergeConflicts(ctx context.Context, l Lease, s Scope, found []MergeConflict) error {
	runs := conflictRuns(found)
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, s.Capture); err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		var packs []tracepack.UUID
		for _, r := range runs {
			if err := c.confirmRun(ctx, tx, s.Capture, r); err != nil {
				return err
			}
			packs = append(packs, r.a, r.b)
		}

		return c.touchScopes(ctx, tx, s.Capture, packs)
	})
	if err != nil {
		return fmt.Errorf("catalog: record the conflicts of %s hour %d: %w", s.Capture, s.Hour, err)
	}

	return nil
}

// pairRun is a run of seqs at which two packs hold different versions.
type pairRun struct {
	a, b     tracepack.UUID
	from, to uint64
}

// conflictRuns turns a merge's conflicts into runs: per unordered pair of packs holding different versions, consecutive seqs coalesced.
func conflictRuns(found []MergeConflict) []pairRun {
	type pair struct{ a, b tracepack.UUID }
	seqs := map[pair][]uint64{}
	var order []pair
	for _, f := range found {
		for i, vi := range f.Packs {
			for _, vj := range f.Packs[i+1:] {
				for _, p := range vi {
					for _, o := range vj {
						k := pair{p, o}
						if bytes.Compare(o[:], p[:]) < 0 {
							k = pair{o, p}
						}
						if _, ok := seqs[k]; !ok {
							order = append(order, k)
						}
						seqs[k] = append(seqs[k], f.Seq)
					}
				}
			}
		}
	}
	var runs []pairRun
	for _, k := range order {
		ss := seqs[k]
		slices.Sort(ss)
		ss = slices.Compact(ss)
		for i := 0; i < len(ss); {
			j := i
			for j+1 < len(ss) && ss[j+1] == ss[j]+1 {
				j++
			}
			runs = append(runs, pairRun{a: k.a, b: k.b, from: ss[i], to: ss[j]})
			i = j + 1
		}
	}

	return runs
}

// confirmRun confirms the suspect rows of r's pair that meet r, either orientation, and writes r confirmed when no row of the pair meets it.
func (c *Catalog) confirmRun(ctx context.Context, tx *sqlx.Tx, capture tracepack.UUID, r pairRun) error {
	from, to := int64(r.from), int64(r.to) //nolint:gosec // seqs are below 2^63
	const pairMeets = `capture_id = ? AND ((pack_id = ? AND other_pack_id = ?) OR (pack_id = ? AND other_pack_id = ?))
		AND seq_from <= ? AND seq_to >= ?`
	args := []any{capture[:], r.a[:], r.b[:], r.b[:], r.a[:], to, from}
	if _, err := tx.ExecContext(ctx, `UPDATE conflicts SET state = 'confirmed' WHERE `+pairMeets+` AND state = 'suspect'`, args...); err != nil {
		return err
	}
	var n int
	if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM conflicts WHERE `+pairMeets, args...); err != nil || n > 0 {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO conflicts (capture_id, seq_from, seq_to, pack_id, other_pack_id, state) VALUES (?, ?, ?, ?, ?, 'confirmed')`,
		capture[:], from, to, r.a[:], r.b[:])

	return err
}

// touchScopes advances the revision of every scope holding one of packs and emits a conflict event on each, in hour order, last.
func (c *Catalog) touchScopes(ctx context.Context, tx *sqlx.Tx, capture tracepack.UUID, packs []tracepack.UUID) error {
	if len(packs) == 0 {
		return nil
	}
	var hours []int64
	q, args := inList(`SELECT DISTINCT hour FROM packs WHERE capture_id = ? AND pack_id IN`, packs, capture[:])
	if err := tx.SelectContext(ctx, &hours, q, args...); err != nil {
		return err
	}
	slices.Sort(hours)
	var evs []event
	for _, hour := range hours {
		res, err := tx.ExecContext(ctx, `UPDATE scopes SET revision = revision + 1 WHERE capture_id = ? AND hour = ?`, capture[:], hour)
		if err := affected(res, err, errStale); errors.Is(err, errStale) {
			continue // an hour retention evicted has no scope left
		} else if err != nil {
			return err
		}
		var rev int64
		if err := tx.GetContext(ctx, &rev, `SELECT revision FROM scopes WHERE capture_id = ? AND hour = ?`, capture[:], hour); err != nil {
			return err
		}
		evs = append(evs, event{capture, hour, "conflict", rev})
	}

	return c.emit(ctx, tx, evs)
}
