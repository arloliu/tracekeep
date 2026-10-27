package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// Scope is one (capture, hour), the unit a merge settles (compactor.md §8.1).
type Scope struct {
	Capture tracepack.UUID
	Hour    int64
}

// touchingConflict is true when a conflicts row touches the scope s: either of its packs is a pack of s, in any state,
// and neither is excluded, since an excluded pack is never a merge input (compactor.md §8.1 step 2).
const touchingConflict = `EXISTS (SELECT 1 FROM conflicts f JOIN packs k ON k.pack_id IN (f.pack_id, f.other_pack_id)
	WHERE f.capture_id = s.capture_id AND k.capture_id = s.capture_id AND k.hour = s.hour
	AND NOT EXISTS (SELECT 1 FROM packs x WHERE x.pack_id IN (f.pack_id, f.other_pack_id) AND x.state = 'excluded'))`

// MergeCandidates lists the scopes a merge may settle (compactor.md §8.1 step 2):
// open or reopened, of an hour before readyHour (its end plus the quiet period passed), holding staged packs, and touched by no conflict.
// It is a read; the snapshot and the commit judge again.
func (c *Catalog) MergeCandidates(ctx context.Context, readyHour int64) ([]Scope, error) {
	var rows []struct {
		Capture []byte `db:"capture_id"`
		Hour    int64  `db:"hour"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT s.capture_id, s.hour FROM scopes s
		WHERE s.state IN ('open', 'reopened') AND s.hour < ?
		AND EXISTS (SELECT 1 FROM packs p WHERE p.capture_id = s.capture_id AND p.hour = s.hour AND p.state = 'staged')
		AND NOT `+touchingConflict+`
		ORDER BY s.hour, s.capture_id`, readyHour)
	if err != nil {
		return nil, fmt.Errorf("catalog: merge candidates: %w", err)
	}
	out := make([]Scope, len(rows))
	for i, r := range rows {
		out[i] = Scope{Capture: tracepack.UUID(r.Capture), Hour: r.Hour}
	}

	return out, nil
}

// Input is a merge input: a staged pack of the scope.
type Input struct {
	ID   tracepack.UUID
	Role string
	Key  string
	Size int64
}

// Snapshot is a scope as a merge reads it (compactor.md §8.1 step 3).
type Snapshot struct {
	Scope
	ToolID string
	// State is the scope's state, empty for a scope the catalog does not hold.
	State string
	// Revision is the scope's revision, read before everything else:
	// whatever changes after it advances the revision, so the commit's compare-and-set catches it on either engine.
	Revision int64
	// Inputs are the scope's staged packs, segments and archives alike.
	Inputs []Input
	// Eligible reports a scope open or reopened, holding inputs, and touched by no conflict.
	Eligible bool
}

// MergeSnapshot reads s's revision, then its inputs and its eligibility.
func (c *Catalog) MergeSnapshot(ctx context.Context, s Scope) (Snapshot, error) {
	snap := Snapshot{Scope: s}
	var head struct {
		State    string `db:"state"`
		Revision int64  `db:"revision"`
	}
	err := c.read.GetContext(ctx, &head, `SELECT state, revision FROM scopes WHERE capture_id = ? AND hour = ?`, s.Capture[:], s.Hour)
	if errors.Is(err, sql.ErrNoRows) {
		return snap, nil
	}
	if err != nil {
		return snap, fmt.Errorf("catalog: merge snapshot: %w", err)
	}
	snap.State, snap.Revision = head.State, head.Revision
	var rows []struct {
		ID     []byte `db:"pack_id"`
		ToolID string `db:"tool_id"`
		Role   string `db:"role"`
		Key    string `db:"object_key"`
		Size   int64  `db:"size"`
	}
	err = c.read.SelectContext(ctx, &rows, `SELECT pack_id, tool_id, role, object_key, size FROM packs
		WHERE capture_id = ? AND hour = ? AND state = 'staged' ORDER BY seq_start, pack_id`, s.Capture[:], s.Hour)
	if err != nil {
		return snap, fmt.Errorf("catalog: merge snapshot: %w", err)
	}
	for _, r := range rows {
		snap.ToolID = r.ToolID
		snap.Inputs = append(snap.Inputs, Input{ID: tracepack.UUID(r.ID), Role: r.Role, Key: r.Key, Size: r.Size})
	}
	var touched bool
	err = c.read.GetContext(ctx, &touched, `SELECT `+touchingConflict+` FROM scopes s WHERE s.capture_id = ? AND s.hour = ?`, s.Capture[:], s.Hour)
	if err != nil {
		return snap, fmt.Errorf("catalog: merge snapshot: %w", err)
	}
	snap.Eligible = (head.State == "open" || head.State == "reopened") && len(snap.Inputs) > 0 && !touched

	return snap, nil
}

// Merge is a merge to commit: the snapshot's revision and inputs, and the archive written from them.
type Merge struct {
	Scope
	Revision int64
	Inputs   []tracepack.UUID
	// Archive is the archive's pack: its exact ranges and its losses, read from the archive itself.
	Archive *Pack
	// Request is the operator's request this merge executes, nil for a scheduled merge:
	// the commit ends it done in the same transaction, so a merge that committed never runs again for it.
	Request *tracepack.UUID
}

// MergeOutcome is what a merge commit did (catalog.md §7.2 `CommitMerge`).
type MergeOutcome int

// The merge commit's outcomes.
const (
	// Merged: the archive is registered, its inputs merged and the scope settled.
	Merged MergeOutcome = iota + 1
	// MergeStale: the scope changed since the snapshot (its revision, a conflict, an input no longer staged, its state);
	// nothing was written, and the archive is left to the reconciler.
	MergeStale
	// ArchiveRegistered: this merge committed before its result was lost; nothing more was written.
	ArchiveRegistered
	// ArchiveAdopted: the reconciler adopted the archive before the commit, which raised its suspects and reopened the scope;
	// nothing was written, and the next cycle merges the scope with the archive as an input.
	ArchiveAdopted
	// ArchiveTombstoned: the archive's key is tombstoned; nothing was written, and the reconciler deletes the object.
	ArchiveTombstoned
)

// ErrInvalidMerge refuses a merge the transaction cannot take; nothing was written.
var ErrInvalidMerge = errors.New("catalog: invalid merge")

// CommitMerge commits m under l, in one transaction that starts with the capture row and checks the lease (compactor.md §8.1 step 5):
// the scope's eligibility is re-read under the row, the archive registered with its ranges and losses, the merge and its inputs recorded,
// the inputs merged and the scope settled by compare-and-set on the snapshot's revision, the settled event last.
// It is no registration of the capture: it raises no suspect against its own inputs, writes no receipt and moves no registration fact.
// An outcome other than Merged writes nothing, the prologue included; a lost lease returns ErrLeaseLost.
func (c *Catalog) CommitMerge(ctx context.Context, l Lease, m *Merge) (MergeOutcome, error) {
	if err := m.check(); err != nil {
		return 0, err
	}
	var out MergeOutcome
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		var err error
		out, err = c.commitMergeIn(ctx, tx, l, m)
		if err == nil && out != Merged {
			err = errRollback
		}

		return err
	}))
	if err != nil {
		return 0, fmt.Errorf("catalog: commit merge of %s hour %d: %w", m.Capture, m.Hour, err)
	}

	return out, nil
}

func (m *Merge) check() error {
	a := m.Archive
	switch {
	case a == nil || len(m.Inputs) == 0:
		return fmt.Errorf("%w: no archive or no input", ErrInvalidMerge)
	case a.Role != RoleArchive || a.Capture != m.Capture || a.Hour != m.Hour:
		return fmt.Errorf("%w: archive %s is no archive of the scope", ErrInvalidMerge, a.ID)
	}
	if err := a.check(Registration{Lease: &Lease{}}); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMerge, err)
	}

	return nil
}

func (c *Catalog) commitMergeIn(ctx context.Context, tx *sqlx.Tx, l Lease, m *Merge) (MergeOutcome, error) {
	if _, err := c.lockCapture(ctx, tx, m.Capture); err != nil {
		return 0, err
	}
	if err := c.checkLease(ctx, tx, l); err != nil {
		return 0, err
	}
	if out, done, err := c.mergeGuards(ctx, tx, m); done || err != nil {
		return out, err
	}
	if err := c.insertPack(ctx, tx, m.Archive, Registration{}); err != nil {
		return 0, err
	}
	now := c.now()
	a := m.Archive
	// One merge per archive, so the archive's pack_id names the merge.
	_, err := tx.ExecContext(ctx, `INSERT INTO merges (merge_id, capture_id, hour, output_pack_id, outcome, at) VALUES (?, ?, ?, ?, 'committed', ?)`,
		a.ID[:], m.Capture[:], m.Hour, a.ID[:], now)
	if err != nil {
		return 0, err
	}
	for _, in := range m.Inputs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO merge_inputs (merge_id, pack_id) VALUES (?, ?)`, a.ID[:], in[:]); err != nil {
			return 0, err
		}
	}
	q, args := inList(`UPDATE packs SET state = 'merged' WHERE state = 'staged' AND pack_id IN`, m.Inputs)
	res, err := tx.ExecContext(ctx, q, args...)
	if err := rowsWant(res, err, len(m.Inputs)); err != nil {
		return 0, err
	}
	res, err = tx.ExecContext(ctx, `UPDATE scopes SET state = 'settled', archive_pack_id = ?, merged_at = ?, revision = revision + 1
		WHERE capture_id = ? AND hour = ? AND revision = ?`, a.ID[:], now, m.Capture[:], m.Hour, m.Revision)
	if err := affected(res, err, errStale); errors.Is(err, errStale) {
		return MergeStale, nil
	} else if err != nil {
		return 0, err
	}
	if m.Request != nil {
		_, err := tx.ExecContext(ctx, `UPDATE compact_requests SET state = 'done', outcome = 'merged', done_at = ? WHERE request_id = ? AND state = 'pending'`,
			now, m.Request[:])
		if err != nil {
			return 0, err
		}
	}

	return Merged, c.emit(ctx, tx, []event{{m.Capture, m.Hour, "settled", m.Revision + 1}})
}

// errStale marks a compare-and-set that found the scope changed.
var errStale = errors.New("catalog: scope changed")

// mergeGuards re-reads, under the capture row, what the snapshot judged:
// the scope still at the snapshot's revision and open or reopened, no conflict touching it, every input still staged,
// and the archive neither registered nor tombstoned.
func (c *Catalog) mergeGuards(ctx context.Context, tx *sqlx.Tx, m *Merge) (MergeOutcome, bool, error) {
	var n int
	if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM packs WHERE pack_id = ?`, m.Archive.ID[:]); err != nil {
		return 0, true, err
	}
	if n > 0 {
		if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM merges WHERE output_pack_id = ?`, m.Archive.ID[:]); err != nil {
			return 0, true, err
		}
		if n > 0 {
			return ArchiveRegistered, true, nil
		}

		return ArchiveAdopted, true, nil
	}
	if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, m.Archive.Key); err != nil {
		return 0, true, err
	}
	if n > 0 {
		return ArchiveTombstoned, true, nil
	}
	var head struct {
		State    string `db:"state"`
		Revision int64  `db:"revision"`
		Touched  bool   `db:"touched"`
	}
	err := tx.GetContext(ctx, &head, `SELECT s.state, s.revision, `+touchingConflict+` AS touched FROM scopes s
		WHERE s.capture_id = ? AND s.hour = ?`, m.Capture[:], m.Hour)
	if errors.Is(err, sql.ErrNoRows) {
		return MergeStale, true, nil
	}
	if err != nil {
		return 0, true, err
	}
	if head.Revision != m.Revision || (head.State != "open" && head.State != "reopened") || head.Touched {
		return MergeStale, true, nil
	}
	q, args := inList(`SELECT COUNT(*) FROM packs WHERE capture_id = ? AND hour = ? AND state = 'staged' AND pack_id IN`, m.Inputs, m.Capture[:], m.Hour)
	if err := tx.GetContext(ctx, &n, q, args...); err != nil {
		return 0, true, err
	}
	if n != len(m.Inputs) {
		return MergeStale, true, nil
	}

	return 0, false, nil
}

// inList appends "(?, ...)" for ids to query, after the leading args.
func inList(query string, ids []tracepack.UUID, lead ...any) (string, []any) {
	args := append([]any(nil), lead...)
	for _, id := range ids {
		args = append(args, id[:])
	}

	return query + " (" + strings.TrimSuffix(strings.Repeat("?, ", len(ids)), ", ") + ")", args
}

// rowsWant checks that a statement changed exactly want rows: anything else is a fault the guards should have caught.
func rowsWant(res sql.Result, err error, want int) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != int64(want) {
		return fmt.Errorf("catalog: %d rows changed, want %d", n, want)
	}

	return nil
}

// ScopeStats is what the compact metrics read of the scopes (operations.md §10.4 compact).
type ScopeStats struct {
	// ByState counts the scopes in each state.
	ByState map[string]int
	// OpenConflicts counts the conflicts an operator has not acknowledged: suspect or confirmed.
	OpenConflicts int
	// OldestUnmerged is the hour of the oldest open or reopened scope; nil when there is none.
	OldestUnmerged *int64
}

// ScopeStats reads the scopes' counts by state, the open conflicts and the oldest unmerged scope's hour.
func (c *Catalog) ScopeStats(ctx context.Context) (ScopeStats, error) {
	st := ScopeStats{ByState: map[string]int{}}
	var rows []struct {
		State string `db:"state"`
		N     int    `db:"n"`
	}
	if err := c.read.SelectContext(ctx, &rows, `SELECT state, COUNT(*) AS n FROM scopes GROUP BY state`); err != nil {
		return st, fmt.Errorf("catalog: scope stats: %w", err)
	}
	for _, r := range rows {
		st.ByState[r.State] = r.N
	}
	if err := c.read.GetContext(ctx, &st.OpenConflicts, `SELECT COUNT(*) FROM conflicts WHERE state IN ('suspect', 'confirmed')`); err != nil {
		return st, fmt.Errorf("catalog: scope stats: %w", err)
	}
	var oldest sql.NullInt64
	if err := c.read.GetContext(ctx, &oldest, `SELECT MIN(hour) FROM scopes WHERE state IN ('open', 'reopened')`); err != nil {
		return st, fmt.Errorf("catalog: scope stats: %w", err)
	}
	if oldest.Valid {
		st.OldestUnmerged = &oldest.Int64
	}

	return st, nil
}
