package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// Deletable is a merged input past its merge's delete_at (compactor.md §8.3).
type Deletable struct {
	Pack    tracepack.UUID
	Capture tracepack.UUID
	Key     string
}

// deleteAtSQL is a merge's deletion time: its delete_at, or for a merge a version before it committed (delete_at 0, the column's default),
// its time plus the deciding process's D, the first argument (compactor.md §8.3).
const deleteAtSQL = `CASE WHEN m.delete_at = 0 THEN m.at + ? ELSE m.delete_at END`

// deletableWhere is the deletion relation: a merged input of a committed merge whose output is staged or merged,
// or deleted, which only a merged pack becomes, its records living on in a later merge's output (compactor.md §8.3);
// its arguments are D and now.
const deletableWhere = `FROM packs p JOIN merge_inputs i ON i.pack_id = p.pack_id JOIN merges m ON m.merge_id = i.merge_id
	JOIN packs o ON o.pack_id = m.output_pack_id
	WHERE p.state = 'merged' AND m.outcome = 'committed' AND o.state IN ('staged', 'merged', 'deleted') AND ` + deleteAtSQL + ` <= ?`

// DeleteCandidates returns the merged inputs whose deletion time has passed at now, delay being this process's D.
func (c *Catalog) DeleteCandidates(ctx context.Context, now int64, delay time.Duration) ([]Deletable, error) {
	var rows []struct {
		Pack    []byte `db:"pack_id"`
		Capture []byte `db:"capture_id"`
		Key     string `db:"object_key"`
	}
	if err := c.read.SelectContext(ctx, &rows, `SELECT DISTINCT p.pack_id, p.capture_id, p.object_key `+deletableWhere, delay.Nanoseconds(), now); err != nil {
		return nil, fmt.Errorf("catalog: delete candidates: %w", err)
	}
	out := make([]Deletable, 0, len(rows))
	for _, r := range rows {
		out = append(out, Deletable{Pack: tracepack.UUID(r.Pack), Capture: tracepack.UUID(r.Capture), Key: r.Key})
	}

	return out, nil
}

// DeletePack deletes d under the capture row and l (catalog.md §7.2 `DeletePack`):
// the deletion relation re-read for it at now, no other packs row naming its key, then the pack deleted and its key tombstoned.
// deleted is false, and nothing written, when a guard refuses; the caller removes the object only after a true.
func (c *Catalog) DeletePack(ctx context.Context, l Lease, d Deletable, now int64, delay time.Duration) (bool, error) {
	deleted := false
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, d.Capture); err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		var still, others int
		if err := tx.GetContext(ctx, &still, `SELECT COUNT(*) `+deletableWhere+` AND p.pack_id = ?`, delay.Nanoseconds(), now, d.Pack[:]); err != nil {
			return err
		}
		if err := tx.GetContext(ctx, &others, `SELECT COUNT(*) FROM packs WHERE object_key = ? AND pack_id <> ?`, d.Key, d.Pack[:]); err != nil {
			return err
		}
		if still == 0 || others > 0 {
			return errRollback
		}
		res, err := tx.ExecContext(ctx, `UPDATE packs SET state = 'deleted' WHERE pack_id = ? AND state = 'merged'`, d.Pack[:])
		if err := affected(res, err, errRollback); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO tombstones (object_key, reason, at) VALUES (?, 'deleted', ?)`, d.Key, now); err != nil {
			return err
		}
		deleted = true

		return nil
	}))
	if err != nil {
		return false, fmt.Errorf("catalog: delete pack %s: %w", d.Pack, err)
	}

	return deleted, nil
}

// Deadline returns the earliest time pack's object may be deleted (compactor.md §8.3):
// its hour's under the recorded retention, and for a merged input the earlier of that and its merge's deletion time,
// delay being this process's D for a merge that recorded none. ok is false before the compactor's first retention record, when no deadline exists.
func (c *Catalog) Deadline(ctx context.Context, pack tracepack.UUID, delay time.Duration) (int64, bool, error) {
	d, err := c.PackDownload(ctx, pack, delay)

	return d.Deadline, d.Known, err
}

// Download is what a download URL of a pack needs (api.md §10.1 *Downloads*): its object, capture and state, and its deadline,
// Known false before the compactor's first retention record, when no deadline exists.
type Download struct {
	Key      string
	Capture  tracepack.UUID
	State    string
	Deadline int64
	Known    bool
}

// PackDownload reads pack's object, capture, state and deadline (Deadline) from one snapshot.
func (c *Catalog) PackDownload(ctx context.Context, pack tracepack.UUID, delay time.Duration) (Download, error) {
	var out Download
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		var p struct {
			Hour    int64  `db:"hour"`
			State   string `db:"state"`
			Key     string `db:"object_key"`
			Capture []byte `db:"capture_id"`
		}
		err := tx.GetContext(ctx, &p, `SELECT hour, state, object_key, capture_id FROM packs WHERE pack_id = ?`, pack[:])
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrUnknownPack, pack)
		}
		if err != nil {
			return err
		}
		out.Key, out.State = p.Key, p.State
		copy(out.Capture[:], p.Capture)
		w, ok, err := readRetention(ctx, tx)
		if err != nil || !ok {
			return err
		}
		out.Deadline, out.Known = w.retention().Deadline(p.Hour), true
		var at sql.NullInt64
		err = tx.GetContext(ctx, &at, `SELECT MIN(`+deleteAtSQL+`) FROM merge_inputs i JOIN merges m ON m.merge_id = i.merge_id
			WHERE i.pack_id = ? AND m.outcome = 'committed'`, delay.Nanoseconds(), pack[:])
		if err != nil {
			return err
		}
		if at.Valid {
			out.Deadline = min(out.Deadline, at.Int64)
		}

		return nil
	})
	if err != nil && !errors.Is(err, ErrUnknownPack) {
		return Download{}, fmt.Errorf("catalog: deadline of %s: %w", pack, err)
	}

	return out, err
}
