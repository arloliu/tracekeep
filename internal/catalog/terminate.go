package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// ApplyOrder terminates the delivery's version under the active termination order that covers it,
// in a transaction of its own (parking.md *Operator termination*), and returns the version's resolution as re-read under the capture row:
// ResolvedTerminated once it is terminated, now or before; ResolvedStaged for a version staged meanwhile, which an order never undoes;
// Unresolved when no active order covers the delivery.
func (c *Catalog) ApplyOrder(ctx context.Context, d *Identity) (Resolution, error) {
	var r Resolution
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, d.Version.Capture); err != nil {
			return err
		}
		var err error
		if r, err = c.recheck(ctx, tx, d); err != nil {
			return err
		}
		if r == CoveredByOrder {
			r = ResolvedTerminated
			return nil
		}

		return errRollback
	}))
	if err != nil {
		return 0, fmt.Errorf("catalog: apply order to %s.%d: %w", d.Version.Capture, d.Version.Seq, err)
	}

	return r, nil
}

// recheck is the termination re-check every ingest effect of a capture runs under its row (catalog.md §7.2, T-32):
// it re-reads the delivery's resolution and the capture's active orders, and applies an order that covers the delivery.
// CoveredByOrder reports that it applied one, which the transaction commits as its only effect;
// ResolvedTerminated reports a version terminated before, for which nothing is written.
func (c *Catalog) recheck(ctx context.Context, tx *sqlx.Tx, d *Identity) (Resolution, error) {
	r, action, err := resolveIn(ctx, tx, &d.Version, d.Incarnation, d.StreamSeq)
	if err != nil || r != CoveredByOrder {
		return r, err
	}

	return CoveredByOrder, c.terminateUnder(ctx, tx, d, action)
}

// lostToTermination runs the re-check for an admission or a rejection: lost reports a delivery that lost to a termination,
// with errRollback when nothing was written, so even the prologue is undone.
func (c *Catalog) lostToTermination(ctx context.Context, tx *sqlx.Tx, d *Identity) (bool, error) {
	r, err := c.recheck(ctx, tx, d)
	switch {
	case err != nil:
		return true, err
	case r == ResolvedTerminated:
		return true, errRollback
	case r == CoveredByOrder:
		return true, nil
	default:
		return false, nil
	}
}

// terminateUnder terminates the delivery's unresolved version under the order action:
// the version terminated with the delivery's copy row, the audit row, and the terminated event of its hour, emitted last (T-33).
func (c *Catalog) terminateUnder(ctx context.Context, tx *sqlx.Tx, d *Identity, action string) error {
	v := &d.Version
	now := c.now()
	res, err := tx.ExecContext(ctx, `UPDATE parked SET state = 'terminated', terminated_by = ?, resolved_at = ?
		WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ? AND state IN ('parking', 'parked', 'evicted-hour')`,
		action, now, v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		_, err := tx.ExecContext(ctx, `INSERT INTO parked (capture_id, seq, header, payload_digest, state, reason, hour, terminated_by, parked_at, resolved_at)
			VALUES (?, ?, ?, ?, 'terminated', 'termination-order', ?, ?, ?, ?)`,
			v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:], d.Hour, action, now, now)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, c.d.insertIgnore+` INTO parked_copies (incarnation_id, stream_seq, capture_id, seq, header, payload_digest, state)
		VALUES (?, ?, ?, ?, ?, ?, 'waiting')`, d.Incarnation, d.StreamSeq, v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
	if err != nil {
		return err
	}
	capture := v.Capture
	err = c.auditIn(ctx, tx, AuditEntry{
		Actor: "ingest", Action: "terminate", ActionID: action, Capture: &capture,
		Detail: fmt.Sprintf("seq %d incarnation %d stream seq %d", v.Seq, d.Incarnation, d.StreamSeq),
	})
	if err != nil {
		return err
	}
	evs, err := c.terminatedScope(ctx, tx, d)
	if err != nil {
		return err
	}

	return c.emit(ctx, tx, evs)
}

// terminatedScope advances the scope of the delivery's hour, creating it when missing, and returns its terminated event;
// an hour retention evicted gets neither, since its seqs read from evicted_ranges and parked (catalog.md §7.2).
func (c *Catalog) terminatedScope(ctx context.Context, tx *sqlx.Tx, d *Identity) ([]event, error) {
	capture := d.Version.Capture
	var n int
	if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM evicted_ranges WHERE capture_id = ? AND hour = ?`, capture[:], d.Hour); err != nil || n > 0 {
		return nil, err
	}
	var rev int64
	err := tx.GetContext(ctx, &rev, `SELECT revision FROM scopes WHERE capture_id = ? AND hour = ?`, capture[:], d.Hour)
	if errors.Is(err, sql.ErrNoRows) {
		rev = 1
		_, err = tx.ExecContext(ctx, `INSERT INTO scopes (capture_id, hour, state, revision) VALUES (?, ?, 'open', 1)`, capture[:], d.Hour)
	} else if err == nil {
		rev++
		_, err = tx.ExecContext(ctx, `UPDATE scopes SET revision = ? WHERE capture_id = ? AND hour = ?`, rev, capture[:], d.Hour)
	}
	if err != nil {
		return nil, err
	}

	return []event{{capture, d.Hour, "terminated", rev}}, nil
}
