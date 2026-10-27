package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// ErrNoConflict reports a seq no open conflict of the capture covers.
var ErrNoConflict = errors.New("catalog: no conflict at that seq")

// AcknowledgeConflict is conflict acknowledge (operations.md §10.4): every suspect or confirmed conflict of capture covering seq
// becomes acknowledged, which closes the incident and keeps the scope unmerged for its lifetime;
// the scopes it touches advance and emit a conflict event, and the audit row commits with it.
// It returns the number of rows acknowledged, and ErrNoConflict when there is none.
func (c *Catalog) AcknowledgeConflict(ctx context.Context, capture tracepack.UUID, seq uint64, actor, action string) (int, error) {
	at := int64(seq) //nolint:gosec // checked below 2^63 by the caller's parse at most; a larger seq matches no row
	var n int
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, capture); err != nil {
			return err
		}
		var packs []struct {
			Pack  []byte `db:"pack_id"`
			Other []byte `db:"other_pack_id"`
		}
		err := tx.SelectContext(ctx, &packs, `SELECT pack_id, other_pack_id FROM conflicts
			WHERE capture_id = ? AND seq_from <= ? AND seq_to >= ? AND state IN ('suspect', 'confirmed')`, capture[:], at, at)
		if err != nil {
			return err
		}
		if n = len(packs); n == 0 {
			return errRollback
		}
		_, err = tx.ExecContext(ctx, `UPDATE conflicts SET state = 'acknowledged'
			WHERE capture_id = ? AND seq_from <= ? AND seq_to >= ? AND state IN ('suspect', 'confirmed')`, capture[:], at, at)
		if err != nil {
			return err
		}
		ids := make([]tracepack.UUID, 0, 2*n)
		for _, p := range packs {
			ids = append(ids, tracepack.UUID(p.Pack), tracepack.UUID(p.Other))
		}
		e := AuditEntry{Actor: actor, Action: "conflict-acknowledge", ActionID: action, Capture: &capture, Detail: fmt.Sprintf("seq %d", seq)}
		if err := c.auditIn(ctx, tx, e); err != nil {
			return err
		}

		return c.touchScopes(ctx, tx, capture, ids)
	}))
	if err == nil && n == 0 {
		err = ErrNoConflict
	}
	if err != nil {
		return 0, fmt.Errorf("catalog: acknowledge %s seq %d: %w", capture, seq, err)
	}

	return n, nil
}

// ErrUnknownPack reports a pack the catalog does not register.
var ErrUnknownPack = errors.New("catalog: unknown pack")

// ErrNotExcludable reports a pack that is not staged, which only a staged pack can stop being: a merge input.
var ErrNotExcludable = errors.New("catalog: only a staged pack can be excluded")

// ExcludeInput is scope exclude-input (operations.md §10.4): the staged pack becomes excluded under its capture row,
// so the next merge snapshot leaves it out and a merge snapshotted before fails its compare-and-set;
// it stays readable, is never deleted by the merge relation, and a conflict naming it no longer blocks its scope's merge.
// Its scope advances and emits an excluded event, and the audit row commits with it.
func (c *Catalog) ExcludeInput(ctx context.Context, pack tracepack.UUID, actor, action string) (Scope, error) {
	var s Scope
	var capture []byte
	err := c.read.QueryRowContext(ctx, `SELECT capture_id, hour FROM packs WHERE pack_id = ?`, pack[:]).Scan(&capture, &s.Hour)
	if errors.Is(err, sql.ErrNoRows) {
		return Scope{}, fmt.Errorf("catalog: exclude %s: %w", pack, ErrUnknownPack)
	}
	if err != nil {
		return Scope{}, fmt.Errorf("catalog: exclude %s: %w", pack, err)
	}
	s.Capture = tracepack.UUID(capture)
	err = c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, s.Capture); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE packs SET state = 'excluded' WHERE pack_id = ? AND state = 'staged'`, pack[:])
		if err := affected(res, err, ErrNotExcludable); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE scopes SET revision = revision + 1 WHERE capture_id = ? AND hour = ?`, capture, s.Hour); err != nil {
			return err
		}
		var rev int64
		if err := tx.GetContext(ctx, &rev, `SELECT revision FROM scopes WHERE capture_id = ? AND hour = ?`, capture, s.Hour); err != nil {
			return err
		}
		e := AuditEntry{Actor: actor, Action: "scope-exclude-input", ActionID: action, Capture: &s.Capture, Detail: "pack " + pack.String()}
		if err := c.auditIn(ctx, tx, e); err != nil {
			return err
		}

		return c.emit(ctx, tx, []event{{s.Capture, s.Hour, "excluded", rev}})
	})
	if err != nil {
		return Scope{}, fmt.Errorf("catalog: exclude %s: %w", pack, err)
	}

	return s, nil
}

// The kinds of compaction an operator requests.
const (
	// RequestMerge is scope merge: a merge of one scope now.
	RequestMerge = "merge"
	// RequestClose is capture close: a close intent now instead of waiting for I.
	RequestClose = "close"
)

// Request is an operator's request the compactor executes (T-41).
type Request struct {
	ID          tracepack.UUID
	Kind        string
	Capture     tracepack.UUID
	Hour        *int64
	RequestedBy string
	RequestedAt int64
}

// RequestCompaction records an operator's request of kind for capture (and hour, for a merge), with its audit row;
// the compactor holding the lease executes it at its next cycle, under that lease.
func (c *Catalog) RequestCompaction(ctx context.Context, kind string, capture tracepack.UUID, hour *int64, actor string) (tracepack.UUID, error) {
	var id tracepack.UUID
	_, _ = rand.Read(id[:])
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO compact_requests (request_id, kind, capture_id, hour, requested_by, requested_at, state)
			VALUES (?, ?, ?, ?, ?, ?, 'pending')`, id[:], kind, capture[:], hour, actor, c.now())
		if err != nil {
			return err
		}
		detail := "request " + id.String()
		if hour != nil {
			detail += fmt.Sprintf(" hour %d", *hour)
		}

		return c.auditIn(ctx, tx, AuditEntry{Actor: actor, Action: "scope-" + kind, ActionID: id.String(), Capture: &capture, Detail: detail})
	})
	if err != nil {
		return tracepack.UUID{}, fmt.Errorf("catalog: request a %s of %s: %w", kind, capture, err)
	}

	return id, nil
}

// PendingRequests lists the requests not yet executed, oldest first.
func (c *Catalog) PendingRequests(ctx context.Context) ([]Request, error) {
	var rows []struct {
		ID          []byte        `db:"request_id"`
		Kind        string        `db:"kind"`
		Capture     []byte        `db:"capture_id"`
		Hour        sql.NullInt64 `db:"hour"`
		RequestedBy string        `db:"requested_by"`
		RequestedAt int64         `db:"requested_at"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT request_id, kind, capture_id, hour, requested_by, requested_at FROM compact_requests
		WHERE state = 'pending' ORDER BY requested_at, request_id`)
	if err != nil {
		return nil, fmt.Errorf("catalog: pending requests: %w", err)
	}
	out := make([]Request, len(rows))
	for i, r := range rows {
		out[i] = Request{ID: tracepack.UUID(r.ID), Kind: r.Kind, Capture: tracepack.UUID(r.Capture), RequestedBy: r.RequestedBy, RequestedAt: r.RequestedAt}
		if r.Hour.Valid {
			out[i].Hour = &r.Hour.Int64
		}
	}

	return out, nil
}

// Mergeable reports a merge request whose scope a merge may settle now: DecideRequest ended nothing,
// and the request is ended by its merge's commit.
const Mergeable = ""

// DecideRequest ends the pending merge request q under l when its scope cannot be merged now,
// judged under the scope's capture row in the transaction that ends it, so no observation made outside it decides (catalog.md §7.2):
// a settled scope ends it done, a scope without a staged pack or touched by a conflict ends it failed, each with the reason it returns.
// It returns Mergeable, writing nothing, for a scope a merge may settle, and for a request already ended.
func (c *Catalog) DecideRequest(ctx context.Context, l Lease, q Request) (string, error) {
	if q.Kind != RequestMerge || q.Hour == nil {
		return "", fmt.Errorf("catalog: decide request %s: not a merge request", q.ID)
	}
	var result string
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, q.Capture); err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		var err error
		if result, err = c.requestVerdict(ctx, tx, Scope{Capture: q.Capture, Hour: *q.Hour}); err != nil {
			return err
		}
		if result == Mergeable {
			return errRollback
		}
		state := "failed"
		if result == "settled" {
			state = "done"
		}
		res, err := tx.ExecContext(ctx, `UPDATE compact_requests SET state = ?, outcome = ?, done_at = ? WHERE request_id = ? AND state = 'pending'`,
			state, result, c.now(), q.ID[:])
		if err := affected(res, err, errRollback); err != nil {
			result = Mergeable

			return err
		}

		return nil
	}))
	if err != nil {
		return "", fmt.Errorf("catalog: decide request %s: %w", q.ID, err)
	}

	return result, nil
}

// requestVerdict judges, under the capture row, whether the scope s can be merged now, or why not.
func (c *Catalog) requestVerdict(ctx context.Context, tx *sqlx.Tx, s Scope) (string, error) {
	var head struct {
		State   string `db:"state"`
		Touched bool   `db:"touched"`
		Staged  int    `db:"staged"`
	}
	err := tx.GetContext(ctx, &head, `SELECT s.state, `+touchingConflict+` AS touched,
		(SELECT COUNT(*) FROM packs p WHERE p.capture_id = s.capture_id AND p.hour = s.hour AND p.state = 'staged') AS staged
		FROM scopes s WHERE s.capture_id = ? AND s.hour = ?`, s.Capture[:], s.Hour)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "the scope has no staged pack", nil
	case err != nil:
		return "", err
	case head.State == "settled":
		return "settled", nil
	case head.Staged == 0:
		return "the scope has no staged pack", nil
	case head.Touched:
		return "a conflict touches the scope", nil
	default:
		return Mergeable, nil
	}
}

// RequestStatus reads a request's state, pending, done or failed, and its outcome once it ended.
func (c *Catalog) RequestStatus(ctx context.Context, id tracepack.UUID) (state, outcome string, err error) {
	var o sql.NullString
	err = c.read.QueryRowContext(ctx, `SELECT state, outcome FROM compact_requests WHERE request_id = ?`, id[:]).Scan(&state, &o)
	if err != nil {
		return "", "", fmt.Errorf("catalog: request %s: %w", id, err)
	}

	return state, o.String, nil
}
