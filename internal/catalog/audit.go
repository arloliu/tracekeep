package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// AuditEntry is one row of the append-only audit log.
type AuditEntry struct {
	Actor    string
	Action   string
	ActionID string
	Capture  *tracepack.UUID
	Detail   string
}

// Audit appends e to the audit log, outside any domain transaction (catalog.md §7.2).
func (c *Catalog) Audit(ctx context.Context, e AuditEntry) error {
	return c.auditIn(ctx, c.write, e)
}

// auditIn appends e on q: the write pool, or the transaction whose audit row it is.
// Its failure wraps ErrAudit, so a caller tells a failed audit row from the rest of a transaction's failures (api.md §10.1).
func (c *Catalog) auditIn(ctx context.Context, q sqlx.ExecerContext, e AuditEntry) error {
	var id [16]byte
	_, _ = rand.Read(id[:])
	var capture []byte
	if e.Capture != nil {
		capture = e.Capture[:]
	}
	_, err := q.ExecContext(ctx, `INSERT INTO audit_log (audit_id, at, actor, action, action_id, capture_id, detail) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id[:], c.now(), e.Actor, e.Action, nullString(e.ActionID), capture, nullString(e.Detail))
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrAudit, e.Action, err)
	}

	return nil
}

// auditByCaptureQuery reads a capture's audit rows, oldest first; auditByActorQuery an actor's.
const (
	auditByCaptureQuery = `SELECT actor, action, action_id, detail FROM audit_log WHERE capture_id = ? ORDER BY at, audit_id`
	auditByActorQuery   = `SELECT actor, action, action_id, capture_id, detail FROM audit_log WHERE actor = ? ORDER BY at, audit_id`
)

// AuditEntries returns the audit rows of capture, oldest first.
func (c *Catalog) AuditEntries(ctx context.Context, capture tracepack.UUID) ([]AuditEntry, error) {
	var rows []struct {
		Actor    string         `db:"actor"`
		Action   string         `db:"action"`
		ActionID sql.NullString `db:"action_id"`
		Detail   sql.NullString `db:"detail"`
	}
	err := c.read.SelectContext(ctx, &rows, auditByCaptureQuery, capture[:])
	if err != nil {
		return nil, fmt.Errorf("catalog: audit of %s: %w", capture, err)
	}
	out := make([]AuditEntry, len(rows))
	for i, r := range rows {
		out[i] = AuditEntry{Actor: r.Actor, Action: r.Action, ActionID: r.ActionID.String, Capture: &capture, Detail: r.Detail.String}
	}

	return out, nil
}

// AuditByActor returns the audit rows actor wrote, oldest first.
func (c *Catalog) AuditByActor(ctx context.Context, actor string) ([]AuditEntry, error) {
	var rows []struct {
		Actor    string         `db:"actor"`
		Action   string         `db:"action"`
		ActionID sql.NullString `db:"action_id"`
		Capture  []byte         `db:"capture_id"`
		Detail   sql.NullString `db:"detail"`
	}
	err := c.read.SelectContext(ctx, &rows, auditByActorQuery, actor)
	if err != nil {
		return nil, fmt.Errorf("catalog: audit by %s: %w", actor, err)
	}
	out := make([]AuditEntry, len(rows))
	for i, r := range rows {
		out[i] = AuditEntry{Actor: r.Actor, Action: r.Action, ActionID: r.ActionID.String, Detail: r.Detail.String}
		if len(r.Capture) == 16 {
			id := tracepack.UUID(r.Capture)
			out[i].Capture = &id
		}
	}

	return out, nil
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
