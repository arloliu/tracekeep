package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"

	"github.com/arloliu/go-secs/tracepack"
)

// VersionKey identifies one version of a record (parking.md *Parking*): capture, seq, the 44 header bytes and the payload digest.
type VersionKey struct {
	Capture tracepack.UUID
	Seq     uint64
	Header  [44]byte
	Digest  [32]byte
}

// Resolution is a version's resolution, or the termination order that covers the message carrying it.
type Resolution int

const (
	// Unresolved: no parked row resolves the version, and no active order covers the message.
	Unresolved Resolution = iota + 1
	// ResolvedStaged: a parked row of the version is staged.
	ResolvedStaged
	// ResolvedTerminated: a parked row of the version is terminated.
	ResolvedTerminated
	// CoveredByOrder: unresolved, and an active termination order of the capture covers the message's stream sequence on its incarnation.
	CoveredByOrder
)

// ReceiptMatch is how the receipts of (capture, seq) compare with a delivery's version (ingest.md §6.1).
type ReceiptMatch int

const (
	// ReceiptNone: no receipt of the seq.
	ReceiptNone ReceiptMatch = iota + 1
	// ReceiptEqual: a receipt of this very version.
	ReceiptEqual
	// ReceiptOther: receipts of other versions only.
	ReceiptOther
)

// Resolve looks the version up in parked and the capture's active termination orders,
// for the message at streamSeq on incarnation (ingest.md §6.1 *resolution before framing and admission*).
// It is a read: a transaction that acts on the answer re-reads it under the capture row.
func (c *Catalog) Resolve(ctx context.Context, v *VersionKey, incarnation, streamSeq int64) (Resolution, error) {
	var state string
	err := c.read.GetContext(ctx, &state, `SELECT state FROM parked WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ?`,
		v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
	switch {
	case err == nil && state == "staged":
		return ResolvedStaged, nil
	case err == nil && state == "terminated":
		return ResolvedTerminated, nil
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("catalog: resolve %s.%d: %w", v.Capture, v.Seq, err)
	}
	var n int
	err = c.read.GetContext(ctx, &n, `SELECT COUNT(*) FROM termination_orders WHERE capture_id = ? AND incarnation_id = ? AND state = 'active' AND cut_off >= ?`,
		v.Capture[:], incarnation, streamSeq)
	if err != nil {
		return 0, fmt.Errorf("catalog: resolve %s.%d: %w", v.Capture, v.Seq, err)
	}
	if n > 0 {
		return CoveredByOrder, nil
	}

	return Unresolved, nil
}

// Receipt compares the receipts of the version's (capture, seq) with the version.
func (c *Catalog) Receipt(ctx context.Context, v *VersionKey) (ReceiptMatch, error) {
	var rows []struct {
		Header []byte `db:"header"`
		Digest []byte `db:"payload_sha256"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT header, payload_sha256 FROM claim_records WHERE capture_id = ? AND seq = ?`, v.Capture[:], int64(v.Seq))
	if err != nil {
		return 0, fmt.Errorf("catalog: receipt %s.%d: %w", v.Capture, v.Seq, err)
	}
	if len(rows) == 0 {
		return ReceiptNone, nil
	}
	for _, r := range rows {
		if [44]byte(r.Header) == v.Header && [32]byte(r.Digest) == v.Digest {
			return ReceiptEqual, nil
		}
	}

	return ReceiptOther, nil
}

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
	var id [16]byte
	_, _ = rand.Read(id[:])
	var capture []byte
	if e.Capture != nil {
		capture = e.Capture[:]
	}
	_, err := c.write.ExecContext(ctx, `INSERT INTO audit_log (audit_id, at, actor, action, action_id, capture_id, detail) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id[:], c.now(), e.Actor, e.Action, nullString(e.ActionID), capture, nullString(e.Detail))
	if err != nil {
		return fmt.Errorf("catalog: audit %s: %w", e.Action, err)
	}

	return nil
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// Conflict is a run of seqs two packs of a capture both hold.
type Conflict struct {
	SeqFrom, SeqTo int64
	Pack, Other    tracepack.UUID
	// State is suspect until compact compares the packs, or confirmed when they differ.
	State string
}

// Conflicts returns the conflicts of capture.
func (c *Catalog) Conflicts(ctx context.Context, capture tracepack.UUID) ([]Conflict, error) {
	var rows []struct {
		From  int64  `db:"seq_from"`
		To    int64  `db:"seq_to"`
		Pack  []byte `db:"pack_id"`
		Other []byte `db:"other_pack_id"`
		State string `db:"state"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT seq_from, seq_to, pack_id, other_pack_id, state FROM conflicts WHERE capture_id = ? ORDER BY seq_from`, capture[:])
	if err != nil {
		return nil, fmt.Errorf("catalog: conflicts of %s: %w", capture, err)
	}
	out := make([]Conflict, len(rows))
	for i, r := range rows {
		out[i] = Conflict{SeqFrom: r.From, SeqTo: r.To, Pack: tracepack.UUID(r.Pack), Other: tracepack.UUID(r.Other), State: r.State}
	}

	return out, nil
}
