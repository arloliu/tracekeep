package catalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// unresolvedStates are the parked versions' states that still defer their messages (parking.md).
const unresolvedStates = `('parking', 'parked', 'evicted-hour')`

// PendingCapture is a capture parking created before its start was seen, by capture id: it has no tool yet.
type PendingCapture struct {
	Capture tracepack.UUID
	// SinceNS is when the catalog first saw it.
	SinceNS int64
}

// ToolStatus is one tool's line of tracekeep status (operations.md §10.4).
type ToolStatus struct {
	ToolID string
	// LastRecordNS is the time of the latest record registered of the tool's admitted captures, nil before any.
	LastRecordNS *int64
	OpenCaptures int
	// Deferred counts the unresolved parked versions of the tool's captures.
	Deferred int
}

// Status is what tracekeep status reads of the catalog: the pending captures by id, then each tool by id.
type Status struct {
	Pending []PendingCapture
	Tools   []ToolStatus
}

// Status reads the catalog's part of tracekeep status from one snapshot.
func (c *Catalog) Status(ctx context.Context) (Status, error) {
	var out Status
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		var pending []struct {
			Capture []byte `db:"capture_id"`
			Since   int64  `db:"created_at"`
		}
		if err := tx.SelectContext(ctx, &pending, `SELECT capture_id, created_at FROM captures WHERE state = 'pending' ORDER BY capture_id`); err != nil {
			return err
		}
		for _, p := range pending {
			out.Pending = append(out.Pending, PendingCapture{Capture: tracepack.UUID(p.Capture), SinceNS: p.Since})
		}
		var tools []struct {
			Tool string        `db:"tool_id"`
			Last sql.NullInt64 `db:"last_record"`
			Open int           `db:"open_captures"`
		}
		err := tx.SelectContext(ctx, &tools, `SELECT tool_id,
				MAX(CASE WHEN state IN `+admittedStates+` THEN highest_seq_ts END) AS last_record,
				SUM(CASE WHEN state = 'open' THEN 1 ELSE 0 END) AS open_captures
			FROM captures WHERE tool_id IS NOT NULL GROUP BY tool_id ORDER BY tool_id`)
		if err != nil {
			return err
		}
		var deferred []struct {
			Tool string `db:"tool_id"`
			N    int    `db:"n"`
		}
		err = tx.SelectContext(ctx, &deferred, `SELECT k.tool_id, COUNT(*) AS n FROM parked p JOIN captures k ON k.capture_id = p.capture_id
			WHERE p.state IN `+unresolvedStates+` AND k.tool_id IS NOT NULL GROUP BY k.tool_id`)
		if err != nil {
			return err
		}
		byTool := map[string]int{}
		for _, d := range deferred {
			byTool[d.Tool] = d.N
		}
		for _, t := range tools {
			ts := ToolStatus{ToolID: t.Tool, OpenCaptures: t.Open, Deferred: byTool[t.Tool]}
			if t.Last.Valid {
				ts.LastRecordNS = &t.Last.Int64
			}
			out.Tools = append(out.Tools, ts)
		}

		return nil
	})
	if err != nil {
		return Status{}, fmt.Errorf("catalog: status: %w", err)
	}

	return out, nil
}

// DeferredGroup is the unresolved parked versions of one capture and reason: what waits, why, and since when.
type DeferredGroup struct {
	Capture tracepack.UUID
	// ToolID is the capture's tool, empty for a pending capture.
	ToolID string
	Reason string
	Count  int
	// OldestNS is when the group's oldest version was parked.
	OldestNS int64
}

// Deferred lists the unresolved parked versions by capture and reason (operations.md §10.4 deferred list), of capture alone when it is not nil.
func (c *Catalog) Deferred(ctx context.Context, capture *tracepack.UUID) ([]DeferredGroup, error) {
	where, args := "", []any{}
	if capture != nil {
		where, args = " AND p.capture_id = ?", []any{capture[:]}
	}
	var rows []struct {
		Capture []byte         `db:"capture_id"`
		Tool    sql.NullString `db:"tool_id"`
		Reason  string         `db:"reason"`
		N       int            `db:"n"`
		Oldest  int64          `db:"oldest"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT p.capture_id, k.tool_id, p.reason, COUNT(*) AS n, MIN(p.parked_at) AS oldest
		FROM parked p JOIN captures k ON k.capture_id = p.capture_id
		WHERE p.state IN `+unresolvedStates+where+` GROUP BY p.capture_id, k.tool_id, p.reason ORDER BY p.capture_id, p.reason`, args...)
	if err != nil {
		return nil, fmt.Errorf("catalog: deferred: %w", err)
	}
	out := make([]DeferredGroup, 0, len(rows))
	for _, r := range rows {
		out = append(out, DeferredGroup{Capture: tracepack.UUID(r.Capture), ToolID: r.Tool.String, Reason: r.Reason, Count: r.N, OldestNS: r.Oldest})
	}

	return out, nil
}
