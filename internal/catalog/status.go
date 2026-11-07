package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// unresolvedStates are the parked versions' states that still defer their messages (parking.md).
const unresolvedStates = `('parking', 'parked', 'evicted-hour')`

// PendingCapture is a capture parking created before its start was seen, by capture id: it has no equipment yet.
type PendingCapture struct {
	Capture tracepack.UUID
	// SinceNS is when the catalog first saw it.
	SinceNS int64
}

// EquipmentStatus is one piece of equipment's line of tracekeep status (operations.md §10.4).
type EquipmentStatus struct {
	EquipmentID string
	// LastRecordNS is the time of the latest record registered of the equipment's admitted captures, nil before any.
	LastRecordNS *int64
	OpenCaptures int
	// Deferred counts the unresolved parked versions of the equipment's captures.
	Deferred int
}

// Status is what tracekeep status reads of the catalog: the pending captures by id, then each piece of equipment by id,
// and the number of equipment whose log captures fell silent (logfeed.md §15.11).
type Status struct {
	Pending   []PendingCapture
	Equipment []EquipmentStatus
	// SilentLogEquipment counts the equipment whose log captures registered a pack in the past SilentWithin and none for SilentAfter.
	SilentLogEquipment int
}

// The windows of Status's silent log equipment.
const (
	SilentAfter  = 15 * time.Minute
	SilentWithin = 24 * time.Hour
)

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
		var equipment []struct {
			Equipment string        `db:"equipment_id"`
			Last      sql.NullInt64 `db:"last_record"`
			Open      int           `db:"open_captures"`
		}
		err := tx.SelectContext(ctx, &equipment, `SELECT equipment_id,
				MAX(CASE WHEN state IN `+admittedStates+` THEN highest_seq_ts END) AS last_record,
				SUM(CASE WHEN state = 'open' THEN 1 ELSE 0 END) AS open_captures
			FROM captures WHERE equipment_id IS NOT NULL GROUP BY equipment_id ORDER BY equipment_id`)
		if err != nil {
			return err
		}
		var deferred []struct {
			Equipment string `db:"equipment_id"`
			N         int    `db:"n"`
		}
		err = tx.SelectContext(ctx, &deferred, `SELECT k.equipment_id, COUNT(*) AS n FROM parked p JOIN captures k ON k.capture_id = p.capture_id
			WHERE p.state IN `+unresolvedStates+` AND k.equipment_id IS NOT NULL GROUP BY k.equipment_id`)
		if err != nil {
			return err
		}
		byEquipment := map[string]int{}
		for _, d := range deferred {
			byEquipment[d.Equipment] = d.N
		}
		for _, t := range equipment {
			ts := EquipmentStatus{EquipmentID: t.Equipment, OpenCaptures: t.Open, Deferred: byEquipment[t.Equipment]}
			if t.Last.Valid {
				ts.LastRecordNS = &t.Last.Int64
			}
			out.Equipment = append(out.Equipment, ts)
		}
		// shortcut: scans the log captures' rows, an operator's command; index (capture_method, last_registered_at) if status grows slow.
		now := c.now()

		return tx.GetContext(ctx, &out.SilentLogEquipment, `SELECT COUNT(*) FROM (SELECT equipment_id FROM captures
			WHERE capture_method = 'log' AND last_registered_at >= ? GROUP BY equipment_id HAVING MAX(last_registered_at) < ?) s`,
			now-int64(SilentWithin), now-int64(SilentAfter))
	})
	if err != nil {
		return Status{}, fmt.Errorf("catalog: status: %w", err)
	}

	return out, nil
}

// DeferredGroup is the unresolved parked versions of one capture and reason: what waits, why, and since when.
type DeferredGroup struct {
	Capture tracepack.UUID
	// EquipmentID is the capture's equipment, empty for a pending capture.
	EquipmentID string
	Reason      string
	Count       int
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
		Capture   []byte         `db:"capture_id"`
		Equipment sql.NullString `db:"equipment_id"`
		Reason    string         `db:"reason"`
		N         int            `db:"n"`
		Oldest    int64          `db:"oldest"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT p.capture_id, k.equipment_id, p.reason, COUNT(*) AS n, MIN(p.parked_at) AS oldest
		FROM parked p JOIN captures k ON k.capture_id = p.capture_id
		WHERE p.state IN `+unresolvedStates+where+` GROUP BY p.capture_id, k.equipment_id, p.reason ORDER BY p.capture_id, p.reason`, args...)
	if err != nil {
		return nil, fmt.Errorf("catalog: deferred: %w", err)
	}
	out := make([]DeferredGroup, 0, len(rows))
	for _, r := range rows {
		out = append(out, DeferredGroup{Capture: tracepack.UUID(r.Capture), EquipmentID: r.Equipment.String, Reason: r.Reason, Count: r.N, OldestNS: r.Oldest})
	}

	return out, nil
}
