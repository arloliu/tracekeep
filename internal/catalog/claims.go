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

// ClaimObject is a claim object's row (compactor.md §8.4): its bucket-qualified key, capture, seq, the record's hour and size,
// its state, live, survivor or deletable, and a survivor's evicted hour.
type ClaimObject struct {
	Key         string
	Capture     tracepack.UUID
	Seq         uint64
	Hour        int64
	Size        int64
	State       string
	EvictedHour *int64
}

// UnattributedObject is a claim bucket object without the SDK's metadata, which only an operator deletes.
type UnattributedObject struct {
	Key    string
	Size   int64
	SeenAt int64
}

// Listed is one object of a claim bucket listing: its bucket-qualified key and size,
// and Attr its attribution by its name and the SDK's metadata, nil when it has none.
type Listed struct {
	Key  string
	Size int64
	Attr *ClaimObject
}

// RecordListing records a listing of the claim bucket under l at now (compactor.md §8.3–§8.4):
// an attributed object without a row gets a live one, whatever its capture's state, and a row it finds stays as it is;
// an unattributed object without a row is recorded with its first sighting, and one the listing no longer holds,
// or that has gained a row since, is forgotten.
// It returns how many claim object rows it added.
func (c *Catalog) RecordListing(ctx context.Context, l Lease, objs []Listed, now int64) (int, error) {
	added := 0
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		added = 0
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		listed := map[string]bool{}
		for _, o := range objs {
			if o.Attr == nil {
				// An object with a row, a receipt's, is attributed by it; only one without is the operator's.
				var n int
				if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM claim_objects WHERE object_key = ?`, o.Key); err != nil {
					return err
				}
				if n > 0 {
					continue
				}
				listed[o.Key] = true
				_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO unattributed_objects (object_key, size, seen_at) VALUES (?, ?, ?)`, o.Key, o.Size, now)
				if err != nil {
					return err
				}

				continue
			}
			res, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO claim_objects (object_key, capture_id, seq, hour, size, state) VALUES (?, ?, ?, ?, ?, 'live')`,
				o.Key, o.Attr.Capture[:], int64(o.Attr.Seq), o.Attr.Hour, o.Size) //nolint:gosec // a parsed seq is below 2^63
			if err != nil {
				return err
			}
			if n, err := res.RowsAffected(); err != nil {
				return err
			} else if n > 0 {
				added++
			}
		}
		var known []string
		if err := tx.SelectContext(ctx, &known, `SELECT object_key FROM unattributed_objects`); err != nil {
			return err
		}
		for _, key := range known {
			if listed[key] {
				continue
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM unattributed_objects WHERE object_key = ?`, key); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: record the claim listing: %w", err)
	}

	return added, nil
}

type claimRecord struct {
	Key         string        `db:"object_key"`
	Capture     []byte        `db:"capture_id"`
	Seq         int64         `db:"seq"`
	Hour        int64         `db:"hour"`
	Size        int64         `db:"size"`
	State       string        `db:"state"`
	EvictedHour sql.NullInt64 `db:"evicted_hour"`
}

func (r *claimRecord) object() ClaimObject {
	o := ClaimObject{Key: r.Key, Capture: tracepack.UUID(r.Capture), Seq: uint64(r.Seq), Hour: r.Hour, Size: r.Size, State: r.State} //nolint:gosec // stored seqs are below 2^63
	if r.EvictedHour.Valid {
		o.EvictedHour = &r.EvictedHour.Int64
	}

	return o
}

const claimColumns = `object_key, capture_id, seq, hour, size, state, evicted_hour`

// ClaimObjects returns the claim object rows in the given states, every row when none is given, by key.
func (c *Catalog) ClaimObjects(ctx context.Context, states ...string) ([]ClaimObject, error) {
	q, args := `SELECT `+claimColumns+` FROM claim_objects`, []any{}
	if len(states) > 0 {
		q += ` WHERE state IN (?` + strings.Repeat(", ?", len(states)-1) + `)`
		for _, s := range states {
			args = append(args, s)
		}
	}
	var rows []claimRecord
	if err := c.read.SelectContext(ctx, &rows, q+` ORDER BY object_key`, args...); err != nil {
		return nil, fmt.Errorf("catalog: claim objects: %w", err)
	}
	out := make([]ClaimObject, 0, len(rows))
	for i := range rows {
		out = append(out, rows[i].object())
	}

	return out, nil
}

// UnattributedObjects returns the claim bucket objects without the SDK's metadata, by key.
func (c *Catalog) UnattributedObjects(ctx context.Context) ([]UnattributedObject, error) {
	var rows []struct {
		Key    string `db:"object_key"`
		Size   int64  `db:"size"`
		SeenAt int64  `db:"seen_at"`
	}
	if err := c.read.SelectContext(ctx, &rows, `SELECT object_key, size, seen_at FROM unattributed_objects ORDER BY object_key`); err != nil {
		return nil, fmt.Errorf("catalog: unattributed objects: %w", err)
	}
	out := make([]UnattributedObject, 0, len(rows))
	for _, r := range rows {
		out = append(out, UnattributedObject{Key: r.Key, Size: r.Size, SeenAt: r.SeenAt})
	}

	return out, nil
}

// claimIn locks the capture of the claim object key and returns its row, ok false when there is none.
func (c *Catalog) claimIn(ctx context.Context, tx *sqlx.Tx, l Lease, key string) (claimRecord, bool, error) {
	var r claimRecord
	err := c.read.GetContext(ctx, &r, `SELECT `+claimColumns+` FROM claim_objects WHERE object_key = ?`, key)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, c.checkLease(ctx, tx, l)
	}
	if err != nil {
		return r, false, err
	}
	if _, err := c.lockCapture(ctx, tx, tracepack.UUID(r.Capture)); err != nil {
		return r, false, err
	}
	if err := c.checkLease(ctx, tx, l); err != nil {
		return r, false, err
	}
	err = tx.GetContext(ctx, &r, `SELECT `+claimColumns+` FROM claim_objects WHERE object_key = ?`, key)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}

	return r, err == nil, err
}

// MarkSurvivorDeletable marks the survivor key deletable under its capture row and l once §8.4's predicate holds (catalog.md §7.2),
// and reports whether the row is deletable; the caller then deletes the object and removes the row. A deletable row stays deletable.
func (c *Catalog) MarkSurvivorDeletable(ctx context.Context, l Lease, key string) (bool, error) {
	deletable := false
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		deletable = false
		r, ok, err := c.claimIn(ctx, tx, l, key)
		if err != nil || !ok {
			return err
		}
		switch r.State {
		case "deletable":
			deletable = true

			return errRollback
		case "survivor":
		default:
			return errRollback
		}
		if ok, err := released(ctx, tx, tracepack.UUID(r.Capture), r.Seq); err != nil || !ok {
			if err == nil {
				err = errRollback
			}

			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE claim_objects SET state = 'deletable' WHERE object_key = ? AND state = 'survivor'`, key); err != nil {
			return err
		}
		deletable = true

		return nil
	}))
	if err != nil {
		return false, fmt.Errorf("catalog: release survivor %s: %w", key, err)
	}

	return deletable, nil
}

// RemoveClaimObject removes the deletable row of key under its capture row and l, once its object is deleted; any other row stays.
func (c *Catalog) RemoveClaimObject(ctx context.Context, l Lease, key string) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, _, err := c.claimIn(ctx, tx, l, key); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM claim_objects WHERE object_key = ? AND state = 'deletable'`, key)

		return err
	})
	if err != nil {
		return fmt.Errorf("catalog: remove claim object %s: %w", key, err)
	}

	return nil
}

// ClaimStats is what the claim object gauges read: the rows in every state, their bytes, and the survivors among them.
type ClaimStats struct {
	Objects, Bytes, Survivors int64
}

// ClaimStats reads the claim object gauges.
func (c *Catalog) ClaimStats(ctx context.Context) (ClaimStats, error) {
	var st struct {
		Objects   int64 `db:"objects"`
		Bytes     int64 `db:"bytes"`
		Survivors int64 `db:"survivors"`
	}
	err := c.read.GetContext(ctx, &st, `SELECT COUNT(*) AS objects, COALESCE(SUM(size), 0) AS bytes,
		COALESCE(SUM(CASE WHEN state IN ('survivor', 'deletable') THEN 1 ELSE 0 END), 0) AS survivors FROM claim_objects`)
	if err != nil {
		return ClaimStats{}, fmt.Errorf("catalog: claim stats: %w", err)
	}

	return ClaimStats{Objects: st.Objects, Bytes: st.Bytes, Survivors: st.Survivors}, nil
}
