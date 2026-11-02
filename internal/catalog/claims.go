package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
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

// listingChunk is the most listed objects one transaction of RecordListing records, and the most unattributed rows a page of its sweep reads.
const listingChunk = 1024

// listingParams is the most placeholders one statement of RecordListing binds.
const listingParams = 900

// RecordListing records a listing of the claim bucket under l at now (compactor.md §8.3–§8.4):
// an attributed object without a row gets a live one, whatever its capture's state, and a row it finds stays as it is;
// an unattributed object without a row is recorded with its first sighting, and one the listing no longer holds,
// or that has gained a row since, is forgotten.
// The listing is read against the rows already recorded and written in transactions of at most listingChunk objects,
// each checking the lease, so a steady listing writes nothing; the lease is verified even then.
// The unattributed rows are forgotten page by page only once every chunk committed, so a failure before then forgets nothing,
// and the rows added so far stay for a retry, which adds only what is still missing.
// It returns how many claim object rows it added, zero with any error.
func (c *Catalog) RecordListing(ctx context.Context, l Lease, objs []Listed, now int64) (int, error) {
	if err := c.VerifyLease(ctx, l); err != nil {
		return 0, fmt.Errorf("catalog: record the claim listing: %w", err)
	}
	// listed is the listing's unattributed keys, which the sweep keeps unless a row attributes them.
	listed := map[string]bool{}
	added := 0
	for chunk := range slices.Chunk(objs, listingChunk) {
		n, err := c.recordChunk(ctx, l, chunk, now, listed)
		if err != nil {
			return 0, fmt.Errorf("catalog: record the claim listing: %w", err)
		}
		added += n
	}
	if err := c.sweepUnattributed(ctx, l, listed); err != nil {
		return 0, fmt.Errorf("catalog: record the claim listing: %w", err)
	}

	return added, nil
}

// recordChunk writes what chunk adds to the rows recorded, in one transaction under l if it adds any, adding its unattributed keys to listed;
// it returns the claim object rows it added.
func (c *Catalog) recordChunk(ctx context.Context, l Lease, chunk []Listed, now int64, listed map[string]bool) (int, error) {
	keys := make([]string, len(chunk))
	var odd []string
	for i, o := range chunk {
		keys[i] = o.Key
		if o.Attr == nil {
			odd = append(odd, o.Key)
			listed[o.Key] = true
		}
	}
	claimed, err := c.recordedKeys(ctx, "claim_objects", keys)
	if err != nil {
		return 0, err
	}
	known, err := c.recordedKeys(ctx, "unattributed_objects", odd)
	if err != nil {
		return 0, err
	}
	c.hook(hookListingRead)
	var claims, unattributed [][]any
	for _, o := range chunk {
		switch {
		case claimed[o.Key]:
			// An object with a row, a receipt's, is attributed by it; only one without is the operator's.
		case o.Attr != nil:
			claims = append(claims, []any{o.Key, o.Attr.Capture[:], int64(o.Attr.Seq), o.Attr.Hour, o.Size, "live"}) //nolint:gosec // a parsed seq is below 2^63
			claimed[o.Key] = true
		case !known[o.Key]:
			unattributed = append(unattributed, []any{o.Key, o.Size, now})
			known[o.Key] = true
		default:
			// Recorded unattributed already: its first sighting stays.
		}
	}
	if len(claims)+len(unattributed) == 0 {
		return 0, nil
	}
	added := 0
	err = c.inTx(ctx, func(tx *sqlx.Tx) error {
		added = 0
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		n, err := insertListed(ctx, tx, c.d.insertIgnore+` INTO claim_objects (object_key, capture_id, seq, hour, size, state) VALUES `, claims)
		if err != nil {
			return err
		}
		added = n
		_, err = insertListed(ctx, tx, c.d.insertIgnore+` INTO unattributed_objects (object_key, size, seen_at) VALUES `, unattributed)

		return err
	})

	return added, err
}

// recordedKeys returns which of keys table holds, read in statements of at most listingParams keys.
func (c *Catalog) recordedKeys(ctx context.Context, table string, keys []string) (map[string]bool, error) {
	found := map[string]bool{}
	for batch := range slices.Chunk(keys, listingParams) {
		var got []string
		if err := c.read.SelectContext(ctx, &got, `SELECT object_key FROM `+table+` WHERE object_key IN `+listingTuple(len(batch)), listingArgs(batch)...); err != nil {
			return nil, err
		}
		for _, k := range got {
			found[k] = true
		}
	}

	return found, nil
}

// insertListed runs the multi-row insert prefix ... VALUES over rows, in statements of at most listingParams placeholders,
// and returns the rows it inserted.
func insertListed(ctx context.Context, tx *sqlx.Tx, prefix string, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	row := listingTuple(len(rows[0]))
	inserted := 0
	for batch := range slices.Chunk(rows, listingParams/len(rows[0])) {
		args := make([]any, 0, len(batch)*len(batch[0]))
		for _, r := range batch {
			args = append(args, r...)
		}
		res, err := tx.ExecContext(ctx, prefix+strings.TrimSuffix(strings.Repeat(row+", ", len(batch)), ", "), args...)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		inserted += int(n)
	}

	return inserted, nil
}

// sweepUnattributed forgets, page by page under l, the unattributed rows whose keys are not listed or have gained a claim object row;
// a page with nothing to forget writes nothing.
func (c *Catalog) sweepUnattributed(ctx context.Context, l Lease, listed map[string]bool) error {
	after := ""
	for {
		var page []struct {
			Key   string         `db:"object_key"`
			Claim sql.NullString `db:"claim_key"`
		}
		err := c.read.SelectContext(ctx, &page, `SELECT u.object_key, c.object_key AS claim_key FROM unattributed_objects u
			LEFT JOIN claim_objects c ON c.object_key = u.object_key
			WHERE u.object_key > ? ORDER BY u.object_key LIMIT `+strconv.Itoa(listingChunk), after)
		if err != nil {
			return err
		}
		var gone []string
		for _, r := range page {
			if !listed[r.Key] || r.Claim.Valid {
				gone = append(gone, r.Key)
			}
		}
		if len(gone) > 0 {
			err := c.inTx(ctx, func(tx *sqlx.Tx) error {
				if err := c.checkLease(ctx, tx, l); err != nil {
					return err
				}
				for batch := range slices.Chunk(gone, listingParams) {
					if _, err := tx.ExecContext(ctx, `DELETE FROM unattributed_objects WHERE object_key IN `+listingTuple(len(batch)), listingArgs(batch)...); err != nil {
						return err
					}
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
		if len(page) < listingChunk {
			return nil
		}
		after = page[len(page)-1].Key
	}
}

// listingTuple is a parenthesized list of n placeholders.
func listingTuple(n int) string { return "(?" + strings.Repeat(", ?", n-1) + ")" }

// listingArgs is keys as statement arguments.
func listingArgs(keys []string) []any {
	args := make([]any, len(keys))
	for i, k := range keys {
		args[i] = k
	}

	return args
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
