package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// PruneEvents deletes the change cursor's seq prefix under the compactor lease l (catalog.md §7.3):
// the events below the lowest seq of an event younger than period at now, never by time alone,
// with counters.low_water raised to that seq, never lowered, in the same transaction, which allocates no seq.
// With no younger event it deletes nothing.
// It returns how many events it deleted.
func (c *Catalog) PruneEvents(ctx context.Context, l Lease, period time.Duration, now int64) (int, error) {
	var n int64
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		n = 0
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		var low sql.NullInt64
		if err := tx.GetContext(ctx, &low, `SELECT MIN(seq) FROM scope_events WHERE at > ?`, now-int64(period)); err != nil {
			return err
		}
		if !low.Valid {
			return nil
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM scope_events WHERE seq < ?`, low.Int64)
		if err != nil {
			return err
		}
		if n, err = res.RowsAffected(); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE counters SET low_water = ? WHERE name = 'scope_events' AND low_water < ?`, low.Int64, low.Int64)

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: prune events: %w", err)
	}

	return int(n), nil
}
