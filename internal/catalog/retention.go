package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jmoiron/sqlx"
)

const nsPerHour = int64(time.Hour)

// Retention is the latest recorded retention period (compactor.md §8.3):
// the period, and after a shortening the longer period held in force until HeldUntil, in UTC nanoseconds.
// Every process computes an object's deadline from it alone, never from its own configuration.
type Retention struct {
	// Known is false before the compactor's first record, when no deadline exists.
	Known     bool
	Period    time.Duration
	Held      time.Duration
	HeldUntil int64
}

// InForce returns the period in force at t.
func (r Retention) InForce(t int64) time.Duration {
	if t < r.HeldUntil {
		return max(r.Period, r.Held)
	}

	return r.Period
}

// Deadline returns the earliest time hour may be evicted:
// the first t at which the hour's end plus the period in force at t has passed.
// It saturates at the last representable time, which an hour at the end of int64 nanoseconds reaches.
func (r Retention) Deadline(hour int64) int64 {
	end := int64(math.MaxInt64)
	if hour < math.MaxInt64/nsPerHour {
		end = (hour + 1) * nsPerHour
	}
	if held := satAdd(end, int64(max(r.Period, r.Held))); held < r.HeldUntil {
		return held
	}

	return max(r.HeldUntil, satAdd(end, int64(r.Period)))
}

// satAdd is a + b for b ≥ 0, saturated at the last representable time.
func satAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}

	return a + b
}

// HorizonHour returns the oldest hour retention keeps at now: every older hour is past its deadline.
func (r Retention) HorizonHour(now int64) int64 {
	return floorHour(now - int64(r.InForce(now)))
}

// floorHour is the hour, in hours since the Unix epoch, that holds the UTC time ns.
func floorHour(ns int64) int64 {
	q := ns / nsPerHour
	if ns%nsPerHour < 0 {
		q--
	}

	return q
}

type retentionRow struct {
	RecordedAt int64 `db:"recorded_at"`
	Period     int64 `db:"period_ns"`
	Held       int64 `db:"held_ns"`
	HeldUntil  int64 `db:"held_until"`
}

func (w retentionRow) retention() Retention {
	return Retention{Known: true, Period: time.Duration(w.Period), Held: time.Duration(w.Held), HeldUntil: w.HeldUntil}
}

func readRetention(ctx context.Context, q sqlx.QueryerContext) (retentionRow, bool, error) {
	var w retentionRow
	err := sqlx.GetContext(ctx, q, &w, `SELECT recorded_at, period_ns, held_ns, held_until FROM retention_periods ORDER BY recorded_at DESC LIMIT 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return w, false, nil
	}

	return w, err == nil, err
}

// Retention returns the latest recorded retention period, Known false before the first record.
func (c *Catalog) Retention(ctx context.Context) (Retention, error) {
	w, ok, err := readRetention(ctx, c.read)
	if err != nil || !ok {
		return Retention{}, err
	}

	return w.retention(), nil
}

// RecordRetention records period, the compactor's configured retention, under l at now, and returns the row in force.
// A changed period inserts a row, so the record locks no row but the lease's:
// a longer one keeps the hold as it was, a shorter one holds the period in force now for hold (D),
// which exceeds every download URL's lifetime.
func (c *Catalog) RecordRetention(ctx context.Context, l Lease, period, hold time.Duration, now int64) (Retention, error) {
	var out Retention
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		w, ok, err := readRetention(ctx, tx)
		if err != nil {
			return err
		}
		if ok && time.Duration(w.Period) == period {
			out = w.retention()

			return nil
		}
		next := retentionRow{RecordedAt: max(now, w.RecordedAt+1), Period: int64(period), Held: w.Held, HeldUntil: w.HeldUntil}
		if ok && period < time.Duration(w.Period) {
			next.Held, next.HeldUntil = int64(w.retention().InForce(now)), now+int64(hold)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO retention_periods (recorded_at, period_ns, held_ns, held_until) VALUES (?, ?, ?, ?)`,
			next.RecordedAt, next.Period, next.Held, next.HeldUntil)
		out = next.retention()

		return err
	})
	if err != nil {
		return Retention{}, fmt.Errorf("catalog: record retention: %w", err)
	}

	return out, nil
}
