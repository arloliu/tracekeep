package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// ErrLeaseLost reports a lease its holder no longer holds: a worker that lost its lease does nothing further (compactor.md §8.1).
var ErrLeaseLost = errors.New("catalog: lease lost")

// Lease is a held lease; its fence token grows with every take, so a stale holder's writes are refused.
type Lease struct {
	Name   string
	Holder string
	Fence  int64
}

// TakeLease takes the lease name for holder when it is free or lapsed, for ttl by the database's clock;
// ok is false while another holder's lease is live.
func (c *Catalog) TakeLease(ctx context.Context, name, holder string, ttl time.Duration) (Lease, bool, error) {
	l := Lease{Name: name, Holder: holder}
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO leases (name, holder, fence_token, until_ns) VALUES (?, '', 0, 0)`, name)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE leases SET holder = ?, fence_token = fence_token + 1, until_ns = `+c.d.nowNS+` + ?
			WHERE name = ? AND until_ns < `+c.d.nowNS, holder, ttl.Nanoseconds(), name)
		if err := affected(res, err, ErrLeaseLost); err != nil {
			return err
		}

		return tx.GetContext(ctx, &l.Fence, `SELECT fence_token FROM leases WHERE name = ?`, name)
	})
	if errors.Is(err, ErrLeaseLost) {
		return Lease{}, false, nil
	}
	if err != nil {
		return Lease{}, false, fmt.Errorf("catalog: take lease %s: %w", name, err)
	}

	return l, true, nil
}

// RenewLease extends l for ttl from the database's clock; a lease that lapsed or passed to another holder returns ErrLeaseLost.
func (c *Catalog) RenewLease(ctx context.Context, l Lease, ttl time.Duration) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE leases SET until_ns = `+c.d.nowNS+` + ?
			WHERE name = ? AND holder = ? AND fence_token = ? AND until_ns >= `+c.d.nowNS, ttl.Nanoseconds(), l.Name, l.Holder, l.Fence)

		return affected(res, err, ErrLeaseLost)
	})
	if err != nil {
		return fmt.Errorf("catalog: renew lease %s: %w", l.Name, err)
	}

	return nil
}

// checkLease is the lease check of every compactor transaction that changes domain state, after the prologue (T-32):
// it updates the lease row, so the transaction holds it, and returns ErrLeaseLost when l is no longer held.
func (c *Catalog) checkLease(ctx context.Context, tx *sqlx.Tx, l Lease) error {
	res, err := tx.ExecContext(ctx, `UPDATE leases SET until_ns = until_ns WHERE name = ? AND holder = ? AND fence_token = ? AND until_ns >= `+c.d.nowNS,
		l.Name, l.Holder, l.Fence)

	return affected(res, err, ErrLeaseLost)
}
