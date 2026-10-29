package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

var (
	// ErrConfigNotExecuted refuses a membership whose replica does not execute every configuration not yet retired (rollouts.md §6.4).
	ErrConfigNotExecuted = errors.New("catalog: the replica does not execute every active configuration")
	// ErrNotQuiescent refuses the retirement of a configuration a capture the caller did not find quiescent is bound to.
	ErrNotQuiescent = errors.New("catalog: a capture bound to the configuration is not quiescent")
	// ErrMembershipLapsed reports a membership whose lease lapsed or whose generation is not the current one: register again.
	ErrMembershipLapsed = errors.New("catalog: membership lapsed")
)

// Membership is a replica's registration in the activation gate.
type Membership struct {
	Replica string
	// Generation changes with every registration, so a delivery is checked against the membership it was pulled under.
	Generation int64
}

// ActivateConfig stores content as a configuration and activates it, under the membership sentinel, and returns its id:
// what tracekeep dev does for its own configuration, and tests for theirs. Equal content is one configuration;
// a staged one is activated and a retired one stays retired. Every other activation is config activate's (ActivateStaged).
func (c *Catalog) ActivateConfig(ctx context.Context, content []byte) (int64, error) {
	var id int64
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		var err error
		id, err = c.storeConfig(ctx, tx, content, true)

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: activate configuration: %w", err)
	}

	return id, nil
}

// StoredConfig is a processing configuration as the catalog holds it: its id and its content.
type StoredConfig struct {
	ID      int64  `db:"config_id"`
	Content []byte `db:"content"`
}

// RequiredConfigs lists, by id, the configurations a replica must execute:
// every active one, which registration requires, and every one a capture the catalog holds is bound to,
// since retiring a configuration never removes the ability to execute it (rollouts.md §6.4); a staged one is neither.
func (c *Catalog) RequiredConfigs(ctx context.Context) ([]StoredConfig, error) {
	var cs []StoredConfig
	err := c.read.SelectContext(ctx, &cs, `SELECT p.config_id, p.content FROM processing_configs p
		WHERE (p.activated_at IS NOT NULL AND p.retired_at IS NULL) OR EXISTS (SELECT 1 FROM captures k WHERE k.config_id = p.config_id)
		ORDER BY p.config_id`)
	if err != nil {
		return nil, fmt.Errorf("catalog: required configurations: %w", err)
	}

	return cs, nil
}

// RetireConfig retires configuration id: no new capture binds it, and a replica need not execute it to register.
// quiescent are the captures the caller found producer-quiescent (rollouts.md §6.4);
// under the membership sentinel, which every binding takes, the retirement is refused with ErrNotQuiescent
// unless every capture bound to id is among them, so a capture admitted after the caller looked keeps its configuration.
func (c *Catalog) RetireConfig(ctx context.Context, id int64, quiescent []tracepack.UUID) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		var bound [][]byte
		if err := tx.SelectContext(ctx, &bound, `SELECT capture_id FROM captures WHERE config_id = ?`, id); err != nil {
			return err
		}
		for _, b := range bound {
			if !slices.Contains(quiescent, tracepack.UUID(b)) {
				return fmt.Errorf("%w: capture %s", ErrNotQuiescent, tracepack.UUID(b))
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE processing_configs SET retired_at = ? WHERE config_id = ? AND retired_at IS NULL`, c.now(), id)

		return err
	})
	if err != nil {
		return fmt.Errorf("catalog: retire configuration %d: %w", id, err)
	}

	return nil
}

// Register makes replica a member executing configs, staged ones included, with a lease of ttl by the database's clock.
// It is refused with ErrConfigNotExecuted unless configs holds every active configuration; a staged one is never required.
func (c *Catalog) Register(ctx context.Context, replica string, configs []int64, ttl time.Duration) (Membership, error) {
	m := Membership{Replica: replica}
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		var unretired []int64
		if err := tx.SelectContext(ctx, &unretired, `SELECT config_id FROM processing_configs WHERE activated_at IS NOT NULL AND retired_at IS NULL`); err != nil {
			return err
		}
		for _, id := range unretired {
			if !slices.Contains(configs, id) {
				return fmt.Errorf("%w: configuration %d", ErrConfigNotExecuted, id)
			}
		}
		// The sentinel's value counts registrations, so a generation is never reused, after a leave included.
		if _, err := tx.ExecContext(ctx, `UPDATE counters SET value = value + 1 WHERE name = 'membership'`); err != nil {
			return err
		}
		if err := tx.GetContext(ctx, &m.Generation, `SELECT value FROM counters WHERE name = 'membership'`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM members WHERE replica_id = ?`, replica); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM member_configs WHERE replica_id = ?`, replica); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO members (replica_id, lease_until, generation, registered_at) VALUES (?, `+c.d.nowNS+` + ?, ?, ?)`,
			replica, ttl.Nanoseconds(), m.Generation, c.now())
		if err != nil {
			return err
		}
		for _, id := range configs {
			if _, err := tx.ExecContext(ctx, `INSERT INTO member_configs (replica_id, config_id) VALUES (?, ?)`, replica, id); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return Membership{}, fmt.Errorf("catalog: register %s: %w", replica, err)
	}

	return m, nil
}

// Renew extends m's lease by ttl from the database's clock; a lapsed lease or a replaced registration returns ErrMembershipLapsed.
func (c *Catalog) Renew(ctx context.Context, m Membership, ttl time.Duration) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE members SET lease_until = `+c.d.nowNS+` + ?
			WHERE replica_id = ? AND generation = ? AND lease_until >= `+c.d.nowNS, ttl.Nanoseconds(), m.Replica, m.Generation)

		return affected(res, err, ErrMembershipLapsed)
	})
	if err != nil {
		return fmt.Errorf("catalog: renew %s: %w", m.Replica, err)
	}

	return nil
}

// Leave ends replica's membership.
func (c *Catalog) Leave(ctx context.Context, replica string) error {
	return c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM members WHERE replica_id = ?`, replica); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM member_configs WHERE replica_id = ?`, replica)

		return err
	})
}

// lockMembership updates the membership sentinel row: the first statement of every membership change,
// and of every binding path of Admit after the capture row (catalog.md §7.2, T-32).
func (c *Catalog) lockMembership(ctx context.Context, tx *sqlx.Tx) error {
	_, err := tx.ExecContext(ctx, `UPDATE counters SET value = value WHERE name = 'membership'`)

	return err
}

// bindableConfig returns the newest activated, unretired configuration every leased member supports that bindable admits,
// any when bindable is nil; ok is false when there is none. The caller holds the membership sentinel.
func (c *Catalog) bindableConfig(ctx context.Context, tx *sqlx.Tx, bindable func(int64) bool) (int64, bool, error) {
	var ids []int64
	err := tx.SelectContext(ctx, &ids, `SELECT p.config_id FROM processing_configs p
		WHERE p.activated_at IS NOT NULL AND p.retired_at IS NULL
		AND NOT EXISTS (SELECT 1 FROM members m WHERE m.lease_until >= `+c.d.nowNS+`
			AND NOT EXISTS (SELECT 1 FROM member_configs mc WHERE mc.replica_id = m.replica_id AND mc.config_id = p.config_id))
		ORDER BY p.config_id DESC`)
	if err != nil {
		return 0, false, err
	}
	for _, id := range ids {
		if bindable == nil || bindable(id) {
			return id, true, nil
		}
	}

	return 0, false, nil
}

// affected turns an update that changed no row into errNone.
func affected(res sql.Result, err, errNone error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errNone
	}

	return nil
}
