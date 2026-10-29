package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

var (
	// ErrPolicyConflict refuses a policy whose id and version are stored with other content: a policy version never changes.
	ErrPolicyConflict = errors.New("catalog: a policy of this id and version is stored with other content")
	// ErrUnknownConfig reports a configuration id the catalog does not hold.
	ErrUnknownConfig = errors.New("catalog: no such configuration")
	// ErrConfigRetired refuses to activate a retired configuration.
	ErrConfigRetired = errors.New("catalog: the configuration is retired")
)

// Configuration states (rollouts.md §6.4).
const (
	ConfigStaged  = "staged"
	ConfigActive  = "active"
	ConfigRetired = "retired"
)

// StoredPolicy is a redaction policy as the catalog holds it: its canonical bytes under its id and version (masking-keys.md §9).
type StoredPolicy struct {
	ID        string `db:"policy_id"`
	Version   int64  `db:"version"`
	Hash      []byte `db:"content_sha256"`
	Body      []byte `db:"body"`
	CreatedAt int64  `db:"created_at"`
}

// Staging is what StageConfig stores: the configuration's content, the policy it names if any, and the audit row of an operator's staging.
type Staging struct {
	Content []byte
	// Policy, when set, is stored too, insert-or-compare on its id and version.
	Policy *StoredPolicy
	// Audit, when set, is written in the same transaction.
	Audit *AuditEntry
}

// StageConfig stores s's configuration as staged, and its policy, in one transaction, and returns the configuration's id:
// equal content is one configuration, left in whatever state it is; a policy whose id and version are stored with another hash
// is refused with ErrPolicyConflict. Staging activates nothing (rollouts.md §6.4).
func (c *Catalog) StageConfig(ctx context.Context, s Staging) (int64, error) {
	var id int64
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		// The sentinel serializes the allocation of configuration ids with every activation and binding.
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		if s.Policy != nil {
			if err := c.storePolicy(ctx, tx, s.Policy); err != nil {
				return err
			}
		}
		var err error
		if id, err = c.storeConfig(ctx, tx, s.Content, false); err != nil {
			return err
		}
		if s.Audit != nil {
			return c.auditIn(ctx, tx, *s.Audit)
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: stage configuration: %w", err)
	}

	return id, nil
}

// storePolicy inserts p, or compares it with the stored one of its id and version.
func (c *Catalog) storePolicy(ctx context.Context, tx *sqlx.Tx, p *StoredPolicy) error {
	sum := sha256.Sum256(p.Body)
	if p.Hash != nil && !bytes.Equal(p.Hash, sum[:]) {
		return fmt.Errorf("catalog: policy %s version %d: its hash is not its body's", p.ID, p.Version)
	}
	_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO policies (policy_id, version, content_sha256, body, created_at) VALUES (?, ?, ?, ?, ?)`,
		p.ID, p.Version, sum[:], p.Body, c.now())
	if err != nil {
		return err
	}
	var stored []byte
	if err := tx.GetContext(ctx, &stored, `SELECT content_sha256 FROM policies WHERE policy_id = ? AND version = ?`, p.ID, p.Version); err != nil {
		return err
	}
	if !bytes.Equal(stored, sum[:]) {
		return fmt.Errorf("%w: %s version %d", ErrPolicyConflict, p.ID, p.Version)
	}

	return nil
}

// storeConfig inserts content as a configuration, activated when activate, or finds the one of equal content and returns its id;
// with activate, a staged one found is activated, and a retired one stays retired.
// The caller holds the membership sentinel.
func (c *Catalog) storeConfig(ctx context.Context, tx *sqlx.Tx, content []byte, activate bool) (int64, error) {
	sum := sha256.Sum256(content)
	var id int64
	err := tx.GetContext(ctx, &id, `SELECT config_id FROM processing_configs WHERE content_sha256 = ?`, sum[:])
	switch {
	case err == nil:
		if activate {
			_, err = tx.ExecContext(ctx, `UPDATE processing_configs SET activated_at = ? WHERE config_id = ? AND activated_at IS NULL AND retired_at IS NULL`,
				c.now(), id)
		}

		return id, err
	case !errors.Is(err, sql.ErrNoRows):
		return 0, err
	}
	if err := tx.GetContext(ctx, &id, `SELECT COALESCE(MAX(config_id), 0) + 1 FROM processing_configs`); err != nil {
		return 0, err
	}
	now := c.now()
	var activated sql.NullInt64
	if activate {
		activated = sql.NullInt64{Int64: now, Valid: true}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO processing_configs (config_id, content, content_sha256, created_at, activated_at) VALUES (?, ?, ?, ?, ?)`,
		id, content, sum[:], now, activated)

	return id, err
}

// ActivateStaged activates configuration id under the membership sentinel, as config activate does (rollouts.md §6.4),
// and reports whether it changed anything: an active one is left as it is.
// An unknown id is refused with ErrUnknownConfig and a retired one with ErrConfigRetired;
// check, when set, judges the configuration's content inside the transaction, and its error refuses the activation.
// audit, when set, is written in the same transaction.
func (c *Catalog) ActivateStaged(ctx context.Context, id int64, check func(content []byte) error, audit *AuditEntry) (bool, error) {
	var changed bool
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		var row struct {
			Content   []byte        `db:"content"`
			Activated sql.NullInt64 `db:"activated_at"`
			Retired   sql.NullInt64 `db:"retired_at"`
		}
		err := tx.GetContext(ctx, &row, `SELECT content, activated_at, retired_at FROM processing_configs WHERE config_id = ?`, id)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%w: %d", ErrUnknownConfig, id)
		case err != nil:
			return err
		case check != nil:
			if err := check(row.Content); err != nil {
				return err
			}
		default: // no check to run
		}
		switch {
		case row.Retired.Valid:
			return fmt.Errorf("%w: %d", ErrConfigRetired, id)
		case !row.Activated.Valid:
			if _, err := tx.ExecContext(ctx, `UPDATE processing_configs SET activated_at = ? WHERE config_id = ?`, c.now(), id); err != nil {
				return err
			}
			changed = true
		default: // active: unchanged
		}
		if audit != nil {
			return c.auditIn(ctx, tx, *audit)
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("catalog: activate configuration %d: %w", id, err)
	}

	return changed, nil
}

// ConfigInfo is a configuration with its state, for config list and for a replica choosing what it loads.
type ConfigInfo struct {
	ID          int64         `db:"config_id"`
	Content     []byte        `db:"content"`
	CreatedAt   int64         `db:"created_at"`
	ActivatedAt sql.NullInt64 `db:"activated_at"`
	RetiredAt   sql.NullInt64 `db:"retired_at"`
	// Bound reports whether a capture the catalog holds is bound to it.
	Bound bool `db:"bound"`
}

// State is the configuration's state: retired, active or staged.
func (i *ConfigInfo) State() string {
	switch {
	case i.RetiredAt.Valid:
		return ConfigRetired
	case i.ActivatedAt.Valid:
		return ConfigActive
	default:
		return ConfigStaged
	}
}

// Configs lists every configuration by id with its state and whether a capture is bound to it.
func (c *Catalog) Configs(ctx context.Context) ([]ConfigInfo, error) {
	var cs []ConfigInfo
	err := c.read.SelectContext(ctx, &cs, `SELECT p.config_id, p.content, p.created_at, p.activated_at, p.retired_at,
		EXISTS (SELECT 1 FROM captures k WHERE k.config_id = p.config_id) AS bound
		FROM processing_configs p ORDER BY p.config_id`)
	if err != nil {
		return nil, fmt.Errorf("catalog: configurations: %w", err)
	}

	return cs, nil
}

// Policy returns the stored policy of id and version; ok is false when there is none.
func (c *Catalog) Policy(ctx context.Context, id string, version int64) (StoredPolicy, bool, error) {
	var p StoredPolicy
	err := c.read.GetContext(ctx, &p, `SELECT policy_id, version, content_sha256, body, created_at FROM policies WHERE policy_id = ? AND version = ?`, id, version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return StoredPolicy{}, false, nil
	case err != nil:
		return StoredPolicy{}, false, fmt.Errorf("catalog: policy %s version %d: %w", id, version, err)
	}

	return p, true, nil
}

// Policies lists every stored policy by id and version.
func (c *Catalog) Policies(ctx context.Context) ([]StoredPolicy, error) {
	var ps []StoredPolicy
	if err := c.read.SelectContext(ctx, &ps, `SELECT policy_id, version, content_sha256, body, created_at FROM policies ORDER BY policy_id, version`); err != nil {
		return nil, fmt.Errorf("catalog: policies: %w", err)
	}

	return ps, nil
}
