package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// Key binding kinds, a key id's one purpose (masking-keys.md §9).
const (
	KindDigest   = "digest"
	KindMaster   = "master"
	KindSession  = "session"
	KindDownload = "download"
)

// KeyBinding is a key id's binding (masking-keys.md §9): its kind, the provider reference and version it was first resolved at,
// and the fingerprint of the material found there; it is never updated.
type KeyBinding struct {
	KeyID           string `db:"key_id"`
	Kind            string `db:"kind"`
	ProviderRef     string `db:"provider_ref"`
	ProviderVersion string `db:"provider_version"`
	Fingerprint     string `db:"fingerprint"`
	BoundAt         int64  `db:"bound_at"`
}

// ErrKeyBindingMismatch marks a key id whose binding holds another kind or another fingerprint: the caller fails closed.
var ErrKeyBindingMismatch = errors.New("catalog: key binding mismatch")

// KeyBinding returns keyID's binding, false when it has none.
func (c *Catalog) KeyBinding(ctx context.Context, keyID string) (KeyBinding, bool, error) {
	var b KeyBinding
	err := c.read.GetContext(ctx, &b, `SELECT key_id, kind, provider_ref, provider_version, fingerprint, bound_at FROM key_bindings WHERE key_id = ?`, keyID)
	if errors.Is(err, sql.ErrNoRows) {
		return KeyBinding{}, false, nil
	}
	if err != nil {
		return KeyBinding{}, false, fmt.Errorf("catalog: key binding: %w", err)
	}

	return b, true, nil
}

// BindKey runs the insert-or-compare of b (masking-keys.md §9):
// b is bound when its key id has no binding, and otherwise compared with the stored one, which it returns either way;
// a stored binding of another kind or fingerprint returns ErrKeyBindingMismatch.
// It takes no lease and no capture row; two first bindings of one key id serialize on the key,
// so they agree or one of them fails.
func (c *Catalog) BindKey(ctx context.Context, b KeyBinding) (KeyBinding, error) {
	var stored KeyBinding
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO key_bindings (key_id, kind, provider_ref, provider_version, fingerprint, bound_at)
			VALUES (?, ?, ?, ?, ?, ?)`, b.KeyID, b.Kind, b.ProviderRef, b.ProviderVersion, b.Fingerprint, c.now())
		if err != nil {
			return err
		}

		return tx.GetContext(ctx, &stored, `SELECT key_id, kind, provider_ref, provider_version, fingerprint, bound_at FROM key_bindings WHERE key_id = ?`, b.KeyID)
	})
	if err != nil {
		return KeyBinding{}, fmt.Errorf("catalog: bind key %s: %w", b.KeyID, err)
	}
	if stored.Kind != b.Kind || stored.Fingerprint != b.Fingerprint {
		return stored, fmt.Errorf("%w: key %s is bound as %s with another fingerprint or kind", ErrKeyBindingMismatch, b.KeyID, stored.Kind)
	}

	return stored, nil
}
