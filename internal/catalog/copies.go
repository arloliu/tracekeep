package catalog

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// Copy is a pack holding a copy of a record.
type Copy struct {
	Pack  tracepack.UUID
	Key   string
	Hour  int64
	State string
}

// RecordCopies lists, from one snapshot, the packs of capture whose range holds seq, in any hour and in any state but excluded and deleted,
// a merged input beside its archive, in hour and then pack order, at most limit of them (masking-keys.md §9 *Restore*).
// It first tells whether hour holds such a pack, inHour, and lists nothing when it does not.
// A pack registered after the snapshot is not listed; a pack merged after it stays readable for D.
func (c *Catalog) RecordCopies(ctx context.Context, capture tracepack.UUID, seq uint64, hour int64, limit int) (copies []Copy, inHour bool, err error) {
	s := int64(seq) //nolint:gosec // stored seqs are below 2^63
	err = c.snapshot(ctx, func(tx *sqlx.Tx) error {
		var n int
		err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM (SELECT 1 FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
			WHERE r.capture_id = ? AND r.seq_from <= ? AND r.seq_to >= ? AND k.hour = ? AND k.state NOT IN ('excluded', 'deleted') LIMIT 1) AS named`,
			capture[:], s, s, hour)
		if inHour = n > 0; err != nil || !inHour {
			return err
		}
		var rows []struct {
			Pack  []byte `db:"pack_id"`
			Key   string `db:"object_key"`
			Hour  int64  `db:"hour"`
			State string `db:"state"`
		}
		err = tx.SelectContext(ctx, &rows, `SELECT DISTINCT k.pack_id, k.object_key, k.hour, k.state
			FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
			WHERE r.capture_id = ? AND r.seq_from <= ? AND r.seq_to >= ? AND k.state NOT IN ('excluded', 'deleted')
			ORDER BY k.hour, k.pack_id LIMIT ?`, capture[:], s, s, limit)
		for _, r := range rows {
			copies = append(copies, Copy{Pack: tracepack.UUID(r.Pack), Key: r.Key, Hour: r.Hour, State: r.State})
		}

		return err
	})
	if err != nil {
		return nil, false, fmt.Errorf("catalog: copies of %s seq %d: %w", capture, seq, err)
	}

	return copies, inHour, nil
}
