package catalog

import (
	"context"
	"fmt"

	"github.com/arloliu/go-secs/tracepack"
)

// Copy is a pack holding a copy of a record.
type Copy struct {
	Pack  tracepack.UUID
	Key   string
	Hour  int64
	State string
}

// RecordCopies lists, in one read, the packs of capture whose range holds seq, in any hour and in any state but excluded and deleted,
// a merged input beside its archive, in hour and then pack order (masking-keys.md §9 *Restore*).
// A pack registered after the read is not listed; a pack merged after it stays readable for D.
func (c *Catalog) RecordCopies(ctx context.Context, capture tracepack.UUID, seq uint64) ([]Copy, error) {
	var rows []struct {
		Pack  []byte `db:"pack_id"`
		Key   string `db:"object_key"`
		Hour  int64  `db:"hour"`
		State string `db:"state"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT DISTINCT k.pack_id, k.object_key, k.hour, k.state
		FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
		WHERE r.capture_id = ? AND r.seq_from <= ? AND r.seq_to >= ? AND k.state NOT IN ('excluded', 'deleted')
		ORDER BY k.hour, k.pack_id`, capture[:], int64(seq), int64(seq)) //nolint:gosec // stored seqs are below 2^63
	if err != nil {
		return nil, fmt.Errorf("catalog: copies of %s seq %d: %w", capture, seq, err)
	}
	out := make([]Copy, len(rows))
	for i, r := range rows {
		out[i] = Copy{Pack: tracepack.UUID(r.Pack), Key: r.Key, Hour: r.Hour, State: r.State}
	}

	return out, nil
}
