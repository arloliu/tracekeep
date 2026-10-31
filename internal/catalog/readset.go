package catalog

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// The states of a pack a human read reads.
const (
	PackStaged   = "staged"
	PackExcluded = "excluded"
)

// ReadSet is what a human read reads of one capture's hour (catalog.md §7.3 *Read set*).
type ReadSet struct {
	Capture tracepack.UUID
	Hour    int64
	// ConfigID is the capture's configuration, whose policy decides whether the capture is withheld; nil for none.
	ConfigID *int64
	// Packs are the hour's staged packs, then its excluded ones, each by pack id; never a merged input or a deleted pack.
	Packs []ReadPack
	// Scoped tells whether the hour has a scope; Evicted, whether retention evicted ranges of it.
	Scoped, Evicted bool
}

// ReadPack is one pack of a read set.
type ReadPack struct {
	ID          tracepack.UUID
	Key         string
	Size        int64
	Role        string
	State       string
	FormatMinor int
	Ranges      []Range
}

// ReadSet reads capture's read set of hour from one snapshot.
func (c *Catalog) ReadSet(ctx context.Context, capture tracepack.UUID, hour int64) (ReadSet, error) {
	out := ReadSet{Capture: capture, Hour: hour}
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		if err := tx.GetContext(ctx, &out.ConfigID, `SELECT config_id FROM captures WHERE capture_id = ?`, capture[:]); err != nil {
			return err
		}
		var n int
		if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM scopes WHERE capture_id = ? AND hour = ?`, capture[:], hour); err != nil {
			return err
		}
		out.Scoped = n > 0
		if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM evicted_ranges WHERE capture_id = ? AND hour = ?`, capture[:], hour); err != nil {
			return err
		}
		out.Evicted = n > 0
		var packs []struct {
			ID          []byte `db:"pack_id"`
			Key         string `db:"object_key"`
			Size        int64  `db:"size"`
			Role        string `db:"role"`
			State       string `db:"state"`
			FormatMinor int    `db:"format_minor"`
		}
		// Staged before excluded: 'e' < 's', so descending.
		err := tx.SelectContext(ctx, &packs, `SELECT pack_id, object_key, size, role, state, format_minor FROM packs
			WHERE capture_id = ? AND hour = ? AND state IN ('staged', 'excluded') ORDER BY state DESC, pack_id`, capture[:], hour)
		if err != nil {
			return err
		}
		at := make(map[tracepack.UUID]int, len(packs))
		for _, p := range packs {
			at[tracepack.UUID(p.ID)] = len(out.Packs)
			out.Packs = append(out.Packs, ReadPack{ID: tracepack.UUID(p.ID), Key: p.Key, Size: p.Size, Role: p.Role, State: p.State, FormatMinor: p.FormatMinor})
		}
		var ranges []struct {
			Pack   []byte `db:"pack_id"`
			From   int64  `db:"seq_from"`
			To     int64  `db:"seq_to"`
			TSFrom int64  `db:"ts_from"`
			TSTo   int64  `db:"ts_to"`
		}
		err = tx.SelectContext(ctx, &ranges, `SELECT r.pack_id, r.seq_from, r.seq_to, r.ts_from, r.ts_to FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
			WHERE k.capture_id = ? AND k.hour = ? AND k.state IN ('staged', 'excluded') ORDER BY r.pack_id, r.seq_from`, capture[:], hour)
		if err != nil {
			return err
		}
		for _, r := range ranges {
			p := &out.Packs[at[tracepack.UUID(r.Pack)]]
			p.Ranges = append(p.Ranges, Range{SeqFrom: uint64(r.From), SeqTo: uint64(r.To), TSFrom: r.TSFrom, TSTo: r.TSTo}) //nolint:gosec // stored seqs are below 2^63
		}

		return nil
	})
	if err != nil {
		return ReadSet{}, fmt.Errorf("catalog: read set of %s hour %d: %w", capture, hour, err)
	}

	return out, nil
}
