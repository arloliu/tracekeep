package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// The roles a bus stream plays.
const (
	// RoleTraffic is the traffic stream the producers publish to.
	RoleTraffic = "traffic"
	// RoleParking is the parking stream ingest moves indefinite deferrals to.
	RoleParking = "parking"
)

// ErrIncarnationChanged reports a stream that is not the incarnation the catalog records:
// another creation time, or a last sequence below one already observed (a restore of an older snapshot).
// The service stops fetching and publishing until an operator adopts the stream (parking.md *incarnations*).
var ErrIncarnationChanged = errors.New("catalog: bus-incarnation-changed")

// Incarnation is a stream as the catalog knows it (parking.md §7.1 `bus_streams`).
type Incarnation struct {
	ID     int64  `db:"incarnation_id"`
	Role   string `db:"role"`
	Stream string `db:"stream_name"`
	// CreatedNS is the stream's StreamInfo.Created, in nanoseconds.
	CreatedNS int64 `db:"created_ns"`
	// LastSeqSeen is the highest last sequence observed on it.
	LastSeqSeen int64 `db:"last_seq_seen"`
}

// Incarnate returns the live incarnation of role, recording stream as its first one when the catalog has none;
// a live incarnation of another stream or creation time returns ErrIncarnationChanged.
// A later incarnation is only ever recorded by adopting the stream, never here.
func (c *Catalog) Incarnate(ctx context.Context, role, stream string, createdNS int64) (Incarnation, error) {
	var inc Incarnation
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		err := tx.GetContext(ctx, &inc, `SELECT incarnation_id, role, stream_name, created_ns, last_seq_seen FROM bus_streams
			WHERE role = ? AND retired_at IS NULL`, role)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if err := tx.GetContext(ctx, &inc.ID, `SELECT COALESCE(MAX(incarnation_id), 0) + 1 FROM bus_streams`); err != nil {
				return err
			}
			inc.Role, inc.Stream, inc.CreatedNS = role, stream, createdNS
			_, err := tx.ExecContext(ctx, `INSERT INTO bus_streams (incarnation_id, role, stream_name, created_ns, last_seq_seen) VALUES (?, ?, ?, ?, 0)`,
				inc.ID, role, stream, createdNS)

			return err
		case err != nil:
			return err
		case inc.Stream != stream || inc.CreatedNS != createdNS:
			return fmt.Errorf("%w: %s %s created at %d, the catalog has %s created at %d", ErrIncarnationChanged, role, stream, createdNS, inc.Stream, inc.CreatedNS)
		}

		return nil
	})
	if err != nil {
		return Incarnation{}, fmt.Errorf("catalog: incarnation of %s: %w", role, err)
	}

	return inc, nil
}

// ObserveLastSeq records lastSeq as observed on incarnation id;
// a last sequence below the highest observed, which only a restore of an older snapshot causes, returns ErrIncarnationChanged.
func (c *Catalog) ObserveLastSeq(ctx context.Context, id, lastSeq int64) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		var seen int64
		if err := tx.GetContext(ctx, &seen, `SELECT last_seq_seen FROM bus_streams WHERE incarnation_id = ?`, id); err != nil {
			return err
		}
		if lastSeq < seen {
			return fmt.Errorf("%w: last sequence %d below the %d observed", ErrIncarnationChanged, lastSeq, seen)
		}
		_, err := tx.ExecContext(ctx, `UPDATE bus_streams SET last_seq_seen = ? WHERE incarnation_id = ? AND last_seq_seen < ?`, lastSeq, id, lastSeq)

		return err
	})
	if err != nil {
		return fmt.Errorf("catalog: incarnation %d: %w", id, err)
	}

	return nil
}
