package compact

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// ErrAllowDirect reports a stream that allows direct gets: a GetMsg could then be served by a replica
// that has not yet received a sequence, so the copy pass advances nothing (parking.md *resolution*).
var ErrAllowDirect = errors.New("compact: the stream allows direct gets")

// Copies marks acked the waiting copy rows of resolved versions whose message the work-queue stream s, playing role, no longer holds:
// an acknowledgement the server confirmed whose catalog update was lost (parking.md *resolution*).
// It first refreshes s, the very handle it reads with, and checks on that answer the incarnation and that AllowDirect is off;
// then only GetMsg's not-found for a sequence at or below that answer's LastSeq marks a row, under lease.
// A refresh, incarnation or AllowDirect failure is returned and advances nothing; a GetMsg error on one copy skips that copy.
// It returns how many rows it marked.
func (r *Compactor) Copies(ctx context.Context, lease catalog.Lease, role string, s jetstream.Stream) (int, error) {
	inc, err := bus.Incarnate(ctx, r.Catalog, role, s)
	if err != nil {
		return 0, err
	}
	// Incarnate's last Info refreshed the handle, so GetMsg takes the leader-served API route its cached AllowDirect names.
	info := s.CachedInfo()
	if info.Config.AllowDirect {
		return 0, fmt.Errorf("%w: %s", ErrAllowDirect, info.Config.Name)
	}
	copies, err := r.Catalog.ResolvedWaitingCopies(ctx, inc.ID)
	if err != nil {
		return 0, err
	}
	n := 0
	for i := range copies {
		d := &copies[i]
		seq := uint64(d.StreamSeq) //nolint:gosec // a stream sequence is positive
		if seq > info.State.LastSeq {
			continue // not yet accepted as far as this answer knows
		}
		_, err := s.GetMsg(ctx, seq)
		if err == nil {
			continue // still held: not acknowledged yet
		}
		if !errors.Is(err, jetstream.ErrMsgNotFound) {
			if ctx.Err() != nil {
				return n, ctx.Err()
			}
			r.Log.Warn("compact: copy not checked", "incarnation", inc.ID, "stream_seq", seq, "error", err)

			continue
		}
		ok, err := r.Catalog.ReconcileCopy(ctx, d, lease)
		if err != nil {
			return n, err
		}
		if ok {
			n++
		}
	}

	return n, nil
}
