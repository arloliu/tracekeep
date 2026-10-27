package compact

import (
	"context"
	"errors"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// RequestReport is what a request pass did.
type RequestReport struct {
	// Done and Failed count the requests ended each way.
	Done, Failed int
	// Pending counts the requests left for a later cycle: a stale or failed attempt, or a kind this compactor does not execute yet.
	Pending int
	// Merges are the merges the requests attempted.
	Merges MergeReport
}

// Requests executes the operators' pending requests under lease (T-41).
// A merge request merges its scope now, its quiet period aside, if the scope can be merged, and its merge's commit ends it done;
// a scope already settled ends it done, one that cannot be merged (a conflict, no staged pack, conflicting records) ends it failed with the reason.
// A stale merge or an error leaves the request for the next cycle; a lost lease ends the pass with ErrLeaseLost.
func (r *Compactor) Requests(ctx context.Context, lease catalog.Lease) (RequestReport, error) {
	var rep RequestReport
	reqs, err := r.Catalog.PendingRequests(ctx)
	if err != nil {
		return rep, err
	}
	for _, q := range reqs {
		if q.Kind != catalog.RequestMerge || q.Hour == nil {
			rep.Pending++ // a close is executed by the compactor that writes closes

			continue
		}
		end, err := r.mergeRequested(ctx, lease, q, &rep.Merges)
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
		if errors.Is(err, catalog.ErrLeaseLost) {
			return rep, err
		}
		switch {
		case err != nil:
			r.Log.Warn("compact: merge request", "request", q.ID, "error", err)
			rep.Pending++
		case end == "merged":
			rep.Done++ // ended by the merge's commit
		case end == "":
			rep.Pending++
		default:
			failed := end != "settled"
			if err := r.Catalog.FinishRequest(ctx, lease, q, failed, end); err != nil {
				return rep, err
			}
			if failed {
				rep.Failed++
			} else {
				rep.Done++
			}
		}
		if end != "" {
			r.Log.Info("compact: merge request ended", "request", q.ID, "capture", q.Capture, "hour", *q.Hour, "outcome", end)
		}
	}

	return rep, nil
}

// mergeRequested merges the scope q names and returns how q ends: merged (its merge's commit ended it), settled, a failure's reason,
// or empty to leave it pending; the merge it attempts is added to merges.
func (r *Compactor) mergeRequested(ctx context.Context, lease catalog.Lease, q catalog.Request, merges *MergeReport) (string, error) {
	s := catalog.Scope{Capture: q.Capture, Hour: *q.Hour}
	snap, err := r.Catalog.MergeSnapshot(ctx, s)
	switch {
	case err != nil:
		return "", err
	case snap.State == "settled":
		return "settled", nil
	case len(snap.Inputs) == 0:
		return "the scope has no staged pack", nil
	case !snap.Eligible:
		return "a conflict touches the scope", nil
	}
	out, err := r.attempt(ctx, lease, s, &q.ID, merges)
	switch {
	case err != nil:
		return "", err
	case out == merged:
		return "merged", nil
	case out == conflicted:
		return "the inputs hold conflicting records", nil
	default:
		return "", nil // stale, failed, or no longer eligible: judged again next cycle
	}
}
