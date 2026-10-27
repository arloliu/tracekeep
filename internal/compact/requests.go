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
}

// Requests executes the operators' pending requests under lease (T-41).
// A merge request merges its scope now, its quiet period aside, if the scope can be merged; it ends done when the scope is settled,
// and failed with the reason when it cannot be (a conflict, no staged pack, conflicting records).
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
		failed, outcome, err := r.mergeRequested(ctx, lease, catalog.Scope{Capture: q.Capture, Hour: *q.Hour})
		if errors.Is(err, catalog.ErrLeaseLost) || ctx.Err() != nil {
			return rep, err
		}
		if err != nil || outcome == "" {
			if err != nil {
				r.Log.Warn("compact: merge request", "request", q.ID, "error", err)
			}
			rep.Pending++

			continue
		}
		if err := r.Catalog.FinishRequest(ctx, lease, q.ID, failed, outcome); err != nil {
			return rep, err
		}
		if failed {
			rep.Failed++
		} else {
			rep.Done++
		}
		r.Log.Info("compact: merge request ended", "request", q.ID, "capture", q.Capture, "hour", *q.Hour, "failed", failed, "outcome", outcome)
	}

	return rep, nil
}

// mergeRequested merges s for a request and returns how the request ends; an empty outcome leaves it pending.
func (r *Compactor) mergeRequested(ctx context.Context, lease catalog.Lease, s catalog.Scope) (bool, string, error) {
	snap, err := r.Catalog.MergeSnapshot(ctx, s)
	switch {
	case err != nil:
		return false, "", err
	case snap.State == "settled":
		return false, "settled", nil
	case len(snap.Inputs) == 0:
		return true, "the scope has no staged pack", nil
	case !snap.Eligible:
		return true, "a conflict touches the scope", nil
	}
	out, err := r.merge(ctx, lease, s)
	switch {
	case err != nil:
		return false, "", err
	case out == merged:
		return false, "merged", nil
	case out == conflicted:
		return true, "the inputs hold conflicting records", nil
	default:
		return false, "", nil // stale, or no longer eligible: judged again next cycle
	}
}
