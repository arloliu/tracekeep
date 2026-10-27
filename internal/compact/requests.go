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
// A merge request whose scope cannot be merged now is ended by the catalog under the scope's capture row:
// done when the scope is settled, failed with the reason when it has no staged pack or a conflict touches it.
// Otherwise its scope is merged now, its quiet period aside, and the merge's commit ends it done;
// a stale or failed merge leaves it for the next cycle, and a merge that fails on conflicting records leaves it to be ended
// by the conflict that merge recorded. A lost lease ends the pass with ErrLeaseLost.
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
		ended, err := r.mergeRequested(ctx, lease, q, &rep.Merges)
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
		case ended == "":
			rep.Pending++
		case ended == "merged" || ended == "settled":
			rep.Done++
		default:
			rep.Failed++
		}
		if ended != "" {
			r.Log.Info("compact: merge request ended", "request", q.ID, "capture", q.Capture, "hour", *q.Hour, "outcome", ended)
		}
	}

	return rep, nil
}

// mergeRequested executes q and returns how it ended, empty when it stays pending; the merge it attempts is added to merges.
func (r *Compactor) mergeRequested(ctx context.Context, lease catalog.Lease, q catalog.Request, merges *MergeReport) (string, error) {
	ended, err := r.Catalog.DecideRequest(ctx, lease, q)
	if err != nil || ended != catalog.Mergeable {
		return ended, err
	}
	out, err := r.attempt(ctx, lease, catalog.Scope{Capture: q.Capture, Hour: *q.Hour}, &q.ID, merges)
	if err != nil || out != merged {
		return "", err
	}

	return "merged", nil
}
