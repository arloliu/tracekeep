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
// A close request whose capture cannot be closed now is ended by the catalog under the capture row (closed: done; not open or stopped: failed);
// otherwise the capture is closed now by an intent with the operator's trigger, whose commit ends the request, and the close pass publishes it.
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
		var ended string
		var err error
		if q.Kind == catalog.RequestClose {
			ended, err = r.closeRequested(ctx, lease, q)
		} else {
			ended, err = r.mergeRequested(ctx, lease, q, &rep.Merges)
		}
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
		case ended == "merged" || ended == "settled" || ended == "closed":
			rep.Done++
		default:
			rep.Failed++
		}
		if ended != "" {
			r.Log.Info("compact: request ended", "request", q.ID, "kind", q.Kind, "capture", q.Capture, "outcome", ended)
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
	switch {
	case err != nil:
		return "", err
	case out == merged:
		return "merged", nil
	case out == refused: // the refusal ended it failed
		return overBudgetReason, nil
	default:
		return "", nil
	}
}

// closeRequested closes the capture q names by an intent with the operator's trigger and returns how q ended, empty when it stays pending.
func (r *Compactor) closeRequested(ctx context.Context, lease catalog.Lease, q catalog.Request) (string, error) {
	ended, err := r.Catalog.DecideRequest(ctx, lease, q)
	if err != nil || ended != catalog.Mergeable {
		return ended, err
	}
	out, err := r.intend(ctx, lease, q.Capture, catalog.CloseOperator, r.now().UnixNano(), &q.ID)
	if err != nil || out != catalog.Intended {
		return "", err
	}
	r.Metrics.Closes(map[string]int{catalog.CloseOperator: 1})

	return "closed", nil
}
