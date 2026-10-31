package compact

import (
	"cmp"
	"context"
	"errors"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// clockSkew is the clock skew the export cleanup waits out twice past a put deadline or a bundle's deadline
// (api.md §10.1 *Exports*, compactor.md §8.3).
const clockSkew = 2 * time.Minute

// exportBatch bounds the export objects one cycle deletes.
const exportBatch = 1000

// ExportReport is what an export cleanup did.
type ExportReport struct {
	// Deleted counts the objects deleted with their rows, Forgotten the jobs deleted, Failed the steps left for the next cycle.
	Deleted, Forgotten, Failed int
}

// ExportCleanup deletes, under lease, the export objects no put can still complete and the bundles past their deadline,
// each made deleting first, then its object deleted, then its row; then the jobs no row of which is left (compactor.md §8.3).
// A failed step is logged by job and fence, never by key, counted, and left for the next cycle.
func (r *Compactor) ExportCleanup(ctx context.Context, lease catalog.Lease, rep *ExportReport) error {
	objs, err := r.Catalog.DoomExportObjects(ctx, lease, 2*clockSkew, exportBatch)
	if err != nil {
		return err
	}
	for _, o := range objs {
		if err := r.Catalog.VerifyLease(ctx, lease); err != nil {
			return err
		}
		if err := r.Store.Delete(ctx, o.Key); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.Log.Error("compact: delete an export object", "job", o.Job.String(), "fence", o.Fence) // the store's error names the key
			rep.Failed++

			continue
		}
		if err := r.Catalog.ForgetExportObject(ctx, lease, o.Key); err != nil {
			if errors.Is(err, catalog.ErrLeaseLost) || ctx.Err() != nil {
				return err
			}
			r.Log.Error("compact: forget an export object", "job", o.Job.String(), "fence", o.Fence, "error", err)
			rep.Failed++

			continue
		}
		rep.Deleted++
	}
	n, err := r.Catalog.ForgetExports(ctx, lease, cmp.Or(r.ExportRetention, 24*time.Hour))
	rep.Forgotten = int(n)

	return err
}

// exportPass runs the export cleanup and records its metrics: the task succeeds when it ended in no error and no step failed.
func (r *Compactor) exportPass(ctx context.Context, lease catalog.Lease) error {
	started := time.Now()
	var rep ExportReport
	err := r.ExportCleanup(ctx, lease, &rep)
	r.Metrics.Cycle("exports", time.Since(started), err == nil && rep.Failed == 0)
	r.Metrics.ExportCleanup(rep.Deleted, rep.Failed)
	if err != nil || r.Metrics == nil {
		return err
	}
	if byState, err := r.Catalog.ExportJobCounts(ctx); err == nil {
		r.Metrics.ExportJobs(byState)
	} else {
		r.Log.Warn("compact: read the export job gauges", "error", err)
	}

	return nil
}
