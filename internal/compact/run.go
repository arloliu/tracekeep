package compact

import (
	"context"
	"errors"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// leaseName is the compactor lease the migrations create.
const leaseName = "compactor"

// Run runs a cycle every Interval until ctx is done or a stream is no longer the incarnation the catalog records,
// which stops the reconciler with ErrIncarnationChanged until an operator adopts the stream (parking.md *incarnations*).
func (r *Reconciler) Run(ctx context.Context) error {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		if err := r.Step(ctx); errors.Is(err, catalog.ErrIncarnationChanged) || ctx.Err() != nil {
			return err
		} else if err != nil {
			r.Log.Warn("compact: reconciler cycle", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Step is one cycle: the compactor lease taken or renewed, then the orphan pass and the copy pass on both streams under it.
// While another process holds the lease it does nothing. A lease lost during the cycle ends it, and the next cycle takes the lease again
// under a new fence token; a worker that lost its lease does nothing further under the old one (compactor.md §8.1).
func (r *Reconciler) Step(ctx context.Context) error {
	if !r.hold(ctx) {
		return nil
	}
	lease := *r.lease
	err := r.cycle(ctx, lease)
	if errors.Is(err, catalog.ErrLeaseLost) {
		r.lease = nil
		r.Log.Warn("compact: compactor lease lost", "holder", r.Holder)

		return nil
	}

	return err
}

func (r *Reconciler) cycle(ctx context.Context, lease catalog.Lease) error {
	rep, err := r.Orphans(ctx, lease)
	if err != nil {
		return err
	}
	if len(rep.Quarantined)+len(rep.Unadmitted) > 0 {
		r.Log.Warn("compact: orphans left for an operator", "quarantined", len(rep.Quarantined), "unadmitted", len(rep.Unadmitted))
	}
	if _, err := r.Copies(ctx, lease, catalog.RoleTraffic, r.Traffic); err != nil {
		return err
	}
	_, err = r.Copies(ctx, lease, catalog.RoleParking, r.Parked)

	return err
}

// hold takes the lease, or renews it while held; it reports whether this process holds it.
func (r *Reconciler) hold(ctx context.Context) bool {
	if r.lease != nil {
		err := r.Catalog.RenewLease(ctx, *r.lease, r.LeaseTTL)
		switch {
		case err == nil:
			return true
		case !errors.Is(err, catalog.ErrLeaseLost):
			// The lease may still be live; this cycle is skipped and the next renews it again.
			r.Log.Warn("compact: renew the compactor lease", "error", err)

			return false
		}
		r.lease = nil
	}
	l, ok, err := r.Catalog.TakeLease(ctx, leaseName, r.Holder, r.LeaseTTL)
	if err != nil {
		r.Log.Warn("compact: take the compactor lease", "error", err)
	}
	if !ok {
		return false
	}
	r.lease = &l

	return true
}
