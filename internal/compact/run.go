package compact

import (
	"context"
	"errors"
	"time"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// leaseName is the compactor lease the migrations create.
const leaseName = "compactor"

// The test seams of the package.
const (
	// hookRenewed runs after the keeper renewed the lease during a cycle.
	hookRenewed = iota
	numHooks
)

func (r *Reconciler) hook(id int) {
	if h := r.hooks[id]; h != nil {
		h()
	}
}

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

// Step is one cycle: the compactor lease taken or renewed and both streams checked against their incarnations before any effect,
// then the orphan pass and the copy pass on both streams under the lease, which a keeper renews every third of LeaseTTL while they run.
// While another process holds the lease it does nothing. A lease lost during the cycle cancels it, and the next cycle takes the lease again
// under a new fence token; a worker that lost its lease does nothing further under the old one (compactor.md §8.1).
func (r *Reconciler) Step(ctx context.Context) error {
	if !r.hold(ctx) {
		return nil
	}
	lease := *r.lease
	if err := r.incarnations(ctx); err != nil {
		return err
	}
	cctx, cancel := context.WithCancelCause(ctx)
	kept := make(chan struct{})
	go func() {
		defer close(kept)
		r.keep(cctx, cancel, lease)
	}()
	err := r.cycle(cctx, lease)
	cancel(nil)
	<-kept
	if cause := context.Cause(cctx); errors.Is(cause, catalog.ErrLeaseLost) {
		err = cause
	}
	if errors.Is(err, catalog.ErrLeaseLost) {
		r.lease = nil
		r.Log.Warn("compact: compactor lease lost", "holder", r.Holder)

		return nil
	}

	return err
}

// keep renews lease every third of LeaseTTL until ctx is done, so a pass longer than the lease keeps it;
// a lost lease cancels ctx with ErrLeaseLost, and any other failure leaves the next renewal to try again.
func (r *Reconciler) keep(ctx context.Context, cancel context.CancelCauseFunc, lease catalog.Lease) {
	t := time.NewTicker(max(r.LeaseTTL/3, time.Millisecond))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		err := r.Catalog.RenewLease(ctx, lease, r.LeaseTTL)
		switch {
		case errors.Is(err, catalog.ErrLeaseLost):
			cancel(err)
			return
		case err == nil:
			r.hook(hookRenewed)
		case ctx.Err() == nil:
			r.Log.Warn("compact: renew the compactor lease", "error", err)
		default:
			// The cycle ended during the renewal.
		}
	}
}

// incarnations checks both streams against the incarnations the catalog records, each on a handle of its own (parking.md *incarnations*).
func (r *Reconciler) incarnations(ctx context.Context) error {
	for _, role := range []string{catalog.RoleTraffic, catalog.RoleParking} {
		s, err := r.JS.Stream(ctx, bus.StreamName(r.Bus, role))
		if err != nil {
			return err
		}
		if _, err := bus.Incarnate(ctx, r.Catalog, role, s); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reconciler) cycle(ctx context.Context, lease catalog.Lease) error {
	rep, err := r.Orphans(ctx, lease)
	if err != nil {
		return err
	}
	if len(rep.Quarantined)+len(rep.Unadmitted) > 0 {
		r.Log.Warn("compact: orphans left for an operator", "quarantined", len(rep.Quarantined), "unadmitted", len(rep.Unadmitted))
	}
	for _, role := range []string{catalog.RoleTraffic, catalog.RoleParking} {
		s, err := r.JS.Stream(ctx, bus.StreamName(r.Bus, role))
		if err != nil {
			return err
		}
		if _, err := r.Copies(ctx, lease, role, s); err != nil {
			return err
		}
	}

	return nil
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
