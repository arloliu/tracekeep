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
	// hookMergeSnapshot runs after a merge read its snapshot, before it reads its inputs.
	hookMergeSnapshot
	// hookMergePut runs after a merge put its archive, before its commit.
	hookMergePut
	// hookCompared runs after a suspect's packs were compared, before the result is recorded.
	hookCompared
	numHooks
)

func (r *Compactor) hook(id int) {
	if h := r.hooks[id]; h != nil {
		h()
	}
}

// Run runs a cycle every Interval until ctx is done or a stream is no longer the incarnation the catalog records,
// which stops the reconciler with ErrIncarnationChanged until an operator adopts the stream (parking.md *incarnations*).
// On its way out it releases the lease it holds, so a standby takes over without waiting for the lease to lapse.
func (r *Compactor) Run(ctx context.Context) error {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	defer r.release()
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
// then the orphan pass, the copy pass on both streams, the conflict pass, the operators' requests and the merge pass under the lease, which a keeper renews every third of LeaseTTL while they run.
// While another process holds the lease it does nothing. A lease lost during the cycle cancels it, and the next cycle takes the lease again
// under a new fence token; a worker that lost its lease does nothing further under the old one (compactor.md §8.1).
// The metrics count a cycle that lost its lease as failed, and the lease as held only until it lapses.
func (r *Compactor) Step(ctx context.Context) error {
	asked := time.Now()
	if !r.hold(ctx) {
		r.Metrics.Lease(time.Time{})
		return nil
	}
	r.Metrics.Lease(asked.Add(r.LeaseTTL))
	started := time.Now()
	err := r.underLease(ctx)
	lost := r.lease == nil
	if lost {
		r.Metrics.Lease(time.Time{})
	}
	r.Metrics.Cycle("reconcile", time.Since(started), err == nil && !lost)

	return err
}

// errNotYetHeld reports a reconciler that has not yet learned the compactor lease's state.
var errNotYetHeld = errors.New("compact: the lease's state not read yet")

// Ready reports whether the reconciler knows the compactor lease's state, holding it or standing by, for readiness.
func (r *Compactor) Ready(context.Context) error {
	r.holdMu.Lock()
	defer r.holdMu.Unlock()
	if !r.holdKnown && r.holdErr == nil {
		return errNotYetHeld
	}

	return r.holdErr
}

func (r *Compactor) setHoldErr(err error) {
	r.holdMu.Lock()
	defer r.holdMu.Unlock()
	r.holdErr, r.holdKnown = err, r.holdKnown || err == nil
}

// underLease is one cycle under the lease r holds.
func (r *Compactor) underLease(ctx context.Context) error {
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
	if cause := context.Cause(cctx); errors.Is(cause, catalog.ErrLeaseLost) || errors.Is(cause, catalog.ErrIncarnationChanged) {
		err = cause
	}
	if errors.Is(err, catalog.ErrLeaseLost) {
		r.lease = nil
		r.Log.Warn("compact: compactor lease lost", "holder", r.Holder)

		return nil
	}

	return err
}

// keep renews lease every third of LeaseTTL until ctx is done, so a pass longer than the lease keeps it,
// and checks both streams' incarnations with every renewal (parking.md *incarnations*), on handles of its own;
// a lost lease or a changed stream cancels ctx with its error, and any other failure leaves the next renewal to try again.
func (r *Compactor) keep(ctx context.Context, cancel context.CancelCauseFunc, lease catalog.Lease) {
	t := time.NewTicker(max(r.LeaseTTL/3, time.Millisecond))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		asked := time.Now()
		err := r.Catalog.RenewLease(ctx, lease, r.LeaseTTL)
		switch {
		case errors.Is(err, catalog.ErrLeaseLost):
			cancel(err)
			return
		case err == nil:
			r.Metrics.Lease(asked.Add(r.LeaseTTL))
			r.hook(hookRenewed)
		case ctx.Err() == nil:
			r.Log.Warn("compact: renew the compactor lease", "error", err)
		default:
			// The cycle ended during the renewal.
		}
		err = r.incarnations(ctx)
		switch {
		case errors.Is(err, catalog.ErrIncarnationChanged):
			cancel(err)
			return
		case err != nil && ctx.Err() == nil:
			r.Log.Warn("compact: check the streams", "error", err)
		default:
		}
	}
}

// release ends the lease this process holds, if any.
func (r *Compactor) release() {
	if r.lease == nil {
		return
	}
	// The caller's context is done; the release gets a short one of its own.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Catalog.ReleaseLease(ctx, *r.lease); err != nil {
		r.Log.Warn("compact: release the compactor lease", "error", err)
	}
	r.lease = nil
	r.Metrics.Lease(time.Time{})
}

// incarnations checks both streams against the incarnations the catalog records, each on a handle of its own (parking.md *incarnations*).
func (r *Compactor) incarnations(ctx context.Context) error {
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

func (r *Compactor) cycle(ctx context.Context, lease catalog.Lease) error {
	rep, err := r.Orphans(ctx, lease)
	r.Metrics.Orphans(len(rep.Adopted), len(rep.Quarantined), len(rep.Tombstoned))
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
	conflicts, err := r.Conflicts(ctx, lease)
	if err != nil {
		return err
	}
	if conflicts.Failed > 0 {
		r.Log.Warn("compact: suspects left uncompared", "failed", conflicts.Failed)
	}
	if _, err := r.Requests(ctx, lease); err != nil {
		return err
	}

	return r.mergePass(ctx, lease)
}

// mergePass runs the merge pass and records its metrics, then reads the scope gauges;
// a failed read of them keeps their previous values.
func (r *Compactor) mergePass(ctx context.Context, lease catalog.Lease) error {
	started := time.Now()
	rep, err := r.Merges(ctx, lease)
	r.Metrics.Cycle("merge", time.Since(started), err == nil)
	r.Metrics.Merges(len(rep.Merged), len(rep.Stale), len(rep.Conflicted), len(rep.Failed))
	if err != nil {
		return err
	}
	if len(rep.Conflicted)+len(rep.Failed) > 0 {
		r.Log.Warn("compact: scopes left unmerged", "conflicted", len(rep.Conflicted), "failed", len(rep.Failed))
	}
	if r.Metrics == nil {
		return nil
	}
	st, err := r.Catalog.ScopeStats(ctx)
	if err != nil {
		r.Log.Warn("compact: read the scope gauges", "error", err)

		return nil
	}
	var end time.Time
	if st.OldestUnmerged != nil {
		end = time.Unix(0, (*st.OldestUnmerged+1)*nsPerHour)
	}
	r.Metrics.Scopes(st.ByState, st.OpenConflicts, end)

	return nil
}

// hold takes the lease, or renews it while held; it reports whether this process holds it.
func (r *Compactor) hold(ctx context.Context) bool {
	if r.lease != nil {
		err := r.Catalog.RenewLease(ctx, *r.lease, r.LeaseTTL)
		switch {
		case err == nil:
			r.setHoldErr(nil)
			return true
		case !errors.Is(err, catalog.ErrLeaseLost):
			// The lease may still be live; this cycle is skipped and the next renews it again.
			r.Log.Warn("compact: renew the compactor lease", "error", err)
			r.setHoldErr(err)

			return false
		}
		r.lease = nil
	}
	l, ok, err := r.Catalog.TakeLease(ctx, leaseName, r.Holder, r.LeaseTTL)
	r.setHoldErr(err)
	if err != nil {
		r.Log.Warn("compact: take the compactor lease", "error", err)
	}
	if !ok {
		return false
	}
	r.lease = &l

	return true
}
