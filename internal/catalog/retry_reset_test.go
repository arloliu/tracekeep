package catalog

import (
	"errors"
	"testing"
)

// failOnceThen fails c's next commit retryably and runs change between the failed attempt and its retry.
// The returned flag reports that change ran, so a case whose first commit never failed cannot pass.
func failOnceThen(c *Catalog, change func()) *bool {
	ran := new(bool)
	FailFirstCommit(c, errors.New("commit failed retryably"))
	c.hooks[hookRetry] = func() {
		c.hooks[hookRetry] = nil
		*ran = true
		change()
	}

	return ran
}

// A retried transaction returns its committed attempt's results, never a rolled-back attempt's:
// each case's first commit fails after its statements succeeded, the rows change before the retry,
// and the retry takes a path that records nothing, so what the failed attempt recorded would leak without the closure's reset.
//
// Every closure that resets its results on entry, by what a failed attempt could leak:
//   - here: Incarnate (a retired incarnation's last seq seen), ActivateStaged (changed),
//     ReconcileCopy (ok), RegisterPackGaps (the gaps of a registration that did not commit);
//   - tested apart: DeletePack (TestDeletionRetriedOverChangedRows), ClaimExport (TestClaimRetriedOverChangedRows),
//     DoomExportObjects (TestDoomRetriedOverChangedRows);
//   - nothing, since every path that returns success assigns the whole result and every failure returns the zero value:
//     CloseIntent, PublishClose, BindKey, AcknowledgeConflict, DecideRequest, BeginParking, PrepareAck,
//     moveVersion (AdvanceParked, EvictParked), RecordCopy, TerminateParked, TerminateCapture, ApplyOrder, SpendOrders,
//     AdoptStream, StageConfig, ActivateConfig, Register, RecordConflict, AdmitWhere, Reject, RecordRetention,
//     CommitMerge, RefuseMerge, IntendExport, CompleteExport, ForgetExports, TakeLease;
//     their resets keep the contract for a path a later change adds.
func TestRetriedClosuresReturnTheCommittedResult(t *testing.T) {
	t.Parallel()
	for _, tc := range retriedClosures {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engines(t, tc.run)
		})
	}
}

// retriedClosures are TestRetriedClosuresReturnTheCommittedResult's cases.
var retriedClosures = []struct {
	name string
	run  func(t *testing.T, c *Catalog)
}{
	{"Incarnate", func(t *testing.T, c *Catalog) {
		// The first attempt reads a live incarnation; it is retired before the retry, which records a new one.
		ctx := t.Context()
		old, err := c.Incarnate(ctx, RoleTraffic, "S", 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.ObserveLastSeq(ctx, old.ID, 42); err != nil {
			t.Fatal(err)
		}
		ran := failOnceThen(c, func() { exec(t, c, `UPDATE bus_streams SET retired_at = 1 WHERE incarnation_id = ?`, old.ID) })
		inc, err := c.Incarnate(ctx, RoleTraffic, "S", 1)
		if err != nil || !*ran || inc.ID == old.ID || inc.LastSeqSeen != 0 {
			t.Errorf("Incarnate = %+v, %v (retried %v); want a new incarnation with no sequence seen, not the retired one's 42", inc, err, *ran)
		}
	}},
	{"ActivateStaged", func(t *testing.T, c *Catalog) {
		// Another operator activates the configuration before the retry, which finds it active.
		id := stage(t, c, "next")
		ran := failOnceThen(c, func() {
			if changed, err := c.ActivateStaged(t.Context(), id, nil, nil); err != nil || !changed {
				t.Errorf("the other activation = %v, %v", changed, err)
			}
		})
		if changed, err := c.ActivateStaged(t.Context(), id, nil, nil); err != nil || !*ran || changed {
			t.Errorf("ActivateStaged = %v, %v (retried %v); want unchanged, the other activation's", changed, err, *ran)
		}
	}},
	{"ReconcileCopy", func(t *testing.T, c *Catalog) {
		// Another reconciler marks the copy acked before the retry, which finds nothing waiting.
		ctx := t.Context()
		traffic, err := c.Incarnate(ctx, RoleTraffic, "S", 1)
		if err != nil {
			t.Fatal(err)
		}
		lease := compactorLease(t, c, "w")
		d := identity(newID(t), 1, 10)
		d.Incarnation = traffic.ID
		begin(t, c, d, 0)
		setState(t, c, d, "staged")
		ran := failOnceThen(c, func() {
			if ok, err := c.ReconcileCopy(ctx, d, lease); err != nil || !ok {
				t.Errorf("the other reconciliation = %v, %v", ok, err)
			}
		})
		if ok, err := c.ReconcileCopy(ctx, d, lease); err != nil || !*ran || ok {
			t.Errorf("ReconcileCopy = %v, %v (retried %v); want nothing marked, the other reconciler's", ok, err, *ran)
		}
	}},
	{"RegisterPackGaps", func(t *testing.T, c *Catalog) {
		// The pack, opening a missing prefix, registers elsewhere before the retry, which finds it registered.
		p := segment(t, admitted(t, c), 1, 3, 4)
		ran := failOnceThen(c, func() { register(t, c, p, Registration{Evidence: &Evidence{}}) })
		if out, gaps, err := c.RegisterPackGaps(t.Context(), p, Registration{Evidence: &Evidence{}}); err != nil || !*ran || out != AlreadyRegistered || gaps != 0 {
			t.Errorf("RegisterPackGaps = %v with %d gaps, %v (retried %v); want AlreadyRegistered with none", out, gaps, err, *ran)
		}
	}},
}
