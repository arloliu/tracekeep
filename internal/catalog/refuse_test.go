package catalog

import (
	"errors"
	"slices"
	"testing"
)

func refuse(t *testing.T, c *Catalog, l Lease, r *Refusal) MergeOutcome {
	t.Helper()
	out, err := c.RefuseMerge(t.Context(), l, r)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

// A merge refused over the metadata budget marks its scope unmergeable at the snapshot's revision, with its event last:
// the scope is no candidate from then on, a registration leaves it unmergeable, and its packs stay staged (compactor.md §8.1).
func TestRefuseMergeMarksTheScope(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		if out := refuse(t, c, l, &Refusal{Scope: snap.Scope, Revision: snap.Revision, Reason: "over budget"}); out != Refused {
			t.Fatalf("RefuseMerge = %v, want refused", out)
		}
		if sc := scopeOf(t, c, capture, 1); sc.State != "unmergeable" || sc.Revision != snap.Revision+1 {
			t.Errorf("scope = %+v, want unmergeable at revision %d", sc, snap.Revision+1)
		}
		if evs := events(t, c); !slices.Equal(evs, []string{"created@1", "unmergeable@1"}) {
			t.Errorf("events = %v, want created then unmergeable", evs)
		}
		if s := packState(t, c, in.ID); s != "staged" {
			t.Errorf("input is %s, want staged", s)
		}
		if cands, err := c.MergeCandidates(t.Context(), 10); err != nil || len(cands) != 0 {
			t.Errorf("candidates = %v, %v; want none", cands, err)
		}
		register(t, c, segment(t, capture, 1, 10, 19), Registration{Evidence: &Evidence{}})
		if sc := scopeOf(t, c, capture, 1); sc.State != "unmergeable" {
			t.Errorf("after a registration the scope is %s, want unmergeable", sc.State)
		}
		// An operator's request may still merge it.
		if snap := snapshot(t, c, capture); !snap.Eligible {
			t.Errorf("snapshot of an unmergeable scope = %+v, want eligible for a request", snap)
		}
	})
}

// A refusal whose snapshot is stale writes nothing; one under a lost lease returns ErrLeaseLost and writes nothing.
func TestRefuseMergeIsStaleAfterARegistration(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		register(t, c, segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		register(t, c, segment(t, capture, 1, 10, 19), Registration{Evidence: &Evidence{}})
		rev, evs := captureRevision(t, c, capture), events(t, c)
		if out := refuse(t, c, l, &Refusal{Scope: snap.Scope, Revision: snap.Revision, Reason: "over budget"}); out != MergeStale {
			t.Fatalf("RefuseMerge = %v, want stale", out)
		}
		if sc := scopeOf(t, c, capture, 1); sc.State != "open" {
			t.Errorf("scope is %s, want open", sc.State)
		}
		if got := captureRevision(t, c, capture); got != rev {
			t.Errorf("capture revision = %d, want %d: a stale refusal writes nothing", got, rev)
		}
		if got := events(t, c); !slices.Equal(got, evs) {
			t.Errorf("events = %v, want %v", got, evs)
		}
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		compactorLease(t, c, "new")
		snap = snapshot(t, c, capture)
		if _, err := c.RefuseMerge(t.Context(), l, &Refusal{Scope: snap.Scope, Revision: snap.Revision}); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("RefuseMerge under a lost lease = %v, want ErrLeaseLost", err)
		}
		if sc := scopeOf(t, c, capture, 1); sc.State != "open" {
			t.Errorf("scope is %s, want open", sc.State)
		}
	})
}

// A refused merge that executes an operator's request ends it failed with the reason, in the same transaction;
// a later request merges the unmergeable scope and settles it.
func TestRefuseMergeEndsItsRequest(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		hour := int64(1)
		id, err := c.RequestCompaction(t.Context(), RequestMerge, capture, &hour, "op")
		if err != nil {
			t.Fatal(err)
		}
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		if out := refuse(t, c, l, &Refusal{Scope: snap.Scope, Revision: snap.Revision, Request: &id, Reason: "over budget"}); out != Refused {
			t.Fatalf("RefuseMerge = %v, want refused", out)
		}
		if state, outcome, err := c.RequestStatus(t.Context(), id); err != nil || state != "failed" || outcome != "over budget" {
			t.Errorf("request is %s, %q, %v; want failed with the reason", state, outcome, err)
		}
		again, err := c.RequestCompaction(t.Context(), RequestMerge, capture, &hour, "op")
		if err != nil {
			t.Fatal(err)
		}
		if v, err := c.DecideRequest(t.Context(), l, Request{ID: again, Kind: RequestMerge, Capture: capture, Hour: &hour}); err != nil || v != Mergeable {
			t.Fatalf("DecideRequest of an unmergeable scope = %q, %v; want mergeable", v, err)
		}
		snap = snapshot(t, c, capture)
		if !snap.Eligible {
			t.Fatalf("snapshot = %+v, want eligible for the request", snap)
		}
		m := mergeOf(snap, archiveOf(t, capture, 1, in))
		m.Request = &again
		if out := commit(t, c, l, m); out != Merged {
			t.Fatalf("CommitMerge of an unmergeable scope = %v, want merged", out)
		}
		if sc := scopeOf(t, c, capture, 1); sc.State != "settled" {
			t.Errorf("scope is %s, want settled", sc.State)
		}
	})
}
