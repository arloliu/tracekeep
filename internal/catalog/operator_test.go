package catalog

import (
	"errors"
	"slices"
	"testing"
)

func TestAcknowledgeConflict(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture, s := suspected(t, c)
		before := events(t, c)
		n, err := c.AcknowledgeConflict(t.Context(), capture, 7, "op", "a1")
		if err != nil || n != 1 {
			t.Fatalf("AcknowledgeConflict = %d, %v; want one row", n, err)
		}
		if got := conflictStates(t, c, capture); !slices.Equal(got, []string{"acknowledged"}) {
			t.Errorf("conflicts = %v, want acknowledged", got)
		}
		if got := events(t, c)[len(before):]; !slices.Equal(got, []string{"conflict@1", "conflict@2"}) {
			t.Errorf("new events = %v, want a conflict on each scope", got)
		}
		// The scope stays unmerged, and a comparison still running finds its suspect gone.
		if got, err := c.MergeCandidates(t.Context(), 5); err != nil || len(got) != 0 {
			t.Errorf("candidates = %v, %v; want none", got, err)
		}
		l := compactorLease(t, c, "h")
		if out, err := c.RecordConflict(t.Context(), l, s, false); err != nil || out != ConflictChanged {
			t.Errorf("RecordConflict after the acknowledgement = %v, %v; want changed", out, err)
		}
		if got := auditActions(t, c, capture); !slices.Equal(got, []string{"conflict-acknowledge"}) {
			t.Errorf("audit = %v", got)
		}
		if _, err := c.AcknowledgeConflict(t.Context(), capture, 100, "op", "a2"); !errors.Is(err, ErrNoConflict) {
			t.Errorf("AcknowledgeConflict at a seq without one = %v, want ErrNoConflict", err)
		}
	})
}

func auditActions(t *testing.T, c *Catalog, capture [16]byte) []string {
	t.Helper()
	es, err := c.AuditEntries(t.Context(), capture)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Action
	}

	return out
}

func TestExcludeInput(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		a, b := segment(t, capture, 1, 0, 9), segment(t, capture, 1, 5, 14)
		register(t, c, a, Registration{Evidence: &Evidence{}})
		register(t, c, b, Registration{Evidence: &Evidence{}})
		if got, _ := c.MergeCandidates(t.Context(), 5); len(got) != 0 {
			t.Fatalf("candidates with a suspect = %v", got)
		}
		snap := snapshot(t, c, capture)
		scope, err := c.ExcludeInput(t.Context(), b.ID, "op", "a1")
		if err != nil || scope != (Scope{Capture: capture, Hour: 1}) {
			t.Fatalf("ExcludeInput = %v, %v", scope, err)
		}
		if s := packState(t, c, b.ID); s != "excluded" {
			t.Errorf("pack is %s, want excluded", s)
		}
		if got := events(t, c); got[len(got)-1] != "excluded@1" {
			t.Errorf("events = %v, want excluded last", got)
		}
		if got := auditActions(t, c, capture); !slices.Equal(got, []string{"scope-exclude-input"}) {
			t.Errorf("audit = %v", got)
		}
		// The excluded pack is no input, its conflict no longer blocks the merge, and a merge snapshotted before fails.
		after := snapshot(t, c, capture)
		if len(after.Inputs) != 1 || after.Inputs[0].ID != a.ID || !after.Eligible {
			t.Errorf("snapshot = %+v, want the other pack the one input, eligible", after)
		}
		l := compactorLease(t, c, "h")
		if out := commit(t, c, l, mergeOf(snap, archiveOf(t, capture, 1, a, b))); out != MergeStale {
			t.Errorf("a merge snapshotted before the exclusion = %v, want stale", out)
		}
		if _, err := c.ExcludeInput(t.Context(), b.ID, "op", "a2"); !errors.Is(err, ErrNotExcludable) {
			t.Errorf("excluding it again = %v, want ErrNotExcludable", err)
		}
		if _, err := c.ExcludeInput(t.Context(), newID(t), "op", "a3"); !errors.Is(err, ErrUnknownPack) {
			t.Errorf("excluding an unknown pack = %v, want ErrUnknownPack", err)
		}
	})
}

func TestCompactRequests(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		hour := int64(1)
		id, err := c.RequestCompaction(t.Context(), RequestMerge, capture, &hour, "op")
		if err != nil {
			t.Fatal(err)
		}
		pending, err := c.PendingRequests(t.Context())
		if err != nil || len(pending) != 1 || pending[0].ID != id || pending[0].Kind != RequestMerge || *pending[0].Hour != 1 {
			t.Fatalf("PendingRequests = %+v, %v", pending, err)
		}
		if got := auditActions(t, c, capture); !slices.Equal(got, []string{"scope-merge"}) {
			t.Errorf("audit = %v", got)
		}
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		l := compactorLease(t, c, "new")
		q := pending[0]
		if err := c.FinishRequest(t.Context(), stale, q, false, "settled"); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("FinishRequest under a lost lease = %v, want ErrLeaseLost", err)
		}
		before := captureRevision(t, c, capture)
		if err := c.FinishRequest(t.Context(), l, q, false, "settled"); err != nil {
			t.Fatal(err)
		}
		// It runs under the capture row, which its prologue advances.
		rev := captureRevision(t, c, capture)
		if rev != before+1 {
			t.Errorf("finishing a request left the capture row at revision %d, want %d", rev, before+1)
		}
		if err := c.FinishRequest(t.Context(), l, q, true, "again"); err != nil {
			t.Errorf("finishing it again = %v, want nothing done", err)
		}
		if got := captureRevision(t, c, capture); got != rev {
			t.Errorf("finishing an ended request wrote the capture row: revision %d, want %d", got, rev)
		}
		if pending, err := c.PendingRequests(t.Context()); err != nil || len(pending) != 0 {
			t.Errorf("PendingRequests = %+v, %v; want none", pending, err)
		}
		var state, outcome string
		row := c.read.QueryRowContext(t.Context(), `SELECT state, outcome FROM compact_requests WHERE request_id = ?`, id[:])
		if err := row.Scan(&state, &outcome); err != nil || state != "done" || outcome != "settled" {
			t.Errorf("request = %s, %q, %v; want done, settled", state, outcome, err)
		}
	})
}

// A merge that executes a request ends it in its own commit, and a refused commit leaves it pending.
func TestCommitMergeEndsItsRequest(t *testing.T) {
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
		register(t, c, segment(t, capture, 1, 10, 19), Registration{Evidence: &Evidence{}})
		stale := mergeOf(snap, archiveOf(t, capture, 1, in))
		stale.Request = &id
		if out := commit(t, c, l, stale); out != MergeStale {
			t.Fatalf("CommitMerge = %v, want stale", out)
		}
		if state, _, err := c.RequestStatus(t.Context(), id); err != nil || state != "pending" {
			t.Errorf("after a stale merge the request is %s, %v; want pending", state, err)
		}
		m := mergeOf(snapshot(t, c, capture), archiveOf(t, capture, 1, in))
		m.Request = &id
		if out := commit(t, c, l, m); out != Merged {
			t.Fatalf("CommitMerge = %v, want merged", out)
		}
		if state, outcome, err := c.RequestStatus(t.Context(), id); err != nil || state != "done" || outcome != "merged" {
			t.Errorf("after the merge the request is %s, %q, %v; want done, merged", state, outcome, err)
		}
	})
}
