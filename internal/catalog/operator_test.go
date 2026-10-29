package catalog

import (
	"errors"
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
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
		q := pending[0]
		if got := auditActions(t, c, capture); !slices.Equal(got, []string{"scope-merge"}) {
			t.Errorf("audit = %v", got)
		}
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		l := compactorLease(t, c, "new")
		if _, err := c.DecideRequest(t.Context(), stale, q); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("DecideRequest under a lost lease = %v, want ErrLeaseLost", err)
		}
		// No scope yet: it ends failed, under the capture row, which its prologue advances.
		before := captureRevision(t, c, capture)
		if got, err := c.DecideRequest(t.Context(), l, q); err != nil || got != "the scope has no staged pack" {
			t.Fatalf("DecideRequest = %q, %v", got, err)
		}
		if rev := captureRevision(t, c, capture); rev != before+1 {
			t.Errorf("the decision left the capture row at revision %d, want %d", rev, before+1)
		}
		if state, outcome, err := c.RequestStatus(t.Context(), id); err != nil || state != "failed" || outcome != "the scope has no staged pack" {
			t.Errorf("request = %s, %q, %v", state, outcome, err)
		}
		if pending, err := c.PendingRequests(t.Context()); err != nil || len(pending) != 0 {
			t.Errorf("PendingRequests = %+v, %v; want none", pending, err)
		}
	})
}

// The decision reads the scope under its capture row, so a change after any earlier look decides:
// a scope reopened after it settled is mergeable, one whose one staged pack was excluded ends failed,
// one touched by a conflict ends failed, a settled one ends done; a mergeable one writes nothing.
func TestDecideRequest(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "h")
		hour := int64(1)
		request := func(capture tracepack.UUID) Request {
			t.Helper()
			id, err := c.RequestCompaction(t.Context(), RequestMerge, capture, &hour, "op")
			if err != nil {
				t.Fatal(err)
			}

			return Request{ID: id, Kind: RequestMerge, Capture: capture, Hour: &hour}
		}
		decide := func(q Request) string {
			t.Helper()
			got, err := c.DecideRequest(t.Context(), l, q)
			if err != nil {
				t.Fatal(err)
			}

			return got
		}
		reopened := admitted(t, c)
		first := segment(t, reopened, 1, 0, 9)
		register(t, c, first, Registration{Evidence: &Evidence{}})
		commit(t, c, l, mergeOf(snapshot(t, c, reopened), archiveOf(t, reopened, 1, first)))
		settled := request(reopened)
		if got := decide(settled); got != "settled" {
			t.Errorf("a settled scope = %q, want settled", got)
		}
		register(t, c, segment(t, reopened, 1, 10, 19), Registration{Evidence: &Evidence{}})
		q := request(reopened)
		rev := captureRevision(t, c, reopened)
		if got := decide(q); got != Mergeable {
			t.Errorf("a reopened scope = %q, want mergeable", got)
		}
		if got := captureRevision(t, c, reopened); got != rev {
			t.Errorf("a mergeable decision wrote the capture row: revision %d, want %d", got, rev)
		}
		if state, _, _ := c.RequestStatus(t.Context(), q.ID); state != "pending" {
			t.Errorf("a mergeable request is %s, want pending", state)
		}

		excluded := admitted(t, c)
		only := segment(t, excluded, 1, 0, 9)
		register(t, c, only, Registration{Evidence: &Evidence{}})
		q = request(excluded)
		if _, err := c.ExcludeInput(t.Context(), only.ID, "op", "a1"); err != nil {
			t.Fatal(err)
		}
		if got := decide(q); got != "the scope has no staged pack" {
			t.Errorf("a scope whose one input was excluded = %q", got)
		}

		conflicted, _ := suspected(t, c)
		if got := decide(request(conflicted)); got != "a conflict touches the scope" {
			t.Errorf("a scope touched by a conflict = %q", got)
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
