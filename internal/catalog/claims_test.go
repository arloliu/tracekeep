package catalog

import (
	"encoding/hex"
	"errors"
	"maps"
	"slices"
	"testing"
)

func hexBytes(b []byte) string { return hex.EncodeToString(b) }

func recordListing(t *testing.T, c *Catalog, l Lease, objs []Listed, now int64) int {
	t.Helper()
	n, err := c.RecordListing(t.Context(), l, objs, now)
	if err != nil {
		t.Fatal(err)
	}

	return n
}

func claimStates(t *testing.T, c *Catalog) map[string]string {
	t.Helper()
	objs, err := c.ClaimObjects(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, o := range objs {
		out[o.Key] = o.State
	}

	return out
}

func unattributedKeys(t *testing.T, c *Catalog) []string {
	t.Helper()
	objs, err := c.UnattributedObjects(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	slices.Sort(keys)

	return keys
}

// A listing gives an attributed object without a row a live one, whatever its capture's state, and leaves a row it finds as it is;
// an object without the SDK's metadata is recorded unattributed until the bucket no longer holds it.
func TestRecordListing(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		unknown := newID(t)
		claimRow(t, c, capture, 3, 1, "claims/known")
		lapse(t, c, `UPDATE claim_objects SET state = 'survivor', evicted_hour = 1 WHERE object_key = 'claims/known'`)
		objs := []Listed{
			{Key: "claims/known", Size: 10, Attr: &ClaimObject{Capture: capture, Seq: 3, Hour: 1}},
			{Key: "claims/a", Size: 20, Attr: &ClaimObject{Capture: capture, Seq: 4, Hour: 2}},
			{Key: "claims/b", Size: 30, Attr: &ClaimObject{Capture: unknown, Seq: 1, Hour: 2}},
			{Key: "claims/odd", Size: 40},
			{Key: "claims/odder", Size: 50},
		}
		if n := recordListing(t, c, l, objs, 100); n != 2 {
			t.Errorf("RecordListing added %d rows, want the two attributed objects without one", n)
		}
		want := map[string]string{"claims/known": "survivor", "claims/a": "live", "claims/b": "live"}
		if got := claimStates(t, c); !maps.Equal(got, want) {
			t.Errorf("claim objects = %v, want %v", got, want)
		}
		if st, err := c.ClaimStats(t.Context()); err != nil || st != (ClaimStats{Objects: 3, Bytes: 60, Survivors: 1}) {
			t.Errorf("ClaimStats = %+v, %v", st, err)
		}
		if got := unattributedKeys(t, c); !slices.Equal(got, []string{"claims/odd", "claims/odder"}) {
			t.Errorf("unattributed = %v", got)
		}
		// The operator deleted one: the next listing forgets it, and keeps the first sighting of the other.
		recordListing(t, c, l, objs[:4], 200)
		got, err := c.UnattributedObjects(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Key != "claims/odd" || got[0].SeenAt != 100 || got[0].Size != 40 {
			t.Errorf("unattributed after the operator's deletion = %+v", got)
		}
	})
}

func TestRecordListingUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		if _, err := c.RecordListing(t.Context(), stale, []Listed{{Key: "claims/odd", Size: 1}}, 1); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("RecordListing = %v, want ErrLeaseLost", err)
		}
		if len(unattributedKeys(t, c)) != 0 {
			t.Error("a refused listing wrote")
		}
	})
}

// A survivor is released only once §8.4's predicate holds, under the capture row:
// marked deletable, then its row removed after its object is deleted; a deletable row is removed again after a crash.
func TestSurvivorRelease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		waiting := identity(capture, 12, 9)
		waiting.Hour = 1
		park(t, c, waiting, "parked")
		claimRow(t, c, capture, 12, 1, "claims/12")
		recordRetention(t, c, l, month, 0)
		if ev := evict(t, c, l, Scope{capture, 1}, hourEvictedNow); ev.Survivors != 1 {
			t.Fatalf("EvictHour = %+v, want one survivor", ev)
		}
		if ok, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/12"); err != nil || ok {
			t.Fatalf("MarkSurvivorDeletable while its version is unresolved = %v, %v, want kept", ok, err)
		}
		// Terminated, with a copy of the current incarnation still unacknowledged: still kept.
		lapse(t, c, `INSERT INTO bus_streams (incarnation_id, role, stream_name, created_ns, last_seq_seen) VALUES (1, 'traffic', 's', 0, 0)`)
		setState(t, c, waiting, "terminated")
		v := &waiting.Version
		lapse(t, c, `INSERT INTO parked_copies (incarnation_id, stream_seq, capture_id, seq, header, payload_digest, state)
			VALUES (1, 9, X'`+hexOf(capture)+`', 12, X'`+hexBytes(v.Header[:])+`', X'`+hexBytes(v.Digest[:])+`', 'waiting')`)
		if ok, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/12"); err != nil || ok {
			t.Fatalf("MarkSurvivorDeletable with a copy waiting = %v, %v, want kept", ok, err)
		}
		lapse(t, c, `UPDATE parked_copies SET state = 'acked'`)
		if ok, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/12"); err != nil || !ok {
			t.Fatalf("MarkSurvivorDeletable once released = %v, %v", ok, err)
		}
		if got := claimStates(t, c)["claims/12"]; got != "deletable" {
			t.Fatalf("survivor is %s, want deletable", got)
		}
		// Marked again after a crash: it stays deletable, and its removal resumes.
		if ok, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/12"); err != nil || !ok {
			t.Errorf("MarkSurvivorDeletable of a deletable row = %v, %v", ok, err)
		}
		if err := c.RemoveClaimObject(t.Context(), l, "claims/12"); err != nil {
			t.Fatal(err)
		}
		if _, ok := claimStates(t, c)["claims/12"]; ok {
			t.Error("the released survivor kept its row")
		}
		if err := c.RemoveClaimObject(t.Context(), l, "claims/12"); err != nil {
			t.Errorf("a second removal = %v, want nothing to do", err)
		}
	})
}

// A live claim object is never removed by the survivor path, and a lost lease refuses both steps.
func TestSurvivorReleaseGuards(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		claimRow(t, c, capture, 1, 1, "claims/live")
		if ok, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/live"); err != nil || ok {
			t.Errorf("MarkSurvivorDeletable of a live row = %v, %v, want refused", ok, err)
		}
		if err := c.RemoveClaimObject(t.Context(), l, "claims/live"); err != nil {
			t.Fatal(err)
		}
		if claimStates(t, c)["claims/live"] != "live" {
			t.Error("a live row was removed")
		}
		lapse(t, c, `UPDATE claim_objects SET state = 'deletable', evicted_hour = 1`)
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		if err := c.RemoveClaimObject(t.Context(), l, "claims/live"); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("RemoveClaimObject under a lost lease = %v, want ErrLeaseLost", err)
		}
		if _, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/live"); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("MarkSurvivorDeletable under a lost lease = %v, want ErrLeaseLost", err)
		}
	})
}
