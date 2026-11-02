package catalog

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
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

// An object without the SDK's metadata that a receipt names is attributed by the receipt's row, never the operator's;
// one recorded unattributed before its receipt arrived is forgotten once the row exists.
func TestRecordListingDefersToAReceipt(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		objs := []Listed{{Key: "claims/3", Size: 10}, {Key: "claims/4", Size: 10}}
		claimRow(t, c, capture, 3, 1, "claims/3")
		recordListing(t, c, l, objs, 100)
		if got := unattributedKeys(t, c); !slices.Equal(got, []string{"claims/4"}) {
			t.Fatalf("unattributed = %v, want only the object no receipt names", got)
		}
		claimRow(t, c, capture, 4, 1, "claims/4")
		recordListing(t, c, l, objs, 200)
		if got := unattributedKeys(t, c); len(got) != 0 {
			t.Errorf("unattributed after its receipt = %v, want none", got)
		}
	})
}

func TestRecordListingUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
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
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		compactorLease(t, c, "new")
		if err := c.RemoveClaimObject(t.Context(), l, "claims/live"); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("RemoveClaimObject under a lost lease = %v, want ErrLeaseLost", err)
		}
		if _, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/live"); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("MarkSurvivorDeletable under a lost lease = %v, want ErrLeaseLost", err)
		}
	})
}

// attributedListing is a listing of n attributed objects of capture, seqs seq0 onwards, keyed in seq order.
func attributedListing(capture tracepack.UUID, seq0, n int) []Listed {
	out := make([]Listed, n)
	for i := range n {
		seq := uint64(seq0 + i) //nolint:gosec // a test's seq is small
		out[i] = Listed{Key: fmt.Sprintf("claims/a%06d", seq), Size: 1, Attr: &ClaimObject{Capture: capture, Seq: seq, Hour: 1}}
	}

	return out
}

// listedKeys is the distinct keys of objs, sorted.
func listedKeys(objs []Listed) []string {
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	slices.Sort(keys)

	return slices.Compact(keys)
}

func claimKeys(t *testing.T, c *Catalog) []string {
	t.Helper()

	return slices.Sorted(maps.Keys(claimStates(t, c)))
}

// failCommit makes the n-th commit from now fail with err, which no transaction retries.
func failCommit(c *Catalog, n int64, err error) {
	var commits atomic.Int64
	c.testCommit = func() error {
		if commits.Add(1) == n {
			return err
		}

		return nil
	}
}

// A listing longer than a transaction records each key once wherever it appears, and forgets only what the whole listing lacks:
// an unattributed key only in its last chunk keeps its first sighting.
func TestRecordListingInChunks(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		recordListing(t, c, l, []Listed{{Key: "claims/odd-last", Size: 1}, {Key: "claims/stale", Size: 1}}, 100)
		attributed := attributedListing(tracepack.UUID{7}, 0, listingChunk+76)
		objs := slices.Concat(attributed[:1], []Listed{{Key: "claims/odd-new", Size: 1}}, attributed[1:listingChunk+10],
			attributed[:1], attributed[listingChunk+10:], []Listed{{Key: "claims/odd-new", Size: 1}, {Key: "claims/odd-last", Size: 1}})
		if n := recordListing(t, c, l, objs, 200); n != len(attributed) {
			t.Errorf("RecordListing added %d rows, want %d", n, len(attributed))
		}
		if got := claimKeys(t, c); !slices.Equal(got, listedKeys(attributed)) {
			t.Errorf("%d claim objects, want the %d listed", len(got), len(attributed))
		}
		got, err := c.UnattributedObjects(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := []UnattributedObject{{Key: "claims/odd-last", Size: 1, SeenAt: 100}, {Key: "claims/odd-new", Size: 1, SeenAt: 200}}
		if !slices.Equal(got, want) {
			t.Errorf("unattributed = %+v, want %+v", got, want)
		}
	})
}

// A failure between chunks keeps the chunks committed before it and skips the sweep, so nothing is forgotten;
// the retry adds only what is still missing, then sweeps.
func TestRecordListingFailureBetweenChunks(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		recordListing(t, c, l, []Listed{{Key: "claims/stale", Size: 1}}, 100)
		attributed := attributedListing(tracepack.UUID{7}, 0, 2*listingChunk)
		objs := append(slices.Clone(attributed), Listed{Key: "claims/odd", Size: 1})
		broken := errors.New("broken commit")
		failCommit(c, 2, broken)
		if n, err := c.RecordListing(t.Context(), l, objs, 200); !errors.Is(err, broken) || n != 0 {
			t.Fatalf("RecordListing with its second chunk failing = %d, %v; want 0 and the failure", n, err)
		}
		if got := claimKeys(t, c); !slices.Equal(got, listedKeys(attributed[:listingChunk])) {
			t.Errorf("%d claim objects after the failure, want the first chunk's %d", len(got), listingChunk)
		}
		if got := unattributedKeys(t, c); !slices.Equal(got, []string{"claims/stale"}) {
			t.Errorf("unattributed after the failure = %v, want the stale row kept and nothing added", got)
		}
		c.testCommit = nil
		if n := recordListing(t, c, l, objs, 300); n != listingChunk {
			t.Errorf("the retry added %d rows, want the missing %d", n, listingChunk)
		}
		if got := claimKeys(t, c); !slices.Equal(got, listedKeys(attributed)) {
			t.Errorf("%d claim objects after the retry, want %d", len(got), len(attributed))
		}
		if got := unattributedKeys(t, c); !slices.Equal(got, []string{"claims/odd"}) {
			t.Errorf("unattributed after the retry = %v, want only the listed one", got)
		}
	})
}

// sweepFixture records 2*listingChunk+52 unattributed rows and returns a listing of every third, by key,
// the keys a sweep's first page leaves (the listed ones among its first listingChunk, and every later one) and the listed keys.
func sweepFixture(t *testing.T, c *Catalog) (objs []Listed, afterFirstPage, listed []string) {
	t.Helper()
	all := seedUnattributed(t, c, 2*listingChunk+52)
	slices.SortFunc(all, func(a, b Listed) int { return strings.Compare(a.Key, b.Key) })
	for i, o := range all {
		if i%3 == 0 {
			objs = append(objs, o)
			listed = append(listed, o.Key)
		}
		if i%3 == 0 || i >= listingChunk {
			afterFirstPage = append(afterFirstPage, o.Key)
		}
	}

	return objs, afterFirstPage, listed
}

// A failure after a sweep page committed keeps that page's deletions and the rest of the rows; the retry finishes the sweep.
func TestRecordListingFailureBetweenSweepPages(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		objs, afterFirstPage, listed := sweepFixture(t, c)
		broken := errors.New("broken commit")
		failCommit(c, 2, broken)
		if n, err := c.RecordListing(t.Context(), l, objs, 200); !errors.Is(err, broken) || n != 0 {
			t.Fatalf("RecordListing with its second sweep page failing = %d, %v; want 0 and the failure", n, err)
		}
		if got := unattributedKeys(t, c); !slices.Equal(got, afterFirstPage) {
			t.Errorf("%d unattributed rows after the failure, want the %d the first page left", len(got), len(afterFirstPage))
		}
		c.testCommit = nil
		recordListing(t, c, l, objs, 300)
		if got := unattributedKeys(t, c); !slices.Equal(got, listed) {
			t.Errorf("%d unattributed rows after the retry, want the %d listed", len(got), len(listed))
		}
	})
}

// A lease lost behind the lease row a chunk or a sweep page holds lands after that transaction committed:
// the next one is refused, nothing after it is written and the sweep is not finished.
func TestRecordListingLeaseLostBetweenTransactions(t *testing.T) {
	t.Parallel()
	lost := func(t *testing.T, c *Catalog, l Lease, objs []Listed) {
		t.Helper()
		held, release := holdAt(c, hookBeforeCommit)
		var err error
		var n int
		ran := run(func() { n, err = c.RecordListing(t.Context(), l, objs, 200) })
		<-held
		waits := c.write.Stats().WaitCount
		leaseRowHeld(t, c, release)
		var lerr error
		lapsed := run(func() {
			_, lerr = c.write.ExecContext(t.Context(), `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		})
		waitBehindLeaseRow(t, c, waits, release)
		close(release)
		<-ran
		<-lapsed
		if lerr != nil || !errors.Is(err, ErrLeaseLost) || n != 0 {
			t.Fatalf("RecordListing = %d, %v (lapse %v); want 0 and ErrLeaseLost", n, err, lerr)
		}
	}
	t.Run("chunks", func(t *testing.T) {
		t.Parallel()
		engines(t, func(t *testing.T, c *Catalog) {
			l := compactorLease(t, c, "w")
			recordListing(t, c, l, []Listed{{Key: "claims/stale", Size: 1}}, 100)
			attributed := attributedListing(tracepack.UUID{7}, 0, 2*listingChunk)
			lost(t, c, l, attributed)
			if got := claimKeys(t, c); !slices.Equal(got, listedKeys(attributed[:listingChunk])) {
				t.Errorf("%d claim objects, want the first chunk's %d", len(got), listingChunk)
			}
			if got := unattributedKeys(t, c); !slices.Equal(got, []string{"claims/stale"}) {
				t.Errorf("unattributed = %v, want the stale row kept", got)
			}
		})
	})
	t.Run("sweep pages", func(t *testing.T) {
		t.Parallel()
		engines(t, func(t *testing.T, c *Catalog) {
			l := compactorLease(t, c, "w")
			objs, afterFirstPage, _ := sweepFixture(t, c)
			lost(t, c, l, objs)
			if got := unattributedKeys(t, c); !slices.Equal(got, afterFirstPage) {
				t.Errorf("%d unattributed rows, want the %d the first page left", len(got), len(afterFirstPage))
			}
		})
	})
}

// A key a receipt attributes after the listing read it and before the sweep keeps the receipt's row,
// and its unattributed row, recorded before or by this listing, is forgotten.
func TestRecordListingAttributedAfterItsRead(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		recordListing(t, c, l, []Listed{{Key: "claims/recorded", Size: 1}}, 100)
		objs := []Listed{{Key: "claims/recorded", Size: 1}, {Key: "claims/new", Size: 1}}
		held, release := holdAt(c, hookListingRead)
		var err error
		var n int
		ran := run(func() { n, err = c.RecordListing(t.Context(), l, objs, 200) })
		select {
		case <-held:
		case <-ran:
			t.Fatal("the listing never paused after its read")
		}
		claimRow(t, c, capture, 1, 1, "claims/recorded")
		claimRow(t, c, capture, 2, 1, "claims/new")
		close(release)
		<-ran
		if err != nil || n != 0 {
			t.Fatalf("RecordListing = %d, %v; want nothing added", n, err)
		}
		want := map[string]string{"claims/recorded": "live", "claims/new": "live"}
		if got := claimStates(t, c); !maps.Equal(got, want) {
			t.Errorf("claim objects = %v, want the receipts' %v", got, want)
		}
		if got := unattributedKeys(t, c); len(got) != 0 {
			t.Errorf("unattributed = %v, want the attributed keys forgotten", got)
		}
	})
}

// A steady listing writes nothing and leaves every row as it is, survivors, deletable rows and first sightings included;
// a stale holder's listing is refused even when it would write nothing.
func TestRecordListingWithoutWrites(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		objs := []Listed{
			{Key: "claims/survivor", Size: 1, Attr: &ClaimObject{Capture: capture, Seq: 1, Hour: 1}},
			{Key: "claims/deletable", Size: 1, Attr: &ClaimObject{Capture: capture, Seq: 2, Hour: 1}},
			{Key: "claims/live", Size: 1, Attr: &ClaimObject{Capture: capture, Seq: 3, Hour: 2}},
			{Key: "claims/odd", Size: 1},
		}
		recordListing(t, c, l, objs, 100)
		lapse(t, c, `UPDATE claim_objects SET state = 'survivor', evicted_hour = 1 WHERE object_key = 'claims/survivor'`)
		lapse(t, c, `UPDATE claim_objects SET state = 'deletable', evicted_hour = 1 WHERE object_key = 'claims/deletable'`)
		claims, err := c.ClaimObjects(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		unattributed, err := c.UnattributedObjects(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var commits atomic.Int64
		c.hooks[hookBeforeCommit] = func() { commits.Add(1) }
		if n := recordListing(t, c, l, objs, 200); n != 0 || commits.Load() != 0 {
			t.Errorf("a steady listing added %d rows in %d transactions, want none", n, commits.Load())
		}
		if got, err := c.ClaimObjects(t.Context()); err != nil || !reflect.DeepEqual(got, claims) {
			t.Errorf("claim objects = %+v, %v; want %+v", got, err, claims)
		}
		if got, err := c.UnattributedObjects(t.Context()); err != nil || !slices.Equal(got, unattributed) {
			t.Errorf("unattributed = %+v, %v; want %+v", got, err, unattributed)
		}
		c.hooks[hookBeforeCommit] = nil
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		compactorLease(t, c, "new")
		for name, ls := range map[string][]Listed{"empty": nil, "unchanged": objs} {
			if _, err := c.RecordListing(t.Context(), l, ls, 300); !errors.Is(err, ErrLeaseLost) {
				t.Errorf("a stale holder's %s listing = %v, want ErrLeaseLost", name, err)
			}
		}
	})
}
