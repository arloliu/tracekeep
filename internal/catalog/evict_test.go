package catalog

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// §8.4's predicate: an object goes only once every parked version of its (capture, seq) is staged,
// or terminated with every copy of a current incarnation acknowledged.
func TestObjectReleased(t *testing.T) {
	t.Parallel()
	current := map[int64]bool{2: true, 3: true}
	v := func(state VersionState, copies ...copyRow) versionCopies {
		return versionCopies{State: state, Copies: copies}
	}
	cases := []struct {
		name     string
		versions []versionCopies
		want     bool
	}{
		{"no parked version", nil, true},
		{"staged, its copy unacknowledged", []versionCopies{v(VersionStaged, copyRow{2, "waiting"})}, true},
		{"terminated, a current copy waiting", []versionCopies{v(VersionTerminated, copyRow{2, "acked"}, copyRow{3, "waiting"})}, false},
		{"terminated, every current copy acked", []versionCopies{v(VersionTerminated, copyRow{2, "acked"}, copyRow{3, "acked"})}, true},
		{"terminated, a retired copy lost", []versionCopies{v(VersionTerminated, copyRow{1, "lost"}, copyRow{2, "acked"})}, true},
		{"terminated, no copy", []versionCopies{v(VersionTerminated)}, true},
		{"parking", []versionCopies{v(VersionParking)}, false},
		{"parked", []versionCopies{v(VersionParked, copyRow{2, "acked"})}, false},
		{"evicted-hour", []versionCopies{v(VersionEvictedHour)}, false},
		{"unresolved, every copy lost", []versionCopies{v(VersionParked, copyRow{1, "lost"})}, false},
		{"two versions, the last unresolved", []versionCopies{v(VersionStaged), v(VersionParked)}, false},
		{"two versions, both satisfied", []versionCopies{v(VersionStaged), v(VersionTerminated, copyRow{2, "acked"})}, true},
	}
	for _, tc := range cases {
		if got := objectReleased(tc.versions, current); got != tc.want {
			t.Errorf("%s: objectReleased = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// hourEvictedNow is the time hour 1 is past its deadline under a recorded month.
var hourEvictedNow = hourNS(2) + int64(month)

func evictCandidates(t *testing.T, c *Catalog, now int64) []Scope {
	t.Helper()
	got, err := c.EvictCandidates(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}

	return got
}

func evict(t *testing.T, c *Catalog, l Lease, s Scope, now int64) Eviction {
	t.Helper()
	ev, err := c.EvictHour(t.Context(), l, s, now)
	if err != nil {
		t.Fatal(err)
	}

	return ev
}

// claimRow writes a claim object row of capture's seq in hour, as a receipt or the listing would.
func claimRow(t *testing.T, c *Catalog, capture tracepack.UUID, seq, hour int64, key string) {
	t.Helper()
	_, err := c.write.ExecContext(t.Context(), `INSERT INTO claim_objects (object_key, capture_id, seq, hour, size, state) VALUES (?, ?, ?, ?, 10, 'live')`,
		key, capture[:], seq, hour)
	if err != nil {
		t.Fatal(err)
	}
}

// The candidates are the hours of admitted captures past the eviction horizon holding a scope, a pack, a live claim object or a parked row;
// none exists before the first retention record.
func TestEvictCandidates(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		register(t, c, segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		register(t, c, segment(t, capture, 5, 10, 19), Registration{Evidence: &Evidence{}})
		claimRow(t, c, capture, 30, 2, "claims/a")
		pending := newID(t)
		if _, err := c.Reject(t.Context(), start(pending), []byte("x")); err != nil {
			t.Fatal(err)
		}
		claimRow(t, c, pending, 1, 1, "claims/b")
		unknown := newID(t)
		claimRow(t, c, unknown, 1, 1, "claims/c")
		parked := identity(capture, 40, 7)
		parked.Hour = 3
		park(t, c, parked, "parked")
		if got := evictCandidates(t, c, hourEvictedNow+int64(3*time.Hour)); len(got) != 0 {
			t.Fatalf("candidates before a retention record = %v", got)
		}
		recordRetention(t, c, l, month, 0)
		got := evictCandidates(t, c, hourEvictedNow+int64(2*time.Hour))
		want := []Scope{{capture, 1}, {capture, 2}, {capture, 3}}
		slices.SortFunc(got, func(a, b Scope) int { return int(a.Hour - b.Hour) })
		if !slices.Equal(got, want) {
			t.Errorf("candidates = %v, want hours 1, 2 and 3 of the admitted capture", got)
		}
	})
}

// Eviction removes the hour's rows and keeps a survivor:
// its ranges coalesced into evicted_ranges, its packs and their rows, receipts and scope gone, its keys tombstoned,
// a claim object nothing needs released, one an unresolved parked version names kept as a survivor with the version moved to evicted-hour.
func TestEvictHour(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		a, b := segment(t, capture, 1, 0, 9), segment(t, capture, 1, 10, 19)
		a.Losses = []Loss{{SeqFrom: ptr(uint64(25)), SeqTo: ptr(uint64(26))}}
		ev := &Evidence{Receipts: []Receipt{{Seq: 3, Header: [44]byte{3}, PayloadSHA256: [32]byte{1}, ObjectKey: "claims/3", ObjectSize: 10}}}
		register(t, c, a, Registration{Evidence: ev})
		register(t, c, b, Registration{Evidence: &Evidence{}})
		archive := archiveOf(t, capture, 1, a, b)
		if out := commit(t, c, l, mergeOf(snapshot(t, c, capture), archive)); out != Merged {
			t.Fatalf("CommitMerge = %v", out)
		}
		kept := segment(t, capture, 2, 20, 29)
		register(t, c, kept, Registration{Evidence: &Evidence{}})
		// An unresolved parked version of seq 12 names its object, which must survive.
		waiting := identity(capture, 12, 9)
		waiting.Hour = 1
		park(t, c, waiting, "parked")
		claimRow(t, c, capture, 12, 1, "claims/12")
		recordRetention(t, c, l, month, 0)
		// a's key a deletion tombstoned already: the eviction tombstones the other two.
		lapse(t, c, `INSERT INTO tombstones (object_key, reason, at) VALUES ('`+a.Key+`', 'deleted', 1)`)

		got := evict(t, c, l, Scope{capture, 1}, hourEvictedNow)
		if !got.Evicted || got.Survivors != 1 || got.Tombstoned != 2 {
			t.Fatalf("EvictHour = %+v, want evicted with one survivor and two keys tombstoned", got)
		}
		slices.Sort(got.Packs)
		wantKeys := []string{a.Key, archive.Key, b.Key}
		slices.Sort(wantKeys)
		if !slices.Equal(got.Packs, wantKeys) || !slices.Equal(got.Claims, []string{"claims/3"}) {
			t.Errorf("objects to delete = %v and %v, want the hour's packs and claims/3", got.Packs, got.Claims)
		}
		rs, err := ranges(t.Context(), c.read, `SELECT seq_from, seq_to, ts_from, ts_to FROM evicted_ranges WHERE capture_id = ? AND hour = 1`, capture)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(rs, []Range{{SeqFrom: 0, SeqTo: 19, TSFrom: 0, TSTo: 19}}) {
			t.Errorf("evicted ranges = %v, want 0–19 coalesced", rs)
		}
		assertHourGone(t, c, capture, a, b, archive)
		if n := count(t, c, `SELECT COUNT(*) FROM claim_objects WHERE object_key = 'claims/3'`); n != 0 {
			t.Error("a released claim object kept its row")
		}
		if n := count(t, c, `SELECT COUNT(*) FROM claim_objects WHERE object_key = 'claims/12' AND state = 'survivor' AND evicted_hour = 1`); n != 1 {
			t.Error("the needed claim object is not a survivor of hour 1")
		}
		if s, _ := versionState(t, c, waiting); s != "evicted-hour" {
			t.Errorf("the parked version is %s, want evicted-hour", s)
		}
		if packState(t, c, kept.ID) != "staged" || count(t, c, `SELECT COUNT(*) FROM scopes WHERE capture_id = ? AND hour = 2`, capture[:]) != 1 {
			t.Error("eviction touched another hour")
		}
		// The hour is no longer a candidate; an object the listing attributes to it later makes it one again,
		// and its eviction leaves the evicted ranges as they were.
		if got := evictCandidates(t, c, hourEvictedNow); len(got) != 0 {
			t.Errorf("candidates after the eviction = %v", got)
		}
		claimRow(t, c, capture, 4, 1, "claims/4")
		if got := evictCandidates(t, c, hourEvictedNow); !slices.Equal(got, []Scope{{capture, 1}}) {
			t.Fatalf("candidates with a late claim object = %v", got)
		}
		again := evict(t, c, l, Scope{capture, 1}, hourEvictedNow)
		if !again.Evicted || len(again.Packs) != 0 || !slices.Equal(again.Claims, []string{"claims/4"}) {
			t.Errorf("a second eviction = %+v, want only claims/4", again)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM evicted_ranges WHERE capture_id = ?`, capture[:]); n != 1 {
			t.Errorf("evicted ranges after a second eviction = %d rows, want 1", n)
		}
	})
}

// A conflict naming an evicted pack goes with it, and the retained scope of its other pack advances with a conflict event.
func TestEvictHourClearsACrossHourConflict(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		old, other := segment(t, capture, 1, 0, 9), segment(t, capture, 2, 5, 14)
		register(t, c, old, Registration{Evidence: &Evidence{}})
		register(t, c, other, Registration{Evidence: &Evidence{}})
		if n := count(t, c, `SELECT COUNT(*) FROM conflicts`); n != 1 {
			t.Fatalf("conflicts = %d, want the overlap's suspect", n)
		}
		rev := scopeOf(t, c, capture, 2).Revision
		recordRetention(t, c, l, month, 0)
		evict(t, c, l, Scope{capture, 1}, hourEvictedNow)
		if n := count(t, c, `SELECT COUNT(*) FROM conflicts`); n != 0 {
			t.Errorf("conflicts after the eviction = %d, want none", n)
		}
		if got := scopeOf(t, c, capture, 2).Revision; got != rev+1 {
			t.Errorf("hour 2 revision = %d, want %d", got, rev+1)
		}
		if evs := events(t, c); evs[len(evs)-1] != "conflict@2" {
			t.Errorf("events = %v, want a conflict event on hour 2 last", evs)
		}
	})
}

// Nothing is evicted before the hour's deadline, of a capture not admitted, with a key another packs row names, or under a lost lease.
func TestEvictHourGuards(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		p := segment(t, capture, 1, 0, 9)
		register(t, c, p, Registration{Evidence: &Evidence{}})
		recordRetention(t, c, l, month, 0)
		if ev := evict(t, c, l, Scope{capture, 1}, hourEvictedNow-1); ev.Evicted {
			t.Error("evicted before the deadline")
		}
		rejected := newID(t)
		if _, err := c.Reject(t.Context(), start(rejected), []byte("x")); err != nil {
			t.Fatal(err)
		}
		claimRow(t, c, rejected, 1, 1, "claims/r")
		if ev := evict(t, c, l, Scope{rejected, 1}, hourEvictedNow); ev.Evicted {
			t.Error("evicted an hour of a rejected capture")
		}
		lapse(t, c, `INSERT INTO packs (pack_id, capture_id, tool_id, hour, role, object_key, size, seq_start, format_minor, state, registered_at)
			VALUES (X'`+hexOf(newID(t))+`', X'`+hexOf(capture)+`', 'T1', 7, 'segment', '`+p.Key+`', 1, 0, 1, 'staged', 0)`)
		if ev := evict(t, c, l, Scope{capture, 1}, hourEvictedNow); ev.Evicted {
			t.Error("evicted a pack whose key another row names")
		}
		if packState(t, c, p.ID) != "staged" || count(t, c, `SELECT COUNT(*) FROM tombstones`) != 0 {
			t.Error("a refused eviction wrote")
		}
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		if _, err := c.EvictHour(t.Context(), l, Scope{capture, 1}, hourEvictedNow); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("EvictHour under a lost lease = %v, want ErrLeaseLost", err)
		}
	})
}

// assertHourGone checks that hour 1's rows and the rows of its packs are gone, and its keys tombstoned once.
func assertHourGone(t *testing.T, c *Catalog, capture tracepack.UUID, packs ...*Pack) {
	t.Helper()
	for table, q := range map[string]string{
		"packs":         `SELECT COUNT(*) FROM packs WHERE capture_id = ? AND hour = 1`,
		"scopes":        `SELECT COUNT(*) FROM scopes WHERE capture_id = ? AND hour = 1`,
		"claim_records": `SELECT COUNT(*) FROM claim_records WHERE capture_id = ? AND hour = 1`,
		"merges":        `SELECT COUNT(*) FROM merges WHERE capture_id = ? AND hour = 1`,
	} {
		if n := count(t, c, q, capture[:]); n != 0 {
			t.Errorf("%s left %d rows of the hour", table, n)
		}
	}
	for _, p := range packs {
		for _, q := range []string{`SELECT COUNT(*) FROM pack_ranges WHERE pack_id = ?`, `SELECT COUNT(*) FROM pack_losses WHERE pack_id = ?`,
			`SELECT COUNT(*) FROM merge_inputs WHERE pack_id = ?`} {
			if n := count(t, c, q, p.ID[:]); n != 0 {
				t.Errorf("%q left %d rows of an evicted pack", q, n)
			}
		}
		if n := count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, p.Key); n != 1 {
			t.Errorf("%s has %d tombstones, want one", p.Key, n)
		}
	}
}
