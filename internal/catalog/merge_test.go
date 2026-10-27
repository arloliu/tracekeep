package catalog

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// compactorLease takes the compactor lease for holder.
func compactorLease(t *testing.T, c *Catalog, holder string) Lease {
	t.Helper()
	l, ok, err := c.TakeLease(t.Context(), "compactor", holder, time.Minute)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}

	return l
}

// archiveOf is the archive a merge of inputs writes: their ranges, one run per input here.
func archiveOf(t *testing.T, capture tracepack.UUID, hour int64, inputs ...*Pack) *Pack {
	t.Helper()
	id := newID(t)
	a := &Pack{
		ID: id, Capture: capture, ToolID: "T1", Hour: hour, Role: RoleArchive, Key: "archive/" + id.String(), Size: 200,
		SeqStart: inputs[0].SeqStart, FormatMinor: 1,
	}
	for _, p := range inputs {
		a.Ranges = append(a.Ranges, p.Ranges...)
		a.Losses = append(a.Losses, p.Losses...)
	}

	return a
}

// mergeOf is the merge of snap's inputs into archive.
func mergeOf(snap Snapshot, archive *Pack) *Merge {
	m := &Merge{Scope: snap.Scope, Revision: snap.Revision, Archive: archive}
	for _, in := range snap.Inputs {
		m.Inputs = append(m.Inputs, in.ID)
	}

	return m
}

// snapshot reads the merge snapshot of capture's hour 1.
func snapshot(t *testing.T, c *Catalog, capture tracepack.UUID) Snapshot {
	t.Helper()
	snap, err := c.MergeSnapshot(t.Context(), Scope{Capture: capture, Hour: 1})
	if err != nil {
		t.Fatal(err)
	}

	return snap
}

func commit(t *testing.T, c *Catalog, l Lease, m *Merge) MergeOutcome {
	t.Helper()
	out, err := c.CommitMerge(t.Context(), l, m)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

func packState(t *testing.T, c *Catalog, id tracepack.UUID) string {
	t.Helper()
	var s string
	if err := c.read.GetContext(t.Context(), &s, `SELECT state FROM packs WHERE pack_id = ?`, id[:]); err != nil {
		t.Fatal(err)
	}

	return s
}

type scopeRow struct {
	State    string `db:"state"`
	Revision int64  `db:"revision"`
	Archive  []byte `db:"archive_pack_id"`
}

func scopeOf(t *testing.T, c *Catalog, capture tracepack.UUID, hour int64) scopeRow {
	t.Helper()
	var s scopeRow
	err := c.read.GetContext(t.Context(), &s, `SELECT state, revision, archive_pack_id FROM scopes WHERE capture_id = ? AND hour = ?`, capture[:], hour)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

// captureRevision is the capture row's data revision, which every committed pack-changing transaction advances.
func captureRevision(t *testing.T, c *Catalog, capture tracepack.UUID) int64 {
	t.Helper()
	var r int64
	if err := c.read.GetContext(t.Context(), &r, `SELECT data_revision FROM captures WHERE capture_id = ?`, capture[:]); err != nil {
		t.Fatal(err)
	}

	return r
}

// facts are what a registration records on its capture.
type facts struct {
	LastRegistered int64 `db:"last_registered_at"`
	HighestSeq     int64 `db:"highest_seq"`
	StopSeen       bool  `db:"stop_seen"`
}

func registrationFacts(t *testing.T, c *Catalog, capture tracepack.UUID) facts {
	t.Helper()
	var f facts
	err := c.read.GetContext(t.Context(), &f, `SELECT last_registered_at, highest_seq, stop_seen FROM captures WHERE capture_id = ?`, capture[:])
	if err != nil {
		t.Fatal(err)
	}

	return f
}

func TestMergeCandidates(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ready, current := admitted(t, c), admitted(t, c)
		register(t, c, segment(t, ready, 1, 0, 9), Registration{Evidence: &Evidence{}})
		// Hour 5 has not ended plus Q when hour 5 is the first not ready.
		register(t, c, segment(t, current, 5, 0, 9), Registration{Evidence: &Evidence{}})
		// A conflict touches a scope through either of its packs: hour 2's suspect against hour 3 keeps both out.
		conflicted := admitted(t, c)
		register(t, c, segment(t, conflicted, 2, 0, 9), Registration{Evidence: &Evidence{}})
		register(t, c, segment(t, conflicted, 3, 5, 15), Registration{Evidence: &Evidence{}})
		got, err := c.MergeCandidates(t.Context(), 5)
		if err != nil {
			t.Fatal(err)
		}
		if want := []Scope{{Capture: ready, Hour: 1}}; !slices.Equal(got, want) {
			t.Errorf("MergeCandidates = %v, want %v", got, want)
		}
		// A settled scope is no candidate; a registration reopening it makes it one again.
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, ready)
		if out := commit(t, c, l, mergeOf(snap, archiveOf(t, ready, 1, segment(t, ready, 1, 0, 9)))); out != Merged {
			t.Fatalf("CommitMerge = %v, want merged", out)
		}
		if got, err := c.MergeCandidates(t.Context(), 5); err != nil || len(got) != 0 {
			t.Errorf("after the merge MergeCandidates = %v, %v; want none", got, err)
		}
		register(t, c, segment(t, ready, 1, 10, 19), Registration{Evidence: &Evidence{}})
		if got, err := c.MergeCandidates(t.Context(), 5); err != nil || len(got) != 1 {
			t.Errorf("after a reopening MergeCandidates = %v, %v; want the scope", got, err)
		}
	})
}

func TestCommitMerge(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		a, b := segment(t, capture, 1, 0, 9), segment(t, capture, 1, 10, 19)
		b.Losses = []Loss{{SeqFrom: ptr(uint64(20))}}
		register(t, c, a, Registration{Evidence: &Evidence{}})
		register(t, c, b, Registration{Evidence: &Evidence{}})
		before := registrationFacts(t, c, capture)
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		if !snap.Eligible || snap.Revision != 2 || len(snap.Inputs) != 2 {
			t.Fatalf("snapshot = %+v, want eligible at revision 2 with two inputs", snap)
		}
		archive := archiveOf(t, capture, 1, a, b)
		if out := commit(t, c, l, mergeOf(snap, archive)); out != Merged {
			t.Fatalf("CommitMerge = %v, want merged", out)
		}
		for _, p := range []*Pack{a, b} {
			if s := packState(t, c, p.ID); s != "merged" {
				t.Errorf("input %s is %s, want merged", p.ID, s)
			}
		}
		if s := packState(t, c, archive.ID); s != "staged" {
			t.Errorf("archive is %s, want staged", s)
		}
		sc := scopeOf(t, c, capture, 1)
		if sc.State != "settled" || sc.Revision != 3 || tracepack.UUID(sc.Archive) != archive.ID {
			t.Errorf("scope = %+v, want settled at revision 3 naming the archive", sc)
		}
		if evs := events(t, c); !slices.Equal(evs, []string{"created@1", "settled@1"}) {
			t.Errorf("events = %v, want created then settled", evs)
		}
		inputs := count(t, c, `SELECT COUNT(*) FROM merge_inputs i JOIN merges m ON m.merge_id = i.merge_id
			WHERE m.output_pack_id = ? AND m.outcome = 'committed'`, archive.ID[:])
		if n := inputs; n != 2 {
			t.Errorf("merge inputs = %d, want 2", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM pack_ranges WHERE pack_id = ?`, archive.ID[:]); n != 2 {
			t.Errorf("archive ranges = %d, want 2", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM pack_losses WHERE pack_id = ?`, archive.ID[:]); n != 1 {
			t.Errorf("archive losses = %d, want 1", n)
		}
		// The merge rewrote records the capture already holds: it raises no suspect against its own inputs,
		// writes no receipt and is no registration of the capture.
		if n := count(t, c, `SELECT COUNT(*) FROM conflicts`); n != 0 {
			t.Errorf("conflicts = %d, want none", n)
		}
		if after := registrationFacts(t, c, capture); after != before {
			t.Errorf("capture after the merge = %+v, want its registration facts unchanged from %+v", after, before)
		}
	})
}

func TestCommitMergeIsStaleAfterARegistration(t *testing.T) {
	t.Parallel()
	for _, reopened := range []bool{false, true} {
		engines(t, func(t *testing.T, c *Catalog) {
			capture := admitted(t, c)
			l := compactorLease(t, c, "h")
			first := segment(t, capture, 1, 0, 9)
			register(t, c, first, Registration{Evidence: &Evidence{}})
			if reopened {
				snap := snapshot(t, c, capture)
				commit(t, c, l, mergeOf(snap, archiveOf(t, capture, 1, first)))
				register(t, c, segment(t, capture, 1, 10, 19), Registration{Evidence: &Evidence{}})
			}
			snap := snapshot(t, c, capture)
			late := segment(t, capture, 1, 20, 29)
			register(t, c, late, Registration{Evidence: &Evidence{}})
			rev, evs := captureRevision(t, c, capture), events(t, c)
			archive := archiveOf(t, capture, 1, first)
			if out := commit(t, c, l, mergeOf(snap, archive)); out != MergeStale {
				t.Fatalf("CommitMerge after a registration = %v, want stale", out)
			}
			assertNothingWritten(t, c, capture, archive, rev, evs)
			if s := packState(t, c, late.ID); s != "staged" {
				t.Errorf("the late segment is %s, want staged", s)
			}
		})
	}
}

// assertNothingWritten checks that a refused merge left no write of its own: no archive row, range, loss or merge relation,
// no input changed, no event, and the capture row's revision as it was.
func assertNothingWritten(t *testing.T, c *Catalog, capture tracepack.UUID, archive *Pack, rev int64, evs []string) {
	t.Helper()
	for _, q := range []string{
		`SELECT COUNT(*) FROM packs WHERE pack_id = ?`,
		`SELECT COUNT(*) FROM pack_ranges WHERE pack_id = ?`,
		`SELECT COUNT(*) FROM pack_losses WHERE pack_id = ?`,
		`SELECT COUNT(*) FROM merges WHERE output_pack_id = ?`,
	} {
		if n := count(t, c, q, archive.ID[:]); n != 0 {
			t.Errorf("%s = %d, want 0", q, n)
		}
	}
	if n := count(t, c, `SELECT COUNT(*) FROM packs WHERE capture_id = ? AND state = 'merged'`, capture[:]); n != 0 &&
		count(t, c, `SELECT COUNT(*) FROM merges WHERE capture_id = ?`, capture[:]) == 0 {
		t.Errorf("%d inputs merged by no merge", n)
	}
	if got := captureRevision(t, c, capture); got != rev {
		t.Errorf("capture revision = %d, want %d: the refused merge's prologue was kept", got, rev)
	}
	if got := events(t, c); !slices.Equal(got, evs) {
		t.Errorf("events = %v, want %v", got, evs)
	}
}

// The commit re-reads the scope's eligibility under the capture row, whatever the snapshot said.
func TestCommitMergeRechecksEligibility(t *testing.T) {
	t.Parallel()
	cases := map[string]func(t *testing.T, c *Catalog, capture tracepack.UUID, input tracepack.UUID){
		"a conflict touching the scope": func(t *testing.T, c *Catalog, capture, input tracepack.UUID) {
			t.Helper()
			// Written without advancing the revision, so only the re-read can see it.
			other := newID(t)
			_, err := c.write.ExecContext(t.Context(), `INSERT INTO conflicts (capture_id, seq_from, seq_to, pack_id, other_pack_id, state)
				VALUES (?, 5, 5, ?, ?, 'suspect')`, capture[:], other[:], input[:])
			if err != nil {
				t.Fatal(err)
			}
		},
		"an input no longer staged": func(t *testing.T, c *Catalog, _, input tracepack.UUID) {
			t.Helper()
			if _, err := c.write.ExecContext(t.Context(), `UPDATE packs SET state = 'excluded' WHERE pack_id = ?`, input[:]); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				capture := admitted(t, c)
				in := segment(t, capture, 1, 0, 9)
				register(t, c, in, Registration{Evidence: &Evidence{}})
				l := compactorLease(t, c, "h")
				snap := snapshot(t, c, capture)
				change(t, c, capture, in.ID)
				rev, evs := captureRevision(t, c, capture), events(t, c)
				archive := archiveOf(t, capture, 1, in)
				if out := commit(t, c, l, mergeOf(snap, archive)); out != MergeStale {
					t.Fatalf("CommitMerge = %v, want stale", out)
				}
				assertNothingWritten(t, c, capture, archive, rev, evs)
			})
		})
	}
}

func TestCommitMergeUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		snap := snapshot(t, c, capture)
		rev, evs := captureRevision(t, c, capture), events(t, c)
		archive := archiveOf(t, capture, 1, in)
		_, err := c.CommitMerge(t.Context(), stale, mergeOf(snap, archive))
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("CommitMerge under a lost lease = %v, want ErrLeaseLost", err)
		}
		assertNothingWritten(t, c, capture, archive, rev, evs)
	})
}

// A commit whose result was lost is retried: the archive is found registered and nothing is written twice.
func TestCommitMergeOfARegisteredArchive(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		m := mergeOf(snap, archiveOf(t, capture, 1, in))
		if out := commit(t, c, l, m); out != Merged {
			t.Fatalf("CommitMerge = %v, want merged", out)
		}
		rev, evs := captureRevision(t, c, capture), events(t, c)
		if out := commit(t, c, l, m); out != ArchiveRegistered {
			t.Fatalf("a retried CommitMerge = %v, want the archive registered", out)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM merges WHERE output_pack_id = ?`, m.Archive.ID[:]); n != 1 {
			t.Errorf("merges = %d, want 1", n)
		}
		if got := captureRevision(t, c, capture); got != rev {
			t.Errorf("capture revision = %d, want %d", got, rev)
		}
		if got := events(t, c); !slices.Equal(got, evs) {
			t.Errorf("events = %v, want %v", got, evs)
		}
	})
}

func TestCommitMergeRefusesAMismatchedArchive(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		for name, a := range map[string]*Pack{
			"another hour":   archiveOf(t, capture, 2, in),
			"a segment role": func() *Pack { p := archiveOf(t, capture, 1, in); p.Role = RoleSegment; return p }(),
			"no input":       archiveOf(t, capture, 1, in),
		} {
			m := mergeOf(snap, a)
			if name == "no input" {
				m.Inputs = nil
			}
			if _, err := c.CommitMerge(t.Context(), l, m); !errors.Is(err, ErrInvalidMerge) {
				t.Errorf("%s: CommitMerge = %v, want ErrInvalidMerge", name, err)
			}
		}
	})
}
