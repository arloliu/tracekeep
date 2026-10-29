package catalog

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

func conflictStates(t *testing.T, c *Catalog, capture tracepack.UUID) []string {
	t.Helper()
	var rows []struct {
		From  int64  `db:"seq_from"`
		To    int64  `db:"seq_to"`
		State string `db:"state"`
	}
	err := c.read.SelectContext(t.Context(), &rows, `SELECT seq_from, seq_to, state FROM conflicts WHERE capture_id = ?
		ORDER BY seq_from, seq_to, state`, capture[:])
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.State
	}

	return out
}

func suspects(t *testing.T, c *Catalog) []Suspect {
	t.Helper()
	got, err := c.Suspects(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return got
}

// Two packs in hours 1 and 2 overlapping at seqs 5-9 raise one suspect, which Suspects lists with both packs.
func suspected(t *testing.T, c *Catalog) (tracepack.UUID, Suspect) {
	t.Helper()
	capture := admitted(t, c)
	a, b := segment(t, capture, 1, 0, 9), segment(t, capture, 2, 5, 14)
	register(t, c, a, Registration{Evidence: &Evidence{}})
	register(t, c, b, Registration{Evidence: &Evidence{}})
	ss := suspects(t, c)
	if len(ss) != 1 {
		t.Fatalf("Suspects = %+v, want one", ss)
	}
	s := ss[0]
	if s.Capture != capture || s.SeqFrom != 5 || s.SeqTo != 9 || s.Pack.ID != b.ID || s.Other.ID != a.ID ||
		s.Pack.Hour != 2 || s.Other.Hour != 1 || s.Pack.Key != b.Key || s.Other.Key != a.Key {
		t.Fatalf("suspect = %+v, want hours 2 and 1 at seqs 5-9", s)
	}

	return capture, s
}

func TestRecordConflict(t *testing.T) {
	t.Parallel()
	for _, differ := range []bool{false, true} {
		engines(t, func(t *testing.T, c *Catalog) {
			capture, s := suspected(t, c)
			l := compactorLease(t, c, "h")
			before := events(t, c)
			out, err := c.RecordConflict(t.Context(), l, s, differ)
			if err != nil || out != ConflictRecorded {
				t.Fatalf("RecordConflict = %v, %v; want recorded", out, err)
			}
			want := []string{}
			if differ {
				want = []string{"confirmed"}
			}
			if got := conflictStates(t, c, capture); !slices.Equal(got, want) {
				t.Errorf("conflicts = %v, want %v", got, want)
			}
			// Both scopes it touches advance and emit a conflict event, so a replayed event is never ignored.
			if got := events(t, c)[len(before):]; !slices.Equal(got, []string{"conflict@1", "conflict@2"}) {
				t.Errorf("new events = %v, want a conflict on each scope", got)
			}
			for hour, rev := range map[int64]int64{1: 3, 2: 2} {
				if sc := scopeOf(t, c, capture, hour); sc.Revision != rev {
					t.Errorf("scope %d revision %d, want %d", hour, sc.Revision, rev)
				}
			}
		})
	}
}

// A comparison is recorded only while its row still names exactly the packs compared, as a suspect;
// otherwise it returns changed and writes nothing, and another pair's row is never touched.
func TestRecordConflictChanged(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture, s := suspected(t, c)
		// A third pack overlapping both raises its own pair rows.
		register(t, c, segment(t, capture, 2, 7, 8), Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		before := conflictStates(t, c, capture)
		rev, evs := captureRevision(t, c, capture), events(t, c)
		// While the compared row is live, a comparison naming other packs or another run finds no row of its own.
		for name, changed := range map[string]Suspect{
			"another pack":    func() Suspect { x := s; x.Other.ID = newID(t); return x }(),
			"swapped packs":   func() Suspect { x := s; x.Pack, x.Other = s.Other, s.Pack; return x }(),
			"another seq run": func() Suspect { x := s; x.SeqTo = 8; return x }(),
		} {
			for _, differ := range []bool{false, true} {
				if out, err := c.RecordConflict(t.Context(), l, changed, differ); err != nil || out != ConflictChanged {
					t.Errorf("%s: RecordConflict = %v, %v; want changed", name, out, err)
				}
			}
		}
		if got := conflictStates(t, c, capture); !slices.Equal(got, before) {
			t.Errorf("conflicts = %v, want %v", got, before)
		}
		if got := captureRevision(t, c, capture); got != rev {
			t.Errorf("capture revision = %d, want %d", got, rev)
		}
		if got := events(t, c); !slices.Equal(got, evs) {
			t.Errorf("events = %v, want %v", got, evs)
		}
		// The compared row itself is recorded, and the third pack's rows stay.
		if out, err := c.RecordConflict(t.Context(), l, s, false); err != nil || out != ConflictRecorded {
			t.Fatalf("RecordConflict = %v, %v", out, err)
		}
		if got := conflictStates(t, c, capture); len(got) != 2 {
			t.Errorf("conflicts after clearing the pair = %v, want the third pack's two rows", got)
		}
		if out, err := c.RecordConflict(t.Context(), l, s, true); err != nil || out != ConflictChanged {
			t.Errorf("a cleared row recorded again = %v, %v; want changed", out, err)
		}
	})
}

func TestRecordConflictUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture, s := suspected(t, c)
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		compactorLease(t, c, "new")
		if _, err := c.RecordConflict(t.Context(), stale, s, true); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("RecordConflict = %v, want ErrLeaseLost", err)
		}
		if got := conflictStates(t, c, capture); !slices.Equal(got, []string{"suspect"}) {
			t.Errorf("conflicts = %v, want the suspect kept", got)
		}
	})
}

// A merge that fails on conflicts writes them as runs per pair of packs: a suspect of the pair meeting a run is confirmed in place,
// a run no row of the pair meets is written confirmed; the scope's revision advances with its event.
func TestRecordMergeConflicts(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		a, b := segment(t, capture, 1, 0, 9), segment(t, capture, 1, 3, 4)
		register(t, c, a, Registration{Evidence: &Evidence{}})
		register(t, c, b, Registration{Evidence: &Evidence{}}) // a suspect of the pair at 3-4
		other := segment(t, capture, 1, 20, 29)
		register(t, c, other, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		v := func(x, y *Pack) [][]tracepack.UUID { return [][]tracepack.UUID{{x.ID}, {y.ID}} }
		found := []MergeConflict{
			{Seq: 3, Packs: v(a, b)}, {Seq: 4, Packs: v(b, a)}, // within the suspect's run, either order
			{Seq: 6, Packs: v(a, other)}, {Seq: 7, Packs: v(a, other)}, {Seq: 9, Packs: v(a, other)}, // runs 6-7 and 9
		}
		if err := c.RecordMergeConflicts(t.Context(), l, Scope{Capture: capture, Hour: 1}, found); err != nil {
			t.Fatal(err)
		}
		var rows []struct {
			From  int64  `db:"seq_from"`
			To    int64  `db:"seq_to"`
			State string `db:"state"`
		}
		if err := c.read.SelectContext(t.Context(), &rows, `SELECT seq_from, seq_to, state FROM conflicts WHERE capture_id = ? ORDER BY seq_from`, capture[:]); err != nil {
			t.Fatal(err)
		}
		want := []string{"3-4 confirmed", "6-7 confirmed", "9-9 confirmed"}
		got := make([]string, len(rows))
		for i, r := range rows {
			got[i] = fmt.Sprintf("%d-%d %s", r.From, r.To, r.State)
		}
		if !slices.Equal(got, want) {
			t.Errorf("conflicts = %v, want %v", got, want)
		}
		if evs := events(t, c); evs[len(evs)-1] != "conflict@1" {
			t.Errorf("events = %v, want a conflict last", evs)
		}
		if got, err := c.MergeCandidates(t.Context(), 5); err != nil || len(got) != 0 {
			t.Errorf("candidates = %v, %v; want the scope blocked", got, err)
		}
	})
}
