package catalog

import (
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// pack registers a pack of capture in hour 5 holding the seq runs, each record's time its seq, as ingest's registration would.
func pack(t *testing.T, c *Catalog, capture tracepack.UUID, runs ...[2]uint64) {
	t.Helper()
	p := segment(t, capture, 5, runs[0][0], runs[0][1])
	p.Ranges = nil
	for _, r := range runs {
		p.Ranges = append(p.Ranges, Range{SeqFrom: r[0], SeqTo: r[1], TSFrom: int64(r[0]), TSTo: int64(r[1])})
	}
	ev := &Evidence{Deliveries: []Identity{*identity(capture, runs[0][0], 1)}}
	if out := register(t, c, p, Registration{Evidence: ev}); out != Registered {
		t.Fatalf("RegisterPack = %v", out)
	}
}

func completeness(t *testing.T, c *Catalog, capture tracepack.UUID) Completeness {
	t.Helper()
	got, err := c.Completeness(t.Context(), capture)
	if err != nil {
		t.Fatal(err)
	}

	return got
}

func gapRuns(gs []Gap) [][2]uint64 {
	out := make([][2]uint64, 0, len(gs))
	for _, g := range gs {
		out = append(out, [2]uint64{g.From, g.To})
	}

	return out
}

// The gate's two-pack counterexamples: completeness comes from exact seq runs, never from a pack's first and last seq.
// Two packs whose spans both cover seq 5, which neither holds, leave a gap at 5, timed by its neighbours;
// two packs each holding the other's holes leave none.
func TestCompletenessTwoPacks(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		spans := admitted(t, c)
		pack(t, c, spans, [2]uint64{0, 3}, [2]uint64{6, 9})
		pack(t, c, spans, [2]uint64{2, 4}, [2]uint64{7, 9})
		got := completeness(t, c, spans)
		if !slices.Equal(gapRuns(got.Gaps), [][2]uint64{{5, 5}}) || !got.HasFrontier || got.Frontier != 9 {
			t.Fatalf("gaps %v, frontier %d; want the gap at 5 below frontier 9", got.Gaps, got.Frontier)
		}
		if g := got.Gaps[0]; g.Start == nil || *g.Start != 4 || g.End == nil || *g.End != 6 {
			t.Errorf("gap %+v, want it between the records at 4 and 6", g)
		}

		filled := admitted(t, c)
		pack(t, c, filled, [2]uint64{0, 2}, [2]uint64{5, 6})
		pack(t, c, filled, [2]uint64{3, 4}, [2]uint64{7, 9})
		if got := completeness(t, c, filled); len(got.Gaps) != 0 || got.Frontier != 9 {
			t.Errorf("gaps %v, frontier %d; want none below frontier 9", got.Gaps, got.Frontier)
		}
	})
}

// Each seq is classified once, retained > evicted > deferred > declared loss > gap, up to the frontier,
// which a parked seq above the highest seq raises; a missing prefix is open-ended from the capture's origin; above the frontier is the unknown tail.
func TestCompletenessClassifies(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		pack(t, c, capture, [2]uint64{3, 4}, [2]uint64{10, 10})
		if _, err := c.write.ExecContext(ctx, `INSERT INTO evicted_ranges (capture_id, hour, seq_from, seq_to, ts_from, ts_to) VALUES (?, 2, 4, 6, 4, 6)`, capture[:]); err != nil {
			t.Fatal(err)
		}
		park(t, c, identity(capture, 7, 7), "parked")
		park(t, c, identity(capture, 8, 8), "terminated")
		park(t, c, identity(capture, 10, 10), "terminated") // a declared loss of one version beside another retained
		park(t, c, identity(capture, 12, 12), "parking")    // above the highest seq: the frontier
		park(t, c, identity(capture, 3, 3), "parked")       // retained wins over deferred

		got := completeness(t, c, capture)
		if !got.HasFrontier || got.Frontier != 12 {
			t.Fatalf("frontier %d, want the parked seq 12", got.Frontier)
		}
		if !slices.Equal(gapRuns(got.Gaps), [][2]uint64{{0, 2}, {9, 9}, {11, 11}}) {
			t.Errorf("gaps %v, want 0-2, 9 and 11", got.Gaps)
		}
		if g := got.Gaps[0]; !g.OpenStart || g.Start == nil || *g.Start != 1 || *g.End != 3 {
			t.Errorf("missing prefix %+v, want it open-ended from the origin 1 to the record at 3", g)
		}
		if !slices.Equal(evictedRuns(got.Evicted), [][2]uint64{{5, 6}}) {
			t.Errorf("evicted %v, want 5-6, seq 4 counting as retained", got.Evicted)
		}
		deferred := make([]uint64, 0, len(got.Deferred))
		for _, d := range got.Deferred {
			deferred = append(deferred, d.Seq)
		}
		if !slices.Equal(deferred, []uint64{7, 12}) {
			t.Errorf("deferred %v, want 7 and 12", deferred)
		}
		lost := make([]uint64, 0, len(got.Losses))
		for _, l := range got.Losses { // no pack declares a coverage entry here: every loss is a terminated version
			lost = append(lost, *l.SeqFrom)
		}
		if !slices.Equal(lost, []uint64{8, 10}) {
			t.Errorf("terminated losses %v, want 8 and 10, the retained seq's version included", lost)
		}
		if got.UnknownTailFrom != 13 {
			t.Errorf("unknown tail from %d, want 13", got.UnknownTailFrom)
		}
	})
}

// A capture with nothing registered or parked has no frontier: all of it is the unknown tail.
func TestCompletenessEmpty(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		got := completeness(t, c, admitted(t, c))
		if got.HasFrontier || len(got.Gaps) != 0 || got.UnknownTailFrom != 0 {
			t.Errorf("completeness %+v, want no frontier and the unknown tail from 0", got)
		}
	})
}

// A pack's coverage entry is a declared loss: its seqs read as lost, not as a gap, and the entry is reported as declared.
func TestCompletenessPackLoss(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		p := segment(t, capture, 5, 0, 2)
		from, to, ts := uint64(3), uint64(4), int64(3)
		p.Losses = []Loss{{SeqFrom: &from, SeqTo: &to, TSFrom: &ts, TSTo: &ts}}
		register(t, c, p, Registration{Evidence: &Evidence{Deliveries: []Identity{*identity(capture, 0, 1)}}})
		pack(t, c, capture, [2]uint64{5, 6})
		got := completeness(t, c, capture)
		if len(got.Gaps) != 0 || len(got.Losses) != 1 || *got.Losses[0].SeqFrom != 3 || *got.Losses[0].SeqTo != 4 || got.Losses[0].ActionID != "" {
			t.Errorf("gaps %v, losses %+v; want no gap and the pack's declared 3-4", got.Gaps, got.Losses)
		}
	})
}

func evictedRuns(rs []Run) [][2]uint64 {
	out := make([][2]uint64, 0, len(rs))
	for _, r := range rs {
		out = append(out, [2]uint64{r.SeqFrom, r.SeqTo})
	}

	return out
}

// Completeness reads one snapshot: a registration staging a parked seq between its reads cannot turn that seq into a gap
// (barrier-controlled, both engines): the read sees the seq deferred, as it was when the read began.
func TestCompletenessReadsOneSnapshot(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		pack(t, c, capture, [2]uint64{0, 4}, [2]uint64{6, 10})
		five := identity(capture, 5, 50)
		park(t, c, five, "parked")
		held, release := holdAt(c, hookCompletenessRead)
		var got Completeness
		var err error
		done := run(func() { got, err = c.Completeness(ctx, capture) })
		<-held
		p := segment(t, capture, 5, 5, 5) // the parked seq registered and its version staged, between the reads
		if out := register(t, c, p, Registration{Evidence: &Evidence{Deliveries: []Identity{*five}}}); out != Registered {
			t.Fatalf("RegisterPack = %v", out)
		}
		close(release)
		<-done
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Gaps) != 0 || len(got.Deferred) != 1 || got.Deferred[0].Seq != 5 {
			t.Errorf("gaps %v, deferred %v; want seq 5 deferred as the snapshot holds it", got.Gaps, got.Deferred)
		}
	})
}

// A time the catalog does not hold stays absent: beside a declared loss without times, a gap's bound is nil, never zero;
// an evicted run a retained one cuts has no time at its cut endpoints.
func TestCompletenessUnknownTimes(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		p := segment(t, capture, 5, 0, 0)
		one := uint64(1)
		p.Losses = []Loss{{SeqFrom: &one, SeqTo: &one}} // seq 1 declared lost, no time
		register(t, c, p, Registration{Evidence: &Evidence{Deliveries: []Identity{*identity(capture, 0, 1)}}})
		pack(t, c, capture, [2]uint64{3, 3}, [2]uint64{24, 26})
		if _, err := c.write.ExecContext(ctx, `INSERT INTO evicted_ranges (capture_id, hour, seq_from, seq_to, ts_from, ts_to) VALUES (?, 2, 20, 30, 20, 30)`, capture[:]); err != nil {
			t.Fatal(err)
		}
		got := completeness(t, c, capture)
		if len(got.Gaps) == 0 || got.Gaps[0].From != 2 || got.Gaps[0].Start != nil || got.Gaps[0].End == nil || *got.Gaps[0].End != 3 {
			t.Errorf("gaps %+v, want 2 with no start time and the record at 3 after it", got.Gaps)
		}
		if len(got.Evicted) != 2 || got.Evicted[0].TSTo != nil || got.Evicted[1].TSFrom != nil ||
			got.Evicted[0].TSFrom == nil || *got.Evicted[0].TSFrom != 20 || got.Evicted[1].TSTo == nil || *got.Evicted[1].TSTo != 30 {
			t.Errorf("evicted %+v, want 20-23 and 27-30 with no time at the cut", got.Evicted)
		}
	})
}

// A capture with no producer seq still reports its diagnostics: here a close declaring a loss, no frontier and no gap.
func TestCompletenessWithoutAFrontier(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		lease, _, err := c.TakeLease(t.Context(), "compactor", "w", ttl)
		if err != nil {
			t.Fatal(err)
		}
		p := segment(t, capture, 5, 0, 0)
		p.Role, p.SeqStart, p.Ranges = RoleClose, closeSentinel, nil
		from := uint64(0)
		p.Losses = []Loss{{SeqFrom: &from}} // everything from seq 0 lost, its end unknown
		if out := register(t, c, p, Registration{Lease: &lease}); out != Registered {
			t.Fatalf("RegisterPack of the close = %v", out)
		}
		got := completeness(t, c, capture)
		if got.HasFrontier || len(got.Gaps) != 0 || len(got.Losses) != 1 || got.Losses[0].SeqTo != nil {
			t.Errorf("completeness %+v, want no frontier and the close's loss reported", got)
		}
	})
}

// A time the catalog holds is kept wherever it applies: a gap beside a seq both lost without a time and retained with one
// is timed by the retained record; an evicted fragment of one seq, cut on its other side, keeps that record's time.
func TestCompletenessKnownTimesKept(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		p := segment(t, capture, 5, 0, 0)
		from, to := uint64(1), uint64(3)
		p.Losses = []Loss{{SeqFrom: &from, SeqTo: &to}} // 1-3 declared lost, no time
		register(t, c, p, Registration{Evidence: &Evidence{Deliveries: []Identity{*identity(capture, 0, 1)}}})
		pack(t, c, capture, [2]uint64{3, 3}, [2]uint64{5, 5}, [2]uint64{21, 29})
		if _, err := c.write.ExecContext(ctx, `INSERT INTO evicted_ranges (capture_id, hour, seq_from, seq_to, ts_from, ts_to) VALUES (?, 2, 20, 30, 20, 30)`, capture[:]); err != nil {
			t.Fatal(err)
		}
		got := completeness(t, c, capture)
		if len(got.Gaps) == 0 || got.Gaps[0].From != 4 || got.Gaps[0].Start == nil || *got.Gaps[0].Start != 3 {
			t.Errorf("gaps %+v, want 4 after the retained record at 3", got.Gaps)
		}
		if len(got.Evicted) != 2 || got.Evicted[0].TSTo == nil || *got.Evicted[0].TSTo != 20 || got.Evicted[1].TSFrom == nil || *got.Evicted[1].TSFrom != 30 {
			t.Errorf("evicted %+v, want 20 and 30, each its own record's time at both ends", got.Evicted)
		}
	})
}
