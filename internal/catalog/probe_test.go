package catalog

import (
	"fmt"
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// The probes of a capture's ranges read only those starting at or above the probed seq minus the capture's largest stored span
// (catalog.md §7.1, T-55); these cases hold what each probe finds against ranges starting far below it, at the seqs' edges,
// and against spans that grow and shrink between probes.

// largestSeq is the largest seq a range may hold, just below the close sentinel.
const largestSeq = closeSentinel - 1

// packOf is a segment of capture in hour holding ranges, each [from, to].
func packOf(t *testing.T, capture tracepack.UUID, hour int64, ranges ...[2]uint64) *Pack {
	t.Helper()
	p := segment(t, capture, hour, ranges[0][0], ranges[0][1])
	p.Ranges = p.Ranges[:0]
	for _, r := range ranges {
		p.Ranges = append(p.Ranges, Range{SeqFrom: r[0], SeqTo: r[1], TSFrom: int64(r[0]), TSTo: int64(r[1])}) //nolint:gosec // a test seq fits
	}

	return p
}

// suspectsOf are the suspect runs p's registration recorded, each "from-to" against the index in packs of the other pack.
func suspectsOf(t *testing.T, c *Catalog, p *Pack, packs []*Pack) []string {
	t.Helper()
	var rows []struct {
		From  int64  `db:"seq_from"`
		To    int64  `db:"seq_to"`
		Other []byte `db:"other_pack_id"`
	}
	err := c.read.SelectContext(t.Context(), &rows, `SELECT seq_from, seq_to, other_pack_id FROM conflicts WHERE pack_id = ? AND state = 'suspect'`, p.ID[:])
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%d-%d@%d", r.From, r.To, slices.IndexFunc(packs, func(q *Pack) bool { return q.ID == tracepack.UUID(r.Other) }))
	}
	slices.Sort(out)

	return out
}

// A registration records a suspect against every range of the capture it overlaps, however far below the probed range that range starts,
// in its own hour or another, and none against a range beside it.
func TestOverlapProbeFindsEveryOverlap(t *testing.T) {
	t.Parallel()
	type pk struct {
		hour   int64
		ranges [][2]uint64
	}
	cases := []struct {
		name  string
		prior []pk
		probe pk
		want  []string
	}{
		{"a wide range from far below, another hour", []pk{{1, [][2]uint64{{0, 1000}}}, {2, [][2]uint64{{1001, 1001}}}}, pk{3, [][2]uint64{{900, 905}}}, []string{"900-905@0"}},
		{"a range of the probed pack from far below", []pk{{1, [][2]uint64{{0, 1000}}}}, pk{1, [][2]uint64{{2, 3}, {998, 1003}}}, []string{"2-3@0", "998-1000@0"}},
		{"nested inside", []pk{{1, [][2]uint64{{10, 20}}}}, pk{1, [][2]uint64{{12, 14}}}, []string{"12-14@0"}},
		{"nesting", []pk{{1, [][2]uint64{{12, 14}}}}, pk{1, [][2]uint64{{10, 20}}}, []string{"12-14@0"}},
		{"equal", []pk{{1, [][2]uint64{{10, 20}}}}, pk{2, [][2]uint64{{10, 20}}}, []string{"10-20@0"}},
		{"singletons", []pk{{1, [][2]uint64{{5, 5}}}, {1, [][2]uint64{{6, 6}}}}, pk{1, [][2]uint64{{5, 5}}}, []string{"5-5@0"}},
		{"beside, not overlapping", []pk{{1, [][2]uint64{{0, 4}}}, {1, [][2]uint64{{16, 20}}}}, pk{1, [][2]uint64{{5, 15}}}, nil},
		{"seq 0", []pk{{1, [][2]uint64{{0, 0}}}, {1, [][2]uint64{{1, 100}}}}, pk{1, [][2]uint64{{0, 0}}}, []string{"0-0@0"}},
		{"the largest seq", []pk{{1, [][2]uint64{{0, 10}}}, {1, [][2]uint64{{largestSeq - 1, largestSeq}}}}, pk{1, [][2]uint64{{largestSeq, largestSeq}}}, []string{fmt.Sprintf("%d-%d@1", uint64(largestSeq), uint64(largestSeq))}},
		{"a range from seq 0 to the largest", []pk{{1, [][2]uint64{{0, largestSeq}}}}, pk{2, [][2]uint64{{largestSeq - 1, largestSeq}}}, []string{fmt.Sprintf("%d-%d@0", uint64(largestSeq-1), uint64(largestSeq))}},
	}
	engines(t, func(t *testing.T, c *Catalog) {
		for _, tc := range cases {
			capture := admitted(t, c)
			var packs []*Pack
			for _, q := range tc.prior {
				p := packOf(t, capture, q.hour, q.ranges...)
				register(t, c, p, Registration{Evidence: &Evidence{}})
				packs = append(packs, p)
			}
			p := packOf(t, capture, tc.probe.hour, tc.probe.ranges...)
			if out := register(t, c, p, Registration{Evidence: &Evidence{}}); out != Registered {
				t.Fatalf("%s: RegisterPack = %v", tc.name, out)
			}
			if got := suspectsOf(t, c, p, packs); !slices.Equal(got, tc.want) {
				t.Errorf("%s: suspects %v, want %v", tc.name, got, tc.want)
			}
		}
	})
}

// A merged hour's archive holds one wide range: a probe finds it from far below while it is stored,
// and once retention evicted the hour, the narrower ranges left are still found.
func TestOverlapProbeAroundAnEvictedWideRange(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		a, b := packOf(t, capture, 1, [2]uint64{0, 499}), packOf(t, capture, 1, [2]uint64{500, 999})
		register(t, c, a, Registration{Evidence: &Evidence{}})
		register(t, c, b, Registration{Evidence: &Evidence{}})
		archive := archiveOf(t, capture, 1, a, b)
		archive.Ranges = []Range{{SeqFrom: 0, SeqTo: 999}}
		if out := commit(t, c, l, mergeOf(snapshot(t, c, capture), archive)); out != Merged {
			t.Fatalf("CommitMerge = %v", out)
		}
		kept := packOf(t, capture, 2, [2]uint64{1000, 1009})
		register(t, c, kept, Registration{Evidence: &Evidence{}})
		p := packOf(t, capture, 3, [2]uint64{990, 1001})
		register(t, c, p, Registration{Evidence: &Evidence{}})
		packs := []*Pack{a, b, archive, kept, p}
		if got, want := suspectsOf(t, c, p, packs), []string{"1000-1001@3", "990-999@1", "990-999@2"}; !slices.Equal(got, want) {
			t.Errorf("suspects beside the stored wide range %v, want %v", got, want)
		}

		recordRetention(t, c, l, month, 0)
		if ev := evict(t, c, l, Scope{capture, 1}, hourEvictedNow); !ev.Evicted {
			t.Fatalf("EvictHour = %+v, want hour 1 evicted", ev)
		}
		q := packOf(t, capture, 4, [2]uint64{995, 1008})
		register(t, c, q, Registration{Evidence: &Evidence{}})
		if got, want := suspectsOf(t, c, q, packs), []string{"1000-1008@3", "995-1001@4"}; !slices.Equal(got, want) {
			t.Errorf("suspects after the eviction %v, want %v", got, want)
		}
	})
}

// resolveHeld resolves capture's seq under the order covering stream seq 1 on incarnation 1: staged when a range holds the seq, covered otherwise.
func resolveHeld(t *testing.T, c *Catalog, capture tracepack.UUID, seq uint64) Resolution {
	t.Helper()
	r, err := c.Resolve(t.Context(), &VersionKey{Capture: capture, Seq: seq, Header: [44]byte{7}, Digest: [32]byte{7}}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	return r
}

// A seq covered by an order resolves staged when any stored range of the capture holds it:
// at the start of a long seeded history, inside a wide range from far below, in a deleted pack's range,
// at seq 0 and the largest seq, and in a wide range stored after an earlier probe; otherwise the order covers it.
func TestHeldProbeFindsEveryHoldingRange(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		check := func(capture tracepack.UUID, seq uint64, want Resolution) {
			t.Helper()
			if got := resolveHeld(t, c, capture, seq); got != want {
				t.Errorf("Resolve of seq %d = %v, want %v", seq, got, want)
			}
		}
		// The seeders insert ranges directly, naming no span: the engine computes it.
		captures := seedCaptures(t, c, 1, 2)
		seeded, capture := captures[0], captures[1]
		order(t, c, seeded, "a1")
		_, next := seedPacks(t, c, seeded, packsSeed{hour0: 1, hours: 3, packs: 10, ranges: 10, span: 10})
		check(seeded, 0, ResolvedStaged)
		check(seeded, 5, ResolvedStaged)
		check(seeded, next-1, ResolvedStaged)
		check(seeded, next, CoveredByOrder)

		order(t, c, capture, "a2")
		check(capture, 0, CoveredByOrder) // no range stored
		lease, _, _ := c.TakeLease(t.Context(), "compactor/2", "w", ttl)
		cl := segment(t, capture, 1, 0, 0)
		cl.Role, cl.Ranges, cl.SeqStart = RoleClose, nil, closeSentinel
		register(t, c, cl, Registration{Lease: &lease})
		check(capture, 0, CoveredByOrder) // a close holds no range

		register(t, c, packOf(t, capture, 1, [2]uint64{0, 0}), Registration{Evidence: &Evidence{}})
		register(t, c, packOf(t, capture, 1, [2]uint64{largestSeq, largestSeq}), Registration{Evidence: &Evidence{}})
		check(capture, 0, ResolvedStaged)
		check(capture, 1, CoveredByOrder)
		check(capture, largestSeq, ResolvedStaged)
		check(capture, largestSeq-1, CoveredByOrder)

		wide := packOf(t, capture, 2, [2]uint64{5000, 9000})
		register(t, c, wide, Registration{Evidence: &Evidence{}})
		register(t, c, packOf(t, capture, 2, [2]uint64{9001, 9001}), Registration{Evidence: &Evidence{}})
		check(capture, 8999, ResolvedStaged)
		check(capture, 12000, CoveredByOrder)
		// A wider range stored after that probe is found by the next.
		register(t, c, packOf(t, capture, 3, [2]uint64{10000, 20000}), Registration{Evidence: &Evidence{}})
		check(capture, 19999, ResolvedStaged)
		check(capture, 20001, CoveredByOrder)
		// A deleted pack's range still holds its seqs: no pack state filters the probe.
		exec(t, c, `UPDATE packs SET state = 'deleted' WHERE pack_id = ?`, wide.ID[:])
		check(capture, 8999, ResolvedStaged)
	})
}

// The copies of a seq include a wide range from far below, in the named hour's check and in the listing,
// which keeps its exclusions, its hour order and its limit.
func TestRecordCopiesFindsAWideRange(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		wide := packOf(t, capture, 1, [2]uint64{0, 1000})
		near := packOf(t, capture, 3, [2]uint64{995, 996})
		excluded, deleted := packOf(t, capture, 2, [2]uint64{990, 999}), packOf(t, capture, 2, [2]uint64{991, 999})
		for _, p := range []*Pack{wide, packOf(t, capture, 2, [2]uint64{1001, 1010}), near, excluded, deleted} {
			register(t, c, p, Registration{Evidence: &Evidence{}})
		}
		exec(t, c, `UPDATE packs SET state = 'excluded' WHERE pack_id = ?`, excluded.ID[:])
		exec(t, c, `UPDATE packs SET state = 'deleted' WHERE pack_id = ?`, deleted.ID[:])

		if ids := copyIDs(t, c, capture, 995); !slices.Equal(ids, []tracepack.UUID{wide.ID, near.ID}) {
			t.Errorf("RecordCopies(995) named hour 1 = %v, want the wide pack then the near one", ids)
		}
		if got, inHour, err := c.RecordCopies(t.Context(), capture, 995, 3, 1); err != nil || !inHour || len(got) != 1 || got[0].Pack != wide.ID {
			t.Errorf("RecordCopies(995) named hour 3, at most 1 = %+v, %v, %v; want the wide pack and the hour held", got, inHour, err)
		}
		if got, inHour, err := c.RecordCopies(t.Context(), capture, 997, 3, 10); err != nil || inHour || len(got) != 0 {
			t.Errorf("RecordCopies(997) named hour 3 = %+v, %v, %v; want nothing for an hour not holding it", got, inHour, err)
		}
		if got, inHour, err := c.RecordCopies(t.Context(), capture, 992, 2, 10); err != nil || inHour || len(got) != 0 {
			t.Errorf("RecordCopies(992) named hour 2 = %+v, %v, %v; want nothing: hour 2's packs holding it are excluded or deleted", got, inHour, err)
		}
	})
}
