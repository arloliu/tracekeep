package catalog

import (
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

func readSet(t *testing.T, c *Catalog, capture tracepack.UUID, hour int64) ReadSet {
	t.Helper()
	rs, err := c.ReadSet(t.Context(), capture, hour)
	if err != nil {
		t.Fatal(err)
	}

	return rs
}

// ids are the read set's packs as id and state, in its order.
func ids(rs ReadSet) []string {
	out := make([]string, 0, len(rs.Packs))
	for _, p := range rs.Packs {
		out = append(out, p.ID.String()+":"+p.State)
	}

	return out
}

// The read set of an hour (catalog.md §7.3): its staged segments before a merge, the archive and the segments registered after,
// excluded packs read after the staged ones, never a merged input, and nothing once retention evicted the hour.
func TestReadSet(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		a, b := segment(t, capture, 1, 0, 9), segment(t, capture, 1, 10, 19)
		register(t, c, a, Registration{Evidence: &Evidence{}})
		register(t, c, b, Registration{Evidence: &Evidence{}})
		rs := readSet(t, c, capture, 1)
		if !rs.Scoped || rs.Evicted || len(rs.Packs) != 2 || rs.Packs[0].State != PackStaged || rs.Packs[1].State != PackStaged {
			t.Fatalf("before the merge = %+v, want both segments staged", rs)
		}
		checkPacks(t, rs, a, b)
		archive := archiveOf(t, capture, 1, a, b)
		if out := commit(t, c, l, mergeOf(snapshot(t, c, capture), archive)); out != Merged {
			t.Fatalf("CommitMerge = %v", out)
		}
		late := segment(t, capture, 1, 20, 29)
		register(t, c, late, Registration{Evidence: &Evidence{}})
		odd := segment(t, capture, 1, 25, 34)
		register(t, c, odd, Registration{Evidence: &Evidence{}})
		if _, err := c.ExcludeInput(t.Context(), odd.ID, "op", "a1"); err != nil {
			t.Fatal(err)
		}
		rs = readSet(t, c, capture, 1)
		want := make([]string, 0, 3)
		want = append(want, archive.ID.String()+":staged", late.ID.String()+":staged")
		slices.Sort(want)
		want = append(want, odd.ID.String()+":excluded")
		if got := ids(rs); !slices.Equal(got, want) {
			t.Errorf("after the merge, a late segment and an excluded one = %v, want %v", got, want)
		}
		if rs.ConfigID == nil {
			t.Error("the read set does not name the capture's configuration")
		}
		// Another hour of the capture, and an hour with no scope.
		if rs := readSet(t, c, capture, 2); rs.Scoped || rs.Evicted || len(rs.Packs) != 0 {
			t.Errorf("an hour with no scope = %+v", rs)
		}
		recordRetention(t, c, l, month, 0)
		if ev := evict(t, c, l, Scope{capture, 1}, hourEvictedNow); !ev.Evicted {
			t.Fatalf("eviction = %+v", ev)
		}
		if rs := readSet(t, c, capture, 1); rs.Scoped || !rs.Evicted || len(rs.Packs) != 0 {
			t.Errorf("an evicted hour = %+v, want no scope, evicted, no pack", rs)
		}
	})
}

// checkPacks checks that rs holds each of want with its key, size, role and ranges.
func checkPacks(t *testing.T, rs ReadSet, want ...*Pack) {
	t.Helper()
	for _, w := range want {
		i := slices.IndexFunc(rs.Packs, func(p ReadPack) bool { return p.ID == w.ID })
		if i < 0 {
			t.Errorf("pack %s missing from %+v", w.ID, rs.Packs)
			continue
		}
		p := rs.Packs[i]
		if p.Key != w.Key || p.Size != w.Size || p.Role != w.Role || !slices.Equal(p.Ranges, w.Ranges) {
			t.Errorf("pack %s = %+v, want %+v's key, size, role and ranges", p.ID, p, w)
		}
	}
}
