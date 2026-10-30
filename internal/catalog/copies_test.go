package catalog

import (
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

func copyIDs(t *testing.T, c *Catalog, capture tracepack.UUID, seq uint64) []tracepack.UUID {
	t.Helper()
	got, err := c.RecordCopies(t.Context(), capture, seq)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]tracepack.UUID, len(got))
	for i, cp := range got {
		ids[i] = cp.Pack
	}

	return ids
}

// The copies of a record are every pack of its capture whose range holds the seq, in any hour,
// merged inputs beside their archive, never an excluded or a deleted pack, nor another capture's.
func TestRecordCopies(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture, inputs, archive := mergedScope(t, c, l, 0)
		later := segment(t, capture, 3, 3, 6)
		register(t, c, later, Registration{Evidence: &Evidence{}})
		excluded := segment(t, capture, 4, 5, 5)
		register(t, c, excluded, Registration{Evidence: &Evidence{}})
		lapse(t, c, `UPDATE packs SET state = 'excluded' WHERE pack_id = X'`+hexOf(excluded.ID)+`'`)
		other := admittedAnother(t, c)
		register(t, c, segment(t, other, 1, 0, 9), Registration{Evidence: &Evidence{}})

		got, err := c.RecordCopies(t.Context(), capture, 5)
		if err != nil {
			t.Fatal(err)
		}
		want := map[tracepack.UUID]Copy{
			inputs[0].ID: {Pack: inputs[0].ID, Key: inputs[0].Key, Hour: 1, State: "merged"},
			archive.ID:   {Pack: archive.ID, Key: archive.Key, Hour: 1, State: "staged"},
			later.ID:     {Pack: later.ID, Key: later.Key, Hour: 3, State: "staged"},
		}
		if len(got) != len(want) {
			t.Fatalf("RecordCopies(5) = %+v, want %d copies", got, len(want))
		}
		for _, cp := range got {
			if cp != want[cp.Pack] {
				t.Errorf("copy %+v, want %+v", cp, want[cp.Pack])
			}
		}
		if !slices.IsSortedFunc(got, func(a, b Copy) int { return int(a.Hour - b.Hour) }) {
			t.Errorf("copies %+v not in hour order", got)
		}
		if ids := copyIDs(t, c, capture, 15); !sameIDs(ids, inputs[1].ID, archive.ID) {
			t.Errorf("RecordCopies(15) = %v, want the second input and the archive", ids)
		}
		if ids := copyIDs(t, c, capture, 40); len(ids) != 0 {
			t.Errorf("RecordCopies(40) = %v, want none", ids)
		}

		d := Deletable{Pack: inputs[0].ID, Capture: capture, Key: inputs[0].Key}
		if ok, err := c.DeletePack(t.Context(), l, d, deleteAt(t, c, archive), time.Hour); err != nil || !ok {
			t.Fatalf("DeletePack = %v, %v", ok, err)
		}
		if ids := copyIDs(t, c, capture, 5); !sameIDs(ids, archive.ID, later.ID) {
			t.Errorf("RecordCopies(5) after a deletion = %v, want the archive and the later segment", ids)
		}
	})
}

// sameIDs reports whether got holds exactly want, in any order.
func sameIDs(got []tracepack.UUID, want ...tracepack.UUID) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		if !slices.Contains(got, w) {
			return false
		}
	}

	return true
}
