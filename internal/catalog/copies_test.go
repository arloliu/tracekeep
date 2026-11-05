package catalog

import (
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// copyIDs are the copies of seq whose named hour 1 holds one, every one of them listed.
func copyIDs(t *testing.T, c *Catalog, capture tracepack.UUID, seq uint64) []tracepack.UUID {
	t.Helper()
	got, inHour, err := c.RecordCopies(t.Context(), capture, seq, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !inHour && len(got) != 0 {
		t.Errorf("RecordCopies(%d) listed %v for an hour holding none", seq, got)
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

		got, inHour, err := c.RecordCopies(t.Context(), capture, 5, 1, 100)
		if err != nil || !inHour {
			t.Fatal(inHour, err)
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
		// The listing stops at its limit, in hour order; the named hour is checked whole first, whether or not its copy is listed.
		if got, inHour, err := c.RecordCopies(t.Context(), capture, 5, 3, 2); err != nil || !inHour || len(got) != 2 || got[0].Hour != 1 || got[1].Hour != 1 {
			t.Errorf("RecordCopies(5) named hour 3, at most 2 = %+v, %v, %v; want hour 1's two copies and the hour held", got, inHour, err)
		}
		if got, inHour, err := c.RecordCopies(t.Context(), capture, 5, 2, 100); err != nil || inHour || len(got) != 0 {
			t.Errorf("RecordCopies(5) named hour 2 = %+v, %v, %v; want nothing listed for an hour holding none", got, inHour, err)
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

// The in-hour check and the listing read one snapshot:
// a pack of another hour registered and committed between them is not listed, and the next call lists it.
func TestRecordCopiesReadsOneSnapshot(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		first := segment(t, capture, 1, 0, 9)
		register(t, c, first, Registration{Evidence: &Evidence{}})
		held, release := holdAt(c, hookCopiesChecked)
		var got []Copy
		var inHour bool
		var err error
		listed := run(func() { got, inHour, err = c.RecordCopies(t.Context(), capture, 5, 1, 100) })
		<-held
		later := segment(t, capture, 3, 5, 5)
		register(t, c, later, Registration{Evidence: &Evidence{}})
		close(release)
		<-listed
		if err != nil || !inHour {
			t.Fatal(inHour, err)
		}
		if len(got) != 1 || got[0].Pack != first.ID {
			t.Errorf("RecordCopies(5) held between its check and its listing = %+v, want only the pack committed before its snapshot", got)
		}
		if ids := copyIDs(t, c, capture, 5); !sameIDs(ids, first.ID, later.ID) {
			t.Errorf("RecordCopies(5) after both commits = %v, want both packs", ids)
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
