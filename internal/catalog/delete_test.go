package catalog

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// mergedScope registers two segments of capture's hour 1 and merges them with delete delay d, returning the inputs and the archive.
func mergedScope(t *testing.T, c *Catalog, l Lease, d time.Duration) (capture tracepack.UUID, inputs []*Pack, archive *Pack) {
	t.Helper()
	capture = admitted(t, c)
	a, b := segment(t, capture, 1, 0, 9), segment(t, capture, 1, 10, 19)
	register(t, c, a, Registration{Evidence: &Evidence{}})
	register(t, c, b, Registration{Evidence: &Evidence{}})
	archive = archiveOf(t, capture, 1, a, b)
	m := mergeOf(snapshot(t, c, capture), archive)
	m.DeleteDelay = d
	if out := commit(t, c, l, m); out != Merged {
		t.Fatalf("CommitMerge = %v, want merged", out)
	}

	return capture, []*Pack{a, b}, archive
}

func deleteCandidates(t *testing.T, c *Catalog, now int64) []Deletable {
	t.Helper()
	got, err := c.DeleteCandidates(t.Context(), now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	return got
}

func deleteAt(t *testing.T, c *Catalog, archive *Pack) int64 {
	t.Helper()
	var at int64
	if err := c.read.GetContext(t.Context(), &at, `SELECT delete_at FROM merges WHERE output_pack_id = ?`, archive.ID[:]); err != nil {
		t.Fatal(err)
	}

	return at
}

// A merged input becomes a candidate at its merge's delete_at, fixed by the commit as its time plus D,
// and the deletion marks it deleted with its tombstone; the archive stays.
func TestDeletePack(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		before := c.now()
		_, inputs, archive := mergedScope(t, c, l, time.Hour)
		at := deleteAt(t, c, archive)
		if at < before+int64(time.Hour) || at > c.now()+int64(time.Hour) {
			t.Fatalf("delete_at = %d, want the commit's time plus D", at)
		}
		if got := deleteCandidates(t, c, at-1); len(got) != 0 {
			t.Fatalf("candidates before delete_at = %v, want none", got)
		}
		got := deleteCandidates(t, c, at)
		if len(got) != 2 {
			t.Fatalf("candidates at delete_at = %v, want both inputs", got)
		}
		for _, d := range got {
			ok, err := c.DeletePack(t.Context(), l, d, at, time.Hour)
			if err != nil || !ok {
				t.Fatalf("DeletePack = %v, %v", ok, err)
			}
		}
		for _, p := range inputs {
			if s := packState(t, c, p.ID); s != "deleted" {
				t.Errorf("input is %s, want deleted", s)
			}
			if n := count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ? AND reason = 'deleted'`, p.Key); n != 1 {
				t.Errorf("tombstones of %s = %d, want 1", p.Key, n)
			}
		}
		if s := packState(t, c, archive.ID); s != "staged" {
			t.Errorf("archive is %s, want staged", s)
		}
		if got := deleteCandidates(t, c, at); len(got) != 0 {
			t.Errorf("candidates after the deletion = %v, want none", got)
		}
		// A deleted input is deleted once: a retried deletion writes nothing.
		again := Deletable{Pack: inputs[0].ID, Capture: inputs[0].Capture, Key: inputs[0].Key}
		if ok, err := c.DeletePack(t.Context(), l, again, at, time.Hour); err != nil || ok {
			t.Errorf("a second DeletePack = %v, %v, want nothing done", ok, err)
		}
	})
}

// No input is deleted while another packs row names its key, or once its output is no longer staged or merged.
func TestDeletePackGuards(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture, inputs, archive := mergedScope(t, c, l, 0)
		now := deleteAt(t, c, archive)
		shared := inputs[0]
		lapse(t, c, `INSERT INTO packs (pack_id, capture_id, tool_id, hour, role, object_key, size, seq_start, format_minor, state, registered_at)
			VALUES (X'`+hexOf(newID(t))+`', X'`+hexOf(capture)+`', 'T1', 1, 'segment', '`+shared.Key+`', 1, 0, 1, 'staged', 0)`)
		d := Deletable{Pack: shared.ID, Capture: capture, Key: shared.Key}
		if ok, err := c.DeletePack(t.Context(), l, d, now, time.Hour); err != nil || ok {
			t.Errorf("DeletePack of a shared key = %v, %v, want refused", ok, err)
		}
		if s := packState(t, c, shared.ID); s != "merged" {
			t.Errorf("a refused deletion left the input %s", s)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM tombstones`); n != 0 {
			t.Errorf("a refused deletion wrote %d tombstones", n)
		}
		lapse(t, c, `UPDATE packs SET state = 'excluded' WHERE pack_id = X'`+hexOf(archive.ID)+`'`)
		if got := deleteCandidates(t, c, now); len(got) != 0 {
			t.Errorf("candidates of an excluded output = %v, want none", got)
		}
		other := Deletable{Pack: inputs[1].ID, Capture: capture, Key: inputs[1].Key}
		if ok, err := c.DeletePack(t.Context(), l, other, now, time.Hour); err != nil || ok {
			t.Errorf("DeletePack under an excluded output = %v, %v, want refused", ok, err)
		}
	})
}

func TestDeletePackUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		stale := compactorLease(t, c, "old")
		capture, inputs, archive := mergedScope(t, c, stale, 0)
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		d := Deletable{Pack: inputs[0].ID, Capture: capture, Key: inputs[0].Key}
		if _, err := c.DeletePack(t.Context(), stale, d, deleteAt(t, c, archive), time.Hour); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("DeletePack = %v, want ErrLeaseLost", err)
		}
		if s := packState(t, c, d.Pack); s != "merged" || count(t, c, `SELECT COUNT(*) FROM tombstones`) != 0 {
			t.Error("a refused deletion wrote")
		}
	})
}

// A pack's deadline is its hour's under the recorded retention, and for a merged input the earlier of that and its merge's delete_at.
func TestPackDeadline(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		_, inputs, archive := mergedScope(t, c, l, time.Hour)
		if _, ok, err := c.Deadline(t.Context(), archive.ID, time.Hour); err != nil || ok {
			t.Fatalf("Deadline before a retention record = %v, %v, want none", ok, err)
		}
		// A period long enough that hour 1 outlives the merge's delete_at.
		r := recordRetention(t, c, l, 1_000_000*time.Hour, c.now())
		hour := r.Deadline(1)
		if hour <= deleteAt(t, c, archive) {
			t.Fatal("the hour's deadline does not outlive delete_at")
		}
		got, ok, err := c.Deadline(t.Context(), archive.ID, time.Hour)
		if err != nil || !ok || got != hour {
			t.Errorf("archive deadline = %d, %v, %v, want its hour's %d", got, ok, err, hour)
		}
		got, ok, err = c.Deadline(t.Context(), inputs[0].ID, time.Hour)
		if want := deleteAt(t, c, archive); err != nil || !ok || got != want {
			t.Errorf("merged input deadline = %d, %v, %v, want %d", got, ok, err, want)
		}
		if _, _, err := c.Deadline(t.Context(), newID(t), time.Hour); !errors.Is(err, ErrUnknownPack) {
			t.Errorf("Deadline of an unknown pack = %v, want ErrUnknownPack", err)
		}
		d, err := c.PackDownload(t.Context(), inputs[0].ID, time.Hour)
		if want := deleteAt(t, c, archive); err != nil || !d.Known || d.Deadline != want || d.Key != inputs[0].Key ||
			d.Capture != inputs[0].Capture || d.State != "merged" {
			t.Errorf("PackDownload of a merged input = %+v, %v; want its key, capture, state and deadline %d", d, err, want)
		}
	})
}

// In a chain of merges, A and B into X, then X and a late S into Y, deleting X first leaves A and B candidates:
// X's records live on in Y, so an input whose output was deleted after its own merge is deleted all the same,
// and an input refused for a shared key stays a candidate, refused again every cycle.
func TestDeletePackAlongAChainOfMerges(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture, inputs, x := mergedScope(t, c, l, 0)
		late := segment(t, capture, 1, 20, 29)
		register(t, c, late, Registration{Evidence: &Evidence{}})
		y := archiveOf(t, capture, 1, x, late)
		m := mergeOf(snapshot(t, c, capture), y)
		if out := commit(t, c, l, m); out != Merged {
			t.Fatalf("second CommitMerge = %v", out)
		}
		now := deleteAt(t, c, y)
		if ok, err := c.DeletePack(t.Context(), l, Deletable{Pack: x.ID, Capture: capture, Key: x.Key}, now, time.Hour); err != nil || !ok {
			t.Fatalf("DeletePack of the intermediate archive = %v, %v", ok, err)
		}
		lapse(t, c, `INSERT INTO packs (pack_id, capture_id, tool_id, hour, role, object_key, size, seq_start, format_minor, state, registered_at)
			VALUES (X'`+hexOf(newID(t))+`', X'`+hexOf(capture)+`', 'T1', 7, 'segment', '`+inputs[0].Key+`', 1, 0, 1, 'staged', 0)`)
		for cycle := range 2 {
			got := deleteCandidates(t, c, now)
			ids := map[tracepack.UUID]bool{}
			for _, d := range got {
				ids[d.Pack] = true
			}
			if !ids[inputs[0].ID] || (cycle == 0) != ids[inputs[1].ID] {
				t.Fatalf("cycle %d: candidates %v, want the refused input every cycle and the other until deleted", cycle, got)
			}
			for _, d := range got {
				ok, err := c.DeletePack(t.Context(), l, d, now, time.Hour)
				if err != nil || ok != (d.Pack != inputs[0].ID) {
					t.Fatalf("cycle %d: DeletePack(%s) = %v, %v", cycle, d.Key, ok, err)
				}
			}
		}
	})
}

// A merge a version before delete_at committed, even after the migration, leaves it 0, the column's default:
// its inputs are deleted at its time plus the deciding process's D, never at once, and their deadline says so.
func TestDeletePackOfALegacyMerge(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		_, inputs, archive := mergedScope(t, c, l, 0)
		lapse(t, c, `UPDATE merges SET delete_at = 0`)
		var at int64
		if err := c.read.GetContext(t.Context(), &at, `SELECT at FROM merges WHERE output_pack_id = ?`, archive.ID[:]); err != nil {
			t.Fatal(err)
		}
		due := at + int64(time.Hour)
		if got, err := c.DeleteCandidates(t.Context(), due-1, time.Hour); err != nil || len(got) != 0 {
			t.Fatalf("candidates before at + D = %v, %v, want none", got, err)
		}
		if got, err := c.DeleteCandidates(t.Context(), due, time.Hour); err != nil || len(got) != 2 {
			t.Fatalf("candidates at at + D = %v, %v, want both inputs", got, err)
		}
		recordRetention(t, c, l, 1_000_000*time.Hour, c.now())
		if got, ok, err := c.Deadline(t.Context(), inputs[0].ID, time.Hour); err != nil || !ok || got != due {
			t.Errorf("deadline = %d, %v, %v, want %d", got, ok, err, due)
		}
	})
}
