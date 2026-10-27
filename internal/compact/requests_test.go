package compact

import (
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

func (f *fixture) request(t *testing.T, kind string, capture tracepack.UUID) tracepack.UUID {
	t.Helper()
	h := hour
	id, err := f.c.RequestCompaction(t.Context(), kind, capture, &h, "op")
	if err != nil {
		t.Fatal(err)
	}

	return id
}

func (f *fixture) status(t *testing.T, id tracepack.UUID) (string, string) {
	t.Helper()
	state, outcome, err := f.c.RequestStatus(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}

	return state, outcome
}

// An operator's merge request is executed at the next cycle, the quiet period aside, and ends done or failed with its outcome;
// a close request waits for the compactor that writes closes.
func TestMergeRequests(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		// Inside the quiet period: only a request merges.
		f.r.Now = func() time.Time { return time.Unix(0, (hour+1)*nsPerHour) }
		ready := f.admit(t)
		f.stage(t, segment(t, ready, []uint64{0, 1}, false))
		conflicted := f.admit(t)
		f.stage(t, segmentOf(t, conflicted, hour, recordsAt(hour, 0, 1, 2), noAdjust))
		f.stage(t, segmentOf(t, conflicted, hour, recordsAt(hour, 2, 3), noAdjust))
		merge, blocked, empty := f.request(t, catalog.RequestMerge, ready), f.request(t, catalog.RequestMerge, conflicted), f.request(t, catalog.RequestMerge, f.admit(t))
		closing := f.request(t, catalog.RequestClose, ready)
		rep, err := f.r.Requests(t.Context(), f.lease)
		if err != nil {
			t.Fatal(err)
		}
		if rep.Done != 1 || rep.Failed != 2 || rep.Pending != 1 {
			t.Errorf("Requests = %+v, want one done, two failed, one pending", rep)
		}
		for id, want := range map[tracepack.UUID][2]string{
			merge:   {"done", "merged"},
			blocked: {"failed", "a conflict touches the scope"},
			empty:   {"failed", "the scope has no staged pack"},
			closing: {"pending", ""},
		} {
			if state, outcome := f.status(t, id); state != want[0] || outcome != want[1] {
				t.Errorf("request %s = %s, %q; want %v", id, state, outcome, want)
			}
		}
		// A request for a scope since settled ends done.
		again := f.request(t, catalog.RequestMerge, ready)
		if _, err := f.r.Requests(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		if state, outcome := f.status(t, again); state != "done" || outcome != "settled" {
			t.Errorf("a request for a settled scope = %s, %q; want done, settled", state, outcome)
		}
	})
}

// A merge request whose merge fails on conflicting records ends failed, with the conflict recorded.
func TestMergeRequestOnConflictingRecords(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		f.stage(t, segmentOf(t, capture, hour, recordsAt(hour, 0, 1, 2), noAdjust))
		// The second segment holds a differing seq 2, but its registration names 3 only: no suspect is raised.
		changed := recordsAt(hour, 2, 3)
		changed[0].Payload = append([]byte(nil), changed[0].Payload...)
		changed[0].Payload[0] ^= 0xff
		b := segmentOf(t, capture, hour, changed, noAdjust)
		f.put(t, b.key, b.data)
		p, reason, err := f.r.read(t.Context(), store.ObjectInfo{Key: b.key, Size: int64(len(b.data))})
		if err != nil || reason != "" {
			t.Fatalf("read = %q, %v", reason, err)
		}
		p.Ranges = []catalog.Range{{SeqFrom: 3, SeqTo: 3, TSFrom: p.Ranges[0].TSTo, TSTo: p.Ranges[0].TSTo}}
		if out, err := c.RegisterPack(t.Context(), p, catalog.Registration{Evidence: &catalog.Evidence{}, HorizonHour: f.horizon}); err != nil || out != catalog.Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		id := f.request(t, catalog.RequestMerge, capture)
		if rep, err := f.r.Requests(t.Context(), f.lease); err != nil || rep.Failed != 1 {
			t.Fatalf("Requests = %+v, %v; want one failed", rep, err)
		}
		if state, outcome := f.status(t, id); state != "failed" || outcome != "the inputs hold conflicting records" {
			t.Errorf("request = %s, %q", state, outcome)
		}
	})
}
