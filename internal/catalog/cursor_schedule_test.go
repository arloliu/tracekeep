package catalog

import (
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// hourOne is a window holding hour 1, where the schedules below register.
var hourOne = window(hourNS(1), hourNS(2))

// registerIn registers a segment of capture in hour 1 holding seq, which emits the scope's created or reopened event.
func registerIn(t *testing.T, c *Catalog, capture tracepack.UUID, seq uint64) (PackOutcome, error) {
	t.Helper()

	return c.RegisterPack(t.Context(), segment(t, capture, 1, seq, seq), Registration{Evidence: &Evidence{}})
}

func eventSeqs(evs []ScopeEvent) []int64 {
	out := make([]int64, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Seq)
	}

	return out
}

// The gate's event cursor with an uncommitted earlier transaction:
// an event allocated and not yet committed is below no watermark a reader sees,
// a later emitter waits on the counters row behind it, and a reader that has seen the watermark reads both, in seq order, once they commit.
func TestCursorWithAnUncommittedEarlierEvent(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		first := described(t, c, spec{instance: newID(t), origin: 1})
		second := described(t, c, spec{instance: newID(t), origin: 1})
		w0 := listPacks(t, c, hourOne).Cursor
		held, release := holdAt(c, hookBeforeCommit)
		var err1, err2 error
		a := run(func() { _, err1 = registerIn(t, c, first, 0) })
		<-held
		listing := listPacks(t, c, hourOne)
		if listing.Cursor != w0 || !listing.Hours[0].NoData {
			t.Errorf("listing during the uncommitted event: cursor %+v, hour %+v; want %+v and no data", listing.Cursor, listing.Hours[0], w0)
		}
		if got := changes(t, c, w0, hourOne, 10); len(got.Events) != 0 || got.Next != w0 {
			t.Errorf("changes during the uncommitted event = %+v", got)
		}
		waits := c.write.Stats().WaitCount
		b := run(func() { _, err2 = registerIn(t, c, second, 0) })
		waitBlocked(t, c, waits, "UPDATE counters SET value", 1)
		close(release)
		<-a
		<-b
		if err1 != nil || err2 != nil {
			t.Fatalf("registrations = %v, %v", err1, err2)
		}
		got := changes(t, c, w0, hourOne, 10)
		if len(got.Events) != 2 || got.Events[0].Capture != first || got.Events[1].Capture != second ||
			!slices.Equal(eventSeqs(got.Events), []int64{w0.Seq + 1, w0.Seq + 2}) || got.Next.Seq != w0.Seq+2 {
			t.Errorf("changes after both commits = %+v, want the held event, then the waiting one", got)
		}
	})
}

// An event committed between a listing's watermark and its rows lies above the watermark the listing returns,
// so the client replays it whether or not the listing's rows reflected it.
func TestEventCommittedDuringAListing(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := described(t, c, spec{instance: newID(t), origin: 1})
		w0 := listPacks(t, c, hourOne).Cursor
		held, release := holdAt(c, hookWatermarkRead)
		var listing Listing
		var lerr error
		listed := run(func() { listing, lerr = c.ListPacks(t.Context(), hourOne, readQ) })
		<-held
		if out, err := registerIn(t, c, capture, 0); err != nil || out != Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		close(release)
		<-listed
		if lerr != nil {
			t.Fatal(lerr)
		}
		if listing.Cursor != w0 {
			t.Errorf("listing cursor %+v, want the watermark read first, %+v", listing.Cursor, w0)
		}
		got := changes(t, c, listing.Cursor, hourOne, 10)
		if len(got.Events) != 1 || got.Events[0].Event != "created" || got.Events[0].Seq <= listing.Cursor.Seq {
			t.Errorf("changes after the listing = %+v, want the created event above its watermark", got)
		}
	})
}

// The gate's VE read: a settled hour listed, then reopened by a registration;
// the client following the cursor from the listing's watermark learns of the reopen with the scope's new revision.
func TestSettledHourLearnsOfAReopen(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := described(t, c, spec{instance: newID(t), origin: 1})
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		if out := commit(t, c, compactorLease(t, c, "h"), mergeOf(snapshot(t, c, capture), archiveOf(t, capture, 1, in))); out != Merged {
			t.Fatalf("CommitMerge = %v", out)
		}
		listing := listPacks(t, c, hourOne)
		settled := listing.Hours[0].Scopes
		if len(settled) != 1 || settled[0].State != "settled" {
			t.Fatalf("hour 1 = %+v, want its settled scope", listing.Hours[0])
		}
		if out, err := registerIn(t, c, capture, 10); err != nil || out != Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		got := changes(t, c, listing.Cursor, hourOne, 10)
		if len(got.Events) != 1 || got.Events[0].Event != "reopened" || got.Events[0].Hour != 1 || got.Events[0].Revision <= settled[0].Revision {
			t.Fatalf("changes = %+v, want the reopen above revision %d", got, settled[0].Revision)
		}
		if s := listPacks(t, c, hourOne).Hours[0].Scopes[0]; s.State != "reopened" || s.Revision != got.Events[0].Revision {
			t.Errorf("relisted scope %+v, want reopened at the event's revision", s)
		}
	})
}

// The gate's cursor with pruning: a reader that read the watermark before a prune committed reads its page from its snapshot,
// every event it reports present; a reader after it whose cursor lies in the pruned prefix reads stale,
// re-lists and continues from the listing's cursor without missing a later event.
func TestPruningRacingAReader(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := described(t, c, spec{instance: newID(t), origin: 1})
		for hour := range int64(3) {
			if out := register(t, c, segment(t, capture, hour+1, uint64(hour), uint64(hour)), Registration{Evidence: &Evidence{}}); out != Registered {
				t.Fatalf("RegisterPack = %v", out)
			}
		}
		exec(t, c, `UPDATE scope_events SET at = seq * 10`) // seqs 1–3 at 10, 20 and 30
		l := compactorLease(t, c, "h")
		all := Selector{ToolID: "T1", From: 0, To: hourNS(9)}
		held, release := holdAt(c, hookWatermarkRead)
		var page Changes
		var rerr error
		read := run(func() { page, rerr = c.ListScopeChanges(t.Context(), Cursor{}, all, 10) })
		<-held
		if n, err := c.PruneEvents(t.Context(), l, 15, 40); err != nil || n != 2 {
			t.Fatalf("PruneEvents = %d, %v; want seqs 1 and 2 pruned", n, err)
		}
		close(release)
		<-read
		if rerr != nil || page.Stale || !slices.Equal(eventSeqs(page.Events), []int64{1, 2, 3}) {
			t.Errorf("the page read before the prune = %+v, %v; want every event of its snapshot", page, rerr)
		}
		if got := changes(t, c, Cursor{}, all, 10); !got.Stale {
			t.Fatalf("a cursor in the pruned prefix read %+v, want stale", got)
		}
		relisted := listPacks(t, c, all).Cursor
		if out := register(t, c, segment(t, capture, 4, 3, 3), Registration{Evidence: &Evidence{}}); out != Registered {
			t.Fatalf("RegisterPack = %v", out)
		}
		if got := changes(t, c, relisted, all, 10); got.Stale || len(got.Events) != 1 || got.Events[0].Hour != 4 {
			t.Errorf("after the re-list = %+v, want the later event", got)
		}
		// An operator's truncation changes the epoch: the old cursor is stale, and the re-listed one continues.
		exec(t, c, `UPDATE counters SET epoch = epoch + 1 WHERE name = 'scope_events'`)
		if got := changes(t, c, relisted, all, 10); !got.Stale {
			t.Error("a cursor of the old epoch was not stale")
		}
		if got := changes(t, c, listPacks(t, c, all).Cursor, all, 10); got.Stale {
			t.Error("the re-listed cursor of the new epoch was stale")
		}
	})
}

// A lease takeover waits on the leases row a prune holds, so the prune commits under the lease it checked.
func TestLeaseTakeoverAgainstAPrune(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "old")
		eventsAt(t, c, 10, 70)
		held, release := holdAt(c, hookBeforeCommit)
		var n int
		var perr, lerr error
		pruned := run(func() { n, perr = c.PruneEvents(t.Context(), l, 25, 80) })
		<-held
		waits := c.write.Stats().WaitCount
		leaseRowHeld(t, c, release)
		lapsed := run(func() {
			_, lerr = c.write.ExecContext(t.Context(), `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		})
		waitBehindLeaseRow(t, c, waits, release)
		close(release)
		<-pruned
		<-lapsed
		if perr != nil || lerr != nil || n != 1 || lowWater(t, c) != 2 {
			t.Errorf("prune = %d, %v; lapse %v; low_water %d; want the prune committed first", n, perr, lerr, lowWater(t, c))
		}
	})
}
