package catalog

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

const (
	inactivity = 2 * time.Hour
	quiet      = 15 * time.Minute
)

// admittedAs admits a capture of the recorder instance at origin.
func admittedAs(t *testing.T, c *Catalog, instance tracepack.UUID, origin int64) tracepack.UUID {
	t.Helper()
	gate(t, c)
	id := newID(t)
	d := &Descriptor{Bytes: []byte("d"), ToolID: "T1", RecorderInstanceID: instance, Device: "eqp", OriginNS: origin}
	if _, err := c.Admit(t.Context(), start(id), d); err != nil {
		t.Fatal(err)
	}

	return id
}

func candidates(t *testing.T, c *Catalog, now int64) []CloseCandidate {
	t.Helper()
	got, err := c.CloseCandidates(t.Context(), now, inactivity, quiet)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(got, func(a, b CloseCandidate) int { return compareUUID(a.Capture, b.Capture) })

	return got
}

func compareUUID(a, b tracepack.UUID) int {
	for i := range a {
		if a[i] != b[i] {
			return int(a[i]) - int(b[i])
		}
	}

	return 0
}

// intentOf is the intent of closing capture by trigger at now.
func intentOf(t *testing.T, capture tracepack.UUID, trigger string, now int64) *Intent {
	t.Helper()

	return &Intent{
		Capture: capture, Trigger: trigger, TriggeredAt: now, Now: now, Inactivity: inactivity, Quiet: quiet,
		PackID: newID(t), WriterTime: now, Meta: []byte("meta"), ToleranceNS: 1000,
	}
}

func closeIntent(t *testing.T, c *Catalog, l Lease, in *Intent) CloseOutcome {
	t.Helper()
	out, err := c.CloseIntent(t.Context(), l, in)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

func captureState(t *testing.T, c *Catalog, capture tracepack.UUID) string {
	t.Helper()
	got, err := c.Capture(t.Context(), capture)
	if err != nil {
		t.Fatal(err)
	}

	return got.State
}

// The inactivity trigger: an open capture without stop and without a registration for I, its first sighting standing in
// for a capture never registered; a stopped, closed or recently registered one is no candidate.
func TestCloseCandidatesByInactivity(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		silent := admitted(t, c)
		register(t, c, segment(t, silent, 1, 0, 9), Registration{Evidence: &Evidence{}})
		never := admitted(t, c)
		stopped := admitted(t, c)
		p := segment(t, stopped, 1, 0, 9)
		p.Stop = true
		register(t, c, p, Registration{Evidence: &Evidence{}})
		now := c.now()
		if got := candidates(t, c, now); len(got) != 0 {
			t.Errorf("candidates now = %v, want none", got)
		}
		later := now + int64(inactivity) + 1
		want := []CloseCandidate{{Capture: silent, Trigger: CloseInactivity}, {Capture: never, Trigger: CloseInactivity}}
		slices.SortFunc(want, func(a, b CloseCandidate) int { return compareUUID(a.Capture, b.Capture) })
		if got := candidates(t, c, later); !slices.Equal(got, want) {
			t.Errorf("candidates after I = %v, want %v", got, want)
		}
	})
}

// The successor trigger: a later capture of the recorder instance with a registration at least Q old, and none of the capture's own for Q;
// a capture of another instance, or an earlier one, triggers nothing, and the trigger time is the successor's origin.
func TestCloseCandidatesBySuccessor(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		instance := newID(t)
		old := admittedAs(t, c, instance, 100)
		register(t, c, segment(t, old, 1, 0, 9), Registration{Evidence: &Evidence{}})
		other := admittedAs(t, c, newID(t), 300)
		register(t, c, segment(t, other, 1, 0, 9), Registration{Evidence: &Evidence{}})
		next := admittedAs(t, c, instance, 200)
		now := c.now()
		if got := candidates(t, c, now); len(got) != 0 {
			t.Errorf("candidates before the successor registered = %v, want none", got)
		}
		register(t, c, segment(t, next, 1, 0, 9), Registration{Evidence: &Evidence{}})
		if got := candidates(t, c, c.now()+int64(quiet)/2); len(got) != 0 {
			t.Errorf("candidates before Q passed = %v, want none", got)
		}
		got := candidates(t, c, c.now()+int64(quiet)+1)
		if want := []CloseCandidate{{Capture: old, Trigger: CloseSuccessor, TriggeredAt: 200}}; !slices.Equal(got, want) {
			t.Errorf("candidates after Q = %+v, want %+v", got, want)
		}
	})
}

func TestCloseIntent(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		register(t, c, segment(t, capture, 1, 0, 100), Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		now := c.now() + int64(inactivity) + 1
		in := intentOf(t, capture, CloseInactivity, now)
		if out := closeIntent(t, c, l, in); out != Intended {
			t.Fatalf("CloseIntent = %v, want intended", out)
		}
		if s := captureState(t, c, capture); s != StateClosed {
			t.Errorf("capture is %s, want closed", s)
		}
		closes := pendingCloses(t, c)
		if len(closes) != 1 {
			t.Fatalf("pending closes = %+v, want one", closes)
		}
		got := closes[0]
		if got.Capture != capture || got.Trigger != CloseInactivity || got.PackID != in.PackID || got.WriterTime != now ||
			string(got.Meta) != "meta" || got.HighestSeq == nil || *got.HighestSeq != 100 || got.HighestSeqTS == nil || *got.HighestSeqTS != 100 ||
			got.ToolID != "T1" || got.TriggeredAt != now {
			t.Errorf("pending close = %+v", got)
		}
		gap := count(t, c, `SELECT COUNT(*) FROM capture_closes WHERE capture_id = ? AND gap_start = ? AND gap_end = ?`, capture[:], 100-1000, now+1000)
		if gap != 1 {
			t.Error("the intent's crash gap is not the highest seq's time minus the tolerance to the trigger plus it")
		}
		// A second intent finds the capture closed.
		if out := closeIntent(t, c, l, intentOf(t, capture, CloseOperator, now)); out != CloseIneligible {
			t.Errorf("a second CloseIntent = %v, want ineligible", out)
		}
	})
}

// The intent re-reads the capture's eligibility under its row, whatever the scan said:
// a registration since, or a stop, refuses it and writes nothing; the operator's trigger needs only an open capture without stop.
func TestCloseIntentRechecksEligibility(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "h")
		capture := admitted(t, c)
		register(t, c, segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		// Judged at a time before I has passed since the registration: refused, nothing written.
		rev := captureRevision(t, c, capture)
		if out := closeIntent(t, c, l, intentOf(t, capture, CloseInactivity, c.now())); out != CloseIneligible {
			t.Errorf("CloseIntent inside I = %v, want ineligible", out)
		}
		if out := closeIntent(t, c, l, intentOf(t, capture, CloseSuccessor, c.now()+int64(quiet)+1)); out != CloseIneligible {
			t.Errorf("CloseIntent by a successor that does not exist = %v, want ineligible", out)
		}
		if got := captureRevision(t, c, capture); got != rev || captureState(t, c, capture) != StateOpen || len(pendingCloses(t, c)) != 0 {
			t.Errorf("a refused intent wrote: revision %d (want %d), state %s", got, rev, captureState(t, c, capture))
		}
		stopped := admitted(t, c)
		p := segment(t, stopped, 1, 0, 9)
		p.Stop = true
		register(t, c, p, Registration{Evidence: &Evidence{}})
		if out := closeIntent(t, c, l, intentOf(t, stopped, CloseOperator, c.now())); out != CloseIneligible {
			t.Errorf("CloseIntent of a stopped capture = %v, want ineligible", out)
		}
		if out := closeIntent(t, c, l, intentOf(t, capture, CloseOperator, c.now())); out != Intended {
			t.Errorf("the operator's CloseIntent = %v, want intended", out)
		}
	})
}

func TestCloseIntentUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		if _, err := c.CloseIntent(t.Context(), stale, intentOf(t, capture, CloseOperator, c.now())); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("CloseIntent = %v, want ErrLeaseLost", err)
		}
		if captureState(t, c, capture) != StateOpen || len(pendingCloses(t, c)) != 0 {
			t.Error("a refused intent wrote")
		}
	})
}

// An operator's close request is ended by its intent's commit.
func TestCloseIntentEndsItsRequest(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		id, err := c.RequestCompaction(t.Context(), RequestClose, capture, nil, "op")
		if err != nil {
			t.Fatal(err)
		}
		l := compactorLease(t, c, "h")
		in := intentOf(t, capture, CloseOperator, c.now())
		in.Request = &id
		if out := closeIntent(t, c, l, in); out != Intended {
			t.Fatalf("CloseIntent = %v", out)
		}
		if state, outcome, err := c.RequestStatus(t.Context(), id); err != nil || state != "done" || outcome != "closed" {
			t.Errorf("request = %s, %q, %v; want done, closed", state, outcome, err)
		}
	})
}

func pendingCloses(t *testing.T, c *Catalog) []Close {
	t.Helper()
	got, err := c.PendingCloses(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return got
}
