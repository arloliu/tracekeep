package catalog

import (
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
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

// closePack is the close pack of capture's intent, in hour 2.
func closePack(t *testing.T, capture tracepack.UUID, id tracepack.UUID) *Pack {
	t.Helper()

	return &Pack{
		ID: id, Capture: capture, ToolID: "T1", Hour: 2, Role: RoleClose, Key: "staging/close-" + id.String(), Size: 50,
		SeqStart: closeSentinel, FormatMinor: 2,
	}
}

// intended closes a capture registered up to seq 100 by the operator, and returns it with its intent.
func intended(t *testing.T, c *Catalog, l Lease) (tracepack.UUID, *Intent) {
	t.Helper()
	capture := admitted(t, c)
	register(t, c, segment(t, capture, 1, 0, 100), Registration{Evidence: &Evidence{}})
	in := intentOf(t, capture, CloseOperator, c.now())
	if out := closeIntent(t, c, l, in); out != Intended {
		t.Fatalf("CloseIntent = %v", out)
	}

	return capture, in
}

func publish(t *testing.T, c *Catalog, l Lease, p *Pack, horizon int64) string {
	t.Helper()
	state, err := c.PublishClose(t.Context(), l, p, horizon)
	if err != nil {
		t.Fatal(err)
	}

	return state
}

// A close is published as a pack holding no range: coverage and the highest seq stay, its scope gains it with an event,
// and completeness reads its crash gap.
func TestPublishClose(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "h")
		capture, in := intended(t, c, l)
		p := closePack(t, capture, in.PackID)
		if state := publish(t, c, l, p, 0); state != "published" {
			t.Fatalf("PublishClose = %s, want published", state)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM packs WHERE pack_id = ? AND role = 'close'`, p.ID[:]); n != 1 {
			t.Errorf("close packs = %d, want 1", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM pack_ranges WHERE pack_id = ?`, p.ID[:]); n != 0 {
			t.Errorf("close ranges = %d, want none", n)
		}
		got, err := c.Capture(t.Context(), capture)
		if err != nil || got.HighestSeq != 100 || got.State != StateClosed {
			t.Errorf("capture = %+v, %v; want closed at highest seq 100", got, err)
		}
		if evs := events(t, c); evs[len(evs)-1] != "created@2" {
			t.Errorf("events = %v, want the close's scope created last", evs)
		}
		cp, err := c.Completeness(t.Context(), capture)
		if err != nil || len(cp.CrashGaps) != 1 {
			t.Errorf("completeness = %+v, %v; want the close's crash gap", cp, err)
		}
		if len(pendingCloses(t, c)) != 0 {
			t.Error("the published intent is still pending")
		}
		if state := publish(t, c, l, p, 0); state != "published" {
			t.Errorf("a second PublishClose = %s, want published as it was", state)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM packs WHERE pack_id = ?`, p.ID[:]); n != 1 {
			t.Errorf("packs = %d after a second publication, want 1", n)
		}
	})
}

// The reconciler adopting the close's object first: the publication finds it registered and publishes the intent.
func TestPublishCloseAdoptedFirst(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "h")
		capture, in := intended(t, c, l)
		p := closePack(t, capture, in.PackID)
		if out := register(t, c, p, Registration{Lease: &l, Adopted: true}); out != Registered {
			t.Fatalf("adoption = %v", out)
		}
		evs := events(t, c)
		if state := publish(t, c, l, p, 0); state != "published" {
			t.Fatalf("PublishClose = %s, want published", state)
		}
		if got := events(t, c); !slices.Equal(got, evs) {
			t.Errorf("events = %v, want %v: the adoption emitted the scope's", got, evs)
		}
	})
}

// A close the horizon guard refuses tombstones its key and is abandoned, never retried; a tombstoned key abandons it too.
func TestPublishCloseAbandoned(t *testing.T) {
	t.Parallel()
	for _, beyond := range []bool{true, false} {
		engines(t, func(t *testing.T, c *Catalog) {
			l := compactorLease(t, c, "h")
			capture, in := intended(t, c, l)
			p := closePack(t, capture, in.PackID)
			horizon := int64(0)
			if beyond {
				horizon = 3
			} else if _, err := c.write.ExecContext(t.Context(), `INSERT INTO tombstones (object_key, reason, at) VALUES (?, 'beyond-retention', 1)`, p.Key); err != nil {
				t.Fatal(err)
			}
			if state := publish(t, c, l, p, horizon); state != "abandoned" {
				t.Fatalf("PublishClose = %s, want abandoned", state)
			}
			if got, err := c.KeyStanding(t.Context(), p.Key); err != nil || got != KeyTombstoned {
				t.Errorf("the key's standing = %v, %v; want tombstoned", got, err)
			}
			if n := count(t, c, `SELECT COUNT(*) FROM packs WHERE pack_id = ?`, p.ID[:]); n != 0 {
				t.Errorf("packs = %d, want none", n)
			}
			if len(pendingCloses(t, c)) != 0 {
				t.Error("the abandoned intent is still pending")
			}
			if state := publish(t, c, l, p, 0); state != "abandoned" {
				t.Errorf("a retried PublishClose = %s, want abandoned as it was", state)
			}
		})
	}
}

func TestPublishCloseUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		stale := compactorLease(t, c, "old")
		capture, in := intended(t, c, stale)
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		if _, err := c.PublishClose(t.Context(), stale, closePack(t, capture, in.PackID), 0); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("PublishClose = %v, want ErrLeaseLost", err)
		}
		if len(pendingCloses(t, c)) != 1 {
			t.Error("the intent is no longer pending")
		}
	})
}

// A close request is decided under the capture row: open, it is left for the compactor to execute;
// closed, it ends done; stopped or never admitted, it ends failed.
func TestDecideCloseRequest(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "h")
		request := func(capture tracepack.UUID) Request {
			t.Helper()
			id, err := c.RequestCompaction(t.Context(), RequestClose, capture, nil, "op")
			if err != nil {
				t.Fatal(err)
			}

			return Request{ID: id, Kind: RequestClose, Capture: capture}
		}
		open := admitted(t, c)
		if got, err := c.DecideRequest(t.Context(), l, request(open)); err != nil || got != Mergeable {
			t.Errorf("an open capture = %q, %v; want it left to execute", got, err)
		}
		closed, _ := intended(t, c, l)
		if got, err := c.DecideRequest(t.Context(), l, request(closed)); err != nil || got != "closed" {
			t.Errorf("a closed capture = %q, %v", got, err)
		}
		stopped := admitted(t, c)
		p := segment(t, stopped, 1, 0, 9)
		p.Stop = true
		register(t, c, p, Registration{Evidence: &Evidence{}})
		if got, err := c.DecideRequest(t.Context(), l, request(stopped)); err != nil || got != "the capture is not open" {
			t.Errorf("a stopped capture = %q, %v", got, err)
		}
		if got, err := c.DecideRequest(t.Context(), l, request(newID(t))); err != nil || got != "the capture is not open" {
			t.Errorf("an unknown capture = %q, %v", got, err)
		}
	})
}

// A registration racing a close intent waits on the capture row and lands after it: staged, the capture stays closed,
// and the intent keeps the highest seq it fixed, so the close's bytes do not change.
func TestRegistrationAfterACloseIntent(t *testing.T) {
	t.Parallel()
	for _, stop := range []bool{false, true} {
		engines(t, func(t *testing.T, c *Catalog) {
			capture := admitted(t, c)
			register(t, c, segment(t, capture, 1, 0, 100), Registration{Evidence: &Evidence{}})
			l := compactorLease(t, c, "h")
			held, release := holdAt(c, hookLocked)
			in := intentOf(t, capture, CloseOperator, c.now())
			var out CloseOutcome
			intended := run(func() { out, _ = c.CloseIntent(t.Context(), l, in) })
			<-held
			waits := c.write.Stats().WaitCount
			late := segment(t, capture, 1, 101, 110)
			late.Stop = stop
			var reg PackOutcome
			registered := run(func() { reg, _ = c.RegisterPack(t.Context(), late, Registration{Evidence: &Evidence{}}) })
			waitBlocked(t, c, waits, "INSERT INTO captures", 1)
			close(release)
			<-intended
			<-registered
			if out != Intended || reg != Registered {
				t.Fatalf("intent = %v, registration = %v; want both to land", out, reg)
			}
			got, err := c.Capture(t.Context(), capture)
			if err != nil || got.State != StateClosed || got.HighestSeq != 110 || got.StopSeen != stop {
				t.Errorf("capture = %+v, %v; want closed at highest seq 110, stop %v", got, err, stop)
			}
			closes := pendingCloses(t, c)
			if len(closes) != 1 || *closes[0].HighestSeq != 100 {
				t.Errorf("pending closes = %+v; want the intent's highest seq 100", closes)
			}
		})
	}
}

// A registration that commits first refuses the inactivity intent judged before it: the capture is no longer silent for I.
func TestRegistrationBeforeACloseIntent(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		register(t, c, segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		judged := c.now() + int64(inactivity) + 1
		if got := candidates(t, c, judged); len(got) != 1 {
			t.Fatalf("candidates = %v, want the silent capture", got)
		}
		// The registration lands after the scan, at a time the intent's judgement reaches.
		lapse(t, c, "UPDATE captures SET last_registered_at = "+itoa(judged-1)+" WHERE state = 'open'")
		if out := closeIntent(t, c, l, intentOf(t, capture, CloseInactivity, judged)); out != CloseIneligible {
			t.Errorf("CloseIntent after a registration = %v, want ineligible", out)
		}
		if captureState(t, c, capture) != StateOpen {
			t.Error("the capture was closed")
		}
	})
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// The successor trigger needs both silences: the capture's own for Q, and the successor's registration at least Q old.
func TestSuccessorNeedsBothSilences(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "h")
		instance := newID(t)
		old := admittedAs(t, c, instance, 100)
		register(t, c, segment(t, old, 1, 0, 9), Registration{Evidence: &Evidence{}})
		next := admittedAs(t, c, instance, 200)
		register(t, c, segment(t, next, 1, 0, 9), Registration{Evidence: &Evidence{}})
		now := c.now()
		// The capture silent for long, its successor registered just now: not yet Q old.
		lapse(t, c, "UPDATE captures SET last_registered_at = "+itoa(now-int64(time.Hour))+" WHERE capture_id = x'"+hexOf(old)+"'")
		if got := candidates(t, c, now+int64(quiet)/2); len(got) != 0 {
			t.Errorf("candidates with a successor registered under Q ago = %v, want none", got)
		}
		if out := closeIntent(t, c, l, intentOf(t, old, CloseSuccessor, now+int64(quiet)/2)); out != CloseIneligible {
			t.Errorf("CloseIntent with a successor registered under Q ago = %v, want ineligible", out)
		}
		// The successor Q old, the capture registered within Q: refused too.
		later := now + int64(quiet) + 1
		lapse(t, c, "UPDATE captures SET last_registered_at = "+itoa(later-1)+" WHERE capture_id = x'"+hexOf(old)+"'")
		if out := closeIntent(t, c, l, intentOf(t, old, CloseSuccessor, later)); out != CloseIneligible {
			t.Errorf("CloseIntent of a capture registered within Q = %v, want ineligible", out)
		}
	})
}

func hexOf(id tracepack.UUID) string { return hex.EncodeToString(id[:]) }
