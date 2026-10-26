package catalog

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func begin(t *testing.T, c *Catalog, d *Identity, horizon int64) Begun {
	t.Helper()
	b, err := c.BeginParking(t.Context(), d, "unknown-capture", "", horizon)
	if err != nil {
		t.Fatal(err)
	}

	return b
}

// Begin inserts or finds the version's row and the delivery's copy row under the capture row (parking.md step (1)):
// a pending row for an unknown capture, the horizon guard for a new version of an admitted one,
// a resolved version returned as it is, and an order applied.
func TestBeginParking(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()

		// An unknown capture: a pending row, no guard however old the record.
		d := identity(newID(t), 3, 1)
		if b := begin(t, c, d, 100); b != (Begun{State: VersionParking}) {
			t.Fatalf("BeginParking = %+v, want parking", b)
		}
		if got, err := c.Capture(ctx, d.Version.Capture); err != nil || got.State != StatePending {
			t.Errorf("capture = %+v, %v; want pending", got, err)
		}
		again := *d
		again.StreamSeq = 2
		if b := begin(t, c, &again, 100); b.State != VersionParking {
			t.Errorf("BeginParking of another message = %+v, want the version found parking", b)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked`); n != 1 {
			t.Errorf("%d version rows, want 1", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked_copies WHERE state = 'waiting'`); n != 2 {
			t.Errorf("%d copy rows, want one per message", n)
		}

		// An admitted capture: a new version past the horizon is not parked, and nothing is written.
		capture := admitted(t, c)
		old := identity(capture, 1, 1)
		if b := begin(t, c, old, 6); b != (Begun{BeyondRetention: true}) {
			t.Errorf("BeginParking past the horizon = %+v, want beyond retention", b)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked WHERE capture_id = ?`, capture[:]); n != 0 {
			t.Errorf("a version past the horizon was parked")
		}
		if b := begin(t, c, old, 5); b.State != VersionParking {
			t.Errorf("BeginParking within the horizon = %+v, want parking", b)
		}

		// A resolved version publishes nothing, its copy row recorded; an order is applied.
		staged := identity(capture, 2, 20)
		park(t, c, staged, "staged")
		if b := begin(t, c, staged, 0); b.State != VersionStaged {
			t.Errorf("BeginParking of a staged version = %+v", b)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked_copies WHERE seq = 2`); n != 1 {
			t.Errorf("the staged version's message has %d copy rows, want 1", n)
		}
		order(t, c, capture, "a1")
		covered := identity(capture, 4, 9)
		if b := begin(t, c, covered, 0); b.State != VersionTerminated {
			t.Errorf("BeginParking of a covered delivery = %+v, want terminated", b)
		}
		if state, by := versionState(t, c, covered); state != "terminated" || by != "a1" {
			t.Errorf("covered version = %s by %q", state, by)
		}
	})
}

// A version's state moves forward only: parking → parked → evicted-hour → terminated, never back.
func TestParkingStatesMoveForward(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		d := identity(newID(t), 3, 1)
		v := &d.Version
		if _, err := c.AdvanceParked(ctx, v); !errors.Is(err, ErrUnknownVersion) {
			t.Errorf("AdvanceParked of an unknown version = %v", err)
		}
		begin(t, c, d, 0)
		if s, err := c.EvictParked(ctx, v); err != nil || s != VersionParking {
			t.Errorf("EvictParked of a parking version = %v, %v; want it left parking", s, err)
		}
		for range 2 {
			if s, err := c.AdvanceParked(ctx, v); err != nil || s != VersionParked {
				t.Errorf("AdvanceParked = %v, %v; want parked", s, err)
			}
		}
		if s, err := c.EvictParked(ctx, v); err != nil || s != VersionEvictedHour {
			t.Errorf("EvictParked = %v, %v; want evicted-hour", s, err)
		}
		if s, err := c.AdvanceParked(ctx, v); err != nil || s != VersionEvictedHour {
			t.Errorf("AdvanceParked of an evicted version = %v, %v; want no regress", s, err)
		}
		if n, err := c.TerminateParked(ctx, v.Capture, nil, "op", "p1"); err != nil || n != 1 {
			t.Fatalf("TerminateParked = %d, %v", n, err)
		}
		for _, step := range []func() (VersionState, error){
			func() (VersionState, error) { return c.AdvanceParked(ctx, v) },
			func() (VersionState, error) { return c.EvictParked(ctx, v) },
		} {
			if s, err := step(); err != nil || s != VersionTerminated {
				t.Errorf("a step after the termination = %v, %v; want terminated", s, err)
			}
		}
	})
}

// The parked consumer's copy row is inserted or found; an acknowledged copy is marked acked, forward only,
// and the version's acked_at is set once no known copy waits.
func TestCopyRows(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		orig := identity(newID(t), 3, 1)
		cp := *orig
		cp.Incarnation, cp.StreamSeq = 2, 7
		if _, err := c.RecordCopy(ctx, &cp); !errors.Is(err, ErrUnknownVersion) {
			t.Errorf("RecordCopy of an unknown version = %v", err)
		}
		begin(t, c, orig, 0)
		for range 2 {
			if s, err := c.RecordCopy(ctx, &cp); err != nil || s != VersionParking {
				t.Errorf("RecordCopy = %v, %v", s, err)
			}
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked_copies`); n != 2 {
			t.Errorf("%d copy rows, want the original's and the copy's", n)
		}
		if err := c.AckCopy(ctx, orig); err != nil {
			t.Fatal(err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked WHERE acked_at IS NOT NULL`); n != 0 {
			t.Errorf("acked_at set while the copy waits")
		}
		if err := c.AckCopy(ctx, &cp); err != nil {
			t.Fatal(err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked_copies WHERE state = 'acked'`); n != 2 {
			t.Errorf("%d copies acked, want 2", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked WHERE acked_at IS NOT NULL`); n != 1 {
			t.Errorf("acked_at not set once every copy is acked")
		}
	})
}

// parked terminate writes the named unresolved versions terminated under its action, with one audit row
// and a terminated event for each hour it touches; a resolved version is never changed.
func TestTerminateParked(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		one, two, three := identity(capture, 1, 1), identity(capture, 2, 2), identity(capture, 3, 3)
		two.Hour = 6
		park(t, c, one, "parked")
		park(t, c, two, "evicted-hour")
		park(t, c, three, "staged")
		seq := uint64(1)
		if n, err := c.TerminateParked(ctx, capture, &seq, "op", "p1"); err != nil || n != 1 {
			t.Fatalf("TerminateParked of seq 1 = %d, %v", n, err)
		}
		if state, _ := versionState(t, c, two); state != "evicted-hour" {
			t.Errorf("seq 2 = %s, want untouched", state)
		}
		if n, err := c.TerminateParked(ctx, capture, nil, "op", "p2"); err != nil || n != 1 {
			t.Fatalf("TerminateParked of the capture = %d, %v; want seq 2 only", n, err)
		}
		for d, want := range map[*Identity][2]string{one: {"terminated", "p1"}, two: {"terminated", "p2"}, three: {"staged", ""}} {
			if state, by := versionState(t, c, d); state != want[0] || by != want[1] {
				t.Errorf("seq %d = %s by %q, want %v", d.Version.Seq, state, by, want)
			}
		}
		if n, err := c.TerminateParked(ctx, capture, nil, "op", "p3"); err != nil || n != 0 {
			t.Errorf("TerminateParked with nothing unresolved = %d, %v", n, err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM audit_log WHERE action_id IN ('p1', 'p2') AND actor = 'op'`); n != 2 {
			t.Errorf("%d audit rows, want one per action that terminated", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM audit_log WHERE action_id = 'p3'`); n != 0 {
			t.Errorf("an action that terminated nothing was audited")
		}
		stray := newID(t)
		if n, err := c.TerminateParked(ctx, stray, nil, "op", "p4"); err != nil || n != 0 {
			t.Errorf("TerminateParked of an unknown capture = %d, %v", n, err)
		}
		if _, err := c.Capture(ctx, stray); !errors.Is(err, ErrUnknownCapture) {
			t.Errorf("terminating nothing left a capture row: %v", err)
		}
		if got := events(t, c); !slices.Equal(got, []string{"terminated@5", "terminated@6"}) {
			t.Errorf("events = %v", got)
		}
	})
}

// capture terminate terminates every unresolved version and writes the order that covers the originals never parked;
// a pending capture keeps its placeholder row.
func TestTerminateCapture(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		d := identity(newID(t), 1, 3)
		capture := d.Version.Capture
		begin(t, c, d, 0)
		if n, err := c.TerminateCapture(ctx, capture, 1, 10, "op", "c1"); err != nil || n != 1 {
			t.Fatalf("TerminateCapture = %d, %v", n, err)
		}
		if state, by := versionState(t, c, d); state != "terminated" || by != "c1" {
			t.Errorf("version = %s by %q", state, by)
		}
		if got, err := c.Capture(ctx, capture); err != nil || got.State != StatePending {
			t.Errorf("capture = %+v, %v; want the placeholder kept", got, err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM audit_log WHERE action = 'capture-terminate' AND action_id = 'c1'`); n != 1 {
			t.Errorf("%d audit rows of the action, want 1", n)
		}
		quiet := newID(t)
		if n, err := c.TerminateCapture(ctx, quiet, 1, 10, "op", "c2"); err != nil || n != 0 {
			t.Fatalf("TerminateCapture with nothing parked = %d, %v", n, err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM audit_log WHERE action_id = 'c2'`); n != 1 {
			t.Errorf("an order with nothing parked to terminate was not audited")
		}
		orders, err := c.TerminationOrders(ctx, capture)
		if err != nil || len(orders) != 1 || orders[0] != (TerminationOrder{ActionID: "c1", Incarnation: 1, CutOff: 10, State: "active"}) {
			t.Fatalf("orders = %+v, %v", orders, err)
		}
		never := identity(capture, 5, 10)
		if r, err := c.Resolve(ctx, &never.Version, 1, 10); err != nil || r != CoveredByOrder {
			t.Errorf("an original never parked = %v, %v; want covered", r, err)
		}
	})
}

// An order is spent once the stream's first sequence passed its cut-off, on its incarnation only.
func TestSpendOrders(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := newID(t)
		order(t, c, capture, "a1")
		if n, err := c.SpendOrders(ctx, 2, 100); err != nil || n != 0 {
			t.Errorf("SpendOrders on another incarnation = %d, %v", n, err)
		}
		if n, err := c.SpendOrders(ctx, 1, 10); err != nil || n != 0 {
			t.Errorf("SpendOrders with the cut-off still stored = %d, %v", n, err)
		}
		if n, err := c.SpendOrders(ctx, 1, 11); err != nil || n != 1 {
			t.Errorf("SpendOrders past the cut-off = %d, %v", n, err)
		}
		if orders, _ := c.TerminationOrders(ctx, capture); len(orders) != 1 || orders[0].State != "spent" {
			t.Errorf("orders = %+v, want spent", orders)
		}
	})
}

// Adopting a replaced stream allocates a new incarnation, and marks the retired one's waiting copies lost and its orders spent.
func TestAdoptStream(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		old, err := c.Incarnate(ctx, RoleTraffic, "S", 1)
		if err != nil {
			t.Fatal(err)
		}
		d := identity(newID(t), 1, 5)
		d.Incarnation = old.ID
		begin(t, c, d, 0)
		acked := *d
		acked.StreamSeq = 6
		begin(t, c, &acked, 0)
		if err := c.AckCopy(ctx, &acked); err != nil {
			t.Fatal(err)
		}
		_, err = c.write.ExecContext(ctx, `INSERT INTO termination_orders (action_id, capture_id, incarnation_id, cut_off, state) VALUES ('a1', ?, ?, 10, 'active')`,
			d.Version.Capture[:], old.ID)
		if err != nil {
			t.Fatal(err)
		}

		adopted, err := c.AdoptStream(ctx, RoleTraffic, "S", 2, "op")
		if err != nil || adopted.ID == old.ID || adopted.CreatedNS != 2 {
			t.Fatalf("AdoptStream = %+v, %v", adopted, err)
		}
		if inc, err := c.Incarnate(ctx, RoleTraffic, "S", 2); err != nil || inc.ID != adopted.ID {
			t.Errorf("Incarnate after the adoption = %+v, %v", inc, err)
		}
		if err := c.AckCopy(ctx, d); err != nil {
			t.Fatal(err)
		}
		states := map[int64]string{}
		for _, v := range mustVersions(t, c, d) {
			for _, cp := range v.Copies {
				states[cp.StreamSeq] = cp.State
			}
		}
		if states[5] != "lost" || states[6] != "acked" {
			t.Errorf("copies = %v; want the waiting one lost for good, the acked one kept", states)
		}
		if orders, _ := c.TerminationOrders(ctx, d.Version.Capture); len(orders) != 1 || orders[0].State != "spent" {
			t.Errorf("orders = %+v, want spent", orders)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM audit_log WHERE action = 'adopt-stream'`); n != 1 {
			t.Errorf("the adoption was not audited")
		}
	})
}

// No stream is adopted while a tracekeep process may run: a live membership or a live compactor lease refuses it.
func TestAdoptStreamRefusedWhileALeaseIsLive(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		cfg := activate(t, c, "config")
		if _, err := c.Register(ctx, "r", []int64{cfg}, ttl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.AdoptStream(ctx, RoleTraffic, "S", 2, "op"); !errors.Is(err, ErrLeaseLive) {
			t.Fatalf("AdoptStream with a live membership = %v", err)
		}
		if err := c.Leave(ctx, "r"); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := c.TakeLease(ctx, "compactor", "h", time.Minute); err != nil || !ok {
			t.Fatalf("TakeLease = %v, %v", ok, err)
		}
		if _, err := c.AdoptStream(ctx, RoleTraffic, "S", 2, "op"); !errors.Is(err, ErrLeaseLive) {
			t.Fatalf("AdoptStream with a live compactor lease = %v", err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM bus_streams`); n != 0 {
			t.Errorf("a refused adoption recorded %d incarnations", n)
		}
	})
}

func mustVersions(t *testing.T, c *Catalog, d *Identity) []ParkedVersion {
	t.Helper()
	vs, err := c.ParkedVersions(t.Context(), &d.Version.Capture)
	if err != nil {
		t.Fatal(err)
	}

	return vs
}

// parked list reads each version with its copies; capture show adds the orders.
func TestParkedVersions(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		d := identity(newID(t), 2, 4)
		if _, err := c.BeginParking(ctx, d, "claim-missing", "claims/x", 0); err != nil {
			t.Fatal(err)
		}
		other := identity(newID(t), 1, 1)
		begin(t, c, other, 0)
		vs := mustVersions(t, c, d)
		if len(vs) != 1 {
			t.Fatalf("%d versions, want the capture's one", len(vs))
		}
		v := vs[0]
		if v.Version != d.Version || v.State != VersionParking || v.Reason != "claim-missing" || v.Hour != 5 || v.ObjectKey != "claims/x" ||
			!slices.Equal(v.Copies, []ParkedCopy{{Incarnation: 1, StreamSeq: 4, State: "waiting"}}) {
			t.Errorf("version = %+v", v)
		}
		if all, err := c.ParkedVersions(ctx, nil); err != nil || len(all) != 2 {
			t.Errorf("every capture's versions = %d, %v", len(all), err)
		}
	})
}

// Parking re-reads the capture's state under its row: an admission committed first makes the horizon guard apply,
// and a capture terminated first makes the delivery terminated (T-32).
func TestParkingRacingAdmissionAndTermination(t *testing.T) {
	t.Parallel()
	for _, rival := range []string{"admission", "termination"} {
		t.Run(rival, func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				gate(t, c)
				capture := newID(t)
				d := identity(capture, 3, 5)
				first := func() {
					if _, err := c.Admit(t.Context(), start(capture), descriptor(t, "d")); err != nil {
						t.Error(err)
					}
				}
				want := Begun{BeyondRetention: true}
				if rival == "termination" {
					first = func() {
						if _, err := c.TerminateCapture(t.Context(), capture, 1, 10, "op", "c1"); err != nil {
							t.Error(err)
						}
					}
					want = Begun{State: VersionTerminated}
				}
				held, release := holdAt(c, hookLocked)
				a := run(first)
				<-held
				waits := c.write.Stats().WaitCount
				var b Begun
				var err error
				p := run(func() { b, err = c.BeginParking(t.Context(), d, "unknown-capture", "", 6) })
				waitBlocked(t, c, waits, "INSERT INTO captures", 1)
				close(release)
				<-a
				<-p
				if err != nil || b != want {
					t.Errorf("BeginParking = %+v, %v; want %+v", b, err, want)
				}
			})
		})
	}
}

// Checklist 5: the parked consumer records a copy's row immediately before or after a capture's termination,
// both serializing on the capture row: the copy row exists either way, and the version ends terminated.
func TestCopyRowRacingATermination(t *testing.T) {
	t.Parallel()
	for _, termFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "termination first", false: "copy row first"}[termFirst], func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				orig := identity(newID(t), 1, 3)
				capture := orig.Version.Capture
				begin(t, c, orig, 0)
				cp := *orig
				cp.Incarnation, cp.StreamSeq = 2, 9
				var state VersionState
				var errC, errT error
				record := func() { state, errC = c.RecordCopy(t.Context(), &cp) }
				terminate := func() { _, errT = c.TerminateCapture(t.Context(), capture, 1, 10, "op", "c1") }
				first, second := terminate, record
				if !termFirst {
					first, second = record, terminate
				}
				held, release := holdAt(c, hookLocked)
				a := run(first)
				<-held
				waits := c.write.Stats().WaitCount
				b := run(second)
				waitBlocked(t, c, waits, "INSERT INTO captures", 1)
				close(release)
				<-a
				<-b
				if errC != nil || errT != nil {
					t.Fatalf("RecordCopy = %v, TerminateCapture = %v", errC, errT)
				}
				want := VersionParking
				if termFirst {
					want = VersionTerminated
				}
				if state != want {
					t.Errorf("RecordCopy found %s, want %s", state, want)
				}
				if s, by := versionState(t, c, orig); s != "terminated" || by != "c1" {
					t.Errorf("version = %s by %q, want terminated by c1", s, by)
				}
				if n := count(t, c, `SELECT COUNT(*) FROM parked_copies WHERE incarnation_id = 2 AND stream_seq = 9`); n != 1 {
					t.Errorf("%d copy rows of the copy, want 1", n)
				}
			})
		})
	}
}
