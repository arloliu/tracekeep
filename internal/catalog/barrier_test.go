package catalog

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// holdAt holds the first transaction that reaches hook id until release is closed; held is closed once it is held.
// Set it before the transactions it concerns start.
func holdAt(c *Catalog, id int) (held, release chan struct{}) {
	held, release = make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	c.hooks[id] = func() {
		if first.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
	}

	return held, release
}

// waitBlocked waits until another transaction waits on the one held:
// on SQLite for the one writer connection, on MariaDB inside stmt, the statement that needs the held lock,
// which n transactions must be inside.
// (INNODB_TRX is a cached view that can miss a short wait, so MariaDB's wait is read from the process list.)
func waitBlocked(t *testing.T, c *Catalog, waitsBefore int64, stmt string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		switch c.d.name {
		case EngineSQLite:
			if c.write.Stats().WaitCount > waitsBefore {
				return
			}
		default:
			var inside int
			err := c.read.GetContext(t.Context(), &inside, `SELECT COUNT(*) FROM information_schema.PROCESSLIST
				WHERE DB = DATABASE() AND INFO LIKE ?`, stmt+"%")
			if err != nil {
				t.Fatal(err)
			}
			if inside >= n {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the second transaction never waited in %q", stmt)
}

// leaseRowHeld proves the held transaction holds the compactor's lease row before any other transaction asks for it:
// on MariaDB a locking read with NOWAIT is refused in this test's own database, so only the held transaction can own the row;
// SQLite serializes whole transactions, so there is nothing to prove.
// Call it before starting the transaction that is to wait behind the row; on a failure it closes release first.
func leaseRowHeld(t *testing.T, c *Catalog, release chan struct{}) {
	t.Helper()
	if c.d.name == EngineSQLite {
		return
	}
	tx, err := c.write.BeginTxx(t.Context(), nil)
	if err == nil {
		_, err = tx.ExecContext(t.Context(), `SELECT name FROM leases WHERE name = 'compactor/2' FOR UPDATE NOWAIT`)
		_ = tx.Rollback()
	}
	if err == nil || !strings.Contains(err.Error(), "1205") {
		close(release)
		t.Fatalf("a locking read of the lease row with NOWAIT = %v, want it refused: the held transaction does not hold the row", err)
	}
}

// waitBehindLeaseRow waits until the lapse of the compactor lease, `UPDATE leases`, waits on the lease row leaseRowHeld proved held:
// on MariaDB until the lapse is inside its statement in this test's own database, which the held row then keeps waiting,
// on SQLite until it waits for the one writer.
// On a failure it closes release first, so the held transaction ends.
func waitBehindLeaseRow(t *testing.T, c *Catalog, waitsBefore int64, release chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		reached := c.write.Stats().WaitCount > waitsBefore
		if c.d.name != EngineSQLite {
			var inside int
			err := c.read.GetContext(t.Context(), &inside, `SELECT COUNT(*) FROM information_schema.PROCESSLIST
				WHERE DB = DATABASE() AND INFO LIKE 'UPDATE leases%'`)
			if err != nil {
				close(release)
				t.Fatalf("process list: %v", err)
			}
			reached = inside > 0
		}
		switch {
		case reached:
			return
		case time.Now().After(deadline):
			close(release)
			t.Fatal("the lapse never reached the lease row")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// run runs f on its own goroutine and returns a channel closed when it returns.
func run(f func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()

	return done
}

// Two first sightings of one capture: the second waits on the capture row the first holds,
// then finds the capture admitted with equal bytes.
func TestTwoFirstSightings(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		cfg := gate(t, c)
		id := newID(t)
		d := descriptor(t, "d")
		held, release := holdAt(c, hookLocked)
		var first, second Result
		var err1, err2 error
		a := run(func() { first, err1 = c.Admit(t.Context(), start(id), d) })
		<-held
		waits := c.write.Stats().WaitCount
		b := run(func() { second, err2 = c.Admit(t.Context(), start(id), d) })
		waitBlocked(t, c, waits, "INSERT INTO captures", 1)
		close(release)
		<-a
		<-b
		if err1 != nil || err2 != nil {
			t.Fatalf("Admit = %v, %v", err1, err2)
		}
		if first != (Result{Outcome: Admitted, ConfigID: cfg}) || second != (Result{Outcome: Unchanged, ConfigID: cfg}) {
			t.Errorf("first sightings = %+v then %+v, want admitted then unchanged", first, second)
		}
	})
}

// Two overlapping registrations of one capture serialize on its row, so the second always sees the first's ranges:
// the suspect is never missed (ingest.md §6.2).
func TestOverlappingRegistrationsNeverMissTheSuspect(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		held, release := holdAt(c, hookLocked)
		var out1, out2 PackOutcome
		var err1, err2 error
		a := run(func() {
			out1, err1 = c.RegisterPack(t.Context(), segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		})
		<-held
		waits := c.write.Stats().WaitCount
		b := run(func() {
			out2, err2 = c.RegisterPack(t.Context(), segment(t, capture, 2, 5, 14), Registration{Evidence: &Evidence{}})
		})
		waitBlocked(t, c, waits, "INSERT INTO captures", 1)
		close(release)
		<-a
		<-b
		if err1 != nil || err2 != nil || out1 != Registered || out2 != Registered {
			t.Fatalf("registrations = %v %v, %v %v", out1, err1, out2, err2)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM conflicts WHERE state = 'suspect' AND seq_from = 5 AND seq_to = 9`); n != 1 {
			t.Errorf("%d suspects, want the overlap's", n)
		}
	})
}

// An event allocated by a transaction not yet committed holds back every later allocation,
// so a reader that has seen seq s never misses a lower seq committed later (catalog.md §7.3).
func TestEventAllocationWaitsForAnUncommittedEarlierOne(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		other := admittedAnother(t, c)
		held, release := holdAt(c, hookBeforeCommit)
		var err1, err2 error
		a := run(func() {
			_, err1 = c.RegisterPack(t.Context(), segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		})
		<-held // its created event holds seq 1, uncommitted
		waits := c.write.Stats().WaitCount
		b := run(func() {
			_, err2 = c.RegisterPack(t.Context(), segment(t, other, 1, 0, 9), Registration{Evidence: &Evidence{}})
		})
		waitBlocked(t, c, waits, "UPDATE counters SET value = value + ? WHERE name = 'scope_events'", 1)
		if n := count(t, c, `SELECT COUNT(*) FROM scope_events`); n != 0 {
			t.Errorf("a reader sees %d events while the first is uncommitted, want none", n)
		}
		close(release)
		<-a
		<-b
		if err1 != nil || err2 != nil {
			t.Fatalf("registrations: %v, %v", err1, err2)
		}
		var caps [][]byte
		if err := c.read.SelectContext(t.Context(), &caps, `SELECT capture_id FROM scope_events ORDER BY seq`); err != nil {
			t.Fatal(err)
		}
		if len(caps) != 2 || tracepack.UUID(caps[0]) != capture || tracepack.UUID(caps[1]) != other {
			t.Errorf("events in seq order belong to %x, want the held transaction's first", caps)
		}
	})
}

// A membership renewal still uncommitted when an admission binds holds the admission back (T-32):
// the admission then sees the renewed lease and binds what the renewed member executes,
// although the lease had lapsed in the committed state while the renewal was held.
func TestRenewalRacingAnAdmission(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		v1 := activate(t, c, "v1")
		// Long enough that the renewal starts before the lease lapses on a loaded server, short enough to wait out.
		const short = 2 * time.Second
		old, err := c.Register(ctx, "old", []int64{v1}, short)
		if err != nil {
			t.Fatal(err)
		}
		v2 := activate(t, c, "v2")
		if _, err := c.Register(ctx, "new", []int64{v1, v2}, ttl); err != nil {
			t.Fatal(err)
		}
		held, release := holdAt(c, hookBeforeCommit)
		var renewErr error
		renewed := run(func() { renewErr = c.Renew(ctx, old, ttl) })
		select {
		case <-held:
		case <-renewed:
			t.Fatalf("the renewal ended before its commit: %v", renewErr)
		}
		time.Sleep(2 * short) // the scenario: the old member's committed lease lapses while its renewal is uncommitted
		var lapsed int
		if err := c.read.GetContext(ctx, &lapsed, `SELECT COUNT(*) FROM members WHERE replica_id = 'old' AND lease_until < `+c.d.nowNS); err != nil || lapsed != 1 {
			t.Fatalf("the committed lease has not lapsed (%d, %v)", lapsed, err)
		}

		waits := c.write.Stats().WaitCount
		var res Result
		var admitErr error
		admitted := run(func() { res, admitErr = c.Admit(ctx, start(newID(t)), descriptor(t, "d")) })
		waitBlocked(t, c, waits, "UPDATE counters SET value = value WHERE name = 'membership'", 1)
		close(release)
		<-renewed
		<-admitted
		if renewErr != nil || admitErr != nil {
			t.Fatalf("Renew = %v, Admit = %v", renewErr, admitErr)
		}
		if res.ConfigID != v1 {
			t.Errorf("the admission bound %d, want v1, which the renewed member executes", res.ConfigID)
		}
	})
}

// Transactions waiting on one existing capture's row take it in turn, never deadlocking:
// the prologue is one upsert, so no waiter holds a shared lock it must later upgrade.
func TestWaitersOnAnExistingCaptureDoNotDeadlock(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		var retries atomic.Int32
		c.hooks[hookRetry] = func() { retries.Add(1) }
		held, release := holdAt(c, hookLocked)
		errs := make([]error, 3)
		var done []<-chan struct{}
		done = append(done, run(func() {
			_, errs[0] = c.RegisterPack(t.Context(), segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		}))
		<-held
		for i := 1; i < 3; i++ {
			waits := c.write.Stats().WaitCount
			done = append(done, run(func() {
				_, errs[i] = c.RegisterPack(t.Context(), segment(t, capture, 1, uint64(10*i), uint64(10*i+9)), Registration{Evidence: &Evidence{}})
			}))
			waitBlocked(t, c, waits, "INSERT INTO captures", i)
		}
		close(release)
		for _, d := range done {
			<-d
		}
		for i, err := range errs {
			if err != nil {
				t.Errorf("registration %d: %v", i, err)
			}
		}
		if n := retries.Load(); n != 0 {
			t.Errorf("%d retries, want none: the waiters deadlocked", n)
		}
	})
}

// A registration that finds its hour past the horizon tombstones the key under the capture row,
// so a registration of the same pack waiting on that row finds the tombstone and registers nothing.
func TestHorizonTombstoneHoldsTheCaptureRow(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		p := segment(t, capture, 1, 0, 9)
		held, release := holdAt(c, hookLocked) // before it decides
		var out1, out2 PackOutcome
		var err1, err2 error
		a := run(func() {
			out1, err1 = c.RegisterPack(t.Context(), p, Registration{Evidence: &Evidence{}, HorizonHour: 2})
		})
		<-held
		waits := c.write.Stats().WaitCount
		b := run(func() { out2, err2 = c.RegisterPack(t.Context(), p, Registration{Evidence: &Evidence{}}) })
		waitBlocked(t, c, waits, "INSERT INTO captures", 1)
		close(release)
		<-a
		<-b
		if err1 != nil || err2 != nil || out1 != BeyondHorizon || out2 != Tombstoned {
			t.Fatalf("registrations = %v %v, %v %v; want beyond the horizon, then tombstoned", out1, err1, out2, err2)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM packs`); n != 0 {
			t.Errorf("%d packs registered under a tombstoned key", n)
		}
	})
}

// admittedAnother admits a second capture under the gate admitted already set up.
func admittedAnother(t *testing.T, c *Catalog) tracepack.UUID {
	t.Helper()
	id := newID(t)
	if _, err := c.Admit(t.Context(), start(id), descriptor(t, "d2")); err != nil {
		t.Fatal(err)
	}

	return id
}

// A termination and an ingest effect of one capture serialize on its row, and whichever commits first wins (T-32):
// two messages of one start version, one an order covers and one above its cut-off.
// The order's application held under the row makes the admission waiting behind it return terminated;
// an admission held under the row admits, and the application then terminates the version only.
func TestTerminationRacingAnAdmission(t *testing.T) {
	t.Parallel()
	for _, termFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "termination first", false: "admission first"}[termFirst], func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				gate(t, c)
				d := identity(newID(t), 0, 10)
				order(t, c, d.Version.Capture, "a1")
				late := *d
				late.StreamSeq = 20
				var res Result
				var r Resolution
				var errA, errT error
				admit := func() { res, errA = c.Admit(t.Context(), &late, descriptor(t, "d")) }
				apply := func() { r, errT = c.ApplyOrder(t.Context(), d) }
				first, second := apply, admit
				if !termFirst {
					first, second = admit, apply
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
				if errA != nil || errT != nil || r != ResolvedTerminated {
					t.Fatalf("Admit = %v, ApplyOrder = %v, %v", errA, r, errT)
				}
				want, state := Terminated, StatePending
				if !termFirst {
					want, state = Admitted, StateOpen
				}
				got, err := c.Capture(t.Context(), d.Version.Capture)
				if res.Outcome != want || err != nil || got.State != state {
					t.Errorf("admission = %v, capture %s (%v); want %v, %s", res.Outcome, got.State, err, want, state)
				}
				if n := count(t, c, `SELECT COUNT(*) FROM scope_events WHERE event = 'terminated'`); n != 1 {
					t.Errorf("%d terminated events, want 1", n)
				}
			})
		})
	}
}

// A registration and an order's application of one parked version, through a message above the cut-off and one the order covers:
// the first to commit wins.
// The application held under the row makes the registration abort and tombstone its key, which the reconciler then never adopts;
// a registration held under the row stages the version, which the application then leaves staged.
func TestTerminationRacingARegistration(t *testing.T) {
	t.Parallel()
	for _, termFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "termination first", false: "registration first"}[termFirst], func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				capture := admitted(t, c)
				d := identity(capture, 3, 10)
				park(t, c, d, "parked")
				order(t, c, capture, "a1")
				p := segment(t, capture, 5, 0, 9)
				var out PackOutcome
				var r Resolution
				var errR, errT error
				late := *d
				late.StreamSeq = 20
				register := func() {
					out, errR = c.RegisterPack(t.Context(), p, Registration{Evidence: &Evidence{Deliveries: []Identity{late}}})
				}
				apply := func() { r, errT = c.ApplyOrder(t.Context(), d) }
				first, second := apply, register
				if !termFirst {
					first, second = register, apply
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
				if errR != nil || errT != nil {
					t.Fatalf("RegisterPack = %v, ApplyOrder = %v", errR, errT)
				}
				wantOut, wantR, wantState, packs, tombs := LostToTermination, ResolvedTerminated, "terminated", 0, 1
				if !termFirst {
					wantOut, wantR, wantState, packs, tombs = Registered, ResolvedStaged, "staged", 1, 0
				}
				// The losing key is tombstoned with the abort, so the reconciler never adopts its object.
				if n := count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, p.Key); n != tombs {
					t.Errorf("%d tombstones of the pack's key, want %d", n, tombs)
				}
				if out != wantOut || r != wantR {
					t.Errorf("registration = %v, application = %v; want %v, %v", out, r, wantOut, wantR)
				}
				if state, _ := versionState(t, c, d); state != wantState {
					t.Errorf("version = %s, want %s", state, wantState)
				}
				if n := count(t, c, `SELECT COUNT(*) FROM packs`); n != packs {
					t.Errorf("%d packs, want %d", n, packs)
				}
			})
		})
	}
}
