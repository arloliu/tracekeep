package catalog

import (
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
		a := run(func() { first, err1 = c.Admit(t.Context(), id, d) })
		<-held
		waits := c.write.Stats().WaitCount
		b := run(func() { second, err2 = c.Admit(t.Context(), id, d) })
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
		if err := c.read.GetContext(ctx, &lapsed, `SELECT COUNT(*) FROM consumers WHERE replica_id = 'old' AND lease_until < `+c.d.nowNS); err != nil || lapsed != 1 {
			t.Fatalf("the committed lease has not lapsed (%d, %v)", lapsed, err)
		}

		waits := c.write.Stats().WaitCount
		var res Result
		var admitErr error
		admitted := run(func() { res, admitErr = c.Admit(ctx, newID(t), descriptor(t, "d")) })
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
	if _, err := c.Admit(t.Context(), id, descriptor(t, "d2")); err != nil {
		t.Fatal(err)
	}

	return id
}
