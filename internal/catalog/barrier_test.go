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

// waitBlocked waits until a second transaction waits on the one held:
// on SQLite for the one writer connection, on MariaDB inside stmt, the statement that needs the held lock.
// (INNODB_TRX is a cached view that can miss a short wait, so MariaDB's wait is read from the process list.)
func waitBlocked(t *testing.T, c *Catalog, waitsBefore int64, stmt string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		switch c.d.name {
		case EngineSQLite:
			if c.write.Stats().WaitCount > waitsBefore {
				return
			}
		default:
			var n int
			err := c.read.GetContext(t.Context(), &n, `SELECT COUNT(*) FROM information_schema.PROCESSLIST
				WHERE DB = DATABASE() AND INFO LIKE ?`, stmt+"%")
			if err != nil {
				t.Fatal(err)
			}
			if n > 0 {
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
		waitBlocked(t, c, waits, "INSERT IGNORE INTO captures")
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
		waitBlocked(t, c, waits, "INSERT IGNORE INTO captures")
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
		waitBlocked(t, c, waits, "UPDATE counters SET value = value + ? WHERE name = 'scope_events'")
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
		const short = 300 * time.Millisecond
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
		<-held
		time.Sleep(2 * short) // the scenario: the old member's committed lease lapses while its renewal is uncommitted

		waits := c.write.Stats().WaitCount
		var res Result
		var admitErr error
		admitted := run(func() { res, admitErr = c.Admit(ctx, newID(t), descriptor(t, "d")) })
		waitBlocked(t, c, waits, "UPDATE counters SET value = value WHERE name = 'membership'")
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

// admittedAnother admits a second capture under the gate admitted already set up.
func admittedAnother(t *testing.T, c *Catalog) tracepack.UUID {
	t.Helper()
	id := newID(t)
	if _, err := c.Admit(t.Context(), id, descriptor(t, "d2")); err != nil {
		t.Fatal(err)
	}

	return id
}
