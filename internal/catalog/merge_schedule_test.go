package catalog

import (
	"errors"
	"slices"
	"testing"
)

// An archive the reconciler adopted before the merge's commit stays as adopted, with the suspects it raised;
// the commit writes nothing, and the merge's own committed archive is told apart from it.
func TestCommitMergeOfAnAdoptedArchive(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		archive := archiveOf(t, capture, 1, in)
		if out := register(t, c, archive, Registration{Lease: &l, Adopted: true, HorizonHour: 0}); out != Registered {
			t.Fatalf("adoption = %v", out)
		}
		rev, evs := captureRevision(t, c, capture), events(t, c)
		if out := commit(t, c, l, mergeOf(snap, archive)); out != ArchiveAdopted {
			t.Fatalf("CommitMerge of an adopted archive = %v, want ArchiveAdopted", out)
		}
		if s := packState(t, c, in.ID); s != "staged" {
			t.Errorf("input is %s, want staged", s)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM merges`); n != 0 {
			t.Errorf("merges = %d, want none", n)
		}
		if got := conflictStates(t, c, capture); !slices.Equal(got, []string{"suspect"}) {
			t.Errorf("conflicts = %v, want the adoption's suspect", got)
		}
		if got := captureRevision(t, c, capture); got != rev {
			t.Errorf("capture revision = %d, want %d", got, rev)
		}
		if got := events(t, c); !slices.Equal(got, evs) {
			t.Errorf("events = %v, want %v", got, evs)
		}
	})
}

// A lease that lapsed with no other holder fails the commit too.
func TestCommitMergeUnderALapsedLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		rev, evs := captureRevision(t, c, capture), events(t, c)
		archive := archiveOf(t, capture, 1, in)
		if _, err := c.CommitMerge(t.Context(), l, mergeOf(snap, archive)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("CommitMerge under a lapsed lease = %v, want ErrLeaseLost", err)
		}
		assertNothingWritten(t, c, capture, archive, rev, evs)
	})
}

// A conflict transition between the snapshot and the commit fails the commit:
// a suspect raised by a registration in another hour, and a suspect cleared or confirmed on the scope.
func TestConflictTransitionsFailAMergeCommit(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		register(t, c, segment(t, capture, 2, 5, 14), Registration{Evidence: &Evidence{}})
		archive := archiveOf(t, capture, 1, in)
		if out := commit(t, c, l, mergeOf(snap, archive)); out != MergeStale {
			t.Errorf("CommitMerge after another hour raised a suspect = %v, want stale", out)
		}
		for _, differ := range []bool{false, true} {
			snap := snapshot(t, c, capture)
			ss := suspects(t, c)
			if len(ss) == 0 {
				// Confirm the next pair, raised again by another overlap.
				register(t, c, segment(t, capture, 2, 6, 7), Registration{Evidence: &Evidence{}})
				snap, ss = snapshot(t, c, capture), suspects(t, c)
			}
			if out, err := c.RecordConflict(t.Context(), l, ss[0], differ); err != nil || out != ConflictRecorded {
				t.Fatalf("RecordConflict = %v, %v", out, err)
			}
			rev, evs := captureRevision(t, c, capture), events(t, c)
			archive := archiveOf(t, capture, 1, in)
			if out := commit(t, c, l, mergeOf(snap, archive)); out != MergeStale {
				t.Errorf("CommitMerge after a suspect was recorded (differ %v) = %v, want stale", differ, out)
			}
			assertNothingWritten(t, c, capture, archive, rev, evs)
		}
	})
}

// A registration waits for a merge that holds the capture row and lands after it: the scope reopens, never lost.
func TestRegistrationWaitsForACommittingMerge(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		in := segment(t, capture, 1, 0, 9)
		register(t, c, in, Registration{Evidence: &Evidence{}})
		l := compactorLease(t, c, "h")
		snap := snapshot(t, c, capture)
		held, release := holdAt(c, hookLocked)
		var out MergeOutcome
		merged := run(func() { out, _ = c.CommitMerge(t.Context(), l, mergeOf(snap, archiveOf(t, capture, 1, in))) })
		<-held
		waits := c.write.Stats().WaitCount
		var reg PackOutcome
		late := segment(t, capture, 1, 10, 19)
		registered := run(func() { reg, _ = c.RegisterPack(t.Context(), late, Registration{Evidence: &Evidence{}}) })
		waitBlocked(t, c, waits, "INSERT INTO captures", 1)
		close(release)
		<-merged
		<-registered
		if out != Merged || reg != Registered {
			t.Fatalf("merge = %v, registration = %v; want both to land", out, reg)
		}
		if sc := scopeOf(t, c, capture, 1); sc.State != "reopened" {
			t.Errorf("scope = %+v, want reopened", sc)
		}
		if got := events(t, c); !slices.Equal(got, []string{"created@1", "settled@1", "reopened@1"}) {
			t.Errorf("events = %v", got)
		}
	})
}

// A lease takeover against a committing merge, by where it meets the merge's locks:
// one blocked behind the lease row the merge holds lands after the merge, which committed under the lease it held;
// one before the merge took the lease row wins, and the merge then writes nothing.
// SQLite serializes whole transactions, so only MariaDB can interleave the second.
func TestLeaseTakeoverAgainstAMergeCommit(t *testing.T) {
	t.Parallel()
	for _, beforeLock := range []bool{false, true} {
		engines(t, func(t *testing.T, c *Catalog) {
			if beforeLock && c.d.name == EngineSQLite {
				t.Skip("SQLite serializes whole transactions")
			}
			capture := admitted(t, c)
			in := segment(t, capture, 1, 0, 9)
			register(t, c, in, Registration{Evidence: &Evidence{}})
			l := compactorLease(t, c, "old")
			snap := snapshot(t, c, capture)
			rev, evs := captureRevision(t, c, capture), events(t, c)
			hook := hookBeforeCommit
			if beforeLock {
				hook = hookLocked
			}
			held, release := holdAt(c, hook)
			var out MergeOutcome
			var err error
			archive := archiveOf(t, capture, 1, in)
			merged := run(func() { out, err = c.CommitMerge(t.Context(), l, mergeOf(snap, archive)) })
			<-held
			waits := c.write.Stats().WaitCount
			if !beforeLock {
				leaseRowHeld(t, c, release)
			}
			var lerr error
			lapsed := run(func() {
				_, lerr = c.write.ExecContext(t.Context(), `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
			})
			if beforeLock {
				<-lapsed
				compactorLease(t, c, "new")
			} else {
				waitBehindLeaseRow(t, c, waits, release)
			}
			close(release)
			<-merged
			<-lapsed
			if lerr != nil {
				t.Fatal(lerr)
			}
			if beforeLock {
				if !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("a merge whose lease was taken first = %v, %v; want ErrLeaseLost", out, err)
				}
				assertNothingWritten(t, c, capture, archive, rev, evs)

				return
			}
			if err != nil || out != Merged {
				t.Fatalf("a merge holding the lease row = %v, %v; want merged", out, err)
			}
			nl := compactorLease(t, c, "new")
			if nl.Fence != l.Fence+1 {
				t.Errorf("the takeover's fence %d, want %d", nl.Fence, l.Fence+1)
			}
		})
	}
}
