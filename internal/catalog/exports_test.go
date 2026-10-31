package catalog

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

const owner = "oidc:person-1"

func createExport(t *testing.T, c *Catalog) tracepack.UUID {
	t.Helper()
	id := newID(t)
	if err := c.CreateExport(t.Context(), id, owner, []byte(`{"kind":"canonical"}`), AuditEntry{Actor: owner, Action: "export-start", Detail: "job " + id.String()}); err != nil {
		t.Fatal(err)
	}

	return id
}

func claim(t *testing.T, c *Catalog, holder string) (Attempt, bool) {
	t.Helper()
	cl, ok, err := c.ClaimExport(t.Context(), holder, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}

	return cl.Attempt, ok
}

func exportState(t *testing.T, c *Catalog, id tracepack.UUID) ExportJob {
	t.Helper()
	j, err := c.ExportJob(t.Context(), id, owner)
	if err != nil {
		t.Fatal(err)
	}

	return j
}

// lapse makes job's lease passed, as the catalog's clock moving past it would.
func lapseExport(t *testing.T, c *Catalog, id tracepack.UUID) {
	t.Helper()
	exec(t, c, `UPDATE export_jobs SET lease_until = 0 WHERE job_id = ?`, id[:])
}

// A job is claimed, its object intended and the job completed with its deadline; the owner alone reads it.
func TestExportLifecycle(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		id := createExport(t, c)
		if j := exportState(t, c, id); j.State != ExportQueued || j.Attempts != 0 {
			t.Fatalf("created = %+v, want queued", j)
		}
		if _, err := c.ExportJob(t.Context(), id, "static:someone-else"); !errors.Is(err, ErrExportNotFound) {
			t.Errorf("another owner's read = %v, want ErrExportNotFound", err)
		}
		a, ok := claim(t, c, "r1")
		if !ok || a.Job != id || a.Fence != 1 {
			t.Fatalf("claim = %+v, %v", a, ok)
		}
		if _, ok := claim(t, c, "r2"); ok {
			t.Error("a running job with a live lease was claimed again")
		}
		if err := c.RenewExport(t.Context(), a, time.Minute); err != nil {
			t.Fatal(err)
		}
		key := "exports/" + id.String() + "/1.tar"
		if _, err := c.IntendExport(t.Context(), a, key, 10*time.Minute); err != nil {
			t.Fatal(err)
		}
		deleteAt, err := c.CompleteExport(t.Context(), a, key, 1234, 24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		j := exportState(t, c, id)
		if j.State != ExportDone || j.ObjectKey == nil || *j.ObjectKey != key || *j.Size != 1234 || j.DeleteAt == nil || *j.DeleteAt != deleteAt ||
			deleteAt-*j.EndedAt != int64(24*time.Hour) {
			t.Errorf("completed = %+v, want done with its bundle and a deadline a day after its end", j)
		}
		if err := c.CancelExport(t.Context(), id, owner, AuditEntry{Actor: owner, Action: "export-cancel"}); !errors.Is(err, ErrExportEnded) {
			t.Errorf("cancel of a done job = %v, want ErrExportEnded", err)
		}
		ownersOnly(t, c, id)
	})
}

// ownersOnly checks that owner lists id alone and another owner nothing.
func ownersOnly(t *testing.T, c *Catalog, id tracepack.UUID) {
	t.Helper()
	if jobs, err := c.ExportJobs(t.Context(), owner, 10); err != nil || len(jobs) != 1 || jobs[0].ID != id {
		t.Errorf("ExportJobs = %v, %v", jobs, err)
	}
	if jobs, err := c.ExportJobs(t.Context(), "static:someone-else", 10); err != nil || len(jobs) != 0 {
		t.Errorf("another owner's ExportJobs = %v, %v", jobs, err)
	}
}

// Every step of an attempt needs its job running at its fence with its lease live: a lapsed lease changes nothing even before another claim,
// and a claim after it is a new fence the old attempt cannot complete under.
func TestExportAttemptGuard(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		id := createExport(t, c)
		a, _ := claim(t, c, "r1")
		key := "exports/" + id.String() + "/1.tar"
		if _, err := c.IntendExport(t.Context(), a, key, 10*time.Minute); err != nil {
			t.Fatal(err)
		}
		lapseExport(t, c, id)
		if err := c.RenewExport(t.Context(), a, time.Minute); !errors.Is(err, ErrAttemptLost) {
			t.Errorf("renewal of a lapsed lease = %v", err)
		}
		if _, err := c.CompleteExport(t.Context(), a, key, 1, time.Hour); !errors.Is(err, ErrAttemptLost) {
			t.Errorf("completion under a lapsed lease = %v", err)
		}
		if _, err := c.IntendExport(t.Context(), a, key+"x", time.Minute); !errors.Is(err, ErrAttemptLost) {
			t.Errorf("intent under a lapsed lease = %v", err)
		}
		if err := c.FailExport(t.Context(), a, "x"); !errors.Is(err, ErrAttemptLost) {
			t.Errorf("failure under a lapsed lease = %v", err)
		}
		b, ok := claim(t, c, "r2")
		if !ok || b.Fence != 2 {
			t.Fatalf("claim after the lapse = %+v, %v; want fence 2", b, ok)
		}
		exec(t, c, `UPDATE export_jobs SET lease_until = lease_until + 3600000000000 WHERE job_id = ?`, id[:])
		if _, err := c.CompleteExport(t.Context(), a, key, 1, time.Hour); !errors.Is(err, ErrAttemptLost) {
			t.Errorf("the old fence's completion = %v", err)
		}
		if j := exportState(t, c, id); j.State != ExportRunning || j.Fence != 2 || j.Attempts != 2 {
			t.Errorf("job = %+v, want running at fence 2 after two attempts", j)
		}
	})
}

// A job claimable past its attempts is failed instead.
func TestExportAttemptsBound(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		id := createExport(t, c)
		for i := range 3 {
			if a, ok := claim(t, c, "r1"); !ok || a.Fence != int64(i+1) {
				t.Fatalf("claim %d = %+v, %v", i+1, a, ok)
			}
			lapseExport(t, c, id)
		}
		if _, ok := claim(t, c, "r1"); ok {
			t.Error("a fourth attempt was claimed")
		}
		if j := exportState(t, c, id); j.State != ExportFailed || j.Reason == nil {
			t.Errorf("job = %+v, want failed with a reason", j)
		}
	})
}

// Cancel and completion are compare-and-sets of one row: exactly one wins, either way round.
func TestExportCancelAgainstCompletion(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		audit := AuditEntry{Actor: owner, Action: "export-cancel"}
		id := createExport(t, c)
		a, _ := claim(t, c, "r1")
		key := "exports/" + id.String() + "/1.tar"
		if _, err := c.IntendExport(t.Context(), a, key, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := c.CancelExport(t.Context(), id, owner, audit); err != nil {
			t.Fatal(err)
		}
		if _, err := c.CompleteExport(t.Context(), a, key, 1, time.Hour); !errors.Is(err, ErrAttemptLost) {
			t.Errorf("completion after a cancel = %v", err)
		}
		if j := exportState(t, c, id); j.State != ExportCancelled {
			t.Errorf("job = %+v, want cancelled", j)
		}
		if err := c.CancelExport(t.Context(), newID(t), owner, audit); !errors.Is(err, ErrExportNotFound) {
			t.Errorf("cancel of an unknown job = %v", err)
		}
		other := createExport(t, c)
		if err := c.CancelExport(t.Context(), other, "static:someone-else", audit); !errors.Is(err, ErrExportNotFound) {
			t.Errorf("cancel of another owner's job = %v", err)
		}
	})
}

// The cleanup's doom and a completion race on the attempt's row: whichever moves it first wins, either way round;
// a job goes only once no row of it is left.
func TestExportCleanupAgainstCompletion(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		// Doom first: the attempt's put deadline passed, its completion stalled.
		id := createExport(t, c)
		a, _ := claim(t, c, "r1")
		key := "exports/" + id.String() + "/1.tar"
		if _, err := c.IntendExport(t.Context(), a, key, time.Minute); err != nil {
			t.Fatal(err)
		}
		exec(t, c, `UPDATE export_objects SET put_deadline = 0 WHERE object_key = ?`, key)
		doomed, err := c.DoomExportObjects(t.Context(), l, 2*time.Minute, 100)
		if err != nil || len(doomed) != 1 || doomed[0].Key != key {
			t.Fatalf("doomed = %v, %v", doomed, err)
		}
		if _, err := c.CompleteExport(t.Context(), a, key, 1, time.Hour); !errors.Is(err, ErrAttemptLost) {
			t.Errorf("completion of a doomed attempt = %v", err)
		}
		// Completion first: a bundle is never doomed before its deadline.
		id2 := createExport(t, c)
		b, _ := claim(t, c, "r1")
		key2 := "exports/" + id2.String() + "/1.tar"
		if _, err := c.IntendExport(t.Context(), b, key2, time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, err := c.CompleteExport(t.Context(), b, key2, 1, time.Hour); err != nil {
			t.Fatal(err)
		}
		exec(t, c, `UPDATE export_objects SET put_deadline = 0 WHERE object_key = ?`, key2)
		if doomed, _ := c.DoomExportObjects(t.Context(), l, 2*time.Minute, 100); len(doomed) != 1 || doomed[0].Key != key {
			t.Errorf("doomed = %v, want only the first attempt's object, never a live bundle", doomed)
		}
		// The first job keeps its row until its object is forgotten, and then goes once its retention passes.
		exec(t, c, `UPDATE export_jobs SET state = 'failed', ended_at = 0 WHERE job_id = ?`, id[:])
		if n, err := c.ForgetExports(t.Context(), l, time.Hour); err != nil || n != 0 {
			t.Errorf("ForgetExports with a row left = %d, %v; want none", n, err)
		}
		if err := c.ForgetExportObject(t.Context(), l, key); err != nil {
			t.Fatal(err)
		}
		if n, err := c.ForgetExports(t.Context(), l, time.Hour); err != nil || n != 1 {
			t.Errorf("ForgetExports once its row is gone = %d, %v; want 1", n, err)
		}
		// The bundle, past its deadline, is doomed, then forgotten with its job.
		exec(t, c, `UPDATE export_jobs SET delete_at = 0 WHERE job_id = ?`, id2[:])
		if doomed, _ := c.DoomExportObjects(t.Context(), l, 2*time.Minute, 100); len(doomed) != 1 || doomed[0].Key != key2 {
			t.Fatalf("doomed = %v, want the expired bundle", doomed)
		}
		if err := c.ForgetExportObject(t.Context(), l, key2); err != nil {
			t.Fatal(err)
		}
		if n, err := c.ForgetExports(t.Context(), l, time.Hour); err != nil || n != 1 {
			t.Errorf("ForgetExports of the expired job = %d, %v", n, err)
		}
	})
}
