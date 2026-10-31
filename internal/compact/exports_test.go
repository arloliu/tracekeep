package compact

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

const exportOwner = "static:exporter"

// attempt creates a job, claims it, intends its object with putTimeout and puts it.
func (f *fixture) attempt(t *testing.T, putTimeout time.Duration) (catalog.Attempt, string) {
	t.Helper()
	ctx := t.Context()
	id := newID(t)
	if err := f.c.CreateExport(ctx, id, exportOwner, []byte(`{}`), catalog.AuditEntry{Actor: exportOwner, Action: "export-start"}); err != nil {
		t.Fatal(err)
	}
	cl, ok, err := f.c.ClaimExport(ctx, "r1", time.Hour, 3)
	if err != nil || !ok || cl.Attempt.Job != id {
		t.Fatalf("claim = %+v, %v, %v", cl, ok, err)
	}
	key := "exports/" + id.String() + "/1.tar"
	if _, err := f.c.IntendExport(ctx, cl.Attempt, key, putTimeout); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Put(ctx, key, bytes.NewReader([]byte("bundle")), 6); err != nil {
		t.Fatal(err)
	}

	return cl.Attempt, key
}

func (f *fixture) exists(t *testing.T, key string) bool {
	t.Helper()
	_, err := f.s.Stat(t.Context(), key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}

	return err == nil
}

// failingDeletes fails every delete of a key in fail, the key in its error, as a store's errors name it.
type failingDeletes struct {
	store.ObjectStore
	fail map[string]bool
}

func (s *failingDeletes) Delete(ctx context.Context, key string) error {
	if s.fail[key] {
		return errors.New("the store broke deleting " + key)
	}

	return s.ObjectStore.Delete(ctx, key)
}

// The cleanup deletes an attempt's object once its put deadline has passed by twice the skew, and a bundle past its deadline,
// each object before its row, and a job only once no row of it is left; a live attempt's object and a live bundle stay.
func TestExportCleanup(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		ctx := t.Context()
		_, abandoned := f.attempt(t, -10*time.Minute) // its put deadline long past
		_, live := f.attempt(t, 10*time.Minute)
		expired, expiredKey := f.attempt(t, 10*time.Minute)
		if _, err := c.CompleteExport(ctx, expired, expiredKey, 6, -time.Hour); err != nil { // its deadline an hour past
			t.Fatal(err)
		}
		kept, keptKey := f.attempt(t, 10*time.Minute)
		if _, err := c.CompleteExport(ctx, kept, keptKey, 6, time.Hour); err != nil {
			t.Fatal(err)
		}
		var rep ExportReport
		if err := f.r.ExportCleanup(ctx, f.lease, &rep); err != nil {
			t.Fatal(err)
		}
		if rep.Deleted != 2 || rep.Failed != 0 || rep.Forgotten != 1 {
			t.Errorf("report = %+v, want two objects deleted and the expired job forgotten", rep)
		}
		for key, want := range map[string]bool{abandoned: false, live: true, expiredKey: false, keptKey: true} {
			if got := f.exists(t, key); got != want {
				t.Errorf("object %s exists = %v, want %v", key, got, want)
			}
		}
		if _, err := c.ExportJob(ctx, expired.Job, exportOwner); !errors.Is(err, catalog.ErrExportNotFound) {
			t.Errorf("the expired job = %v, want it forgotten", err)
		}
	})
}

// A deletion that fails is counted and logged without the key, its row and job kept, and the next cycle finishes it.
func TestExportCleanupRetriesAFailure(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		ctx := t.Context()
		a, key := f.attempt(t, -10*time.Minute)
		var logs bytes.Buffer
		mem := f.s
		f.r.Store, f.r.Log = &failingDeletes{ObjectStore: mem, fail: map[string]bool{key: true}}, slog.New(slog.NewTextHandler(&logs, nil))
		var rep ExportReport
		if err := f.r.ExportCleanup(ctx, f.lease, &rep); err != nil {
			t.Fatal(err)
		}
		objs, _ := c.ExportObjects(ctx, a.Job)
		if rep.Failed != 1 || len(objs) != 1 || objs[0].State != catalog.ObjectDeleting || !f.exists(t, key) {
			t.Errorf("after a failed deletion: report %+v, rows %+v; want the failure counted and the row deleting", rep, objs)
		}
		if strings.Contains(logs.String(), key) || !strings.Contains(logs.String(), a.Job.String()) {
			t.Errorf("the log %q names the key, or not the job", logs.String())
		}
		f.r.Store = mem
		rep = ExportReport{}
		if err := f.r.ExportCleanup(ctx, f.lease, &rep); err != nil {
			t.Fatal(err)
		}
		if objs, _ := c.ExportObjects(ctx, a.Job); rep.Deleted != 1 || len(objs) != 0 || f.exists(t, key) {
			t.Errorf("the next cycle: report %+v, rows %+v; want the object and its row gone", rep, objs)
		}
	})
}
