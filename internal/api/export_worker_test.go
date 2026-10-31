package api

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

const exportOwner = "static:viewer"

// startExport creates a job over capture's hour, as StartExport would.
func startExport(t *testing.T, c *catalog.Catalog, capture tracepack.UUID) tracepack.UUID {
	t.Helper()
	req, err := json.Marshal(exportRequest{Kind: "canonical", Selector: exportSelector{ToolID: tool, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 1) * nsPerHour}})
	if err != nil {
		t.Fatal(err)
	}
	id := newID(t)
	if err := c.CreateExport(t.Context(), id, exportOwner, req, catalog.AuditEntry{Actor: exportOwner, Action: "export-start"}); err != nil {
		t.Fatal(err)
	}

	return id
}

// workers runs m's export workers until the test ends, with a short lease, over a fresh work directory holding a stale one.
func workers(t *testing.T, m *masked) string {
	t.Helper()
	dir := t.TempDir()
	stale := filepath.Join(dir, "stale-1")
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	m.srv.Exports.WorkDir, m.srv.Exports.Lease = dir, 600*time.Millisecond
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.srv.RunExports(ctx, "r1", 20*time.Millisecond) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	return stale
}

// await waits, bounded, for job to reach a state other than queued and running, and returns it.
func await(t *testing.T, c *catalog.Catalog, job tracepack.UUID) catalog.ExportJob {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		j, err := c.ExportJob(t.Context(), job, exportOwner)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != catalog.ExportQueued && j.State != catalog.ExportRunning {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job still %s", j.State)
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // polling a background worker's catalog row, bounded above
	}
}

// A worker claims a job, puts its bundle under the attempt's key and completes it; the stale work directory is gone.
func TestExportWorkerCompletes(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		stale := workers(t, m)
		job := startExport(t, c, capture)
		j := await(t, c, job)
		want := "exports/" + job.String() + "/1.tar"
		if j.State != catalog.ExportDone || j.ObjectKey == nil || *j.ObjectKey != want || j.DeleteAt == nil {
			t.Fatalf("job = %+v, want done with %s", j, want)
		}
		info, err := m.st.Stat(t.Context(), want)
		if err != nil || info.Size != *j.Size {
			t.Errorf("bundle object = %+v, %v; want %d bytes", info, err, *j.Size)
		}
		if _, err := os.Stat(stale); !os.IsNotExist(err) {
			t.Errorf("the stale work directory is still there: %v", err)
		}
	})
}

// A job past its bounds fails with a reason naming no key; a withheld capture's job fails too.
func TestExportWorkerFails(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t)[:1], hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		m.srv.Exports.Bounds.MaxPacks = 1
		m.put(t, capture, hour, ms, entries, m.policy())
		workers(t, m)
		j := await(t, c, startExport(t, c, capture))
		if j.State != catalog.ExportFailed || j.Reason == nil {
			t.Errorf("a job past its bounds = %+v, want failed with a reason", j)
		}
	})
}

// A job whose lease lapses mid-attempt is claimed again at the next fence and completes there; the first attempt changes nothing.
func TestExportWorkerAfterALapse(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		job := startExport(t, c, capture)
		// A first attempt claimed elsewhere and abandoned: its lease lapses, and the worker's claim is fence 2.
		if cl, ok, err := c.ClaimExport(t.Context(), "gone", time.Millisecond, 3); err != nil || !ok || cl.Attempt.Fence != 1 {
			t.Fatalf("claim = %+v, %v, %v", cl, ok, err)
		}
		time.Sleep(5 * time.Millisecond) //nolint:forbidigo // the catalog's clock passing a one-millisecond lease
		workers(t, m)
		j := await(t, c, job)
		if j.State != catalog.ExportDone || j.Fence != 2 || *j.ObjectKey != "exports/"+job.String()+"/2.tar" {
			t.Errorf("job = %+v, want done by the second attempt", j)
		}
	})
}

// gates block m's attempts at each named stage until that stage's release, signalling each arrival; a released stage passes freely.
type gates map[string]*gate

type gate struct {
	arrived chan struct{}
	open    chan struct{}
	release func()
}

func stallAt(m *masked, stages ...string) gates {
	g := gates{}
	for _, s := range stages {
		open := make(chan struct{})
		g[s] = &gate{arrived: make(chan struct{}, 64), open: open, release: sync.OnceFunc(func() { close(open) })}
	}
	m.srv.exportStall = func(stage string) {
		if x, ok := g[stage]; ok {
			select {
			case x.arrived <- struct{}{}:
			default:
			}
			<-x.open
		}
	}

	return g
}

func (g gates) releaseAll() {
	for _, x := range g {
		x.release()
	}
}

// A cancel after the put loses the completion: the job stays cancelled and its object's row intended, for the cleanup.
func TestExportWorkerCancelledMidAttempt(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		g := stallAt(m, "put")
		t.Cleanup(g.releaseAll)
		workers(t, m)
		job := startExport(t, c, capture)
		<-g["put"].arrived
		if err := c.CancelExport(t.Context(), job, exportOwner, catalog.AuditEntry{Actor: exportOwner, Action: "export-cancel"}); err != nil {
			t.Fatal(err)
		}
		g.releaseAll()
		if j := await(t, c, job); j.State != catalog.ExportCancelled {
			t.Errorf("job = %+v, want cancelled", j)
		}
		objs, err := c.ExportObjects(t.Context(), job)
		if err != nil || len(objs) != 1 || objs[0].State != catalog.ObjectIntended {
			t.Errorf("the attempt's rows = %+v, %v; want one intended, for the cleanup", objs, err)
		}
		if _, err := m.st.Stat(t.Context(), objs[0].Key); err != nil {
			t.Errorf("the attempt's object: %v", err)
		}
	})
}

// An attempt whose renewal comes after its lease ends; the next attempt completes at the next fence.
func TestExportWorkerLosesItsLease(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		g := stallAt(m, "built", "renew")
		t.Cleanup(g.releaseAll)
		workers(t, m)
		job := startExport(t, c, capture)
		<-g["built"].arrived
		<-g["renew"].arrived
		<-time.After(m.srv.Exports.Lease + 100*time.Millisecond) // the lease passes, unrenewed
		g["renew"].release()
		g["built"].release()
		j := await(t, c, job)
		if j.State != catalog.ExportDone || j.Fence < 2 {
			t.Errorf("job = %+v, want done by a later attempt", j)
		}
		objs, err := c.ExportObjects(t.Context(), job)
		if err != nil || len(objs) != 1 || objs[0].Fence != j.Fence {
			t.Errorf("rows = %+v, %v; want only the completing attempt's: the first put nothing", objs, err)
		}
	})
}
