package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

const testOwner = "static:viewer"

// startExport creates a job over capture's hour, as StartExport would.
func startExport(t *testing.T, c *catalog.Catalog, capture tracepack.UUID) tracepack.UUID {
	t.Helper()
	req, err := json.Marshal(exportRequest{Kind: "canonical", Selector: exportSelector{EquipmentID: equipment, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 1) * nsPerHour}})
	if err != nil {
		t.Fatal(err)
	}
	id := newID(t)
	if err := c.CreateExport(t.Context(), id, testOwner, req, catalog.AuditEntry{Actor: testOwner, Action: "export-start"}); err != nil {
		t.Fatal(err)
	}

	return id
}

// workers runs m's export workers until the test ends, with a short lease, over a fresh work directory holding a stale one.
func workers(t *testing.T, m *masked) string {
	t.Helper()
	dir := t.TempDir()
	stale := filepath.Join(dir, "0192f7a4-f5c1-7d3a-9b2e-4f6a8c0d1e2f-f1") // a job id with an f after one of its hyphens
	other := filepath.Join(dir, "not-an-attempt")
	if err := os.Mkdir(other, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(other); err != nil {
			t.Errorf("a directory not named as an attempt was removed: %v", err)
		}
	})
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	m.srv.Exports.WorkDir, m.srv.Exports.Lease = dir, 3*time.Second // a third of it between renewals outlasts a slow runner's stalls
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

// await waits, bounded, for testOwner's job to reach a state other than queued and running, and returns it.
func await(t *testing.T, c *catalog.Catalog, job tracepack.UUID) catalog.ExportJob {
	t.Helper()

	return awaitOwned(t, c, job, testOwner)
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

// A job past its bounds fails with a reason naming no key (a withheld capture fails no job: its bundle names it, TestExportBundleWithheldAndGone).
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
		workers(t, m)
		t.Cleanup(g.releaseAll) // before the workers' own cleanup, which waits for their attempts
		job := startExport(t, c, capture)
		<-g["put"].arrived
		if err := c.CancelExport(t.Context(), job, testOwner, catalog.AuditEntry{Actor: testOwner, Action: "export-cancel"}); err != nil {
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
		workers(t, m)
		t.Cleanup(g.releaseAll) // before the workers' own cleanup, which waits for their attempts
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

// puts is a store whose puts under exports/ fail the first fail times, and, when block is set, wait for their context to end,
// recording how long each waited.
type puts struct {
	store.ObjectStore
	mu     sync.Mutex
	fail   int
	block  bool
	waited []time.Duration
}

func (p *puts) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if strings.HasPrefix(key, "exports/") {
		p.mu.Lock()
		fail, block := p.fail > 0, p.block
		if fail {
			p.fail--
		}
		p.mu.Unlock()
		if fail {
			return errors.New("the store broke putting " + key)
		}
		if block {
			start := time.Now()
			<-ctx.Done()
			p.mu.Lock()
			p.waited = append(p.waited, time.Since(start))
			p.mu.Unlock()

			return ctx.Err()
		}
	}

	return p.ObjectStore.Put(ctx, key, r, size)
}

// An attempt whose put fails ends and frees its slot: its lease lapses, and the next attempt completes the job.
func TestExportWorkerRetriesAFailedPut(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		m.srv.Store = &puts{ObjectStore: m.st, fail: 1}
		workers(t, m)
		j := await(t, c, startExport(t, c, capture))
		if j.State != catalog.ExportDone || j.Fence != 2 {
			t.Errorf("job = %+v, want done by the second attempt", j)
		}
	})
}

// A renewal that stalls leaves the attempt to its lease: the attempt, its put blocked, is cancelled when the lease passes by its own clock.
func TestExportWorkerStalledRenewal(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		st := &puts{ObjectStore: m.st, block: true}
		m.srv.Store = st
		g := stallAt(m, "renew")
		workers(t, m)
		t.Cleanup(g.releaseAll) // before the workers' own cleanup, which waits for their attempts
		job := startExport(t, c, capture)
		<-g["renew"].arrived
		deadline := time.Now().Add(10 * time.Second)
		for {
			st.mu.Lock()
			n := len(st.waited)
			st.mu.Unlock()
			if n > 0 || time.Now().After(deadline) {
				break
			}
			<-time.After(20 * time.Millisecond)
		}
		st.mu.Lock()
		waited := slices.Clone(st.waited)
		st.block = false
		st.mu.Unlock()
		if len(waited) == 0 || waited[0] > 2*m.srv.Exports.Lease {
			t.Fatalf("the blocked put waited %v, want cancelled within the lease", waited)
		}
		g.releaseAll()
		if j := await(t, c, job); j.State != catalog.ExportDone {
			t.Errorf("job = %+v, want done by a later attempt", j)
		}
	})
}

// slowClaims answers each claim only after delay, as a slow catalog would, its lease already running.
type slowClaims struct {
	Catalog
	delay time.Duration
}

func (c slowClaims) ClaimExport(ctx context.Context, holder string, lease time.Duration, maxAttempts int) (catalog.Claim, bool, error) {
	cl, ok, err := c.Catalog.ClaimExport(ctx, holder, lease, maxAttempts)
	if ok {
		<-time.After(c.delay)
	}

	return cl, ok, err
}

// An attempt whose claim was answered late is cancelled when the claim's lease passes, counted from before the claim, not from its answer.
func TestExportWorkerCountsTheLeaseFromItsClaim(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		st := &puts{ObjectStore: m.st, block: true}
		m.srv.Store = st
		g := stallAt(m, "renew") // no renewal: only the lease's own timer can end the put
		startExport(t, c, capture)
		dir := t.TempDir()
		// The lease counted from the claim ends the put about 400 ms after the answer, counted from the answer 2 s after it:
		// the bound between leaves both a loaded runner's scheduling delay.
		m.srv.Exports.WorkDir, m.srv.Exports.Lease = dir, 2*time.Second
		m.srv.Catalog = slowClaims{Catalog: c, delay: 1600 * time.Millisecond}
		ctx, stop := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- m.srv.RunExports(ctx, "r1", 20*time.Millisecond) }()
		t.Cleanup(func() {
			g.releaseAll()
			stop()
			<-done //nolint:errcheck // the workers' end, its error the cancellation's
		})
		deadline := time.Now().Add(10 * time.Second)
		for {
			st.mu.Lock()
			n := len(st.waited)
			st.mu.Unlock()
			if n > 0 || time.Now().After(deadline) {
				break
			}
			<-time.After(20 * time.Millisecond)
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		if len(st.waited) == 0 || st.waited[0] > 1200*time.Millisecond {
			t.Errorf("the put waited %v, want it cancelled when the claim's lease passed, about 400 ms after the claim's answer", st.waited)
		}
	})
}

// breaksOnce is a store whose first open of key opens, then fails every read as a broken store does, naming the key;
// opened records each open's key in order.
type breaksOnce struct {
	store.ObjectStore
	key    string
	mu     sync.Mutex
	broken bool
	opened []string
}

func (b *breaksOnce) Open(ctx context.Context, key string) (store.Object, error) {
	obj, err := b.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opened = append(b.opened, key)
	if key == b.key && !b.broken {
		b.broken = true

		return brokenObject{obj, key}, nil
	}

	return obj, nil
}

type brokenObject struct {
	store.Object
	key string
}

func (o brokenObject) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("the store broke reading " + o.key + ": " + storeSentinel)
}

// A store that fails to read a pack after the hour's earlier packs exported fails the attempt, never leaving the pack out:
// the failed attempt puts and completes no bundle, its work directory is removed, and the next attempt, under a new fence,
// exports every pack; the failed attempt's fence can complete nothing afterwards,
// and its logged end names neither the broken object's key nor the store's text.
func TestExportWorkerStoreFailureFailsTheAttempt(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)
		packs := make([]*catalog.Pack, 0, 3)
		for i := range 3 {
			ms, entries := m.mask(t, capture, captured(recordsOf(cs[i:i+1], hour)))
			packs = append(packs, m.put(t, capture, hour, ms, entries, m.policy()))
		}
		// The read set's last pack, by pack id, is the one whose reads break.
		slices.SortFunc(packs, func(a, b *catalog.Pack) int { return strings.Compare(a.ID.String(), b.ID.String()) })
		st := &breaksOnce{ObjectStore: m.st, key: packs[2].Key}
		m.srv.Store = st
		stale := workers(t, m)
		job := startExport(t, c, capture)
		j := await(t, c, job)
		if j.State != catalog.ExportDone || j.Fence != 2 || *j.ObjectKey != "exports/"+job.String()+"/2.tar" {
			t.Fatalf("job = %+v, want done by the second attempt", j)
		}
		st.mu.Lock()
		at := slices.Index(st.opened, packs[2].Key)
		st.mu.Unlock()
		if at < 2 {
			t.Errorf("the broken pack was the first attempt's open %d, want after both others", at)
		}
		_, man := stored(t, m, *j.ObjectKey)
		h := man.Captures[0].Hours[0]
		if len(h.Packs) != 3 || len(h.LeftOut) != 0 {
			t.Errorf("hour = %+v, want every pack exported, none left out", h)
		}
		if _, err := m.st.Stat(t.Context(), "exports/"+job.String()+"/1.tar"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("the failed attempt's bundle: %v, want none put", err)
		}
		if objs, err := c.ExportObjects(t.Context(), job); err != nil || len(objs) != 1 || objs[0].Fence != 2 {
			t.Errorf("rows = %+v, %v; want only the completing attempt's", objs, err)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(stale), job.String()+"-f1")); !os.IsNotExist(err) {
			t.Errorf("the failed attempt's work directory: %v, want removed", err)
		}
		failed := catalog.Attempt{Job: job, Fence: 1, Holder: "r1"}
		if _, err := c.CompleteExport(t.Context(), failed, "exports/"+job.String()+"/1.tar", 1, time.Hour); err == nil {
			t.Error("the failed attempt completed the job after the next one")
		}
		// The failed attempt's line was written before the next attempt claimed the job, which await read.
		logs := m.logs.String()
		if !strings.Contains(logs, `msg="api: exports: attempt ended" job=`+job.String()+" fence=1") {
			t.Errorf("the log = %s\nwant the failed attempt's end", logs)
		}
		if strings.Contains(logs, packs[2].Key) || strings.Contains(logs, storeSentinel) {
			t.Errorf("the log = %s\nnaming the broken object's key or the store's text", logs)
		}
	})
}
