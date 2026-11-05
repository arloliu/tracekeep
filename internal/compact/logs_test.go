package compact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkrecords "github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
)

// storeSentinel stands in a store's error text beside the key it names, as a backend's text does: neither may reach a log.
const storeSentinel = "store-sentinel"

// logRecord is one log record's message and attributes, as text.
type logRecord struct {
	msg   string
	attrs map[string]string
}

// logs is a slog handler keeping every record; on, when set before the logging starts, sees each.
type logs struct {
	mu   sync.Mutex
	recs []logRecord
	on   func(logRecord)
}

var _ slog.Handler = (*logs)(nil)

// logTo makes r log to a new logs.
func logTo(r *Compactor) *logs {
	l := &logs{}
	r.Log = slog.New(l)

	return l
}

func (l *logs) Enabled(context.Context, slog.Level) bool { return true }

func (l *logs) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Resolve().String()
		return true
	})
	l.mu.Lock()
	l.recs = append(l.recs, rec)
	l.mu.Unlock()
	if l.on != nil {
		l.on(rec)
	}

	return nil
}

func (l *logs) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logs) WithGroup(string) slog.Handler { return l }

// with is the records whose message is msg.
func (l *logs) with(msg string) []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.DeleteFunc(slices.Clone(l.recs), func(r logRecord) bool { return r.msg != msg })
}

// clean fails t for each field of each record holding one of markers, each marker checked apart.
func (l *logs) clean(t *testing.T, markers ...string) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.recs {
		fields := map[string]string{"msg": r.msg}
		maps.Copy(fields, r.attrs)
		for name, v := range fields {
			for _, m := range markers {
				if strings.Contains(v, m) {
					t.Errorf("log %q: its %s holds %q: %q", r.msg, name, m, v)
				}
			}
		}
	}
}

// faulty is a store failing the operations fail names for a key (put, open, read, delete, list),
// each error naming the key beside storeSentinel, as a store's error does; read fails the reads of an opened object.
// failed records the keys it failed.
type faulty struct {
	store.ObjectStore
	fail   func(op, key string) bool
	mu     sync.Mutex
	failed []string
}

func (s *faulty) fault(op, key string) error {
	s.mu.Lock()
	s.failed = append(s.failed, key)
	s.mu.Unlock()

	return fmt.Errorf("%s %s: %s: access denied", op, key, storeSentinel)
}

// keys is the keys s failed.
func (s *faulty) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.failed)
}

func (s *faulty) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if s.fail("put", key) {
		return s.fault("put", key)
	}

	return s.ObjectStore.Put(ctx, key, r, size)
}

func (s *faulty) Open(ctx context.Context, key string) (store.Object, error) {
	if s.fail("open", key) {
		return nil, s.fault("open", key)
	}
	o, err := s.ObjectStore.Open(ctx, key)
	if err != nil || !s.fail("read", key) {
		return o, err
	}

	return failingObject{Object: o, err: s.fault("read", key)}, nil
}

func (s *faulty) Delete(ctx context.Context, key string) error {
	if s.fail("delete", key) {
		return s.fault("delete", key)
	}

	return s.ObjectStore.Delete(ctx, key)
}

// List fails, once it listed every key, naming the last.
func (s *faulty) List(ctx context.Context, prefix string, fn func(store.ObjectInfo) error) error {
	var last string
	err := s.ObjectStore.List(ctx, prefix, func(info store.ObjectInfo) error {
		last = info.Key
		return fn(info)
	})
	if err == nil && s.fail("list", prefix) {
		err = s.fault("list", last)
	}

	return err
}

// failsOn fails op on the keys given.
func failsOn(op string, keys ...string) func(string, string) bool {
	return func(o, key string) bool { return o == op && slices.Contains(keys, key) }
}

// anyOf fails what any of fs fails.
func anyOf(fs ...func(string, string) bool) func(string, string) bool {
	return func(op, key string) bool {
		return slices.ContainsFunc(fs, func(f func(string, string) bool) bool { return f(op, key) })
	}
}

// storeFailures fails t unless n records of msg exist, each error the store's class, and returns them.
func (l *logs) storeFailures(t *testing.T, msg string, n int) []logRecord {
	t.Helper()
	recs := l.with(msg)
	if len(recs) != n {
		t.Fatalf("%d records %q, want %d: %v", len(recs), msg, n, recs)
	}
	for _, r := range recs {
		if !strings.Contains(r.attrs["error"], tkrecords.ErrStore.Error()) {
			t.Errorf("%q's error %q, want the store's class", msg, r.attrs["error"])
		}
	}

	return recs
}

// A reconciler cycle's orphan pass logs each outcome by the pack's id, capture and hour, or by its operation and class,
// never its object key, the key its header spells, or a store's text:
// an adoption, a misnamed and a corrupt object quarantined, an unadmitted capture's orphan,
// and a tombstoned key's deletion, an open and a read that fail.
func TestCycleLogsNameNoKey(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := reconcilers(t, c)
		r := p.a
		f := &fixture{c: c, s: p.s}
		capture := f.admit(t)
		adopted := segment(t, capture, []uint64{1}, false)
		misnamed := segment(t, capture, []uint64{3}, false)
		spelled := misnamed.key
		misnamed.key, _ = tracepack.SegmentKey("", tool, capture, 3, newID(t))
		corrupt := segment(t, capture, []uint64{4, 5}, false)
		corrupt.data = corrupt.data[:len(corrupt.data)-1]
		unadmitted := segment(t, newID(t), []uint64{1}, false)
		gone := segment(t, capture, []uint64{2}, false)
		tombstone(t, c, capture, gone)
		unopened, unread := segment(t, capture, []uint64{6}, false), segment(t, capture, []uint64{7}, false)
		for _, b := range []built{adopted, misnamed, corrupt, unadmitted, gone, unopened, unread} {
			f.put(t, b.key, b.data)
		}
		r.Store = &faulty{ObjectStore: p.s, fail: anyOf(failsOn("delete", gone.key), failsOn("open", unopened.key), failsOn("read", unread.key))}
		l := logTo(r)
		step(t, r)
		step(t, r)

		hourText := fmt.Sprint(hour)
		if recs := l.with("compact: orphan adopted"); len(recs) != 1 ||
			recs[0].attrs["pack"] != adopted.id.String() || recs[0].attrs["capture"] != capture.String() || recs[0].attrs["hour"] != hourText {
			t.Errorf("adoption logs %v, want one naming the pack, its capture and hour", recs)
		}
		quarantined := l.with("compact: orphan quarantined")
		if len(quarantined) != 2 || !slices.ContainsFunc(quarantined, func(r logRecord) bool { return r.attrs["pack"] == misnamed.id.String() }) {
			t.Errorf("quarantine logs %v, want two, the misnamed one by its pack", quarantined)
		}
		if recs := l.with("compact: orphan of an unadmitted capture"); len(recs) != 1 || recs[0].attrs["pack"] != unadmitted.id.String() {
			t.Errorf("unadmitted logs %v, want one naming the pack", recs)
		}
		// The tombstoned key's deletion fails in both cycles, the open and the read in the second, which examines them.
		l.storeFailures(t, "compact: orphan", 4)
		if !f.present(t, gone.key) || !f.present(t, unread.key) {
			t.Error("a failed deletion or read removed its object")
		}
		l.clean(t, adopted.key, misnamed.key, spelled, corrupt.key, unadmitted.key, gone.key, unopened.key, unread.key, storeSentinel)
	})
}

// A failed orphan listing ends the pass with the store's class, and the cycle logs it without its key or text;
// the reconciler goes on until it is stopped.
func TestOrphanListingFailureLogged(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := reconcilers(t, c)
		r := p.a
		f := &fixture{c: c, s: p.s}
		b := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, b.key, b.data)
		r.Store = &faulty{ObjectStore: p.s, fail: func(op, _ string) bool { return op == "list" }}
		l := &logs{}
		ctx, cancel := context.WithCancel(t.Context())
		l.on = func(rec logRecord) {
			if rec.msg == "compact: reconciler cycle" {
				cancel()
			}
		}
		r.Log = slog.New(l)
		if err := r.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want its cancellation", err)
		}
		l.storeFailures(t, "compact: reconciler cycle", 1)
		l.clean(t, b.key, storeSentinel)
		lease, ok, err := c.TakeLease(t.Context(), leaseName, "a", time.Hour)
		if err != nil || !ok {
			t.Fatalf("TakeLease = %v, %v", ok, err)
		}
		if _, err := r.Orphans(t.Context(), lease); !errors.Is(err, tkrecords.ErrStore) {
			t.Errorf("Orphans over a failing listing = %v, want the store's class", err)
		}
	})
}

// A lease lost while the listing's callback settles a chunk passes through the listing unchanged:
// the cycle ends, and the reconciler drops the lease and logs its loss.
func TestOrphanLeaseLostThroughTheListing(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		p := reconcilers(t, c)
		r := p.a
		f := &fixture{c: c, s: p.s}
		keys := f.orphans(t, f.admit(t), 1, catalog.MaxKeyStandings+2)
		step(t, r)
		r.hooks[hookStandings] = func() {
			r.hooks[hookStandings] = nil
			if err := c.ReleaseLease(ctx, *r.lease); err != nil {
				t.Error(err)
			}
			if _, ok, err := c.TakeLease(ctx, leaseName, "other", time.Hour); err != nil || !ok {
				t.Errorf("TakeLease = %v, %v", ok, err)
			}
		}
		l := logTo(r)
		if err := r.Step(ctx); err != nil {
			t.Fatalf("Step = %v, want the lost lease taken as the cycle's end", err)
		}
		if r.lease != nil || len(l.with("compact: compactor lease lost")) != 1 {
			t.Errorf("after the loss the lease is %v, logs %v; want it dropped and its loss logged", r.lease, l.recs)
		}
		l.clean(t, keys...)
	})
}

// A chunk's look that fails is logged by its count, not by a key; the pass goes on.
func TestOrphanStandingsFailureLogged(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, b.key, b.data)
		l := logTo(f.r)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := f.r.Orphans(t.Context(), f.lease); err != nil {
			t.Fatalf("Orphans with its look failing = %v, want it logged and the pass ended", err)
		}
		if recs := l.with("compact: orphan standings"); len(recs) != 1 || recs[0].attrs["keys"] != "1" || recs[0].attrs["error"] == "" {
			t.Errorf("standings logs %v, want one with the count and the error", recs)
		}
		l.clean(t, b.key)
	})
}

// cancelledList is a store whose listing fails with a cancellation, its text naming a key.
type cancelledList struct{ store.ObjectStore }

func (cancelledList) List(_ context.Context, prefix string, _ func(store.ObjectInfo) error) error {
	return fmt.Errorf("list %sx.tpk: %s: %w", prefix, storeSentinel, context.Canceled)
}

// A listing the store reports cancelled stays a cancellation once made keyless.
func TestOrphanListingCancellationKept(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.r.Store = cancelledList{f.s}
		_, err := f.r.Orphans(t.Context(), f.lease)
		if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "x.tpk") || strings.Contains(err.Error(), storeSentinel) {
			t.Errorf("Orphans over a cancelled listing = %v, want the cancellation without the key or the store's text", err)
		}
	})
}

// A merged input's removal whose store fails is logged by the pack and its capture with the store's class;
// the deletion's refusal and the catalog's failure are logged in the catalog's tests, which hold its seams.
func TestDeletionLogsNameNoKey(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		f.r.DeleteDelay = time.Hour
		capture := f.admit(t)
		a := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		b := f.stage(t, writeSegment(t, capture, []uint64{2, 3}, noAdjust))
		if rep := f.merges(t); len(rep.Merged) != 1 {
			t.Fatalf("Merges = %+v", rep)
		}
		f.r.Store = &faulty{ObjectStore: f.s, fail: failsOn("delete", a.Key, b.Key)}
		l := logTo(f.r)
		f.r.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
		var rep RetentionReport
		if err := f.r.Deletions(t.Context(), f.lease, &rep); err != nil {
			t.Fatal(err)
		}
		recs := l.storeFailures(t, "compact: delete a tombstoned pack", 2)
		for _, r := range recs {
			if (r.attrs["pack"] != a.ID.String() && r.attrs["pack"] != b.ID.String()) || r.attrs["capture"] != capture.String() {
				t.Errorf("a removal logged %v, want an input's pack and capture", r.attrs)
			}
		}
		if rep.Deleted != 2 || rep.Failed != 2 {
			t.Errorf("Deletions = %+v, want both deleted and both removals failed", rep)
		}
		l.clean(t, a.Key, b.Key, storeSentinel)
	})
}

// faultyBucket is a claim bucket whose listing and deletions fail as listed, each error naming an object beside storeSentinel.
type faultyBucket struct {
	*memBucket
	list, delete bool
}

func (b *faultyBucket) List(ctx context.Context) ([]ClaimInfo, error) {
	infos, err := b.memBucket.List(ctx)
	if err == nil && b.list && len(infos) > 0 {
		err = fmt.Errorf("list %s: %s: access denied", infos[0].Name, storeSentinel)
	}

	return infos, err
}

func (b *faultyBucket) Delete(ctx context.Context, name string) error {
	if b.delete {
		return fmt.Errorf("delete %s: %s: access denied", name, storeSentinel)
	}

	return b.memBucket.Delete(ctx, name)
}

// Retention's claim listing that fails ends the pass with the store's class, its text gone;
// an evicted hour whose pack and claim object fail to be removed logs each by its capture and hour, never a key.
func TestRetentionLogsNameNoKey(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		mb := f.retaining(t)
		capture := f.admit(t)
		seg := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		obj := sdkObject(t, capture, 5, hour*nsPerHour+10)
		mb.put(obj)
		fb := &faultyBucket{memBucket: mb, list: true}
		f.r.Claims = fb
		l := logTo(f.r)
		var rep RetentionReport
		err := f.r.Retain(t.Context(), f.lease, &rep)
		if !errors.Is(err, tkrecords.ErrStore) || strings.Contains(err.Error(), obj.Name) || strings.Contains(err.Error(), storeSentinel) {
			t.Errorf("Retain over a failing claim listing = %v, want the store's class without the object or its text", err)
		}
		fb.list, fb.delete = false, true
		f.r.Store = &faulty{ObjectStore: f.s, fail: failsOn("delete", seg.Key)}
		rep = f.retain(t)
		if rep.Evicted != 1 || rep.Failed != 2 {
			t.Fatalf("Retain = %+v, want the hour evicted and both removals failed", rep)
		}
		for _, msg := range []string{"compact: delete a tombstoned pack", "compact: delete a claim object"} {
			for _, r := range l.storeFailures(t, msg, 1) {
				if r.attrs["capture"] != capture.String() || r.attrs["hour"] != fmt.Sprint(hour) {
					t.Errorf("%q logged %v, want the evicted hour's capture and hour", msg, r.attrs)
				}
			}
		}
		l.clean(t, seg.Key, obj.Name, storeSentinel)
	})
}

// A close's publication whose put fails is logged by its capture with the store's class;
// an abandoned close whose deletion fails, by its pack, capture and hour.
func TestCloseLogsNameNoKey(t *testing.T) {
	t.Parallel()
	for _, abandoned := range []bool{false, true} {
		name := map[bool]string{false: "put fails", true: "abandoned close's deletion fails"}[abandoned]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				now := time.Now().Add(3 * time.Hour)
				f.closing(t, now)
				capture := f.described(t, newID(t), now.Add(-4*time.Hour).UnixNano())
				op := "put"
				if abandoned {
					f.horizon, op = floorHour(now.UnixNano())+1, "delete"
				}
				s := &faulty{ObjectStore: f.s, fail: func(o, _ string) bool { return o == op }}
				f.r.Store = s
				l := logTo(f.r)
				rep := f.closes(t)
				msg := map[bool]string{false: "compact: publish a close", true: "compact: delete an abandoned close"}[abandoned]
				recs := l.storeFailures(t, msg, 1)
				if recs[0].attrs["capture"] != capture.String() || (abandoned && (rep.Abandoned != 1 || recs[0].attrs["pack"] == "")) || (!abandoned && rep.Failed != 1) {
					t.Errorf("%q logged %v, report %+v; want its capture, and an abandoned close's pack", msg, recs[0].attrs, rep)
				}
				l.clean(t, append(s.keys(), storeSentinel)...)
			})
		})
	}
}

// A merge whose input fails to open or to read, or whose archive's put fails, is logged by its scope with the store's class.
func TestMergeLogsNameNoKey(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"open", "read", "put"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				f.merging(t)
				capture := f.admit(t)
				in := f.stage(t, segment(t, capture, []uint64{0, 1}, false))
				f.stage(t, segment(t, capture, []uint64{2, 3}, false))
				s := &faulty{ObjectStore: f.s, fail: func(o, key string) bool { return o == op && (op == "put" || key == in.Key) }}
				f.r.Store = s
				l := logTo(f.r)
				if rep := f.merges(t); len(rep.Failed) != 1 {
					t.Fatalf("Merges = %+v, want the scope failed", rep)
				}
				recs := l.storeFailures(t, "compact: merge failed", 1)
				if recs[0].attrs["capture"] != capture.String() {
					t.Errorf("the merge's failure logged %v, want its capture", recs[0].attrs)
				}
				l.clean(t, append(s.keys(), in.Key, storeSentinel)...)
			})
		})
	}
}

// A suspect whose pack fails to open or to read is logged by its capture and run with the store's class.
func TestConflictLogsNameNoKey(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"open", "read"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				f.merging(t)
				capture := f.admit(t)
				a := f.stage(t, segmentOf(t, capture, hour, recordsAt(hour, 0, 1, 2, 3, 4), noAdjust))
				b := f.stage(t, segmentOf(t, capture, hour, recordsAt(hour, 3, 4, 5, 6, 7), noAdjust))
				f.r.Store = &faulty{ObjectStore: f.s, fail: failsOn(op, a.Key, b.Key)}
				l := logTo(f.r)
				if rep := f.conflicts(t); rep.Failed != 1 {
					t.Fatalf("Conflicts = %+v, want the comparison failed", rep)
				}
				recs := l.storeFailures(t, "compact: compare a suspect", 1)
				if recs[0].attrs["capture"] != capture.String() {
					t.Errorf("the comparison's failure logged %v, want its capture", recs[0].attrs)
				}
				l.clean(t, a.Key, b.Key, storeSentinel)
			})
		})
	}
}
