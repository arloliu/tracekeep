package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store/s3fake"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// sizedSets is a catalog whose read sets give each pack the size resize makes of its stored one.
type sizedSets struct {
	Catalog
	resize func(int64) int64
}

func (s sizedSets) ReadSet(ctx context.Context, capture tracepack.UUID, hour int64) (catalog.ReadSet, error) {
	rs, err := s.Catalog.ReadSet(ctx, capture, hour)
	for i := range rs.Packs {
		rs.Packs[i].Size = s.resize(rs.Packs[i].Size)
	}

	return rs, err
}

// onFakeS3 is m with its one pack of capture's hour moved to a fake S3 store, read on bucket, its read sets' sizes resized.
func onFakeS3(t *testing.T, m *masked, p *catalog.Pack, bucket string, resize func(int64) int64) *s3fake.Server {
	t.Helper()
	body := objectBytes(t, m.st, p.Key)
	f := s3fake.New(t)
	f.Edit(func(objs map[string][]byte) { objs[p.Key] = body })
	m.srv.Store = f.Store(t, bucket)
	m.srv.Catalog = sizedSets{Catalog: m.srv.Catalog, resize: resize}

	return f
}

// The outcomes of a sized open, as a reader of the pack sees them.
const (
	readsWhole = "the pack read whole"
	storeFails = "the store's failure"
	leftGone   = "the pack gone"
)

// sizedCase is a pack's stored size and object, and what its read comes to.
type sizedCase struct {
	name string
	// resize is the stored size given the pack's; absent removes its object, extra appends bytes to it.
	resize        func(int64) int64
	absent, extra bool
	bucket        string
	ignoreRange   bool
	outcome       string
	heads         int64
}

// sizedCases are the stored sizes and objects whose reads storage.md §5.3 decides.
func sizedCases() []sizedCase {
	same := func(n int64) int64 { return n }
	tiny := func(int64) int64 { return 1 }

	return []sizedCase{
		{name: "the stored size", resize: same, outcome: readsWhole},
		{name: "smaller", resize: func(n int64) int64 { return n - 1 }, outcome: storeFails},
		{name: "larger", resize: func(n int64) int64 { return n + 1 }, outcome: storeFails},
		{name: "past the object's end", resize: func(n int64) int64 { return 4 * n }, outcome: storeFails},
		{name: "zero", resize: func(int64) int64 { return 0 }, outcome: readsWhole, heads: 1},
		{name: "one", resize: tiny, outcome: readsWhole, heads: 1},
		{name: "79", resize: func(int64) int64 { return 79 }, outcome: readsWhole, heads: 1},
		{name: "80", resize: func(int64) int64 { return 80 }, outcome: storeFails},
		{name: "a valid pack with extra bytes", resize: same, extra: true, outcome: storeFails},
		{name: "missing, a tiny stored size", resize: tiny, absent: true, outcome: leftGone, heads: 4},
		{name: "missing at the first GET", resize: same, absent: true, outcome: leftGone},
		{name: "a missing bucket", resize: same, bucket: "other", outcome: storeFails},
		{name: "a missing bucket, a tiny stored size", resize: tiny, bucket: "other", outcome: storeFails, heads: 2},
		{name: "an ignored range", resize: same, ignoreRange: true, outcome: storeFails},
	}
}

// sizedPack is a capture of one pack of one record on a fake S3 store.
type sizedPack struct {
	m       *masked
	capture tracepack.UUID
	p       *catalog.Pack
	f       *s3fake.Server
}

// sized is a capture of one pack of one record, on a fake S3 store as tc sets it.
func sized(t *testing.T, tc *sizedCase) sizedPack {
	t.Helper()
	m := newMasked(t, catalogtest.Open(t, catalog.EngineSQLite))
	m.srv.Dev = true
	capture := m.admitUnmasked(t)
	p := m.put(t, capture, hour, captured(recordsOf(carriers(t)[:1], hour)), nil, nil)
	f := onFakeS3(t, m, p, cmp.Or(tc.bucket, s3fake.Bucket), tc.resize)
	f.Edit(func(objs map[string][]byte) {
		if tc.extra {
			objs[p.Key] = append(objs[p.Key], make([]byte, 16)...)
		}
		if tc.absent {
			delete(objs, p.Key)
		}
	})
	f.IgnoreRange.Store(tc.ignoreRange)

	return sizedPack{m: m, capture: capture, p: p, f: f}
}

// readOf is a stream's records, the reasons of its incomplete markers, and its end.
func readOf(msgs []*tkv1.QueryRecordsResponse) (recs int, reasons []string, end *tkv1.QueryEnd) {
	for _, msg := range msgs {
		for _, it := range msg.GetItems() {
			if it.GetRecord() != nil {
				recs++
			}
			if in := it.GetIncomplete(); in != nil {
				reasons = append(reasons, in.GetReason())
			}
		}
		end = msg.GetEnd()
	}

	return recs, reasons, end
}

// QueryRecords over a pack of the catalog's size, as its client sees it:
// a pack read whole streams its record with no incomplete marker, the store's failure ends the stream unavailable with no record and nothing marked incomplete,
// and a pack gone is marked gone, never corrupt.
func TestQueryRecordsOpensByTheStoredSize(t *testing.T) {
	t.Parallel()
	for _, tc := range sizedCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sp := sized(t, &tc)
			msgs, err := sp.m.streamed(t, query(sp.capture, nil))
			recs, reasons, end := readOf(msgs)
			switch tc.outcome {
			case readsWhole:
				if err != nil || recs != 1 || len(reasons) != 0 || end == nil || end.GetRecords() != 1 {
					t.Errorf("QueryRecords = %d records, incomplete %v, end %v, %v; want the record, nothing marked incomplete", recs, reasons, end, err)
				}
			case storeFails:
				if connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgStore || recs != 0 || len(reasons) != 0 {
					t.Errorf("QueryRecords = %d records, incomplete %v, %v; want unavailable, the store failed, nothing read or marked", recs, reasons, err)
				}
			default:
				if err != nil || recs != 0 || len(reasons) != 1 || reasons[0] != records.ReasonGone || end == nil || !end.GetIncomplete() {
					t.Errorf("QueryRecords = %d records, incomplete %v, end %v, %v; want the hour marked gone", recs, reasons, end, err)
				}
			}
			if h := sp.f.Heads.Load(); h != tc.heads {
				t.Errorf("%d HEADs, want %d", h, tc.heads)
			}
		})
	}
}

// QueryRecords cancelled by its client during a pack's first GET ends cancelled.
func TestQueryRecordsCancelledAtAPacksFirstGet(t *testing.T) {
	t.Parallel()
	sp := sized(t, &sizedCase{resize: func(n int64) int64 { return n }})
	held := sp.f.HoldNextGet()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		s, err := sp.m.cl.QueryRecords(ctx, query(sp.capture, nil))
		if err == nil {
			for s.Receive() {
			}
			err = s.Err()
			_ = s.Close()
		}
		done <- err
	}()
	<-held
	cancel()
	if err := <-done; connect.CodeOf(err) != connect.CodeCanceled {
		t.Errorf("QueryRecords cancelled at a pack's first GET = %v, want canceled", err)
	}
}

// An export attempt over a pack of the catalog's size: a pack read whole is exported, the store's failure fails the attempt,
// and a pack gone is left out as gone, never as corrupt.
func TestExportOpensByTheStoredSize(t *testing.T) {
	t.Parallel()
	const gone = "its object is gone, again after the hour's read set was read again"
	for _, tc := range sizedCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sp := sized(t, &tc)
			m, capture, p, f := sp.m, sp.capture, sp.p, sp.f
			if tc.outcome == storeFails {
				req := &exportRequest{Kind: "canonical", Selector: exportSelector{ToolID: tool, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 1) * nsPerHour}}
				if _, _, err := m.srv.buildBundle(t.Context(), newID(t), req, t.TempDir()); !errors.Is(err, records.ErrStore) || errors.Is(err, store.ErrNotFound) {
					t.Errorf("the attempt = %v, want the store's failure", err)
				}
			} else {
				_, man := build(t, m, capture)
				h := man.Captures[0].Hours[0]
				switch {
				case tc.outcome == readsWhole && (len(h.Packs) != 1 || len(h.LeftOut) != 0):
					t.Errorf("hour = %+v, want the pack exported", h)
				case tc.outcome == leftGone && (len(h.Packs) != 0 || len(h.LeftOut) != 1 || h.LeftOut[0].PackID != p.ID.String() || h.LeftOut[0].Reason != gone):
					t.Errorf("hour = %+v, want the pack left out as gone", h)
				default:
				}
			}
			if h := f.Heads.Load(); h != tc.heads {
				t.Errorf("%d HEADs, want %d", h, tc.heads)
			}
		})
	}
}

// A sized open failing as the store's fails the worker's attempt: it puts and completes no bundle,
// and the next attempt, under a new fence, finds the store mended and exports the pack, none left out.
// The first attempt reads a wrong stored size, extra bytes or an ignored range; the store is mended at the next read set.
func TestExportWorkerRetriesASizedOpensFailure(t *testing.T) {
	t.Parallel()
	same := func(n int64) int64 { return n }
	for _, tc := range []sizedCase{
		{name: "smaller", resize: func(n int64) int64 { return n - 1 }},
		{name: "larger", resize: func(n int64) int64 { return n + 1 }},
		{name: "80", resize: func(int64) int64 { return 80 }},
		{name: "a valid pack with extra bytes", resize: same, extra: true},
		{name: "an ignored range", resize: same, ignoreRange: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sets atomic.Int64
			var mend func()
			wrong := tc.resize
			tc.resize = func(n int64) int64 {
				if sets.Add(1) == 1 {
					return wrong(n)
				}
				mend()

				return n
			}
			sp := sized(t, &tc)
			m, capture, p, f := sp.m, sp.capture, sp.p, sp.f
			body := objectBytes(t, m.st, p.Key)
			mend = func() {
				f.IgnoreRange.Store(false)
				f.Edit(func(objs map[string][]byte) { objs[p.Key] = body })
			}
			stale := workers(t, m)
			job := startExport(t, m.c, capture)
			j := await(t, m.c, job)
			if j.State != catalog.ExportDone || j.Fence != 2 || *j.ObjectKey != "exports/"+job.String()+"/2.tar" {
				t.Fatalf("job = %+v, want done by the second attempt", j)
			}
			path := filepath.Join(t.TempDir(), "bundle.tar")
			if err := os.WriteFile(path, objectBytes(t, m.srv.Store, *j.ObjectKey), 0o600); err != nil {
				t.Fatal(err)
			}
			var man manifest
			if err := json.Unmarshal(unpacked(t, path)["manifest.json"], &man); err != nil {
				t.Fatal(err)
			}
			if h := man.Captures[0].Hours[0]; len(h.Packs) != 1 || len(h.LeftOut) != 0 {
				t.Errorf("hour = %+v, want the pack exported, none left out", h)
			}
			if _, err := m.srv.Store.Stat(t.Context(), "exports/"+job.String()+"/1.tar"); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("the failed attempt's bundle: %v, want none put", err)
			}
			if objs, err := m.c.ExportObjects(t.Context(), job); err != nil || len(objs) != 1 || objs[0].Fence != 2 {
				t.Errorf("rows = %+v, %v; want only the completing attempt's", objs, err)
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(stale), job.String()+"-f1")); !os.IsNotExist(err) {
				t.Errorf("the failed attempt's work directory: %v, want removed", err)
			}
		})
	}
}
