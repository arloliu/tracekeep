package api

import (
	"cmp"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store/s3fake"
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
	obj, err := m.st.Open(t.Context(), p.Key)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, obj.Size())
	if _, err := obj.ReadAt(body, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	f := s3fake.New(t)
	f.Edit(func(objs map[string][]byte) { objs[p.Key] = body })
	m.srv.Store = f.Store(t, bucket)
	m.srv.Catalog = sizedSets{Catalog: m.srv.Catalog, resize: resize}

	return f
}

// An export reads a pack of the catalog's size without a HEAD on a store that opens by size:
// another size, extra bytes, a missing bucket or an ignored range fail the attempt as the store's failure, which the next attempt retries,
// never leaving the pack out as corrupt; a size below a pack's header opens by the store's own;
// a missing object, whether its HEAD or its first GET finds it, is left out as gone, as before.
func TestExportOpensByTheStoredSize(t *testing.T) {
	t.Parallel()
	const gone = "its object is gone, again after the hour's read set was read again"
	same := func(n int64) int64 { return n }
	tiny := func(int64) int64 { return 1 }
	for _, tc := range []struct {
		name          string
		resize        func(int64) int64
		absent, extra bool
		bucket        string
		ignoreRange   bool
		// fails is an attempt failed as the store's; leftOut, the reason the pack is left out; neither, the pack exported.
		fails   bool
		leftOut string
		heads   int64
	}{
		{name: "the stored size", resize: same},
		{name: "smaller", resize: func(n int64) int64 { return n - 1 }, fails: true},
		{name: "larger", resize: func(n int64) int64 { return n + 1 }, fails: true},
		{name: "past the object's end", resize: func(n int64) int64 { return 4 * n }, fails: true},
		{name: "zero", resize: func(int64) int64 { return 0 }, heads: 1},
		{name: "one", resize: tiny, heads: 1},
		{name: "79", resize: func(int64) int64 { return 79 }, heads: 1},
		{name: "80", resize: func(int64) int64 { return 80 }, fails: true},
		{name: "a valid pack with extra bytes", resize: same, extra: true, fails: true},
		{name: "missing, a tiny stored size", resize: tiny, absent: true, leftOut: gone, heads: 4},
		{name: "missing at the first GET", resize: same, absent: true, leftOut: gone},
		{name: "a missing bucket", resize: same, bucket: "other", fails: true},
		{name: "a missing bucket, a tiny stored size", resize: tiny, bucket: "other", fails: true, heads: 2},
		{name: "an ignored range", resize: same, ignoreRange: true, fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			if tc.fails {
				req := &exportRequest{Kind: "canonical", Selector: exportSelector{ToolID: tool, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 1) * nsPerHour}}
				if _, _, err := m.srv.buildBundle(t.Context(), newID(t), req, t.TempDir()); !errors.Is(err, records.ErrStore) || errors.Is(err, store.ErrNotFound) {
					t.Errorf("the attempt = %v, want the store's failure", err)
				}
			} else {
				exported(t, m, capture, p, tc.leftOut)
			}
			if h := f.Heads.Load(); h != tc.heads {
				t.Errorf("%d HEADs, want %d", h, tc.heads)
			}
		})
	}
}

// exported checks a bundle of capture's hour holds p, or names it left out for leftOut when set.
func exported(t *testing.T, m *masked, capture tracepack.UUID, p *catalog.Pack, leftOut string) {
	t.Helper()
	_, man := build(t, m, capture)
	h := man.Captures[0].Hours[0]
	switch {
	case leftOut == "" && (len(h.Packs) != 1 || len(h.LeftOut) != 0):
		t.Errorf("hour = %+v, want the pack exported", h)
	case leftOut != "" && (len(h.Packs) != 0 || len(h.LeftOut) != 1 || h.LeftOut[0].PackID != p.ID.String() || h.LeftOut[0].Reason != leftOut):
		t.Errorf("hour = %+v, want the pack left out: %s", h, leftOut)
	default:
	}
}

// An attempt cancelled during a pack's first GET ends with the context's error.
func TestExportCancelledAtAPacksFirstGet(t *testing.T) {
	t.Parallel()
	m := newMasked(t, catalogtest.Open(t, catalog.EngineSQLite))
	m.srv.Dev = true
	capture := m.admitUnmasked(t)
	p := m.put(t, capture, hour, captured(recordsOf(carriers(t)[:1], hour)), nil, nil)
	f := onFakeS3(t, m, p, s3fake.Bucket, func(n int64) int64 { return n })
	held := f.HoldNextGet()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		req := &exportRequest{Kind: "canonical", Selector: exportSelector{ToolID: tool, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 1) * nsPerHour}}
		_, _, err := m.srv.buildBundle(ctx, newID(t), req, t.TempDir())
		done <- err
	}()
	<-held
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("an attempt cancelled at a pack's first GET = %v, want context.Canceled", err)
	}
}
