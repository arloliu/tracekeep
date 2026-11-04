package records

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store"
)

// fakeS3 is an S3-compatible endpoint holding one bucket's objects, path-style, answering as MinIO and Ceph RGW do:
// a HEAD that misses is a bare 404, a GET that misses names NoSuchKey or NoSuchBucket, a range from past the end is a 416,
// and with ignoreRange set a GET answers the whole object without a Content-Range.
type fakeS3 struct {
	bucket      string
	mu          sync.Mutex
	objs        map[string][]byte
	heads, gets atomic.Int64
	ignoreRange atomic.Bool
	// held, when set, is closed by the first GET, which then waits for its request's end.
	held chan struct{}
}

func newFakeS3(t *testing.T) (*fakeS3, *httptest.Server) {
	t.Helper()
	f := &fakeS3{bucket: "packs", objs: map[string][]byte{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	return f, srv
}

// on is an s3 store on bucket of srv.
func on(t *testing.T, srv *httptest.Server, bucket string) *s3store.Store {
	t.Helper()
	s, err := s3store.New(s3store.Config{Endpoint: srv.URL, Bucket: bucket, AccessKeyID: "ak", SecretAccessKey: "sk"})
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if r.Method == http.MethodHead {
		f.heads.Add(1)
	}
	switch {
	case bucket != f.bucket && r.Method == http.MethodHead:
		w.WriteHeader(http.StatusNotFound)
		return
	case bucket != f.bucket:
		s3Error(w, http.StatusNotFound, "NoSuchBucket")
		return
	case key == "":
		return // the bucket's HEAD
	case r.Method == http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.objs[key] = body
		f.mu.Unlock()

		return
	}
	f.mu.Lock()
	obj, ok := f.objs[key]
	f.mu.Unlock()
	if r.Method == http.MethodHead {
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(obj)))

		return
	}
	f.gets.Add(1)
	f.mu.Lock()
	held := f.held
	f.held = nil
	f.mu.Unlock()
	if held != nil {
		close(held)
		<-r.Context().Done()
		return
	}
	f.serveGet(w, r, obj, ok)
}

func (f *fakeS3) serveGet(w http.ResponseWriter, r *http.Request, obj []byte, ok bool) {
	if !ok {
		s3Error(w, http.StatusNotFound, "NoSuchKey")
		return
	}
	if f.ignoreRange.Load() {
		_, _ = w.Write(obj)
		return
	}
	from, to, _ := strings.Cut(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-")
	lo, err1 := strconv.Atoi(from)
	hi, err2 := strconv.Atoi(to)
	if err1 != nil || err2 != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if lo >= len(obj) {
		s3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
		return
	}
	hi = min(hi, len(obj)-1)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi, len(obj)))
	w.Header().Set("Content-Length", strconv.Itoa(hi-lo+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(obj[lo : hi+1])
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

// outcome is what a read of the test's hour streams and returns.
func outcome(ctx context.Context, r *Reader) ([]string, error) {
	var o out
	err := r.Read(ctx, window(), &o)

	return o.lines, err
}

// A pack of the catalog's size opens without a HEAD on a store that opens by size, its reads checking that size:
// another size is the store's failure, never a corrupt pack or a gone one; a size below a pack's header opens by the store's own;
// a missing object is gone, whether its HEAD or its first GET finds it, and a missing bucket or an ignored range is the store's failure.
func TestReadOpensByTheStoredSize(t *testing.T) {
	t.Parallel()
	whole := []string{"0", "1", "2", "3", "4"}
	for _, tc := range []struct {
		name string
		// size is the stored size given the pack's; absent removes its object, extra appends bytes to it.
		size          func(int64) int64
		absent, extra bool
		bucket        string
		ignoreRange   bool
		want          []string
		wantErr       error
		heads         int64
	}{
		{name: "the stored size", size: func(n int64) int64 { return n }, want: whole},
		{name: "smaller", size: func(n int64) int64 { return n - 1 }, wantErr: ErrStore},
		{name: "larger", size: func(n int64) int64 { return n + 1 }, wantErr: ErrStore},
		{name: "past the object's end", size: func(n int64) int64 { return 4 * n }, wantErr: ErrStore},
		{name: "zero", size: func(int64) int64 { return 0 }, want: whole, heads: 1},
		{name: "one", size: func(int64) int64 { return 1 }, want: whole, heads: 1},
		{name: "79", size: func(int64) int64 { return 79 }, want: whole, heads: 1},
		{name: "80", size: func(int64) int64 { return 80 }, wantErr: ErrStore},
		{name: "a valid pack with extra bytes", size: func(n int64) int64 { return n }, extra: true, wantErr: ErrStore},
		{name: "missing, a tiny stored size", size: func(int64) int64 { return 1 }, absent: true, want: []string{"incomplete:gone"}, heads: 4},
		{name: "missing at the first GET", size: func(n int64) int64 { return n }, absent: true, want: []string{"incomplete:gone"}},
		{name: "a missing bucket", size: func(n int64) int64 { return n }, bucket: "other", wantErr: ErrStore},
		{name: "a missing bucket, a tiny stored size", size: func(int64) int64 { return 1 }, bucket: "other", wantErr: ErrStore, heads: 2},
		{name: "an ignored range", size: func(n int64) int64 { return n }, ignoreRange: true, wantErr: ErrStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, srv := newFakeS3(t)
			w := &world{st: on(t, srv, f.bucket), sets: map[int64][]catalog.ReadSet{}}
			p := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
			f.mu.Lock()
			if tc.extra {
				f.objs[p.Key] = append(f.objs[p.Key], make([]byte, 16)...)
			}
			if tc.absent {
				delete(f.objs, p.Key)
			}
			f.mu.Unlock()
			p.Size = tc.size(p.Size)
			w.serve([]catalog.ReadPack{p})
			if tc.bucket != "" {
				w.st = on(t, srv, tc.bucket)
			}
			f.ignoreRange.Store(tc.ignoreRange)
			f.heads.Store(0)
			got, err := outcome(t.Context(), w.reader(16))
			switch {
			case tc.wantErr != nil && (!errors.Is(err, tc.wantErr) || errors.Is(err, store.ErrNotFound) || len(got) != 0):
				t.Errorf("Read = %v, %v; want %v and nothing streamed, no pack left out", got, err, tc.wantErr)
			case tc.wantErr == nil && (err != nil || !slices.Equal(got, tc.want)):
				t.Errorf("Read = %v, %v; want %v", got, err, tc.want)
			default:
			}
			if h := f.heads.Load(); h != tc.heads {
				t.Errorf("%d HEADs, want %d", h, tc.heads)
			}
		})
	}
}

// A pack found gone at its first GET re-reads the hour's read set, as a HEAD's miss does, and reads the pack that replaced it.
func TestReadRereadsAPackGoneAtItsFirstGet(t *testing.T) {
	t.Parallel()
	f, srv := newFakeS3(t)
	w := &world{st: on(t, srv, f.bucket), sets: map[int64][]catalog.ReadSet{}}
	gone := w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(0, 4)...)
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, seqs(0, 4)...)
	w.serve([]catalog.ReadPack{gone}, []catalog.ReadPack{archive})
	got, err := outcome(t.Context(), w.reader(16))
	if err != nil || !slices.Equal(got, []string{"0", "1", "2", "3", "4"}) || f.heads.Load() != 0 {
		t.Errorf("Read = %v, %v after %d HEADs; want every seq and no HEAD", got, err, f.heads.Load())
	}
}

// A read cancelled during its first GET ends with the context's error: nothing streamed, nothing reported incomplete.
func TestReadCancelledAtItsFirstGet(t *testing.T) {
	t.Parallel()
	f, srv := newFakeS3(t)
	w := &world{st: on(t, srv, f.bucket), sets: map[int64][]catalog.ReadSet{}}
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)})
	held := make(chan struct{})
	f.mu.Lock()
	f.held = held
	f.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	var got []string
	go func() {
		var err error
		got, err = outcome(ctx, w.reader(16))
		done <- err
	}()
	<-held
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || len(got) != 0 {
		t.Errorf("a read cancelled at its first GET = %v, %v; want context.Canceled and nothing", got, err)
	}
}

// Reads of one pack by its stored size in parallel each read it whole, and each fails as the store's when the size is wrong.
func TestReadsByTheStoredSizeInParallel(t *testing.T) {
	t.Parallel()
	f, srv := newFakeS3(t)
	w := &world{st: on(t, srv, f.bucket), sets: map[int64][]catalog.ReadSet{}}
	p := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	for _, size := range []int64{p.Size, p.Size - 1} {
		w := &world{st: w.st, sets: map[int64][]catalog.ReadSet{}}
		q := p
		q.Size = size
		w.serve([]catalog.ReadPack{q})
		var wg sync.WaitGroup
		errs := make([]error, 8)
		for i := range errs {
			wg.Go(func() {
				got, err := outcome(t.Context(), w.reader(16))
				switch {
				case size == p.Size && (err != nil || !slices.Equal(got, []string{"0", "1", "2", "3", "4"})):
					errs[i] = fmt.Errorf("read %d = %v, %w; want every seq", i, got, err)
				case size != p.Size && (!errors.Is(err, ErrStore) || len(got) != 0):
					errs[i] = fmt.Errorf("read %d of a wrong size = %v, %w; want ErrStore", i, got, err)
				default:
				}
			})
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Error(err)
		}
	}
	if h := f.heads.Load(); h != 0 {
		t.Errorf("%d HEADs, want none", h)
	}
}
