package records

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store/s3fake"
)

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
			f := s3fake.New(t)
			w := &world{st: f.Store(t, s3fake.Bucket), sets: map[int64][]catalog.ReadSet{}}
			p := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
			f.Edit(func(objs map[string][]byte) {
				if tc.extra {
					objs[p.Key] = append(objs[p.Key], make([]byte, 16)...)
				}
				if tc.absent {
					delete(objs, p.Key)
				}
			})
			p.Size = tc.size(p.Size)
			w.serve([]catalog.ReadPack{p})
			if tc.bucket != "" {
				w.st = f.Store(t, tc.bucket)
			}
			f.IgnoreRange.Store(tc.ignoreRange)
			f.Heads.Store(0)
			got, err := outcome(t.Context(), w.reader(16))
			switch {
			case tc.wantErr != nil && (!errors.Is(err, tc.wantErr) || errors.Is(err, store.ErrNotFound) || len(got) != 0):
				t.Errorf("Read = %v, %v; want %v and nothing streamed, no pack left out", got, err, tc.wantErr)
			case tc.wantErr == nil && (err != nil || !slices.Equal(got, tc.want)):
				t.Errorf("Read = %v, %v; want %v", got, err, tc.want)
			default:
			}
			if h := f.Heads.Load(); h != tc.heads {
				t.Errorf("%d HEADs, want %d", h, tc.heads)
			}
		})
	}
}

// A pack found gone at its first GET re-reads the hour's read set, as a HEAD's miss does, and reads the pack that replaced it.
func TestReadRereadsAPackGoneAtItsFirstGet(t *testing.T) {
	t.Parallel()
	f := s3fake.New(t)
	w := &world{st: f.Store(t, s3fake.Bucket), sets: map[int64][]catalog.ReadSet{}}
	gone := w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(0, 4)...)
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, seqs(0, 4)...)
	w.serve([]catalog.ReadPack{gone}, []catalog.ReadPack{archive})
	got, err := outcome(t.Context(), w.reader(16))
	if err != nil || !slices.Equal(got, []string{"0", "1", "2", "3", "4"}) || f.Heads.Load() != 0 {
		t.Errorf("Read = %v, %v after %d HEADs; want every seq and no HEAD", got, err, f.Heads.Load())
	}
}

// A read cancelled during its first GET ends with the context's error: nothing streamed, nothing reported incomplete.
func TestReadCancelledAtItsFirstGet(t *testing.T) {
	t.Parallel()
	f := s3fake.New(t)
	w := &world{st: f.Store(t, s3fake.Bucket), sets: map[int64][]catalog.ReadSet{}}
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)})
	held := f.HoldNextGet()
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
	f := s3fake.New(t)
	w := &world{st: f.Store(t, s3fake.Bucket), sets: map[int64][]catalog.ReadSet{}}
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
	if h := f.Heads.Load(); h != 0 {
		t.Errorf("%d HEADs, want none", h)
	}
}
