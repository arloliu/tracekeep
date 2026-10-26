// Package storetest is the conformance suite every ObjectStore backend passes (storage.md §5.2).
package storetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"

	"github.com/arloliu/tracekeep/internal/store"
)

// objectSize exceeds a ranged backend's read-ahead, so reads cross windows.
const objectSize = 300 << 10

var errBroken = errors.New("storetest: broken body")

// Run runs the suite; each subtest gets a store of its own from newStore.
func Run(t *testing.T, newStore func(t *testing.T) store.ObjectStore) {
	t.Helper()
	cases := []struct {
		name string
		run  func(*testing.T, store.ObjectStore)
	}{
		{"PutOpenRead", testPutOpenRead},
		{"ParallelReadAt", testParallelReadAt},
		{"Empty", testEmpty},
		{"NotFound", testNotFound},
		{"List", testList},
		{"FailedBody", testFailedBody},
		{"ShortBody", testShortBody},
		{"Delete", testDelete},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.run(t, newStore(t))
		})
	}
}

func testPutOpenRead(t *testing.T, s store.ObjectStore) {
	ctx := t.Context()
	data := payload(objectSize)
	put(t, s, "staging/c1/2026/p1", data)

	if info, err := s.Stat(ctx, "staging/c1/2026/p1"); err != nil || info.Size != objectSize || info.Key != "staging/c1/2026/p1" {
		t.Fatalf("Stat = %+v, %v", info, err)
	}
	obj := open(t, s, "staging/c1/2026/p1")
	if obj.Size() != objectSize {
		t.Fatalf("Size = %d, want %d", obj.Size(), objectSize)
	}
	for _, r := range []struct{ off, n int64 }{
		{0, objectSize}, {0, 1}, {1, 100}, {objectSize / 2, 70 << 10}, {objectSize - 1, 1}, {100, 10}, {101, 10},
	} {
		readAt(t, obj, data, r.off, r.n)
	}
	// Past the end: the bytes that exist, then io.EOF.
	p := make([]byte, 20)
	if n, err := obj.ReadAt(p, objectSize-5); n != 5 || !errors.Is(err, io.EOF) || !bytes.Equal(p[:5], data[objectSize-5:]) {
		t.Errorf("ReadAt across the end = %d, %v", n, err)
	}
	if n, err := obj.ReadAt(p, objectSize); n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("ReadAt at the end = %d, %v; want 0, io.EOF", n, err)
	}
	if _, err := obj.ReadAt(p, -1); err == nil {
		t.Errorf("ReadAt at a negative offset succeeded")
	}
}

func testParallelReadAt(t *testing.T, s store.ObjectStore) {
	data := payload(objectSize)
	put(t, s, "archive/c1/p1", data)
	obj := open(t, s, "archive/c1/p1")
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for k := range int64(20) {
				off := (int64(g)*7919 + k*104729) % objectSize
				n := min((off*31)%(8<<10)+1, objectSize-off)
				readAt(t, obj, data, off, n)
			}
		})
	}
	wg.Wait()
}

func testEmpty(t *testing.T, s store.ObjectStore) {
	put(t, s, "staging/empty", nil)
	obj := open(t, s, "staging/empty")
	if obj.Size() != 0 {
		t.Errorf("Size = %d, want 0", obj.Size())
	}
	if n, err := obj.ReadAt(make([]byte, 1), 0); n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("ReadAt = %d, %v; want 0, io.EOF", n, err)
	}
}

func testNotFound(t *testing.T, s store.ObjectStore) {
	ctx := t.Context()
	if _, err := s.Open(ctx, "staging/none"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Open = %v, want ErrNotFound", err)
	}
	if _, err := s.Stat(ctx, "staging/none"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Stat = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, "staging/none"); err != nil {
		t.Errorf("Delete of an absent key = %v, want nil", err)
	}
}

func testList(t *testing.T, s store.ObjectStore) {
	ctx := t.Context()
	keys := []string{"staging/a/1", "staging/a/2", "staging/ab/1", "staging/b/1", "archive/a/1"}
	for i, k := range keys {
		put(t, s, k, payload(i+1))
	}
	for _, c := range []struct {
		prefix string
		want   []string
	}{
		{"staging/a/", keys[:2]},
		{"staging/a", keys[:3]},
		{"staging/", keys[:4]},
		{"", keys},
		{"exports/", nil},
	} {
		var got []string
		err := s.List(ctx, c.prefix, func(info store.ObjectInfo) error {
			if i := slices.Index(keys, info.Key); i < 0 || info.Size != int64(i+1) {
				t.Errorf("List(%q) gave %+v", c.prefix, info)
			}
			got = append(got, info.Key)

			return nil
		})
		slices.Sort(got)
		want := slices.Sorted(slices.Values(c.want))
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("List(%q) = %v, %v; want %v", c.prefix, got, err, want)
		}
	}
	calls := 0
	if err := s.List(ctx, "staging/", func(store.ObjectInfo) error { calls++; return errBroken }); !errors.Is(err, errBroken) || calls != 1 {
		t.Errorf("List with a failing fn = %v after %d calls, want its error after one", err, calls)
	}
}

// A body that fails partway leaves nothing visible.
func testFailedBody(t *testing.T, s store.ObjectStore) {
	data := payload(objectSize)
	if err := s.Put(t.Context(), "staging/broken", &brokenReader{data: data, failAt: objectSize / 2}, objectSize); err == nil {
		t.Fatal("Put with a failing body succeeded")
	}
	absent(t, s, "staging/broken")
}

// A body shorter than its declared size leaves nothing visible.
func testShortBody(t *testing.T, s store.ObjectStore) {
	data := payload(1000)
	if err := s.Put(t.Context(), "staging/short", bytes.NewReader(data), 2000); err == nil {
		t.Fatal("Put with a short body succeeded")
	}
	absent(t, s, "staging/short")
}

func testDelete(t *testing.T, s store.ObjectStore) {
	put(t, s, "staging/gone", payload(10))
	if err := s.Delete(t.Context(), "staging/gone"); err != nil {
		t.Fatal(err)
	}
	absent(t, s, "staging/gone")
}

func put(t *testing.T, s store.ObjectStore, key string, data []byte) {
	t.Helper()
	if err := s.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

func open(t *testing.T, s store.ObjectStore, key string) store.Object {
	t.Helper()
	obj, err := s.Open(t.Context(), key)
	if err != nil {
		t.Fatalf("Open %s: %v", key, err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	return obj
}

func readAt(t *testing.T, obj store.Object, data []byte, off, n int64) {
	t.Helper()
	p := make([]byte, n)
	if got, err := obj.ReadAt(p, off); got != int(n) || (err != nil && !errors.Is(err, io.EOF)) {
		t.Errorf("ReadAt(%d bytes at %d) = %d, %v", n, off, got, err)

		return
	}
	if !bytes.Equal(p, data[off:off+n]) {
		t.Errorf("ReadAt(%d bytes at %d) returned other bytes", n, off)
	}
}

// absent checks that key is invisible to Stat, Open and List.
func absent(t *testing.T, s store.ObjectStore, key string) {
	t.Helper()
	ctx := context.WithoutCancel(t.Context())
	if _, err := s.Stat(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Stat %s = %v, want ErrNotFound", key, err)
	}
	if _, err := s.Open(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Open %s = %v, want ErrNotFound", key, err)
	}
	_ = s.List(ctx, "", func(info store.ObjectInfo) error {
		t.Errorf("List shows %+v", info)

		return nil
	})
}

func payload(n int) []byte {
	// Bytes that differ between any two nearby offsets, so a misplaced read shows.
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i ^ i>>8 ^ i>>16 ^ n)
	}

	return b
}

// brokenReader is a seekable body whose reads fail once they reach failAt.
type brokenReader struct {
	data   []byte
	failAt int64
	off    int64
}

func (r *brokenReader) Read(p []byte) (int, error) {
	if r.off >= r.failAt {
		return 0, errBroken
	}
	n := copy(p, r.data[r.off:r.failAt])
	r.off += int64(n)

	return n, nil
}

func (r *brokenReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += int64(len(r.data))
	default:
		return 0, errBroken
	}
	r.off = offset

	return offset, nil
}
