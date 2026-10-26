// Package storetest is the conformance suite every ObjectStore backend passes (storage.md §5.2).
package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
		{"HiddenWhileWriting", testHiddenWhileWriting},
		{"FailedBody", testFailedBody},
		{"FailedUpload", testFailedUpload},
		{"Canceled", testCanceled},
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
	// fn's error comes back as it is, whatever it wraps.
	for _, fnErr := range []error{errBroken, fmt.Errorf("gone: %w", fs.ErrNotExist), fs.SkipDir, fs.SkipAll} {
		calls := 0
		err := s.List(ctx, "staging/", func(store.ObjectInfo) error { calls++; return fnErr })
		if !errors.Is(err, fnErr) || calls != 1 {
			t.Errorf("List with fn failing with %v = %v after %d calls, want that error after one", fnErr, err, calls)
		}
	}
}

// While a put is in progress its key is invisible, and once it returns the whole object is there.
func testHiddenWhileWriting(t *testing.T, s store.ObjectStore) {
	data := payload(objectSize)
	b := &body{data: data, at: objectSize / 2, observe: func() { absent(t, s, "staging/writing") }}
	if err := s.Put(t.Context(), "staging/writing", b, objectSize); err != nil {
		t.Fatal(err)
	}
	if b.seen == 0 {
		t.Fatal("the put never read past the body's middle")
	}
	readAt(t, open(t, s, "staging/writing"), data, 0, objectSize)
}

// A body that fails on its first pass leaves nothing visible.
func testFailedBody(t *testing.T, s store.ObjectStore) {
	b := &body{data: payload(objectSize), at: objectSize / 2, failFrom: 1, observe: func() { absent(t, s, "staging/broken") }}
	if err := s.Put(t.Context(), "staging/broken", b, objectSize); err == nil {
		t.Fatal("Put with a failing body succeeded")
	}
	absent(t, s, "staging/broken")
}

// A body that fails from its second pass on fails a backend that reads it twice,
// as an S3 client hashing the body before sending it does, while the store holds part of the upload;
// nothing is visible then or after.
// A backend that reads the body once stores it whole.
func testFailedUpload(t *testing.T, s store.ObjectStore) {
	data := payload(objectSize)
	b := &body{data: data, at: objectSize / 2, failFrom: 2, observe: func() { absent(t, s, "staging/upload") }}
	err := s.Put(t.Context(), "staging/upload", b, objectSize)
	t.Logf("the body was read in %d passes; Put = %v", b.pass, err)
	switch {
	case b.pass < 2 && err == nil:
		readAt(t, open(t, s, "staging/upload"), data, 0, objectSize)
	case b.pass < 2:
		t.Fatalf("Put of a body read once = %v", err)
	case err == nil:
		t.Fatal("Put with a body failing mid-upload succeeded")
	default:
		absent(t, s, "staging/upload")
	}
}

// Once its context is done, a put leaves nothing visible and every method reports the context's error,
// whether the context ended before the call or during it.
func testCanceled(t *testing.T, s store.ObjectStore) {
	put(t, s, "staging/kept", payload(10))
	put(t, s, "staging/kept2", payload(11))

	mid, stop := context.WithCancel(t.Context())
	b := &body{data: payload(objectSize), at: objectSize / 2, observe: stop}
	if err := s.Put(mid, "staging/canceled-mid", b, objectSize); err == nil {
		t.Error("Put whose context ended mid-body succeeded")
	}
	absent(t, s, "staging/canceled-mid")
	mid, stop = context.WithCancel(t.Context())
	calls := 0
	err := s.List(mid, "staging/", func(store.ObjectInfo) error { calls++; stop(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Errorf("List whose context ended in its first callback = %v after %d calls, want context.Canceled after one", err, calls)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Put(ctx, "staging/canceled", bytes.NewReader(payload(10)), 10); err == nil {
		t.Error("Put with a canceled context succeeded")
	}
	absent(t, s, "staging/canceled")
	if err := s.Delete(ctx, "staging/kept"); err == nil {
		t.Error("Delete with a canceled context succeeded")
	}
	if _, err := s.Stat(t.Context(), "staging/kept"); err != nil {
		t.Errorf("Delete with a canceled context removed the object: %v", err)
	}
	if _, err := s.Open(ctx, "staging/kept"); !errors.Is(err, context.Canceled) {
		t.Errorf("Open = %v, want context.Canceled", err)
	}
	if _, err := s.Stat(ctx, "staging/kept"); !errors.Is(err, context.Canceled) {
		t.Errorf("Stat = %v, want context.Canceled", err)
	}
	if err := s.List(ctx, "", func(store.ObjectInfo) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("List = %v, want context.Canceled", err)
	}
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
	// io.EOF may come with a full read only at the end of the object.
	if got, err := obj.ReadAt(p, off); got != int(n) || (err != nil && (!errors.Is(err, io.EOF) || off+n != int64(len(data)))) {
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
	err := s.List(ctx, "", func(info store.ObjectInfo) error {
		if info.Key == key {
			t.Errorf("List shows %+v", info)
		}

		return nil
	})
	if err != nil {
		t.Errorf("List: %v", err)
	}
}

func payload(n int) []byte {
	// Bytes that differ between any two nearby offsets, so a misplaced read shows.
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i ^ i>>8 ^ i>>16 ^ n)
	}

	return b
}

// body is a seekable body that pauses each read pass at the offset at:
// it runs observe there once per pass, then fails from pass failFrom on (0: never).
// A pass is a run of reads from offset 0; a backend that hashes the body before sending it reads it twice.
type body struct {
	data     []byte
	at       int64
	failFrom int
	observe  func()

	off  int64
	pass int
	seen int
}

func (b *body) Read(p []byte) (int, error) {
	if b.off == 0 {
		b.pass++
	}
	if b.off == b.at {
		if b.seen < b.pass {
			b.seen = b.pass
			if b.observe != nil {
				b.observe()
			}
		}
		if b.failFrom > 0 && b.pass >= b.failFrom {
			return 0, errBroken
		}
	}
	end := int64(len(b.data))
	if b.off < b.at {
		end = b.at
	}
	if b.off >= end {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:end])
	b.off += int64(n)

	return n, nil
}

func (b *body) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += b.off
	case io.SeekEnd:
		offset += int64(len(b.data))
	default:
		return 0, errBroken
	}
	b.off = offset

	return offset, nil
}
