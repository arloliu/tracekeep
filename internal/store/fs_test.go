package store_test

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/storetest"
)

func TestFSConformance(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) store.ObjectStore {
		dir := t.TempDir()
		s := newFS(t, dir)
		// Whatever the case did, no put is left in progress.
		t.Cleanup(func() {
			if n := partialFiles(t, dir); n != 0 {
				t.Errorf("%d files left in .partial", n)
			}
		})

		return s
	})
}

// A put a crashed store left behind is removed when a store opens on the root.
func TestFSClearsACrashedStoresPuts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	newFS(t, dir)
	crashed := filepath.Join(dir, ".partial", "crashed")
	if err := os.MkdirAll(crashed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(crashed, "half"), []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	newFS(t, dir)
	if _, err := os.Stat(crashed); !os.IsNotExist(err) {
		t.Errorf("the crashed store's directory is still there: %v", err)
	}
}

// A store opening on a root while another store's put is in progress leaves that put alone.
func TestFSOpenDuringAnotherStoresPut(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	a := newFS(t, dir)
	data := bytes.Repeat([]byte{7}, 1000)
	held, release := make(chan struct{}), make(chan struct{})
	r := &holdingReader{Reader: bytes.NewReader(data), at: 500, held: held, release: release}
	done := make(chan error, 1)
	go func() { done <- a.Put(t.Context(), "staging/a", r, int64(len(data))) }()
	<-held // a's temporary file holds half the body

	// No prefix reaches the temporary file, not even one inside .partial/.
	entries, err := os.ReadDir(filepath.Join(dir, ".partial"))
	if err != nil {
		t.Fatal(err)
	}
	prefixes := make([]string, 0, 3+len(entries))
	prefixes = append(prefixes, "", ".partial", ".partial/")
	for _, e := range entries {
		prefixes = append(prefixes, ".partial/"+e.Name()+"/")
	}
	for _, prefix := range prefixes {
		show := func(info store.ObjectInfo) error {
			t.Errorf("List(%q) shows %+v while its put is in progress", prefix, info)

			return nil
		}
		if err := a.List(t.Context(), prefix, show); err != nil {
			t.Errorf("List(%q) = %v", prefix, err)
		}
	}

	newFS(t, dir)
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("the first store's put failed once a second store opened: %v", err)
	}
	if info, err := a.Stat(t.Context(), "staging/a"); err != nil || info.Size != int64(len(data)) {
		t.Errorf("Stat = %+v, %v", info, err)
	}
}

func TestFSRefusesARootThatDoesNotExist(t *testing.T) {
	t.Parallel()
	if _, err := store.NewFS(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("NewFS created its root")
	}
}

func TestFSRefusesKeysOutsideTheStore(t *testing.T) {
	t.Parallel()
	s := newFS(t, t.TempDir())
	for _, key := range []string{"", ".", "../x", "/abs", "a/../b", "a//b", ".partial", ".partial/x"} {
		if err := s.Put(t.Context(), key, bytes.NewReader(nil), 0); err == nil {
			t.Errorf("Put(%q) succeeded", key)
		}
	}
	if err := s.List(t.Context(), "/abs/", func(store.ObjectInfo) error { return nil }); err != nil {
		t.Errorf("List of a prefix no key has = %v", err)
	}
}

func newFS(t *testing.T, dir string) *store.FS {
	t.Helper()
	s, err := store.NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// partialFiles counts the puts in progress under dir's .partial/.
func partialFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(filepath.Join(dir, ".partial"), func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() != ".lock" {
			n++
		}

		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	return n
}

// holdingReader closes held when its reads reach at, then waits for release.
type holdingReader struct {
	*bytes.Reader
	at            int64
	held, release chan struct{}
	read          int64
}

func (r *holdingReader) Read(p []byte) (int, error) {
	if r.read == r.at {
		close(r.held)
		<-r.release
	}
	if r.read < r.at {
		p = p[:min(int64(len(p)), r.at-r.read)]
	}
	n, err := r.Reader.Read(p)
	r.read += int64(n)

	return n, err
}

var _ io.ReadSeeker = (*holdingReader)(nil)
