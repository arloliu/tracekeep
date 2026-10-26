package store_test

import (
	"bytes"
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
			if left, _ := os.ReadDir(filepath.Join(dir, ".partial")); len(left) != 0 {
				t.Errorf("%d files left in .partial", len(left))
			}
		})

		return s
	})
}

// A put a crash left behind is removed when the store is opened again.
func TestFSClearsPartialPutsAtOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	newFS(t, dir)
	left := filepath.Join(dir, ".partial", "crashed")
	if err := os.WriteFile(left, []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	newFS(t, dir)
	if _, err := os.Stat(left); !os.IsNotExist(err) {
		t.Errorf("the crashed put is still there: %v", err)
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
