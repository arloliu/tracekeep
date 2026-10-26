package store

import (
	"testing"
)

// A store that closes while another opens, after the opening one listed .partial/ and before it looked at the closing one's directory,
// leaves the opening one to succeed.
// Not parallel: it sets the package's hook.
func TestFSOpenWhileAnotherCloses(t *testing.T) {
	dir := t.TempDir()
	a, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	beforeClear = func(name string) {
		if name == a.own && !closed {
			closed = true
			if err := a.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		}
	}
	defer func() { beforeClear = nil }()

	b, err := NewFS(dir)
	if err != nil {
		t.Fatalf("NewFS while another store closed: %v", err)
	}
	_ = b.Close()
	if !closed {
		t.Fatal("the hook never saw the closing store's directory")
	}
}
