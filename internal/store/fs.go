package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
)

// partialDir holds puts in progress, under the root and outside every key.
const partialDir = ".partial"

// FS is an ObjectStore on a local directory, for standalone pilots and tests (storage.md §5.2):
// a put is written under .partial/, synced, renamed into place, and its directories synced.
type FS struct {
	root *os.Root
}

var _ ObjectStore = (*FS)(nil)

// NewFS opens dir as a store, creating it if needed, and removes the puts a crash left in .partial/.
func NewFS(dir string) (*FS, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if err := root.RemoveAll(partialDir); err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("store: clear %s: %w", partialDir, err)
	}
	if err := root.Mkdir(partialDir, 0o700); err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("store: %w", err)
	}

	return &FS{root: root}, nil
}

// Close releases the store's directory.
func (s *FS) Close() error {
	return s.root.Close()
}

// Put implements ObjectStore.
func (s *FS) Put(_ context.Context, key string, r io.ReadSeeker, size int64) error {
	if err := checkKey(key); err != nil {
		return err
	}
	tmp := path.Join(partialDir, rand.Text())
	if err := s.write(tmp, r, size); err != nil {
		_ = s.root.Remove(tmp)
		return fmt.Errorf("store: put %s: %w", key, err)
	}
	dir := path.Dir(key)
	if err := s.root.MkdirAll(dir, 0o700); err != nil {
		_ = s.root.Remove(tmp)
		return fmt.Errorf("store: put %s: %w", key, err)
	}
	if err := s.root.Rename(tmp, key); err != nil {
		_ = s.root.Remove(tmp)
		return fmt.Errorf("store: put %s: %w", key, err)
	}
	// The rename is durable once its directory is synced, and a new directory once its parent is.
	for d := dir; ; d = path.Dir(d) {
		if err := s.syncDir(d); err != nil {
			return fmt.Errorf("store: put %s: %w", key, err)
		}
		if d == "." {
			return nil
		}
	}
}

// Open implements ObjectStore.
func (s *FS) Open(_ context.Context, key string) (Object, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	f, err := s.root.Open(key)
	if err != nil {
		return nil, notFound(key, err)
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, notFound(key, err)
	}

	return fsObject{File: f, size: info.Size()}, nil
}

// Stat implements ObjectStore.
func (s *FS) Stat(_ context.Context, key string) (ObjectInfo, error) {
	if err := checkKey(key); err != nil {
		return ObjectInfo{}, err
	}
	info, err := s.root.Stat(key)
	if err != nil || !info.Mode().IsRegular() {
		return ObjectInfo{}, notFound(key, err)
	}

	return ObjectInfo{Key: key, Size: info.Size()}, nil
}

// List implements ObjectStore.
func (s *FS) List(_ context.Context, prefix string, fn func(ObjectInfo) error) error {
	// Walk only the directory the prefix names, and filter by the rest.
	start := "."
	if i := strings.LastIndexByte(prefix, '/'); i >= 0 {
		start = prefix[:i]
	}
	err := fs.WalkDir(s.root.FS(), start, func(p string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case p == partialDir:
			return fs.SkipDir
		case d.IsDir() || !d.Type().IsRegular() || !strings.HasPrefix(p, prefix):
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}

		return fn(ObjectInfo{Key: p, Size: info.Size()})
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	return err
}

// Delete implements ObjectStore.
func (s *FS) Delete(_ context.Context, key string) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if err := s.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("store: delete %s: %w", key, err)
	}

	return nil
}

func (s *FS) write(name string, r io.Reader, size int64) error {
	f, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = io.CopyN(f, r, size)
	if err == nil {
		err = f.Sync()
	}

	return errors.Join(err, f.Close())
}

func (s *FS) syncDir(dir string) error {
	d, err := s.root.Open(dir)
	if err != nil {
		return err
	}

	return errors.Join(d.Sync(), d.Close())
}

// checkKey refuses a key that is not a clean relative path, or that lies under .partial/.
func checkKey(key string) error {
	if !fs.ValidPath(key) || key == "." || key == partialDir || strings.HasPrefix(key, partialDir+"/") {
		return fmt.Errorf("store: invalid key %q", key)
	}

	return nil
}

func notFound(key string, err error) error {
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: %s", ErrNotFound, key)
	}

	return fmt.Errorf("store: %s: %w", key, err)
}

type fsObject struct {
	*os.File
	size int64
}

func (o fsObject) Size() int64 { return o.size }
