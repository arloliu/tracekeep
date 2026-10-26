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
	"syscall"
)

const (
	// partialDir holds puts in progress, under the root and outside every key:
	// each open FS writes in a directory of its own there, which it holds an flock on.
	partialDir = ".partial"
	// partialLock serializes opening stores, so a new store's directory is locked before another open looks at it.
	partialLock = partialDir + "/.lock"
)

// beforeClear, set only by the package's tests, runs before clearDead looks at an entry.
var beforeClear func(name string)

// FS is an ObjectStore on a local directory, for standalone pilots and tests (storage.md §5.2):
// a put is written under .partial/, synced, renamed into place, and its directories synced.
// Several FS values, in one process or many, may share a root.
type FS struct {
	root *os.Root
	// own is this store's directory under .partial/, and ownLock the open file whose flock marks it live.
	own     string
	ownLock *os.File
}

var _ ObjectStore = (*FS)(nil)

// NewFS opens the existing directory dir as a store.
// The directory is not created: it must already be durable, since a put's durability ends at it.
// Puts a crashed store left in .partial/ are removed; a live store's are not.
func NewFS(dir string) (*FS, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("store: open root: %w", err)
	}
	s, err := open(root)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("store: open %s: %w", dir, err)
	}

	return s, nil
}

func open(root *os.Root) (*FS, error) {
	if err := root.MkdirAll(partialDir, 0o700); err != nil {
		return nil, err
	}
	lock, err := root.OpenFile(partialLock, os.O_RDONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := flock(lock, syscall.LOCK_EX); err != nil {
		return nil, err
	}
	if err := clearDead(root); err != nil {
		return nil, err
	}
	own := path.Join(partialDir, rand.Text())
	if err := root.Mkdir(own, 0o700); err != nil {
		return nil, err
	}
	ownLock, err := root.Open(own)
	if err == nil {
		err = flock(ownLock, syscall.LOCK_EX|syscall.LOCK_NB)
	}
	if err != nil {
		_ = root.RemoveAll(own)
		return nil, err
	}

	return &FS{root: root, own: own, ownLock: ownLock}, nil
}

// Close removes the store's directory under .partial/ and releases the root.
func (s *FS) Close() error {
	return errors.Join(s.root.RemoveAll(s.own), s.ownLock.Close(), s.root.Close())
}

// Put implements ObjectStore.
func (s *FS) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if err := s.putFile(ctx, key, r, size); err != nil {
		return fmt.Errorf("store: put %s: %w", key, err)
	}

	return nil
}

// Open implements ObjectStore.
func (s *FS) Open(ctx context.Context, key string) (Object, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
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
func (s *FS) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	if err := checkKey(key); err != nil {
		return ObjectInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	info, err := s.root.Stat(key)
	if err != nil || !info.Mode().IsRegular() {
		return ObjectInfo{}, notFound(key, err)
	}

	return ObjectInfo{Key: key, Size: info.Size()}, nil
}

// List implements ObjectStore.
func (s *FS) List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start, ok, err := s.listStart(prefix)
	if !ok || err != nil {
		return err
	}
	var fnErr error
	err = fs.WalkDir(s.root.FS(), start, func(p string, d fs.DirEntry, err error) error {
		switch {
		case err != nil && p != start && errors.Is(err, fs.ErrNotExist):
			return nil // removed while the walk ran
		case err != nil:
			return err
		case p == partialDir:
			return fs.SkipDir
		case d.IsDir() || !d.Type().IsRegular() || !strings.HasPrefix(p, prefix):
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		// fn's error goes back to the caller as it is, never interpreted by the walk.
		if fnErr = fn(ObjectInfo{Key: p, Size: info.Size()}); fnErr != nil {
			return fs.SkipAll
		}

		return nil
	})
	if fnErr != nil {
		return fnErr
	}
	if err != nil {
		return fmt.Errorf("store: list %s: %w", prefix, err)
	}

	return nil
}

// Delete implements ObjectStore.
func (s *FS) Delete(ctx context.Context, key string) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("store: delete %s: %w", key, err)
	}

	return nil
}

func (s *FS) putFile(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp := path.Join(s.own, rand.Text())
	if err := s.write(tmp, ctxReader{ctx, r}, size); err != nil {
		_ = s.root.Remove(tmp)
		return err
	}
	dir := path.Dir(key)
	err := s.root.MkdirAll(dir, 0o700)
	if err == nil {
		err = ctx.Err() // the last point at which nothing is visible
	}
	if err == nil {
		err = s.root.Rename(tmp, key)
	}
	if err != nil {
		_ = s.root.Remove(tmp)
		return err
	}
	// The rename is durable once its directory is synced, and a new directory once its parent is, up to the root.
	for d := dir; ; d = path.Dir(d) {
		if err := s.syncDir(d); err != nil {
			return err
		}
		if d == "." {
			return nil
		}
	}
}

// listStart returns the directory a listing of prefix walks, the deepest one the prefix names;
// ok is false when no key can lie under it.
func (s *FS) listStart(prefix string) (start string, ok bool, err error) {
	start = "."
	if i := strings.LastIndexByte(prefix, '/'); i >= 0 {
		start = prefix[:i]
	}
	if !fs.ValidPath(start) || start == partialDir || strings.HasPrefix(start, partialDir+"/") {
		return "", false, nil
	}
	info, err := s.root.Stat(start)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("store: list %s: %w", prefix, err)
	}

	return start, info.IsDir(), nil
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

// clearDead removes every store directory under .partial/ whose flock is free: its store crashed or closed.
// The caller holds partialLock, so no store is between creating its directory and locking it.
func clearDead(root *os.Root) error {
	dir, err := root.Open(partialDir)
	if err != nil {
		return err
	}
	entries, err := dir.ReadDir(-1)
	_ = dir.Close()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := path.Join(partialDir, e.Name())
		if beforeClear != nil {
			beforeClear(name)
		}
		f, err := root.Open(name)
		if errors.Is(err, fs.ErrNotExist) {
			continue // its store closed since the listing
		} else if err != nil {
			return err
		}
		err = flock(f, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			err = root.RemoveAll(name)
		} else if errors.Is(err, syscall.EWOULDBLOCK) {
			err = nil // a live store's
		}
		_ = f.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func flock(f *os.File, how int) error {
	for {
		err := syscall.Flock(int(f.Fd()), how)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
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

// ctxReader stops a copy once its context ends.
type ctxReader struct {
	ctx context.Context // scoped to one Put's copy
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}

	return c.r.Read(p)
}
