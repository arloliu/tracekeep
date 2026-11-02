package api

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// brittle creates the work files, the first attempt's file of its kind ("pack", a pack's JSONL, or "tar", the bundle) failing at op:
// its creation, a write after its first, its Seek, its Sync or its Close, each with a path error naming the file, as a full disk's would.
type brittle struct{ kind, op string }

func (b brittle) create(path string) (workFile, error) {
	first := strings.Contains(path, "-f1"+string(filepath.Separator))
	if !first || (filepath.Base(path) == "bundle.tar") != (b.kind == "tar") {
		return os.Create(path)
	}
	if b.op == "create" {
		return nil, &fs.PathError{Op: "open", Path: path, Err: syscall.ENOSPC}
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &brittleFile{File: f, op: b.op}, nil
}

type brittleFile struct {
	*os.File
	op     string
	writes int
}

func (f *brittleFile) fails(op string) error {
	if f.op != op {
		return nil
	}

	return &fs.PathError{Op: op, Path: f.Name(), Err: syscall.ENOSPC}
}

func (f *brittleFile) Write(p []byte) (int, error) {
	if f.writes++; f.writes > 1 {
		if err := f.fails("write"); err != nil {
			return 0, err
		}
	}

	return f.File.Write(p)
}

func (f *brittleFile) Seek(off int64, whence int) (int64, error) {
	if err := f.fails("seek"); err != nil {
		return 0, err
	}

	return f.File.Seek(off, whence)
}

func (f *brittleFile) Sync() error {
	if err := f.fails("sync"); err != nil {
		return err
	}

	return f.File.Sync()
}

func (f *brittleFile) Close() error {
	cerr := f.File.Close()
	if err := f.fails("close"); err != nil {
		return err
	}

	return cerr
}

// A failure of an attempt's own work file, a pack's or the bundle's, fails the attempt as a store's does: no pack is left out for it,
// the next attempt, under a new fence, exports every pack, and no path reaches the log, a reason or the manifest;
// a corrupt pack is still left out, its reason its own bytes'.
func TestExportWorkFileFailures(t *testing.T) {
	t.Parallel()
	for _, b := range []brittle{
		{"pack", "create"}, {"pack", "write"}, {"pack", "close"},
		{"tar", "write"}, {"tar", "seek"}, {"tar", "sync"}, {"tar", "close"},
	} {
		t.Run(b.kind+"/"+b.op, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				m := newMasked(t, c)
				capture := m.admitActive(t)
				cs := carriers(t)
				for i := range 2 {
					ms, entries := m.mask(t, capture, captured(recordsOf(cs[i:i+1], hour)))
					m.put(t, capture, hour, ms, entries, m.policy())
				}
				ms, entries := m.mask(t, capture, captured(recordsOf(cs[2:3], hour)))
				m.patch = func(p []byte) []byte { return flipped(p, 20) } // its file header, which then fails its CRC
				corrupt := m.put(t, capture, hour, ms, entries, m.policy())
				m.srv.createWork = b.create
				stale := workers(t, m)
				job := startExport(t, c, capture)
				j := await(t, c, job)
				if j.State != catalog.ExportDone || j.Fence != 2 {
					t.Fatalf("job = %+v, want done by the second attempt", j)
				}
				_, man := stored(t, m, *j.ObjectKey)
				h := man.Captures[0].Hours[0]
				if len(h.Packs) != 2 || len(h.LeftOut) != 1 || h.LeftOut[0].PackID != corrupt.ID.String() {
					t.Errorf("hour = %+v, want the two sound packs exported and the corrupt one left out", h)
				}
				dir := filepath.Dir(stale)
				if text := m.logs.String() + h.LeftOut[0].Reason; strings.Contains(text, dir) || strings.Contains(text, ".jsonl") || strings.Contains(text, "bundle.tar") {
					t.Errorf("a path reached the log or a reason:\n%s", text)
				}
				if !strings.Contains(m.logs.String(), "no space left on device") {
					t.Errorf("the log = %s\nwant the first attempt's end with its cause", m.logs)
				}
			})
		})
	}
}
