package api

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// unpacked is a bundle's files by name.
func unpacked(t *testing.T, path string) map[string][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]byte{}
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out
		} else if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		out[h.Name] = b
	}
}

func build(t *testing.T, m *masked, capture tracepack.UUID) (map[string][]byte, manifest) {
	t.Helper()
	req := &exportRequest{Kind: "canonical", Selector: exportSelector{ToolID: tool, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 2) * nsPerHour}}
	path, size, err := m.srv.buildBundle(t.Context(), newID(t), req, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(path); err != nil || st.Size() != size {
		t.Fatalf("bundle %d bytes, stat %v, %v", size, st, err)
	}
	files := unpacked(t, path)
	var man manifest
	if err := json.Unmarshal(files["manifest.json"], &man); err != nil {
		t.Fatal(err)
	}

	return files, man
}

// leaks are the carriers' secrets found in the JSONL files, every string value also base64-decoded.
func leaks(t *testing.T, files map[string][]byte, cs []maskingtest.Carrier) []string {
	t.Helper()
	var found []string
	for name, b := range files {
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(nil, 64<<20)
		for sc.Scan() {
			var v any
			if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
				t.Fatalf("%s: a line that is not JSON: %v", name, err)
			}
			blobs := [][]byte{sc.Bytes()}
			walk(v, func(s string) {
				blobs = append(blobs, []byte(s))
				if d, err := base64.StdEncoding.DecodeString(s); err == nil {
					blobs = append(blobs, d)
				}
			})
			for _, c := range cs {
				for _, secret := range c.Secrets {
					for _, blob := range blobs {
						if bytes.Contains(blob, secret) {
							found = append(found, c.Name)
						}
					}
				}
			}
		}
	}

	return found
}

// walk calls fn with every string value in v.
func walk(v any, fn func(string)) {
	switch x := v.(type) {
	case string:
		fn(x)
	case []any:
		for _, e := range x {
			walk(e, fn)
		}
	case map[string]any:
		for _, e := range x {
			walk(e, fn)
		}
	default:
	}
}

// A bundle of a masked capture holds each pack's JSONL and a manifest naming it, and no carrier's secret,
// every base64 field decoded; the same records unmasked, under tracekeep dev, show every secret: the scan can fail.
func TestExportBundleHoldsNoClearBody(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(cs, hour)))
		p := m.put(t, capture, hour, ms, entries, m.policy())
		files, man := build(t, m, capture)
		if len(man.Captures) != 1 || len(man.Captures[0].Hours) != 1 || len(man.Captures[0].Hours[0].Packs) != 1 {
			t.Fatalf("manifest = %+v, want one capture, one hour, one pack", man)
		}
		mp := man.Captures[0].Hours[0].Packs[0]
		if mp.PackID != p.ID.String() || mp.Schema != "tracepack-jsonl/2" || len(files[mp.File]) == 0 || man.BundleBuiltAt == "" {
			t.Errorf("pack entry %+v, file %d bytes, built at %q", mp, len(files[mp.File]), man.BundleBuiltAt)
		}
		if got := leaks(t, files, cs); len(got) != 0 {
			t.Errorf("clear secrets of %v in the bundle", got)
		}

		unmasked := m.admitUnmasked(t)
		m.put(t, unmasked, hour, captured(recordsOf(cs, hour)), nil, nil)
		m.srv.Dev = true
		files, _ = build(t, m, unmasked)
		if got := leaks(t, files, cs); len(got) == 0 {
			t.Error("the unmasked control shows no secret: the scan cannot fail")
		}
	})
}

// A withheld capture is named and holds no pack; a pack whose object is gone, again after its hour is read again, is left out and named.
func TestExportBundleWithheldAndGone(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		withheld := m.admitUnmasked(t)
		m.put(t, withheld, hour, captured(recordsOf(carriers(t)[:1], hour)), nil, nil)
		files, man := build(t, m, withheld)
		if len(man.Captures) != 1 || !man.Captures[0].Withheld || len(files) != 1 {
			t.Errorf("a withheld capture's bundle = %+v with %d files; want it named withheld and the manifest alone", man, len(files))
		}

		m.srv.Dev = true // the newest configuration has no policy
		capture := m.admitActive(t)
		p := m.put(t, capture, hour, captured(recordsOf(carriers(t)[:2], hour)), nil, nil)
		if err := m.st.Delete(t.Context(), p.Key); err != nil {
			t.Fatal(err)
		}
		_, man = build(t, m, capture)
		h := man.Captures[0].Hours[0]
		if len(h.Packs) != 0 || len(h.LeftOut) != 1 || h.LeftOut[0].PackID != p.ID.String() || strings.Contains(h.LeftOut[0].Reason, p.Key) {
			t.Errorf("hour = %+v, want its pack left out, named, its reason without the key", h)
		}
	})
}

// A bundle past its packs' bound fails.
func TestExportBundleBounds(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		for i := range 3 {
			m.put(t, capture, hour, captured(recordsOf(carriers(t)[i:i+1], hour)), nil, nil)
		}
		m.srv.Exports.Bounds.MaxPacks = 2
		req := &exportRequest{Kind: "canonical", Selector: exportSelector{ToolID: tool, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 1) * nsPerHour}}
		if _, _, err := m.srv.buildBundle(t.Context(), newID(t), req, t.TempDir()); !errors.Is(err, errBounds) {
			t.Errorf("three packs, at most two = %v, want errBounds", err)
		}
		m.srv.Exports.Bounds = ExportBounds{MaxBundleBytes: 512}
		if _, _, err := m.srv.buildBundle(t.Context(), newID(t), req, t.TempDir()); !errors.Is(err, errBounds) {
			t.Errorf("a work directory past 512 bytes = %v, want errBounds", err)
		}
	})
}

// flaky is a store whose first open of a key in gone finds it gone, as a merge between two reads of an hour would.
type flaky struct {
	store.ObjectStore
	mu   sync.Mutex
	gone map[string]bool
}

func (f *flaky) Open(ctx context.Context, key string) (store.Object, error) {
	f.mu.Lock()
	first := f.gone[key]
	delete(f.gone, key)
	f.mu.Unlock()
	if first {
		return nil, fmt.Errorf("%w: %s", store.ErrNotFound, key)
	}

	return f.ObjectStore.Open(ctx, key)
}

// An object found gone discards the hour's exported files and exports the hour again, once: every pack once in the bundle.
func TestExportBundleRetriesAnHour(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		cs := carriers(t)
		a := m.put(t, capture, hour, captured(recordsOf(cs[:1], hour)), nil, nil)
		b := m.put(t, capture, hour, captured(recordsOf(cs[1:2], hour)), nil, nil)
		// The later of the two in the read set's order is the one found gone.
		later := b
		if b.ID.String() < a.ID.String() {
			later = a
		}
		m.srv.Store = &flaky{ObjectStore: m.st, gone: map[string]bool{later.Key: true}}
		files, man := build(t, m, capture)
		h := man.Captures[0].Hours[0]
		if len(h.Packs) != 2 || len(h.LeftOut) != 0 {
			t.Fatalf("hour = %+v, want both packs, none left out", h)
		}
		if n := len(files); n != 3 {
			t.Errorf("%d files in the bundle, want two packs and the manifest", n)
		}
	})
}
