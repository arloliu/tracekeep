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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
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
		if _, dup := out[h.Name]; dup {
			t.Fatalf("the bundle holds %s twice", h.Name)
		}
		out[h.Name] = b
	}
}

func build(t *testing.T, m *masked, capture tracepack.UUID) (map[string][]byte, manifest) {
	t.Helper()
	req := &exportRequest{Kind: "canonical", Selector: exportSelector{EquipmentID: equipment, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 2) * nsPerHour}}
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

// scanned is what a scan of a bundle read: the JSONL record lines, the redaction entries the JSONL headers hold,
// and each carrier's secret it found, as name:secret.
type scanned struct {
	records, entries int
	found            []string
}

// scan reads a bundle's JSONL files and manifest.json for cs's secrets, in every line, every key and string value,
// and every string base64-decoded, counting the record lines and the headers' redaction entries.
func scan(t *testing.T, files map[string][]byte, cs []maskingtest.Carrier) scanned {
	t.Helper()
	var out scanned
	look := func(line []byte, v any) {
		blobs := [][]byte{line}
		walk(v, func(s string) {
			blobs = append(blobs, []byte(s))
			if d, err := base64.StdEncoding.DecodeString(s); err == nil {
				blobs = append(blobs, d)
			}
		})
		for _, c := range cs {
			for _, secret := range c.Secrets {
				if slices.ContainsFunc(blobs, func(b []byte) bool { return bytes.Contains(b, secret) }) {
					out.found = append(out.found, c.Name+":"+string(secret))
				}
			}
		}
	}
	for name, b := range files {
		if name == "manifest.json" {
			var v any
			if err := json.Unmarshal(b, &v); err != nil {
				t.Fatalf("manifest.json: %v", err)
			}
			look(b, v)
		}
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(nil, 64<<20)
		for sc.Scan() {
			var v map[string]any
			if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
				t.Fatalf("%s: a line that is not a JSON object: %v", name, err)
			}
			if _, header := v["schema"]; header {
				md, _ := v["metadata"].(map[string]any)
				entries, _ := md["redaction"].([]any)
				out.entries += len(entries)
			} else if _, ok := v["seq"]; ok {
				out.records++
			}
			look(sc.Bytes(), v)
		}
		if err := sc.Err(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	return out
}

// walk calls fn with every string value in v and every key of its objects.
func walk(v any, fn func(string)) {
	switch x := v.(type) {
	case string:
		fn(x)
	case []any:
		for _, e := range x {
			walk(e, fn)
		}
	case map[string]any:
		for k, e := range x {
			fn(k)
			walk(e, fn)
		}
	default:
	}
}

// A bundle of a masked capture holds each pack's JSONL and a manifest naming it, and no carrier's secret, TooDeep's included,
// every base64 field decoded; the scan counts every record and redaction entry written.
// The control, records unmasked under tracekeep dev with clear text in a redaction entry and in a header extension too,
// shows every secret: the scan can fail on each, the headers' included.
func TestExportBundleHoldsNoClearBody(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)
		inEntry := maskingtest.Carrier{Name: "redaction entry", Secrets: [][]byte{[]byte("SECRET-IN-AN-ENTRY")}}
		inExtension := maskingtest.Carrier{Name: "header extension", Secrets: [][]byte{[]byte("SECRET-IN-AN-EXTENSION")}}
		search := append(slices.Clone(cs), maskingtest.TooDeep(), inEntry, inExtension)
		rs := captured(recordsOf(cs, hour))
		ms, entries := m.mask(t, capture, rs)
		p := m.put(t, capture, hour, ms, entries, m.policy())
		files, man := build(t, m, capture)
		if len(man.Captures) != 1 || len(man.Captures[0].Hours) != 1 || len(man.Captures[0].Hours[0].Packs) != 1 {
			t.Fatalf("manifest = %+v, want one capture, one hour, one pack", man)
		}
		mp := man.Captures[0].Hours[0].Packs[0]
		if mp.PackID != p.ID.String() || mp.Schema != "tracepack-jsonl/2" || len(files[mp.File]) == 0 || man.BundleBuiltAt == "" {
			t.Errorf("pack entry %+v, file %d bytes, built at %q", mp, len(files[mp.File]), man.BundleBuiltAt)
		}
		if got := scan(t, files, search); len(got.found) != 0 || got.records != len(rs) || got.entries != len(entries) || len(entries) == 0 {
			t.Errorf("scan = %+v; want no secret, %d records and %d redaction entries read", got, len(rs), len(entries))
		}

		unmasked := m.admitUnmasked(t)
		plain := captured(recordsOf(append(slices.Clone(cs), maskingtest.TooDeep()), hour))
		// One record more, masked, whose first redaction entry names its domain in clear text.
		mx, ex := m.mask(t, unmasked, captured([]tracepack.Record{recordAt(&cs[0], hour, uint64(len(plain)+1))}))
		ex[0].Domain = string(inEntry.Secrets[0])
		plain = append(plain, mx...)
		m.extensions = []tracepack.RawEntry{{Tag: 0x8001, Type: 7, Value: inExtension.Secrets[0]}} // a private tag of type bytes
		m.put(t, unmasked, hour, plain, ex, m.policy())
		m.srv.Dev = true
		files, _ = build(t, m, unmasked)
		// Every secret, the data records' found only in their base64 payloads: the scan can fail on each.
		got := scan(t, files, search)
		for _, c := range search {
			for _, secret := range c.Secrets {
				if !slices.Contains(got.found, c.Name+":"+string(secret)) {
					t.Errorf("the unmasked control does not show %s's secret %q: the scan cannot fail on it", c.Name, secret)
				}
			}
		}
		if got.records != len(plain) || got.entries != len(ex) {
			t.Errorf("the control's scan read %d records and %d redaction entries, want %d and %d", got.records, got.entries, len(plain), len(ex))
		}
	})
}

// A piece of equipment's export holding a withheld capture beside a masked one: the withheld capture is named and holds no pack,
// and the scan reads every record and redaction entry of the masked one, and no secret.
func TestExportOfAToolHoldsNoClearBody(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		cs := carriers(t)
		capture := m.admitActive(t)
		rs := captured(recordsOf(cs, hour))
		ms, entries := m.mask(t, capture, rs)
		m.put(t, capture, hour, ms, entries, m.policy())
		withheld := m.admitUnmasked(t) // bound to a configuration without a policy, activated after the masked capture's
		m.put(t, withheld, hour, captured(recordsOf(cs, hour)), nil, nil)
		req := &exportRequest{Kind: "canonical", Selector: exportSelector{EquipmentID: equipment, From: hour * nsPerHour, To: (hour + 1) * nsPerHour}}
		path, _, err := m.srv.buildBundle(t.Context(), newID(t), req, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		files := unpacked(t, path)
		var man manifest
		if err := json.Unmarshal(files["manifest.json"], &man); err != nil {
			t.Fatal(err)
		}
		withholding := 0
		for _, mc := range man.Captures {
			if mc.Withheld {
				withholding++
				if len(mc.Hours) != 0 {
					t.Errorf("the withheld capture holds hours %+v", mc.Hours)
				}
			}
		}
		if len(man.Captures) != 2 || withholding != 1 {
			t.Errorf("manifest captures = %+v, want two, one withheld", man.Captures)
		}
		if got := scan(t, files, append(cs, maskingtest.TooDeep())); len(got.found) != 0 || got.records != len(rs) || got.entries != len(entries) {
			t.Errorf("scan = %+v; want no secret, %d records and %d redaction entries read", got, len(rs), len(entries))
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
		req := &exportRequest{Kind: "canonical", Selector: exportSelector{EquipmentID: equipment, Capture: capture.String(), From: hour * nsPerHour, To: (hour + 1) * nsPerHour}}
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
	// onGone, when set, runs as an object is found gone: what removed it, such as retention evicting its hour.
	onGone func()
}

func (f *flaky) Open(ctx context.Context, key string) (store.Object, error) {
	f.mu.Lock()
	first := f.gone[key]
	delete(f.gone, key)
	onGone := f.onGone
	f.mu.Unlock()
	if first {
		if onGone != nil {
			onGone()
		}
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

// The manifest carries the capture's conflicts in the window with their states, whichever hour holds the other version:
// one suspect and one acknowledged across two hours.
func TestExportBundleConflictsAcrossHours(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		cs := carriers(t)
		at := func(h int64, seq uint64) tracepack.Record {
			r := recordAt(&cs[0], h, seq)
			r.SetCapturedFieldValidity()
			return r
		}
		m.put(t, capture, hour, []tracepack.Record{at(hour, 1), at(hour, 2), at(hour, 3)}, nil, nil)
		m.put(t, capture, hour+1, []tracepack.Record{at(hour+1, 3)}, nil, nil) // seq 3 again, in the next hour: a suspect
		m.put(t, capture, hour, []tracepack.Record{at(hour, 5)}, nil, nil)
		m.put(t, capture, hour+1, []tracepack.Record{at(hour+1, 5)}, nil, nil) // seq 5 again: a suspect, then acknowledged
		if _, err := c.AcknowledgeConflict(t.Context(), capture, 5, "op", "a1"); err != nil {
			t.Fatal(err)
		}
		_, man := build(t, m, capture)
		var comp tkv1.Completeness
		if err := protojson.Unmarshal(man.Captures[0].Completeness, &comp); err != nil {
			t.Fatal(err)
		}
		states := map[uint64]string{}
		for _, k := range comp.GetConflicts() {
			states[k.GetSeqFrom()] = k.GetState()
		}
		if states[3] != "suspect" || states[5] != "acknowledged" {
			t.Errorf("conflicts = %v, want seq 3 suspect and seq 5 acknowledged", states)
		}
		if len(man.Captures[0].Hours) != 2 {
			t.Errorf("hours = %+v, want both", man.Captures[0].Hours)
		}
	})
}

// Retention evicting the hour while it is exported: the object found gone, the hour's read set read again finds no scope,
// and the manifest names the hour evicted, with none of its packs.
func TestExportBundleHourEvictedDuringItsRead(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		now := deadlineOf().Add(time.Hour)
		m.urls(t, &now, true)
		capture := m.admitUnmasked(t)
		p := m.put(t, capture, hour, captured(recordsOf(carriers(t)[:1], hour)), nil, nil)
		m.srv.Store = &flaky{ObjectStore: m.st, gone: map[string]bool{p.Key: true}, onGone: func() {
			if ev, err := c.EvictHour(context.Background(), m.lease, catalog.Scope{Capture: capture, Hour: hour}, now.UnixNano()); err != nil || !ev.Evicted {
				t.Errorf("EvictHour = %+v, %v", ev, err)
			}
		}}
		_, man := build(t, m, capture)
		h := man.Captures[0].Hours[0]
		if !h.Evicted || len(h.Packs) != 0 {
			t.Errorf("hour = %+v, want it evicted, without packs", h)
		}
	})
}
