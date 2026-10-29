package ingest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/store"
)

// The no-clear-text boundary of phase 3.3: no recipe content of a masking configuration's records
// in a working file, a stored object, a pack's records or its metadata, whatever fails on the way.

// secretsOf is every secret the carriers hold, the whole-text one's included.
func secretsOf(t *testing.T) ([]maskingtest.Carrier, [][]byte) {
	t.Helper()
	b := policy.Baseline()
	carriers, err := maskingtest.Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}
	carriers = append(carriers, maskingtest.WholeText())
	var secrets [][]byte
	for _, c := range carriers {
		secrets = append(secrets, c.Secrets...)
	}

	return carriers, secrets
}

// noClearText fails when the bytes of a pack, decoded or not, hold a secret: its raw bytes,
// and, when they open as a pack, every record's payload and its metadata as encoded.
func noClearText(t *testing.T, where string, data []byte, secrets [][]byte) {
	t.Helper()
	check := func(what string, b []byte) {
		for _, s := range secrets {
			if bytes.Contains(b, s) {
				t.Errorf("%s: %s holds %q", where, what, s)
			}
		}
	}
	check("the raw bytes", data)
	r, err := tracepack.Open(context.Background(), bytes.NewReader(data), int64(len(data)), tracepack.ReaderOptions{})
	if err != nil {
		return // a working file still being written: its raw bytes are all there is
	}
	meta, err := r.Header().Meta.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	check("the metadata", meta)
	_, err = r.Iterate(context.Background(), tracepack.Query{Payloads: true}, func(it *tracepack.Item) error {
		check("a record", it.Record.Payload)
		return nil
	})
	if err != nil {
		t.Errorf("%s: %v", where, err)
	}
}

// dirClean checks every file under dir.
func dirClean(t *testing.T, dir string, secrets [][]byte) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		noClearText(t, path, b, secrets)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// storeClean checks every object of st.
func storeClean(t *testing.T, st store.ObjectStore, secrets [][]byte) {
	t.Helper()
	for _, k := range storedKeys(t, st) {
		obj, err := st.Open(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size()))
		_ = obj.Close()
		if err != nil {
			t.Fatal(err)
		}
		noClearText(t, k, b, secrets)
	}
}

// feedCarriers delivers a start and every carrier, inline and by claim-check by turns, from seq 1.
func feedCarriers(t *testing.T, p *pipeline, capture tracepack.UUID, carriers []maskingtest.Carrier) {
	t.Helper()
	p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
	feedFrom(t, p, capture, carriers, 1)
}

// feedFrom delivers carriers from seq first, inline and by claim-check by turns.
func feedFrom(t *testing.T, p *pipeline, capture tracepack.UUID, carriers []maskingtest.Carrier, first uint64) {
	t.Helper()
	for i := range carriers {
		r := carrierRecord(&carriers[i], first+uint64(i))
		m := inline(t, capture, r, nil)
		if i%2 == 1 {
			m = claimCheck(t, p.cl, capture, r, nil)
		}
		p.feed(t, m)
	}
}

// The working file, held at the write barrier before any record is appended and at the crash barrier after the put,
// and the stored segment, hold no secret; nor does any working file the flush leaves, there being none.
func TestBoundaryWorkingFiles(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		carriers, secrets := secretsOf(t)
		p := maskingPipeline(t, c, maskingKeys(t, c))
		inspected := 0
		inspect := func() error {
			dirClean(t, p.f.WorkDir, secrets)
			inspected++

			return nil
		}
		p.f.hooks[hookWrite], p.f.hooks[hookAfterPut] = inspect, inspect
		feedCarriers(t, p, tracepack.UUID{0x81}, carriers)
		p.flushAll(t)
		if inspected != 2 {
			t.Errorf("inspected %d times, want at the write and after the put", inspected)
		}
		storeClean(t, p.st, secrets)
		if left, _ := os.ReadDir(p.f.WorkDir); len(left) != 0 {
			t.Errorf("working files left: %v", left)
		}
	})
}

// A crash after the put leaves an orphan, a failed put leaves nothing, and a record masking cannot screen in the middle of a batch
// is nak'd while the masked ones before and after it are written: no secret anywhere, the working directory empty each time.
func TestBoundaryFailures(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		carriers, secrets := secretsOf(t)
		deep := maskingtest.TooDeep()
		secrets = append(secrets, deep.Secrets...)
		// A crash after the put: the orphan is masked.
		p := maskingPipeline(t, c, maskingKeys(t, c))
		p.f.hooks[hookAfterPut] = func() error { return errCrash }
		feedCarriers(t, p, tracepack.UUID{0x82}, carriers)
		if _, err := p.flushOne(t); err == nil {
			t.Fatal("Flush passed the crash")
		}
		storeClean(t, p.st, secrets)
		dirClean(t, p.f.WorkDir, secrets)
		// A failed put: the bytes it was given and the live working file hold no secret; nothing stored, nothing left.
		q := maskingPipeline(t, c, maskingKeys(t, c))
		fp := &inspectingPut{ObjectStore: q.st, t: t, secrets: secrets, workDir: q.f.WorkDir, fail: func(string) bool { return true }}
		q.f.Store = fp
		feedCarriers(t, q, tracepack.UUID{0x83}, carriers)
		if _, err := q.flushOne(t); err == nil {
			t.Fatal("Flush passed the failed put")
		}
		if fp.inspected != 1 {
			t.Errorf("inspected %d puts, want the failed one", fp.inspected)
		}
		if ks := storedKeys(t, q.st); len(ks) != 0 {
			t.Errorf("objects %v after a failed put", ks)
		}
		dirClean(t, q.f.WorkDir, secrets)
		// A record masking cannot screen amid masked ones.
		r := maskingPipeline(t, c, maskingKeys(t, c))
		capture := tracepack.UUID{0x84}
		feedCarriers(t, r, capture, carriers[:3])
		if _, dec := dispose(t, r.x, inline(t, capture, carrierRecord(&deep, 4), nil)); dec.Action != ActNak || dec.Reason != ReasonCannotScreen {
			t.Errorf("the record masking cannot screen = %+v, want nak'd", dec)
		}
		r.feed(t, inline(t, capture, carrierRecord(&carriers[3], 5), nil))
		r.flushAll(t)
		storeClean(t, r.st, secrets)
		dirClean(t, r.f.WorkDir, secrets)
	})
}

// inspectingPut checks, inside each put fail selects, the bytes the put was given and the live working directory, then fails it,
// as a crash at the put would leave them: the attempted artifact itself, before any cleanup.
type inspectingPut struct {
	store.ObjectStore
	t         *testing.T
	secrets   [][]byte
	workDir   string
	fail      func(key string) bool
	inspected int
}

func (s *inspectingPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if !s.fail(key) {
		return s.ObjectStore.Put(ctx, key, r, size)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if int64(len(b)) != size || size == 0 {
		s.t.Errorf("put %s: %d bytes of %d, want the whole artifact", key, len(b), size)
	}
	noClearText(s.t, "the attempted "+key, b, s.secrets)
	dirClean(s.t, s.workDir, s.secrets)
	s.inspected++

	return errCrash
}

// An interrupted merge of masked segments leaves no secret in the compactor's working directory or the store.
func TestBoundaryInterruptedMerge(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		carriers, secrets := secretsOf(t)
		p := maskingPipeline(t, c, maskingKeys(t, c))
		capture := tracepack.UUID{0x85}
		feedCarriers(t, p, capture, carriers[:5])
		p.flushAll(t)
		feedFrom(t, p, capture, carriers[5:], 6)
		p.flushAll(t)
		work := t.TempDir()
		lease, ok, err := c.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		archive := &inspectingPut{
			ObjectStore: p.st, t: t, secrets: secrets, workDir: work,
			fail: func(key string) bool { return bytes.HasPrefix([]byte(key), []byte("archive/")) },
		}
		r := &compact.Compactor{
			Catalog: c, Store: archive, HorizonHour: func() int64 { return HourOf(now) - 24 }, Log: slog.New(slog.DiscardHandler),
			WorkDir: work, Writer: "tracekeep-test", QuietPeriod: 15 * time.Minute, Now: func() time.Time { return time.Unix(0, now).Add(3 * time.Hour) },
		}
		if rep, err := r.Conflicts(t.Context(), lease); err != nil || rep.Confirmed != 0 {
			t.Fatalf("Conflicts = %+v, %v", rep, err)
		}
		rep, err := r.Merges(t.Context(), lease)
		if err != nil || len(rep.Failed) == 0 || archive.inspected == 0 {
			t.Fatalf("Merges = %+v, %v, %d archives inspected; want the merge interrupted at its archive's put", rep, err, archive.inspected)
		}
		dirClean(t, work, secrets)
		storeClean(t, p.st, secrets)
	})
}
