package ingest

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
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

// noClearText fails when the pack data holds a secret in its raw bytes, its metadata as encoded or a record's payload,
// and when it does not open: a pack's records are compressed, so only the decoded pack shows what it holds.
// It returns the records it read.
func noClearText(t testing.TB, where string, data []byte, secrets [][]byte) int {
	t.Helper()
	textClean(t, where+": the raw bytes", data, secrets)
	r, err := tracepack.Open(context.Background(), bytes.NewReader(data), int64(len(data)), tracepack.ReaderOptions{})
	if err != nil {
		t.Errorf("%s does not open: %v", where, err)
		return 0
	}
	meta, err := r.Header().Meta.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	textClean(t, where+": the metadata", meta, secrets)
	n := 0
	_, err = r.Iterate(context.Background(), tracepack.Query{Payloads: true}, func(it *tracepack.Item) error {
		textClean(t, where+": a record", it.Record.Payload, secrets)
		n++
		return nil
	})
	if err != nil {
		t.Errorf("%s: %v", where, err)
	}

	return n
}

// textClean fails when b holds a secret, as it is or hex-encoded.
func textClean(t testing.TB, where string, b []byte, secrets [][]byte) {
	t.Helper()
	for _, s := range secrets {
		if bytes.Contains(b, s) || bytes.Contains(b, []byte(hex.EncodeToString(s))) {
			t.Errorf("%s holds %q", where, s)
		}
	}
}

// dirClean checks every file under dir, each a complete working file, and fails when it finds none; it returns the records it read.
func dirClean(t testing.TB, dir string, secrets [][]byte) int {
	t.Helper()
	files, n := 0, 0
	err := filepath.WalkDir(dir, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		n += noClearText(t, path, b, secrets)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Errorf("%s: no working file to check", dir)
	}

	return n
}

// dirEmpty fails when dir holds a file: a working file outlives its flush or merge only by a crash.
func dirEmpty(t testing.TB, dir string) {
	t.Helper()
	if left, err := os.ReadDir(dir); err != nil || len(left) != 0 {
		t.Errorf("working files left in %s: %v, %v", dir, left, err)
	}
}

// storeClean checks every object of st, each a pack, and fails when it finds none; it returns the records it read.
func storeClean(t testing.TB, st store.ObjectStore, secrets [][]byte) int {
	t.Helper()
	ks, n := storedKeys(t, st), 0
	if len(ks) == 0 {
		t.Errorf("no stored object to check")
	}
	for _, k := range ks {
		obj, err := st.Open(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size()))
		_ = obj.Close()
		if err != nil {
			t.Fatal(err)
		}
		n += noClearText(t, k, b, secrets)
	}

	return n
}

// recorder is a testing.TB that keeps what a check reports instead of failing, so a test can see the check fail.
type recorder struct {
	testing.TB
	reports []string
}

func (r *recorder) Errorf(format string, args ...any) {
	r.reports = append(r.reports, fmt.Sprintf(format, args...))
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

// The working file, held at the write barrier with its metadata and no record yet and after the put with every record,
// and the stored segment, hold no secret; nor does any working file the flush leaves, there being none.
func TestBoundaryWorkingFiles(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		carriers, secrets := secretsOf(t)
		p := maskingPipeline(t, c, maskingKeys(t, c))
		var read []int
		inspect := func() error {
			read = append(read, dirClean(t, p.f.WorkDir, secrets))
			return nil
		}
		p.f.hooks[hookWrite], p.f.hooks[hookAfterPut] = inspect, inspect
		feedCarriers(t, p, tracepack.UUID{0x81}, carriers)
		p.flushAll(t)
		if want := []int{0, len(carriers) + 1}; !slices.Equal(read, want) {
			t.Errorf("records read at the write and after the put %v, want %v", read, want)
		}
		if n := storeClean(t, p.st, secrets); n != len(carriers)+1 {
			t.Errorf("%d records stored, want the start and every carrier", n)
		}
		dirEmpty(t, p.f.WorkDir)
	})
}

// The scan catches a secret: a policy-less configuration stores a carrier as received,
// and the checks of its working file and of the stored segment report it in the decoded record.
func TestBoundaryScanCatchesASecret(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		carriers, secrets := secretsOf(t)
		p := newPipeline(t, c)
		dir, stored := &recorder{TB: t}, &recorder{TB: t}
		p.f.hooks[hookAfterPut] = func() error {
			dirClean(dir, p.f.WorkDir, secrets)
			return nil
		}
		feedCarriers(t, p, tracepack.UUID{0x86}, carriers)
		p.flushAll(t)
		storeClean(stored, p.st, secrets)
		for what, r := range map[string]*recorder{"the working file": dir, "the stored segment": stored} {
			if !slices.ContainsFunc(r.reports, func(s string) bool { return strings.Contains(s, ": a record holds ") }) {
				t.Errorf("the check of %s reported %q, want a record holding a secret", what, r.reports)
			}
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
		// A crash after the put: the working file it held and the orphan are masked.
		p := maskingPipeline(t, c, maskingKeys(t, c))
		held := 0
		p.f.hooks[hookAfterPut] = func() error {
			held = dirClean(t, p.f.WorkDir, secrets)
			return errCrash
		}
		feedCarriers(t, p, tracepack.UUID{0x82}, carriers)
		if _, err := p.flushOne(t); err == nil {
			t.Fatal("Flush passed the crash")
		}
		if n := storeClean(t, p.st, secrets); held != len(carriers)+1 || n != held {
			t.Errorf("%d records held at the crash, %d stored; want the start and every carrier", held, n)
		}
		dirEmpty(t, p.f.WorkDir)
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
		dirEmpty(t, q.f.WorkDir)
		// A record masking cannot screen amid masked ones.
		r := maskingPipeline(t, c, maskingKeys(t, c))
		capture := tracepack.UUID{0x84}
		feedCarriers(t, r, capture, carriers[:3])
		if _, dec := dispose(t, r.x, inline(t, capture, carrierRecord(&deep, 4), nil)); dec.Action != ActNak || dec.Reason != ReasonCannotScreen {
			t.Errorf("the record masking cannot screen = %+v, want nak'd", dec)
		}
		r.feed(t, inline(t, capture, carrierRecord(&carriers[3], 5), nil))
		r.flushAll(t)
		if n := storeClean(t, r.st, secrets); n != 5 {
			t.Errorf("%d records stored, want the start and the four masked carriers", n)
		}
		dirEmpty(t, r.f.WorkDir)
	})
}

// inspectingPut checks, inside each put, the bytes the put was given and the live working directory,
// then fails the put when fail selects it, as a crash at the put would leave them: the attempted artifact itself, before any cleanup.
type inspectingPut struct {
	store.ObjectStore
	t         *testing.T
	secrets   [][]byte
	workDir   string
	fail      func(key string) bool
	inspected int
}

func (s *inspectingPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if int64(len(b)) != size || size == 0 {
		s.t.Errorf("put %s: %d bytes of %d, want the whole artifact", key, len(b), size)
	}
	noClearText(s.t, "the put "+key, b, s.secrets)
	dirClean(s.t, s.workDir, s.secrets)
	s.inspected++
	if s.fail(key) {
		return errCrash
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return s.ObjectStore.Put(ctx, key, r, size)
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
		var logs bytes.Buffer
		r := &compact.Compactor{
			Catalog: c, Store: archive, HorizonHour: func() int64 { return HourOf(now) - 24 }, Log: slog.New(slog.NewTextHandler(&logs, nil)),
			WorkDir: work, Writer: "tracekeep-test", QuietPeriod: 15 * time.Minute, Now: func() time.Time { return time.Unix(0, now).Add(3 * time.Hour) },
		}
		if rep, err := r.Conflicts(t.Context(), lease); err != nil || rep.Confirmed != 0 {
			t.Fatalf("Conflicts = %+v, %v", rep, err)
		}
		rep, err := r.Merges(t.Context(), lease)
		if err != nil || len(rep.Failed) == 0 || archive.inspected == 0 {
			t.Fatalf("Merges = %+v, %v, %d archives inspected; want the merge interrupted at its archive's put", rep, err, archive.inspected)
		}
		dirEmpty(t, work)
		if n := storeClean(t, p.st, secrets); n != len(carriers)+1 {
			t.Errorf("%d records stored, want the start and every carrier", n)
		}
		// The failed merge is logged, and neither its log line nor the report's errors hold a secret.
		if logs.Len() == 0 {
			t.Error("the interrupted merge logged nothing")
		}
		textClean(t, "the compactor's log", logs.Bytes(), secrets)
		textClean(t, "the merge report", fmt.Appendf(nil, "%+v", rep), secrets)
	})
}

// One run of a masked capture through ingest, the API and the compactor leaves no secret where the service writes:
// every stored object, ingest's and the compactor's working directories while a flush, a merge or a close holds its files,
// the logs of all three, every error returned and every audit row of the capture.
// The run takes a failed put, a refused item, the merge, the close and the deletions.
// The traffic and parking streams and the claim bucket hold clear text by design and are not scanned.
func TestNoClearTextAcrossARun(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		carriers, secrets := secretsOf(t)
		deep := maskingtest.TooDeep()
		secrets = append(secrets, deep.Secrets...)
		p := newPipeline(t, c)
		k := activeMasking(t, p)
		var logs bytes.Buffer
		log := slog.New(slog.NewTextHandler(&logs, nil))
		capture := tracepack.UUID{0x87}
		errs := ingestARun(t, p, p.ingester(log), capture, carriers, &deep, secrets)
		recordErrs := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				resp, err := next(ctx, req)
				errs = append(errs, err)

				return resp, err
			}
		})
		cl := served(t, p, k, log, connect.WithInterceptors(recordErrs))
		restoreAll(t, cl, capture, carriers, "the segments")
		compactARun(t, p, log, len(carriers), secrets)
		restoreAll(t, cl, capture, carriers, "the archive")

		textClean(t, "the logs", logs.Bytes(), secrets)
		for _, want := range []string{"ingest: alert", "compact: capture closed"} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("the logs lack %q", want)
			}
		}
		failed := 0
		for _, err := range errs {
			if err != nil {
				textClean(t, "the error "+strconv.Quote(err.Error()), []byte(err.Error()), secrets)
				failed++
			}
		}
		if failed < 4 {
			t.Errorf("%d errors returned, want the failed put, the refused item and a refused restore from each kind of pack", failed)
		}
		rows, err := c.AuditEntries(t.Context(), capture)
		if err != nil || len(rows) == 0 {
			t.Fatalf("audit rows %v, %v", rows, err)
		}
		for _, row := range rows {
			textClean(t, "an audit row", []byte(strings.Join([]string{row.Actor, row.Action, row.ActionID, row.Detail}, " ")), secrets)
		}
	})
}

// ingestARun feeds the start and half the carriers through g, the first put failing and the batch flushed again as its redelivery would be,
// then the other half and a record masking cannot screen, which g refuses;
// it checks ingest's working directory at every flush barrier and the store after, and returns the errors ingest returned.
func ingestARun(t *testing.T, p *pipeline, g *Ingester, capture tracepack.UUID, carriers []maskingtest.Carrier, deep *maskingtest.Carrier, secrets [][]byte) []error {
	t.Helper()
	puts := 0
	put := &inspectingPut{ObjectStore: p.st, t: t, secrets: secrets, workDir: p.f.WorkDir, fail: func(string) bool { puts++; return puts == 1 }}
	p.f.Store = put
	held := map[int]int{}
	for _, h := range []int{hookWrite, hookAfterPut, hookAfterRegister, hookAfterAck} {
		p.f.hooks[h] = func() error {
			dirClean(t, p.f.WorkDir, secrets)
			held[h]++

			return nil
		}
	}
	half := len(carriers) / 2
	feedCarriers(t, p, capture, carriers[:half])
	batches := p.b.All()
	failedPut := g.flush(t.Context(), batches)
	if !errors.Is(failedPut, errCrash) {
		t.Fatalf("the flush = %v, want the failed put", failedPut)
	}
	if err := g.flush(t.Context(), batches); err != nil {
		t.Fatal(err)
	}
	feedFrom(t, p, capture, carriers[half:], uint64(half+1)) //nolint:gosec // a small count
	d, dec := dispose(t, p.x, inline(t, capture, carrierRecord(deep, uint64(len(carriers)+1)), nil))
	if dec.Action != ActNak || d.MaskErr == nil {
		t.Fatalf("the record masking cannot screen = %+v, %v; want nak'd", dec, d.MaskErr)
	}
	g.act(t.Context(), Entry{D: d, Msg: &msg{}}, dec, 1)
	if err := g.flush(t.Context(), p.b.All()); err != nil {
		t.Fatal(err)
	}
	for _, h := range []int{hookWrite, hookAfterPut, hookAfterRegister, hookAfterAck} {
		if held[h] == 0 {
			t.Errorf("flush barrier %d never held", h)
		}
	}
	if put.inspected != 3 {
		t.Errorf("inspected %d puts, want the failed one and two segments", put.inspected)
	}
	dirEmpty(t, p.f.WorkDir)
	if n := storeClean(t, p.st, secrets); n != len(carriers)+1 {
		t.Errorf("%d records stored, want the start and every carrier", n)
	}

	return []error{failedPut, d.MaskErr}
}

// compactARun merges the capture's hour, closes the capture and deletes the merged segments, logging to log;
// it checks the compactor's working directory at every put and the store after, which holds the archive of the start and n carriers, and the close.
func compactARun(t *testing.T, p *pipeline, log *slog.Logger, n int, secrets [][]byte) {
	t.Helper()
	work := t.TempDir()
	put := &inspectingPut{ObjectStore: p.st, t: t, secrets: secrets, workDir: work, fail: func(string) bool { return false }}
	lease, ok, err := p.cat.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	at := time.Unix(0, now).Add(3 * time.Hour)
	r := &compact.Compactor{
		Catalog: p.cat, Store: put, HorizonHour: func() int64 { return HourOf(now) - 24 }, Log: log, WorkDir: work, Writer: "tracekeep-test",
		QuietPeriod: 15 * time.Minute, DeleteDelay: time.Minute, Inactivity: 2 * time.Hour, FlushInterval: time.Second, Now: func() time.Time { return at },
	}
	if rep, err := r.Merges(t.Context(), lease); err != nil || len(rep.Merged) != 1 || len(rep.Failed)+len(rep.Conflicted)+len(rep.Stale) != 0 {
		t.Fatalf("Merges = %+v, %v; want the hour merged", rep, err)
	}
	// The close and the deletions judged against the catalog's clock, which stamps the capture's registrations and the merge's commit.
	at = time.Now().Add(3 * time.Hour)
	if rep, err := r.Closes(t.Context(), lease); err != nil || rep.Published != 1 {
		t.Fatalf("Closes = %+v, %v; want the capture closed", rep, err)
	}
	if err := r.Deletions(t.Context(), lease, &compact.RetentionReport{}); err != nil {
		t.Fatal(err)
	}
	if put.inspected != 2 {
		t.Errorf("inspected %d puts, want the archive and the close", put.inspected)
	}
	dirEmpty(t, work)
	if ks := storedKeys(t, p.st); len(ks) != 2 {
		t.Errorf("objects %v, want the archive and the close", ks)
	}
	if got := storeClean(t, p.st, secrets); got != n+2 {
		t.Errorf("%d records stored, want the start, the %d carriers and the close's", got, n)
	}
}
