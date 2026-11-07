package catalog

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// logFile is the test's log file of equipment T1.
var logFile = LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}

// logDescriptor is a log capture's descriptor of device.
func logDescriptor(t testing.TB, device string) *Descriptor {
	t.Helper()

	return &Descriptor{
		Bytes: []byte("log-" + device), EquipmentID: "T1", RecorderInstanceID: newID(t), Device: device, Source: "feed-a", OriginNS: 1,
		CaptureMethod: "log", Vantage: "host", SourceDialect: "host-sml-feed/1",
	}
}

// logGroup ensures the lease groups and takes group 1's lease for the holder "a".
func logGroup(t *testing.T, c *Catalog) Lease {
	t.Helper()
	if err := c.EnsureLogGroups(t.Context(), 4); err != nil {
		t.Fatal(err)
	}
	l, ok, err := c.TakeLease(t.Context(), LogGroupLease(1), "a", ttl)
	if err != nil || !ok {
		t.Fatalf("take group 1's lease = %v, %v", ok, err)
	}

	return l
}

// admitLog admits a log capture of device in logFile under l.
func admitLog(t *testing.T, c *Catalog, l Lease, device string) tracepack.UUID {
	t.Helper()
	id := newID(t)
	res, err := c.AdmitLog(t.Context(), l, LogKey{LogFile: logFile, Device: device}, id, logDescriptor(t, device), nil)
	if err != nil || res.Found || res.Capture != id {
		t.Fatalf("AdmitLog of %s = %+v, %v; want %s admitted", device, res, err, id)
	}

	return id
}

func watermark(t *testing.T, c *Catalog, f LogFile) (int64, bool) {
	t.Helper()
	var row struct {
		Watermark int64 `db:"watermark"`
		Stopped   bool  `db:"stopped"`
	}
	err := c.read.GetContext(t.Context(), &row, `SELECT watermark, stopped FROM logfeed_files WHERE source = ? AND equipment_id = ? AND file = ?`, f.Source, f.EquipmentID, f.File)
	if err != nil {
		t.Fatal(err)
	}

	return row.Watermark, row.Stopped
}

func packCount(t *testing.T, c *Catalog, ids ...tracepack.UUID) int {
	t.Helper()
	n := 0
	for _, id := range ids {
		var k int
		if err := c.read.GetContext(t.Context(), &k, `SELECT COUNT(*) FROM packs WHERE pack_id = ?`, id[:]); err != nil {
			t.Fatal(err)
		}
		n += k
	}

	return n
}

// flushFacts is what a flush changes: the captures' highest seqs and revisions, the scopes and events, the file's watermark and the keys' groups.
type logFacts struct {
	highest, revisions, scopes, events, watermark, groups int64
}

func flushFacts(t *testing.T, c *Catalog, captures ...tracepack.UUID) logFacts {
	t.Helper()
	var f logFacts
	for _, id := range captures {
		var row struct {
			Highest  sql.NullInt64 `db:"highest_seq"`
			Revision int64         `db:"data_revision"`
		}
		if err := c.read.GetContext(t.Context(), &row, `SELECT highest_seq, data_revision FROM captures WHERE capture_id = ?`, id[:]); err != nil {
			t.Fatal(err)
		}
		f.highest += row.Highest.Int64
		f.revisions += row.Revision
	}
	for q, v := range map[string]*int64{
		`SELECT COALESCE(SUM(revision), 0) FROM scopes`: &f.scopes, `SELECT COUNT(*) FROM scope_events`: &f.events,
		`SELECT COALESCE(SUM(watermark), 0) FROM logfeed_files`: &f.watermark, `SELECT COALESCE(SUM(lease_group), 0) FROM logfeed_captures`: &f.groups,
	} {
		if err := c.read.GetContext(t.Context(), v, q); err != nil {
			t.Fatal(err)
		}
	}

	return f
}

// A log capture is admitted once for its key, open with its method, vantage and dialect;
// the key's second admission returns the first capture and writes nothing, and a lost lease admits nothing.
func TestAdmitLog(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		l := logGroup(t, c)
		none := newID(t)
		if _, err := c.AdmitLog(ctx, l, LogKey{LogFile: logFile, Device: "eqp"}, none, logDescriptor(t, "eqp"), nil); !errors.Is(err, ErrNoBindableConfig) {
			t.Fatalf("an admission with no bindable configuration = %v, want ErrNoBindableConfig", err)
		}
		var files int
		if err := c.read.GetContext(ctx, &files, `SELECT COUNT(*) FROM logfeed_files`); err != nil || files != 0 {
			t.Errorf("an unbindable admission left %d file rows (%v)", files, err)
		}
		if _, err := c.Capture(ctx, none); !errors.Is(err, ErrUnknownCapture) {
			t.Errorf("an unbindable admission's capture = %v, want no row", err)
		}
		cfg := gate(t, c)
		id := admitLog(t, c, l, "eqp")
		var row struct {
			State   string `db:"state"`
			Method  string `db:"capture_method"`
			Vantage string `db:"vantage"`
			Dialect string `db:"source_dialect"`
			Config  int64  `db:"config_id"`
			Group   int    `db:"lease_group"`
		}
		err := c.read.GetContext(ctx, &row, `SELECT c.state, c.capture_method, c.vantage, c.source_dialect, c.config_id, k.lease_group
			FROM captures c JOIN logfeed_captures k ON k.capture_id = c.capture_id WHERE c.capture_id = ?`, id[:])
		if err != nil || row.State != StateOpen || row.Method != "log" || row.Vantage != "host" || row.Dialect != "host-sml-feed/1" || row.Config != cfg || row.Group != 1 {
			t.Fatalf("the admitted capture = %+v, %v", row, err)
		}
		if w, stopped := watermark(t, c, logFile); w != NoWatermark || stopped {
			t.Errorf("the new file's watermark = %d, stopped %v; want %d, open", w, stopped, NoWatermark)
		}

		other := newID(t)
		res, err := c.AdmitLog(ctx, l, LogKey{LogFile: logFile, Device: "eqp"}, other, logDescriptor(t, "eqp"), nil)
		if err != nil || !res.Found || res.Capture != id {
			t.Fatalf("the key's second admission = %+v, %v; want %s found", res, err, id)
		}
		if _, err := c.Capture(ctx, other); !errors.Is(err, ErrUnknownCapture) {
			t.Errorf("the second admission's capture = %v, want no row", err)
		}

		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'logfeed/1'`)
		if _, ok, err := c.TakeLease(ctx, LogGroupLease(1), "b", ttl); err != nil || !ok {
			t.Fatal(err)
		}
		lost := newID(t)
		if _, err := c.AdmitLog(ctx, l, LogKey{LogFile: logFile, Device: "lp1"}, lost, logDescriptor(t, "lp1"), nil); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("an admission under a lost lease = %v, want ErrLeaseLost", err)
		}
		if _, err := c.Capture(ctx, lost); !errors.Is(err, ErrUnknownCapture) {
			t.Errorf("the lost lease's capture = %v, want no row", err)
		}
	})
}

// One equipment's flush registers every segment, across devices and hours, raises its file's watermark and moves the keys to its group, in one transaction;
// it is idempotent, never lowers a watermark, and is rolled back whole by a lost fence or a segment's guard.
func TestRegisterLogFlush(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		gate(t, c)
		l := logGroup(t, c)
		a, b := admitLog(t, c, l, "eqp"), admitLog(t, c, l, "lp1")
		first := &LogFlush{
			Packs: []*Pack{segment(t, b, 5, 0, 9), segment(t, a, 5, 0, 19), segment(t, a, 6, 20, 29)},
			Files: []LogFileMark{{LogFile: logFile, Watermark: 1000}},
		}
		if out, err := c.RegisterLogFlush(ctx, l, first); err != nil || out != Registered {
			t.Fatalf("the flush = %v, %v; want registered", out, err)
		}
		if n := packCount(t, c, first.Packs[0].ID, first.Packs[1].ID, first.Packs[2].ID); n != 3 {
			t.Errorf("%d of the flush's 3 packs registered", n)
		}
		if w, _ := watermark(t, c, logFile); w != 1000 {
			t.Errorf("watermark = %d, want 1000", w)
		}
		l2, ok, err := c.TakeLease(ctx, LogGroupLease(2), "a", ttl)
		if err != nil || !ok {
			t.Fatal(err)
		}
		moved := &LogFlush{Packs: []*Pack{segment(t, b, 5, 10, 14)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 1100}}}
		if out, err := c.RegisterLogFlush(ctx, l2, moved); err != nil || out != Registered {
			t.Fatalf("a flush under group 2's lease = %v, %v", out, err)
		}
		var groups []int
		if err := c.read.SelectContext(ctx, &groups, `SELECT lease_group FROM logfeed_captures ORDER BY lease_group`); err != nil || len(groups) != 2 || groups[0] != 1 || groups[1] != 2 {
			t.Errorf("the keys' groups = %v, %v; want 1 for the capture group 1 last registered and 2 for the one group 2 did", groups, err)
		}
		if out, err := c.RegisterLogFlush(ctx, l, first); err != nil || out != AlreadyRegistered {
			t.Errorf("the flush again = %v, %v; want already registered", out, err)
		}

		lower := &LogFlush{Packs: []*Pack{segment(t, a, 6, 30, 39)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 500}}}
		if out, err := c.RegisterLogFlush(ctx, l, lower); err != nil || out != Registered {
			t.Fatalf("a flush settling a lower offset = %v, %v", out, err)
		}
		if w, _ := watermark(t, c, logFile); w != 1100 {
			t.Errorf("watermark after a lower one = %d, want 1100 kept", w)
		}
	})
}

// A multi-device, multi-hour flush is rolled back whole by a segment's guard and by a lost fence, changing nothing a flush writes.
func TestRegisterLogFlushRefused(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		gate(t, c)
		l := logGroup(t, c)
		a, b := admitLog(t, c, l, "eqp"), admitLog(t, c, l, "lp1")
		first := &LogFlush{Packs: []*Pack{segment(t, b, 5, 0, 9), segment(t, a, 5, 0, 19)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 1000}}}
		if out, err := c.RegisterLogFlush(ctx, l, first); err != nil || out != Registered {
			t.Fatalf("the flush = %v, %v; want registered", out, err)
		}
		beyond := &LogFlush{Packs: []*Pack{segment(t, b, 6, 15, 19), segment(t, a, 2, 40, 49)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 2000}}, HorizonHour: 4}
		before := flushFacts(t, c, a, b)
		if out, err := c.RegisterLogFlush(ctx, l, beyond); err != nil || out != BeyondHorizon {
			t.Fatalf("a flush with a segment beyond the horizon = %v, %v; want BeyondHorizon", out, err)
		}
		if n := packCount(t, c, beyond.Packs[0].ID); n != 0 {
			t.Error("a flush refused by a segment's guard registered its other segment")
		}
		if after := flushFacts(t, c, a, b); after != before {
			t.Errorf("a refused flush changed the catalog: %+v, before %+v", after, before)
		}

		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'logfeed/1'`)
		if _, ok, err := c.TakeLease(ctx, LogGroupLease(1), "b", ttl); err != nil || !ok {
			t.Fatal(err)
		}
		fenced := &LogFlush{Packs: []*Pack{segment(t, a, 6, 50, 59)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 3000}}}
		if _, err := c.RegisterLogFlush(ctx, l, fenced); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("a flush under a lost lease = %v, want ErrLeaseLost", err)
		}
		if n := packCount(t, c, fenced.Packs[0].ID); n != 0 {
			t.Error("a fenced flush registered its segment")
		}
		if after := flushFacts(t, c, a, b); after != before {
			t.Errorf("a fenced flush changed the catalog: %+v, before %+v", after, before)
		}
	})
}

// A flush that stops a file stops it; a later flush or admission of the file is refused, and an idle stop is refused once the watermark moved.
func TestLogFileStop(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		gate(t, c)
		l := logGroup(t, c)
		a := admitLog(t, c, l, "eqp")
		read := int64(NoWatermark)
		moved := &LogFlush{Packs: []*Pack{segment(t, a, 5, 0, 9)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 100}}}
		if out, err := c.RegisterLogFlush(ctx, l, moved); err != nil || out != Registered {
			t.Fatal(out, err)
		}
		idle := &LogFlush{Packs: []*Pack{segment(t, a, 5, 10, 10)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 100, Stop: true, IdleSince: &read}}}
		if _, err := c.RegisterLogFlush(ctx, l, idle); !errors.Is(err, ErrWatermarkMoved) {
			t.Errorf("an idle stop after the watermark moved = %v, want ErrWatermarkMoved", err)
		}
		read = 100
		idle.Packs[0] = segment(t, a, 5, 10, 10)
		if out, err := c.RegisterLogFlush(ctx, l, idle); err != nil || out != Registered {
			t.Fatalf("the idle stop = %v, %v", out, err)
		}
		if _, stopped := watermark(t, c, logFile); !stopped {
			t.Fatal("the file is not stopped")
		}
		if out, err := c.RegisterLogFlush(ctx, l, idle); err != nil || out != AlreadyRegistered {
			t.Errorf("the committed stopping flush again = %v, %v; want already registered", out, err)
		}
		after := &LogFlush{Packs: []*Pack{segment(t, a, 5, 11, 12)}, Files: []LogFileMark{{LogFile: logFile, Watermark: 200}}}
		if _, err := c.RegisterLogFlush(ctx, l, after); !errors.Is(err, ErrFileStopped) {
			t.Errorf("a flush of a stopped file = %v, want ErrFileStopped", err)
		}
		_, err := c.AdmitLog(ctx, l, LogKey{LogFile: logFile, Device: "lp1"}, newID(t), logDescriptor(t, "lp1"), nil)
		if !errors.Is(err, ErrFileStopped) {
			t.Errorf("a new device's admission to a stopped file = %v, want ErrFileStopped", err)
		}
	})
}

// capture stop of a log capture is a request while its group's lease is held and the stop itself once the lease lapsed; any other capture is refused.
func TestStopLogCapture(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		gate(t, c)
		l := logGroup(t, c)
		a, b := admitLog(t, c, l, "eqp"), admitLog(t, c, l, "lp1")
		if out, err := c.StopLogCapture(ctx, a); err != nil || out != StopRequested {
			t.Fatalf("stop with the lease held = %v, %v; want requested", out, err)
		}
		var requested *int64
		if err := c.read.GetContext(ctx, &requested, `SELECT stop_requested_at FROM captures WHERE capture_id = ?`, a[:]); err != nil || requested == nil {
			t.Errorf("stop_requested_at = %v, %v; want set", requested, err)
		}
		if s := captureState(t, c, a); s != StateOpen {
			t.Errorf("a requested stop's capture is %s, want open until its converter writes the stop", s)
		}

		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'logfeed/1'`)
		if out, err := c.StopLogCapture(ctx, b); err != nil || out != StopWritten {
			t.Fatalf("stop with the lease lapsed = %v, %v; want written", out, err)
		}
		if s := captureState(t, c, b); s != StateStopped {
			t.Errorf("a written stop's capture is %s, want stopped", s)
		}
		if out, err := c.StopLogCapture(ctx, b); err != nil || out != StopAlready {
			t.Errorf("stop of a stopped capture = %v, %v; want already", out, err)
		}

		gw := newID(t)
		if _, err := c.Admit(ctx, start(gw), descriptor(t, "gw")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.StopLogCapture(ctx, gw); !errors.Is(err, ErrNotLogCapture) {
			t.Errorf("stop of a gateway's capture = %v, want ErrNotLogCapture", err)
		}
	})
}

// Flushes over the same captures in opposite orders, admissions to their file and stop requests run side by side without an error:
// each takes its capture rows in ascending capture_id, then the group's lease row, then the file's row (logfeed.md §15.7).
func TestLogTransactionsSideBySide(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		gate(t, c)
		l := logGroup(t, c)
		a, b := admitLog(t, c, l, "eqp"), admitLog(t, c, l, "lp1")
		const rounds = 20
		var retries atomic.Int64
		c.retried = func(string) { retries.Add(1) }
		errs := make(chan error, 4*rounds)
		var wg sync.WaitGroup
		for w := range 2 {
			wg.Go(func() {
				for i := range rounds {
					seq := uint64(1000*w + 10*i) //nolint:gosec // small test counts
					first, second := a, b
					if w == 1 {
						first, second = b, a
					}
					f := &LogFlush{Packs: []*Pack{segment(t, first, 5, seq, seq+4), segment(t, second, 5, seq+5, seq+9)},
						Files: []LogFileMark{{LogFile: logFile, Watermark: int64(seq)}}} //nolint:gosec // small test counts
					if out, err := c.RegisterLogFlush(ctx, l, f); err != nil || out != Registered {
						errs <- fmt.Errorf("flush %d/%d = %v, %w", w, i, out, err)
					}
				}
			})
		}
		wg.Go(func() {
			for i := range rounds {
				if _, err := c.AdmitLog(ctx, l, LogKey{LogFile: logFile, Device: fmt.Sprintf("dev%d", i)}, newID(t), logDescriptor(t, "dev"), nil); err != nil {
					errs <- fmt.Errorf("admission %d: %w", i, err)
				}
			}
		})
		wg.Go(func() {
			for i := range rounds {
				if _, err := c.StopLogCapture(ctx, b); err != nil {
					errs <- fmt.Errorf("stop %d: %w", i, err)
				}
			}
		})
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if n := retries.Load(); n != 0 {
			t.Errorf("%d transactions retried on a deadlock or a busy database, want none: each takes its locks in the one order", n)
		}
	})
}

// A stop of a log capture whose group's lease lapsed holds the lease's row until it commits:
// a converter taking the lease meanwhile waits, then reads the capture stopped (logfeed.md §15.11).
func TestStopLogCaptureHoldsATakeover(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		gate(t, c)
		l := logGroup(t, c)
		a := admitLog(t, c, l, "eqp")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'logfeed/1'`)
		held, release := holdAt(c, hookBeforeCommit)
		var stop LogStop
		var stopErr error
		stopped := run(func() { stop, stopErr = c.StopLogCapture(ctx, a) })
		<-held
		waits := c.write.Stats().WaitCount
		var took bool
		var takeErr error
		taken := run(func() { _, took, takeErr = c.TakeLease(ctx, LogGroupLease(1), "b", ttl) })
		waitBlocked(t, c, waits, "UPDATE leases SET holder", 1)
		close(release)
		<-stopped
		<-taken
		if stopErr != nil || stop != StopWritten {
			t.Fatalf("the stop = %v, %v; want written", stop, stopErr)
		}
		if takeErr != nil || !took {
			t.Fatalf("the takeover = %v, %v", took, takeErr)
		}
		if s := captureState(t, c, a); s != StateStopped {
			t.Errorf("the capture the new holder reads is %s, want stopped", s)
		}
	})
}
