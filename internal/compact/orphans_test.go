package compact

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

const (
	tool = "T1"
	hour = int64(490000) // an hour of 2025
)

type fixture struct {
	c     *catalog.Catalog
	s     *store.Mem
	r     *Compactor
	lease catalog.Lease
	// horizon is the oldest hour retention keeps.
	horizon int64
}

func newFixture(t *testing.T, c *catalog.Catalog) *fixture {
	t.Helper()
	f := &fixture{c: c, s: store.NewMem(), horizon: hour - 24}
	f.r = &Compactor{Catalog: c, Store: f.s, HorizonHour: func() int64 { return f.horizon }, Log: slog.New(slog.DiscardHandler)}
	lease, ok, err := c.TakeLease(t.Context(), "compactor/2", "reconciler", time.Hour)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}
	f.lease = lease

	return f
}

func newID(t *testing.T) tracepack.UUID {
	t.Helper()
	var id tracepack.UUID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}

	return id
}

// admit admits a fresh capture, as ingest's admission of its start would.
func (f *fixture) admit(t *testing.T) tracepack.UUID {
	t.Helper()
	ctx := t.Context()
	cfg, err := f.c.ActivateConfig(ctx, []byte("config"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Register(ctx, "r", []int64{cfg}, time.Hour); err != nil {
		t.Fatal(err)
	}
	id := newID(t)
	d := &catalog.Identity{Version: catalog.VersionKey{Capture: id}, Incarnation: 1, StreamSeq: 1, Hour: hour}
	if _, err := f.c.Admit(ctx, d, &catalog.Descriptor{Bytes: []byte("d"), ToolID: tool, RecorderInstanceID: newID(t), Device: "eqp", OriginNS: 1}); err != nil {
		t.Fatal(err)
	}

	return id
}

// pass runs one orphan pass.
func (f *fixture) pass(t *testing.T) OrphanReport {
	t.Helper()
	rep, err := f.r.Orphans(t.Context(), f.lease)
	if err != nil {
		t.Fatal(err)
	}

	return rep
}

func (f *fixture) put(t *testing.T, key string, b []byte) {
	t.Helper()
	if err := f.s.Put(t.Context(), key, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) present(t *testing.T, key string) bool {
	t.Helper()
	_, err := f.s.Stat(t.Context(), key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}

	return err == nil
}

// built is a written pack and its key.
type built struct {
	id   tracepack.UUID
	key  string
	data []byte
}

// segment writes a segment of capture in hour holding seqs, the last one a stop when stop is set.
func segment(t *testing.T, capture tracepack.UUID, seqs []uint64, stop bool) built {
	t.Helper()
	b := write(t, capture, tracepack.PackRoleSegment, seqs, stop)
	key, err := tracepack.SegmentKey("", tool, capture, seqs[0], b.id)
	if err != nil {
		t.Fatal(err)
	}
	b.key = key

	return b
}

func write(t *testing.T, capture tracepack.UUID, role tracepack.PackRole, seqs []uint64, stop bool) built {
	t.Helper()

	return writeWith(t, capture, role, seqs, stop, func(*tracepack.PackMeta) {})
}

// writeWith is write with the pack metadata adjusted by adjust before the Writer sees it.
func writeWith(t *testing.T, capture tracepack.UUID, role tracepack.PackRole, seqs []uint64, stop bool, adjust func(*tracepack.PackMeta)) built {
	t.Helper()
	recs := make([]tracepack.Record, len(seqs))
	for i, seq := range seqs {
		recs[i] = data(seq)
		if stop && i == len(seqs)-1 {
			recs[i] = stopRecord(t, seq)
		}
	}

	return writeRecords(t, capture, role, hour, recs, adjust)
}

// writeRecords writes recs, in order, into a pack of capture's hour hr, its metadata adjusted by adjust.
func writeRecords(t *testing.T, capture tracepack.UUID, role tracepack.PackRole, hr int64, recs []tracepack.Record, adjust func(*tracepack.PackMeta)) built {
	t.Helper()
	name := "eqp"
	origin, mono, tolerance, interval := int64(1000), int64(0), uint64(time.Second), uint64(5*time.Minute)
	m := &tracepack.PackMeta{
		ToolID: tool, Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
		Vantage: tracepack.VantageHost, Recorder: "test", Writer: "test", TimeSource: tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs: &origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
		LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		RecorderInstanceID: tracepack.UUID{9}, RecorderInstanceName: &name,
		PackRole: role, SeqStart: recs[0].Seq, PeriodStart: hr * nsPerHour, PeriodEnd: (hr + 1) * nsPerHour,
		FlushIntervalNs: &interval, ScopeGeneration: new(uint64),
	}
	adjust(m)
	// A pack masked at write marks every record a redaction entry names.
	for _, e := range m.Redaction {
		for i := range recs {
			if recs[i].Seq == e.Seq {
				recs[i].Quality |= tracepack.QualityRedacted
			}
		}
	}
	id := newID(t)
	var buf bytes.Buffer
	w, err := tracepack.NewWriter(&buf, tracepack.WriterOptions{
		Meta: m, Facts: tracepack.PackFacts{DurableRecorder: true, AnyRedacted: len(m.Redaction) > 0}, PackID: id, CaptureID: capture,
		Now: func() time.Time { return time.Unix(0, hr*nsPerHour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range recs {
		if err := w.Append(&recs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return built{id: id, data: buf.Bytes()}
}

func data(seq uint64) tracepack.Record {
	frame := []byte{0, 0, 0, 10, 0, 1, 1, 1, 0, 0, 0, 0, 0, byte(seq)}

	return tracepack.Record{
		Seq: seq, TSUTCNs: hour*nsPerHour + int64(seq), Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost,
		Fidelity: tracepack.FidelityWireExact, Payload: frame,
	}
}

func stopRecord(t *testing.T, seq uint64) tracepack.Record {
	t.Helper()
	ev := tracepack.TransportEvent{Event: tracepack.EventCaptureBoundary, BoundaryKind: new(tracepack.BoundaryKindStop)}
	p, err := ev.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	return tracepack.Record{
		Seq: seq, TSUTCNs: hour*nsPerHour + int64(seq), Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal,
		Fidelity: tracepack.FidelityNotApplicable, Payload: p,
	}
}

// An orphan is adopted on the second pass that finds it unknown, with its exact seq set and its stop:
// coverage only, and an ingest registration of the same pack then finds it adopted (ingest.md §6.3).
func TestOrphanAdopted(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		capture := f.admit(t)
		b := segment(t, capture, []uint64{1, 2, 3, 5, 6}, true)
		f.put(t, b.key, b.data)

		if rep := f.pass(t); !slices.Equal(rep.Waiting, []string{b.key}) || len(rep.Adopted) != 0 {
			t.Fatalf("first pass = %+v, want the key waiting", rep)
		}
		if rep := f.pass(t); !slices.Equal(rep.Adopted, []string{b.key}) {
			t.Fatalf("second pass = %+v, want the key adopted", rep)
		}
		if got, err := c.KeyStanding(ctx, b.key); err != nil || got != catalog.KeyRegistered {
			t.Errorf("KeyStanding = %v, %v; want registered", got, err)
		}
		got, err := c.Capture(ctx, capture)
		if err != nil || got.HighestSeq != 6 || got.State != catalog.StateStopped {
			t.Errorf("capture = %+v, %v; want highest seq 6 and stopped", got, err)
		}
		p := &catalog.Pack{
			ID: b.id, Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleSegment, Key: b.key, Size: int64(len(b.data)),
			SeqStart: 1, FormatMinor: 1, Ranges: []catalog.Range{{SeqFrom: 1, SeqTo: 6}},
		}
		ev := &catalog.Evidence{Deliveries: []catalog.Identity{{Version: catalog.VersionKey{Capture: capture, Seq: 1}, Incarnation: 1, StreamSeq: 2}}}
		if out, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: ev}); err != nil || out != catalog.FoundAdopted {
			t.Errorf("ingest's registration = %v, %v; want found adopted", out, err)
		}
		if rep := f.pass(t); len(rep.Adopted)+len(rep.Waiting)+len(rep.Deleted)+len(rep.Quarantined) != 0 {
			t.Errorf("a pass over a registered key = %+v, want nothing", rep)
		}
	})
}

// Every key that cannot be registered is deleted: one lost to a termination, kept within retention so only the termination refuses it,
// one beyond the horizon, and a tombstoned key that reappears, at once and on every pass.
func TestOrphanDeleted(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		terminated := f.admit(t)
		if _, err := c.TerminateCapture(ctx, terminated, 1, 10, "op", "a1"); err != nil {
			t.Fatal(err)
		}
		lost := segment(t, terminated, []uint64{1}, false)
		f.put(t, lost.key, lost.data)
		f.pass(t)
		if rep := f.pass(t); !slices.Equal(rep.Deleted, []string{lost.key}) {
			t.Fatalf("pass = %+v, want the terminated orphan deleted", rep)
		}

		old := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, old.key, old.data)
		f.pass(t)
		f.horizon = hour + 1
		if rep := f.pass(t); !slices.Equal(rep.Deleted, []string{old.key}) {
			t.Fatalf("pass = %+v, want the orphan beyond the horizon deleted", rep)
		}
		for _, k := range []string{lost.key, old.key} {
			if f.present(t, k) {
				t.Errorf("%s still stored", k)
			}
			if got, _ := c.KeyStanding(ctx, k); got != catalog.KeyTombstoned {
				t.Errorf("%s standing %v, want tombstoned", k, got)
			}
		}

		f.put(t, old.key, old.data) // a delayed upload after the tombstone
		if rep := f.pass(t); !slices.Equal(rep.Deleted, []string{old.key}) || f.present(t, old.key) {
			t.Errorf("a reappearing tombstoned key = %+v, want deleted at once", rep)
		}
	})
}

// The pack an orphan registers is derived from its records alone: the exact seq runs with their endpoint times,
// the stop, the identity its header names, and its coverage entries as losses.
func TestReadDerivesThePack(t *testing.T) {
	t.Parallel()
	f := &fixture{s: store.NewMem()}
	f.r = &Compactor{Store: f.s}
	capture := newID(t)
	four, ts := uint64(4), hour*nsPerHour+4
	b := writeWith(t, capture, tracepack.PackRoleSegment, []uint64{1, 2, 3, 5, 6}, true, func(m *tracepack.PackMeta) {
		m.Coverage = []tracepack.Coverage{{CaptureID: &capture, SeqFirst: &four, SeqLast: &four, TimeStart: &ts, TimeEnd: &ts}}
	})
	key, err := tracepack.SegmentKey("", tool, capture, 1, b.id)
	if err != nil {
		t.Fatal(err)
	}
	f.put(t, key, b.data)
	p, reason, err := f.r.read(t.Context(), store.ObjectInfo{Key: key, Size: int64(len(b.data))})
	if err != nil || reason != "" {
		t.Fatalf("read = %q, %v", reason, err)
	}
	at := func(seq int64) int64 { return hour*nsPerHour + seq }
	want := []catalog.Range{{SeqFrom: 1, SeqTo: 3, TSFrom: at(1), TSTo: at(3)}, {SeqFrom: 5, SeqTo: 6, TSFrom: at(5), TSTo: at(6)}}
	if !slices.Equal(p.Ranges, want) {
		t.Errorf("ranges %+v, want %+v", p.Ranges, want)
	}
	if !p.Stop || p.ID != b.id || p.Capture != capture || p.Hour != hour || p.SeqStart != 1 || p.Role != catalog.RoleSegment || p.ToolID != tool {
		t.Errorf("pack %+v, want the header's identity and a stop", p)
	}
	if len(p.Losses) != 1 || *p.Losses[0].SeqFrom != 4 || *p.Losses[0].SeqTo != 4 || *p.Losses[0].TSFrom != ts || *p.Losses[0].TSTo != ts {
		t.Errorf("losses %+v, want seq 4 at %d", p.Losses, ts)
	}
}

// An orphan of a capture no start admitted stays unregistered and stored, and leaves no placeholder capture row.
func TestOrphanOfAnUnadmittedCapture(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		capture := newID(t)
		b := segment(t, capture, []uint64{1}, false)
		f.put(t, b.key, b.data)
		f.pass(t)
		rep := f.pass(t)
		if !slices.Equal(rep.Unadmitted, []Unadmitted{{Key: b.key, Capture: capture}}) || !f.present(t, b.key) {
			t.Errorf("pass = %+v, want the orphan reported and kept", rep)
		}
		if _, err := c.Capture(t.Context(), capture); !errors.Is(err, catalog.ErrUnknownCapture) {
			t.Errorf("Capture = %v, want no row", err)
		}
	})
}

// A corrupt or misnamed object is quarantined: reported, never registered, never deleted.
func TestOrphanQuarantined(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		capture := f.admit(t)
		corrupt := segment(t, capture, []uint64{1, 2}, false)
		corrupt.data = corrupt.data[:len(corrupt.data)-1]
		misnamed := segment(t, capture, []uint64{3}, false)
		other := newID(t)
		misnamed.key, _ = tracepack.SegmentKey("", tool, capture, 3, other)
		f.put(t, corrupt.key, corrupt.data)
		f.put(t, misnamed.key, misnamed.data)
		f.pass(t)
		rep := f.pass(t)
		keys := make([]string, 0, len(rep.Quarantined))
		for _, q := range rep.Quarantined {
			keys = append(keys, q.Key)
		}
		slices.Sort(keys)
		want := []string{corrupt.key, misnamed.key}
		slices.Sort(want)
		if !slices.Equal(keys, want) || len(rep.Adopted) != 0 {
			t.Fatalf("pass = %+v, want both quarantined", rep)
		}
		for _, k := range want {
			if !f.present(t, k) {
				t.Errorf("%s deleted", k)
			}
			if got, _ := c.KeyStanding(t.Context(), k); got != catalog.KeyUnknown {
				t.Errorf("%s standing %v, want unknown", k, got)
			}
		}
	})
}

// The quarantine rule: anything but finalized-consistent, or any writer defect, though the outcome is consistent.
func TestJudge(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		vr   tracepack.VerifyReport
		want bool
	}{
		{tracepack.VerifyReport{Outcome: tracepack.OutcomeFinalizedConsistent}, false},
		{tracepack.VerifyReport{Outcome: tracepack.OutcomeFinalizedConsistent, WriterDefects: []tracepack.WriterDefect{{}}}, true},
		{tracepack.VerifyReport{Outcome: tracepack.OutcomeFinalizedInconsistent}, true},
		{tracepack.VerifyReport{Outcome: tracepack.OutcomeUnfinalized}, true},
	} {
		if got := judge(&tc.vr) != ""; got != tc.want {
			t.Errorf("judge(%+v) quarantines = %v, want %v", tc.vr, got, tc.want)
		}
	}
}

// A lost lease ends the pass: a worker that lost it does nothing further.
func TestOrphansUnderALostLease(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, b.key, b.data)
		f.pass(t)
		if _, ok, err := c.TakeLease(t.Context(), "compactor/2", "other", time.Hour); err != nil || ok {
			t.Fatalf("a live lease was taken: %v, %v", ok, err)
		}
		f.lease.Fence-- // a stale holder
		if _, err := f.r.Orphans(t.Context(), f.lease); !errors.Is(err, catalog.ErrLeaseLost) {
			t.Errorf("Orphans under a stale lease = %v, want ErrLeaseLost", err)
		}
		if got, _ := c.KeyStanding(t.Context(), b.key); got != catalog.KeyUnknown {
			t.Errorf("standing %v, want unknown", got)
		}
	})
}

// An archive orphan, a merge's output whose commit was lost, is adopted under its hour's key, here under a key prefix.
func TestOrphanArchiveAdopted(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.r.KeyPrefix = "tk"
		capture := f.admit(t)
		b := write(t, capture, tracepack.PackRoleArchive, []uint64{0, 1, 2}, false)
		key, err := tracepack.ArchiveKey("tk", tool, capture, hour, b.id)
		if err != nil {
			t.Fatal(err)
		}
		f.put(t, key, b.data)
		f.pass(t)
		if rep := f.pass(t); !slices.Equal(rep.Adopted, []string{key}) {
			t.Fatalf("pass = %+v, want the archive adopted", rep)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || got.HighestSeq != 2 {
			t.Errorf("capture = %+v, %v; want highest seq 2", got, err)
		}
	})
}

// A stale holder deletes nothing, not even a tombstoned key, whose deletion is otherwise safe at any time.
func TestTombstonedKeyKeptFromAStaleHolder(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		capture := f.admit(t)
		gone := segment(t, capture, []uint64{2}, false)
		tombstone(t, c, capture, gone)
		f.put(t, gone.key, gone.data)
		stale := f.lease
		stale.Fence--
		if _, err := f.r.Orphans(t.Context(), stale); !errors.Is(err, catalog.ErrLeaseLost) {
			t.Errorf("Orphans under a stale lease = %v, want ErrLeaseLost", err)
		}
		if !f.present(t, gone.key) {
			t.Errorf("a stale holder deleted the tombstoned key")
		}
		if rep := f.pass(t); !slices.Equal(rep.Deleted, []string{gone.key}) {
			t.Errorf("the holder's pass = %+v, want the key deleted", rep)
		}
	})
}

// A read the store fails is retried by the next pass, never taken for a damaged object.
func TestOrphanReadFailureRetried(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, b.key, b.data)
		f.pass(t)
		f.r.Store = &failingReads{ObjectStore: f.s, err: errors.New("connection reset")}
		if rep := f.pass(t); len(rep.Quarantined)+len(rep.Adopted) != 0 {
			t.Fatalf("pass with failing reads = %+v, want nothing settled", rep)
		}
		f.r.Store = f.s
		if rep := f.pass(t); !slices.Equal(rep.Adopted, []string{b.key}) {
			t.Errorf("the next pass = %+v, want the key adopted", rep)
		}
	})
}

// failingReads is a store whose objects' reads fail with err.
type failingReads struct {
	store.ObjectStore
	err error
}

func (s *failingReads) Open(ctx context.Context, key string) (store.Object, error) {
	o, err := s.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}

	return failingObject{Object: o, err: s.err}, nil
}

type failingObject struct {
	store.Object
	err error
}

func (o failingObject) ReadAt([]byte, int64) (int, error) { return 0, o.err }

// A close's orphan, whose seq_start is the close sentinel, is adopted as a pack holding no range: coverage and the highest seq stay.
func TestCloseOrphanAdopted(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		capture := f.admit(t)
		meta := segment(t, capture, []uint64{1}, false)
		rd, err := tracepack.Open(t.Context(), bytes.NewReader(meta.data), int64(len(meta.data)), tracepack.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		id, err := tracepack.WriteClose(&buf, tracepack.CloseOptions{
			Meta: rd.Header().Meta, CaptureID: capture, Trigger: time.Unix(0, hour*nsPerHour+10), PackID: newID(t),
			Now: func() time.Time { return time.Unix(0, hour*nsPerHour+10) },
		})
		if err != nil {
			t.Fatal(err)
		}
		key, err := tracepack.SegmentKey("", tool, capture, math.MaxInt64, id)
		if err != nil {
			t.Fatal(err)
		}
		f.put(t, meta.key, meta.data)
		f.put(t, key, buf.Bytes())
		f.pass(t)
		rep := f.pass(t)
		if !slices.Contains(rep.Adopted, key) || len(rep.Quarantined) != 0 {
			t.Fatalf("pass = %+v, want the close adopted", rep)
		}
		if got, _ := c.KeyStanding(t.Context(), key); got != catalog.KeyRegistered {
			t.Errorf("standing %v, want registered", got)
		}
		got, err := c.Capture(t.Context(), capture)
		if err != nil || got.HighestSeq != 1 {
			t.Errorf("capture = %+v, %v; want the highest seq the segment set, 1", got, err)
		}
		snap, err := c.MergeSnapshot(t.Context(), catalog.Scope{Capture: capture, Hour: hour})
		if err != nil || len(snap.Inputs) != 2 {
			t.Errorf("snapshot = %+v, %v; want the segment and the close as the hour's inputs", snap, err)
		}
	})
}
