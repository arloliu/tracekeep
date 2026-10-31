package records

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
)

const hour = int64(1000)

var capture = tracepack.UUID{1}

// data is a data record of seq with the HSMS header of stream s, function f, at seq nanoseconds into the hour.
func data(seq uint64, s, f byte) tracepack.Record {
	frame := []byte{0, 0, 0, 10, 0, 1, s, f, 0, 0, 0, 0, 0, byte(seq)}

	r := tracepack.Record{
		Seq: seq, TSUTCNs: hour*nsPerHour + int64(seq), Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost,
		Fidelity: tracepack.FidelityWireExact, Payload: frame,
	}
	r.SetCapturedFieldValidity()

	return r
}

// closeRecord is the capture's close record, the sentinel's stop boundary.
func closeRecord(t *testing.T) tracepack.Record {
	t.Helper()
	ev := tracepack.TransportEvent{Event: tracepack.EventCaptureBoundary, BoundaryKind: new(tracepack.BoundaryKindStop)}
	p, err := ev.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	return tracepack.Record{
		Seq: Sentinel, TSUTCNs: hour*nsPerHour + 1e6, Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal,
		Fidelity: tracepack.FidelityNotApplicable, Payload: p,
	}
}

func newID(t *testing.T) tracepack.UUID {
	t.Helper()
	var id tracepack.UUID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}

	return id
}

// world is a store and the read sets a test serves, each hour's list consumed one read at a time, the last repeated.
type world struct {
	st   *store.Mem
	mu   sync.Mutex
	sets map[int64][]catalog.ReadSet
}

func newWorld() *world { return &world{st: store.NewMem(), sets: map[int64][]catalog.ReadSet{}} }

func (w *world) ReadSet(_ context.Context, _ tracepack.UUID, h int64) (catalog.ReadSet, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	l := w.sets[h]
	if len(l) == 0 {
		return catalog.ReadSet{Capture: capture, Hour: h}, nil
	}
	rs := l[0]
	if len(l) > 1 {
		w.sets[h] = l[1:]
	}

	return rs, nil
}

// serve queues read sets of the test's hour holding packs, in order.
func (w *world) serve(packs ...[]catalog.ReadPack) {
	for _, ps := range packs {
		w.sets[hour] = append(w.sets[hour], catalog.ReadSet{Capture: capture, Hour: hour, Scoped: true, Packs: ps})
	}
}

// pack writes recs as a pack of role and state, puts it unless absent, and returns its read-set entry with one range over its producer seqs.
func (w *world) pack(t *testing.T, role string, state string, absent bool, recs ...tracepack.Record) catalog.ReadPack {
	t.Helper()
	name, origin, mono, tolerance, interval := "eqp", int64(1000), int64(0), uint64(time.Second), uint64(5*time.Minute)
	prole := tracepack.PackRoleSegment
	if role == catalog.RoleArchive {
		prole = tracepack.PackRoleArchive
	}
	m := &tracepack.PackMeta{
		ToolID: "T1", Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
		Vantage: tracepack.VantageHost, Recorder: "test", Writer: "test", TimeSource: tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs: &origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
		LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		RecorderInstanceID: tracepack.UUID{9}, RecorderInstanceName: &name,
		PackRole: prole, SeqStart: recs[0].Seq, PeriodStart: hour * nsPerHour, PeriodEnd: (hour + 1) * nsPerHour,
		FlushIntervalNs: &interval, ScopeGeneration: new(uint64),
	}
	id := newID(t)
	var buf bytes.Buffer
	tw, err := tracepack.NewWriter(&buf, tracepack.WriterOptions{Meta: m, Facts: tracepack.PackFacts{DurableRecorder: true}, PackID: id, CaptureID: capture})
	if err != nil {
		t.Fatal(err)
	}
	for i := range recs {
		if err := tw.Append(&recs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	p := catalog.ReadPack{ID: id, Key: "packs/" + id.String(), Size: int64(buf.Len()), Role: role, State: state, FormatMinor: 1}
	lo, hi := Sentinel, uint64(0)
	for _, r := range recs {
		if r.Seq != Sentinel {
			lo, hi = min(lo, r.Seq), max(hi, r.Seq)
		}
	}
	if lo <= hi {
		p.Ranges = []catalog.Range{{SeqFrom: lo, SeqTo: hi}}
	}
	if !absent {
		if err := w.st.Put(t.Context(), p.Key, bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
			t.Fatal(err)
		}
	}

	return p
}

func (w *world) reader(maxReaders int) *Reader {
	return &Reader{Catalog: w, Store: w.st, MaxReaders: maxReaders}
}

// out records a read's output as lines: "seq" for a version, with "!" for a conflict, "x" excluded, and "incomplete:<reason>".
type out struct{ lines []string }

func (o *out) Versions(rs []Record) error {
	if len(rs) == 0 {
		return fmt.Errorf("an empty group")
	}
	for _, r := range rs {
		if r.Seq != rs[0].Seq {
			return fmt.Errorf("a group of seqs %d and %d", rs[0].Seq, r.Seq)
		}
		line := fmt.Sprint(r.Seq)
		if r.Seq == Sentinel {
			line = "close"
		}
		if r.Conflict {
			line += "!"
		}
		if r.Excluded {
			line += "x"
		}
		o.lines = append(o.lines, line)
	}

	return nil
}

func (o *out) Incomplete(in Incomplete) error {
	line := "incomplete:" + in.Reason
	if in.SeqFrom != nil {
		line += fmt.Sprintf("@%d", *in.SeqFrom)
	}
	o.lines = append(o.lines, line)

	return nil
}

func window() *Query {
	return &Query{Capture: capture, From: hour * nsPerHour, To: (hour + 1) * nsPerHour}
}

func read(t *testing.T, r *Reader, q *Query) []string {
	t.Helper()
	var o out
	if err := r.Read(t.Context(), q, &o); err != nil {
		t.Fatal(err)
	}

	return o.lines
}

func seqs(from, to uint64) []tracepack.Record {
	var out []tracepack.Record
	for s := from; s <= to; s++ {
		out = append(out, data(s, 6, 11))
	}

	return out
}

func check(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// Overlapping segments merge: identical copies collapse, differing versions come together, flagged;
// an archive's close record reads after a later segment's seqs, and seqs no staged pack holds are flagged excluded.
func TestReadMergesClusters(t *testing.T) {
	t.Parallel()
	w := newWorld()
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	other := data(4, 6, 12) // seq 4, another version
	b := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, append(seqs(3, 3), other)...)
	w.serve([]catalog.ReadPack{a, b})
	check(t, "two overlapping segments", read(t, w.reader(16), window()), "0", "1", "2", "3", "4!", "4!")

	w = newWorld()
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, append(seqs(0, 2), closeRecord(t))...)
	late := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(3, 4)...)
	odd := w.pack(t, catalog.RoleSegment, catalog.PackExcluded, false, seqs(7, 8)...)
	w.serve([]catalog.ReadPack{archive, late, odd})
	check(t, "an archive with its close, a late segment and an excluded one", read(t, w.reader(16), window()),
		"0", "1", "2", "3", "4", "7x", "8x", "close")
}

// The messages list is applied whole, wildcards and exact entries together, and the seq range excludes the close record.
func TestReadFilters(t *testing.T) {
	t.Parallel()
	w := newWorld()
	recs := []tracepack.Record{data(0, 6, 11), data(1, 2, 41), data(2, 5, 1), data(3, 2, 42), data(4, 1, 1), closeRecord(t)}
	p := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, recs...)
	w.serve([]catalog.ReadPack{p})
	q := window()
	q.Messages = []Message{{Stream: 6, Function: 11}, {Stream: 5, Function: 1}, {Stream: 2, AnyFunction: true}}
	check(t, "S6F11, S5F1 and S2F*", read(t, w.reader(16), q), "0", "1", "2", "3")
	q = window()
	q.Messages = []Message{{Stream: 1, Function: 1}}
	check(t, "S1F1 alone, through tracepack's filter", read(t, w.reader(16), q), "4")
	q = window()
	from, to := uint64(2), Sentinel
	q.SeqFrom, q.SeqTo = &from, &to
	check(t, "seqs 2 on", read(t, w.reader(16), q), "2", "3", "4")
}

// A cluster above the reader bound is reported and skipped; the read resumes after a position.
func TestReadBoundsAndResumes(t *testing.T) {
	t.Parallel()
	w := newWorld()
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	b := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(4, 6)...)
	c := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(10, 11)...)
	w.serve([]catalog.ReadPack{a, b, c})
	check(t, "a cluster of two packs, at most one read", read(t, w.reader(1), window()), "incomplete:too-many-packs@0", "10", "11")
	q := window()
	q.After = &Position{Hour: hour, Seq: 4}
	check(t, "after seq 4", read(t, w.reader(16), q), "5", "6", "10", "11")
	q.After = &Position{Hour: hour + 1, Seq: 0}
	check(t, "after the window's last hour", read(t, w.reader(16), q))
}

// An object found gone re-reads the hour's read set once and resumes after the last seq yielded;
// gone again, or the hour evicted, the rest of the hour is reported.
func TestReadRereadsAGoneObject(t *testing.T) {
	t.Parallel()
	w := newWorld()
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	gone := w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, append(seqs(0, 4), seqs(10, 14)...)...)
	w.serve([]catalog.ReadPack{a, gone}, []catalog.ReadPack{archive})
	check(t, "a segment merged away during the read", read(t, w.reader(16), window()),
		"0", "1", "2", "3", "4", "10", "11", "12", "13", "14")

	w = newWorld()
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	gone = w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	w.serve([]catalog.ReadPack{a, gone}, []catalog.ReadPack{a, gone})
	check(t, "gone twice", read(t, w.reader(16), window()), "0", "1", "2", "3", "4", "incomplete:gone@5")

	w = newWorld()
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	gone = w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	w.serve([]catalog.ReadPack{a, gone})
	w.sets[hour] = append(w.sets[hour], catalog.ReadSet{Capture: capture, Hour: hour, Evicted: true})
	check(t, "evicted during the read", read(t, w.reader(16), window()), "0", "1", "2", "3", "4", "incomplete:evicted@5")
}

// The sink's error ends the read unchanged.
func TestReadStopsOnTheSinksError(t *testing.T) {
	t.Parallel()
	w := newWorld()
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)})
	stop := &stopping{after: 2}
	if err := w.reader(16).Read(t.Context(), window(), stop); err == nil || !strings.Contains(err.Error(), "enough") {
		t.Errorf("Read = %v, want the sink's error", err)
	}
	if stop.n != 2 {
		t.Errorf("groups yielded = %d, want 2", stop.n)
	}
}

type stopping struct{ after, n int }

func (s *stopping) Versions([]Record) error {
	if s.n == s.after {
		return fmt.Errorf("enough")
	}
	s.n++

	return nil
}

func (s *stopping) Incomplete(Incomplete) error { return nil }
