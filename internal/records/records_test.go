package records

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
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
	// block is the writer's block threshold, 0 for tracepack's default.
	block int
	st    store.ObjectStore
	mu    sync.Mutex
	sets  map[int64][]catalog.ReadSet
}

// newWorld is a world over an fs store in a fresh directory.
func newWorld(t *testing.T) *world {
	t.Helper()
	st, err := store.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	return &world{st: st, sets: map[int64][]catalog.ReadSet{}}
}

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
	tw, err := tracepack.NewWriter(&buf, tracepack.WriterOptions{Meta: m, Facts: tracepack.PackFacts{DurableRecorder: true}, PackID: id, CaptureID: capture, BlockThreshold: w.block})
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
type out struct {
	lines       []string
	incompletes []Incomplete
	// last is the previous group's seq; a group of the same seq is one seq split across callbacks.
	last *uint64
}

func (o *out) Versions(rs []Record) error {
	if len(rs) == 0 {
		return fmt.Errorf("an empty group")
	}
	if o.last != nil && *o.last == rs[0].Seq {
		return fmt.Errorf("seq %d split across callbacks", rs[0].Seq)
	}
	seq := rs[0].Seq
	o.last = &seq
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
			if len(r.Holders) == 0 {
				return fmt.Errorf("a conflict's version of seq %d names no holder", r.Seq)
			}
		}
		if r.Excluded {
			line += "x"
		}
		o.lines = append(o.lines, line)
	}

	return nil
}

func (o *out) Incomplete(in Incomplete) error {
	if strings.Contains(in.Detail, "packs/") {
		return fmt.Errorf("an incomplete's detail %q names a key", in.Detail)
	}
	o.incompletes = append(o.incompletes, in)
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
	w := newWorld(t)
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	other := data(4, 6, 12) // seq 4, another version
	b := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, append(seqs(3, 3), other)...)
	w.serve([]catalog.ReadPack{a, b})
	check(t, "two overlapping segments", read(t, w.reader(16), window()), "0", "1", "2", "3", "4!", "4!")

	w = newWorld(t)
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
	w := newWorld(t)
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
	w := newWorld(t)
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
	w := newWorld(t)
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	gone := w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, append(seqs(0, 4), seqs(10, 14)...)...)
	w.serve([]catalog.ReadPack{a, gone}, []catalog.ReadPack{archive})
	check(t, "a segment merged away during the read", read(t, w.reader(16), window()),
		"0", "1", "2", "3", "4", "10", "11", "12", "13", "14")

	w = newWorld(t)
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	gone = w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	w.serve([]catalog.ReadPack{a, gone}, []catalog.ReadPack{a, gone})
	check(t, "gone twice", read(t, w.reader(16), window()), "0", "1", "2", "3", "4", "incomplete:gone@5")

	w = newWorld(t)
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	gone = w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	w.serve([]catalog.ReadPack{a, gone})
	w.sets[hour] = append(w.sets[hour], catalog.ReadSet{Capture: capture, Hour: hour, Evicted: true})
	check(t, "evicted during the read", read(t, w.reader(16), window()), "0", "1", "2", "3", "4", "incomplete:evicted@5")
}

// The sink's error ends the read unchanged.
func TestReadStopsOnTheSinksError(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
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

// failing is a store whose objects under a key in fail return their errors, each naming the key as a store's do.
type failing struct {
	store.ObjectStore
	fail map[string]error
}

func (f *failing) Open(ctx context.Context, key string) (store.Object, error) {
	err := f.fail[key]
	if err != nil && !errors.Is(err, errReads) && !errors.Is(err, errBroken) && !errors.Is(err, errMidway) {
		return nil, fmt.Errorf("%w: %s", err, key)
	}
	obj, oerr := f.ObjectStore.Open(ctx, key)
	if oerr != nil || err == nil {
		return obj, oerr
	}

	return failingObject{obj, key, err}, nil
}

// errReads makes an object open and every read of it fail as gone; errBroken, fail as a broken store;
// errMidway, fail as gone only the reads from its middle to its last 512 KiB, which opening a large pack never makes.
var (
	errReads  = errors.New("reads fail")
	errBroken = errors.New("reads break")
	errMidway = errors.New("reads midway fail")
)

type failingObject struct {
	store.Object
	key  string
	mode error
}

func (o failingObject) ReadAt(p []byte, off int64) (int, error) {
	switch {
	case errors.Is(o.mode, errBroken):
		return 0, fmt.Errorf("the store broke reading %s", o.key)
	case errors.Is(o.mode, errMidway) && (off < o.Size()/2 || off >= o.Size()-512<<10):
		return o.Object.ReadAt(p, off)
	default:
		return 0, fmt.Errorf("%w: %s", store.ErrNotFound, o.key)
	}
}

// A withheld capture's read set is never read, a resumed read included.
func TestReadRefusesAWithheldCapture(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)})
	r := w.reader(16)
	r.Withholds = func(context.Context, *int64) (bool, error) { return true, nil }
	q := window()
	q.After = &Position{Hour: hour, Seq: 0}
	var o out
	if err := r.Read(t.Context(), q, &o); !errors.Is(err, ErrWithheld) || len(o.lines) != 0 {
		t.Errorf("Read of a withheld capture = %v, %v; want ErrWithheld and nothing", err, o.lines)
	}
}

// A store's failure reaches neither an incomplete's detail nor the read's error with the object's key;
// an object whose reads find it gone, past the store's open, re-reads the hour.
func TestReadNamesNoKey(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	b := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(10, 14)...)
	w.st = &failing{ObjectStore: w.st, fail: map[string]error{b.Key: errors.New("the store broke")}}
	w.serve([]catalog.ReadPack{a, b})
	var o out
	err := w.reader(16).Read(t.Context(), window(), &o)
	if err == nil || strings.Contains(err.Error(), b.Key) || strings.Contains(err.Error(), "broke") {
		t.Errorf("Read over a failing store = %v; want an error naming neither the key nor the store's text", err)
	}

	w = newWorld(t)
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	b = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(10, 14)...)
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, append(seqs(0, 4), seqs(10, 14)...)...)
	w.st = &failing{ObjectStore: w.st, fail: map[string]error{b.Key: errReads}}
	w.serve([]catalog.ReadPack{a, b}, []catalog.ReadPack{archive})
	check(t, "an object whose reads find it gone", read(t, w.reader(16), window()), "0", "1", "2", "3", "4", "10", "11", "12", "13", "14")
}

// An hour retention evicted before its first read is reported, not passed over.
func TestReadReportsAnEvictedHour(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.sets[hour] = []catalog.ReadSet{{Capture: capture, Hour: hour, Evicted: true}}
	check(t, "an evicted hour", read(t, w.reader(16), window()), "incomplete:evicted")
}

// A message entry matches only on the header fields the record holds: placeholder bytes never match a wildcard.
func TestReadMatchesOnlyAvailableFields(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	partial := data(1, 6, 11)
	partial.FieldValidity = tracepack.FieldValiditySessionID
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, data(0, 6, 11), partial, data(2, 2, 3))})
	q := window()
	q.Messages = []Message{{Stream: 6, Function: 11}, {Stream: 2, AnyFunction: true}}
	check(t, "S6F11 or S2F* over a record whose stream and function are unavailable", read(t, w.reader(16), q), "0", "2")
	q.Messages = []Message{{Stream: 6, AnyFunction: true}}
	check(t, "S6F* over it", read(t, w.reader(16), q), "0")
}

// Hour boundaries saturate, so the last representable hour's filter is not reversed.
func TestHourStartSaturates(t *testing.T) {
	t.Parallel()
	last := floorHour(math.MaxInt64)
	if got := hourStart(last + 1); got != math.MaxInt64 {
		t.Errorf("hourStart past the last hour = %d", got)
	}
	if got := hourStart(floorHour(math.MinInt64) - 1); got != math.MinInt64 {
		t.Errorf("hourStart before the first hour = %d", got)
	}
	hr := &hourRead{q: &Query{From: math.MinInt64, To: math.MaxInt64}, hour: last}
	if f := hr.filter().Filter; *f.TimeFrom > *f.TimeTo {
		t.Errorf("the last hour's filter runs from %d to %d", *f.TimeFrom, *f.TimeTo)
	}
}

// A store failing a read while tracepack opens a pack fails the read, without the key; it is no corrupt pack.
func TestReadFailsOnABrokenStore(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 4)...)
	w.st = &failing{ObjectStore: w.st, fail: map[string]error{a.Key: errBroken}}
	w.serve([]catalog.ReadPack{a})
	var o out
	if err := w.reader(16).Read(t.Context(), window(), &o); err == nil || strings.Contains(err.Error(), a.Key) || len(o.lines) != 0 {
		t.Errorf("Read over a broken store = %v, %v; want an error without the key and nothing streamed", err, o.lines)
	}
}

// A store's cancellation and deadline errors come back as the context's own, which name no key.
func TestKeylessContextErrors(t *testing.T) {
	t.Parallel()
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := Keyless(fmt.Errorf("store: head packs/k1: %w", want)); got != want { //nolint:errorlint // the sentinel itself, not a wrapper
			t.Errorf("Keyless of a wrapped %v = %v, want the sentinel itself", want, got)
		}
	}
}

// tracepack's defects map to their reasons: a block over the reader's limit is a read limit, a coverage entry is no defect.
func TestDefectReasons(t *testing.T) {
	t.Parallel()
	var o out
	hr := &hourRead{hour: hour, sink: &o}
	c := cluster{packs: []catalog.ReadPack{{ID: tracepack.UUID{7}}}}
	res := &tracepack.Result{Incomplete: []tracepack.Defect{
		{Reason: tracepack.IncompleteReasonLimit}, {Reason: tracepack.IncompleteReasonCoverage}, {Reason: tracepack.IncompleteReasonCrashGap}, {Reason: tracepack.IncompleteReasonCorruptBlock},
	}}
	if err := hr.defects(res, c); err != nil {
		t.Fatal(err)
	}
	check(t, "defects", o.lines, "incomplete:read-limit", "incomplete:crash-gap", "incomplete:corrupt")
}

// A cluster of more packs than a marker names lists the first of them and counts the rest.
func TestIncompleteNamesBoundedPacks(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	packs := make([]catalog.ReadPack, 0, 150)
	for i := range 150 {
		packs = append(packs, catalog.ReadPack{ID: newID(t), Key: fmt.Sprintf("packs/absent-%d", i), Role: catalog.RoleSegment, State: catalog.PackStaged,
			Ranges: []catalog.Range{{SeqFrom: 0, SeqTo: 10}}})
	}
	w.serve(packs)
	var o out
	if err := w.reader(16).Read(t.Context(), window(), &o); err != nil {
		t.Fatal(err)
	}
	if len(o.incompletes) != 1 || len(o.incompletes[0].Packs) != MaxNamedPacks || o.incompletes[0].MorePacks != 50 {
		t.Errorf("incompletes = %d, naming %d and counting %d more; want one naming %d and counting 50", len(o.incompletes),
			len(o.incompletes[0].Packs), o.incompletes[0].MorePacks, MaxNamedPacks)
	}
}

// An object found gone in the middle of a merge, a seq's versions held and not yet yielded,
// re-reads the hour from the last seq yielded: every seq once, a conflict's versions together.
func TestReadRereadsMidMerge(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.block = 256 << 10
	// Larger than tracepack's whole read, in small blocks read a few at a time during the merge.
	large := func(from, to uint64) []tracepack.Record {
		rs := seqs(from, to)
		for i := range rs {
			noise := make([]byte, 100<<10) // incompressible, so the pack stays large
			if _, err := rand.Read(noise); err != nil {
				t.Fatal(err)
			}
			rs[i].Payload = append(rs[i].Payload, noise...)
		}

		return rs
	}
	big := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, large(0, 59)...)
	other := data(30, 6, 12) // seq 30, another version
	small := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, other)
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, large(0, 59)...)
	w.st = &failing{ObjectStore: w.st, fail: map[string]error{big.Key: errMidway}}
	w.serve([]catalog.ReadPack{big, small}, []catalog.ReadPack{archive, small})
	r := w.reader(16)
	r.HeldBytes = 1 << 20
	got := read(t, r, window())
	want := make([]string, 0, 61)
	for s := range 60 {
		if s == 30 {
			want = append(want, "30!", "30!")
			continue
		}
		want = append(want, fmt.Sprint(s))
	}
	check(t, "a merge whose pack goes midway", got, want...)
}

// A merge over its read limit names at most MaxNamedPacks of its packs and counts the rest.
func TestReadLimitNamesBoundedPacks(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	packs := make([]catalog.ReadPack, 0, 120)
	for range 120 {
		packs = append(packs, w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 1)...))
	}
	w.serve(packs)
	r := w.reader(200)
	r.HeldBytes = 1 // no block fits
	var o out
	if err := r.Read(t.Context(), window(), &o); err != nil {
		t.Fatal(err)
	}
	if len(o.incompletes) != 1 || o.incompletes[0].Reason != ReasonReadLimit || len(o.incompletes[0].Packs) != MaxNamedPacks || o.incompletes[0].MorePacks != 20 {
		t.Errorf("incompletes = %+v; want one read limit naming %d packs and counting 20 more", o.incompletes, MaxNamedPacks)
	}
}
