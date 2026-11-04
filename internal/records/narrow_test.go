package records

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// keeping is a sink that keeps every version it receives past its callback.
type keeping struct{ recs []Record }

func (k *keeping) Versions(rs []Record) error { k.recs = append(k.recs, rs...); return nil }

func (k *keeping) Incomplete(in Incomplete) error { return fmt.Errorf("incomplete %s", in.Reason) }

// allocated is the bytes n calls of fn allocate.
// It reads MemStats, which counts the whole process, so its callers are not parallel.
func allocated(n int, fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range n {
		fn()
	}
	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// A record the seq range or a wildcard rejects is never copied: the merge's callback, given a large payload it rejects,
// allocates less than the payload over ten calls, and one it keeps copies it.
// The callback is measured alone, since the read's own buffers come from pools the race detector drains at random.
func TestNarrowReadCopiesOnlyWhatItKeeps(t *testing.T) {
	w := newWorld(t)
	p := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 1)...)
	obj, err := w.st.Open(t.Context(), p.Key)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	rd, err := tracepack.Open(t.Context(), obj, obj.Size(), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const size = 1 << 20
	large := data(1, 6, 11)
	large.Payload = append(large.Payload, make([]byte, size)...)
	it := &tracepack.Item{Record: large, HeaderExtra: make([]byte, 16)}
	two := uint64(2)
	ranged, wild := window(), window()
	ranged.SeqFrom, ranged.SeqTo = &two, &two
	wild.Messages = []Message{{Stream: 5, AnyFunction: true}}
	add := func(q *Query) func() {
		g := &group{hr: &hourRead{q: q, hour: hour, sink: &keeping{}}, rs: &catalog.ReadSet{}, readers: []*tracepack.Reader{rd}}
		return func() {
			if err := g.add(it); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, q := range map[string]*Query{"the seq range": ranged, "a wildcard": wild} {
		if n := allocated(10, add(q)); n >= size {
			t.Errorf("%s: ten rejections of a %d-byte payload allocate %d bytes, want less than one payload", name, size, n)
		}
	}
	if n := allocated(1, add(window())); n < size {
		t.Errorf("keeping a %d-byte payload allocates %d bytes, want its copy", size, n)
	}
}

// A kept record's bytes are its own: they survive the reader's reuse of its buffers across blocks and records.
func TestNarrowReadKeepsItsBytes(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.block = 4 << 10 // several blocks, decoded into reused buffers
	recs := make([]tracepack.Record, 0, 60)
	for s := range uint64(60) {
		r := sized(s, 512)
		if s%3 == 0 {
			r.Payload[6], r.Payload[7] = 2, 41 // S2F41, which the wildcard below rejects
		}
		recs = append(recs, r)
	}
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, recs...)})
	q := window()
	from, to := uint64(10), uint64(50)
	q.SeqFrom, q.SeqTo = &from, &to
	q.Messages = []Message{{Stream: 6, AnyFunction: true}}
	var k keeping
	if err := w.reader(16).Read(t.Context(), q, &k); err != nil {
		t.Fatal(err)
	}
	want := 0
	for s := from; s <= to; s++ {
		if s%3 != 0 {
			want++
		}
	}
	if len(k.recs) != want {
		t.Fatalf("kept %d records, want %d", len(k.recs), want)
	}
	for _, r := range k.recs {
		if !bytes.Equal(r.Payload, recs[r.Seq].Payload) || r.Extra != nil {
			t.Errorf("seq %d: payload of %d bytes differs from the one written, or extra %v", r.Seq, len(r.Payload), r.Extra)
		}
	}
}

// A seq's versions at the range's edges come together, and a kept seq's group is yielded though the next seq is rejected.
func TestNarrowReadKeepsEdgeVersionsTogether(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(2, 7)...)
	b := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, data(2, 6, 12), data(3, 6, 12), data(6, 6, 12), data(7, 6, 12))
	w.serve([]catalog.ReadPack{a, b})
	for _, tc := range []struct {
		from, to uint64
		want     []string
	}{
		{3, 6, []string{"3!", "3!", "4", "5", "6!", "6!"}},
		{3, 3, []string{"3!", "3!"}},
		{6, 6, []string{"6!", "6!"}},
	} {
		q := window()
		q.SeqFrom, q.SeqTo = &tc.from, &tc.to
		check(t, fmt.Sprintf("seqs %d to %d", tc.from, tc.to), read(t, w.reader(16), q), tc.want...)
	}
}

// The seq range and the messages list together: exact entries, through tracepack's filter alone or not, and wildcards.
func TestNarrowReadPredicates(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	recs := []tracepack.Record{data(0, 6, 11), data(1, 2, 41), data(2, 5, 1), data(3, 2, 42), data(4, 1, 1), data(5, 6, 11), closeRecord(t)}
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, recs...)})
	for _, tc := range []struct {
		name     string
		from, to uint64
		messages []Message
		want     []string
	}{
		{"S6F11 alone", 1, 5, []Message{{Stream: 6, Function: 11}}, []string{"5"}},
		{"S2F41 and S5F1", 2, 4, []Message{{Stream: 2, Function: 41}, {Stream: 5, Function: 1}}, []string{"2"}},
		{"S2F*", 2, 4, []Message{{Stream: 2, AnyFunction: true}}, []string{"3"}},
		{"S2F* and S1F1", 0, 4, []Message{{Stream: 2, AnyFunction: true}, {Stream: 1, Function: 1}}, []string{"1", "3", "4"}},
		{"S6F* to the sentinel", 1, Sentinel, []Message{{Stream: 6, AnyFunction: true}}, []string{"5"}},
	} {
		q := window()
		q.SeqFrom, q.SeqTo, q.Messages = &tc.from, &tc.to, tc.messages
		check(t, tc.name, read(t, w.reader(16), q), tc.want...)
	}
}

// A kept seq followed by rejected ones, then an object gone or a store's failure: the hour's rest, and a resumed read,
// start after the last group yielded, never after a rejected seq; a re-read after a gone object repeats nothing.
func TestNarrowReadResumesAfterTheLastYielded(t *testing.T) {
	t.Parallel()
	// Seqs 0 to 4 are S6F11, 5 and 6 S1F1, which the wildcard rejects.
	mixed := func() []tracepack.Record { return append(seqs(0, 4), data(5, 1, 1), data(6, 1, 1)) }
	wild := func() *Query {
		q := window()
		q.Messages = []Message{{Stream: 6, AnyFunction: true}}
		return q
	}
	ranged := func(from, to uint64) *Query {
		q := window()
		q.SeqFrom, q.SeqTo = &from, &to
		return q
	}

	w := newWorld(t)
	a := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(0, 6)...)
	gone := w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	w.serve([]catalog.ReadPack{a, gone}, []catalog.ReadPack{a, gone})
	check(t, "the range rejecting 5 and 6, then gone twice", read(t, w.reader(16), ranged(0, 4)), "0", "1", "2", "3", "4", "incomplete:gone@5")

	w = newWorld(t)
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, mixed()...)
	gone = w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	w.serve([]catalog.ReadPack{a, gone}, []catalog.ReadPack{a, gone})
	check(t, "a wildcard rejecting 5 and 6, then gone twice", read(t, w.reader(16), wild()), "0", "1", "2", "3", "4", "incomplete:gone@5")

	w = newWorld(t)
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, mixed()...)
	gone = w.pack(t, catalog.RoleSegment, catalog.PackStaged, true, seqs(10, 14)...)
	archive := w.pack(t, catalog.RoleArchive, catalog.PackStaged, false, append(mixed(), seqs(10, 14)...)...)
	w.serve([]catalog.ReadPack{a, gone}, []catalog.ReadPack{archive})
	check(t, "gone, then the read set read again", read(t, w.reader(16), wild()), "0", "1", "2", "3", "4", "10", "11", "12", "13", "14")

	w = newWorld(t)
	a = w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, mixed()...)
	b := w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, seqs(10, 14)...)
	healthy := w.st
	w.st = &failing{ObjectStore: healthy, fail: map[string]error{b.Key: errBroken}}
	w.serve([]catalog.ReadPack{a, b})
	var o out
	if err := w.reader(16).Read(t.Context(), wild(), &o); !errors.Is(err, ErrStore) {
		t.Fatalf("Read over a broken store = %v, want ErrStore", err)
	}
	check(t, "before the store failed", o.lines, "0", "1", "2", "3", "4")
	w.st = healthy
	q := wild()
	q.After = &Position{Hour: hour, Seq: 4} // the last group yielded
	check(t, "resumed after it", read(t, w.reader(16), q), "10", "11", "12", "13", "14")
}

// A damaged block past the range is still read and reported: the merge never stops at the range's end.
func TestNarrowReadReportsACorruptTail(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.block = 4 << 10
	recs := make([]tracepack.Record, 0, 60)
	for s := range uint64(60) {
		recs = append(recs, sized(s, 512))
	}
	w.mutate = func(b []byte) {
		rd, err := tracepack.Open(t.Context(), bytes.NewReader(b), int64(len(b)), tracepack.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		blocks := rd.Blocks()
		if len(blocks) < 3 {
			t.Fatalf("%d blocks, want several", len(blocks))
		}
		last := blocks[len(blocks)-1]
		b[last.Offset+uint64(last.OnDiskLen)-1] ^= 0xff
	}
	w.serve([]catalog.ReadPack{w.pack(t, catalog.RoleSegment, catalog.PackStaged, false, recs...)})
	q := window()
	from, to := uint64(1), uint64(2)
	q.SeqFrom, q.SeqTo = &from, &to
	check(t, "the first seqs of a pack whose last block is damaged", read(t, w.reader(16), q), "1", "2", "incomplete:corrupt")
}
