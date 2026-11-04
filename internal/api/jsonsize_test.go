package api

import (
	"bytes"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/records"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// jsonItemSize is an item's size in a JSON stream's batch as protojson encodes it: its text and a separating comma,
// or a whole message for an item that does not encode.
func jsonItemSize(it *tkv1.RecordItem) int {
	b, err := protojson.Marshal(it)
	if err != nil {
		return batchBytes
	}

	return len(b) + 1
}

// escapes holds every class of character protojson's strings treat apart: quote, backslash, the short escapes,
// other control characters, DEL, the line and paragraph separators, HTML's specials, multibyte text and a valid U+FFFD.
const escapes = "\"\\\b\f\n\r\t\x00\x01\x1f\x7f  <>&é日本� plain"

func recordItem(r *tkv1.Record) *tkv1.RecordItem {
	return &tkv1.RecordItem{Item: &tkv1.RecordItem_Record{Record: r}}
}

// A record item's JSON size is counted exactly as protojson encodes it, every field present and absent,
// payloads of every length modulo three, times at each precision and at their range's ends, and strings with every escape;
// a record protojson refuses, and any other item, are sized as before, by encoding them.
func TestJSONRecordSize(t *testing.T) {
	t.Parallel()
	full := &tkv1.Record{
		Hour: hourTS(hour), Seq: math.MaxUint64, Time: ts(hour*nsPerHour + 123_456_789), Epoch: math.MaxUint32, Kind: "data", Direction: "to_host",
		Header:   &tkv1.HSMSHeader{SessionId: math.MaxUint16, Stream: 6, Function: 11, W: true, SystemBytes: []byte{0, 0, 0, 1}},
		Conflict: true, Excluded: true, Unobserved: true, Payload: []byte("a payload"), Sml: escapes, SmlTruncated: true, SmlError: escapes,
		Holders: []*tkv1.Holder{{PackId: "p-1", State: "settled"}, {}, {PackId: escapes}},
	}
	valid := []*tkv1.Record{{}, full, {Header: &tkv1.HSMSHeader{}}, {Holders: []*tkv1.Holder{{}}}, {Seq: 1, Epoch: 1}}
	for n := range 8 {
		valid = append(valid, &tkv1.Record{Payload: make([]byte, n)}, &tkv1.Record{Header: &tkv1.HSMSHeader{SystemBytes: make([]byte, n)}})
	}
	valid = append(valid, &tkv1.Record{Payload: make([]byte, 64<<10+1)}, &tkv1.Record{Sml: strings.Repeat(escapes, 1000)})
	for _, nanos := range []int32{0, 1, 999_999_999, 1_000_000, 120_000_000, 1_000, 123_000, 7} {
		valid = append(valid, &tkv1.Record{Time: &timestamppb.Timestamp{Seconds: hour * 3600, Nanos: nanos}})
	}
	for _, secs := range []int64{-62135596800, 253402300799, 0, -1} {
		valid = append(valid, &tkv1.Record{Hour: &timestamppb.Timestamp{Seconds: secs}})
	}
	for _, c := range []string{"\x00", "\x1f", " ", "\"", "\\", " ", "\U0001F600", "�"} {
		valid = append(valid, &tkv1.Record{Kind: c}, &tkv1.Record{Direction: c + c}, &tkv1.Record{SmlError: "x" + c})
	}
	// A seeded mix of the fields, so their combinations and separators are counted too.
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a test's reproducible mix
	pick := func(s string) string { rs := []rune(s); return string(rs[:rng.IntN(len(rs)+1)]) }
	for range 2000 {
		r := &tkv1.Record{Seq: rng.Uint64N(1 << rng.IntN(64)), Epoch: rng.Uint32N(1 << rng.IntN(32)), Kind: pick("annotation"), Sml: pick(escapes)}
		if rng.IntN(2) == 0 {
			r.Time = ts(rng.Int64N(math.MaxInt64))
		}
		if rng.IntN(2) == 0 {
			r.Header = &tkv1.HSMSHeader{Stream: rng.Uint32N(256), W: rng.IntN(2) == 0, SystemBytes: make([]byte, rng.IntN(5))}
		}
		r.Conflict, r.Excluded, r.SmlTruncated = rng.IntN(2) == 0, rng.IntN(2) == 0, rng.IntN(2) == 0
		r.Payload = make([]byte, rng.IntN(100))
		for range rng.IntN(3) {
			r.Holders = append(r.Holders, &tkv1.Holder{PackId: pick("0198b8f0-0000-7000-8000-000000000000"), State: pick("superseded")})
		}
		valid = append(valid, r)
	}
	w := &recordWriter{json: true}
	for i, r := range valid {
		it := recordItem(r)
		if n, ok := jsonRecordSize(it); !ok || n+1 != jsonItemSize(it) {
			t.Fatalf("record %d (%v) sized %d, %t; protojson encodes it in %d", i, r, n, ok, jsonItemSize(it)-1)
		}
		if got := w.itemSize(it); got != jsonItemSize(it) {
			t.Fatalf("record %d: itemSize = %d, want %d", i, got, jsonItemSize(it))
		}
	}
	others := []*tkv1.RecordItem{
		{Item: &tkv1.RecordItem_Withheld{Withheld: &tkv1.Withheld{}}},
		{Item: &tkv1.RecordItem_UnknownTail{UnknownTail: &tkv1.UnknownTail{Open: true}}},
		{Item: &tkv1.RecordItem_Incomplete{Incomplete: &tkv1.Incomplete{Hour: hourTS(hour), Reason: "gone", Detail: escapes, Packs: []string{"a"}}}},
		{Item: &tkv1.RecordItem_Capture{Capture: &tkv1.CaptureStart{CompletenessTruncated: true}}},
		// Records protojson refuses.
		recordItem(&tkv1.Record{Sml: "not \xff UTF-8"}), recordItem(&tkv1.Record{Holders: []*tkv1.Holder{{State: "\xc3"}}}),
		recordItem(&tkv1.Record{Hour: &timestamppb.Timestamp{Seconds: 253402300800}}), recordItem(&tkv1.Record{Time: &timestamppb.Timestamp{Seconds: -62135596801}}),
		recordItem(&tkv1.Record{Time: &timestamppb.Timestamp{Nanos: -1}}), recordItem(&tkv1.Record{Time: &timestamppb.Timestamp{Nanos: 1_000_000_000}}),
	}
	for i, it := range others {
		if _, ok := jsonRecordSize(it); ok {
			t.Errorf("item %d (%v) sized without encoding it", i, it)
		}
		if got := w.itemSize(it); got != jsonItemSize(it) {
			t.Errorf("item %d: itemSize = %d, want %d", i, got, jsonItemSize(it))
		}
	}
}

// receivedJSON is every message of capture's stream from cursor read by a JSON client, each item's size as protojson encodes it.
func receivedJSON(t *testing.T, m *masked, req *connect.Request[tkv1.QueryRecordsRequest]) ([]*tkv1.QueryRecordsResponse, [][]int) {
	t.Helper()
	cl := tracekeepv1connect.NewMachineServiceClient(m.hs.Client(), m.hs.URL, connect.WithProtoJSON())
	s, err := cl.QueryRecords(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var msgs []*tkv1.QueryRecordsResponse
	var sizes [][]int
	for s.Receive() {
		msgs = append(msgs, s.Msg())
		ns := make([]int, 0, len(s.Msg().GetItems()))
		for _, it := range s.Msg().GetItems() {
			ns = append(ns, jsonItemSize(it))
		}
		sizes = append(sizes, ns)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}

	return msgs, sizes
}

// Over JSON a message is filled to its budget exactly: data records whose SML escapes its quotes and whose payloads travel in base64,
// then two records sized to take the message to its last byte, all in one message, and the next record in the next.
func TestQueryRecordsJSONBatchFillsItsBudget(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		cs := carriers(t)
		// annotation is an annotation record at seq, at ns into the hour, its raw body n bytes.
		annotation := func(seq uint64, at int64, n int) tracepack.Record {
			payload, err := (&tracepack.Annotation{AnnotationKind: tracepack.AnnotationKindSkippedBytes, Raw: make([]byte, n)}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}

			return tracepack.Record{Seq: seq, TSUTCNs: hour*nsPerHour + at, Kind: tracepack.KindAnnotation, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable, Payload: payload}
		}
		// Data records rendering SML, between annotations of 1,000 bytes, each sized from a capture holding the same records, as the stream carries them.
		var data []tracepack.Record
		for seq := uint64(2); seq < 200; seq++ {
			r := recordAt(&cs[seq%21], hour, seq)
			if seq%2 == 0 {
				r = annotation(seq, int64(seq), 1000) //nolint:gosec // a small seq
			}
			data = append(data, r)
		}
		probe := m.admitUnmasked(t)
		m.put(t, probe, hour, captured(slices.Clone(data)), nil, nil)
		_, sizes := receivedJSON(t, m, query(probe, nil))
		var dataSizes []int
		for _, ns := range sizes {
			dataSizes = append(dataSizes, ns...)
		}
		dataSizes = dataSizes[1 : len(dataSizes)-1] // the start marker and the unknown tail
		if wire := escapedSML(t, m, probe); len(dataSizes) != len(data) || !strings.Contains(wire, `\"`) || !strings.Contains(wire, `\n`) {
			t.Fatalf("the probe carried %d record items, want %d, their SML escaping its quotes and line feeds", len(dataSizes), len(data))
		}
		budget, filled, k := batchBytes-envelope, 0, 0
		for k < len(data) && filled+dataSizes[k] <= budget-2*1000 {
			filled += dataSizes[k]
			k++
		}
		if k == len(data) {
			t.Fatal("the data records do not come near the budget")
		}
		// Two annotations at a time of each precision and a payload of each length take the message to its last byte.
		ats := []int64{2e9, 2e9 + 1e6, 2e9 + 1e3, 2e9 + 1}
		size := func(seq uint64, at int64, n int) int {
			r := annotation(seq, at, n)
			return jsonItemSize(recordItem(recordOf(&records.Record{Hour: hour, Record: r})))
		}
		var fill []tracepack.Record
	search:
		for _, at1 := range ats {
			for _, at2 := range ats {
				for n1 := range 1500 {
					first := size(uint64(k+2), at1, n1) //nolint:gosec // a small seq
					rest := budget - filled - first
					near := (rest - size(uint64(k+3), at2+1e9, 0)) * 3 / 4 //nolint:gosec // a small seq
					for n2 := max(0, near-16); n2 <= near+16; n2++ {
						if size(uint64(k+3), at2+1e9, n2) == rest { //nolint:gosec // a small seq
							fill = []tracepack.Record{annotation(uint64(k+2), at1, n1), annotation(uint64(k+3), at2+1e9, n2)} //nolint:gosec // small seqs
							break search
						}
					}
				}
			}
		}
		if fill == nil {
			t.Fatal("no two annotations fill the budget")
		}
		rs := []tracepack.Record{annotation(1, 0, 1)}
		rs = append(rs, data[:k]...)
		rs = append(rs, fill...)
		rs = append(rs, annotation(uint64(k+4), 5e9, 1)) //nolint:gosec // a small seq
		m.put(t, capture, hour, captured(rs), nil, nil)
		// Resumed after seq 1, the stream carries no start marker: its first message is records alone.
		msgs, _ := receivedJSON(t, m, query(capture, func(q *tkv1.QueryRecordsRequest) { q.Limit = 1 }))
		cursor := msgs[len(msgs)-1].GetEnd().GetCursor()
		msgs, sizes = receivedJSON(t, m, query(capture, func(q *tkv1.QueryRecordsRequest) { q.Cursor = cursor }))
		if len(msgs) != 2 {
			t.Fatalf("%d messages, want two", len(msgs))
		}
		first := seqsOf(msgs[0].GetItems())
		if total := sum(sizes[0]); total != budget || len(first) != k+2 || first[len(first)-1] != uint64(k+3) { //nolint:gosec // a small seq
			t.Errorf("the first message = seqs %v in %d bytes of items; want seqs 2 to %d in %d", first, total, k+3, budget)
		}
		if next := seqsOf(msgs[1].GetItems()); !slices.Equal(next, []uint64{uint64(k + 4)}) { //nolint:gosec // a small seq
			t.Errorf("the next message = seqs %v, want %d", next, k+4)
		}
	})
}

// escapedSML is the text of a JSON stream of capture: what its SML looks like on the wire.
func escapedSML(t *testing.T, m *masked, capture tracepack.UUID) string {
	t.Helper()
	msgs, _ := receivedJSON(t, m, query(capture, nil))
	var b strings.Builder
	for _, msg := range msgs {
		for _, it := range msg.GetItems() {
			if sml := it.GetRecord().GetSml(); sml != "" {
				j, err := protojson.Marshal(it)
				if err != nil {
					t.Fatal(err)
				}
				b.Write(j)
			}
		}
	}

	return b.String()
}

func sum(ns []int) int {
	total := 0
	for _, n := range ns {
		total += n
	}

	return total
}

// Over JSON a seq whose two versions of 60 KiB, 80 KiB each in base64, pass a message's budget together travels alone in its message,
// the messages around it within their budget.
func TestQueryRecordsJSONOversizedSeqTravelsAlone(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		cs := carriers(t)
		big := func(seq uint64, fill byte) tracepack.Record {
			r := recordAt(&cs[0], hour, seq)
			r.Payload = append(bytes.Clone(r.Payload), bytes.Repeat([]byte{fill}, 60<<10)...)
			return r
		}
		m.put(t, capture, hour, []tracepack.Record{recordAt(&cs[0], hour, 1), big(2, 1), recordAt(&cs[0], hour, 3)}, nil, nil)
		m.put(t, capture, hour, []tracepack.Record{big(2, 2)}, nil, nil)
		msgs, sizes := receivedJSON(t, m, query(capture, nil))
		got := make([][]uint64, 0, len(msgs))
		for i, msg := range msgs {
			got = append(got, seqsOf(msg.GetItems()))
			if total := sum(sizes[i]); total > batchBytes-envelope && !slices.Equal(got[i], []uint64{2, 2}) {
				t.Errorf("message %d holds seqs %v in %d bytes of items, past its budget", i, got[i], total)
			}
		}
		if !slices.EqualFunc(got, [][]uint64{{1}, {2, 2}, {3}}, slices.Equal) {
			t.Errorf("messages = %v, want seq 1, both versions of seq 2 alone, then seq 3", got)
		}
		if two := sum(sizes[1]); two <= batchBytes-envelope {
			t.Errorf("seq 2's versions take %d bytes as JSON, want past the budget", two)
		}
	})
}
