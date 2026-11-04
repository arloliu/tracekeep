package api

import (
	"fmt"
	"strings"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/records"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// base64Payload is the payload of BenchmarkQueryRecordSizing's base64 item, which JSON carries in base64.
const base64Payload = 64 << 10

// BenchmarkQueryRecordSizing is QueryRecords' sizing of the items it batches alone (P14), recordWriter.itemSize in each codec,
// apart from the whole request BenchmarkQueryRecords measures.
//   - records=n: the starting state is BenchmarkQueryRecords' at n records, whose whole window is read once outside the timer by a binary client,
//     keeping every record item the stream carried, the items each codec's stream sizes;
//     an iteration sizes them all, so its time against QueryRecords/records=n/<codec>/whole is sizing's share of that read.
//     Its sizes are 1,000, 72,000 and 360,000 records (TRACEKEEP_BENCH_SCALE).
//   - marker: one incompleteness marker naming records.MaxNamedPacks packs, more left out, and the seqs lacking.
//   - long-sml: one data record whose SML, of a list of many ASCII items, is cut at its limit.
//   - base64: one annotation record of base64Payload bytes, no SML, whose payload JSON writes in base64.
//
// Each asserts the sizes it sums the same every iteration, and writes nothing.
func BenchmarkQueryRecordSizing(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 72_000, 360_000) {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			m := newMasked(b, catalogtest.Open(b, catalog.EngineSQLite))
			capture := m.admitActive(b)
			m.putHour(b, capture, hour, 0, n, benchPacks)
			items := recordItems(b, m.client(), capture)
			if len(items) != n {
				b.Fatalf("the whole window carried %d record items, want %d", len(items), n)
			}
			benchSizing(b, items)
		})
	}
	marker := &tkv1.RecordItem{Item: &tkv1.RecordItem_Incomplete{Incomplete: &tkv1.Incomplete{
		Hour: hourTS(hour), Reason: records.ReasonCorrupt, Detail: "a damaged block", Packs: ids(packIDs(b, records.MaxNamedPacks)), MorePacks: 7,
		SeqFrom: new(uint64(1000)), SeqTo: new(uint64(1999)),
	}}}
	b.Run("marker", func(b *testing.B) { benchSizing(b, []*tkv1.RecordItem{marker}) })
	b.Run("long-sml", func(b *testing.B) {
		r := recordOf(&records.Record{Hour: hour, Record: tracepack.Record{Seq: 1, Kind: tracepack.KindData, Dir: tracepack.DirHostToEquipment, Payload: longSMLFrame(b)}})
		if !r.GetSmlTruncated() || len(r.GetSml()) < smlLimit-8 || r.GetSmlError() != "" {
			b.Fatalf("the SML = %d bytes, truncated %t, error %q; want it cut at %d", len(r.GetSml()), r.GetSmlTruncated(), r.GetSmlError(), smlLimit)
		}
		benchSizing(b, []*tkv1.RecordItem{{Item: &tkv1.RecordItem_Record{Record: r}}})
	})
	b.Run("base64", func(b *testing.B) {
		payload := []byte(strings.Repeat("annotation text ", base64Payload/16))
		r := recordOf(&records.Record{Hour: hour, Record: tracepack.Record{Seq: 1, Kind: tracepack.KindAnnotation, Dir: tracepack.DirLocal, Payload: payload}})
		if r.GetSml() != "" {
			b.Fatal("an annotation rendered SML")
		}
		benchSizing(b, []*tkv1.RecordItem{{Item: &tkv1.RecordItem_Record{Record: r}}})
	})
}

// benchSizing sizes items as each codec's stream does, the binary and the JSON one apart, asserting the same total every iteration.
func benchSizing(b *testing.B, items []*tkv1.RecordItem) {
	b.Helper()
	for _, codec := range []string{"binary", "json"} {
		b.Run(codec, func(b *testing.B) {
			w := &recordWriter{json: codec == "json"}
			size := func() int {
				total := 0
				for _, it := range items {
					total += w.itemSize(it)
				}

				return total
			}
			want := size()
			if want < len(items) {
				b.Fatalf("%d items sized %d bytes", len(items), want)
			}
			b.ReportAllocs()
			for b.Loop() {
				if got := size(); got != want {
					b.Fatalf("items sized %d bytes, then %d", want, got)
				}
			}
		})
	}
}

// recordItems reads capture's whole window by following its cursors to the end, and returns every record item the stream carried.
func recordItems(b *testing.B, cl tracekeepv1connect.MachineServiceClient, capture tracepack.UUID) []*tkv1.RecordItem {
	b.Helper()
	var out []*tkv1.RecordItem
	cursor := ""
	for {
		s, err := cl.QueryRecords(b.Context(), query(capture, func(r *tkv1.QueryRecordsRequest) { r.Cursor = cursor }))
		if err != nil {
			b.Fatal(err)
		}
		var end *tkv1.QueryEnd
		for s.Receive() {
			for _, it := range s.Msg().GetItems() {
				if it.GetRecord() != nil {
					out = append(out, it)
				}
			}
			end = s.Msg().GetEnd()
		}
		if err := s.Err(); err != nil || end == nil || end.GetIncomplete() {
			b.Fatalf("QueryRecords = end %v, %v", end, err)
		}
		_ = s.Close()
		if cursor = end.GetCursor(); cursor == "" {
			return out
		}
	}
}

// longSMLFrame is the frame of an S6F11 whose body, a list of 2,000 ASCII items of 40 characters, renders past smlLimit.
func longSMLFrame(b *testing.B) []byte {
	b.Helper()
	items := make([]secs2.Item, 2000)
	for i := range items {
		items[i] = secs2.A(fmt.Sprintf("%040d", i))
	}
	msg, err := hsms.NewDataMessage(6, 11, true, 1, [4]byte{0, 0, 0, 1}, secs2.L(items...))
	if err != nil {
		b.Fatal(err)
	}

	return msg.ToBytes()
}

// packIDs is n fresh pack ids.
func packIDs(b *testing.B, n int) []tracepack.UUID {
	b.Helper()
	out := make([]tracepack.UUID, n)
	for i := range out {
		out[i] = newID(b)
	}

	return out
}
