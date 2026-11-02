package api

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// benchPacks is the segments an hour's records are split into: twelve, an hour of five-minute flushes.
const benchPacks = 12

// benchSeqs is the width of the seq range the narrow read selects.
const benchSeqs = 10

// clientCodecs are the clients' codecs, measured apart (P14).
var clientCodecs = []struct {
	name string
	opts []connect.ClientOption
}{{"binary", nil}, {"json", []connect.ClientOption{connect.WithProtoJSON()}}}

// configReads counts the configuration reads a catalog serves.
type configReads struct {
	Catalog
	n atomic.Int64
}

func (c *configReads) ConfigContent(ctx context.Context, id int64) ([]byte, error) {
	c.n.Add(1)

	return c.Catalog.ConfigContent(ctx, id)
}

// BenchmarkQueryRecords streams an hour of n records in benchPacks packs through QueryRecords over HTTP, on SQLite, outside tracekeep dev.
// Its starting state is one capture bound to a configuration with a policy, its hour's segments registered and stored once outside the timer.
// An iteration, by a binary or a JSON client, reads either the first page (the default limit),
// or the whole window, following its cursors to the end, or, binary only, a seq range of benchSeqs seqs in the middle of the hour (P13).
// It writes nothing, and asserts the records streamed, the cursor (empty once the window is read)
// and no incompleteness. At 1,000 records the first page is the whole window.
// Its sizes are 1,000, 72,000 and 360,000 records (TRACEKEEP_BENCH_SCALE).
func BenchmarkQueryRecords(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 72_000, 360_000) {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			m := newMasked(b, catalogtest.Open(b, catalog.EngineSQLite))
			capture := m.admitActive(b)
			m.putHour(b, capture, hour, 0, n, benchPacks)
			for _, codec := range clientCodecs {
				cl := m.client(codec.opts...)
				b.Run(codec.name+"/first-page", func(b *testing.B) {
					want := min(n, defaultRecordLimit)
					b.ReportAllocs()
					for b.Loop() {
						got, end := drain(b, cl, query(capture, nil))
						if got != want || (end.GetCursor() == "") != (n <= defaultRecordLimit) {
							b.Fatalf("first page = %d records, cursor %q; want %d", got, end.GetCursor(), want)
						}
					}
				})
				b.Run(codec.name+"/whole", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if got := whole(b, cl, capture, nil); got != n {
							b.Fatalf("whole window = %d records, want %d", got, n)
						}
					}
				})
			}
			b.Run("binary/seq-range", func(b *testing.B) {
				from := uint64(n / 2) //nolint:gosec // a positive count
				to := from + benchSeqs - 1
				narrow := func(r *tkv1.QueryRecordsRequest) { r.SeqFrom, r.SeqTo = &from, &to }
				b.ReportAllocs()
				for b.Loop() {
					if got, end := drain(b, m.cl, query(capture, narrow)); got != benchSeqs || end.GetCursor() != "" {
						b.Fatalf("seq range = %d records, cursor %q; want %d, none", got, end.GetCursor(), benchSeqs)
					}
				}
			})
		})
	}
}

// BenchmarkQueryRecordsDay streams a 24-hour window, every hour holding n records in one segment, through QueryRecords over HTTP
// by a binary client, on SQLite, outside tracekeep dev, so every hour's read decodes the capture's configuration (P12).
// Its starting state is one capture bound to a configuration with a stored policy, its 24 segments registered and stored once outside the timer.
// An iteration reads the whole window, following its cursors to the end, and asserts 24·n records, the empty last cursor,
// no incompleteness and the configuration read at least once; config-reads/op reports how many reads it took.
// Its sizes are 10, 100 and 1,000 records an hour (TRACEKEEP_BENCH_SCALE).
func BenchmarkQueryRecordsDay(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 10, 100, 1000) {
		b.Run(fmt.Sprintf("records-an-hour=%d", n), func(b *testing.B) {
			m := newMasked(b, catalogtest.Open(b, catalog.EngineSQLite))
			reads := &configReads{Catalog: m.srv.Catalog}
			m.srv.Catalog = reads
			capture := m.admitActive(b)
			for h := range int64(24) {
				m.putHour(b, capture, hour+h, uint64(h)*uint64(n), n, 1) //nolint:gosec // small counts
			}
			day := func(r *tkv1.QueryRecordsRequest) { r.Selector.To = hourTS(hour + 24) }
			b.ReportAllocs()
			total := int64(0)
			for b.Loop() {
				before := reads.n.Load()
				if got := whole(b, m.cl, capture, day); got != 24*n {
					b.Fatalf("the day = %d records, want %d", got, 24*n)
				}
				took := reads.n.Load() - before
				if took == 0 {
					b.Fatal("the day's read read no configuration")
				}
				total += took
			}
			b.ReportMetric(float64(total)/float64(b.N), "config-reads/op")
		})
	}
}

// putHour puts n data records of hour h, seqs from from, as k segments of consecutive seqs, registered under the configuration's policy.
func (m *masked) putHour(b *testing.B, capture tracepack.UUID, h int64, from uint64, n, k int) {
	b.Helper()
	c := carriers(b)[0]
	parts := make([][]tracepack.Record, k)
	for i := range n {
		parts[i*k/n] = append(parts[i*k/n], recordAt(&c, h, from+uint64(i))) //nolint:gosec // a positive index
	}
	for _, rs := range parts {
		m.put(b, capture, h, captured(rs), nil, m.policy())
	}
}

// client is a client of the world's API with opts.
func (w *world) client(opts ...connect.ClientOption) tracekeepv1connect.MachineServiceClient {
	return tracekeepv1connect.NewMachineServiceClient(w.hs.Client(), w.hs.URL, opts...)
}

// whole reads capture's window by following its cursors to the end, and returns the records streamed;
// edit, when set, edits each request before its cursor is set.
func whole(b *testing.B, cl tracekeepv1connect.MachineServiceClient, capture tracepack.UUID, edit func(*tkv1.QueryRecordsRequest)) int {
	b.Helper()
	total, cursor := 0, ""
	for {
		n, end := drain(b, cl, query(capture, func(r *tkv1.QueryRecordsRequest) {
			if edit != nil {
				edit(r)
			}
			r.Cursor = cursor
		}))
		total += n
		if cursor = end.GetCursor(); cursor == "" {
			return total
		}
		if n == 0 {
			b.Fatal("a page with a cursor and no record")
		}
	}
}

// drain reads one stream to its end, and returns its records and its end, asserting the end counts them and reports nothing incomplete.
func drain(b *testing.B, cl tracekeepv1connect.MachineServiceClient, req *connect.Request[tkv1.QueryRecordsRequest]) (int, *tkv1.QueryEnd) {
	b.Helper()
	s, err := cl.QueryRecords(b.Context(), req)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	n := 0
	var end *tkv1.QueryEnd
	for s.Receive() {
		for _, it := range s.Msg().GetItems() {
			if it.GetRecord() != nil {
				n++
			}
		}
		end = s.Msg().GetEnd()
	}
	if err := s.Err(); err != nil || end == nil {
		b.Fatalf("QueryRecords = end %v, %v", end, err)
	}
	if end.GetIncomplete() || end.GetRecords() != uint64(n) { //nolint:gosec // a count
		b.Fatalf("QueryRecords end = %v after %d records; want them counted and nothing incomplete", end, n)
	}

	return n, end
}
