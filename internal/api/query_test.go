package api

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/proto"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/procconfig"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// captured sets each record's field validity as a raw capture's writer does.
func captured(rs []tracepack.Record) []tracepack.Record {
	for i := range rs {
		rs[i].SetCapturedFieldValidity()
	}

	return rs
}

// admitUnmasked admits a capture bound to a configuration without a policy, as one admitted before the cutover is.
func (m *masked) admitUnmasked(t *testing.T) tracepack.UUID {
	t.Helper()
	content := procconfig.Content{Classifier: "test", MaxFrameLen: 1 << 20}
	cfg, err := m.c.StageConfig(t.Context(), catalog.Staging{Content: content.Encode()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.c.ActivateStaged(t.Context(), cfg, nil, nil); err != nil {
		t.Fatal(err)
	}

	return m.admitActive(t)
}

// query is a QueryRecords request over the test's hour for capture, as the viewer.
func query(capture tracepack.UUID, edit func(*tkv1.QueryRecordsRequest)) *connect.Request[tkv1.QueryRecordsRequest] {
	id := capture.String()
	m := &tkv1.QueryRecordsRequest{Selector: &tkv1.Selector{ToolId: tool, CaptureId: &id, From: hourTS(hour), To: hourTS(hour + 1)}}
	if edit != nil {
		edit(m)
	}

	return withToken(m, viewerToken)
}

// streamed is a stream's messages and its error.
func (w *world) streamed(t *testing.T, req *connect.Request[tkv1.QueryRecordsRequest]) ([]*tkv1.QueryRecordsResponse, error) {
	t.Helper()
	s, err := w.cl.QueryRecords(t.Context(), req)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var out []*tkv1.QueryRecordsResponse
	for s.Receive() {
		out = append(out, s.Msg())
	}

	return out, s.Err()
}

// items flattens a stream's items, and returns its end, which only the last message may carry.
func items(t *testing.T, msgs []*tkv1.QueryRecordsResponse) ([]*tkv1.RecordItem, *tkv1.QueryEnd) {
	t.Helper()
	var out []*tkv1.RecordItem
	var end *tkv1.QueryEnd
	for i, m := range msgs {
		if m.End != nil && i != len(msgs)-1 {
			t.Errorf("message %d of %d carries the end", i, len(msgs))
		}
		if m.GetCursor() == "" {
			t.Errorf("message %d carries no cursor", i)
		}
		end = m.End
		out = append(out, m.GetItems()...)
	}
	if end == nil {
		t.Fatal("the stream has no end")
	}

	return out, end
}

func seqsOf(its []*tkv1.RecordItem) []uint64 {
	var out []uint64
	for _, it := range its {
		if r := it.GetRecord(); r != nil {
			out = append(out, r.GetSeq())
		}
	}

	return out
}

// A viewer reads a masked capture's records with their SML, the capture opened and closed by its markers,
// and no clear secret anywhere in the stream (T-48).
func TestQueryRecordsStreamsMaskedRecords(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)
		var err error
		ms, entries := m.mask(t, capture, captured(recordsOf(cs, hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		reg := metrics.NewServer()
		if m.srv.Metrics, err = metrics.NewAPI(reg); err != nil {
			t.Fatal(err)
		}
		msgs, err := m.streamed(t, query(capture, nil))
		if err != nil {
			t.Fatal(err)
		}
		scraped := scrape(t, reg)
		for _, want := range []string{
			`tracekeep_api_requests_total{code="ok",operation="QueryRecords"} 1`,
			fmt.Sprintf("tracekeep_api_records_streamed_total %d", len(cs)),
			"tracekeep_api_record_reads_in_flight 0",
		} {
			if !strings.Contains(scraped, want) {
				t.Errorf("no %s in the metrics", want)
			}
		}
		its, end := items(t, msgs)
		if its[0].GetCapture().GetCapture().GetCaptureId() != capture.String() {
			t.Errorf("first item = %v, want the capture's start", its[0])
		}
		if its[len(its)-1].GetUnknownTail() == nil {
			t.Errorf("last item = %v, want the unknown tail", its[len(its)-1])
		}
		if got := seqsOf(its); len(got) != len(cs) || end.GetRecords() != uint64(len(cs)) || end.GetCursor() != "" {
			t.Errorf("records %v, end %v; want %d records and no cursor", got, end, len(cs))
		}
		rendered := 0
		for _, it := range its {
			r := it.GetRecord()
			if r == nil {
				continue
			}
			if !bytes.Equal(r.GetPayload(), ms[r.GetSeq()-1].Payload) {
				t.Errorf("seq %d: payload is not the masked record's", r.GetSeq())
			}
			if r.GetKind() == "data" {
				// A body that does not decode, as the whole-text carrier's, carries the error instead.
				if (r.GetSml() == "") == (r.GetSmlError() == "") || r.GetHeader() == nil {
					t.Errorf("seq %d: sml %q, error %q, header %v; want SML or its error, and a header", r.GetSeq(), r.GetSml(), r.GetSmlError(), r.GetHeader())
				}
				if r.GetSml() != "" {
					rendered++
				}
			}
		}
		if rendered == 0 {
			t.Fatal("no data record rendered")
		}
		noSecret(t, cs, its)
	})
}

// A limit stops the stream between seqs with a cursor; resuming from it reads the rest once;
// another request's cursor and the predicates' bounds are refused.
func TestQueryRecordsLimitAndCursor(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		msgs, err := m.streamed(t, query(capture, func(q *tkv1.QueryRecordsRequest) { q.Limit = 3 }))
		if err != nil {
			t.Fatal(err)
		}
		its, end := items(t, msgs)
		first, cursor := seqsOf(its), end.GetCursor()
		if len(first) != 3 || cursor == "" {
			t.Fatalf("first page = %v, end %v; want 3 records and a cursor", first, end)
		}
		msgs, err = m.streamed(t, query(capture, func(q *tkv1.QueryRecordsRequest) { q.Cursor = end.GetCursor() }))
		if err != nil {
			t.Fatal(err)
		}
		its, end = items(t, msgs)
		rest := seqsOf(its)
		if len(rest) != len(ms)-3 || rest[0] != first[2]+1 || end.GetCursor() != "" || its[0].GetCapture() != nil {
			t.Errorf("second page = %v, end %v; want the rest from seq %d without the capture's start again", rest, end, first[2]+1)
		}
		for name, edit := range map[string]func(*tkv1.QueryRecordsRequest){
			"another request's cursor": func(q *tkv1.QueryRecordsRequest) {
				q.Cursor, q.Messages = cursor, []string{"S1F1"}
			},
			"a cursor that does not parse": func(q *tkv1.QueryRecordsRequest) { q.Cursor = "!" },
			"a limit above 50,000":         func(q *tkv1.QueryRecordsRequest) { q.Limit = 50001 },
			"a message of another form":    func(q *tkv1.QueryRecordsRequest) { q.Messages = []string{"S1"} },
			"a function above 255":         func(q *tkv1.QueryRecordsRequest) { q.Messages = []string{"S1F256"} },
			"a reversed seq range":         func(q *tkv1.QueryRecordsRequest) { q.SeqFrom, q.SeqTo = proto.Uint64(5), proto.Uint64(4) },
			"a seq range without capture_id": func(q *tkv1.QueryRecordsRequest) {
				q.SeqFrom, q.Selector.CaptureId = proto.Uint64(1), nil
			},
			"an unknown kind":      func(q *tkv1.QueryRecordsRequest) { q.Kinds = []string{"other"} },
			"an unknown direction": func(q *tkv1.QueryRecordsRequest) { q.Direction = "up" },
		} {
			if _, err := m.streamed(t, query(capture, edit)); connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("%s: %v, want invalid_argument", name, err)
			}
		}
		// The cursor's own request with another limit continues.
		if _, err := m.streamed(t, query(capture, func(q *tkv1.QueryRecordsRequest) { q.Cursor, q.Limit = cursor, 7 })); err != nil {
			t.Errorf("the cursor under another limit: %v", err)
		}
		if _, err := m.streamed(t, withToken(query(capture, nil).Msg, readerToken)); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("a pack reader's query = %v, want permission_denied", err)
		}
	})
}

// Outside tracekeep dev a capture whose configuration has no policy streams its start and withheld, no record;
// under tracekeep dev it streams its records.
func TestQueryRecordsWithholds(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitUnmasked(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		msgs, err := m.streamed(t, query(capture, nil))
		if err != nil {
			t.Fatal(err)
		}
		its, end := items(t, msgs)
		if len(its) != 2 || its[0].GetCapture() == nil || its[1].GetWithheld() == nil || end.GetRecords() != 0 {
			t.Errorf("a withheld capture = %v, end %v; want its start and withheld only", its, end)
		}
		m.srv.Dev = true
		msgs, err = m.streamed(t, query(capture, nil))
		if err != nil {
			t.Fatal(err)
		}
		if its, _ := items(t, msgs); len(seqsOf(its)) != len(ms) {
			t.Errorf("under tracekeep dev: %d records, want %d", len(seqsOf(its)), len(ms))
		}
	})
}

// Batches stay within 128 KiB, never splitting a seq's versions, each with its cursor.
func TestQueryRecordsBatches(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		cs := carriers(t)
		rs := make([]tracepack.Record, 0, 400)
		for i := range 400 {
			r := recordAt(&cs[0], hour, uint64(i+1)) //nolint:gosec // small seqs
			r.Payload = append(bytes.Clone(r.Payload), make([]byte, 1000)...)
			rs = append(rs, r)
		}
		m.put(t, capture, hour, rs, nil, nil)
		msgs, err := m.streamed(t, query(capture, nil))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) < 2 {
			t.Fatalf("%d messages, want several", len(msgs))
		}
		for i, msg := range msgs {
			if n := proto.Size(&tkv1.QueryRecordsResponse{Items: msg.GetItems()}); n > batchBytes && len(msg.GetItems()) > 1 {
				t.Errorf("message %d holds %d bytes of items", i, n)
			}
		}
		if its, _ := items(t, msgs); len(seqsOf(its)) != len(rs) {
			t.Errorf("%d records, want %d", len(seqsOf(its)), len(rs))
		}
	})
}

// A replica refuses a read beyond its concurrent reads as resource_exhausted.
func TestRecordReadsBound(t *testing.T) {
	t.Parallel()
	s := &Server{Records: RecordReads{MaxReads: 1}}
	release, err := s.acquire()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.acquire(); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("a second read = %v, want resource_exhausted", err)
	}
	release()
	if release, err := s.acquire(); err != nil {
		t.Errorf("after the first ended: %v", err)
	} else {
		release()
	}
}

// smlHex is b as SML writes a binary item's bytes.
func smlHex(b []byte) string {
	parts := make([]string, 0, len(b))
	for _, c := range b {
		parts = append(parts, fmt.Sprintf("0x%02X", c))
	}

	return strings.Join(parts, " ")
}

// noSecret checks that no carrier's secret, as bytes or as SML's hex, is anywhere in its.
func noSecret(t *testing.T, cs []maskingtest.Carrier, its []*tkv1.RecordItem) {
	t.Helper()
	wire, err := proto.Marshal(&tkv1.QueryRecordsResponse{Items: its})
	if err != nil {
		t.Fatal(err)
	}
	for _, cr := range cs {
		for _, secret := range cr.Secrets {
			if bytes.Contains(wire, secret) || bytes.Contains(wire, []byte(smlHex(secret))) {
				t.Errorf("%s: a clear secret is in the stream", cr.Name)
			}
		}
	}
}
