package api

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/procconfig"
	"github.com/arloliu/tracekeep/internal/records"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// The bounds of QueryRecords (api.md §10.1 *Record predicates of QueryRecords*, *Records*).
const (
	defaultRecordLimit = 5000
	maxRecordLimit     = 50000
	batchBytes         = 128 << 10
)

// RecordReads configures QueryRecords and FindTransaction (api.md §10.1 *Records* *Bounds*); zero fields take the defaults.
type RecordReads struct {
	// MaxReads bounds the reads a replica serves at once (default 8).
	MaxReads int
	// MaxReaders bounds the packs a QueryRecords merge opens (default 16); MaxLookupPacks, an hour a lookup reads (default 64).
	MaxReaders, MaxLookupPacks int
	// HeldBytes is tracepack's MaxHeldBytes for each merge (default 64 MiB).
	HeldBytes int64

	once  sync.Once
	slots chan struct{}
}

// acquire takes one of the replica's concurrent reads, or refuses as resource_exhausted; release returns it.
func (s *Server) acquire() (release func(), err error) {
	s.Records.once.Do(func() { s.Records.slots = make(chan struct{}, cmp.Or(s.Records.MaxReads, 8)) })
	select {
	case s.Records.slots <- struct{}{}:
		s.Metrics.ReadStarted()

		return func() {
			<-s.Records.slots
			s.Metrics.ReadEnded()
		}, nil
	default:
		s.Metrics.ReadRefused()

		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("api: as many record reads as the replica serves at once"))
	}
}

func (s *Server) reader() *records.Reader {
	return &records.Reader{
		Catalog: s.Catalog, Store: s.Store,
		MaxReaders: cmp.Or(s.Records.MaxReaders, 16), HeldBytes: cmp.Or(s.Records.HeldBytes, 64<<20),
		Withholds: s.withholds,
	}
}

// QueryRecords implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) QueryRecords(ctx context.Context, req *connect.Request[tkv1.QueryRecordsRequest], stream *connect.ServerStream[tkv1.QueryRecordsResponse]) error {
	q, err := parseQuery(req.Msg)
	if err != nil {
		return err
	}
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	caps, err := s.Catalog.ListCaptures(ctx, q.sel, s.QuietPeriod)
	if err != nil {
		return failed(err)
	}
	slices.SortFunc(caps, func(a, b catalog.CaptureInfo) int {
		return cmp.Or(cmp.Compare(a.OriginNS, b.OriginNS), bytes.Compare(a.ID[:], b.ID[:]))
	})
	w := &recordWriter{s: s, stream: stream, q: q, json: strings.Contains(req.Header().Get("Content-Type"), "json")}
	for i := range caps {
		c := &caps[i]
		if q.after != nil && before(c, q.after) {
			continue
		}
		if err := w.capture(ctx, c); err != nil {
			if errors.Is(err, errLimit) {
				break
			}

			return err
		}
	}

	return w.end()
}

// errLimit stops a stream at its limit, between seqs.
var errLimit = errors.New("the limit is reached")

// before tells whether c comes before the cursor's capture, or is it and the cursor is past its start.
func before(c *catalog.CaptureInfo, at *recordCursor) bool {
	if n := cmp.Or(cmp.Compare(c.OriginNS, at.Origin), bytes.Compare(c.ID[:], at.Capture[:])); n != 0 {
		return n < 0
	}

	return false
}

// recordQuery is a parsed QueryRecords request.
type recordQuery struct {
	sel    catalog.Selector
	base   records.Query
	limit  int
	digest string
	after  *recordCursor
}

// recordCursor is QueryRecords' cursor: the request's digest, the capture by origin and id, and its start or the last seq streamed.
type recordCursor struct {
	Digest  string         `json:"d"`
	Origin  int64          `json:"o"`
	Capture tracepack.UUID `json:"c"`
	Start   bool           `json:"st,omitempty"`
	Hour    int64          `json:"h,omitempty"`
	Seq     uint64         `json:"s,omitempty"`
}

func (c *recordCursor) text() string {
	b, _ := json.Marshal(c) //nolint:errchkjson // plain fields always encode

	return base64.RawURLEncoding.EncodeToString(b)
}

var messagePattern = regexp.MustCompile(`^S([0-9]{1,3})F([0-9]{1,3}|\*)$`)

var kindNames = map[string]tracepack.Kind{
	"data": tracepack.KindData, "control": tracepack.KindControl, "transport-event": tracepack.KindTransportEvent, "annotation": tracepack.KindAnnotation,
}

// parseQuery checks a request and turns it into the reader's query.
func parseQuery(m *tkv1.QueryRecordsRequest) (*recordQuery, error) {
	sel, err := selector(m.GetSelector())
	if err != nil {
		return nil, err
	}
	q := &recordQuery{sel: sel, limit: defaultRecordLimit}
	q.base.From, q.base.To = sel.From, sel.To
	if err := q.predicates(m); err != nil {
		return nil, err
	}
	if l := int(m.GetLimit()); l > maxRecordLimit {
		return nil, invalid("limit %d above %d", l, maxRecordLimit)
	} else if l > 0 {
		q.limit = l
	}
	// The digest covers the selector and predicates, not the limit or the cursor, so a page size may change.
	d, _ := proto.Clone(m).(*tkv1.QueryRecordsRequest)
	d.Limit, d.Cursor = 0, ""
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(d)
	if err != nil {
		return nil, invalid("a request that does not encode")
	}
	sum := sha256.Sum256(b)
	q.digest = base64.RawURLEncoding.EncodeToString(sum[:16])
	if m.GetCursor() != "" {
		if q.after, err = parseCursor(m.GetCursor(), q.digest); err != nil {
			return nil, err
		}
	}

	return q, nil
}

// predicates parses the record predicates.
func (q *recordQuery) predicates(m *tkv1.QueryRecordsRequest) error {
	for _, text := range m.GetMessages() {
		g := messagePattern.FindStringSubmatch(text)
		if g == nil {
			return invalid("message %q is not SxFy or SxF*", text)
		}
		st, _ := strconv.Atoi(g[1])
		msg := records.Message{AnyFunction: g[2] == "*"}
		fn := 0
		if !msg.AnyFunction {
			fn, _ = strconv.Atoi(g[2])
		}
		if st > 255 || fn > 255 {
			return invalid("message %q: a stream or function above 255", text)
		}
		msg.Stream, msg.Function = uint8(st), uint8(fn) //nolint:gosec // both checked above
		q.base.Messages = append(q.base.Messages, msg)
	}
	switch m.GetDirection() {
	case "":
	case "to_host":
		q.base.Dirs = []tracepack.Dir{tracepack.DirEquipmentToHost}
	case "to_equipment":
		q.base.Dirs = []tracepack.Dir{tracepack.DirHostToEquipment}
	default:
		return invalid("direction %q is not to_host or to_equipment", m.GetDirection())
	}
	for _, k := range m.GetKinds() {
		kind, ok := kindNames[k]
		if !ok {
			return invalid("kind %q is not data, control, transport-event or annotation", k)
		}
		q.base.Kinds = append(q.base.Kinds, kind)
	}
	if len(q.base.Kinds) == 0 {
		q.base.Kinds = []tracepack.Kind{tracepack.KindData, tracepack.KindControl, tracepack.KindAnnotation}
	}
	if m.GetIncludeTransportEvents() && !slices.Contains(q.base.Kinds, tracepack.KindTransportEvent) {
		q.base.Kinds = append(q.base.Kinds, tracepack.KindTransportEvent)
	}
	if (m.SeqFrom != nil || m.SeqTo != nil) && q.sel.Capture == nil {
		return invalid("a seq range needs the selector's capture_id")
	}
	if m.SeqFrom != nil && m.SeqTo != nil && m.GetSeqFrom() > m.GetSeqTo() {
		return invalid("a reversed seq range")
	}
	q.base.SeqFrom, q.base.SeqTo = m.SeqFrom, m.SeqTo

	return nil
}

func parseCursor(text, digest string) (*recordCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(text)
	var c recordCursor
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	if err != nil {
		return nil, invalid("a cursor that does not parse")
	}
	if c.Digest != digest {
		return nil, invalid("a cursor another selector or other predicates issued")
	}

	return &c, nil
}

// recordWriter batches a stream's items and keeps its count, cursor and incompleteness.
type recordWriter struct {
	s       *Server
	stream  *connect.ServerStream[tkv1.QueryRecordsResponse]
	q       *recordQuery
	batch   []*tkv1.RecordItem
	size    int
	cursor  recordCursor
	records int
	// pending are the batch's records, counted as streamed once it is sent.
	pending int
	lacking bool
	limited bool
	// json is set when the stream's codec is JSON, whose encoding sizes the batches.
	json bool
	// configs caches whether a configuration withholds its captures.
	configs map[int64]bool
}

// capture streams one capture: its start, its records hour by hour, and its unknown tail.
func (w *recordWriter) capture(ctx context.Context, c *catalog.CaptureInfo) error {
	w.cursor = recordCursor{Digest: w.q.digest, Origin: c.OriginNS, Capture: c.ID, Start: true}
	rq := w.q.base
	rq.Capture = c.ID
	resuming := w.q.after != nil && w.q.after.Capture == c.ID && !w.q.after.Start
	if resuming {
		rq.After = &records.Position{Hour: w.q.after.Hour, Seq: w.q.after.Seq}
		w.cursor.Start, w.cursor.Hour, w.cursor.Seq = false, w.q.after.Hour, w.q.after.Seq
	}
	w.q.after = nil
	comp, err := w.s.Catalog.Completeness(ctx, c.ID)
	if err != nil {
		return failed(err)
	}
	withheld, err := w.withheld(ctx, c)
	if err != nil {
		return err
	}
	if !resuming {
		if err := w.add(w.start(c, &comp)); err != nil {
			return err
		}
	}
	if withheld {
		if err := w.add(&tkv1.RecordItem{Item: &tkv1.RecordItem_Withheld{Withheld: &tkv1.Withheld{}}}); err != nil {
			return err
		}
	} else if err := w.s.reader().Read(ctx, &rq, w); err != nil {
		return readFailed(err)
	}
	tail := &tkv1.UnknownTail{Open: c.State == catalog.StateOpen}
	if comp.UnknownTailFrom != 0 {
		tail.From = &comp.UnknownTailFrom
	}

	return w.add(&tkv1.RecordItem{Item: &tkv1.RecordItem_UnknownTail{UnknownTail: tail}})
}

// readFailed maps a read's error to its Connect code: the limit passes through, a withheld capture is a failed precondition,
// anything else not already a Connect error is unavailable, which the client retries.
func readFailed(err error) error {
	switch {
	case errors.Is(err, errLimit) || connect.CodeOf(err) != connect.CodeUnknown:
		return err
	case errors.Is(err, records.ErrWithheld):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return connect.NewError(connect.CodeUnavailable, err)
	}
}

// start is c's start marker with its completeness in the window, cut to fit a message when it would not.
func (w *recordWriter) start(c *catalog.CaptureInfo, comp *catalog.Completeness) *tkv1.RecordItem {
	start := &tkv1.CaptureStart{Capture: captureOf(c), Completeness: completenessOf(w.window(comp))}
	it := &tkv1.RecordItem{Item: &tkv1.RecordItem_Capture{Capture: start}}
	for w.itemSize(it) > w.budget() {
		start.CompletenessTruncated = true
		if !cut(start.Completeness) {
			break
		}
	}

	return it
}

// cut drops the last half of comp's longest list of entries, reporting whether anything was left to drop.
func cut(comp *tkv1.Completeness) bool {
	n := max(len(comp.Gaps), len(comp.Evicted), len(comp.Deferred), len(comp.Losses), len(comp.CrashGaps), len(comp.Conflicts))
	switch {
	case n == 0:
		return false
	case len(comp.Gaps) == n:
		comp.Gaps = comp.Gaps[:n/2]
	case len(comp.Evicted) == n:
		comp.Evicted = comp.Evicted[:n/2]
	case len(comp.Deferred) == n:
		comp.Deferred = comp.Deferred[:n/2]
	case len(comp.Losses) == n:
		comp.Losses = comp.Losses[:n/2]
	case len(comp.CrashGaps) == n:
		comp.CrashGaps = comp.CrashGaps[:n/2]
	default:
		comp.Conflicts = comp.Conflicts[:n/2]
	}

	return true
}

// withheld tells whether c is withheld (T-48), each configuration decided once a stream.
func (w *recordWriter) withheld(ctx context.Context, c *catalog.CaptureInfo) (bool, error) {
	if c.ConfigID != nil {
		if v, ok := w.configs[*c.ConfigID]; ok {
			return v, nil
		}
	}
	v, err := w.s.withholds(ctx, c.ConfigID)
	if err == nil && c.ConfigID != nil {
		if w.configs == nil {
			w.configs = map[int64]bool{}
		}
		w.configs[*c.ConfigID] = v
	}

	return v, err
}

// withholds tells whether a capture bound to config is withheld: outside tracekeep dev, its configuration has no policy (T-48).
func (s *Server) withholds(ctx context.Context, config *int64) (bool, error) {
	if s.Dev {
		return false, nil
	}
	if config == nil || *config == 0 {
		return true, nil
	}
	b, err := s.Catalog.ConfigContent(ctx, *config)
	if err != nil {
		return false, failed(err)
	}
	content, err := procconfig.Decode(b)
	if err != nil {
		return false, connect.NewError(connect.CodeInternal, fmt.Errorf("api: configuration %d: %w", *config, err))
	}

	return content.Policy == nil, nil
}

// window is comp as far as it meets the query: its gaps, evicted runs, deferred seqs, declared losses, crash gaps and conflicts,
// by their times, an unknown time meeting it, and by the seq range when there is one; retained runs are left out.
func (w *recordWriter) window(comp *catalog.Completeness) *catalog.Completeness {
	out := inWindow(comp, meeting{from: w.q.base.From, to: w.q.base.To, seqFrom: w.q.base.SeqFrom, seqTo: w.q.base.SeqTo})
	w.lacking = w.lacking || len(out.Gaps)+len(out.Evicted)+len(out.Deferred)+len(out.Losses)+len(out.CrashGaps)+len(out.Conflicts) > 0

	return out
}

// inWindow is comp as far as it meets m, as window says.
func inWindow(comp *catalog.Completeness, m meeting) *catalog.Completeness {
	out := &catalog.Completeness{Frontier: comp.Frontier, HasFrontier: comp.HasFrontier, UnknownTailFrom: comp.UnknownTailFrom}
	out.Gaps = keep(comp.Gaps, func(g catalog.Gap) bool { return m.times(g.Start, g.End) && m.seqs(g.From, g.To) })
	out.Evicted = keep(comp.Evicted, func(r catalog.Run) bool { return m.times(r.TSFrom, r.TSTo) && m.seqs(r.SeqFrom, r.SeqTo) })
	out.Deferred = keep(comp.Deferred, func(d catalog.Deferred) bool { return m.times(&d.TS, &d.TS) && m.seqs(d.Seq, d.Seq) })
	out.Losses = keep(comp.Losses, func(l catalog.DeclaredLoss) bool {
		return m.times(l.TSFrom, l.TSTo) && m.seqs(deref(l.SeqFrom, 0), deref(l.SeqTo, records.Sentinel))
	})
	// In seq order, a loss without a seq last.
	slices.SortStableFunc(out.Losses, func(a, b catalog.DeclaredLoss) int {
		return cmp.Or(cmp.Compare(deref(a.SeqFrom, records.Sentinel), deref(b.SeqFrom, records.Sentinel)),
			cmp.Compare(deref(a.SeqTo, records.Sentinel), deref(b.SeqTo, records.Sentinel)))
	})
	out.CrashGaps = keep(comp.CrashGaps, func(g catalog.CrashGap) bool { return m.times(g.Start, g.End) })
	out.Conflicts = keep(comp.Conflicts, func(k catalog.ConflictRange) bool { return m.seqs(k.SeqFrom, k.SeqTo) })

	return out
}

// meeting tells whether a completeness entry meets the query's window and seq range.
type meeting struct {
	from, to       int64
	seqFrom, seqTo *uint64
}

// times tells whether the interval between a and b meets the window, an unknown bound meeting it;
// after a backward clock step b may lie before a, and the interval is the same.
func (m meeting) times(a, b *int64) bool {
	if a != nil && b != nil && *b < *a {
		a, b = b, a
	}

	return (a == nil || *a < m.to) && (b == nil || *b >= m.from)
}

func (m meeting) seqs(lo, hi uint64) bool {
	return (m.seqFrom == nil || hi >= *m.seqFrom) && (m.seqTo == nil || lo <= *m.seqTo)
}

func keep[T any](in []T, f func(T) bool) []T {
	var out []T
	for _, v := range in {
		if f(v) {
			out = append(out, v)
		}
	}

	return out
}

func deref(p *uint64, absent uint64) uint64 {
	if p == nil {
		return absent
	}

	return *p
}

// Versions implements records.Sink: one seq's versions, never split across messages; the limit stops the stream after them.
func (w *recordWriter) Versions(rs []records.Record) error {
	if w.records >= w.q.limit {
		w.limited = true

		return errLimit
	}
	items := make([]*tkv1.RecordItem, 0, len(rs))
	size := 0
	for i := range rs {
		it := &tkv1.RecordItem{Item: &tkv1.RecordItem_Record{Record: recordOf(&rs[i])}}
		items = append(items, it)
		size += w.itemSize(it)
	}
	if len(w.batch) > 0 && w.size+size > w.budget() {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.batch, w.size = append(w.batch, items...), w.size+size
	w.records += len(rs)
	w.pending += len(rs)
	w.cursor.Start, w.cursor.Hour, w.cursor.Seq = false, rs[0].Hour, rs[0].Seq

	return nil
}

// Incomplete implements records.Sink.
func (w *recordWriter) Incomplete(in records.Incomplete) error {
	w.lacking = true
	w.s.Metrics.ReadIncomplete(in.Reason)
	m := &tkv1.Incomplete{
		Hour: hourTS(in.Hour), Reason: in.Reason, Detail: in.Detail, Packs: ids(in.Packs), MorePacks: uint32(in.MorePacks), SeqFrom: in.SeqFrom, SeqTo: in.SeqTo, //nolint:gosec // a count of packs
	}

	return w.add(&tkv1.RecordItem{Item: &tkv1.RecordItem_Incomplete{Incomplete: m}})
}

// add adds a marker to the batch, sending the batch first when the marker would take it past its size.
func (w *recordWriter) add(it *tkv1.RecordItem) error {
	size := w.itemSize(it)
	if len(w.batch) > 0 && w.size+size > w.budget() {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.batch, w.size = append(w.batch, it), w.size+size

	return nil
}

// envelope is what a message carries besides its items, its cursor and its end, with room to spare.
const envelope = 1 << 10

// budget is the bytes of items a message holds: 128 KiB less its envelope.
func (w *recordWriter) budget() int { return batchBytes - envelope }

// itemSize is it's encoding as an element of a message's items in the stream's codec:
// in binary its tag, length and bytes; in JSON its text and a separating comma, payloads in base64.
func (w *recordWriter) itemSize(it *tkv1.RecordItem) int {
	if w.json {
		b, err := protojson.Marshal(it)
		if err != nil {
			return batchBytes // an item that does not encode travels alone, and fails there
		}

		return len(b) + 1
	}

	return 1 + protowire.SizeBytes(proto.Size(it))
}

func (w *recordWriter) flush() error {
	if len(w.batch) == 0 {
		return nil
	}

	return w.send(&tkv1.QueryRecordsResponse{Items: w.batch, Cursor: w.cursor.text()})
}

// send sends m, the batch, and counts its records as streamed once sent.
func (w *recordWriter) send(m *tkv1.QueryRecordsResponse) error {
	err := w.stream.Send(m)
	if err == nil {
		w.s.Metrics.RecordsStreamed(w.pending)
	}
	w.batch, w.size, w.pending = nil, 0, 0

	return err
}

// end sends the last batch with the stream's end.
func (w *recordWriter) end() error {
	end := &tkv1.QueryEnd{Records: uint64(w.records), Incomplete: w.lacking} //nolint:gosec // a count
	if w.limited {
		end.Cursor = w.cursor.text()
	}
	return w.send(&tkv1.QueryRecordsResponse{Items: w.batch, Cursor: w.cursor.text(), End: end})
}

var recordKindText = map[tracepack.Kind]string{
	tracepack.KindUnknown: "unknown", tracepack.KindData: "data", tracepack.KindControl: "control", tracepack.KindTransportEvent: "transport-event", tracepack.KindAnnotation: "annotation",
}

var dirText = map[tracepack.Dir]string{
	tracepack.DirUnknown: "unknown", tracepack.DirEquipmentToHost: "to_host", tracepack.DirHostToEquipment: "to_equipment", tracepack.DirLocal: "local",
}

// recordOf is r on the wire, with its SML when it is a data record.
func recordOf(r *records.Record) *tkv1.Record {
	out := &tkv1.Record{
		Hour: hourTS(r.Hour), Seq: r.Seq, Time: ts(r.TSUTCNs), Epoch: r.Epoch, Kind: recordKindText[r.Kind], Direction: dirText[r.Dir],
		Conflict: r.Conflict, Excluded: r.Excluded, Unobserved: r.Unobserved, Payload: r.Payload,
	}
	if r.HasHeader {
		h := r.Header
		out.Header = &tkv1.HSMSHeader{SessionId: uint32(h.SessionID), Stream: uint32(h.Stream), Function: uint32(h.Function), W: h.W, SystemBytes: h.SystemBytes[:]}
	}
	if r.Kind == tracepack.KindData && !r.Unobserved {
		out.Sml, out.SmlTruncated, out.SmlError = renderSML(r.Payload)
	}
	for _, p := range r.Holders {
		out.Holders = append(out.Holders, &tkv1.Holder{PackId: p.ID.String(), State: p.State})
	}

	return out
}
