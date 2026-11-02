package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// errLookupPacks refuses an hour of more packs than a lookup passes at once.
// lookupHours is the hours a lookup schedules from the request's hour on: tracepack's default.
const lookupHours = tracepack.DefaultTxMaxHours

var errLookupPacks = errors.New("an hour of more packs than a lookup reads at once")

// FindTransaction implements tracekeepv1connect.MachineServiceHandler (api.md §10.1 *Records*).
func (s *Server) FindTransaction(ctx context.Context, req *connect.Request[tkv1.FindTransactionRequest]) (*connect.Response[tkv1.FindTransactionResponse], error) {
	capture, err := tracepack.ParseUUID(req.Msg.GetCaptureId())
	if err != nil {
		return nil, invalid("capture_id %q: %v", req.Msg.GetCaptureId(), err)
	}
	h := req.Msg.GetHour()
	if !h.IsValid() || nanos(h)%hourNS != 0 {
		return nil, invalid("hour %s is not an hour's start", hourText(h))
	}
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	c, err := s.Catalog.Capture(ctx, capture)
	if errors.Is(err, catalog.ErrUnknownCapture) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("find transaction of %s: the catalog knows no such capture", capture))
	} else if err != nil {
		return nil, failed(err)
	}
	if withheld, err := s.withholds(ctx, &c.ConfigID); err != nil {
		return nil, err
	} else if withheld {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("find transaction of %s: the capture is withheld", capture))
	}
	src := &hourSource{s: s, capture: capture, limit: cmp.Or(s.Records.MaxLookupPacks, 64), sets: map[int64]*catalog.ReadSet{}, readers: map[tracepack.UUID]*tracepack.Reader{}}
	defer src.close()
	key := tracepack.TxKey{CaptureID: capture, Seq: req.Msg.GetSeq(), Hour: floorHour(nanos(h))}
	res, err := tracepack.FindTransaction(ctx, src, key, tracepack.TxOptions{MaxHeldBytes: cmp.Or(s.Records.HeldBytes, 64<<20), MaxHours: lookupHours})
	if err != nil {
		return nil, lookupFailed(capture, req.Msg.GetSeq(), err)
	}

	return connect.NewResponse(src.response(&res, key.Hour)), nil
}

// errOtherCapture refuses a pack the catalog files under a capture its header does not name.
var errOtherCapture = errors.New("the pack is of another capture")

// lookupFailed maps a lookup's error to its Connect code:
// the store's failure, a pack missing or unreadable included, before a pack that does not open as one of the capture,
// which a retry cannot cure (api.md §10.1 *Records*), as tracepack wraps a failed read in what it reports.
func lookupFailed(capture tracepack.UUID, seq uint64, err error) error {
	err = fmt.Errorf("find transaction of %s seq %d: %w", capture, seq, err)
	switch {
	case errors.Is(err, tracepack.ErrNotPrimary), errors.Is(err, records.ErrWithheld):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, tracepack.ErrInvalidQuery):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, tracepack.ErrReadLimit), errors.Is(err, errLookupPacks):
		return connect.NewError(connect.CodeResourceExhausted, err)
	case connect.CodeOf(err) != connect.CodeUnknown:
		return err
	case storeFailed(err):
		return dependency(connect.CodeUnavailable, msgStore, err)
	case errors.Is(err, tracepack.ErrCorrupt), errors.Is(err, tracepack.ErrNotTracepack), errors.Is(err, tracepack.ErrUnsupportedFormat),
		errors.Is(err, errOtherCapture):
		return connect.NewError(connect.CodeDataLoss, err)
	default:
		return dependency(connect.CodeUnavailable, msgCatalog, err)
	}
}

// hourSource answers each hour a lookup asks for with that hour's read set, opened when asked;
// the objects of an hour close when the lookup asks for the next or returns, tracepack's release rule, the readers kept until it returns.
type hourSource struct {
	s       *Server
	capture tracepack.UUID
	limit   int
	sets    map[int64]*catalog.ReadSet
	readers map[tracepack.UUID]*tracepack.Reader
	objs    []store.Object
}

// Readers implements tracepack.PackSource.
func (src *hourSource) Readers(ctx context.Context, hour int64) ([]*tracepack.Reader, error) {
	src.close() // no read of an earlier hour follows this call
	rs, err := src.s.Catalog.ReadSet(ctx, src.capture, hour)
	if err != nil {
		return nil, err
	}
	src.sets[hour] = &rs
	// Every hour's read set is checked, as QueryRecords' are, so no lookup reads a withheld capture's packs.
	if withheld, err := src.s.withholds(ctx, rs.ConfigID); err != nil {
		return nil, err
	} else if withheld {
		return nil, records.ErrWithheld
	}
	if len(rs.Packs) > src.limit {
		return nil, fmt.Errorf("%w: hour %s holds %d packs, at most %d", errLookupPacks, hourRFC(hour), len(rs.Packs), src.limit)
	}
	out := make([]*tracepack.Reader, 0, len(rs.Packs))
	for _, p := range rs.Packs {
		obj, err := records.Open(ctx, src.s.Store, p.Key)
		if err != nil {
			return nil, fmt.Errorf("open pack %s: %w", p.ID, err)
		}
		src.objs = append(src.objs, obj)
		r, err := tracepack.Open(ctx, obj, obj.Size(), tracepack.ReaderOptions{})
		if err == nil && r.Header().CaptureID != src.capture {
			err = errOtherCapture
		}
		if err != nil {
			return nil, fmt.Errorf("open pack %s: %w", p.ID, err)
		}
		src.readers[p.ID] = r
		out = append(out, r)
	}

	return out, nil
}

func (src *hourSource) close() {
	for _, o := range src.objs {
		_ = o.Close()
	}
	src.objs = nil
}

var outcomeText = map[tracepack.TxOutcome]string{tracepack.TxMatched: "matched", tracepack.TxAmbiguous: "ambiguous", tracepack.TxNotFound: "not-found"}

var roleBits = []struct {
	bit  tracepack.TxClass
	name string
}{
	{tracepack.TxPrimary, "primary"}, {tracepack.TxCandidate, "candidate"}, {tracepack.TxPossibleReply, "possible-reply"},
	{tracepack.TxSameKeyPrimary, "same-key-primary"}, {tracepack.TxPossiblePrimary, "possible-primary"},
	{tracepack.TxClosing, "closing"}, {tracepack.TxOutcomeRecord, "outcome"},
}

// response is res on the wire, each record with its flags and SML as QueryRecords streams them;
// first is the lookup's first hour.
func (src *hourSource) response(res *tracepack.TxResult, first int64) *tkv1.FindTransactionResponse {
	out := &tkv1.FindTransactionResponse{
		Outcome: outcomeText[res.Outcome], WindowEnd: res.WindowEnd,
		Key: &tkv1.TransactionKey{
			Epoch: res.Epoch, Direction: dirText[res.Dir], SessionId: uint32(res.SessionID), SystemBytes: res.SystemBytes[:], Function: uint32(res.Function),
		},
	}
	if res.StreamAvailable {
		st := uint32(res.Stream)
		out.Key.Stream = &st
	}
	if res.WAvailable {
		out.Key.W = &res.W
	}
	for i := range res.Records {
		out.Records = append(out.Records, src.record(&res.Records[i]))
	}
	for _, g := range res.Incomplete {
		tg := &tkv1.TransactionGap{Reason: g.Reason.String(), Seq: g.Seq}
		for _, hr := range g.Hours {
			tg.Hours = append(tg.Hours, hourTS(hr))
		}
		if g.Pack != nil {
			id := g.Pack.String()
			tg.PackId = &id
		}
		if g.Reason == tracepack.TxGapOpenWindow {
			// tracepack names the hours scheduled only in its text, as epoch hours; the page shows them from Hours.
			for hr := first; hr < first+lookupHours; hr++ {
				tg.Hours = append(tg.Hours, hourTS(hr))
			}
			tg.Detail = "no record read in the hours scheduled bounds the window"
		} else if g.Err != nil {
			tg.Detail = g.Err.Error()
		}
		out.Gaps = append(out.Gaps, tg)
	}
	for _, sc := range res.Searched {
		out.Hours = append(out.Hours, hourTS(sc.Hour))
	}
	for _, c := range res.Conflicts {
		tc := &tkv1.TransactionConflict{Seq: c.Seq}
		for _, v := range c.Versions {
			tc.Versions = append(tc.Versions, &tkv1.Holders{PackIds: ids(v)})
		}
		out.Conflicts = append(out.Conflicts, tc)
	}
	for _, f := range res.FooterErrs {
		out.Unreadable = append(out.Unreadable, &tkv1.UnreadablePack{Hour: hourTS(f.Hour), PackId: f.Pack.String(), Error: f.Err.Error()})
	}

	return out
}

// record is one record the lookup read, as QueryRecords streams it, with its roles.
func (src *hourSource) record(tr *tracepack.TxRecord) *tkv1.TransactionRecord {
	rec := records.Record{Hour: tr.Hour, Record: tr.Record, Extra: tr.HeaderExtra, Conflict: tr.Conflict}
	if r := src.readers[tr.Pack]; r != nil {
		rec.Unobserved = r.Unobserved(&rec.Record)
	}
	if rec.Kind == tracepack.KindData || rec.Kind == tracepack.KindControl {
		rec.Header = rec.HSMSHeader()
		rec.HasHeader = rec.Header.Available != 0
	}
	if rs := src.sets[tr.Hour]; rs != nil {
		rec.Excluded = records.Excluded(rs, rec.Seq)
		if rec.Conflict {
			rec.Holders = records.Holders(rs, rec.Seq)
		}
	}
	out := &tkv1.TransactionRecord{Record: recordOf(&rec), InWindow: tr.InWindow, Valid: tr.Valid}
	for _, b := range roleBits {
		if tr.Class.HasAny(b.bit) {
			out.Roles = append(out.Roles, b.name)
		}
	}

	return out
}

func floorHour(ns int64) int64 {
	q := ns / hourNS
	if ns%hourNS < 0 {
		q--
	}

	return q
}

// hourNS is an hour in nanoseconds.
const hourNS = int64(3600e9)
