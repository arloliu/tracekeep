package api

import (
	"errors"
	"fmt"
	"math"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// invalid is the Connect error of a request the handler refuses as invalid.
func invalid(format string, args ...any) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, args...))
}

// failed maps a catalog error: a refused selector as invalid, anything else as the catalog unavailable, which a client retries.
func failed(err error) error {
	if errors.Is(err, catalog.ErrInvalidSelector) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	return connect.NewError(connect.CodeUnavailable, err)
}

// selector parses sel, refusing a missing selector or window bound and an id that does not parse;
// the catalog refuses the rest (no tool, an empty window, too many devices or hours).
func selector(sel *tkv1.Selector) (catalog.Selector, error) {
	if sel == nil {
		return catalog.Selector{}, invalid("no selector")
	}
	if !sel.GetFrom().IsValid() || !sel.GetTo().IsValid() {
		return catalog.Selector{}, invalid("a selector needs a valid from and to")
	}
	out := catalog.Selector{ToolID: sel.GetToolId(), Devices: sel.GetDevices(), From: nanos(sel.GetFrom()), To: nanos(sel.GetTo())}
	if sel.CaptureId != nil {
		id, err := tracepack.ParseUUID(sel.GetCaptureId())
		if err != nil {
			return catalog.Selector{}, invalid("capture_id %q: %v", sel.GetCaptureId(), err)
		}
		out.Capture = &id
	}

	return out, nil
}

// nanos is t in Unix nanoseconds, clamped to int64's range: exact inside it, the nearer bound outside.
func nanos(t *timestamppb.Timestamp) int64 {
	at := t.AsTime()
	switch {
	case at.Before(time.Unix(0, math.MinInt64)):
		return math.MinInt64
	case at.After(time.Unix(0, math.MaxInt64)):
		return math.MaxInt64
	default:
		return at.UnixNano()
	}
}

func ts(ns int64) *timestamppb.Timestamp { return timestamppb.New(time.Unix(0, ns)) }

// hourTS is the start of hour, in seconds, which no hour's start overflows.
func hourTS(hour int64) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: hour * int64(time.Hour/time.Second)}
}

func optTS(ns *int64) *timestamppb.Timestamp {
	if ns == nil {
		return nil
	}

	return ts(*ns)
}

func optID(id *tracepack.UUID) *string {
	if id == nil {
		return nil
	}
	s := id.String()

	return &s
}

func ids(in []tracepack.UUID) []string {
	out := make([]string, 0, len(in))
	for _, id := range in {
		out = append(out, id.String())
	}

	return out
}

func captureOf(c *catalog.CaptureInfo) *tkv1.Capture {
	out := &tkv1.Capture{
		CaptureId: c.ID.String(), State: c.State, Device: c.Device, RecorderInstanceId: c.RecorderInstanceID.String(),
		PreviousCaptureId: optID(c.PreviousCaptureID), Origin: ts(c.OriginNS), LastRecord: optTS(c.LastRecordNS),
		ConfigId: c.ConfigID, EquipmentEndpoint: c.EquipmentEndpoint, DeviceId: c.DeviceID, Overlap: c.Overlap,
	}
	for _, th := range c.Thinning {
		out.Thinning = append(out.Thinning, &tkv1.Thinning{
			Stream: th.Stream, Function: th.Function, Source: th.Source, KeepEveryN: th.KeepEveryN, KeepIntervalMs: th.KeepIntervalMs,
		})
	}
	for _, u := range c.Unregistered {
		out.Unregistered = append(out.Unregistered, &tkv1.UnregisteredInterval{Start: optTS(u.Start), End: ts(u.End)})
	}

	return out
}

func deviceOf(d *catalog.DeviceInfo) *tkv1.Device {
	return &tkv1.Device{
		Name: d.Name, EquipmentEndpoint: d.EquipmentEndpoint, DeviceId: d.DeviceID, OpenCaptures: int64(d.OpenCaptures),
		Overlap: d.Overlap, LastRecord: optTS(d.LastRecordNS), Gap: d.Gap,
	}
}

func cursorOf(c catalog.Cursor) *tkv1.Cursor { return &tkv1.Cursor{Epoch: c.Epoch, Seq: c.Seq} }

func rangesOf(in []catalog.Range) []*tkv1.Range {
	out := make([]*tkv1.Range, 0, len(in))
	for _, r := range in {
		out = append(out, &tkv1.Range{SeqFrom: r.SeqFrom, SeqTo: r.SeqTo, From: ts(r.TSFrom), To: ts(r.TSTo)})
	}

	return out
}

func deferredOf(in []catalog.Deferred) []*tkv1.Deferred {
	out := make([]*tkv1.Deferred, 0, len(in))
	for _, d := range in {
		out = append(out, &tkv1.Deferred{Seq: d.Seq, Time: ts(d.TS), State: string(d.State)})
	}

	return out
}

func lossesOf(in []catalog.DeclaredLoss) []*tkv1.DeclaredLoss {
	out := make([]*tkv1.DeclaredLoss, 0, len(in))
	for _, l := range in {
		out = append(out, &tkv1.DeclaredLoss{SeqFrom: l.SeqFrom, SeqTo: l.SeqTo, From: optTS(l.TSFrom), To: optTS(l.TSTo), ActionId: l.ActionID})
	}

	return out
}

func hourOf(h *catalog.HourListing) *tkv1.Hour {
	out := &tkv1.Hour{
		Hour: hourTS(h.Hour), Evicted: ids(h.Evicted), Deferred: deferredOf(h.Deferred), Losses: lossesOf(h.Losses),
		NoData: h.NoData, Spanned: h.Spanned,
	}
	for _, s := range h.Scopes {
		scope := &tkv1.Scope{CaptureId: s.Capture.String(), State: s.State, Revision: s.Revision}
		for _, p := range s.Packs {
			scope.Packs = append(scope.Packs, &tkv1.Pack{
				PackId: p.ID.String(), Role: p.Role, State: p.State, Size: p.Size,
				FormatMinor: uint32(p.FormatMinor), Ranges: rangesOf(p.Ranges), //nolint:gosec // a format minor is a small count
			})
		}
		out.Scopes = append(out.Scopes, scope)
	}

	return out
}

func completenessOf(c *catalog.Completeness) *tkv1.Completeness {
	out := &tkv1.Completeness{
		Retained: rangesOf(c.Retained), Deferred: deferredOf(c.Deferred), Losses: lossesOf(c.Losses),
	}
	if c.HasFrontier {
		out.Frontier = &c.Frontier
	}
	if c.UnknownTailFrom != 0 {
		out.UnknownTailFrom = &c.UnknownTailFrom
	}
	for _, r := range c.Evicted {
		out.Evicted = append(out.Evicted, &tkv1.Run{SeqFrom: r.SeqFrom, SeqTo: r.SeqTo, From: optTS(r.TSFrom), To: optTS(r.TSTo)})
	}
	for _, g := range c.Gaps {
		out.Gaps = append(out.Gaps, &tkv1.Gap{SeqFrom: g.From, SeqTo: g.To, Start: optTS(g.Start), End: optTS(g.End), OpenStart: g.OpenStart})
	}
	for _, g := range c.CrashGaps {
		out.CrashGaps = append(out.CrashGaps, &tkv1.CrashGap{Start: optTS(g.Start), End: optTS(g.End)})
	}
	for _, k := range c.Conflicts {
		out.Conflicts = append(out.Conflicts, &tkv1.Conflict{SeqFrom: k.SeqFrom, SeqTo: k.SeqTo, State: k.State})
	}

	return out
}

func eventOf(e *catalog.ScopeEvent) *tkv1.ScopeEvent {
	return &tkv1.ScopeEvent{
		Seq: e.Seq, CaptureId: e.Capture.String(), Hour: hourTS(e.Hour), Event: e.Event, Revision: e.Revision, At: ts(e.At),
	}
}
