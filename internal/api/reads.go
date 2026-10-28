package api

import (
	"context"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/catalog"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// Page bounds of ListScopeChanges: a request's limit 0 takes defaultPage, and a larger one than maxPage is cut to it.
const (
	defaultPage = 1000
	maxPage     = 10000
)

// ListDevices implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) ListDevices(ctx context.Context, req *connect.Request[tkv1.ListDevicesRequest]) (*connect.Response[tkv1.ListDevicesResponse], error) {
	sel, err := selector(req.Msg.GetSelector())
	if err != nil {
		return nil, err
	}
	devices, err := s.Catalog.ListDevices(ctx, sel, s.QuietPeriod)
	if err != nil {
		return nil, failed(err)
	}
	out := &tkv1.ListDevicesResponse{Devices: make([]*tkv1.Device, 0, len(devices))}
	for i := range devices {
		out.Devices = append(out.Devices, deviceOf(&devices[i]))
	}

	return connect.NewResponse(out), nil
}

// ListCaptures implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) ListCaptures(ctx context.Context, req *connect.Request[tkv1.ListCapturesRequest]) (*connect.Response[tkv1.ListCapturesResponse], error) {
	sel, err := selector(req.Msg.GetSelector())
	if err != nil {
		return nil, err
	}
	captures, err := s.Catalog.ListCaptures(ctx, sel, s.QuietPeriod)
	if err != nil {
		return nil, failed(err)
	}
	out := &tkv1.ListCapturesResponse{Captures: make([]*tkv1.Capture, 0, len(captures))}
	for i := range captures {
		out.Captures = append(out.Captures, captureOf(&captures[i]))
	}

	return connect.NewResponse(out), nil
}

// ListPacks implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) ListPacks(ctx context.Context, req *connect.Request[tkv1.ListPacksRequest]) (*connect.Response[tkv1.ListPacksResponse], error) {
	sel, err := selector(req.Msg.GetSelector())
	if err != nil {
		return nil, err
	}
	l, err := s.Catalog.ListPacks(ctx, sel, s.QuietPeriod)
	if err != nil {
		return nil, failed(err)
	}
	out := &tkv1.ListPacksResponse{
		Cursor: cursorOf(l.Cursor), Hours: make([]*tkv1.Hour, 0, len(l.Hours)), Captures: make([]*tkv1.CaptureCompleteness, 0, len(l.Captures)),
	}
	for i := range l.Hours {
		out.Hours = append(out.Hours, hourOf(&l.Hours[i]))
	}
	for i := range l.Captures {
		c := &l.Captures[i]
		out.Captures = append(out.Captures, &tkv1.CaptureCompleteness{Capture: captureOf(&c.CaptureInfo), Completeness: completenessOf(&c.Completeness)})
	}

	return connect.NewResponse(out), nil
}

// ListScopeChanges implements tracekeepv1connect.MachineServiceHandler; the selector's window does not filter the events (api.md §10.1).
func (s *Server) ListScopeChanges(ctx context.Context, req *connect.Request[tkv1.ListScopeChangesRequest]) (*connect.Response[tkv1.ListScopeChangesResponse], error) {
	sel, err := selector(req.Msg.GetSelector())
	if err != nil {
		return nil, err
	}
	limit := int(req.Msg.GetLimit())
	if limit == 0 {
		limit = defaultPage
	}
	limit = min(limit, maxPage)
	after := catalog.Cursor{Epoch: req.Msg.GetAfter().GetEpoch(), Seq: req.Msg.GetAfter().GetSeq()}
	ch, err := s.Catalog.ListScopeChanges(ctx, after, sel, limit)
	if err != nil {
		return nil, failed(err)
	}
	out := &tkv1.ListScopeChangesResponse{Events: make([]*tkv1.ScopeEvent, 0, len(ch.Events)), Next: cursorOf(ch.Next), Stale: ch.Stale}
	for i := range ch.Events {
		out.Events = append(out.Events, eventOf(&ch.Events[i]))
	}

	return connect.NewResponse(out), nil
}
