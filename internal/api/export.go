package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// exportOwner is the principal's owner of export jobs, its Via and subject, or a refusal of an empty subject (api.md §10.1 *Exports*).
func exportOwner(ctx context.Context) (string, error) {
	p, ok := PrincipalFrom(ctx)
	if !ok || p.Subject == "" {
		return "", connect.NewError(connect.CodePermissionDenied, errors.New("an export needs a principal with a subject"))
	}

	return p.Via + ":" + p.Subject, nil
}

// StartExport implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) StartExport(ctx context.Context, req *connect.Request[tkv1.StartExportRequest]) (*connect.Response[tkv1.StartExportResponse], error) {
	owner, err := exportOwner(ctx)
	if err != nil {
		return nil, err
	}
	switch req.Msg.GetKind() {
	case "canonical":
	case "filtered":
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("kind=filtered waits for a filtered writer of go-secs (T-48)"))
	default:
		return nil, invalid("kind %q is not canonical", req.Msg.GetKind())
	}
	sel, err := selector(req.Msg.GetSelector())
	if err != nil {
		return nil, err
	}
	bounds := s.Exports.Bounds.withDefaults()
	if n := floorHour(sel.To-1) - floorHour(sel.From) + 1; n > int64(bounds.MaxHours) {
		return nil, invalid("a window of %d hours, at most %d", n, bounds.MaxHours)
	}
	if err := s.countSelection(ctx, sel, bounds); err != nil {
		return nil, err
	}
	r := exportRequest{Kind: "canonical", Selector: exportSelector{ToolID: sel.ToolID, Devices: sel.Devices, From: sel.From, To: sel.To}}
	if sel.Capture != nil {
		r.Selector.Capture = sel.Capture.String()
	}
	b, err := json.Marshal(&r)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	var id tracepack.UUID
	_, _ = rand.Read(id[:]) // never fails (crypto/rand)
	id[6], id[8] = id[6]&0x0f|0x40, id[8]&0x3f|0x80
	audit := catalog.AuditEntry{Actor: owner, Action: "export-start", Detail: fmt.Sprintf("job %s, %s", id, b)}
	if err := s.Catalog.CreateExport(ctx, id, owner, b, audit); err != nil {
		return nil, exportFailed(err)
	}

	return connect.NewResponse(&tkv1.StartExportResponse{JobId: id.String()}), nil
}

// countSelection refuses a selection whose read sets hold more packs or object bytes than an export exports, counted from one snapshot.
func (s *Server) countSelection(ctx context.Context, sel catalog.Selector, bounds ExportBounds) error {
	l, err := s.Catalog.ListPacks(ctx, sel, s.QuietPeriod)
	if err != nil {
		return failed(err)
	}
	packs, size := 0, int64(0)
	for _, h := range l.Hours {
		for _, sc := range h.Scopes {
			for _, p := range sc.Packs {
				if p.State == catalog.PackStaged || p.State == catalog.PackExcluded {
					packs, size = packs+1, size+p.Size
				}
			}
		}
	}
	if packs > bounds.MaxPacks || size > bounds.MaxBytes {
		return connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("the selection holds %d packs of %d bytes, an export at most %d packs and %d bytes", packs, size, bounds.MaxPacks, bounds.MaxBytes))
	}

	return nil
}

// GetExport implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) GetExport(ctx context.Context, req *connect.Request[tkv1.GetExportRequest]) (*connect.Response[tkv1.ExportJob], error) {
	owner, j, err := s.ownJob(ctx, req.Msg.GetJobId())
	if err != nil {
		return nil, err
	}
	out := s.jobOf(&j)
	if out.GetState() == stateExpired {
		s.Metrics.DownloadRefused()
	}
	if out.GetState() != catalog.ExportDone {
		return connect.NewResponse(out), nil
	}
	dl := &s.Downloads
	until := time.Unix(min(dl.now().Add(dl.Lifetime).UnixNano(), *j.DeleteAt-int64(dl.Margin))/int64(time.Second), 0)
	u, err := dl.url(ctx, *j.ObjectKey, until)
	if errors.Is(err, store.ErrNoLifetime) || err == nil && u.expires.Sub(dl.now()) < dl.Floor {
		s.Metrics.DownloadRefused()
		out.State = stateExpired

		return connect.NewResponse(out), nil
	} else if err != nil {
		return nil, connect.NewError(connect.CodeInternal, keylessURL(err))
	}
	entry := catalog.AuditEntry{
		Actor: owner, Action: "export-url",
		Detail: fmt.Sprintf("job %s, %s URL expiring %s", j.ID, u.kind, u.expires.UTC().Format(time.RFC3339)),
	}
	if err := s.Catalog.Audit(ctx, entry); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("audit: %w", err))
	}
	// A URL its audit row's write left below the floor is withheld, as PackURL withholds one.
	if u.expires.Sub(dl.now()) < dl.Floor {
		s.Metrics.DownloadRefused()
		out.State = stateExpired

		return connect.NewResponse(out), nil
	}
	s.Metrics.DownloadURL(u.kind)
	out.Url, out.UrlExpires, out.Presigned = u.url, ts(u.expires.UnixNano()), u.kind == kindPresigned

	return connect.NewResponse(out), nil
}

// stateExpired is the answer's state of a done job whose deadline leaves no URL its floor, derived and never stored.
const stateExpired = "expired"

// ListExports implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) ListExports(ctx context.Context, req *connect.Request[tkv1.ListExportsRequest]) (*connect.Response[tkv1.ListExportsResponse], error) {
	owner, err := exportOwner(ctx)
	if err != nil {
		return nil, err
	}
	limit := int(req.Msg.GetLimit())
	switch {
	case limit > 1000:
		return nil, invalid("limit %d above 1,000", limit)
	case limit == 0:
		limit = 100
	default:
	}
	jobs, err := s.Catalog.ExportJobs(ctx, owner, limit)
	if err != nil {
		return nil, failed(err)
	}
	out := &tkv1.ListExportsResponse{Jobs: make([]*tkv1.ExportJob, 0, len(jobs))}
	for i := range jobs {
		out.Jobs = append(out.Jobs, s.jobOf(&jobs[i]))
	}

	return connect.NewResponse(out), nil
}

// CancelExport implements tracekeepv1connect.MachineServiceHandler.
func (s *Server) CancelExport(ctx context.Context, req *connect.Request[tkv1.CancelExportRequest]) (*connect.Response[tkv1.CancelExportResponse], error) {
	owner, err := exportOwner(ctx)
	if err != nil {
		return nil, err
	}
	id, err := tracepack.ParseUUID(req.Msg.GetJobId())
	if err != nil {
		return nil, invalid("job_id %q: %v", req.Msg.GetJobId(), err)
	}
	err = s.Catalog.CancelExport(ctx, id, owner, catalog.AuditEntry{Actor: owner, Action: "export-cancel", Detail: "job " + id.String()})
	switch {
	case errors.Is(err, catalog.ErrExportNotFound):
		return nil, connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, catalog.ErrExportEnded):
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	case err != nil:
		return nil, exportFailed(err)
	}
	s.Metrics.Export(catalog.ExportCancelled, 0)

	return connect.NewResponse(&tkv1.CancelExportResponse{}), nil
}

// exportFailed maps an export transaction's error: an audit row that could not be written is internal, anything else the catalog unavailable.
func exportFailed(err error) error {
	if errors.Is(err, catalog.ErrAudit) {
		return connect.NewError(connect.CodeInternal, err)
	}

	return failed(err)
}

// ownJob reads the principal's job named by text, or answers not found.
func (s *Server) ownJob(ctx context.Context, text string) (string, catalog.ExportJob, error) {
	owner, err := exportOwner(ctx)
	if err != nil {
		return "", catalog.ExportJob{}, err
	}
	id, err := tracepack.ParseUUID(text)
	if err != nil {
		return "", catalog.ExportJob{}, invalid("job_id %q: %v", text, err)
	}
	j, err := s.Catalog.ExportJob(ctx, id, owner)
	if errors.Is(err, catalog.ErrExportNotFound) {
		return "", catalog.ExportJob{}, connect.NewError(connect.CodeNotFound, err)
	} else if err != nil {
		return "", catalog.ExportJob{}, failed(err)
	}

	return owner, j, nil
}

// jobOf is j on the wire, a done job too near its deadline for a URL answered expired; it carries no URL and no key.
func (s *Server) jobOf(j *catalog.ExportJob) *tkv1.ExportJob {
	out := &tkv1.ExportJob{
		JobId: j.ID.String(), State: j.State, Attempts: uint64(j.Attempts), Created: ts(j.CreatedAt), //nolint:gosec // a count
		Ended: optTS(j.EndedAt), Deadline: optTS(j.DeleteAt),
	}
	var r exportRequest
	if json.Unmarshal(j.Request, &r) == nil {
		out.Kind = r.Kind
		out.Selector = &tkv1.Selector{ToolId: r.Selector.ToolID, Devices: r.Selector.Devices, From: ts(r.Selector.From), To: ts(r.Selector.To)}
		if r.Selector.Capture != "" {
			out.Selector.CaptureId = &r.Selector.Capture
		}
	}
	if j.Reason != nil {
		out.Reason = *j.Reason
	}
	if j.Size != nil {
		size := uint64(*j.Size) //nolint:gosec // a size
		out.Size = &size
	}
	dl := &s.Downloads
	if j.State == catalog.ExportDone && j.DeleteAt != nil && time.Unix(0, *j.DeleteAt-int64(dl.Margin)).Sub(dl.now()) < dl.Floor {
		out.State = stateExpired
	}

	return out
}
