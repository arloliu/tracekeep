package api

import (
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

func exportReq(capture tracepack.UUID, kind string) *tkv1.StartExportRequest {
	id := capture.String()

	return &tkv1.StartExportRequest{Kind: kind, Selector: &tkv1.Selector{ToolId: tool, CaptureId: &id, From: hourTS(hour), To: hourTS(hour + 1)}}
}

// An exporter starts a job, a worker completes it, and the exporter alone reads it, with a download URL whose issue is audited.
func TestExportThroughTheAPI(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		now := time.Now()
		m.urls(t, &now, false)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		workers(t, m)
		started, err := m.cl.StartExport(t.Context(), withToken(exportReq(capture, "canonical"), exporterToken))
		if err != nil {
			t.Fatal(err)
		}
		job := started.Msg.GetJobId()
		var got *tkv1.ExportJob
		for deadline := time.Now().Add(20 * time.Second); ; {
			resp, err := m.cl.GetExport(t.Context(), withToken(&tkv1.GetExportRequest{JobId: job}, exporterToken))
			if err != nil {
				t.Fatal(err)
			}
			if got = resp.Msg; got.GetState() != catalog.ExportQueued && got.GetState() != catalog.ExportRunning || time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond) //nolint:forbidigo // polling a background worker, bounded above
		}
		if got.GetState() != catalog.ExportDone || got.GetUrl() == "" || got.GetDeadline() == nil || got.GetSize() == 0 || got.GetKind() != "canonical" {
			t.Fatalf("job = %v, want done with a URL", got)
		}
		if strings.Contains(got.GetUrl(), "exports/") {
			t.Error("the URL names the object key")
		}
		if rows := auditOf(t, c, "static:exporter"); !containsPrefix(rows, "export-url: job "+job) || !containsPrefix(rows, "export-start: job "+job) {
			t.Errorf("audit rows = %v, want the start and the URL", rows)
		}
		if list, err := m.cl.ListExports(t.Context(), withToken(&tkv1.ListExportsRequest{}, exporterToken)); err != nil || len(list.Msg.GetJobs()) != 1 || list.Msg.GetJobs()[0].GetUrl() != "" {
			t.Errorf("ListExports = %v, %v; want the job, without a URL", list, err)
		}
		// A job is its owner's alone: a principal without a subject, or without the role, is refused.
		for token, code := range map[string]connect.Code{nobodyToken: connect.CodePermissionDenied, viewerToken: connect.CodePermissionDenied} {
			if _, err := m.cl.GetExport(t.Context(), withToken(&tkv1.GetExportRequest{JobId: job}, token)); connect.CodeOf(err) != code {
				t.Errorf("GetExport with another token = %v, want %v", err, code)
			}
		}
		if _, err := m.cl.CancelExport(t.Context(), withToken(&tkv1.CancelExportRequest{JobId: job}, exporterToken)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("CancelExport of a done job = %v, want failed_precondition", err)
		}
	})
}

// auditOf is actor's audit rows, each its action and detail.
func auditOf(t *testing.T, c *catalog.Catalog, actor string) []string {
	t.Helper()
	rows, err := c.AuditByActor(t.Context(), actor)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Action + ": " + r.Detail
	}

	return out
}

func containsPrefix(rows []string, prefix string) bool {
	for _, r := range rows {
		if strings.HasPrefix(r, prefix) {
			return true
		}
	}

	return false
}

// StartExport's refusals, a cancel, and a done job too near its deadline answered expired without a URL.
func TestExportRefusalsAndExpiry(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		now := time.Now()
		m.urls(t, &now, false)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t), hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		start := func(r *tkv1.StartExportRequest) error {
			_, err := m.cl.StartExport(t.Context(), withToken(r, exporterToken))
			return err
		}
		if err := start(exportReq(capture, "filtered")); connect.CodeOf(err) != connect.CodeUnimplemented {
			t.Errorf("kind=filtered = %v, want unimplemented", err)
		}
		if err := start(exportReq(capture, "other")); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("another kind = %v, want invalid_argument", err)
		}
		long := exportReq(capture, "canonical")
		long.Selector.To = timestamppb.New(long.Selector.GetFrom().AsTime().Add(745 * time.Hour))
		if err := start(long); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("a window of 745 hours = %v, want invalid_argument", err)
		}
		m.srv.Exports.Bounds.MaxPacks = 0
		m.srv.Exports.Bounds.MaxBytes = 1
		if err := start(exportReq(capture, "canonical")); connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Errorf("a selection above the bytes' bound = %v, want resource_exhausted", err)
		}
		m.srv.Exports.Bounds = ExportBounds{}
		// A queued job cancelled; cancelled again, it has ended; an unknown job is not found.
		resp, err := m.cl.StartExport(t.Context(), withToken(exportReq(capture, "canonical"), exporterToken))
		if err != nil {
			t.Fatal(err)
		}
		cancel := func(job string) error {
			_, err := m.cl.CancelExport(t.Context(), withToken(&tkv1.CancelExportRequest{JobId: job}, exporterToken))
			return err
		}
		if err := cancel(resp.Msg.GetJobId()); err != nil {
			t.Fatal(err)
		}
		if err := cancel(resp.Msg.GetJobId()); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("cancel again = %v, want failed_precondition", err)
		}
		if err := cancel(newID(t).String()); connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("cancel of an unknown job = %v, want not_found", err)
		}
		// A bundle kept a second: its deadline less the margin leaves no URL its floor.
		m.srv.Exports.Retention = time.Second
		workers(t, m)
		resp, err = m.cl.StartExport(t.Context(), withToken(exportReq(capture, "canonical"), exporterToken))
		if err != nil {
			t.Fatal(err)
		}
		id, err := tracepack.ParseUUID(resp.Msg.GetJobId())
		if err != nil {
			t.Fatal(err)
		}
		if j := awaitOwner(t, c, id, "static:exporter"); j.State != catalog.ExportDone {
			t.Fatalf("job = %+v, want done", j)
		}
		got, err := m.cl.GetExport(t.Context(), withToken(&tkv1.GetExportRequest{JobId: resp.Msg.GetJobId()}, exporterToken))
		if err != nil || got.Msg.GetState() != stateExpired || got.Msg.GetUrl() != "" {
			t.Errorf("GetExport of a bundle near its deadline = %v, %v; want expired, no URL", got, err)
		}
	})
}

// awaitOwner is await for owner's job.
func awaitOwner(t *testing.T, c *catalog.Catalog, job tracepack.UUID, owner string) catalog.ExportJob {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		j, err := c.ExportJob(t.Context(), job, owner)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != catalog.ExportQueued && j.State != catalog.ExportRunning || time.Now().After(deadline) {
			return j
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // polling a background worker's catalog row, bounded above
	}
}
