package api

import (
	"encoding/json"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
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

// An export of a masked capture outside dev, over two hours with a suspect, a confirmed and an acknowledged conflict,
// through StartExport, the worker and the bundle it stored: the manifest names each conflict in its state and each hour with its packs,
// each pack's file is in the bundle, and each conflict's two versions are there, each in its own pack's file.
func TestExportOfConflictsAcrossHours(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)[:1]
		records, entries, packs := threeConflicts(t, m, capture, &cs[0])
		workers(t, m)
		id := capture.String()
		req := &tkv1.StartExportRequest{Kind: "canonical", Selector: &tkv1.Selector{ToolId: tool, CaptureId: &id, From: hourTS(hour), To: hourTS(hour + 2)}}
		started, err := m.cl.StartExport(t.Context(), withToken(req, exporterToken))
		if err != nil {
			t.Fatal(err)
		}
		job, err := tracepack.ParseUUID(started.Msg.GetJobId())
		if err != nil {
			t.Fatal(err)
		}
		j := awaitOwned(t, c, job, "static:exporter")
		if j.State != catalog.ExportDone || j.ObjectKey == nil {
			t.Fatalf("job = %+v, want done", j)
		}

		files, man := stored(t, m, *j.ObjectKey)
		if len(man.Captures) != 1 || len(man.Captures[0].Hours) != 2 {
			t.Fatalf("manifest captures = %+v, want one over two hours", man.Captures)
		}
		var comp tkv1.Completeness
		if err := protojson.Unmarshal(man.Captures[0].Completeness, &comp); err != nil {
			t.Fatal(err)
		}
		states := map[uint64]string{}
		for _, k := range comp.GetConflicts() {
			states[k.GetSeqFrom()] = k.GetState()
		}
		if want := map[uint64]string{3: "suspect", 5: "acknowledged", 7: "confirmed"}; !maps.Equal(states, want) {
			t.Errorf("conflicts = %v, want %v", states, want)
		}
		// Each conflict's two versions: one in each hour, in the pack put there, its record of that hour.
		vs := versionsOf(t, files, man.Captures[0].Hours)
		for _, seq := range []string{"3", "5", "7"} {
			got := vs[seq]
			if len(got) != 2 || got[0].hour == got[1].hour || got[0].ts == got[1].ts {
				t.Errorf("seq %s's versions = %+v, want two, of different hours and times", seq, got)
				continue
			}
			for _, v := range got {
				if !slices.Contains(packs[seq], v.pack) || v.ts/nsPerHour != v.hour {
					t.Errorf("seq %s's version %+v, want it in one of the packs %v, its time in its pack's hour", seq, v, packs[seq])
				}
			}
		}
		if got := scan(t, files, cs); len(got.found) != 0 || got.records != records || got.entries != entries {
			t.Errorf("the stored bundle's scan = %+v; want no secret, %d records and %d redaction entries read", got, records, entries)
		}
	})
}

// stored is the bundle the worker stored under key, unpacked, and its manifest.
func stored(t *testing.T, m *masked, key string) (map[string][]byte, manifest) {
	t.Helper()
	obj, err := m.st.Open(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size()))
	_ = obj.Close()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bundle.tar")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	files := unpacked(t, path)
	var man manifest
	if err := json.Unmarshal(files["manifest.json"], &man); err != nil {
		t.Fatal(err)
	}

	return files, man
}

// version is one exported version of a seq: the pack whose file holds it, that pack's hour, and the record's time.
type version struct {
	pack     string
	hour, ts int64
}

// versionsOf are the versions of each seq the hours' packs export, by seq, each hour checked to have its packs, none left out.
func versionsOf(t *testing.T, files map[string][]byte, hours []manifestHour) map[string][]version {
	t.Helper()
	out := map[string][]version{}
	for _, h := range hours {
		if len(h.Packs) == 0 || len(h.LeftOut) != 0 {
			t.Errorf("hour %s: packs %+v, left out %+v; want its packs, none left out", h.Hour, h.Packs, h.LeftOut)
		}
		start, err := time.Parse(time.RFC3339, h.Hour)
		if err != nil {
			t.Fatalf("hour %q: %v", h.Hour, err)
		}
		for _, p := range h.Packs {
			body, ok := files[p.File]
			if !ok {
				t.Errorf("the bundle holds no %s", p.File)
			}
			for line := range strings.Lines(string(body)) {
				var r struct {
					// Seq and TS are uint64 and int64, as JSON strings; a header line has neither.
					Seq string `json:"seq"`
					TS  string `json:"ts_utc_ns"`
				}
				if err := json.Unmarshal([]byte(line), &r); err != nil {
					t.Fatalf("%s: %v", p.File, err)
				}
				if r.Seq == "" {
					continue
				}
				ts, err := strconv.ParseInt(r.TS, 10, 64)
				if err != nil {
					t.Fatalf("%s seq %s: ts_utc_ns %q: %v", p.File, r.Seq, r.TS, err)
				}
				out[r.Seq] = append(out[r.Seq], version{pack: p.PackID, hour: start.Unix() / 3600, ts: ts})
			}
		}
	}

	return out
}

// threeConflicts puts carrier c's records of capture, masked, over two hours, with seqs 3, 5 and 7 in both:
// 3 left suspect, 5 acknowledged and 7 confirmed by a comparison that found its versions differ;
// it returns how many records and redaction entries it wrote, and the packs holding each of the three seqs' versions.
func threeConflicts(t *testing.T, m *masked, capture tracepack.UUID, c *maskingtest.Carrier) (records, entries int, packs map[string][]string) {
	t.Helper()
	packs = map[string][]string{}
	put := func(h int64, seqs ...uint64) {
		rs := make([]tracepack.Record, 0, len(seqs))
		for _, s := range seqs {
			rs = append(rs, recordAt(c, h, s))
		}
		ms, es := m.mask(t, capture, captured(rs))
		p := m.put(t, capture, h, ms, es, m.policy())
		records, entries = records+len(ms), entries+len(es)
		for _, s := range seqs {
			key := strconv.FormatUint(s, 10)
			packs[key] = append(packs[key], p.ID.String())
		}
	}
	put(hour, 1, 2, 3, 5, 7)
	for _, seq := range []uint64{3, 5, 7} {
		put(hour+1, seq) // each again in the next hour, in a pack of its own: three suspects
	}
	lease, ok, err := m.c.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}
	sus, err := m.c.Suspects(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range sus {
		if s.Capture == capture && s.SeqFrom == 7 {
			if _, err := m.c.RecordConflict(t.Context(), lease, s, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := m.c.AcknowledgeConflict(t.Context(), capture, 5, "op", "a1"); err != nil {
		t.Fatal(err)
	}

	return records, entries, packs
}

// awaitOwned waits, bounded, for owner's job to reach a state other than queued and running, and returns it.
func awaitOwned(t *testing.T, c *catalog.Catalog, job tracepack.UUID, owner string) catalog.ExportJob {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); ; {
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
