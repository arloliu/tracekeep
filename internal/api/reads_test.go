package api

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

const (
	tool = "T1"
	// hour is the hour the tests' records fall in.
	hour = int64(495_000)
)

func newID(t *testing.T) tracepack.UUID {
	t.Helper()
	var id tracepack.UUID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}

	return id
}

// world is a catalog with the machine API served over it.
type world struct {
	c *catalog.Catalog
	// lease is the compactor lease urls took, to record retention under.
	lease catalog.Lease
	cl    tracekeepv1connect.MachineServiceClient
	srv   *Server
	hs    *httptest.Server
}

func newWorld(t *testing.T, c *catalog.Catalog) *world {
	t.Helper()
	w := &world{c: c, srv: &Server{Catalog: c, QuietPeriod: 15 * time.Minute, Auth: testAuthenticator(nil)}}
	w.hs = httptest.NewServer(w.srv.Handler())
	t.Cleanup(w.hs.Close)
	w.cl = tracekeepv1connect.NewMachineServiceClient(w.hs.Client(), w.hs.URL)

	return w
}

// admit admits a fresh capture of device eqp begun at hour's start, as ingest's admission of its start would.
func (w *world) admit(t *testing.T) tracepack.UUID {
	t.Helper()
	if _, err := w.c.ActivateConfig(t.Context(), []byte("config")); err != nil {
		t.Fatal(err)
	}

	return w.admitActive(t)
}

// admitActive is admit under the configurations already active, the capture binding the newest.
func (w *world) admitActive(t *testing.T) tracepack.UUID {
	t.Helper()
	origin := hour * nsPerHour
	ctx := t.Context()
	infos, err := w.c.Configs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var active []int64
	for _, ci := range infos {
		if ci.ActivatedAt.Valid && !ci.RetiredAt.Valid {
			active = append(active, ci.ID)
		}
	}
	if _, err := w.c.Register(ctx, "r", active, time.Hour); err != nil {
		t.Fatal(err)
	}
	name, instance := "eqp", newID(t)
	mono, tolerance := int64(0), uint64(time.Second)
	m := &tracepack.PackMeta{
		ToolID: tool, Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
		Vantage: tracepack.VantageHost, Recorder: "test", TimeSource: tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs: &origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
		LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		RecorderInstanceID: instance, RecorderInstanceName: &name,
	}
	b, err := tracepack.EncodeDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	id := newID(t)
	d := &catalog.Identity{Version: catalog.VersionKey{Capture: id}, Incarnation: 1, StreamSeq: 1, Hour: hour}
	if _, err := w.c.Admit(ctx, d, &catalog.Descriptor{Bytes: b, ToolID: tool, RecorderInstanceID: instance, Device: name, OriginNS: origin}); err != nil {
		t.Fatal(err)
	}

	return id
}

// stage registers a segment of capture in hour holding seqs 0..9, their records at hour's start plus the seq in ns.
func (w *world) stage(t *testing.T, capture tracepack.UUID) *catalog.Pack {
	t.Helper()
	from, to := uint64(0), uint64(9)
	id := newID(t)
	base := hour * nsPerHour
	p := &catalog.Pack{
		ID: id, Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleSegment, Key: "staging/" + id.String(), Size: 100,
		SeqStart: from, FormatMinor: 1,
		Ranges: []catalog.Range{{SeqFrom: from, SeqTo: to, TSFrom: base + int64(from), TSTo: base + int64(to)}}, //nolint:gosec // small seqs
	}
	if out, err := w.c.RegisterPack(t.Context(), p, catalog.Registration{Evidence: &catalog.Evidence{}}); err != nil || out != catalog.Registered {
		t.Fatalf("RegisterPack = %v, %v", out, err)
	}

	return p
}

func sel(from, to int64) *tkv1.Selector {
	return &tkv1.Selector{ToolId: tool, From: timestamppb.New(time.Unix(0, from)), To: timestamppb.New(time.Unix(0, to))}
}

func reader[T any](msg *T) *connect.Request[T] { return withToken(msg, readerToken) }

// ListDevices, ListCaptures and ListPacks answer what the catalog's reads answer, mapped field for field.
func TestReads(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		w := newWorld(t, c)
		ctx := t.Context()
		base := hour * nsPerHour
		capture := w.admit(t)
		p := w.stage(t, capture)
		window := sel(base, base+nsPerHour)

		devs, err := w.cl.ListDevices(ctx, reader(&tkv1.ListDevicesRequest{Selector: window}))
		if err != nil || len(devs.Msg.Devices) != 1 || devs.Msg.Devices[0].Name != "eqp" || devs.Msg.Devices[0].OpenCaptures != 1 ||
			devs.Msg.Devices[0].GetLastRecord().AsTime().UnixNano() != base+9 {
			t.Errorf("ListDevices = %v, %v", devs, err)
		}
		caps, err := w.cl.ListCaptures(ctx, reader(&tkv1.ListCapturesRequest{Selector: window}))
		if err != nil || len(caps.Msg.Captures) != 1 || caps.Msg.Captures[0].CaptureId != capture.String() ||
			caps.Msg.Captures[0].State != catalog.StateOpen || caps.Msg.Captures[0].Origin.AsTime().UnixNano() != base {
			t.Errorf("ListCaptures = %v, %v", caps, err)
		}
		packs, err := w.cl.ListPacks(ctx, reader(&tkv1.ListPacksRequest{Selector: window}))
		if err != nil {
			t.Fatal(err)
		}
		onePack(t, packs.Msg.Hours, p)
		if cs := packs.Msg.Captures; len(cs) != 1 || cs[0].Completeness.GetFrontier() != 9 || len(cs[0].Completeness.Retained) != 1 {
			t.Errorf("ListPacks captures = %v", cs)
		}
	})
}

// ListScopeChanges pages the selected tool's events, its window filtering none, and reports a cursor of another epoch stale.
func TestListScopeChanges(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		w := newWorld(t, c)
		ctx := t.Context()
		base := hour * nsPerHour
		w.stage(t, w.admit(t))
		window := sel(base, base+nsPerHour)
		packs, err := w.cl.ListPacks(ctx, reader(&tkv1.ListPacksRequest{Selector: window}))
		if err != nil {
			t.Fatal(err)
		}
		changes, err := w.cl.ListScopeChanges(ctx, reader(&tkv1.ListScopeChangesRequest{Selector: window}))
		if err != nil || len(changes.Msg.Events) != 1 || changes.Msg.Events[0].Event != "created" || changes.Msg.Stale ||
			changes.Msg.Next.Seq != packs.Msg.Cursor.Seq {
			t.Errorf("ListScopeChanges from the start = %v, %v; want the created event, next at the listing's cursor", changes, err)
		}
		w.stage(t, w.admit(t)) // another capture's scope: a created event
		// The window does not filter the events: one far from the change still follows it.
		elsewhere := sel(base+100*nsPerHour, base+101*nsPerHour)
		next, err := w.cl.ListScopeChanges(ctx, reader(&tkv1.ListScopeChangesRequest{After: packs.Msg.Cursor, Selector: elsewhere}))
		if err != nil || len(next.Msg.Events) != 1 || next.Msg.Events[0].Seq <= packs.Msg.Cursor.Seq {
			t.Errorf("ListScopeChanges after the listing = %v, %v; want the other capture's created event", next, err)
		}
		stale, err := w.cl.ListScopeChanges(ctx, reader(&tkv1.ListScopeChangesRequest{After: &tkv1.Cursor{Epoch: 9}, Selector: window}))
		if err != nil || !stale.Msg.Stale {
			t.Errorf("ListScopeChanges under another epoch = %v, %v; want stale", stale, err)
		}
	})
}

// Gate case 1 through the API: the VE service lists a settled hour, a later registration reopens it,
// and ListScopeChanges from the listing's cursor reports the reopen at the scope's new revision, which a relisting shows.
func TestSettledHourReopenedThroughTheAPI(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		w := newWorld(t, c)
		ctx := t.Context()
		capture := w.admit(t)
		in := w.stage(t, capture)
		lease, ok, err := c.TakeLease(ctx, "compactor/2", "merger", time.Hour)
		if err != nil || !ok {
			t.Fatalf("TakeLease = %v, %v", ok, err)
		}
		scope := catalog.Scope{Capture: capture, Hour: hour}
		snap, err := c.MergeSnapshot(ctx, scope)
		if err != nil {
			t.Fatal(err)
		}
		id := newID(t)
		archive := &catalog.Pack{
			ID: id, Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleArchive, Key: "archive/" + id.String(), Size: 200,
			SeqStart: in.SeqStart, FormatMinor: 1, Ranges: in.Ranges,
		}
		m := &catalog.Merge{Scope: scope, Revision: snap.Revision, Inputs: []tracepack.UUID{in.ID}, Archive: archive, DeleteDelay: time.Hour}
		if out, err := c.CommitMerge(ctx, lease, m); err != nil || out != catalog.Merged {
			t.Fatalf("CommitMerge = %v, %v", out, err)
		}
		base := hour * nsPerHour
		window := sel(base, base+nsPerHour)
		listing, err := w.cl.ListPacks(ctx, reader(&tkv1.ListPacksRequest{Selector: window}))
		if err != nil || len(listing.Msg.Hours) != 1 || len(listing.Msg.Hours[0].Scopes) != 1 || listing.Msg.Hours[0].Scopes[0].State != "settled" {
			t.Fatalf("ListPacks = %v, %v; want the settled scope", listing, err)
		}
		settled := listing.Msg.Hours[0].Scopes[0]
		late := &catalog.Pack{
			ID: newID(t), Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleSegment, Size: 100, SeqStart: 10, FormatMinor: 1,
			Ranges: []catalog.Range{{SeqFrom: 10, SeqTo: 19, TSFrom: base + 10, TSTo: base + 19}},
		}
		late.Key = "staging/" + late.ID.String()
		if out, err := c.RegisterPack(ctx, late, catalog.Registration{Evidence: &catalog.Evidence{}}); err != nil || out != catalog.Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		changes, err := w.cl.ListScopeChanges(ctx, reader(&tkv1.ListScopeChangesRequest{After: listing.Msg.Cursor, Selector: window}))
		if err != nil || len(changes.Msg.Events) != 1 {
			t.Fatalf("ListScopeChanges = %v, %v; want the reopen", changes, err)
		}
		ev := changes.Msg.Events[0]
		if ev.Event != "reopened" || ev.CaptureId != capture.String() || ev.Hour.AsTime().UnixNano() != base || ev.Revision <= settled.Revision {
			t.Errorf("event %v, want the reopen of the hour above revision %d", ev, settled.Revision)
		}
		relisted, err := w.cl.ListPacks(ctx, reader(&tkv1.ListPacksRequest{Selector: window}))
		if err != nil || relisted.Msg.Hours[0].Scopes[0].State != "reopened" || relisted.Msg.Hours[0].Scopes[0].Revision != ev.Revision {
			t.Errorf("relisted = %v, %v; want the scope reopened at the event's revision", relisted, err)
		}
	})
}

// onePack checks that hours are hour alone, holding one scope of p alone, its range 0..9 at hour's start plus the seq.
func onePack(t *testing.T, hours []*tkv1.Hour, p *catalog.Pack) {
	t.Helper()
	base := hour * nsPerHour
	if len(hours) != 1 || hours[0].Hour.AsTime().UnixNano() != base || len(hours[0].Scopes) != 1 || len(hours[0].Scopes[0].Packs) != 1 {
		t.Fatalf("ListPacks hours = %v", hours)
	}
	got := hours[0].Scopes[0].Packs[0]
	if got.PackId != p.ID.String() || got.State != "staged" || len(got.Ranges) != 1 || got.Ranges[0].SeqTo != 9 ||
		got.Ranges[0].To.AsTime().UnixNano() != base+9 {
		t.Errorf("ListPacks pack = %v, want %s holding 0..9", got, p.ID)
	}
}

func TestSelectorRefused(t *testing.T) {
	t.Parallel()
	w := newWorld(t, catalogtest.Open(t, catalog.EngineSQLite))
	base := hour * nsPerHour
	ok := sel(base, base+nsPerHour)
	with := func(f func(s *tkv1.Selector)) *tkv1.Selector {
		s := sel(base, base+nsPerHour)
		f(s)

		return s
	}
	many := make([]string, 1001)
	for i := range many {
		many[i] = fmt.Sprintf("d%d", i)
	}
	bad := "not-a-uuid"
	for name, s := range map[string]*tkv1.Selector{
		"none":            nil,
		"no tool":         with(func(s *tkv1.Selector) { s.ToolId = "" }),
		"no from":         with(func(s *tkv1.Selector) { s.From = nil }),
		"an empty window": with(func(s *tkv1.Selector) { s.To = s.From }),
		"1,001 devices":   with(func(s *tkv1.Selector) { s.Devices = many }),
		"a bad capture":   with(func(s *tkv1.Selector) { s.CaptureId = &bad }),
	} {
		ctx := context.Background()
		_, e1 := w.cl.ListDevices(ctx, reader(&tkv1.ListDevicesRequest{Selector: s}))
		_, e2 := w.cl.ListCaptures(ctx, reader(&tkv1.ListCapturesRequest{Selector: s}))
		_, e3 := w.cl.ListPacks(ctx, reader(&tkv1.ListPacksRequest{Selector: s}))
		_, e4 := w.cl.ListScopeChanges(ctx, reader(&tkv1.ListScopeChangesRequest{Selector: s}))
		for i, err := range []error{e1, e2, e3, e4} {
			// ListScopeChanges ignores the window, so its emptiness is no refusal there.
			if i == 3 && name == "an empty window" {
				continue
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("%s: read %d = %v, want invalid argument", name, i, err)
			}
		}
	}
	year := sel(base, base+8785*nsPerHour)
	if _, err := w.cl.ListPacks(context.Background(), reader(&tkv1.ListPacksRequest{Selector: year})); connect.CodeOf(err) != connect.CodeInvalidArgument ||
		!strings.Contains(err.Error(), "8784") {
		t.Errorf("ListPacks of 8,785 hours = %v, want invalid argument", err)
	}
	if _, err := w.cl.ListPacks(context.Background(), reader(&tkv1.ListPacksRequest{Selector: ok})); err != nil {
		t.Errorf("ListPacks of a valid window = %v", err)
	}
}

// A page above 10,000 events is refused, and a read the catalog fails is unavailable, which a client retries.
func TestReadRefusals(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	w := newWorld(t, c)
	window := sel(hour*nsPerHour, (hour+1)*nsPerHour)
	for limit, want := range map[uint32]connect.Code{0: 0, maxPage: 0, maxPage + 1: connect.CodeInvalidArgument} {
		_, err := w.cl.ListScopeChanges(t.Context(), reader(&tkv1.ListScopeChangesRequest{Selector: window, Limit: limit}))
		if got := connect.CodeOf(err); err != nil && got != want || err == nil && want != 0 {
			t.Errorf("ListScopeChanges limit %d = %v, want %v", limit, err, want)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.cl.ListPacks(t.Context(), reader(&tkv1.ListPacksRequest{Selector: window})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("ListPacks over a closed catalog = %v, want unavailable", err)
	}
}
