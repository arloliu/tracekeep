package catalog

import (
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

const readQ = 15 * time.Minute

// spec describes a capture as its start descriptor states it.
type spec struct {
	instance tracepack.UUID
	device   string
	origin   int64
	previous *tracepack.UUID
	endpoint string
	deviceID uint64
	thinning []tracepack.Thinning
	toolID   string
	rejectIt bool
}

// described admits a capture of tool T1 (or s.toolID) with a real descriptor, as ingest's admission of its start would.
func described(t *testing.T, c *Catalog, s spec) tracepack.UUID {
	t.Helper()
	gate(t, c)
	if s.toolID == "" {
		s.toolID = "T1"
	}
	if s.device == "" {
		s.device = "eqp"
	}
	mono, tolerance := int64(0), uint64(time.Second)
	m := &tracepack.PackMeta{
		ToolID: s.toolID, Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
		Vantage: tracepack.VantageHost, Recorder: "test", TimeSource: tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs: &s.origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
		LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		RecorderInstanceID: s.instance, RecorderInstanceName: &s.device, PreviousCaptureID: s.previous,
		Thinning: s.thinning,
	}
	if s.endpoint != "" {
		m.EquipmentEndpoint = &s.endpoint
		m.DeviceID = &s.deviceID
	}
	b, err := tracepack.EncodeDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	id := newID(t)
	d := &Descriptor{Bytes: b, ToolID: s.toolID, RecorderInstanceID: s.instance, Device: s.device, OriginNS: s.origin, PreviousCaptureID: s.previous}
	if s.rejectIt {
		if _, err := c.Reject(t.Context(), start(id), b); err != nil {
			t.Fatal(err)
		}

		return id
	}
	if _, err := c.Admit(t.Context(), start(id), d); err != nil {
		t.Fatal(err)
	}

	return id
}

// exec runs a statement on c's write pool, setting up a state the test needs.
func exec(t *testing.T, c *Catalog, query string, args ...any) {
	t.Helper()
	if _, err := c.write.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatal(err)
	}
}

// lastRecord sets the time of capture's last record, its span's end, leaving its seqs and so its completeness alone.
func lastRecord(t *testing.T, c *Catalog, capture tracepack.UUID, ts int64) {
	t.Helper()
	exec(t, c, `UPDATE captures SET highest_seq_ts = ? WHERE capture_id = ?`, ts, capture[:])
}

func listCaptures(t *testing.T, c *Catalog, sel Selector) map[tracepack.UUID]CaptureInfo {
	t.Helper()
	got, err := c.ListCaptures(t.Context(), sel, readQ)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[tracepack.UUID]CaptureInfo, len(got))
	for _, ci := range got {
		out[ci.ID] = ci
	}

	return out
}

func window(from, to int64) Selector { return Selector{ToolID: "T1", From: from, To: to} }

// Overlap comes from the spans of one recorder's captures, survives a close, and is never computed across recorders;
// an unregistered interval is reported against the latest span end of every earlier capture, only past Q,
// and for a predecessor the catalog never admitted.
func TestListCapturesSpans(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		q := int64(readQ)
		r, other := newID(t), newID(t)
		a := described(t, c, spec{instance: r, origin: 0})
		lastRecord(t, c, a, 100)
		b := described(t, c, spec{instance: r, origin: 50})
		lastRecord(t, c, b, 60)
		atQ := described(t, c, spec{instance: r, origin: 100 + q}) // exactly Q after a's end: no interval, and no record yet
		pastQ := described(t, c, spec{instance: r, origin: 100 + 2*q + 1})
		lastRecord(t, c, pastQ, 100+3*q)
		missing := newID(t)
		orphan := described(t, c, spec{instance: other, origin: 70, previous: &missing})
		lastRecord(t, c, orphan, 80)
		known := described(t, c, spec{instance: newID(t), origin: 10, previous: &a})
		exec(t, c, `UPDATE captures SET state = 'closed' WHERE capture_id = ?`, a[:])

		got := listCaptures(t, c, window(0, 1<<62))
		if len(got) != 6 {
			t.Fatalf("listed %d captures, want 6", len(got))
		}
		for id, want := range map[tracepack.UUID]bool{a: true, b: true, atQ: false, pastQ: false, orphan: false, known: false} {
			if got[id].Overlap != want {
				t.Errorf("capture origin %d: overlap %v, want %v", got[id].OriginNS, got[id].Overlap, want)
			}
		}
		for _, id := range []tracepack.UUID{a, b, atQ, known} {
			if u := got[id].Unregistered; len(u) != 0 {
				t.Errorf("capture origin %d: unregistered %+v, want none", got[id].OriginNS, u)
			}
		}
		// pastQ's earlier captures end at 100 (a) and at atQ's origin, which has no record.
		if u := got[pastQ].Unregistered; len(u) != 1 || u[0].Start == nil || *u[0].Start != 100+q || u[0].End != 100+2*q+1 {
			t.Errorf("past Q: unregistered %+v, want from atQ's origin to its own", u)
		}
		if u := got[orphan].Unregistered; len(u) != 1 || u[0].Start != nil || u[0].End != 70 {
			t.Errorf("missing predecessor: unregistered %+v, want an open-started interval to its origin", u)
		}
		if got[a].State != StateClosed || got[known].PreviousCaptureID == nil || *got[known].PreviousCaptureID != a {
			t.Errorf("state %v, previous %v", got[a].State, got[known].PreviousCaptureID)
		}
	})
}

// A window selects the admitted captures of the tool and devices whose span meets it, an open capture's span unbounded.
func TestListCapturesSelection(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ended := described(t, c, spec{instance: newID(t), origin: 10})
		lastRecord(t, c, ended, 20)
		exec(t, c, `UPDATE captures SET state = 'stopped' WHERE capture_id = ?`, ended[:])
		stillOpen := described(t, c, spec{instance: newID(t), origin: 10})
		lastRecord(t, c, stillOpen, 20)
		late := described(t, c, spec{instance: newID(t), origin: 100})
		other := described(t, c, spec{instance: newID(t), origin: 50, device: "pdf"})
		described(t, c, spec{instance: newID(t), origin: 50, toolID: "T2"})
		described(t, c, spec{instance: newID(t), origin: 50, rejectIt: true})
		unknown := newID(t)
		exec(t, c, `INSERT INTO captures (capture_id, state, created_at) VALUES (?, 'pending', 0)`, unknown[:])

		ids := func(sel Selector) []tracepack.UUID {
			got := listCaptures(t, c, sel)
			out := make([]tracepack.UUID, 0, len(got))
			for id := range got {
				out = append(out, id)
			}
			slices.SortFunc(out, compareUUID)

			return out
		}
		sorted := func(us ...tracepack.UUID) []tracepack.UUID { slices.SortFunc(us, compareUUID); return us }
		for name, tc := range map[string]struct {
			sel  Selector
			want []tracepack.UUID
		}{
			"after the stopped one's end": {window(30, 100), sorted(stillOpen, other)},
			"at its end":                  {window(20, 100), sorted(ended, stillOpen, other)},
			"through the late origin":     {window(30, 101), sorted(stillOpen, other, late)},
			"one device":                  {Selector{ToolID: "T1", Devices: []string{"pdf"}, From: 0, To: 200}, sorted(other)},
			"one capture":                 {Selector{ToolID: "T1", Capture: &late, From: 0, To: 200}, sorted(late)},
		} {
			if got := ids(tc.sel); !slices.Equal(got, tc.want) {
				t.Errorf("%s: %d captures, want %d", name, len(got), len(tc.want))
			}
		}
	})
}

// A capture's listing carries what its descriptor and admission recorded.
func TestListCapturesDescriptor(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		thin := []tracepack.Thinning{{Stream: 6, Function: 11, Source: "fdc"}}
		r := newID(t)
		id := described(t, c, spec{instance: r, device: "eqp", origin: 5, thinning: thin})
		got := listCaptures(t, c, window(0, 10))[id]
		if got.Device != "eqp" || got.RecorderInstanceID != r || !slices.Equal(got.Thinning, thin) || got.ConfigID == nil || got.State != StateOpen {
			t.Errorf("listing %+v", got)
		}
	})
}

// A device reports its latest capture's endpoint and device id, its open captures, overlap,
// its last record time and whether a gap's interval meets the window.
func TestListDevices(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		r := newID(t)
		old := described(t, c, spec{instance: r, origin: 0, endpoint: "10.0.0.1:5000", deviceID: 1})
		pack(t, c, old, [2]uint64{0, 3}, [2]uint64{6, 9}) // a gap at 4–5, between the records at 3 and 6
		exec(t, c, `UPDATE captures SET state = 'closed' WHERE capture_id = ?`, old[:])
		newer := described(t, c, spec{instance: r, origin: 8, endpoint: "10.0.0.2:5000", deviceID: 2})
		lastRecord(t, c, newer, 40)
		described(t, c, spec{instance: newID(t), device: "pdf", origin: 1})

		devices := func(from, to int64) map[string]DeviceInfo {
			got, err := c.ListDevices(t.Context(), window(from, to), readQ)
			if err != nil {
				t.Fatal(err)
			}
			out := map[string]DeviceInfo{}
			for _, d := range got {
				out[d.Name] = d
			}

			return out
		}
		got := devices(0, 100)
		eqp := got["eqp"]
		if eqp.EquipmentEndpoint == nil || *eqp.EquipmentEndpoint != "10.0.0.2:5000" || eqp.DeviceID == nil || *eqp.DeviceID != 2 {
			t.Errorf("eqp endpoint %v, device id %v; want the newer capture's", eqp.EquipmentEndpoint, eqp.DeviceID)
		}
		if eqp.OpenCaptures != 1 || !eqp.Overlap || eqp.LastRecordNS == nil || *eqp.LastRecordNS != 40 || !eqp.Gap {
			t.Errorf("eqp %+v", eqp)
		}
		if pdf := got["pdf"]; pdf.OpenCaptures != 1 || pdf.Overlap || pdf.LastRecordNS != nil || pdf.Gap || pdf.EquipmentEndpoint != nil {
			t.Errorf("pdf %+v", pdf)
		}
		// After the gap's later record: the gap no longer meets the window.
		if eqp := devices(7, 100)["eqp"]; eqp.Gap {
			t.Error("a gap ending before the window reported in it")
		}
	})
}
