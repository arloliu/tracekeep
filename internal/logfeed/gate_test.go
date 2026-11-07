package logfeed

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// The gate's correctness cases (docs/plans/host-log-feed.md step 10a), each through the converter.

var errInjected = errors.New("injected")

// devicePair is a header and its body logged at ts, the header carrying the host program's device-number token.
func devicePair(t *testing.T, f *fixture, token, ts string, stream, function uint8, sys uint32) (*fakeMsg, *fakeMsg) {
	t.Helper()
	_, h, b := f.message(stream, function, stream%2 == 0 && function%2 == 1, sys, secs2.NewListItem(secs2.NewASCIIItem("LOT")), DirReceived)
	with := func(raw []byte, extra map[string]any) *fakeMsg {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		for k, v := range extra {
			m[k] = v
		}
		out, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}

		return &fakeMsg{data: out}
	}

	return with(h.Raw, map[string]any{"device": token, "ts": ts}), with(b.Raw, map[string]any{"ts": ts})
}

// twoDevicesTwoHours is a file's entries on devices eqp (token 1) and lp1 (token 2), each a primary in one hour and another in the next.
func twoDevicesTwoHours(t *testing.T) []*fakeMsg {
	t.Helper()
	f := &fixture{t: t}
	out := make([]*fakeMsg, 0, 8)
	for i, at := range []string{"2026-11-07 08:10:00.000", "2026-11-07 09:20:00.000"} {
		for j, token := range []string{"1", "2"} {
			h, b := devicePair(t, f, token, at, 6, 11, uint32(10*i+j+1)) //nolint:gosec // small test system bytes
			out = append(out, h, b)
		}
	}

	return out
}

var (
	gateFile = catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}
	gateEqp  = catalog.LogKey{LogFile: gateFile, Device: "eqp"}
	gateLp1  = catalog.LogKey{LogFile: gateFile, Device: "lp1"}
)

// redeliver is each message again, as the stream redelivers what was never acknowledged.
func redeliver(ms []*fakeMsg) []*fakeMsg {
	out := make([]*fakeMsg, len(ms))
	for i, m := range ms {
		out[i] = &fakeMsg{data: m.data}
	}

	return out
}

// dataRecord is a data record expected: its entry's system bytes and logged time, in UTC.
type dataRecord struct {
	sys uint32
	at  time.Time
}

// convertedOnce checks each capture holds its start and then want's data records, in order, each in one registered copy at its time,
// seq dense from 0, its highest_seq the last, and the file's watermark watermark.
func convertedOnce(t *testing.T, h *harness, watermark int64, want map[catalog.LogKey][]dataRecord) {
	t.Helper()
	ctx := t.Context()
	for k, sys := range want {
		cp, ok, err := h.c.LogCaptureOf(ctx, k)
		if err != nil || !ok {
			t.Fatalf("%s's capture = %v, %v", k.Device, ok, err)
		}
		recs := h.records(cp.ID)
		if len(recs) != len(sys)+1 || !cp.HasHighestSeq || cp.HighestSeq != int64(len(sys)) {
			t.Errorf("%s: %d records, highest_seq %d (%v); want %d and %d", k.Device, len(recs), cp.HighestSeq, cp.HasHighestSeq, len(sys)+1, len(sys))
			continue
		}
		for i, r := range recs {
			if r.Seq != uint64(i) { //nolint:gosec // a small index
				t.Errorf("%s: record %d is seq %d", k.Device, i, r.Seq)
			}
			if i == 0 {
				if r.Kind != tracepack.KindTransportEvent {
					t.Errorf("%s: record 0 is %v, want the start", k.Device, r.Kind)
				}
				continue
			}
			w := sys[i-1]
			m, err := hsms.DecodeHSMSMessage(r.Payload)
			if err != nil || r.Kind != tracepack.KindData || m.SystemBytes() != hsms.ToSystemBytes(w.sys) || r.TSUTCNs != w.at.UnixNano() {
				t.Errorf("%s: record %d is %v at %d %v (%v), want the data record of system bytes %d at %v", k.Device, i, r.Kind, r.TSUTCNs, m, err, w.sys, w.at)
			}
		}
	}
	if st, err := h.c.LogFileState(ctx, gateFile); err != nil || st.Watermark != watermark {
		t.Errorf("file state = %+v, %v; want watermark %d", st, err, watermark)
	}
}

// bothDevices is twoDevicesTwoHours' records by capture, logged at 08:10 and 09:20 in Asia/Taipei.
var (
	firstHour   = time.Date(2026, 11, 7, 0, 10, 0, 0, time.UTC)
	secondHour  = time.Date(2026, 11, 7, 1, 20, 0, 0, time.UTC)
	bothDevices = map[catalog.LogKey][]dataRecord{
		gateEqp: {{1, firstHour}, {11, secondHour}},
		gateLp1: {{2, firstHour}, {12, secondHour}},
	}
)

// failingPut fails every put while fail is set, storing nothing.
type failingPut struct {
	store.ObjectStore
	fail bool
}

func (s *failingPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if s.fail {
		return errInjected
	}

	return s.ObjectStore.Put(ctx, key, r, size)
}

// failedUnacknowledged checks a failed flush acknowledged none of ms and left orphans puts unregistered, the reconciler's.
func failedUnacknowledged(t *testing.T, h *harness, ms []*fakeMsg, orphans int) {
	t.Helper()
	for i, m := range ms {
		if m.acked() != 0 {
			t.Errorf("entry %d acknowledged by a flush that failed", i)
		}
	}
	n := 0
	_ = h.store.List(t.Context(), "", func(o store.ObjectInfo) error {
		if st, _ := h.c.KeyStanding(t.Context(), o.Key); st == catalog.KeyUnknown {
			n++
		}
		return nil
	})
	if n != orphans {
		t.Errorf("%d unregistered puts, want %d for the reconciler", n, orphans)
	}
}

// One flush of two devices across two hours, refused or lost at each point of its registration (logfeed.md §15.7):
// the converter gives up with nothing acknowledged, and a new one converts the redeliveries once,
// a registration committed under a lost answer leaving them stale; a committed registration retried answers as registered.
func TestGateFlushRefusedAndRecovered(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// inject breaks the converter's first flush; restore undoes it and returns the lease the next converter holds.
		inject  func(t *testing.T, h *harness) (restore func() catalog.Lease)
		fails   bool
		orphans int
	}{
		{name: "lost fence", fails: true, orphans: 4, inject: func(t *testing.T, h *harness) func() catalog.Lease {
			// The lease is lost after every segment was put, as the registration starts.
			h.conv.register = func(ctx context.Context, l catalog.Lease, f *catalog.LogFlush) (catalog.PackOutcome, error) {
				if err := h.c.ReleaseLease(ctx, l); err != nil {
					t.Fatal(err)
				}
				out, err := h.c.RegisterLogFlush(ctx, l, f)
				if !errors.Is(err, catalog.ErrLeaseLost) || len(f.Packs) != 4 {
					t.Errorf("a fenced registration of %d segments = %v, %v; want ErrLeaseLost", len(f.Packs), out, err)
				}

				return out, err
			}

			return func() catalog.Lease {
				l, ok, err := h.c.TakeLease(t.Context(), catalog.LogGroupLease(0), "converter-2", time.Hour)
				if err != nil || !ok {
					t.Fatal(ok, err)
				}

				return l
			}
		}},
		{name: "guard", fails: true, orphans: 4, inject: func(_ *testing.T, h *harness) func() catalog.Lease {
			// The first hour passes the horizon after its entries were converted: the registration refuses the whole flush.
			h.conv.HorizonHour = func() int64 { return 1<<40 - 1 }

			return func() catalog.Lease { return h.conv.Lease }
		}},
		{name: "failed put", fails: true, inject: func(_ *testing.T, h *harness) func() catalog.Lease {
			s := &failingPut{ObjectStore: h.store, fail: true}
			h.conv.Store = s

			return func() catalog.Lease { s.fail = false; return h.conv.Lease }
		}},
		{name: "commit answer lost, committed", fails: true, inject: func(t *testing.T, h *harness) func() catalog.Lease {
			h.conv.register = func(ctx context.Context, l catalog.Lease, f *catalog.LogFlush) (catalog.PackOutcome, error) {
				if out, err := h.c.RegisterLogFlush(ctx, l, f); err != nil || out != catalog.Registered {
					t.Errorf("registration = %v, %v; want it committed", out, err)
				}

				return 0, context.DeadlineExceeded
			}

			return func() catalog.Lease { return h.conv.Lease }
		}},
		{name: "commit answer lost, not committed", fails: true, orphans: 4, inject: func(_ *testing.T, h *harness) func() catalog.Lease {
			h.conv.register = func(context.Context, catalog.Lease, *catalog.LogFlush) (catalog.PackOutcome, error) {
				return 0, context.DeadlineExceeded
			}

			return func() catalog.Lease { return h.conv.Lease }
		}},
		{name: "committed flush retried", inject: func(t *testing.T, h *harness) func() catalog.Lease {
			h.conv.register = func(ctx context.Context, l catalog.Lease, f *catalog.LogFlush) (catalog.PackOutcome, error) {
				if out, err := h.c.RegisterLogFlush(ctx, l, f); err != nil || out != catalog.Registered {
					t.Errorf("first registration = %v, %v", out, err)
				}

				return h.c.RegisterLogFlush(ctx, l, f)
			}

			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				h := newHarness(t, c)
				h.conv.Devices = Devices{Main: "eqp", Rules: []DeviceRule{{Token: "1", Device: "eqp"}, {Token: "2", Device: "lp1"}}}
				first := twoDevicesTwoHours(t)
				dup := &fakeMsg{data: first[0].data} // a redelivery joining its held entry, acknowledged only with it
				h.receive(append(first, dup)...)
				restore := tc.inject(t, h)
				var err error
				for _, after := range []time.Duration{2 * time.Second, 10 * time.Second} {
					h.now = h.now.Add(after)
					if err = h.conv.Tick(t.Context(), h.now); err != nil {
						break
					}
				}
				if (err != nil) != tc.fails {
					t.Fatalf("the flush = %v, want failing %v", err, tc.fails)
				}
				if !tc.fails {
					convertedOnce(t, h, 700, bothDevices)
					if dup.acked() != 1 || first[0].acked() != 1 {
						t.Errorf("an entry and its duplicate acknowledged %d and %d times, want once each", first[0].acked(), dup.acked())
					}

					return
				}
				failedUnacknowledged(t, h, append(first, dup), tc.orphans)

				h.conv.Abandon()
				l := restore()
				h.conv = h.converter(l, firstConfig(h))
				h.conv.Devices = Devices{Main: "eqp", Rules: []DeviceRule{{Token: "1", Device: "eqp"}, {Token: "2", Device: "lp1"}}}
				again := redeliver(first)
				h.receive(again...)
				h.tick(2 * time.Second)
				h.tick(10 * time.Second)
				for i, m := range again {
					if m.acked() != 1 {
						t.Errorf("redelivered entry %d acknowledged %d times, want once", i, m.acked())
					}
				}
				convertedOnce(t, h, 700, bothDevices)
			})
		})
	}
}

// Admission waits for a configuration a log capture may bind (logfeed.md §15.6): the converter gives up with nothing admitted
// or acknowledged, and once one is bindable, the redeliveries are admitted and converted.
func TestGateAdmissionWaitsForABaseline(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		h.conv.Bindable = func(int64) bool { return false }
		f := &fixture{t: t}
		hd, bd := pairMsgs(t, f, 6, 11, true, 1)
		h.receive(hd, bd)
		h.now = h.now.Add(2 * time.Second)
		if err := h.conv.Tick(t.Context(), h.now); !errors.Is(err, ErrGiveUp) || !errors.Is(err, catalog.ErrNoBindableConfig) {
			t.Fatalf("a tick with no bindable configuration = %v, want given up", err)
		}
		if _, ok, _ := c.LogCaptureOf(t.Context(), gateEqp); ok || hd.acked()+bd.acked() != 0 {
			t.Fatalf("admitted %v, acknowledged %d; want nothing", ok, hd.acked()+bd.acked())
		}
		h.conv.Abandon()
		h.conv = h.converter(h.conv.Lease, firstConfig(h))
		h.receive(redeliver([]*fakeMsg{hd, bd})...)
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		convertedOnce(t, h, 100, map[catalog.LogKey][]dataRecord{gateEqp: {{1, time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)}}})
	})
}

// A rotation's old-file tail still inside the reorder window lands in the old capture before its stop (§15.6 *Boundaries*);
// an entry of the stopped file from a device it never had is stale, to the converter that stopped it and to a new one.
func TestGateRotation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		h := newHarness(t, c)
		h.conv.Devices = Devices{Main: "eqp", Rules: []DeviceRule{{Token: "2", Device: "lp1"}}}
		f := &fixture{t: t}
		line := func(file, text string) *fakeMsg {
			return &fakeMsg{data: f.raw(map[string]any{"file": file, "kind": KindUnclassified, "text": text})}
		}
		h.receive(line(gateFile.File, "first"))
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		h.receive(line("host-20261108.log", "next day"))
		h.now = h.now.Add(500 * time.Millisecond)
		tail := line(gateFile.File, "tail") // within the reorder window of the next file's first entry
		h.receive(tail)
		// The next file's entry has left its window and the tail has not: the old file stays open, the tail held.
		h.tick(700 * time.Millisecond)
		if st, _ := c.LogFileState(ctx, gateFile); st.Stopped || tail.acked() != 0 {
			t.Fatalf("the old file stopped %v, its tail acknowledged %d, with the tail still in its window", st.Stopped, tail.acked())
		}
		h.tick(800 * time.Millisecond) // the tail leaves its window: the old file stops at this flush, its tail first
		cp, _, err := c.LogCaptureOf(ctx, gateEqp)
		if err != nil {
			t.Fatal(err)
		}
		kinds := make([]tracepack.Kind, 0, 4)
		var texts []string
		for _, r := range h.records(cp.ID) {
			kinds = append(kinds, r.Kind)
			if a, err := tracepack.UnmarshalAnnotation(r.Payload); r.Kind == tracepack.KindAnnotation && err == nil && a.Text != nil {
				texts = append(texts, *a.Text)
			}
		}
		if len(texts) != 2 || texts[0] != "first" || texts[1] != "tail" {
			t.Errorf("the old capture's annotations %q, want first then tail", texts)
		}
		want := []tracepack.Kind{tracepack.KindTransportEvent, tracepack.KindAnnotation, tracepack.KindAnnotation, tracepack.KindTransportEvent}
		if len(kinds) != len(want) || kinds[2] != want[2] || kinds[3] != want[3] || tail.acked() != 1 {
			t.Errorf("the old capture's records %v, tail acknowledged %d; want %v, the tail before the stop", kinds, tail.acked(), want)
		}
		if st, _ := c.LogFileState(ctx, gateFile); !st.Stopped {
			t.Fatal("the old file is not stopped")
		}

		hd, bd := devicePair(t, f, "2", "2026-11-07 08:00:00.000", 6, 11, 7)
		h.receive(hd, bd)
		if hd.acked() != 1 || bd.acked() != 1 {
			t.Error("a new device's entries of a stopped file were not acknowledged as stale")
		}
		h.conv = h.converter(h.conv.Lease, firstConfig(h))
		again := redeliver([]*fakeMsg{hd, bd})
		h.receive(again...)
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		if again[0].acked() != 1 || again[1].acked() != 1 {
			t.Error("a new converter did not find the stopped file's new device's entries stale")
		}
		if _, ok, _ := c.LogCaptureOf(ctx, gateLp1); ok {
			t.Error("a device the stopped file never had was admitted")
		}
	})
}

// An entry beyond retention is acknowledged at once and writes nothing; entries logged in a DST fall-back hour
// or a spring-forward gap carry ordering-uncertain (§15.5 *Time*).
func TestGateEntries(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		h.conv.HorizonHour = func() int64 { return 491_000 } // 2026-01-05
		f := &fixture{t: t}
		old := &fakeMsg{data: f.raw(map[string]any{"kind": KindUnclassified, "text": "old", "ts": "2025-06-01 08:00:00.000"})}
		h.receive(old)
		if old.acked() != 1 {
			t.Fatal("an entry beyond retention was not acknowledged at once")
		}
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		if _, ok, _ := c.LogCaptureOf(t.Context(), gateEqp); ok {
			t.Fatal("an entry beyond retention admitted a capture")
		}

		ms := make([]*fakeMsg, 0, 4)
		for _, ts := range []string{"2026-11-01 00:30:00.000", "2026-11-01 01:30:00.000", "2027-03-14 02:30:00.000", "2027-03-14 04:00:00.000"} {
			ms = append(ms, &fakeMsg{data: f.raw(map[string]any{"tz": "America/New_York", "kind": KindUnclassified, "text": ts, "ts": ts})})
		}
		h.receive(ms...)
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		cp, ok, err := c.LogCaptureOf(t.Context(), gateEqp)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		var uncertain []bool
		for _, r := range h.records(cp.ID) {
			if r.Kind == tracepack.KindAnnotation {
				uncertain = append(uncertain, r.Quality.HasAny(tracepack.QualityOrderingUncertain))
			}
		}
		if want := []bool{false, true, true, false}; len(uncertain) != len(want) || uncertain[0] || !uncertain[1] || !uncertain[2] || uncertain[3] {
			t.Errorf("ordering-uncertain by entry %v, want %v", uncertain, want)
		}
	})
}
