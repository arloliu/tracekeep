package logfeed

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/secs2"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/store"
)

// fakeMsg is a delivery on subject tklog.0.T1, or subject, whose acknowledgements are counted.
type fakeMsg struct {
	subject              string
	data                 []byte
	mu                   sync.Mutex
	acks, naks, progress int
	// ackErr fails every acknowledgement.
	ackErr error
}

func (m *fakeMsg) Subject() string { return cmp.Or(m.subject, "tklog.0.T1") }
func (m *fakeMsg) Data() []byte    { return m.data }
func (m *fakeMsg) Ack() error {
	if m.ackErr != nil {
		return m.ackErr
	}
	m.mu.Lock()
	m.acks++
	m.mu.Unlock()

	return nil
}
func (m *fakeMsg) Nak() error { m.mu.Lock(); m.naks++; m.mu.Unlock(); return nil }
func (m *fakeMsg) InProgress() error {
	m.mu.Lock()
	m.progress++
	m.mu.Unlock()

	return nil
}

func (m *fakeMsg) acked() int { m.mu.Lock(); defer m.mu.Unlock(); return m.acks }

// harness is a converter of group 0 over a catalog and an in-memory store, its clock driven by the test.
type harness struct {
	t     *testing.T
	c     *catalog.Catalog
	store store.ObjectStore
	conv  *Converter
	now   time.Time
}

func newHarness(t *testing.T, c *catalog.Catalog) *harness {
	t.Helper()
	ctx := t.Context()
	cfg, err := c.ActivateConfig(ctx, []byte(`{"classifier":"go-secs/v2.8.0","max_frame_len":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Register(ctx, "r", []int64{cfg}, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureLogGroups(ctx, 1); err != nil {
		t.Fatal(err)
	}
	l, ok, err := c.TakeLease(ctx, catalog.LogGroupLease(0), "converter", time.Hour)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	h := &harness{t: t, c: c, store: store.NewMem(), now: time.Unix(1_800_000_000, 0)}
	h.conv = h.converter(l, cfg)

	return h
}

func (h *harness) converter(l catalog.Lease, cfg int64) *Converter {
	return &Converter{
		Lease: l, Group: 0, Catalog: h.c, Store: h.store, WorkDir: h.t.TempDir(), Writer: "tracekeep-test",
		Configs: map[int64]ingest.Config{cfg: {Name: "go-secs/v2.8.0"}}, Devices: Devices{Main: "eqp"},
		Flush: 10 * time.Second, Reorder: time.Second, Idle: time.Hour, Pairing: 3,
		HorizonHour: func() int64 { return 0 }, Now: func() time.Time { return h.now },
	}
}

func (h *harness) receive(ms ...*fakeMsg) {
	h.t.Helper()
	for _, m := range ms {
		if err := h.conv.Receive(h.t.Context(), m, h.now); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *harness) tick(after time.Duration) {
	h.t.Helper()
	h.now = h.now.Add(after)
	if err := h.conv.Tick(h.t.Context(), h.now); err != nil {
		h.t.Fatal(err)
	}
}

// records reads every registered pack of capture back with tracepack's reader, in seq order, after verifying each
// and checking each record lies in its pack's hour.
func (h *harness) records(capture tracepack.UUID) []tracepack.Record {
	h.t.Helper()
	ctx := h.t.Context()
	var out []tracepack.Record
	err := h.store.List(ctx, "", func(o store.ObjectInfo) error {
		if !strings.Contains(o.Key, capture.String()) {
			return nil
		}
		// Only a registered pack is converted; an unregistered put is the reconciler's orphan.
		if st, err := h.c.KeyStanding(ctx, o.Key); err != nil || st != catalog.KeyRegistered {
			return err
		}
		obj, err := h.store.Open(ctx, o.Key)
		if err != nil {
			return err
		}
		defer obj.Close()
		r, err := tracepack.Open(ctx, obj, obj.Size(), tracepack.ReaderOptions{})
		if err != nil {
			return err
		}
		if rep, err := r.Verify(ctx); err != nil || rep.Outcome != tracepack.VerifyOutcomeFinalizedConsistent {
			return fmt.Errorf("verify %s: %+v, %w", o.Key, rep, err)
		}
		meta := r.Header().Meta
		_, err = r.Iterate(ctx, tracepack.Query{Payloads: true}, func(it *tracepack.Item) error {
			if ts := it.Record.TSUTCNs; ts < meta.PeriodStart || ts >= meta.PeriodEnd {
				return fmt.Errorf("%s: seq %d at %d outside its pack's hour [%d, %d)", o.Key, it.Record.Seq, ts, meta.PeriodStart, meta.PeriodEnd)
			}
			out = append(out, it.Clone().Record) // the read reuses its buffers
			return nil
		})

		return err
	})
	if err != nil {
		h.t.Fatal(err)
	}
	slices.SortFunc(out, func(a, b tracepack.Record) int { return int(a.Seq) - int(b.Seq) }) //nolint:gosec // small test seqs

	return out
}

func msgOf(t *testing.T, f *fixture, fields map[string]any) *fakeMsg {
	t.Helper()

	return &fakeMsg{data: f.raw(fields)}
}

func pairMsgs(t *testing.T, f *fixture, stream, function uint8, w bool, sys uint32) (*fakeMsg, *fakeMsg) {
	t.Helper()
	_, h, b := f.message(stream, function, w, sys, secs2.NewListItem(secs2.NewASCIIItem("LOT")), DirReceived)

	return &fakeMsg{data: h.Raw}, &fakeMsg{data: b.Raw}
}

// A feed of synthetic entries becomes a log capture whose packs tracepack reads back: a start boundary, data records and annotations,
// seq dense from 0; every entry is acknowledged once registered, the file's watermark raised, and redeliveries are stale.
func TestConverterEndToEnd(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		h := newHarness(t, c)
		f := &fixture{t: t}
		h1, b1 := pairMsgs(t, f, 6, 11, true, 1)
		line := msgOf(t, f, map[string]any{"kind": KindUnclassified, "text": "a line"})
		bad := msgOf(t, f, map[string]any{"kind": KindBody, "sxfy": "S6F11", "text": 17})
		unreadable := &fakeMsg{data: []byte("{nope")}
		h2, b2 := pairMsgs(t, f, 6, 12, false, 1)
		all := make([]*fakeMsg, 0, 8)
		all = append(all, h1, b1, line, bad, h2, b2)
		h.receive(append(all, unreadable, &fakeMsg{data: b1.data})...)
		if unreadable.acked() != 1 {
			t.Error("an unreadable entry was not acknowledged at once")
		}
		h.tick(2 * time.Second)  // released and converted
		h.tick(10 * time.Second) // flushed
		for i, m := range all {
			if m.acked() == 0 {
				t.Errorf("entry %d not acknowledged after the flush", i)
			}
		}
		cp, ok, err := c.LogCaptureOf(ctx, catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: "eqp"})
		if err != nil || !ok {
			t.Fatalf("the capture = %v, %v", ok, err)
		}
		recs := h.records(cp.ID)
		kinds := make([]string, 0, len(recs))
		for i, r := range recs {
			if r.Seq != uint64(i) { //nolint:gosec // a small index
				t.Fatalf("seq %d at %d, want dense from 0", r.Seq, i)
			}
			kinds = append(kinds, r.Kind.String())
		}
		if got := strings.Join(kinds, " "); got != "transport-event data annotation annotation data" {
			t.Errorf("records %s", got)
		}
		st, err := c.LogFileState(ctx, catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"})
		if err != nil || st.Stopped || st.Watermark != 500 {
			t.Errorf("file state = %+v, %v; want watermark 500 and open", st, err)
		}

		// A restarted converter: a redelivery at or below the watermark is stale; a new entry continues the capture's seq.
		h.conv = h.converter(h.conv.Lease, cp.ConfigID)
		again := &fakeMsg{data: b2.data}
		h3, b3 := pairMsgs(t, f, 1, 3, true, 9)
		h.receive(again, h3, b3)
		if again.acked() != 1 {
			t.Error("a redelivery below the watermark was not acknowledged as stale")
		}
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		if recs = h.records(cp.ID); len(recs) != 6 || recs[5].Seq != 5 || recs[5].Kind != tracepack.KindData {
			t.Errorf("after the restart %d records, the last %+v", len(recs), recs[len(recs)-1])
		}
	})
}

// A file stops once the source's next dated file's first entry has left the window, its captures stopped by a stop boundary;
// an operator's stop request is written at the next flush; a later entry of a stopped file or capture is stale.
func TestConverterStops(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		h := newHarness(t, c)
		f := &fixture{t: t}
		h1, b1 := pairMsgs(t, f, 6, 11, true, 1)
		h.receive(h1, b1)
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		old := catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: "eqp"}
		oc, _, err := c.LogCaptureOf(ctx, old)
		if err != nil {
			t.Fatal(err)
		}

		next := &fakeMsg{data: f.raw(map[string]any{"file": "host-20261108.log", "kind": KindUnclassified, "text": "next day"})}
		h.receive(next)
		h.tick(2 * time.Second) // the successor's entry leaves its window: the old file stops at this flush
		if s, err := c.Capture(ctx, oc.ID); err != nil || s.State != catalog.StateStopped {
			t.Fatalf("the old file's capture = %+v, %v; want stopped", s, err)
		}
		if st, _ := c.LogFileState(ctx, old.LogFile); !st.Stopped {
			t.Error("the old file is not stopped")
		}
		recs := h.records(oc.ID)
		if last := recs[len(recs)-1]; last.Kind != tracepack.KindTransportEvent {
			t.Errorf("the old capture's last record %v, want its stop boundary", last.Kind)
		}
		late := &fakeMsg{data: f.raw(map[string]any{"kind": KindUnclassified, "text": "late"})}
		h.receive(late)
		if late.acked() != 1 {
			t.Error("an entry of a stopped file was not acknowledged as stale")
		}

		h.tick(10 * time.Second)
		nc, ok, err := c.LogCaptureOf(ctx, catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261108.log"}, Device: "eqp"})
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		if out, err := c.StopLogCapture(ctx, nc.ID, "operator", "stop-1"); err != nil || out != catalog.StopRequested {
			t.Fatalf("stop request = %v, %v", out, err)
		}
		h.tick(time.Second)
		if s, _ := c.Capture(ctx, nc.ID); s.State != catalog.StateStopped {
			t.Errorf("the requested stop's capture is %s, want stopped at the next flush", s.State)
		}
	})
}

// A capture admitted before a crash and never flushed writes its start at seq 0 when its entries return.
func TestConverterStartAfterAnUnflushedAdmission(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		f := &fixture{t: t}
		h1, b1 := pairMsgs(t, f, 6, 11, true, 1)
		h.receive(h1, b1)
		h.tick(2 * time.Second) // admitted and batched, never flushed
		h.conv.Abandon()
		if h1.naks != 1 || b1.naks != 1 {
			t.Fatalf("Abandon nak'd %d and %d, want each once", h1.naks, b1.naks)
		}
		h.conv = h.converter(h.conv.Lease, firstConfig(h))
		h.receive(&fakeMsg{data: h1.data}, &fakeMsg{data: b1.data})
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		cp, _, err := c.LogCaptureOf(t.Context(), catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: "eqp"})
		if err != nil {
			t.Fatal(err)
		}
		recs := h.records(cp.ID)
		if len(recs) != 2 || recs[0].Seq != 0 || recs[0].Kind != tracepack.KindTransportEvent || recs[1].Kind != tracepack.KindData {
			t.Errorf("records %+v, want the start at seq 0 then the data", recs)
		}
	})
}

func firstConfig(h *harness) int64 {
	for id := range h.conv.Configs {
		return id
	}

	return 0
}

// The next dated file seen before its predecessor's tail still stops the predecessor; a capture bound to a configuration
// this replica does not execute gives the lease up rather than writing unmasked records.
func TestConverterSuccessorFirstAndUnexecutable(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		h := newHarness(t, c)
		f := &fixture{t: t}
		next := &fakeMsg{data: f.raw(map[string]any{"file": "host-20261108.log", "kind": KindUnclassified, "text": "next"})}
		old := &fakeMsg{data: f.raw(map[string]any{"kind": KindUnclassified, "text": "tail"})}
		h.receive(next, old)
		h.tick(2 * time.Second)
		oc, ok, err := c.LogCaptureOf(ctx, catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: "eqp"})
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		if s, _ := c.Capture(ctx, oc.ID); s.State != catalog.StateStopped {
			t.Errorf("the predecessor seen after its successor is %s, want stopped", s.State)
		}

		h.conv.Configs = map[int64]ingest.Config{}
		another := &fakeMsg{data: f.raw(map[string]any{"file": "host-20261108.log", "kind": KindUnclassified, "text": "more"})}
		h.receive(another)
		h.now = h.now.Add(2 * time.Second)
		if err := h.conv.Tick(ctx, h.now); !errors.Is(err, ErrGiveUp) {
			t.Errorf("a capture of an unexecuted configuration = %v, want ErrGiveUp", err)
		}
	})
}

// An entry of a stopped capture's key is stale, and so is a redelivery of it, which is acknowledged at once rather than joined.
func TestConverterStaleCaptureRedelivery(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		h := newHarness(t, c)
		f := &fixture{t: t}
		first := &fakeMsg{data: f.raw(map[string]any{"kind": KindUnclassified, "text": "a"})}
		h.receive(first)
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		cp, _, err := c.LogCaptureOf(ctx, catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: "eqp"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.StopLogCapture(ctx, cp.ID, "operator", "stop-1"); err != nil {
			t.Fatal(err)
		}
		h.tick(time.Second)
		after := &fakeMsg{data: f.raw(map[string]any{"kind": KindUnclassified, "text": "b"})}
		h.receive(after)
		h.tick(2 * time.Second)
		if after.acked() != 1 {
			t.Fatalf("an entry of a stopped capture's key acknowledged %d times, want once", after.acked())
		}
		again := &fakeMsg{data: after.data}
		h.receive(again)
		if again.acked() != 1 {
			t.Errorf("its redelivery acknowledged %d times, want once at once", again.acked())
		}
	})
}

// A batch whose records' hours go back and forth registers exact seq runs, never a range over another hour's seqs.
func TestRuns(t *testing.T) {
	t.Parallel()
	recs := []tracepack.Record{{Seq: 0, TSUTCNs: 1}, {Seq: 1, TSUTCNs: 2}, {Seq: 3, TSUTCNs: 4}, {Seq: 5, TSUTCNs: 6}, {Seq: 6, TSUTCNs: 7}}
	got := runs(recs)
	if fmt.Sprint(got) != fmt.Sprint([]catalog.Range{{SeqFrom: 0, SeqTo: 1, TSFrom: 1, TSTo: 2}, {SeqFrom: 3, SeqTo: 3, TSFrom: 4, TSTo: 4}, {SeqFrom: 5, SeqTo: 6, TSFrom: 6, TSTo: 7}}) {
		t.Errorf("runs = %v", got)
	}
}

// A batch closes before its payload would pass the size limit, and the next part of its capture and hour flushes with it.
func TestConverterBatchParts(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		h.conv.MaxBatchBytes = 60
		f := &fixture{t: t}
		for i := range 4 {
			h.receive(&fakeMsg{data: f.raw(map[string]any{"kind": KindUnclassified, "text": strings.Repeat("x", 20) + string(rune('a'+i))})})
		}
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		cp, _, err := c.LogCaptureOf(t.Context(), catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: "eqp"})
		if err != nil {
			t.Fatal(err)
		}
		packs := 0
		_ = h.store.List(t.Context(), "", func(o store.ObjectInfo) error {
			if strings.Contains(o.Key, cp.ID.String()) {
				packs++
			}

			return nil
		})
		if recs := h.records(cp.ID); len(recs) != 5 || packs < 2 {
			t.Errorf("%d records in %d packs, want 5 records over several packs", len(recs), packs)
		}
	})
}

// Once its context ends, as when the group's lease is lost, the converter acknowledges nothing more; Abandon returns what it holds.
func TestConverterAcknowledgesNothingAfterItsLease(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		f := &fixture{t: t}
		held := msgOf(t, f, map[string]any{"kind": KindUnclassified, "text": "a"})
		h.receive(held)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		unreadable := &fakeMsg{data: []byte("{nope")}
		if err := h.conv.Receive(ctx, unreadable, h.now); err == nil || unreadable.acked() != 0 {
			t.Errorf("an unreadable entry after the lease = %v, acked %d; want an error and no acknowledgement", err, unreadable.acked())
		}
		h.now = h.now.Add(12 * time.Second)
		if err := h.conv.Tick(ctx, h.now); err == nil || held.acked() != 0 {
			t.Errorf("a tick after the lease = %v, acked %d; want an error and no acknowledgement", err, held.acked())
		}
		h.conv.Abandon()
		if held.naks != 1 {
			t.Errorf("Abandon nak'd the held entry %d times, want once", held.naks)
		}
	})
}

// A group past its bound flushes its largest equipment before F, by batched bytes or by held deliveries, and leaves the rest to F.
func TestConverterGroupBound(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		groupBytes, maxHeld int
	}{
		{"bytes", 300, 0},
		{"held deliveries", 0, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				h := newHarness(t, c)
				h.conv.GroupBytes, h.conv.MaxHeld = tc.groupBytes, tc.maxHeld
				large, small := &fixture{t: t}, &fixture{t: t}
				file := "host-20261107.log"
				big := make([]*fakeMsg, 0, 3)
				for range 2 {
					big = append(big, &fakeMsg{data: large.raw(map[string]any{"kind": KindUnclassified, "text": strings.Repeat("x", 200), "file": file})})
				}
				little := &fakeMsg{subject: "tklog.0.T2", data: small.raw(map[string]any{"equipment_id": "T2", "kind": KindUnclassified, "text": "y", "file": file})}
				h.receive(append(big, little)...)
				h.tick(2 * time.Second) // released and converted, F not reached
				for _, m := range big {
					if m.acked() != 1 {
						t.Errorf("the largest equipment's entry acked %d times before F, want once", m.acked())
					}
				}
				if little.acked() != 0 {
					t.Errorf("the smaller equipment's entry acked %d times before F, want none", little.acked())
				}
				h.tick(10 * time.Second)
				if little.acked() != 1 {
					t.Errorf("the smaller equipment's entry acked %d times at F, want once", little.acked())
				}
			})
		})
	}
}

// handledMsg is a delivery whose Handle is another fake, as the stage's deliveries keep only their reply subject.
type handledMsg struct {
	*fakeMsg
	h *fakeMsg
}

func (m handledMsg) Handle() Acker { return m.h }

// A held delivery is acknowledged through its handle, never through the delivery holding its payload.
func TestConverterHoldsHandles(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		f := &fixture{t: t}
		m := handledMsg{fakeMsg: msgOf(t, f, map[string]any{"kind": KindUnclassified, "text": "a"}), h: &fakeMsg{}}
		if err := h.conv.Receive(t.Context(), m, h.now); err != nil {
			t.Fatal(err)
		}
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		if m.acked() != 0 || m.h.acked() != 1 {
			t.Errorf("the delivery acked %d times and its handle %d, want 0 and 1", m.acked(), m.h.acked())
		}
	})
}

// A phase keeps an equipment's flushes on the wall clock's times congruent to it modulo F, whenever the previous flush ran;
// without one, a flush is due F after the previous one.
func TestNextFlushKeepsThePhase(t *testing.T) {
	f := 10 * time.Second
	at := func(s float64) time.Time { return time.Unix(0, int64(s*float64(time.Second))) }
	c := &Converter{Flush: f}
	if got := c.nextFlush("A", at(1000.5)); !got.Equal(at(1010.5)) {
		t.Errorf("without a phase: %v, want F later", got)
	}
	c.Phase = func(id string) time.Duration {
		if id == "A" {
			return 3 * time.Second
		}
		return -time.Second
	}
	for _, tc := range []struct {
		id        string
		now, want float64
	}{
		{"A", 1000.5, 1003}, // its next phase time
		{"A", 1003, 1013},   // at its phase: the next one, never now
		{"A", 1007.2, 1013}, // a flush off its phase, such as an hour's change, keeps the phase
		{"B", 1000.5, 1009}, // a negative phase is taken modulo F
	} {
		if got := c.nextFlush(tc.id, at(tc.now)); !got.Equal(at(tc.want)) {
			t.Errorf("%s at %v: %v, want %v", tc.id, tc.now, got.Sub(at(1000)), at(tc.want).Sub(at(1000)))
		}
	}
}
