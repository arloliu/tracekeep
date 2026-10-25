package producer_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/internal/core"
	"github.com/arloliu/tracekeep/producer/internal/link"
	"github.com/arloliu/tracekeep/producer/producertest"
)

const bucket = "tpk-objects"

var errDown = errors.New("bus down")

// metrics records what the producer reports.
type metrics struct {
	mu     sync.Mutex
	counts map[string]float64
}

func (m *metrics) key(name string, labels map[string]string) string {
	var b strings.Builder
	b.WriteString(name)
	for _, k := range []string{"reason", "outcome", "kind", "dir"} {
		if v, ok := labels[k]; ok {
			b.WriteString("{" + k + "=" + v + "}")
		}
	}

	return b.String()
}

func (m *metrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[m.key(name, labels)] += delta
}
func (m *metrics) Set(string, map[string]string, float64)     {}
func (m *metrics) Observe(string, map[string]string, float64) {}
func (m *metrics) Delete(string, map[string]string)           {}

func (m *metrics) get(key string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.counts[key]
}

type harness struct {
	t   *testing.T
	p   *producer.Producer
	bus *producertest.Bus
	m   *metrics
	rec *core.Recorder
}

func newHarness(t *testing.T, tun producer.Tunables) *harness {
	t.Helper()
	bus := producertest.NewBus()
	m := &metrics{counts: map[string]float64{}}
	p, err := producer.New(
		producer.Config{SubjectPrefix: "tpk", ClaimBucket: bucket, Tunables: tun},
		producer.WithBus(bus), producer.WithMetrics(m),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	return &harness{t: t, p: p, bus: bus, m: m, rec: link.Recorder(p)}
}

func (h *harness) begin() *core.Capture {
	h.t.Helper()
	if err := h.p.Begin(h.t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		h.t.Fatalf("Begin: %v", err)
	}

	return h.rec.Current()
}

// observe feeds one inbound data frame at the present instant.
func (h *harness) observe(payload []byte) {
	wall, mono := core.Now()
	h.rec.Observe(&core.Observation{
		Wall: wall, Mono: mono, Socket: 1,
		Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact,
		Payload: payload,
	})
}

func (h *harness) wait(n int) []producer.Msg {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), 10*time.Second)
	defer cancel()
	msgs, err := h.bus.WaitMessages(ctx, n)
	if err != nil {
		h.t.Fatalf("%v; have %d", err, len(msgs))
	}

	return msgs
}

// check decodes a message as a consumer does.
func check(t *testing.T, m *producer.Msg) tracepack.BusRecord {
	t.Helper()
	env := tracepack.BusEnvelope{
		Layout:        m.Header[tracepack.BusHeaderLayout],
		MsgID:         m.MsgID,
		Descriptor:    m.Header[tracepack.BusHeaderDescriptor],
		PayloadRef:    m.Header[tracepack.BusHeaderPayloadRef],
		PayloadDigest: m.Header[tracepack.BusHeaderPayloadDigest],
	}
	br, err := tracepack.CheckBusMessage(env, m.Data)
	if err != nil {
		t.Fatalf("CheckBusMessage(%s): %v", m.MsgID, err)
	}

	return br
}

func frame(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}

	return b
}

func TestStartAndRecordsArePublished(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	c := h.begin()
	payload := frame(32)
	h.observe(payload)

	msgs := h.wait(2)
	start := check(t, &msgs[0])
	if msgs[0].MsgID != c.ID.String()+".0" || start.Descriptor == nil {
		t.Fatalf("first message %s, descriptor %v; want the start", msgs[0].MsgID, start.Descriptor)
	}
	if want, _ := tracepack.BusSubject("tpk", "T1", c.ID); msgs[0].Subject != want {
		t.Errorf("subject %q, want %q", msgs[0].Subject, want)
	}
	if msgs[0].ExpectStream != busconfig.DefaultTrafficStream {
		t.Errorf("expected stream %q", msgs[0].ExpectStream)
	}
	if m, err := tracepack.DecodeDescriptor(start.Descriptor); err != nil || m.ToolID != "T1" {
		t.Errorf("start descriptor: %v, %v", m, err)
	}
	if desc, _ := base64.StdEncoding.DecodeString(msgs[0].Header[tracepack.BusHeaderDescriptor]); !bytes.Equal(desc, start.Descriptor) {
		t.Errorf("descriptor header does not round-trip")
	}

	rec := check(t, &msgs[1])
	if rec.Record.Seq != 1 || rec.Record.Epoch != 1 || !bytes.Equal(rec.Record.Payload, payload) ||
		rec.Record.Kind != tracepack.KindData || rec.Record.Fidelity != tracepack.FidelityWireExact || !rec.Record.MonoPresent {
		t.Errorf("record = %+v", rec.Record)
	}
}

// The dependency barrier: while the start is not accepted, no record of the capture is published.
func TestRecordsWaitForTheStart(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	refused := make(chan struct{}, 1)
	h.bus.FailPublish(func(m *producer.Msg) error {
		if strings.HasSuffix(m.MsgID, ".0") {
			select {
			case refused <- struct{}{}:
			default:
			}

			return errDown
		}

		return nil
	})
	h.begin()
	h.observe(frame(8))
	h.observe(frame(8))
	<-refused // the start was attempted and refused
	<-refused // and retried after its backoff, with the records already queued behind it

	if n := len(h.bus.Messages()); n != 0 {
		t.Fatalf("%d messages published before the start was accepted", n)
	}
	h.bus.FailPublish(nil)
	msgs := h.wait(3)
	if !strings.HasSuffix(msgs[0].MsgID, ".0") {
		t.Errorf("first accepted message %s, want the start", msgs[0].MsgID)
	}
}

func TestClockStepPrecedesItsRecord(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.begin()
	wall, mono := core.Now()
	h.rec.Observe(&core.Observation{
		Wall: wall + 3*int64(time.Second), Mono: mono, Socket: 1,
		Kind: tracepack.KindData, Dir: tracepack.DirHostToEquipment, Payload: frame(4),
	})

	msgs := h.wait(3)
	step := check(t, &msgs[1])
	ev, err := tracepack.UnmarshalTransportEvent(step.Record.Payload)
	if err != nil || ev.Event != tracepack.EventClockStep || step.Record.Seq != 1 {
		t.Fatalf("second message seq %d event %v (%v), want the clock step at seq 1", step.Record.Seq, ev, err)
	}
	if rec := check(t, &msgs[2]); rec.Record.Seq != 2 {
		t.Errorf("record seq %d, want 2", rec.Record.Seq)
	}
}

func TestLargeRecordGoesByClaimCheck(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.bus.SetMaxPayload(tracepack.BusMinMaxPayload + 100)
	c := h.begin()
	payload := frame(1000) // above max_payload − 4096 = 144
	h.observe(payload)

	msgs := h.wait(2)
	br := check(t, &msgs[1])
	if !br.ClaimCheck || br.Bucket != bucket {
		t.Fatalf("record is not claim-check: %+v", br)
	}
	data, info, ok := h.bus.Object(bucket, c.ID.String()+".1")
	if !ok || info.Metadata[producer.ObjectMetaRecordHeader] == "" {
		t.Fatalf("claim object missing or without its record header: %v %+v", ok, info)
	}
	if err := tracepack.CheckBusPayload(&br, data); err != nil || !bytes.Equal(br.Record.Payload, payload) {
		t.Errorf("CheckBusPayload: %v", err)
	}
}

func TestClaimConflictDropsTheRecord(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.bus.SetMaxPayload(tracepack.BusMinMaxPayload + 100)
	c := h.begin()
	h.bus.PutRawObject(bucket, []byte("another record"), producer.ObjectInfo{
		Name: c.ID.String() + ".1", Digest: tracepack.BusPayloadDigest([]byte("another record")),
	})
	h.observe(frame(1000)) // seq 1 conflicts
	h.observe(frame(1000)) // seq 2 does not

	msgs := h.wait(2)
	if rec := check(t, &msgs[1]); rec.Record.Seq != 2 {
		t.Errorf("published seq %d, want 2 after the conflicting seq 1 was dropped", rec.Record.Seq)
	}
	_ = h.p.Close()
	if got := h.m.get("records_dropped_total{reason=claim_failed}"); got != 1 {
		t.Errorf("dropped{claim_failed} = %v, want 1", got)
	}
	if got := h.m.get("claim_puts_total{outcome=conflict}"); got != 1 {
		t.Errorf("claim_puts{conflict} = %v, want 1", got)
	}
}

// An uncertain put left the record's metadata without all its bytes:
// the retry reads the object back, finds it short, deletes it and puts it again.
func TestUncertainClaimIsReadBackAndReplaced(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.bus.SetMaxPayload(tracepack.BusMinMaxPayload + 100)
	c := h.begin()
	payload := frame(1000)
	var once sync.Once
	h.bus.FailPutsUncertain(func(string) bool {
		first := false
		once.Do(func() { first = true })

		return first
	})
	h.observe(payload)

	msgs := h.wait(2)
	data, _, _ := h.bus.Object(bucket, c.ID.String()+".1")
	if !bytes.Equal(data, payload) {
		t.Fatalf("object holds %d bytes after the retry, want the whole %d", len(data), len(payload))
	}
	br := check(t, &msgs[1])
	if err := tracepack.CheckBusPayload(&br, data); err != nil {
		t.Errorf("CheckBusPayload: %v", err)
	}
	_ = h.p.Close()
	if got := h.m.get("claim_puts_total{outcome=put}"); got != 1 {
		t.Errorf("claim_puts{put} = %v, want 1 successful put after the uncertain one", got)
	}
}

func TestPublishPastTheHorizonDrops(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{RetryHorizon: 200 * time.Millisecond})
	c := h.begin()
	h.bus.FailPublish(func(m *producer.Msg) error {
		if m.MsgID == c.ID.String()+".1" {
			return errDown
		}

		return nil
	})
	h.observe(frame(4)) // seq 1 never accepted
	h.observe(frame(4))

	msgs := h.wait(2)
	if rec := check(t, &msgs[1]); rec.Record.Seq != 2 {
		t.Errorf("seq %d published, want 2", rec.Record.Seq)
	}
	_ = h.p.End(t.Context()) // waits for the drain, by which seq 1 is past its horizon
	_ = h.p.Close()
	if got := h.m.get("records_dropped_total{reason=publish_failed}"); got != 1 {
		t.Errorf("dropped{publish_failed} = %v, want 1", got)
	}
}

func TestEndDrainsAndPublishesTheStop(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	c := h.begin()
	h.observe(frame(4))

	rep := h.p.End(t.Context())
	if !rep.StopQueued || !rep.Drained || rep.Dropped != 0 || rep.CaptureID != c.ID {
		t.Fatalf("End = %+v", rep)
	}
	msgs := h.bus.Messages()
	if len(msgs) != 3 {
		t.Fatalf("%d messages after End, want start, record and stop", len(msgs))
	}
	// The barrier orders reserved records only: the stop may be accepted before an ordinary record below it.
	var stop *tracepack.BusRecord
	for i := range msgs {
		if br := check(t, &msgs[i]); br.Record.Seq == 2 {
			stop = &br
		}
	}
	if stop == nil {
		t.Fatalf("no message at seq 2")
	}
	ev, err := tracepack.UnmarshalTransportEvent(stop.Record.Payload)
	if err != nil || ev.BoundaryKind == nil || *ev.BoundaryKind != tracepack.BoundaryKindStop {
		t.Errorf("seq 2 event %+v (%v), want the stop", ev, err)
	}
}

func TestEndIsBoundedWhenTheBusIsDown(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.bus.FailPublish(func(*producer.Msg) error { return errDown })
	h.begin()
	h.observe(frame(4))

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	rep := h.p.End(ctx)
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("End took %v with a 300ms deadline", took)
	}
	if !rep.StopQueued {
		t.Errorf("End = %+v, want the stop queued", rep)
	}
}

func TestFullLaneWhileTheBusIsDownCountsBusDown(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{OrdinaryLaneRecords: 2})
	refused := make(chan struct{}, 1)
	h.bus.FailPublish(func(*producer.Msg) error {
		select {
		case refused <- struct{}{}:
		default:
		}

		return errDown
	})
	h.begin()
	<-refused // the publisher found the bus down
	<-refused // and has recorded it, since it retried after the first failure
	for range 5 {
		h.observe(frame(4))
	}
	_ = h.p.Close()
	if got := h.m.get("records_dropped_total{reason=bus_down}"); got != 3 {
		t.Errorf("dropped{bus_down} = %v, want 3", got)
	}
}
