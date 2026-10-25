package producer_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
func (m *metrics) Set(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[m.key(name, labels)] = value
}
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

	// The candidate is read back and deleted before the put that completes, and only that put is referenced.
	name := c.ID.String() + ".1"
	id, _ := tracepack.BusMsgID(c.ID, 1)
	var got []string
	for _, op := range h.bus.Ops() {
		if strings.HasSuffix(op, " "+name) || strings.HasSuffix(op, " "+id) {
			got = append(got, strings.Fields(op)[0])
		}
	}
	if want := []string{"info", "put", "info", "get", "delete", "put", "publish-async"}; !slices.Equal(got, want) {
		t.Errorf("operations on seq 1 = %v, want %v", got, want)
	}
}

// The start goes by the synchronous publish, the records above it by the asynchronous one.
func TestStartIsPublishedSynchronously(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	c := h.begin()
	h.observe(frame(4))
	h.wait(2)

	start, _ := tracepack.BusMsgID(c.ID, 0)
	record, _ := tracepack.BusMsgID(c.ID, 1)
	ops := h.bus.Ops()
	if !slices.Contains(ops, "publish "+start) || slices.Contains(ops, "publish-async "+start) {
		t.Errorf("the start's publishes = %v, want the synchronous one only", ops)
	}
	if !slices.Contains(ops, "publish-async "+record) {
		t.Errorf("the record's publishes = %v, want an asynchronous one", ops)
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

// An End whose drain outlives its deadline must not touch the next capture once Begin opens it,
// and ends its own capture for good.
func TestLateDrainSparesTheSuccessor(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	release := make(chan struct{})
	blocked := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	h.bus.FailPublish(func(m *producer.Msg) error {
		first := false
		once.Do(func() { first = true })
		if first {
			close(blocked)
			<-release // the publisher is stuck in the first start's publish past End's deadline

			return errDown
		}

		return nil
	})
	first := h.begin()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("the first start was never attempted")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if rep := h.p.End(ctx); rep.Drained {
		t.Fatalf("End drained while the publisher was blocked: %+v", rep)
	}
	second := h.begin()
	close(release)

	msgs := h.wait(1)
	if msgs[0].MsgID != second.ID.String()+".0" {
		t.Errorf("first accepted message %s, want the successor's start", msgs[0].MsgID)
	}
	_ = h.p.End(t.Context())
	for _, m := range h.bus.Messages() {
		if strings.HasPrefix(m.MsgID, first.ID.String()) {
			t.Errorf("published %s of the drained capture after its drain ended", m.MsgID)
		}
	}
}

// A second End does not wait behind the first one's drain.
func TestOverlappingEndsKeepTheirDeadlines(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	stopTried := make(chan struct{})
	var once sync.Once
	h.bus.FailPublish(func(m *producer.Msg) error {
		switch {
		case strings.HasSuffix(m.MsgID, ".0"):
			return nil // the start is accepted, so the stop is attempted next
		case strings.HasSuffix(m.MsgID, ".1"): // the first End's stop: that End has rotated and is draining
			once.Do(func() { close(stopTried) })
		default:
		}

		return errDown // the stop never drains
	})
	h.begin()

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		h.p.End(ctx)
	}()
	select {
	case <-stopTried:
	case <-time.After(10 * time.Second):
		t.Fatal("the first End's stop was never attempted")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	h.p.End(ctx)
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("the second End took %v with a 100ms deadline", took)
	}
	<-firstDone
}

// A publish still in flight when a drain ends is counted when it fails, never retried.
func TestFlightAfterDrainIsNotRetried(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	c := h.begin()
	h.wait(1)
	h.bus.HoldAsync(true)
	h.observe(frame(4)) // seq 1 goes out asynchronously and stays unresolved
	id := c.ID.String() + ".1"
	wctx, wcancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer wcancel()
	if err := h.bus.WaitAsync(wctx, id); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	h.p.End(ctx)
	h.bus.HoldAsync(false)
	h.bus.ReleaseAsync(errDown)
	_ = h.p.Close() // the publisher has seen the failure by the time it stops

	if got := h.bus.AsyncCalls(id); got != 1 {
		t.Errorf("%d publishes of %s, want 1: a failure after the drain is final", got, id)
	}
	if got := h.m.get("records_dropped_total{reason=publish_failed}"); got != 1 {
		t.Errorf("dropped{publish_failed} = %v, want 1", got)
	}
}

// Past the retry horizon a record is dropped without another attempt.
func TestNoAttemptPastTheHorizon(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{RetryHorizon: 50 * time.Millisecond}) // below the first 100ms backoff
	c := h.begin()
	var attempts atomic.Int32
	h.bus.FailPublish(func(m *producer.Msg) error {
		if m.MsgID == c.ID.String()+".1" {
			attempts.Add(1)
			return errDown
		}

		return nil
	})
	h.observe(frame(4))
	h.wait(1)

	rep := h.p.End(t.Context()) // drains: seq 1 is dropped once its retry falls past H
	if !rep.Drained {
		t.Fatalf("End = %+v", rep)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("%d attempts at seq 1, want 1: its retry would fall past the horizon", got)
	}
}

// No attempt at a stopped capture starts after End's deadline, even when a blocking publish crossed it inside a pass.
func TestNoAttemptAfterEndDeadline(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	c := h.begin()
	h.wait(1) // the start
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	stopTried := make(chan struct{})
	var once sync.Once
	h.bus.FailPublish(func(m *producer.Msg) error {
		switch m.MsgID {
		case c.ID.String() + ".1":
			return errDown // the record fails and waits for its retry
		case c.ID.String() + ".2":
			once.Do(func() { close(stopTried) })
			<-release // the stop's publish blocks past End's deadline

			return errDown
		default:
			return nil
		}
	})
	h.observe(frame(4))
	wctx, wcancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer wcancel()
	if err := h.bus.WaitAsync(wctx, c.ID.String()+".1"); err != nil { // its first attempt failed; the retry is 100ms out
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	h.p.End(ctx)
	select {
	case <-stopTried:
	case <-time.After(10 * time.Second):
		t.Fatal("the stop was never attempted")
	}
	close(release) // the pass resumes after the deadline, with the record's retry due
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the capture was not terminated")
	}
	if got := h.bus.AsyncCalls(c.ID.String() + ".1"); got != 1 {
		t.Errorf("%d attempts at seq 1, want 1: none may start after End's deadline", got)
	}
}

// End stops every capture a rotation closed since the previous End, not only the current one.
func TestEndCoversRotatedCaptures(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	a := h.begin()
	h.wait(1)
	h.bus.FailPublish(func(m *producer.Msg) error {
		if m.MsgID == a.ID.String()+".1" {
			return errDown // A's stop is never accepted
		}

		return nil
	})
	sw := "v2"
	if err := h.p.Rotate(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", HostSoftware: &sw}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	h.p.End(ctx)
	select {
	case <-a.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the rotated capture was not stopped by End")
	}
	if got := a.Dropped(); got != 1 {
		t.Errorf("rotated capture dropped %d records at End, want its stop", got)
	}
}

// Close counts a publish still unresolved and returns its lane budget.
func TestCloseSettlesUnresolvedPublishes(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	c := h.begin()
	h.wait(1)
	h.bus.HoldAsync(true)
	h.observe(frame(4))
	wctx, wcancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer wcancel()
	if err := h.bus.WaitAsync(wctx, c.ID.String()+".1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	h.p.End(ctx)
	_ = h.p.Close() // the publish is never resolved

	if got := h.m.get("records_dropped_total{reason=publish_failed}"); got < 1 {
		t.Errorf("dropped{publish_failed} = %v, want the unresolved publish counted", got)
	}
	if n, b := h.rec.OrdinaryDepth(); n != 0 || b != 0 {
		t.Errorf("ordinary lane budget %d records, %d bytes after Close, want 0", n, b)
	}
}

// End's Report counts every loss of the captures it stopped, including one during the drain.
func TestReportCountsLossesDuringTheDrain(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{RetryHorizon: 50 * time.Millisecond})
	c := h.begin()
	h.wait(1)
	h.bus.FailPublish(func(m *producer.Msg) error {
		if m.MsgID == c.ID.String()+".1" {
			return errDown // the record is lost at its horizon, while End drains
		}

		return nil
	})
	h.observe(frame(4))

	rep := h.p.End(t.Context())
	if !rep.Drained || rep.Dropped != 1 {
		t.Errorf("End = %+v, want drained with the one record lost", rep)
	}
}

// warnLog signals every Warn line on warned.
type warnLog struct{ warned chan string }

func (l warnLog) Info(string, ...any)  {}
func (l warnLog) Error(string, ...any) {}
func (l warnLog) Warn(msg string, _ ...any) {
	select {
	case l.warned <- msg:
	default:
	}
}

// A bus not yet connected knows no max_payload: the start waits for it as for a bus that is down,
// and the capture neither faults nor publishes until the bus reports one.
func TestStartWaitsForAKnownMaxPayload(t *testing.T) {
	t.Parallel()
	bus := producertest.NewBus()
	bus.SetMaxPayload(0)
	log := warnLog{warned: make(chan string, 1)}
	p, err := producer.New(
		producer.Config{SubjectPrefix: "tpk", ClaimBucket: bucket},
		producer.WithBus(bus), producer.WithLogger(log),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	h := &harness{t: t, p: p, bus: bus, rec: link.Recorder(p)}
	c := h.begin()
	h.observe(frame(8))
	<-log.warned // the bus counts as unreachable

	if n := len(bus.Messages()); n != 0 || c.Faulted() {
		t.Fatalf("%d messages, faulted %v, before max_payload was known", n, c.Faulted())
	}
	bus.SetMaxPayload(producertest.DefaultMaxPayload)
	if msgs := h.wait(2); !strings.HasSuffix(msgs[0].MsgID, ".0") {
		t.Errorf("first message %s, want the start", msgs[0].MsgID)
	}
}

// An in-flight asynchronous publish that fails while its capture is live, as a reconnect or τ fails it,
// is published again under the same message id, and End then drains.
func TestFailedFlightIsRetriedWithItsMessageID(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	c := h.begin()
	h.wait(1)
	h.bus.HoldAsync(true)
	h.observe(frame(4)) // seq 1 goes out asynchronously and stays unresolved
	id := c.ID.String() + ".1"
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := h.bus.WaitAsync(ctx, id); err != nil {
		t.Fatal(err)
	}

	h.bus.HoldAsync(false)
	h.bus.ReleaseAsync(errDown) // the in-flight publish fails; its retry is not held
	msgs := h.wait(2)
	if msgs[1].MsgID != id {
		t.Fatalf("second message %s, want %s", msgs[1].MsgID, id)
	}
	if got := h.bus.AsyncCalls(id); got != 2 {
		t.Errorf("%d publishes of %s, want 2: the failed one and its retry", got, id)
	}
	if rep := h.p.End(ctx); !rep.Drained || rep.Dropped != 0 {
		t.Errorf("End = %+v, want drained with nothing dropped", rep)
	}
}
