package ingest

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
)

// The test traffic: bus messages built as a producer publishes them (the tracepack storage specification §4).

const bucket = "claims"

// claims is an in-memory claim bucket.
type claims struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func newClaims() *claims { return &claims{objs: map[string][]byte{}} }

func (c *claims) Get(_ context.Context, name string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.objs[name]
	if !ok {
		return nil, ErrClaimNotFound
	}

	return b, nil
}

func (c *claims) put(name string, b []byte) {
	c.mu.Lock()
	c.objs[name] = b
	c.mu.Unlock()
}

// descriptorOf is a start descriptor this service accepts.
func descriptorOf(t *testing.T, name string) []byte {
	t.Helper()
	origin, mono, tolerance := int64(1000), int64(0), uint64(time.Second)
	m := &tracepack.PackMeta{
		ToolID: "T1", Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
		Vantage: tracepack.VantageHost, Recorder: "test", TimeSource: tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs: &origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
		LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		RecorderInstanceID: tracepack.UUID{9}, RecorderInstanceName: &name,
	}
	b, err := tracepack.EncodeDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}

	return b
}

// startRecord is a capture's start: a capture-boundary transport event of kind start.
func startRecord(t *testing.T, ts int64) tracepack.Record {
	t.Helper()
	ev := tracepack.TransportEvent{Event: tracepack.EventCaptureBoundary, BoundaryKind: new(tracepack.BoundaryKindStart)}
	p, err := ev.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	return tracepack.Record{Seq: 0, TSUTCNs: ts, Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable, Payload: p}
}

// dataRecord is a data record of seq holding an HSMS frame.
func dataRecord(seq uint64, ts int64) tracepack.Record {
	frame := []byte{0, 0, 0, 10, 0, 1, 1, 1, 0, 0, 0, 0, 0, byte(seq)}

	return tracepack.Record{Seq: seq, TSUTCNs: ts, Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact, Payload: frame}
}

// inline is the message of r, carrying descriptor when it is a start.
func inline(t *testing.T, capture tracepack.UUID, r tracepack.Record, descriptor []byte) Message {
	t.Helper()
	br := tpbus.NewRecord(&r)
	br.CaptureID = capture
	body, err := tpbus.EncodeBody(&br)
	if err != nil {
		t.Fatal(err)
	}

	return Message{Env: envelope(t, capture, r.Seq, descriptor), Body: body, Incarnation: 1, StreamSeq: int64(r.Seq) + 1}
}

// claimCheck is the message of r with its payload put in the claim bucket c.
func claimCheck(t *testing.T, c *claims, capture tracepack.UUID, r tracepack.Record, descriptor []byte) Message {
	t.Helper()
	payload := r.Payload
	br := tpbus.NewRecord(&r)
	br.CaptureID, br.ClaimCheck, br.Record.Payload = capture, true, nil
	body, err := tpbus.EncodeBody(&br)
	if err != nil {
		t.Fatal(err)
	}
	env := envelope(t, capture, r.Seq, descriptor)
	env.PayloadRef, env.PayloadDigest = bucket+"/"+env.MsgID, tpbus.PayloadDigest(payload)
	c.put(env.MsgID, payload)

	return Message{Env: env, Body: body, Incarnation: 1, StreamSeq: int64(r.Seq) + 1}
}

func envelope(t *testing.T, capture tracepack.UUID, seq uint64, descriptor []byte) tpbus.Envelope {
	t.Helper()
	id, err := tpbus.MsgID(capture, seq)
	if err != nil {
		t.Fatal(err)
	}
	env := tpbus.Envelope{Layout: "1", MsgID: id}
	if descriptor != nil {
		env.Descriptor = base64.StdEncoding.EncodeToString(descriptor)
	}

	return env
}
