package secsobs

import (
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/producertest"
)

func dataFrame(sb byte, stream, function byte, text ...byte) []byte {
	return append([]byte{0, 0, 0, byte(10 + len(text)), 0, 1, stream, function, 0, 0, 0, 0, 0, sb}, text...)
}

// thinningHarness opens a capture that thins every poll but each 1000th, and records a frame on socket 3 first,
// so the polls that register after it bind without a reconnect.
func thinningHarness(t *testing.T) *harness {
	t.Helper()
	bus := producertest.NewBus()
	p, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects"}, producer.WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	d := producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", Thinning: &producer.SamplingRule{KeepEveryN: 1000, KeepInterval: time.Hour}}
	if err := p.Begin(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, p: p, obs: New(p), bus: bus}
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, Generation: 1, At: time.Now(), Frame: frame(5)})

	return h
}

// An inbound primary whose System Bytes collide with a thin-selected poll's is no reply and is kept in full,
// while the poll's own reply is thinned.
func TestInboundPrimaryUnderAPollsKeyIsKept(t *testing.T) {
	t.Parallel()
	h := thinningHarness(t)
	for _, sb := range []uint32{1, 2} {
		h.p.Pending().RegisterRequest(producer.Request{
			SessionID: 1, SystemBytes: sb, Stream: 1, Function: 3, Channel: "fdc", Origin: producer.OriginIntervalPoll,
		})
	}
	wire := func(dir hsms.WireDirection, f []byte) {
		h.obs.wire(hsms.WireEvent{Direction: dir, Socket: 3, Generation: 1, At: time.Now(), Frame: f})
	}
	wire(hsms.WireOutbound, dataFrame(2, 0x81, 3, 0xa5, 0x01, 0x07))
	primary := dataFrame(2, 0x81, 3, 0xa5, 0x01, 0x09) // the equipment's own S1F3 under the poll's key
	wire(hsms.WireInbound, primary)
	wire(hsms.WireInbound, dataFrame(2, 0x01, 4, 0x01, 0x01, 0xa5, 0x01, 0x07))

	recs := h.records(4)[1:]
	if r := recs[1]; !slices.Equal(r.Payload, primary) || r.Quality&tracepack.QualityUnobserved != 0 {
		t.Errorf("inbound primary = % x, quality %v; want it in full", r.Payload, r.Quality)
	}
	if recs[2].Quality&tracepack.QualityUnobserved == 0 {
		t.Error("the poll's reply was kept: the control case does not thin")
	}
}

// A marked poll the rule gives up is published with its reply as unobserved records:
// header and length kept, text zero-filled; the poll the rule keeps is published in full (producer.md §4.6).
func TestThinnedPollIsPublishedUnobserved(t *testing.T) {
	t.Parallel()
	bus := producertest.NewBus()
	p, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects"}, producer.WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	d := producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", Thinning: &producer.SamplingRule{KeepEveryN: 2, KeepInterval: time.Hour}}
	if err := p.Begin(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, p: p, obs: New(p), bus: bus}
	// The socket is the connection's before the polls register, so their sends bind without a reconnect.
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, Generation: 1, At: time.Now(), Frame: frame(5)})
	sv := []byte{0x01, 0x01, 0xa5, 0x01, 0x07}
	frames := make([][]byte, 0, 4)
	for _, sb := range []byte{1, 2} {
		p.Pending().RegisterRequest(producer.Request{
			SessionID: 1, SystemBytes: uint32(sb), Stream: 1, Function: 3, Channel: "fdc", Origin: producer.OriginIntervalPoll,
		})
		poll, reply := dataFrame(sb, 0x81, 3, 0xa5, 0x01, 0x07), dataFrame(sb, 0x01, 4, sv...)
		h.obs.wire(hsms.WireEvent{Direction: hsms.WireOutbound, Socket: 3, Generation: 1, At: time.Now(), Frame: poll})
		h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, Generation: 1, At: time.Now(), Frame: reply})
		frames = append(frames, poll, reply)
	}

	recs := h.records(5)[1:]
	for i, r := range recs {
		thinned := i >= 2
		want := frames[i]
		if thinned {
			want = append(slices.Clone(want[:14]), make([]byte, len(want)-14)...)
		}
		if !slices.Equal(r.Payload, want) || (r.Quality&tracepack.QualityUnobserved != 0) != thinned {
			t.Errorf("record %d = % x, quality %v; want % x, unobserved %v", i, r.Payload, r.Quality, want, thinned)
		}
	}
}
