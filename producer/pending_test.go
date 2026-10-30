package producer_test

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

func TestPendingTokenReportsAndCounts(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})

	tok := h.p.Pending().Register(1, 100, "fdc-eqp")
	tok.Sent()
	tok.Sent()
	tok.Awaiting(time.Minute)
	tok.Replied()
	tok.Cancelled()
	reused := h.p.Pending().Register(1, 100, "fdc-eqp")
	reused.SubmissionFailed()
	reused.TimedOut(time.Minute)
	h.p.Pending().Register(1, 101, "fdc-eqp").TimedOut(time.Minute) // never sent, so never bound

	_ = h.p.Close()
	for key, want := range map[string]float64{
		"pending_reports_ignored_total{reason=duplicate_progress}": 1,
		"pending_reports_ignored_total{reason=sealed}":             1,
		"pending_reports_ignored_total{reason=removed}":            1,
		"pending_timeouts_dropped_total{reason=unbound}":           1,
		"ambiguous_total":  2,
		"pending_requests": 2, // completed entries are kept until their expiry
	} {
		if got := h.m.get(key); got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
}

// A thinned record and a header disagreement are counted, and thinning is reported enabled under a sampling rule.
func TestThinningMetrics(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	d := producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", Thinning: &producer.SamplingRule{KeepEveryN: 1000}}
	if err := h.p.Begin(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	pt := h.rec.Pending
	pt.SetSocket(1)
	for sb := range uint32(3) {
		h.p.Pending().RegisterRequest(producer.Request{
			SessionID: 1, SystemBytes: sb, Stream: 1, Function: 3, Channel: "fdc", Origin: producer.OriginIntervalPoll,
		})
	}
	pt.Bind(1, 1, 1, 3, 1)
	pt.Bind(1, 2, 1, 1, 1) // another message went out under the key
	for sb := range uint32(3) {
		wall, mono := core.Now()
		h.rec.Observe(&core.Observation{
			Wall: wall, Mono: mono, Socket: 1, Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact,
			Payload: []byte{0, 0, 0, 13, 0, 1, 1, 4, 0, 0, 0, 0, 0, byte(sb), 0xa5, 0x01, 0x07}, Pending: pt.Lookup(1, sb),
		})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := h.bus.WaitMessages(ctx, 4); err != nil {
		t.Fatal(err)
	}

	_ = h.p.Close()
	for key, want := range map[string]float64{"thinned_total": 1, "thinning_header_mismatches_total": 1, "thinning_enabled": 1} {
		if got := h.m.get(key); got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
}
