package producer_test

import (
	"testing"
	"time"

	"github.com/arloliu/tracekeep/producer"
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
