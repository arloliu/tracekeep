package producer_test

import (
	"strings"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
)

// A producer started and closed with no traffic reports every counter and every label value it can emit, at zero,
// so a first event after a start is seen by its increase (producer.md §4.3 *Diagnostics*).
func TestEveryCounterIsReportedFromZero(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	if err := h.p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	labelled := map[string][]string{
		"records_dropped_total{reason=":          {"lane_full", "bus_down", "publish_failed", "claim_failed", "capture_fault", "abandoned", "after_stop"},
		"capture_rotations_total{reason=":        {"start", "hotreload", "fault", "stop", "seq_limit"},
		"claim_puts_total{outcome=":              {"put", "reused", "conflict", "error"},
		"pending_reports_ignored_total{reason=":  {"duplicate_progress", "duplicate_awaiting", "sealed", "removed", "expired"},
		"pending_timeouts_dropped_total{reason=": {"unbound", "ambiguous", "unattributed"},
	}
	var want []string
	for prefix, values := range labelled {
		for _, v := range values {
			want = append(want, prefix+v+"}")
		}
	}
	observed := []struct {
		kind tracepack.Kind
		dir  tracepack.Dir
	}{
		{tracepack.KindData, tracepack.DirEquipmentToHost},
		{tracepack.KindData, tracepack.DirHostToEquipment},
		{tracepack.KindControl, tracepack.DirEquipmentToHost},
		{tracepack.KindControl, tracepack.DirHostToEquipment},
		{tracepack.KindTransportEvent, tracepack.DirLocal},
	}
	for _, o := range observed {
		want = append(want, "records_observed_total{kind="+o.kind.String()+"}{dir="+o.dir.String()+"}")
	}
	want = append(want,
		"records_published_total", "dropped_bytes_total", "clock_steps_total", "observer_panics_total",
		"log_lines_dropped_total", "ambiguous_total", "thinned_total", "thinning_header_mismatches_total",
	)
	for _, k := range want {
		if !h.m.has(k) {
			t.Errorf("%s never reported", k)
		} else if got := h.m.get(k); got != 0 {
			t.Errorf("%s = %v, want 0", k, got)
		}
	}

	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	n := 0
	for k := range h.m.counts {
		if strings.HasPrefix(k, "records_observed_total") {
			n++
		}
	}
	if n != len(observed) {
		t.Errorf("%d records_observed_total series, want only the %d the SDK emits", n, len(observed))
	}
}

// A panic recovered at a callback's entry is counted, and the publisher logs one error line for it,
// never the panic's value.
func TestObserverPanicIsCountedAndLogged(t *testing.T) {
	t.Parallel()
	log := producer.NewLineLog()
	h := newHarness(t, producer.Tunables{}, producer.WithLogger(log))
	const value = "panic-value-sentinel"
	func() {
		defer h.rec.Recover()
		panic(value)
	}()
	if err := h.p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := h.m.get("observer_panics_total"); got != 1 {
		t.Errorf("observer_panics_total = %v, want 1", got)
	}
	var panicLines []string
	for _, l := range log.Lines() {
		if strings.Contains(l, value) {
			t.Errorf("a line holds the panic's value: %q", l)
		}
		if strings.Contains(l, "panic") {
			panicLines = append(panicLines, l)
		}
	}
	if len(panicLines) != 1 || !strings.HasPrefix(panicLines[0], "ERROR ") {
		t.Errorf("panic lines %q, want one error line", panicLines)
	}
}
