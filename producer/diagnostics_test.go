package producer_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
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

// failureLines waits for the publisher's two lines about a reserved record's failed attempt and returns them;
// every other line it reads must hold no sentinel either.
func failureLines(t *testing.T, log *producer.LineLog) []string {
	t.Helper()
	var got []string
	for len(got) < 2 {
		select {
		case l := <-log.Seen:
			if strings.Contains(l, "not accepted") || strings.Contains(l, "bus unreachable") {
				got = append(got, l)
			} else if strings.Contains(l, "sentinel-") {
				t.Errorf("line %q holds a sentinel", l)
			}
		case <-t.Context().Done():
			t.Fatalf("lines %q, want the retry's and the bus's", got)
		}
	}

	return got
}

// A publish failing with a credential in a wrapped error is logged by the fields the SDK builds, never the error's text.
func TestPublishFailureLogsFieldsNotText(t *testing.T) {
	t.Parallel()
	log := producer.NewLineLog()
	h := newHarness(t, producer.Tunables{}, producer.WithLogger(log))
	h.bus.FailPublish(func(*producer.Msg) error { return producer.SentinelError() })
	h.begin()
	for _, l := range failureLines(t, log) {
		producer.CheckFieldLine(t, l)
	}
}

// A reserved record's claim read and put failing with the object's name and a backend's text are logged by their fields.
func TestClaimFailureLogsFieldsNotText(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"info", "put"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			log := producer.NewLineLog()
			h := newHarness(t, producer.Tunables{}, producer.WithLogger(log))
			h.bus.SetMaxPayload(tpbus.MinMaxPayload) // the start's body no longer fits inline: it goes by claim-check
			h.bus.FailObjects(func(o, name string) error {
				if o == op {
					return fmt.Errorf("%s %s: %w", o, name, producer.SentinelError())
				}

				return nil
			})
			c := h.begin()
			name, err := tpbus.MsgID(c.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, l := range failureLines(t, log) {
				producer.CheckFieldLine(t, l)
				if strings.Contains(l, name) {
					t.Errorf("line %q names the claim object", l)
				}
			}
		})
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
