package ingest

import (
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/producer"
)

// A start names its series: a device and a source each valid UTF-8 of 1 to 255 bytes, not whitespace only, kept byte for byte,
// and the recorder_instance_id derived from them with equipment_id; any other descriptor is not accepted (architecture.md §3.4).
func TestAcceptedDescriptorNamesItsSeries(t *testing.T) {
	t.Parallel()
	encode := func(device, source *string, instance *tracepack.UUID) []byte {
		t.Helper()
		origin, mono, tolerance := int64(1000), int64(0), uint64(time.Second)
		m := &tracepack.PackMeta{
			EquipmentID: "T1", Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
			Vantage: tracepack.VantageHost, Recorder: "test", TimeSource: tracepack.TimeSourceCaptureClock,
			CaptureOriginUTCNs: &origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
			LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
			Device: device, RecorderInstanceName: source,
		}
		switch {
		case instance != nil:
			m.RecorderInstanceID = *instance
		case device != nil && source != nil:
			m.RecorderInstanceID = producer.RecorderInstanceID("T1", *device, *source)
		default: // no series to derive an id from: the zero id
		}
		b, err := tracepack.EncodeDescriptor(m)
		if err != nil {
			t.Fatal(err)
		}

		return b
	}
	str := func(s string) *string { return &s }
	long, multibyte := strings.Repeat("d", 256), strings.Repeat("é", 127)+"d"
	other := producer.RecorderInstanceID("T1", "eqp", "another-source")
	cases := []struct {
		name           string
		device, source *string
		instance       *tracepack.UUID
		ok             bool
	}{
		{"valid", str("eqp"), str("gw"), nil, true},
		{"255-byte multibyte", str(multibyte), str(multibyte), nil, true},
		{"surrounding spaces kept", str(" eqp "), str("gw "), nil, true},
		{"no device", nil, str("gw"), nil, false},
		{"no source", str("eqp"), nil, nil, false},
		{"empty device", str(""), str("gw"), nil, false},
		{"empty source", str("eqp"), str(""), nil, false},
		{"whitespace device", str(" \n"), str("gw"), nil, false},
		{"whitespace source", str("eqp"), str(" \t"), nil, false},
		{"256-byte device", str(long), str("gw"), nil, false},
		{"256-byte source", str("eqp"), str(long), nil, false},
		{"another series' id", str("eqp"), str("gw"), &other, false},
	}
	for _, c := range cases {
		if got := AcceptedDescriptor(encode(c.device, c.source, c.instance)); got != c.ok {
			t.Errorf("%s: accepted %v, want %v", c.name, got, c.ok)
		}
	}
}
