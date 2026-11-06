package producer

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

func TestRecorderInstanceID(t *testing.T) {
	t.Parallel()

	// Expected values computed independently: SHA-1 over the namespace and the length-prefixed triple, RFC 9562 §5.5.
	// Each component changes the id, and the length prefixes keep "ab"+"c"+"d", "a"+"bc"+"d" and "a"+"b"+"cd" apart.
	cases := []struct {
		equipment, device, source, want string
	}{
		{"T1", "eqp", "gateway-a", "b8fe2295-6a3a-541c-902e-813a16cb2814"},
		{"T1", "eqp", "gateway-b", "5fd76a73-7c3f-5869-9e25-efbdcdc1f67a"},
		{"T1", "tlp", "gateway-a", "6ebc282f-a50f-5515-9ff8-5ab87ea7ce23"},
		{"T2", "eqp", "gateway-a", "1631afc6-f2a3-5317-8d85-00d98277af5e"},
		{"ab", "c", "d", "328fc04f-2688-5257-a904-40417ff4bd6a"},
		{"a", "bc", "d", "3ea16f2a-0be7-5406-9bc3-3457178f568d"},
		{"a", "b", "cd", "d35c08bf-d4de-5b45-bc99-e97e006cd8c3"},
	}
	for _, c := range cases {
		d := CaptureDescriptor{EquipmentID: c.equipment, Device: c.device}
		if got := d.RecorderInstanceID(c.source).String(); got != c.want {
			t.Errorf("RecorderInstanceID(%q, %q, %q) = %s, want %s", c.equipment, c.device, c.source, got, c.want)
		}
		if got := RecorderInstanceID(c.equipment, c.device, c.source).String(); got != c.want {
			t.Errorf("RecorderInstanceID func(%q, %q, %q) = %s, want %s", c.equipment, c.device, c.source, got, c.want)
		}
	}
}

func TestCaptureDescriptorValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		d    CaptureDescriptor
		ok   bool
	}{
		{"valid", CaptureDescriptor{EquipmentID: "T1", Device: "eqp"}, true},
		{"no equipment", CaptureDescriptor{Device: "eqp"}, false},
		{"equipment a key cannot hold", CaptureDescriptor{EquipmentID: "..", Device: "eqp"}, false},
		{"no device", CaptureDescriptor{EquipmentID: "T1"}, false},
		{"whitespace device", CaptureDescriptor{EquipmentID: "T1", Device: " \t"}, false},
		{"invalid UTF-8 device", CaptureDescriptor{EquipmentID: "T1", Device: "e\xffq"}, false},
		{"256-byte device", CaptureDescriptor{EquipmentID: "T1", Device: strings.Repeat("d", 256)}, false},
		{"255-byte multibyte device", CaptureDescriptor{EquipmentID: "T1", Device: strings.Repeat("é", 127) + "d"}, true},
		{"device with surrounding spaces", CaptureDescriptor{EquipmentID: "T1", Device: " eqp "}, true},
		{"connect mode out of range", CaptureDescriptor{EquipmentID: "T1", Device: "eqp", EquipmentConnectMode: new(tracepack.SocketRole(99))}, false},
		{"sampling rule", CaptureDescriptor{EquipmentID: "T1", Device: "eqp", Thinning: &SamplingRule{KeepEveryN: 10, KeepInterval: 1500 * time.Millisecond}}, true},
		{"default sampling rule", CaptureDescriptor{EquipmentID: "T1", Device: "eqp", Thinning: &SamplingRule{}}, true},
		{"negative keep every", CaptureDescriptor{EquipmentID: "T1", Device: "eqp", Thinning: &SamplingRule{KeepEveryN: -1}}, false},
		{"negative keep interval", CaptureDescriptor{EquipmentID: "T1", Device: "eqp", Thinning: &SamplingRule{KeepInterval: -time.Second}}, false},
		{"keep interval below a millisecond", CaptureDescriptor{EquipmentID: "T1", Device: "eqp", Thinning: &SamplingRule{KeepInterval: 1500 * time.Microsecond}}, false},
	}
	for _, c := range cases {
		err := c.d.Validate()
		if c.ok && err != nil {
			t.Errorf("%s: Validate() = %v, want nil", c.name, err)
		}
		if !c.ok && !errors.Is(err, ErrInvalidDescriptor) {
			t.Errorf("%s: Validate() = %v, want ErrInvalidDescriptor", c.name, err)
		}
	}
}

func TestStartDescriptorRoundTrip(t *testing.T) {
	t.Parallel()

	endpoint := "192.0.2.10:5000"
	mode := tracepack.SocketRoleActive
	devID := uint64(7)
	t3 := uint64(45000)
	d := CaptureDescriptor{
		EquipmentID:          "T1",
		Device:               "eqp",
		DeviceID:             &devID,
		EquipmentEndpoint:    &endpoint,
		EquipmentConnectMode: &mode,
		HSMSTimers:           &tracepack.HSMSTimers{T3Ms: &t3},
	}
	prev := tracepack.UUID{1}
	origin := time.Unix(1_800_000_000, 5)

	b, err := encodeStartDescriptor(&d, "test-source", &prev, origin, tracepack.DefaultClockStepTolerance, "tracekeep-producer/test")
	if err != nil {
		t.Fatalf("encodeStartDescriptor: %v", err)
	}
	m, err := tracepack.DecodeDescriptor(b)
	if err != nil {
		t.Fatalf("DecodeDescriptor: %v", err)
	}

	source, device := "test-source", "eqp"
	originNs := origin.UnixNano()
	originMono := int64(0)
	tolerance := uint64(tracepack.DefaultClockStepTolerance)
	want := &tracepack.PackMeta{
		EquipmentID:          "T1",
		Transport:            tracepack.TransportHSMSSS,
		CaptureMethod:        tracepack.CaptureMethodRawStream,
		Vantage:              tracepack.VantageHost,
		Recorder:             "tracekeep-producer/test",
		TimeSource:           tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs:   &originNs,
		CaptureOriginMonoNs:  &originMono,
		ClockStepToleranceNs: &tolerance,
		LifecycleCoverage:    tracepack.LifecycleCoverageSubscribed,
		QualityEvaluated:     true,
		RecorderInstanceID:   tracepack.UUID{0x74, 0xd8, 0xfc, 0xf2, 0xe9, 0x2e, 0x53, 0x07, 0xa7, 0x50, 0xc9, 0x55, 0xee, 0xf4, 0x3c, 0x9d},
		RecorderInstanceName: &source,
		Device:               &device,
		PreviousCaptureID:    &prev,
		DeviceID:             &devID,
		HSMSTimers:           &tracepack.HSMSTimers{T3Ms: &t3},
		EquipmentConnectMode: &mode,
		EquipmentEndpoint:    &endpoint,
	}
	if !reflect.DeepEqual(m, want) {
		t.Errorf("decoded descriptor =\n%+v\nwant\n%+v", m, want)
	}
}

func TestStartDescriptorWithoutPrevious(t *testing.T) {
	t.Parallel()

	d := CaptureDescriptor{EquipmentID: "T1", Device: "eqp"}
	b, err := encodeStartDescriptor(&d, "test-source", nil, time.Unix(1_800_000_000, 0), tracepack.DefaultClockStepTolerance, "tracekeep-producer/test")
	if err != nil {
		t.Fatalf("encodeStartDescriptor: %v", err)
	}
	m, err := tracepack.DecodeDescriptor(b)
	if err != nil {
		t.Fatalf("DecodeDescriptor: %v", err)
	}
	if m.PreviousCaptureID != nil {
		t.Errorf("previous_capture_id = %s, want none", m.PreviousCaptureID)
	}
}

// A sampling rule declares S1F3 and S1F4 with one source text, its zero fields taking their defaults (producer.md §4.6).
func TestSamplingRuleDeclaresItsEntries(t *testing.T) {
	t.Parallel()

	cases := []struct {
		rule   *SamplingRule
		source string
	}{
		{nil, ""},
		{&SamplingRule{}, "fdc interval polling by request origin; keep_every_n=60; keep_interval_ms=60000; abnormal=kept"},
		{&SamplingRule{KeepEveryN: 10, KeepInterval: 1500 * time.Millisecond}, "fdc interval polling by request origin; keep_every_n=10; keep_interval_ms=1500; abnormal=kept"},
	}
	for _, c := range cases {
		d := CaptureDescriptor{EquipmentID: "T1", Device: "eqp", Thinning: c.rule}
		b, err := encodeStartDescriptor(&d, "test-source", nil, time.Unix(1_800_000_000, 0), tracepack.DefaultClockStepTolerance, "tracekeep-producer/test")
		if err != nil {
			t.Fatalf("%+v: encodeStartDescriptor: %v", c.rule, err)
		}
		m, err := tracepack.DecodeDescriptor(b)
		if err != nil {
			t.Fatalf("%+v: DecodeDescriptor: %v", c.rule, err)
		}
		var want []tracepack.Thinning
		if c.rule != nil {
			want = []tracepack.Thinning{{Stream: 1, Function: 3, Source: c.source}, {Stream: 1, Function: 4, Source: c.source}}
		}
		if !reflect.DeepEqual(m.Thinning, want) {
			t.Errorf("%+v: thinning = %+v, want %+v", c.rule, m.Thinning, want)
		}
	}
}
