package producer

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

func TestRecorderInstanceID(t *testing.T) {
	t.Parallel()

	// Expected values computed independently: SHA-1 over the namespace and the length-prefixed pair, RFC 9562 §5.5.
	cases := []struct {
		tool, device, want string
	}{
		{"T1", "eqp", "b6f2d47e-4c06-5b99-89ee-006b945d34cf"},
		{"ab", "c", "6884448a-f7da-55e4-afe9-5e7284e5f888"},
		{"a", "bc", "fa9c19ad-2c40-5f53-b714-6a0c73915e0f"},
	}
	for _, c := range cases {
		d := CaptureDescriptor{ToolID: c.tool, DeviceName: c.device}
		if got := d.RecorderInstanceID().String(); got != c.want {
			t.Errorf("RecorderInstanceID(%q, %q) = %s, want %s", c.tool, c.device, got, c.want)
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
		{"valid", CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}, true},
		{"no tool", CaptureDescriptor{DeviceName: "eqp"}, false},
		{"tool a key cannot hold", CaptureDescriptor{ToolID: "..", DeviceName: "eqp"}, false},
		{"no device", CaptureDescriptor{ToolID: "T1"}, false},
		{"negative tolerance", CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", ClockStepTolerance: -time.Second}, false},
		{"thinning outside SECS-II", CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", Thinning: []tracepack.Thinning{{Stream: 128, Function: 1}}}, false},
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
		ToolID:               "T1",
		DeviceName:           "eqp",
		DeviceID:             &devID,
		EquipmentEndpoint:    &endpoint,
		EquipmentConnectMode: &mode,
		HSMSTimers:           &tracepack.HSMSTimers{T3: &t3},
	}
	prev := tracepack.UUID{1}
	origin := time.Unix(1_800_000_000, 5)

	b, err := encodeStartDescriptor(&d, &prev, origin, "tracekeep-producer/test")
	if err != nil {
		t.Fatalf("encodeStartDescriptor: %v", err)
	}
	m, err := tracepack.DecodeDescriptor(b)
	if err != nil {
		t.Fatalf("DecodeDescriptor: %v", err)
	}

	name := "eqp"
	originNs := origin.UnixNano()
	originMono := int64(0)
	tolerance := uint64(tracepack.DefaultClockStepTolerance)
	want := &tracepack.PackMeta{
		ToolID:               "T1",
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
		RecorderInstanceID:   tracepack.UUID{0xb6, 0xf2, 0xd4, 0x7e, 0x4c, 0x06, 0x5b, 0x99, 0x89, 0xee, 0x00, 0x6b, 0x94, 0x5d, 0x34, 0xcf},
		RecorderInstanceName: &name,
		PreviousCaptureID:    &prev,
		DeviceID:             &devID,
		HSMSTimers:           &tracepack.HSMSTimers{T3: &t3},
		EquipmentConnectMode: &mode,
		EquipmentEndpoint:    &endpoint,
	}
	if !reflect.DeepEqual(m, want) {
		t.Errorf("decoded descriptor =\n%+v\nwant\n%+v", m, want)
	}
}

func TestStartDescriptorWithoutPrevious(t *testing.T) {
	t.Parallel()

	d := CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}
	b, err := encodeStartDescriptor(&d, nil, time.Unix(1_800_000_000, 0), "tracekeep-producer/test")
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
