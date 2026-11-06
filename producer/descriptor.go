package producer

import (
	"crypto/sha1" //nolint:gosec // RFC 9562 UUIDv5 is defined over SHA-1; it names, it does not protect.
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"

	"github.com/arloliu/tracekeep/producer/internal/core"
)

// ErrInvalidDescriptor reports a capture descriptor the SDK refuses to start or rotate to.
var ErrInvalidDescriptor = errors.New("producer: invalid capture descriptor")

// recorderNamespace is the fixed namespace UUID of every recorder_instance_id the SDK derives.
// Changing it changes every series' identity, so it never changes.
var recorderNamespace = tracepack.UUID{
	0xe4, 0xbb, 0x8a, 0x4e, 0x8a, 0x26, 0x46, 0xc0, 0x8b, 0x44, 0x79, 0x99, 0x4b, 0xc4, 0x0a, 0x3e,
}

// CaptureDescriptor is what an integrator says about one capture of one device:
// the descriptor values it owns (docs/specs/tracekeep/producer.md §4.5).
//
// The SDK owns the rest of the published descriptor:
// recorder_instance_id, derived from EquipmentID, Device and Config.Source, which it writes as recorder_instance_name,
// previous_capture_id, the previous capture of the same series, the capture origin,
// and the values fixed by how the SDK observes:
// HSMS-SS, raw-stream, the host vantage (the gateway is the host endpoint of the connection it records),
// capture clock, subscribed lifecycle coverage, and quality evaluated (the SDK sets each stored quality bit where it holds).
// A pointer or slice field is published only when it is non-nil or non-empty.
type CaptureDescriptor struct {
	// EquipmentID names the equipment (equipment_id); the gateway maps its own term to it; required.
	EquipmentID string
	// Device is the unit of the equipment the recorded link serves, the eqp-hsms device name of a unit's connection (device);
	// required, valid UTF-8 of 1 to 255 bytes and not whitespace only.
	Device string

	// DeviceID is the configured device ID (device_id).
	DeviceID *uint64
	// HSMSTimers are the configured HSMS timers (hsms_timers).
	HSMSTimers *tracepack.HSMSTimers
	// EquipmentConnectMode is the equipment side's role (equipment_connect_mode).
	EquipmentConnectMode *tracepack.SocketRole
	// EquipmentEndpoint is the equipment side, "address:port" (equipment_endpoint).
	EquipmentEndpoint *string
	// HostSoftware is the gateway and its version (host_software).
	HostSoftware *string
	// EquipmentModel is the equipment model, only if configured (equipment_model).
	EquipmentModel *string
	// EquipmentSWRev is the equipment software revision, only if configured (equipment_sw_rev).
	EquipmentSWRev *string
	// Thinning is the capture's sampling rule; nil, the capture thins nothing and declares nothing.
	// The SDK declares S1F3 and S1F4 from it (docs/specs/tracekeep/producer.md §4.6),
	// so a changed rule rotates the capture.
	Thinning *SamplingRule
}

// Defaults of a SamplingRule's zero fields (producer.md §4.6 *Sampling rule*).
const (
	DefaultKeepEveryN   = 60
	DefaultKeepInterval = 60 * time.Second
)

// SamplingRule decides which interval polls keep their bodies:
// a marked S1F3 keeps them when count mod KeepEveryN == 0 or now − last_kept ≥ KeepInterval,
// both counted over the capture current at its registration, so each capture's first poll keeps them.
type SamplingRule struct {
	// KeepEveryN is N; 0 takes DefaultKeepEveryN.
	KeepEveryN int
	// KeepInterval is T, a whole number of milliseconds; 0 takes DefaultKeepInterval.
	KeepInterval time.Duration
}

// filled returns r with its defaults filled in.
func (r SamplingRule) filled() SamplingRule {
	if r.KeepEveryN == 0 {
		r.KeepEveryN = DefaultKeepEveryN
	}
	if r.KeepInterval == 0 {
		r.KeepInterval = DefaultKeepInterval
	}

	return r
}

func (r *SamplingRule) validate() error {
	if r.KeepEveryN < 0 {
		return fmt.Errorf("%w: thinning keep_every_n %d is negative", ErrInvalidDescriptor, r.KeepEveryN)
	}
	if r.KeepInterval < 0 || r.KeepInterval%time.Millisecond != 0 {
		return fmt.Errorf("%w: thinning keep_interval %s is not a whole, non-negative number of milliseconds", ErrInvalidDescriptor, r.KeepInterval)
	}

	return nil
}

// sampling is r for the capture core, its defaults filled in; nil for nil.
func sampling(r *SamplingRule) *core.Sampling {
	if r == nil {
		return nil
	}
	f := r.filled()

	return &core.Sampling{KeepEveryN: uint64(f.KeepEveryN), KeepInterval: int64(f.KeepInterval)} //nolint:gosec // validate refused a negative field
}

// thinningEntries declares what r thins, under the source convention tracekeep's catalog parses.
func thinningEntries(r *SamplingRule) []tracepack.Thinning {
	if r == nil {
		return nil
	}
	f := r.filled()
	src := fmt.Sprintf("fdc interval polling by request origin; keep_every_n=%d; keep_interval_ms=%d; abnormal=kept",
		f.KeepEveryN, f.KeepInterval.Milliseconds())

	return []tracepack.Thinning{{Stream: 1, Function: 3, Source: src}, {Stream: 1, Function: 4, Source: src}}
}

// maxIdentityBytes bounds a device and a source, the catalog's column width (docs/specs/tracekeep/producer.md §4.5).
const maxIdentityBytes = 255

// checkIdentity reports whether s is a device or a source tracekeep accepts:
// valid UTF-8 of 1 to maxIdentityBytes bytes, not whitespace only.
func checkIdentity(s string) error {
	switch {
	case len(s) > maxIdentityBytes:
		return fmt.Errorf("%d bytes, at most %d", len(s), maxIdentityBytes)
	case !utf8.ValidString(s):
		return errors.New("not valid UTF-8")
	case strings.TrimSpace(s) == "":
		return errors.New("empty or whitespace only")
	}

	return nil
}

func writeLenPrefixed(h hash.Hash, s string) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s))) //nolint:gosec // a descriptor string is far below 4 GiB
	h.Write(n[:])
	h.Write([]byte(s))
}

// encodeStartDescriptor encodes the descriptor a capture's start publishes in Tpk-Capture-Descriptor.
// origin is the capture's start: its wall clock is capture_origin_utc_ns, and every mono_ns is measured from it.
// The caller has run Validate on d, and tolerance (ε) is positive; source is the producer's Config.Source.
func encodeStartDescriptor(d *CaptureDescriptor, source string, previous *tracepack.UUID, origin time.Time, tolerance time.Duration, recorder string) ([]byte, error) {
	device := d.Device
	originNs := origin.UnixNano()
	originMono := int64(0)
	toleranceNs := uint64(tolerance) //nolint:gosec // the caller passes a positive tolerance

	m := &tracepack.PackMeta{
		EquipmentID:          d.EquipmentID,
		Transport:            tracepack.TransportHSMSSS,
		CaptureMethod:        tracepack.CaptureMethodRawStream,
		Vantage:              tracepack.VantageHost,
		Recorder:             recorder,
		TimeSource:           tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs:   &originNs,
		CaptureOriginMonoNs:  &originMono,
		ClockStepToleranceNs: &toleranceNs,
		LifecycleCoverage:    tracepack.LifecycleCoverageSubscribed,
		QualityEvaluated:     true,
		RecorderInstanceID:   d.RecorderInstanceID(source),
		RecorderInstanceName: &source,
		Device:               &device,
		PreviousCaptureID:    previous,
		DeviceID:             d.DeviceID,
		HSMSTimers:           d.HSMSTimers,
		EquipmentConnectMode: d.EquipmentConnectMode,
		EquipmentEndpoint:    d.EquipmentEndpoint,
		HostSoftware:         d.HostSoftware,
		EquipmentModel:       d.EquipmentModel,
		EquipmentSWRev:       d.EquipmentSWRev,
		Thinning:             thinningEntries(d.Thinning),
	}

	return tracepack.EncodeDescriptor(m)
}

// Validate reports whether the SDK can start a capture with d.
//
// Returns:
//   - error: nil, or an error wrapping ErrInvalidDescriptor naming the first value it refuses.
func (d *CaptureDescriptor) Validate() error {
	if _, err := tpbus.EquipmentToken(d.EquipmentID); err != nil {
		return fmt.Errorf("%w: equipment_id: %w", ErrInvalidDescriptor, err)
	}
	if err := checkIdentity(d.Device); err != nil {
		return fmt.Errorf("%w: device: %w", ErrInvalidDescriptor, err)
	}
	// The codec does not range-check this enum, and it is the only one the integrator sets.
	if d.EquipmentConnectMode != nil && *d.EquipmentConnectMode > tracepack.SocketRoleActive {
		return fmt.Errorf("%w: equipment_connect_mode %d is not a socket role", ErrInvalidDescriptor, *d.EquipmentConnectMode)
	}
	if d.Thinning != nil {
		if err := d.Thinning.validate(); err != nil {
			return err
		}
	}
	// The descriptor codec checks the remaining values, UTF-8 included, as a consumer's decoder will.
	if _, err := encodeStartDescriptor(d, "validate", nil, time.Unix(0, 0), tracepack.DefaultClockStepTolerance, "validate"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDescriptor, err)
	}

	return nil
}

// RecorderInstanceID returns the recorder_instance_id of the series source records of d's device:
// the UUIDv5 (RFC 9562) under the SDK's fixed namespace of EquipmentID, Device and source, in that order,
// each prefixed by its byte length as a 4-byte big-endian integer, so no choice of characters makes two triples collide.
// It depends on nothing else, so it is stable across restarts, rotations and leadership changes.
func (d *CaptureDescriptor) RecorderInstanceID(source string) tracepack.UUID {
	return RecorderInstanceID(d.EquipmentID, d.Device, source)
}

// RecorderInstanceID returns the recorder_instance_id of the series one source records of one device of one equipment
// (docs/specs/tracekeep/producer.md §4.5), which the service checks at admission.
func RecorderInstanceID(equipmentID, device, source string) tracepack.UUID {
	h := sha1.New() //nolint:gosec // see the import
	h.Write(recorderNamespace[:])
	writeLenPrefixed(h, equipmentID)
	writeLenPrefixed(h, device)
	writeLenPrefixed(h, source)

	var u tracepack.UUID
	copy(u[:], h.Sum(nil))
	u[6] = u[6]&0x0f | 0x50
	u[8] = u[8]&0x3f | 0x80

	return u
}
