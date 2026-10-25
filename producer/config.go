package producer

import (
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// Defaults of the tunables (docs/specs/tracekeep/tracekeep-decisions.md T-12, T-29, producer-sdk.md §4.7).
const (
	// DefaultReservedLaneRecords bounds the reserved lane (T-12).
	DefaultReservedLaneRecords = 4096
	// DefaultOrdinaryLaneRecords bounds the ordinary lane by count.
	DefaultOrdinaryLaneRecords = 16384
	// DefaultOrdinaryLaneBytes bounds the ordinary lane by frame bytes.
	DefaultOrdinaryLaneBytes = 64 << 20
	// DefaultReservationTimeout is the unqueued-reservation bound (T-12).
	DefaultReservationTimeout = time.Second
	// DefaultRetryHorizon is H (T-12).
	DefaultRetryHorizon = 5 * time.Minute
	// DefaultPublishTimeout is τ (T-12).
	DefaultPublishTimeout = 10 * time.Second
	// DefaultStallWait is the asynchronous publish's stall wait (producer-sdk.md §4.7).
	DefaultStallWait = 100 * time.Millisecond
	// DefaultFaultBackoff is the pause before rotating away from a fresh capture that faulted again within a minute.
	DefaultFaultBackoff = 30 * time.Second
	// DefaultClockStepTolerance is ε (T-29).
	DefaultClockStepTolerance = tracepack.DefaultClockStepTolerance
)

// ErrInvalidConfig reports a Config New refuses.
var ErrInvalidConfig = errors.New("producer: invalid config")

// Config names the traffic bus and nothing about how to drive it (docs/specs/tracekeep/producer-sdk.md §4.7).
// It holds no identity: every capture's descriptor carries tool_id and the device name.
type Config struct {
	// Servers are the NATS URLs of the traffic cluster; unused with WithConn or WithBus.
	Servers []string
	// Auth authenticates to the traffic cluster.
	Auth AuthConfig
	// TLS secures the connection to the traffic cluster.
	TLS TLSConfig
	// SubjectPrefix is the traffic stream's subject prefix.
	SubjectPrefix string
	// Stream is the traffic stream every publish expects; empty takes busconfig.DefaultTrafficStream.
	Stream string
	// ClaimBucket is the object store large records are put into.
	ClaimBucket string
	// Tunables bounds the lanes and the publisher; every zero field takes its default.
	Tunables Tunables
}

// AuthConfig holds one way of authenticating to the traffic cluster; at most one field is set.
type AuthConfig struct {
	// CredsFile is a NATS credentials file.
	CredsFile string
	// NKeySeedFile is a file holding an NKey seed.
	NKeySeedFile string
	// User and Password authenticate by name.
	User, Password string
	// Token authenticates by token.
	Token string
}

// TLSConfig holds the TLS material of the connection to the traffic cluster.
type TLSConfig struct {
	// CAFile is the CA bundle the server's certificate is checked against.
	CAFile string
	// CertFile and KeyFile are the client certificate and its key.
	CertFile, KeyFile string
	// ServerName overrides the name checked in the server's certificate.
	ServerName string
}

// Tunables bounds the producer's memory and its attempts; a zero field takes its default.
type Tunables struct {
	// ReservedLaneRecords bounds the reserved lane, which holds start, stop and clock-step records and never drops.
	ReservedLaneRecords int
	// OrdinaryLaneRecords bounds the ordinary lane by record count.
	OrdinaryLaneRecords int
	// OrdinaryLaneBytes bounds the ordinary lane by frame bytes.
	OrdinaryLaneBytes int64
	// ReservationTimeout is how long a reserved seq may stay unqueued before it faults the capture.
	ReservationTimeout time.Duration
	// RetryHorizon is H: past it a record that could not be published or put is dropped as a seq gap.
	RetryHorizon time.Duration
	// PublishTimeout is τ, the deadline of every publish and object operation.
	PublishTimeout time.Duration
	// StallWait is how long an asynchronous publish waits for a free slot before it counts as a failed attempt.
	StallWait time.Duration
	// FaultBackoff pauses rotation after a fresh capture faults again within a minute.
	FaultBackoff time.Duration
	// ClockStepTolerance is ε, the wall-against-monotonic drift a capture tolerates before it records a clock step (T-29);
	// every capture's descriptor carries it as clock_step_tolerance_ns.
	ClockStepTolerance time.Duration
}

func (c *Config) validate() error {
	if _, err := tracepack.BusSubject(c.SubjectPrefix, "t", tracepack.UUID{}); err != nil {
		return fmt.Errorf("%w: subject prefix %q: %w", ErrInvalidConfig, c.SubjectPrefix, err)
	}
	if c.ClaimBucket == "" {
		return fmt.Errorf("%w: claim bucket is empty", ErrInvalidConfig)
	}
	if err := c.Auth.validate(); err != nil {
		return err
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return fmt.Errorf("%w: a TLS client certificate needs both its certificate and its key", ErrInvalidConfig)
	}

	return c.Tunables.validate()
}

func (a *AuthConfig) validate() error {
	methods := 0
	for _, set := range []bool{a.CredsFile != "", a.NKeySeedFile != "", a.User != "" || a.Password != "", a.Token != ""} {
		if set {
			methods++
		}
	}
	if methods > 1 {
		return fmt.Errorf("%w: more than one authentication method set", ErrInvalidConfig)
	}
	if (a.User == "") != (a.Password == "") {
		return fmt.Errorf("%w: user and password go together", ErrInvalidConfig)
	}

	return nil
}

func (t Tunables) validate() error {
	if t.ReservedLaneRecords < 0 || t.OrdinaryLaneRecords < 0 || t.OrdinaryLaneBytes < 0 ||
		t.ReservationTimeout < 0 || t.RetryHorizon < 0 || t.PublishTimeout < 0 || t.StallWait < 0 || t.FaultBackoff < 0 ||
		t.ClockStepTolerance < 0 {
		return fmt.Errorf("%w: negative tunable in %+v", ErrInvalidConfig, t)
	}

	return nil
}

func (t Tunables) withDefaults() Tunables {
	orDefault(&t.ReservedLaneRecords, DefaultReservedLaneRecords)
	orDefault(&t.OrdinaryLaneRecords, DefaultOrdinaryLaneRecords)
	orDefault(&t.OrdinaryLaneBytes, DefaultOrdinaryLaneBytes)
	orDefault(&t.ReservationTimeout, DefaultReservationTimeout)
	orDefault(&t.RetryHorizon, DefaultRetryHorizon)
	orDefault(&t.PublishTimeout, DefaultPublishTimeout)
	orDefault(&t.StallWait, DefaultStallWait)
	orDefault(&t.FaultBackoff, DefaultFaultBackoff)
	orDefault(&t.ClockStepTolerance, DefaultClockStepTolerance)

	return t
}

func orDefault[T comparable](v *T, d T) {
	var zero T
	if *v == zero {
		*v = d
	}
}
