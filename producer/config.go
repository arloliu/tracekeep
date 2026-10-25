package producer

import (
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// Defaults of the tunables (docs/specs/tracekeep/tracekeep-decisions.md T-12, producer-sdk.md §4.7).
const (
	DefaultReservedLaneRecords = 4096
	DefaultOrdinaryLaneRecords = 65536
	DefaultOrdinaryLaneBytes   = 64 << 20
	DefaultReservationTimeout  = time.Second
	DefaultRetryHorizon        = 5 * time.Minute
	DefaultPublishTimeout      = 10 * time.Second
	DefaultStallWait           = 100 * time.Millisecond
	DefaultFaultBackoff        = 30 * time.Second
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
}

func (c *Config) validate() error {
	if _, err := tracepack.BusSubject(c.SubjectPrefix, "t", tracepack.UUID{}); err != nil {
		return fmt.Errorf("%w: subject prefix %q: %w", ErrInvalidConfig, c.SubjectPrefix, err)
	}
	if c.ClaimBucket == "" {
		return fmt.Errorf("%w: claim bucket is empty", ErrInvalidConfig)
	}

	return c.Tunables.validate()
}

func (t Tunables) validate() error {
	if t.ReservedLaneRecords < 0 || t.OrdinaryLaneRecords < 0 || t.OrdinaryLaneBytes < 0 ||
		t.ReservationTimeout < 0 || t.RetryHorizon < 0 || t.PublishTimeout < 0 || t.StallWait < 0 || t.FaultBackoff < 0 {
		return fmt.Errorf("%w: negative tunable in %+v", ErrInvalidConfig, t)
	}

	return nil
}

func (t Tunables) withDefaults() Tunables {
	def := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	defDur := func(v *time.Duration, d time.Duration) {
		if *v == 0 {
			*v = d
		}
	}
	def(&t.ReservedLaneRecords, DefaultReservedLaneRecords)
	def(&t.OrdinaryLaneRecords, DefaultOrdinaryLaneRecords)
	if t.OrdinaryLaneBytes == 0 {
		t.OrdinaryLaneBytes = DefaultOrdinaryLaneBytes
	}
	defDur(&t.ReservationTimeout, DefaultReservationTimeout)
	defDur(&t.RetryHorizon, DefaultRetryHorizon)
	defDur(&t.PublishTimeout, DefaultPublishTimeout)
	defDur(&t.StallWait, DefaultStallWait)
	defDur(&t.FaultBackoff, DefaultFaultBackoff)

	return t
}
