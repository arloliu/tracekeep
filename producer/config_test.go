package producer

import (
	"errors"
	"testing"
	"time"
)

func TestTunablesDefaults(t *testing.T) {
	t.Parallel()

	got := Tunables{}.withDefaults()
	// The defaults decided in docs/specs/tracekeep/tracekeep-decisions.md T-12 and producer-sdk.md.
	if got.ReservedLaneRecords != 4096 {
		t.Errorf("ReservedLaneRecords = %d, want 4096", got.ReservedLaneRecords)
	}
	if got.ReservationTimeout != time.Second {
		t.Errorf("ReservationTimeout = %v, want 1s", got.ReservationTimeout)
	}
	if got.RetryHorizon != 5*time.Minute {
		t.Errorf("RetryHorizon = %v, want 5m", got.RetryHorizon)
	}
	if got.PublishTimeout != 10*time.Second {
		t.Errorf("PublishTimeout = %v, want 10s", got.PublishTimeout)
	}
	if got.StallWait != 100*time.Millisecond {
		t.Errorf("StallWait = %v, want 100ms", got.StallWait)
	}
	if got.OrdinaryLaneRecords <= 0 || got.OrdinaryLaneBytes <= 0 || got.FaultBackoff <= 0 {
		t.Errorf("unset defaults: %+v", got)
	}

	set := Tunables{ReservedLaneRecords: 8, RetryHorizon: time.Second}.withDefaults()
	if set.ReservedLaneRecords != 8 || set.RetryHorizon != time.Second {
		t.Errorf("withDefaults overrode set values: %+v", set)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	valid := Config{Servers: []string{"nats://127.0.0.1:4222"}, SubjectPrefix: "tpk", ClaimBucket: "tpk-objects"}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	cases := map[string]func(*Config){
		"bad prefix":        func(c *Config) { c.SubjectPrefix = "tpk.>" },
		"no bucket":         func(c *Config) { c.ClaimBucket = "" },
		"negative tunable":  func(c *Config) { c.Tunables.OrdinaryLaneBytes = -1 },
		"zero-size reserve": func(c *Config) { c.Tunables.ReservedLaneRecords = -1 },
	}
	for name, mutate := range cases {
		c := valid
		mutate(&c)
		if err := c.validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: validate() = %v, want ErrInvalidConfig", name, err)
		}
	}
}
