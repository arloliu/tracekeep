package producer_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// claimStream is the claim bucket's stream as tracekeep migrate leaves it: nats.go's settings with AllowDirect off.
func claimStream() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        busconfig.ClaimStreamName(bucket),
		Subjects:    []string{"$O." + bucket + ".C.>", "$O." + bucket + ".M.>"},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxBytes:    -1,
		MaxMsgs:     -1,
		AllowDirect: false,
	}
}

// configured sets up the streams a correct deployment has.
func configured(bus *producertest.Bus) {
	bus.SetStream(busconfig.StreamConfig(busconfig.DefaultTrafficStream, "tpk"))
	bus.SetStream(claimStream())
	bus.SetStream(busconfig.ParkedStreamConfig(busconfig.DefaultParkedStream, busconfig.DefaultParkPrefix, 1))
}

func checks(problems []producer.Problem) []string {
	out := make([]string, 0, len(problems))
	for _, p := range problems {
		out = append(out, p.Check)
	}

	return out
}

func TestPreflightPassesACorrectBus(t *testing.T) {
	t.Parallel()

	h := newHarness(t, producer.Tunables{})
	configured(h.bus)
	h.begin()
	problems, err := h.p.Preflight(t.Context())
	if err != nil || len(problems) != 0 {
		t.Fatalf("Preflight = %v, %v; want no problem", problems, err)
	}
}

func TestPreflightNamesEachMisconfiguration(t *testing.T) {
	t.Parallel()

	traffic := func(f func(c *jetstream.StreamConfig)) func(*producertest.Bus) {
		return func(b *producertest.Bus) {
			c := busconfig.StreamConfig(busconfig.DefaultTrafficStream, "tpk")
			f(&c)
			b.SetStream(c)
		}
	}
	parked := func(f func(c *jetstream.StreamConfig)) func(*producertest.Bus) {
		return func(b *producertest.Bus) {
			c := busconfig.ParkedStreamConfig(busconfig.DefaultParkedStream, busconfig.DefaultParkPrefix, 1)
			f(&c)
			b.SetStream(c)
		}
	}
	claim := func(f func(c *jetstream.StreamConfig)) func(*producertest.Bus) {
		return func(b *producertest.Bus) {
			c := claimStream()
			f(&c)
			b.SetStream(c)
		}
	}
	tests := []struct {
		name  string
		setup func(*producertest.Bus)
		check string
	}{
		{"traffic stream missing", func(b *producertest.Bus) { b.DeleteStream(busconfig.DefaultTrafficStream) }, "stream"},
		{"limits retention", traffic(func(c *jetstream.StreamConfig) { c.Retention = jetstream.LimitsPolicy }), "stream"},
		{"memory storage", traffic(func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }), "stream"},
		{"no compression", traffic(func(c *jetstream.StreamConfig) { c.Compression = jetstream.NoCompression }), "stream"},
		{"age limit", traffic(func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour }), "stream"},
		{"size limit", traffic(func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 30 }), "stream"},
		{"count limit", traffic(func(c *jetstream.StreamConfig) { c.MaxMsgs = 1000 }), "stream"},
		{"per-subject limit", traffic(func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 10 }), "stream"},
		{"direct reads", traffic(func(c *jetstream.StreamConfig) { c.AllowDirect = true }), "stream"},
		{"mirror", traffic(func(c *jetstream.StreamConfig) { c.Mirror = &jetstream.StreamSource{Name: "UP"} }), "stream"},
		{"sources", traffic(func(c *jetstream.StreamConfig) { c.Sources = []*jetstream.StreamSource{{Name: "UP"}} }), "stream"},
		{"subjects miss the prefix", traffic(func(c *jetstream.StreamConfig) { c.Subjects = []string{"other.>"} }), "stream"},
		{"claim bucket missing", func(b *producertest.Bus) { b.DeleteStream(busconfig.ClaimStreamName(bucket)) }, "bucket"},
		{"claim direct reads", claim(func(c *jetstream.StreamConfig) { c.AllowDirect = true }), "bucket"},
		{"claim age limit", claim(func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour }), "bucket"},
		{"claim size limit", claim(func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 30 }), "bucket"},
		{"claim memory storage", claim(func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }), "bucket"},
		{"claim interest retention", claim(func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy }), "bucket"},
		{"claim per-subject limit", claim(func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 }), "bucket"},
		{"parking stream missing", func(b *producertest.Bus) { b.DeleteStream(busconfig.DefaultParkedStream) }, "parking"},
		{"parking limits retention", parked(func(c *jetstream.StreamConfig) { c.Retention = jetstream.LimitsPolicy }), "parking"},
		{"parking memory storage", parked(func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }), "parking"},
		{"parking age limit", parked(func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour }), "parking"},
		{"parking count limit", parked(func(c *jetstream.StreamConfig) { c.MaxMsgs = 1000 }), "parking"},
		{"parking direct reads", parked(func(c *jetstream.StreamConfig) { c.AllowDirect = true }), "parking"},
		{"parking mirror", parked(func(c *jetstream.StreamConfig) { c.Mirror = &jetstream.StreamSource{Name: "UP"} }), "parking"},
		{"parking takes traffic subjects", parked(func(c *jetstream.StreamConfig) { c.Subjects = []string{"*.>"} }), "parking"},
		{"parking fewer replicas", traffic(func(c *jetstream.StreamConfig) { c.Replicas = 3 }), "parking"},
		{"small max_payload", func(b *producertest.Bus) { b.SetMaxPayload(4000) }, "max_payload"},
		{"connection", func(b *producertest.Bus) {
			b.SetConnProblems([]producer.Problem{{Check: "tls", Detail: "not in use"}})
		}, "tls"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t, producer.Tunables{})
			configured(h.bus)
			tt.setup(h.bus)
			problems, err := h.p.Preflight(t.Context())
			if err != nil {
				t.Fatalf("Preflight: %v", err)
			}
			if got := checks(problems); len(got) != 1 || got[0] != tt.check {
				t.Fatalf("Preflight = %v; want one %q problem", problems, tt.check)
			}
			if problems[0].Detail == "" || !strings.HasPrefix(problems[0].String(), tt.check+": ") {
				t.Fatalf("problem %q does not read as %q: <detail>", problems[0].String(), tt.check)
			}
		})
	}
}

// A start whose descriptor fits the header block inline can still overflow it once the claim-check headers join it,
// which only a max_payload small enough to send the start by claim-check reveals.
func TestPreflightChecksTheCurrentStart(t *testing.T) {
	t.Parallel()

	h := newHarness(t, producer.Tunables{})
	configured(h.bus)
	h.bus.SetMaxPayload(4140)
	var begun bool
	for n := 3200; n > 0 && !begun; n -= 8 {
		sw := strings.Repeat("x", n)
		err := h.p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", HostSoftware: &sw})
		switch {
		case err == nil:
			begun = true
		case !errors.Is(err, producer.ErrInvalidDescriptor):
			t.Fatalf("Begin: %v", err)
		default: // too long for the header block; try a shorter one
		}
	}
	if !begun {
		t.Fatal("no descriptor length began")
	}
	problems, err := h.p.Preflight(t.Context())
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if got := checks(problems); len(got) != 1 || got[0] != "start" {
		t.Fatalf("Preflight = %v; want one start problem", problems)
	}
}

func TestPreflightWithoutABus(t *testing.T) {
	t.Parallel()

	h := newHarness(t, producer.Tunables{})
	h.bus.FailStreamInfo(errDown)
	if _, err := h.p.Preflight(t.Context()); !errors.Is(err, errDown) {
		t.Fatalf("Preflight error %v, want %v", err, errDown)
	}
}
