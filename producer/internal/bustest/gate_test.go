package bustest_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/internal/link"
)

// logLines hands every log line, its arguments formatted, to lines.
type logLines struct{ lines chan string }

func newLog() logLines { return logLines{lines: make(chan string, 256)} }

func (l logLines) put(msg string, args []any) {
	select {
	case l.lines <- fmt.Sprint(msg, " ", args):
	default:
	}
}

func (l logLines) Info(msg string, args ...any)  { l.put(msg, args) }
func (l logLines) Warn(msg string, args ...any)  { l.put(msg, args) }
func (l logLines) Error(msg string, args ...any) { l.put(msg, args) }

// wait returns the first line that starts with prefix, failing after ten seconds.
func (l logLines) wait(t *testing.T, prefix string) string {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case line := <-l.lines:
			if len(line) >= len(prefix) && line[:len(prefix)] == prefix {
				return line
			}
		case <-timeout:
			t.Fatalf("no log line %q within ten seconds", prefix)
		}
	}
}

func waitStatus(t *testing.T, ch chan nats.Status, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("connection not %s within ten seconds", what)
	}
}

// With the reconnect buffer disabled, a publish while the bus is down fails at once,
// not when τ (here 30 s) would expire; once the bus is back the record is published.
func TestPublishWhileDisconnectedFailsAtOnce(t *testing.T) {
	t.Parallel()

	b := newBus(t)
	nc, err := nats.Connect(b.url, nats.ReconnectBufSize(-1), nats.MaxReconnects(-1), nats.ReconnectWait(10*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	log := newLog()
	p, err := producer.New(
		producer.Config{SubjectPrefix: prefix, ClaimBucket: bucket, Tunables: producer.Tunables{PublishTimeout: 30 * time.Second}},
		producer.WithConn(nc), producer.WithLogger(log),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	rec := link.Recorder(p)
	c := rec.Current()
	b.fetch(t, 1) // the start is in

	down := nc.StatusChanged(nats.RECONNECTING)
	b.srv.Stop()
	waitStatus(t, down, "reconnecting")
	sent := time.Now()
	observe(rec, frame(64))
	log.wait(t, "producer: traffic bus unreachable")
	if took := time.Since(sent); took > 2*time.Second {
		t.Fatalf("the failed publish took %s to report", took)
	}

	up := nc.StatusChanged(nats.CONNECTED)
	b.srv.Restart()
	waitStatus(t, up, "connected")
	msgs := b.fetch(t, 1)
	if want, _ := tracepack.BusMsgID(c.ID, 1); msgs[0].Headers().Get(jetstream.MsgIDHeader) != want {
		t.Fatalf("after the restart got %q, want %q", msgs[0].Headers().Get(jetstream.MsgIDHeader), want)
	}
}

// At the smallest max_payload a deployment may set, every record goes by claim-check
// and its object is split into many chunks that each fit.
func TestClaimObjectAtTheMinimumMaxPayload(t *testing.T) {
	t.Parallel()

	b := newBusWith(t, testbus.Options{MaxPayload: tracepack.BusMinMaxPayload}, nil, false)
	p, err := producer.New(producer.Config{Servers: []string{b.url}, SubjectPrefix: prefix, ClaimBucket: bucket})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := p.Begin(ctx, producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	rec := link.Recorder(p)
	c := rec.Current()
	payload := frame(1000)
	observe(rec, payload)

	msgs := b.fetch(t, 2)
	os, err := b.js.ObjectStore(ctx, bucket)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	for i, m := range msgs {
		if m.Headers().Get(tracepack.BusHeaderPayloadRef) == "" {
			t.Errorf("message %d travelled inline at max_payload %d", i, tracepack.BusMinMaxPayload)
		}
	}
	name, _ := tracepack.BusMsgID(c.ID, 1)
	info, err := os.GetInfo(ctx, name)
	if err != nil || info.Chunks < 2 {
		t.Fatalf("claim object %+v, %v; want many chunks", info, err)
	}
	if data, err := os.GetBytes(ctx, name); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("claim object read back: %d bytes, %v", len(data), err)
	}
}

// Preflight names each misconfiguration of a live bus, and the plain nats:// connection every time.
func TestPreflightOnALiveBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    testbus.Options
		traffic func(*jetstream.StreamConfig)
		raw     bool
		claim   func(*jetstream.StreamConfig)
		want    []string
	}{
		{"correct", testbus.Options{}, nil, false, nil, []string{"tls"}},
		{"traffic stream missing", testbus.Options{}, func(c *jetstream.StreamConfig) { c.Name = "OTHER" }, false, nil, []string{"stream", "tls"}},
		{"limits and an age", testbus.Options{}, func(c *jetstream.StreamConfig) {
			c.Retention, c.MaxAge = jetstream.LimitsPolicy, time.Hour
		}, false, nil, []string{"stream", "stream", "tls"}},
		{"claim stream with direct reads", testbus.Options{}, nil, true, nil, []string{"bucket", "tls"}},
		{"claim per-subject limit", testbus.Options{}, nil, false, func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 }, []string{"bucket", "tls"}},
		{"small max_payload", testbus.Options{MaxPayload: 4000}, nil, false, nil, []string{"max_payload", "tls"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBusWith(t, tt.opts, tt.traffic, tt.raw)
			if tt.claim != nil {
				s, err := b.js.Stream(t.Context(), busconfig.ClaimStreamName(bucket))
				if err != nil {
					t.Fatalf("claim stream: %v", err)
				}
				cfg := s.CachedInfo().Config
				tt.claim(&cfg)
				if _, err := b.js.UpdateStream(t.Context(), cfg); err != nil {
					t.Fatalf("update claim stream: %v", err)
				}
			}
			// A connection already up, so Preflight need not wait for the background dial.
			nc, err := nats.Connect(b.url, nats.ReconnectBufSize(-1))
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			t.Cleanup(nc.Close)
			p, err := producer.New(producer.Config{SubjectPrefix: prefix, ClaimBucket: bucket}, producer.WithConn(nc))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			problems, err := p.Preflight(ctx)
			if err != nil {
				t.Fatalf("Preflight: %v", err)
			}
			got := make([]string, 0, len(problems))
			for _, pr := range problems {
				got = append(got, pr.Check)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("Preflight = %v; want checks %v", problems, tt.want)
			}
		})
	}
}

// Once the connection is up, the producer runs Preflight by itself and logs what it found.
func TestPreflightRunsOnceConnected(t *testing.T) {
	t.Parallel()

	b := newBusWith(t, testbus.Options{}, nil, true)
	log := newLog()
	p, err := producer.New(producer.Config{Servers: []string{b.url}, SubjectPrefix: prefix, ClaimBucket: bucket}, producer.WithLogger(log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	line := log.wait(t, "producer: preflight [check bucket")
	if !bytes.Contains([]byte(line), []byte(busconfig.ClaimStreamName(bucket))) {
		t.Errorf("preflight line %q does not name the claim stream", line)
	}
}

// A claim bucket whose stream allows direct reads is refused until it is fixed (T-28):
// a direct read could reach a lagging replica, so the never-overwrite check would not hold.
func TestClaimBucketWithDirectReadsIsRefused(t *testing.T) {
	t.Parallel()

	b := newBusWith(t, testbus.Options{MaxPayload: tracepack.BusMinMaxPayload}, nil, true)
	log := newLog()
	p, err := producer.New(producer.Config{Servers: []string{b.url}, SubjectPrefix: prefix, ClaimBucket: bucket}, producer.WithLogger(log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	log.wait(t, "producer: preflight [check bucket") // connected, and the bucket reported
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// At the minimum max_payload the start itself goes by claim-check.
	line := log.wait(t, "producer: reserved record not accepted")
	if !strings.Contains(line, "direct reads") {
		t.Fatalf("retry logged %q, want the direct-read refusal", line)
	}

	ctx := t.Context()
	s, err := b.js.Stream(ctx, busconfig.ClaimStreamName(bucket))
	if err != nil {
		t.Fatalf("claim stream: %v", err)
	}
	cfg := s.CachedInfo().Config
	cfg.AllowDirect = false
	if _, err := b.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatalf("turn direct reads off: %v", err)
	}
	b.fetch(t, 1) // the start, once the bucket is fixed
}

// The start's synchronous publish makes one attempt: with no stream on its subject, nats.go answers no responders,
// and the publisher sees the failure at once instead of after the client's own retries (two, 250 ms apart by default).
func TestStartPublishMakesOneAttempt(t *testing.T) {
	t.Parallel()
	b := newBusWith(t, testbus.Options{}, func(sc *jetstream.StreamConfig) { sc.Subjects = []string{"other.>"} }, false)
	nc, err := nats.Connect(b.url, nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	log := newLog()
	p, err := producer.New(producer.Config{SubjectPrefix: prefix, ClaimBucket: bucket}, producer.WithConn(nc), producer.WithLogger(log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	start := time.Now()
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	line := log.wait(t, "producer: reserved record not accepted, retrying")
	// Two client retries take at least 500 ms; the bound leaves the rest for scheduling under the race detector.
	if took := time.Since(start); took >= 400*time.Millisecond {
		t.Errorf("the start's first failure came after %v: the client retried it", took)
	}
	if !strings.Contains(line, "no response from stream") {
		t.Errorf("first failure %q, want no response from a stream", line)
	}
}
