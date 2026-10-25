package producer_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// deadURL returns a localhost URL nothing listens on.
func deadURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	return "nats://" + addr
}

// conn returns a connection to nothing, retried in the background, with opts applied.
func conn(t *testing.T, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(deadURL(t), append([]nats.Option{nats.RetryOnFailedConnect(true)}, opts...)...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	return nc
}

func TestNewRefusesAMissingBus(t *testing.T) {
	t.Parallel()

	_, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: bucket})
	if !errors.Is(err, producer.ErrInvalidConfig) {
		t.Fatalf("New without servers: %v, want ErrInvalidConfig", err)
	}
}

func TestNewRefusesAnEnabledReconnectBuffer(t *testing.T) {
	t.Parallel()

	cfg := producer.Config{SubjectPrefix: "tpk", ClaimBucket: bucket}
	if _, err := producer.New(cfg, producer.WithConn(conn(t))); !errors.Is(err, producer.ErrInvalidConfig) {
		t.Fatalf("New with the default reconnect buffer: %v, want ErrInvalidConfig", err)
	}
	nc := conn(t, nats.ReconnectBufSize(-1))
	p, err := producer.New(cfg, producer.WithConn(nc))
	if err != nil {
		t.Fatalf("New with the reconnect buffer disabled: %v", err)
	}
	_ = p.Close()
	if nc.IsClosed() {
		t.Error("Close closed a connection the producer did not dial")
	}
}

func TestNewRefusesConnAndBusTogether(t *testing.T) {
	t.Parallel()

	_, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: bucket},
		producer.WithConn(conn(t, nats.ReconnectBufSize(-1))), producer.WithBus(producertest.NewBus()))
	if !errors.Is(err, producer.ErrInvalidConfig) {
		t.Fatalf("New with both: %v, want ErrInvalidConfig", err)
	}
}

// An unreachable cluster delays neither New nor Begin, and Preflight says it cannot reach the bus.
func TestUnreachableClusterDelaysNothing(t *testing.T) {
	t.Parallel()

	p, err := producer.New(producer.Config{Servers: []string{deadURL(t)}, SubjectPrefix: "tpk", ClaimBucket: bucket})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := p.Begin(ctx, producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := p.Preflight(ctx); err == nil {
		t.Fatal("Preflight on an unreachable bus returned no error")
	}
	// The stop cannot drain to an unreachable bus; End returns at its deadline all the same.
	endCtx, endCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer endCancel()
	// Whether the drain's give-up lands before End's own deadline check is a race, so only the stop is asserted.
	if rep := p.End(endCtx); !rep.StopQueued {
		t.Errorf("End = %+v, want the stop queued", rep)
	}
}

// A server that accepts the connection but never answers holds nats.go's first attempt for its dial timeout;
// New, Begin and Close return while it is held.
func TestStalledHandshakeDelaysNothing(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := l.Accept(); err == nil {
			accepted <- c // held, never answered, until the test ends
		}
	}()

	start := time.Now()
	p, err := producer.New(producer.Config{Servers: []string{"nats://" + l.Addr().String()}, SubjectPrefix: "tpk", ClaimBucket: bucket})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	select {
	case c := <-accepted:
		t.Cleanup(func() { _ = c.Close() })
	case <-time.After(10 * time.Second):
		t.Fatal("the dial never reached the server")
	}
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// nats.go waits two seconds for the server's INFO; all of the above ran inside that wait.
	if took := time.Since(start); took > time.Second {
		t.Fatalf("New, Begin and Close took %s against a stalled handshake", took)
	}
}
