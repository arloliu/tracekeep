package bustest_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/link"
)

// A cluster down when the producer starts is dialed again in the background:
// the start and a record queued meanwhile are published once it is up, and Preflight then runs by itself.
func TestDefaultConnectionRecoversFromAFailedFirstDial(t *testing.T) {
	t.Parallel()

	b := newBus(t)
	b.srv.Stop()
	// Barrier: a listener on the server's port takes the SDK's first dial and hangs up on it,
	// so the first attempt has failed before the server comes back.
	l, err := net.Listen("tcp", strings.TrimPrefix(b.url, "nats://"))
	if err != nil {
		t.Fatalf("listen on the server's port: %v", err)
	}
	refused := make(chan struct{})
	go func() {
		if c, err := l.Accept(); err == nil {
			_ = c.Close()
			close(refused)
		}
	}()
	log := newLog()
	p, err := producer.New(producer.Config{Servers: []string{b.url}, SubjectPrefix: prefix, ClaimBucket: bucket}, producer.WithLogger(log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	rec := link.Recorder(p)
	c := rec.Current()
	observe(rec, frame(64))

	select {
	case <-refused:
	case <-time.After(10 * time.Second):
		t.Fatal("the first dial never reached the server's port")
	}
	_ = l.Close()
	b.srv.Restart()
	msgs := b.fetch(t, 2)
	for i, m := range msgs {
		if want, _ := tracepack.BusMsgID(c.ID, uint64(i)); m.Headers().Get("Nats-Msg-Id") != want {
			t.Errorf("message %d is %q, want %q", i, m.Headers().Get("Nats-Msg-Id"), want)
		}
	}
	log.wait(t, "producer: preflight [check tls")
}

// A server that keeps refusing the producer's credentials does not end the connection for good:
// once it accepts them again, the producer reconnects and publishes.
func TestDefaultConnectionOutlivesRepeatedAuthErrors(t *testing.T) {
	t.Parallel()

	auth := testbus.Options{User: "producer", Password: "right"}
	b := newBusWith(t, auth, nil, false)
	log := newLog()
	p, err := producer.New(producer.Config{
		Servers: []string{b.url}, SubjectPrefix: prefix, ClaimBucket: bucket,
		Auth: producer.AuthConfig{User: auth.User, Password: auth.Password},
	}, producer.WithLogger(log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	rec := link.Recorder(p)
	c := rec.Current()
	b.fetch(t, 1)

	b.srv.Stop()
	b.srv.RestartWith(testbus.Options{User: auth.User, Password: "rotated"})
	// nats.go gives up after the same authentication error twice on one server unless told not to.
	log.wait(t, "producer: traffic connection error")
	log.wait(t, "producer: traffic connection error")
	log.wait(t, "producer: traffic connection error")
	b.srv.Stop()
	b.srv.RestartWith(auth)
	log.wait(t, "producer: traffic connection restored")

	observe(rec, frame(64))
	msgs := b.fetch(t, 1)
	if want, _ := tracepack.BusMsgID(c.ID, 1); msgs[0].Headers().Get("Nats-Msg-Id") != want {
		t.Fatalf("after the credentials returned got %q, want %q", msgs[0].Headers().Get("Nats-Msg-Id"), want)
	}
}

// Close on a connection WithConn brought stops the JetStream publisher's reply subscription,
// so producers built in turn on one connection leave no subscription behind,
// and the connection stays open and usable.
// nats.go keeps one shared request inbox per connection, which the first request opens and no producer owns.
func TestCloseCleansUpAnInjectedConnection(t *testing.T) {
	t.Parallel()

	b := newBus(t)
	nc, err := nats.Connect(b.url, nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	run := func(records int) int {
		p, err := producer.New(producer.Config{SubjectPrefix: prefix, ClaimBucket: bucket}, producer.WithConn(nc))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := p.Begin(ctx, producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		observe(link.Recorder(p), frame(64)) // an asynchronous publish, which opens the reply subscription
		b.fetch(t, records)
		open := nc.NumSubscriptions()
		if rep := p.End(ctx); !rep.Drained {
			t.Fatalf("End = %+v", rep)
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if after := nc.NumSubscriptions(); after >= open {
			t.Fatalf("%d subscriptions after Close, %d before it", after, open)
		}

		return nc.NumSubscriptions()
	}
	first := run(2)
	if second := run(3); second != first { // the first capture's stop, then the second capture's start and record
		t.Errorf("%d subscriptions after the second producer, %d after the first", second, first)
	}
	if nc.IsClosed() {
		t.Fatal("Close closed the injected connection")
	}
	if err := nc.Flush(); err != nil {
		t.Errorf("injected connection unusable after Close: %v", err)
	}
}
