package testbus_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/testbus"
)

func TestStartServesJetStream(t *testing.T) {
	t.Parallel()

	nc, err := nats.Connect(testbus.Start(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	ctx := t.Context()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	if _, err := js.Publish(ctx, "test.one", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	if msg.Subject != "test.one" || string(msg.Data) != "hello" {
		t.Fatalf("got %q %q, want %q %q", msg.Subject, msg.Data, "test.one", "hello")
	}
}

// A restarted server answers on the same URL with what it stored before it stopped.
func TestRestartKeepsAddressAndStorage(t *testing.T) {
	t.Parallel()

	srv := testbus.StartServer(t, testbus.Options{MaxPayload: 4140})
	nc, err := nats.Connect(srv.URL(), nats.MaxReconnects(-1), nats.ReconnectWait(10*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	if got := nc.MaxPayload(); got != 4140 {
		t.Fatalf("max_payload %d, want 4140", got)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := t.Context()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Subjects: []string{"test.>"}, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if _, err := js.Publish(ctx, "test.one", []byte("kept")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	reconnected := nc.StatusChanged(nats.CONNECTED)
	srv.Stop()
	srv.Restart()
	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("no reconnect after the restart")
	}
	msg, err := stream.GetMsg(ctx, 1)
	if err != nil || string(msg.Data) != "kept" {
		t.Fatalf("after restart: %v, %v", msg, err)
	}
}
