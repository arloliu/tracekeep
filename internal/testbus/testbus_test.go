package testbus_test

import (
	"testing"

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
