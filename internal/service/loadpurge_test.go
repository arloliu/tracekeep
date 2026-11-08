package service

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/testbus"
)

// A stream kept by limits holds its bound as the feed's work queue does: full, it refuses the publish (discard-new),
// and a purge below a group's checkpoint, its own subjects alone on a shared stream, frees room again (the feed-streams mode's purge).
func TestPurgeFreesTheLimitsBound(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	nc, err := nats.Connect(testbus.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "PURGE", Subjects: []string{"p.>"}, Retention: jetstream.LimitsPolicy,
		Storage: jetstream.FileStorage, MaxBytes: 64 << 10, Discard: jetstream.DiscardNew})
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 1<<10)
	var last uint64
	for i := 0; ; i++ {
		ack, err := js.Publish(ctx, []string{"p.0.a", "p.1.b"}[i%2], body)
		if err != nil {
			break // full: refused
		}
		last = ack.Sequence
		if i > 1000 {
			t.Fatal("a stream past its max_bytes still takes publishes")
		}
	}
	if _, err := js.Publish(ctx, "p.0.a", body); err == nil {
		t.Fatal("a full stream took a publish")
	}
	// Group 0 purges below its checkpoint: group 1's entries stay.
	if err := s.Purge(ctx, jetstream.WithPurgeSequence(last), jetstream.WithPurgeSubject("p.0.>")); err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter("p.>"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.State.Subjects; got["p.0.a"] > 1 || got["p.1.b"] < last/2-1 {
		t.Fatalf("after group 0's purge: %v of %d", got, last)
	}
	if _, err := js.Publish(ctx, "p.0.a", body); err != nil {
		t.Fatalf("a purge freed no room: %v", err)
	}
}
