package bus_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

var cfg = bus.Config{SubjectPrefix: "tk", ClaimBucket: "tk-claims"}

func jetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(testbus.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	return js
}

// Ensure creates the traffic stream and the claim bucket with AllowDirect off, and running it again changes nothing.
func TestEnsure(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	js := jetStream(t)
	for range 2 {
		s, err := bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.CachedInfo().Config; got.Name != busconfig.DefaultTrafficStream || got.Retention != jetstream.WorkQueuePolicy {
			t.Fatalf("traffic stream %+v", got)
		}
	}
	claims, err := js.Stream(ctx, busconfig.ClaimStreamName(cfg.ClaimBucket))
	if err != nil {
		t.Fatal(err)
	}
	if claims.CachedInfo().Config.AllowDirect {
		t.Error("the claim bucket's stream has AllowDirect on")
	}
}

// Ingest's consumer is named for the stream's incarnation, with explicit acks and no delivery limit.
func TestConsumerIsNamedForTheIncarnation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		s, err := bus.Ensure(ctx, jetStream(t), cfg)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, cat, s)
		if err != nil {
			t.Fatal(err)
		}
		c, err := bus.Consumer(ctx, s, inc, bus.ConsumerConfig{AckWait: time.Minute, MaxAckPending: 1000})
		if err != nil {
			t.Fatal(err)
		}
		got := c.CachedInfo().Config
		if got.Durable != "tk-ingest-1" || got.AckPolicy != jetstream.AckExplicitPolicy || got.MaxDeliver != -1 || got.AckWait != time.Minute {
			t.Errorf("consumer %+v", got)
		}
	})
}

// A stream deleted and created again is another incarnation: the service fetches nothing from it until an operator adopts it.
func TestRecreatedStreamIsAnotherIncarnation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		js := jetStream(t)
		s, err := bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, s); err != nil {
			t.Fatal(err)
		}
		if err := js.DeleteStream(ctx, busconfig.DefaultTrafficStream); err != nil {
			t.Fatal(err)
		}
		// Created has a nanosecond resolution; a later creation differs even within the same second.
		if s, err = bus.Ensure(ctx, js, cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, s); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Incarnate of the recreated stream = %v, want ErrIncarnationChanged", err)
		}
	})
}

// A stream whose last sequence is below one observed was rewound by a restore that kept its creation time.
func TestRewoundStreamIsRefused(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		s, err := bus.Ensure(ctx, jetStream(t), cfg)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, cat, s)
		if err != nil {
			t.Fatal(err)
		}
		if err := cat.ObserveLastSeq(ctx, inc.ID, 50); err != nil { // as if 50 messages had been seen before the restore
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, s); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Incarnate of the rewound stream = %v, want ErrIncarnationChanged", err)
		}
	})
}

// Two replicas renewing at once: one reads the stream, the other observes a later sequence and records it,
// then the first compares; a stream that only grew is no rewind. Not parallel: the seam is the package's.
func TestConcurrentObservationsAreNoRewind(t *testing.T) {
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		js := jetStream(t)
		s, err := bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, s); err != nil {
			t.Fatal(err)
		}
		read, resume := make(chan struct{}), make(chan struct{})
		bus.SetHookAfterInfo(func() {
			bus.SetHookAfterInfo(nil)
			close(read)
			<-resume
		})
		t.Cleanup(func() { bus.SetHookAfterInfo(nil) })
		done := make(chan error, 1)
		go func() { _, err := bus.Incarnate(ctx, cat, s); done <- err }()
		<-read // the first replica read the stream's last sequence
		if _, err := js.Publish(ctx, cfg.SubjectPrefix+".T1.x", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, s); err != nil { // the second records the later sequence
			t.Fatal(err)
		}
		close(resume)
		if err := <-done; err != nil {
			t.Errorf("the first replica's Incarnate = %v, want no rewind", err)
		}
	})
}
