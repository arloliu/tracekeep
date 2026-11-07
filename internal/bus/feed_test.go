package bus_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

func feed(groups, replicas int) bus.Feed {
	return bus.Feed{Stream: "TRACEKEEP_LOGFEED", Prefix: "tklog", Groups: groups, MaxBytes: 1 << 30, Duplicates: 2 * time.Minute,
		Replicas: replicas, AckWait: 7 * time.Minute, MaxAckPending: 100_000}
}

var (
	trafficStream = busconfig.StreamConfig("TRACEKEEP_TRAFFIC", "tk")
	parkedStream  = busconfig.ParkedStreamConfig("TRACEKEEP_PARKED", busconfig.DefaultParkPrefix, 1)
)

// checkFeed asserts the stream and its consumers as logfeed.md §15.4 and T-64 specify them.
func checkFeed(t *testing.T, s jetstream.Stream, f bus.Feed) {
	t.Helper()
	ctx := t.Context()
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := info.Config
	if c.Retention != jetstream.WorkQueuePolicy || c.Storage != jetstream.FileStorage || c.Compression != jetstream.NoCompression ||
		c.Discard != jetstream.DiscardNew || c.MaxBytes != f.MaxBytes || c.Duplicates != f.Duplicates || c.Replicas != f.Replicas ||
		c.SubjectTransform == nil || c.SubjectTransform.Destination != fmt.Sprintf("tklog.{{partition(%d,1)}}.{{wildcard(1)}}", f.Groups) {
		t.Errorf("feed stream %+v", c)
	}
	n := 0
	names := s.ConsumerNames(ctx)
	for range names.Name() {
		n++
	}
	if n != f.Groups {
		t.Errorf("%d consumers, want %d", n, f.Groups)
	}
	for g := range f.Groups {
		cons, err := s.Consumer(ctx, bus.FeedConsumer(g))
		if err != nil {
			t.Fatalf("consumer %d: %v", g, err)
		}
		cc := cons.CachedInfo().Config
		if !cc.MemoryStorage || cc.FilterSubject != fmt.Sprintf("tklog.%d.>", g) || cc.AckWait != f.AckWait || cc.MaxAckPending != f.MaxAckPending || cc.AckPolicy != jetstream.AckExplicitPolicy {
			t.Errorf("consumer %d %+v", g, cc)
		}
	}
}

// migrate creates the feed's stream and group consumers, a second run changes nothing, a change of G is refused while the stream holds a message
// and applied once it is drained, and a feed whose subjects overlap the traffic stream's is refused.
func TestEnsureFeed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	js := jetStream(t)
	for range 2 {
		s, regrouped, err := bus.EnsureFeed(ctx, js, feed(4, 1), trafficStream, parkedStream)
		if err != nil || regrouped {
			t.Fatalf("EnsureFeed = %v, regrouped %v", err, regrouped)
		}
		checkFeed(t, s, feed(4, 1))
	}
	if _, err := js.Publish(ctx, "tklog.EQ-1", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := bus.EnsureFeed(ctx, js, feed(2, 1), trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedNotDrained) {
		t.Fatalf("a change of G over a message = %v, want ErrFeedNotDrained", err)
	}
	s, err := js.Stream(ctx, "TRACEKEEP_LOGFEED")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	s, regrouped, err := bus.EnsureFeed(ctx, js, feed(2, 1), trafficStream, parkedStream)
	if err != nil || !regrouped {
		t.Fatalf("a change of G on a drained stream = %v, regrouped %v", err, regrouped)
	}
	checkFeed(t, s, feed(2, 1))

	over := feed(2, 1)
	over.Stream, over.Prefix = "OVERLAP", "tk"
	if _, _, err := bus.EnsureFeed(ctx, js, over, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedOverlap) {
		t.Errorf("a feed over the traffic prefix = %v, want ErrFeedOverlap", err)
	}
}

// On a three-node cluster the stream and its memory consumers take three replicas, and a publish lands under its group's subject.
func TestEnsureFeedClustered(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	nc, err := nats.Connect(testbus.StartCluster(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	var s jetstream.Stream
	testbus.Retry(t, func(context.Context) error {
		var err error
		s, _, err = bus.EnsureFeed(ctx, js, feed(4, 3), trafficStream, parkedStream)

		return err
	})
	testbus.AwaitStream(t, js, s)
	checkFeed(t, s, feed(4, 3))
	if _, err := js.Publish(ctx, "tklog.EQ-1", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	m, err := s.GetLastMsgForSubject(ctx, "tklog.*.EQ-1")
	if err != nil {
		t.Fatalf("the published entry under its group's subject: %v", err)
	}
	if m.Subject == "tklog.EQ-1" {
		t.Errorf("the stored subject %s, want the transformed one", m.Subject)
	}
}
