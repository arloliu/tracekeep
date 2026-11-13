package bus_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
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

// checkFeed asserts the group streams and their consumers as logfeed.md §15.4 and T-64 specify them, and no stream of a group at or above G.
func checkFeed(t *testing.T, js jetstream.JetStream, f bus.Feed) {
	t.Helper()
	ctx := t.Context()
	for n := range f.Groups {
		s, err := js.Stream(ctx, bus.FeedStreamName(f, n))
		if err != nil {
			t.Fatalf("group %d's stream: %v", n, err)
		}
		c := s.CachedInfo().Config
		if c.Retention != jetstream.WorkQueuePolicy || c.Storage != jetstream.FileStorage || c.Compression != jetstream.NoCompression ||
			c.Discard != jetstream.DiscardNew || c.MaxBytes != f.MaxBytes/int64(f.Groups)*5/4 || c.Duplicates != f.Duplicates || c.Replicas != f.Replicas ||
			c.SubjectTransform != nil || len(c.Subjects) != 1 || c.Subjects[0] != fmt.Sprintf("tklog.%d.*", n) {
			t.Errorf("group %d's stream %+v", n, c)
		}
		checkConsumer(t, s, f, n)
	}
	for _, name := range []string{bus.FeedStreamName(f, f.Groups), f.Stream} {
		if _, err := js.Stream(ctx, name); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("stream %s = %v, want none", name, err)
		}
	}
}

// checkConsumer asserts group n's consumer, its stream's only one.
func checkConsumer(t *testing.T, s jetstream.Stream, f bus.Feed, n int) {
	t.Helper()
	ctx := t.Context()
	names := s.ConsumerNames(ctx)
	var got []string
	for name := range names.Name() {
		got = append(got, name)
	}
	if len(got) != 1 || got[0] != bus.FeedConsumer(n) {
		t.Errorf("group %d's consumers %v, want %s alone", n, got, bus.FeedConsumer(n))
	}
	cons, err := s.Consumer(ctx, bus.FeedConsumer(n))
	if err != nil {
		t.Fatalf("consumer %d: %v", n, err)
	}
	cc := cons.CachedInfo().Config
	if !cc.MemoryStorage || cc.AckWait != f.AckWait || cc.MaxAckPending != f.MaxAckPending || cc.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("consumer %d %+v", n, cc)
	}
}

func publish(t *testing.T, js jetstream.JetStream, subject string) {
	t.Helper()
	if _, err := js.Publish(t.Context(), subject, []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func purge(t *testing.T, js jetstream.JetStream, name string) {
	t.Helper()
	s, err := js.Stream(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Purge(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// migrate creates the group streams and consumers, a second run changes nothing, a change of G is refused while a stream holds a message
// and applied once the feed is drained, a stream lost below G is created again whatever the others hold,
// and a feed whose subjects overlap the traffic stream's is refused.
func TestEnsureFeed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	js := jetStream(t)
	for _, previous := range []int{0, 4} {
		if err := bus.EnsureFeed(ctx, js, feed(4, 1), previous, trafficStream, parkedStream); err != nil {
			t.Fatalf("EnsureFeed = %v", err)
		}
		checkFeed(t, js, feed(4, 1))
	}
	publish(t, js, "tklog.1.EQ-1")
	if err := bus.EnsureFeed(ctx, js, feed(2, 1), 4, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedNotDrained) {
		t.Fatalf("a change of G over a message = %v, want ErrFeedNotDrained", err)
	}
	checkFeed(t, js, feed(4, 1))
	purge(t, js, bus.FeedStreamName(feed(4, 1), 1))
	if err := bus.EnsureFeed(ctx, js, feed(2, 1), 4, trafficStream, parkedStream); err != nil {
		t.Fatalf("a change of G on a drained feed = %v", err)
	}
	checkFeed(t, js, feed(2, 1))
	publish(t, js, "tklog.0.EQ-1")
	if err := bus.EnsureFeed(ctx, js, feed(6, 1), 2, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedNotDrained) {
		t.Fatalf("a growth of G over a message = %v, want ErrFeedNotDrained", err)
	}
	purge(t, js, bus.FeedStreamName(feed(2, 1), 0))
	if err := bus.EnsureFeed(ctx, js, feed(6, 1), 2, trafficStream, parkedStream); err != nil {
		t.Fatalf("a growth of G on a drained feed = %v", err)
	}
	checkFeed(t, js, feed(6, 1))

	publish(t, js, "tklog.1.EQ-1")
	if err := js.DeleteStream(ctx, bus.FeedStreamName(feed(6, 1), 0)); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureFeed(ctx, js, feed(6, 1), 6, trafficStream, parkedStream); err != nil {
		t.Fatalf("migrate with a stream lost and a message held = %v", err)
	}
	checkFeed(t, js, feed(6, 1))

	for name, over := range map[string]func(f *bus.Feed, traffic, parked *jetstream.StreamConfig){
		"the traffic prefix": func(f *bus.Feed, _, _ *jetstream.StreamConfig) { f.Stream, f.Prefix = "OVERLAP", "tk" },
		"the parking prefix": func(f *bus.Feed, _, _ *jetstream.StreamConfig) {
			f.Stream, f.Prefix = "OVERLAP", busconfig.DefaultParkPrefix
		},
		"the traffic stream":   func(f *bus.Feed, traffic, _ *jetstream.StreamConfig) { traffic.Name = bus.FeedStreamName(*f, 0) },
		"the parking's stream": func(f *bus.Feed, _, parked *jetstream.StreamConfig) { parked.Name = bus.FeedStreamName(*f, 1) },
	} {
		f, traffic, parked := feed(2, 1), trafficStream, parkedStream
		over(&f, &traffic, &parked)
		if err := bus.EnsureFeed(ctx, js, f, 0, traffic, parked); !errors.Is(err, bus.ErrFeedOverlap) {
			t.Errorf("a feed on %s = %v, want ErrFeedOverlap", name, err)
		}
	}
}

// Each stream's limit is its share of the feed's scaled by 1.25, computed without overflow at the largest totals,
// and migrate refuses a total that leaves a stream no limit, which the server would take as none.
func TestFeedStreamLimit(t *testing.T) {
	t.Parallel()
	f := feed(32, 1)
	f.MaxBytes = 1 << 61
	if got := bus.FeedStreamConfig(f, 0).MaxBytes; got != 1<<56*5/4 {
		t.Errorf("the limit of 2^61 bytes over 32 groups = %d, want %d", got, int64(1<<56*5/4))
	}
	f.MaxBytes = 31
	if err := bus.EnsureFeed(t.Context(), jetStream(t), f, 0, trafficStream, parkedStream); err == nil {
		t.Error("migrate with a share below a byte succeeded, want refused")
	}
}

// migrate replaces the earlier layout's single stream once it holds no message, and refuses while it holds one,
// or when a stream under its name is not the earlier feed's.
func TestEnsureFeedReplacesSingleStream(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	js := jetStream(t)
	single := jetstream.StreamConfig{
		Name: "TRACEKEEP_LOGFEED", Subjects: []string{"tklog.*"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage,
		SubjectTransform: &jetstream.SubjectTransformConfig{Source: "tklog.*", Destination: "tklog.{{partition(4,1)}}.{{wildcard(1)}}"},
	}
	if _, err := js.CreateStream(ctx, single); err != nil {
		t.Fatal(err)
	}
	publish(t, js, "tklog.EQ-1")
	if err := bus.EnsureFeed(ctx, js, feed(4, 1), 0, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedNotDrained) {
		t.Fatalf("the single stream holding a message = %v, want ErrFeedNotDrained", err)
	}
	purge(t, js, single.Name)
	if err := bus.EnsureFeed(ctx, js, feed(4, 1), 0, trafficStream, parkedStream); err != nil {
		t.Fatalf("the drained single stream = %v", err)
	}
	checkFeed(t, js, feed(4, 1))

	foreign := jetstream.StreamConfig{Name: "TRACEKEEP_LOGFEED", Subjects: []string{"other.>"}}
	if _, err := js.CreateStream(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureFeed(ctx, js, feed(4, 1), 4, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedOverlap) {
		t.Errorf("another stream under the feed's name = %v, want ErrFeedOverlap", err)
	}
}

// migrate corrects a setting an update can change, a limit included, and refuses one it cannot, the compression's included (T-64),
// and a stream under a group's name whose subjects are not the group's.
func TestEnsureFeedCorrects(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	js := jetStream(t)
	if err := bus.EnsureFeed(ctx, js, feed(4, 1), 0, trafficStream, parkedStream); err != nil {
		t.Fatal(err)
	}
	drifted := bus.FeedStreamConfig(feed(4, 1), 2)
	drifted.MaxBytes, drifted.MaxAge, drifted.MaxMsgs = 1<<20, time.Hour, 1000
	if _, err := js.UpdateStream(ctx, drifted); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureFeed(ctx, js, feed(4, 1), 4, trafficStream, parkedStream); err != nil {
		t.Fatalf("migrate over drifted limits = %v", err)
	}
	checkFeed(t, js, feed(4, 1))
	s, err := js.Stream(ctx, drifted.Name)
	if err != nil {
		t.Fatal(err)
	}
	if c := s.CachedInfo().Config; c.MaxAge != 0 || c.MaxMsgs != -1 {
		t.Errorf("limits after migrate: max_age %s, max_msgs %d; want none", c.MaxAge, c.MaxMsgs)
	}

	f := feed(2, 1)
	f.Stream, f.Prefix = "COMPRESSED", "tkz"
	compressed := bus.FeedStreamConfig(f, 0)
	compressed.Compression = jetstream.S2Compression
	if _, err := js.CreateStream(ctx, compressed); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureFeed(ctx, js, f, 0, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedImmutable) {
		t.Errorf("migrate over a compressed stream = %v, want ErrFeedImmutable", err)
	}

	f.Stream, f.Prefix = "MISNAMED", "tkm"
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: bus.FeedStreamName(f, 0), Subjects: []string{"tkm.1.*"}}); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureFeed(ctx, js, f, 0, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedOverlap) {
		t.Errorf("migrate over a group stream on another group's subjects = %v, want ErrFeedOverlap", err)
	}
	if err := bus.CheckFeedStreams(ctx, js, f, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedOverlap) {
		t.Errorf("preflight over a group stream on another group's subjects = %v, want ErrFeedOverlap", err)
	}
}

// Preflight passes the group streams migrate ensured, and refuses a feed missing one or overlapping the traffic stream.
func TestCheckFeedStreams(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	js := jetStream(t)
	f := feed(3, 1)
	if err := bus.EnsureFeed(ctx, js, f, 0, trafficStream, parkedStream); err != nil {
		t.Fatal(err)
	}
	if err := bus.CheckFeedStreams(ctx, js, f, trafficStream, parkedStream); err != nil {
		t.Errorf("preflight = %v", err)
	}
	if err := bus.CheckFeedStreams(ctx, js, feed(4, 1), trafficStream, parkedStream); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("preflight with a group stream missing = %v, want not found", err)
	}
	traffic := trafficStream
	traffic.Subjects = []string{"tklog.>"}
	if err := bus.CheckFeedStreams(ctx, js, f, traffic, parkedStream); !errors.Is(err, bus.ErrFeedOverlap) {
		t.Errorf("preflight against an overlapping traffic stream = %v, want ErrFeedOverlap", err)
	}
}

// again runs migrate on f over a feed holding one message: it refuses nothing, re-creates no stream, and the message stays.
func again(t *testing.T, js jetstream.JetStream, f bus.Feed) {
	t.Helper()
	ctx := t.Context()
	created := map[int]time.Time{}
	for n := range f.Groups {
		s, err := js.Stream(ctx, bus.FeedStreamName(f, n))
		if err != nil {
			t.Fatal(err)
		}
		created[n] = s.CachedInfo().Created
	}
	if err := bus.EnsureFeed(ctx, js, f, f.Groups, trafficStream, parkedStream); err != nil {
		t.Fatalf("migrate again over a held message = %v", err)
	}
	for n := range f.Groups {
		s, err := js.Stream(ctx, bus.FeedStreamName(f, n))
		if err != nil {
			t.Fatal(err)
		}
		if !s.CachedInfo().Created.Equal(created[n]) {
			t.Errorf("group %d's stream was re-created", n)
		}
	}
	if msgs, _, err := bus.FeedBacklog(ctx, js, f); err != nil || msgs != 1 {
		t.Errorf("the feed's backlog = %d, %v; want the one message", msgs, err)
	}
}

// cluster is a three-server cluster's servers and a JetStream client of them.
func cluster(t *testing.T) ([]*server.Server, jetstream.JetStream) {
	t.Helper()
	srvs := testbus.StartClusterServers(t, 3)
	urls := ""
	for _, s := range srvs {
		urls += s.ClientURL() + ","
	}
	nc, err := nats.Connect(urls)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	return srvs, js
}

// On a three-node cluster the streams and their memory consumers take three replicas,
// and a publish under the servers' mapping lands on its group's stream.
func TestEnsureFeedClustered(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	srvs, js := cluster(t)
	for _, s := range srvs {
		if err := testbus.MapFeed(s, "tklog", 4); err != nil {
			t.Fatal(err)
		}
	}
	if err := bus.EnsureFeed(ctx, js, feed(4, 3), 0, trafficStream, parkedStream); err != nil {
		t.Fatal(err)
	}
	for n := range 4 {
		s, err := js.Stream(ctx, bus.FeedStreamName(feed(4, 3), n))
		if err != nil {
			t.Fatal(err)
		}
		testbus.AwaitStream(t, js, s)
	}
	checkFeed(t, js, feed(4, 3))
	publish(t, js, "tklog.EQ-1")
	again(t, js, feed(4, 3))
	found := 0
	for n := range 4 {
		s, err := js.Stream(ctx, bus.FeedStreamName(feed(4, 3), n))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetLastMsgForSubject(ctx, fmt.Sprintf("tklog.%d.EQ-1", n)); err == nil {
			found++
		}
	}
	if found != 1 {
		t.Errorf("the entry on %d group streams under their group's subject, want one", found)
	}
}

// migrate refuses, and deletes nothing, while a group stream it would retire is on a server that is down:
// the listing reports such a stream with no state, as if drained.
func TestEnsureFeedOffline(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	srvs, js := cluster(t)
	f := feed(3, 1)
	f.PlacementTags = []string{"tb-0", "tb-1", "tb-2"}
	if err := bus.EnsureFeed(ctx, js, f, 0, trafficStream, parkedStream); err != nil {
		t.Fatal(err)
	}
	publish(t, js, "tklog.2.EQ-1")
	srvs[2].Shutdown()
	srvs[2].WaitForShutdown()
	shrunk := feed(2, 1)
	shrunk.PlacementTags = f.PlacementTags[:2]
	if err := bus.EnsureFeed(ctx, js, shrunk, 3, trafficStream, parkedStream); err == nil {
		t.Fatal("migrate retiring a stream on a server that is down succeeded, want refused")
	}
	found := false
	names := js.StreamNames(ctx)
	for name := range names.Name() {
		found = found || name == bus.FeedStreamName(f, 2)
	}
	if !found {
		t.Error("the retired group's stream on the server that is down was deleted")
	}
}

// Placement tags place group n's stream of one replica on the server of tag n mod their number,
// and a changed list re-places the streams of a drained feed only.
func TestEnsureFeedPlaced(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, js := cluster(t)
	placed := func(f bus.Feed) {
		t.Helper()
		for n := range f.Groups {
			s, err := js.Stream(ctx, bus.FeedStreamName(f, n))
			if err != nil {
				t.Fatal(err)
			}
			testbus.AwaitStream(t, js, s)
			if s, err = js.Stream(ctx, bus.FeedStreamName(f, n)); err != nil {
				t.Fatal(err)
			}
			if got, want := s.CachedInfo().Cluster.Leader, f.PlacementTags[n%len(f.PlacementTags)]; got != want {
				t.Errorf("group %d's stream on %s, want %s", n, got, want)
			}
		}
	}
	f := feed(4, 1)
	f.PlacementTags = []string{"tb-0", "tb-1", "tb-2"}
	if err := bus.EnsureFeed(ctx, js, f, 0, trafficStream, parkedStream); err != nil {
		t.Fatal(err)
	}
	placed(f)
	publish(t, js, "tklog.3.EQ-1")
	again(t, js, f)
	moved := f
	moved.PlacementTags = []string{"tb-2", "tb-1"}
	if err := bus.EnsureFeed(ctx, js, moved, 4, trafficStream, parkedStream); !errors.Is(err, bus.ErrFeedNotDrained) {
		t.Fatalf("a changed placement over a message = %v, want ErrFeedNotDrained", err)
	}
	placed(f)
	purge(t, js, bus.FeedStreamName(f, 3))
	if err := bus.EnsureFeed(ctx, js, moved, 4, trafficStream, parkedStream); err != nil {
		t.Fatalf("a changed placement on a drained feed = %v", err)
	}
	placed(moved)
}
