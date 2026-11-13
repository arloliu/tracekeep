package bus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/producer/busconfig"
)

// ErrFeedNotDrained refuses a change of the feed's layout while a feed stream holds a message (logfeed.md §15.4):
// a change of its lease groups or its placement, or the earlier layout's single stream replaced.
var ErrFeedNotDrained = errors.New("bus: a feed stream holds messages; drain the feed before changing its layout")

// ErrFeedOverlap refuses a feed stream whose name is the traffic or parking stream's, or whose subjects could take their messages or they its own,
// and a stream under a feed stream's name whose subjects are not its group's.
var ErrFeedOverlap = errors.New("bus: a feed stream overlaps another stream")

// ErrFeedImmutable refuses an existing feed stream whose storage, retention or compression differs from its specification,
// which an update cannot change in place: the operator drains it, deletes it and runs migrate again (T-64).
var ErrFeedImmutable = errors.New("bus: a feed stream's storage, retention or compression differs; drain and delete it, then migrate again")

// Feed is the host log feed's group streams and their consumers as migrate ensures them (logfeed.md §15.4, T-64, T-67).
type Feed struct {
	// Stream names the group streams, group n's being Stream_n; Prefix is their subjects', group n's being Prefix.n.*.
	Stream, Prefix string
	// Groups is G, the lease groups and their streams.
	Groups int
	// MaxBytes is the feed's total; each group's stream holds MaxBytes / Groups × 1.25 (T-68).
	MaxBytes int64
	// Duplicates is each stream's duplicate window.
	Duplicates time.Duration
	// Replicas are each group stream's.
	Replicas int
	// PlacementTags places group n's stream on the server tagged PlacementTags[n mod their number]; empty places none (T-68).
	PlacementTags []string
	// AckWait is ingest's; MaxAckPending each group consumer's.
	AckWait       time.Duration
	MaxAckPending int
}

// FeedStreamName is group n's stream.
func FeedStreamName(f Feed, n int) string { return f.Stream + "_" + strconv.Itoa(n) }

// FeedStreamConfig is group n's stream: a work queue on file storage without compression, discard-new at its share of the feed's limit,
// its duplicate window, and its placement (T-62, T-64, T-68).
// The servers' account mapping partitions the published subjects into the groups', so the stream has no transform (T-67).
func FeedStreamConfig(f Feed, n int) jetstream.StreamConfig {
	c := jetstream.StreamConfig{
		Name: FeedStreamName(f, n), Subjects: []string{f.Prefix + "." + strconv.Itoa(n) + ".*"}, Retention: jetstream.WorkQueuePolicy,
		Storage: jetstream.FileStorage, Compression: jetstream.NoCompression, MaxBytes: f.MaxBytes * 5 / (4 * int64(f.Groups)), MaxMsgs: -1,
		MaxMsgsPerSubject: -1, Discard: jetstream.DiscardNew, Duplicates: f.Duplicates, Replicas: f.Replicas,
	}
	if len(f.PlacementTags) > 0 {
		c.Placement = &jetstream.Placement{Tags: []string{f.PlacementTags[n%len(f.PlacementTags)]}}
	}

	return c
}

// FeedConsumer is group n's durable name.
func FeedConsumer(n int) string { return "logfeed-" + strconv.Itoa(n) }

// FeedConsumerConfig is group n's consumer, its stream's only one: explicit acknowledgements, unlimited deliveries,
// and its state in memory, at the stream's replicas, which a work queue requires (T-64).
func FeedConsumerConfig(f Feed, n int) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: FeedConsumer(n), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: f.AckWait, MaxAckPending: f.MaxAckPending, MaxDeliver: -1, MemoryStorage: true,
	}
}

// CheckFeed refuses a feed stream named as the traffic or parking stream or whose subjects overlap theirs (T-60).
func CheckFeed(feed, traffic, parked jetstream.StreamConfig) error {
	for _, other := range []jetstream.StreamConfig{traffic, parked} {
		if feed.Name == other.Name || busconfig.SubjectsOverlap(feed.Subjects, other.Subjects) {
			return fmt.Errorf("%w: %s %v and %s %v", ErrFeedOverlap, feed.Name, feed.Subjects, other.Name, other.Subjects)
		}
	}

	return nil
}

// EnsureFeed brings the feed's streams and consumers to f, after CheckFeed of each group stream against the traffic and parking streams (logfeed.md §15.4):
// it deletes the earlier layout's single stream, the streams of groups at or above f.Groups and those placed otherwise, creates the missing ones,
// corrects the settings an update can, and ensures each group's consumer.
// previous is G as the catalog's lease rows record it, 0 before any;
// a G other than it, or any deletion, is refused with ErrFeedNotDrained while a feed stream holds a message,
// and a stream an update cannot correct with ErrFeedImmutable.
// Each request is retried as Retry does, for a cluster just formed.
func EnsureFeed(ctx context.Context, js jetstream.JetStream, f Feed, previous int, traffic, parked jetstream.StreamConfig) error {
	for n := range f.Groups {
		if err := CheckFeed(FeedStreamConfig(f, n), traffic, parked); err != nil {
			return err
		}
	}
	have, err := feedStreams(ctx, js, f)
	if err != nil {
		return err
	}
	drop, err := feedDrops(f, previous, have)
	if err != nil {
		return err
	}
	for _, name := range drop {
		err := Retry(ctx, time.Second, func(ctx context.Context) error { return js.DeleteStream(ctx, name) })
		if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("bus: delete feed stream %s: %w", name, err)
		}
	}
	for n := range f.Groups {
		want := FeedStreamConfig(f, n)
		err := Retry(ctx, time.Second, func(ctx context.Context) error {
			_, err := js.CreateOrUpdateStream(ctx, want)
			return err
		})
		if err != nil {
			return fmt.Errorf("bus: feed stream %s: %w", want.Name, err)
		}
		err = Retry(ctx, time.Second, func(ctx context.Context) error {
			_, err := js.CreateOrUpdateConsumer(ctx, want.Name, FeedConsumerConfig(f, n))
			return err
		})
		if err != nil {
			return fmt.Errorf("bus: feed consumer %s: %w", FeedConsumer(n), err)
		}
	}

	return nil
}

// CheckFeedStreams opens every group's stream and refuses one whose subjects are not its group's or overlap the traffic or parking stream's,
// as every role does at its start with the feed enabled (logfeed.md §15.4).
func CheckFeedStreams(ctx context.Context, js jetstream.JetStream, f Feed, traffic, parked jetstream.StreamConfig) error {
	for n := range f.Groups {
		name := FeedStreamName(f, n)
		s, err := js.Stream(ctx, name)
		if err != nil {
			return fmt.Errorf("bus: feed stream %s: %w", name, err)
		}
		c := s.CachedInfo().Config
		if want := FeedStreamConfig(f, n).Subjects; !slices.Equal(c.Subjects, want) {
			return fmt.Errorf("%w: %s %v, not group %d's %v", ErrFeedOverlap, name, c.Subjects, n, want)
		}
		if err := CheckFeed(c, traffic, parked); err != nil {
			return err
		}
	}

	return nil
}

// FeedBacklog is the messages and bytes every group's stream holds.
func FeedBacklog(ctx context.Context, js jetstream.JetStream, f Feed) (msgs, bytes uint64, err error) {
	for n := range f.Groups {
		s, err := js.Stream(ctx, FeedStreamName(f, n))
		if err != nil {
			return 0, 0, fmt.Errorf("bus: feed stream %s: %w", FeedStreamName(f, n), err)
		}
		st := s.CachedInfo().State
		msgs, bytes = msgs+st.Msgs, bytes+st.Bytes
	}

	return msgs, bytes, nil
}

// feedStreams is the existing streams under the feed's names, the earlier layout's single one included.
func feedStreams(ctx context.Context, js jetstream.JetStream, f Feed) (map[string]*jetstream.StreamInfo, error) {
	have := map[string]*jetstream.StreamInfo{}
	err := Retry(ctx, time.Second, func(ctx context.Context) error {
		clear(have)
		l := js.ListStreams(ctx)
		for info := range l.Info() {
			if name := info.Config.Name; name == f.Stream || feedGroup(f, name) >= 0 {
				have[name] = info
			}
		}

		return l.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("bus: feed streams: %w", err)
	}

	return have, nil
}

// feedDrops is the existing feed streams to delete: the earlier layout's single stream, retired groups' and those placed otherwise.
// It refuses a stream under a feed name whose subjects are not its own, any deletion or a G other than previous while a feed stream holds a message,
// and a kept stream an update cannot correct.
func feedDrops(f Feed, previous int, have map[string]*jetstream.StreamInfo) ([]string, error) {
	var drop []string
	var held uint64
	for name, info := range have {
		held += info.State.Msgs
		c := info.Config
		if name == f.Stream {
			if c.SubjectTransform == nil || !slices.Equal(c.Subjects, []string{f.Prefix + ".*"}) {
				return nil, fmt.Errorf("%w: %s %v is not the earlier layout's feed stream", ErrFeedOverlap, name, c.Subjects)
			}
			drop = append(drop, name)

			continue
		}
		n := feedGroup(f, name)
		want := FeedStreamConfig(f, n)
		switch {
		case !slices.Equal(c.Subjects, want.Subjects):
			return nil, fmt.Errorf("%w: %s %v, not group %d's %v", ErrFeedOverlap, name, c.Subjects, n, want.Subjects)
		case n >= f.Groups || !slices.Equal(placementTags(c.Placement), placementTags(want.Placement)):
			drop = append(drop, name)
		case c.Storage != want.Storage || c.Retention != want.Retention || c.Compression != want.Compression:
			return nil, fmt.Errorf("%w: %s", ErrFeedImmutable, name)
		default:
			// Kept: an update corrects the rest.
		}
	}
	if held > 0 && (len(drop) > 0 || previous != 0 && previous != f.Groups) {
		return nil, fmt.Errorf("%w: they hold %d", ErrFeedNotDrained, held)
	}

	return drop, nil
}

// feedGroup is the group whose stream name is, or -1.
func feedGroup(f Feed, name string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(name, f.Stream+"_"))
	if err != nil || n < 0 || FeedStreamName(f, n) != name {
		return -1
	}

	return n
}

func placementTags(p *jetstream.Placement) []string {
	if p == nil {
		return nil
	}

	return p.Tags
}
