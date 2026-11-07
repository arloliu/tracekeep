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

// ErrFeedNotDrained refuses a change of the feed's lease groups while its stream holds a message (logfeed.md §15.4).
var ErrFeedNotDrained = errors.New("bus: the feed stream holds messages; drain it before changing its lease groups")

// ErrFeedOverlap refuses a feed stream whose name is the traffic or parking stream's, or whose subjects could take their messages or they its own.
var ErrFeedOverlap = errors.New("bus: the feed stream overlaps another stream")

// ErrFeedImmutable refuses an existing feed stream whose storage, retention or compression differs from its specification,
// which an update cannot change in place: the operator drains it, deletes it and runs migrate again (T-64).
var ErrFeedImmutable = errors.New("bus: the feed stream's storage, retention or compression differs; drain and delete it, then migrate again")

// Feed is the host log feed's stream and group consumers as migrate ensures them (logfeed.md §15.4, T-64).
type Feed struct {
	Stream, Prefix string
	// Groups is G, the stream's partitions and its consumers.
	Groups   int
	MaxBytes int64
	// Duplicates is the stream's duplicate window.
	Duplicates time.Duration
	// Replicas are the traffic stream's, the cluster the feed shares.
	Replicas int
	// AckWait is ingest's; MaxAckPending each group consumer's.
	AckWait       time.Duration
	MaxAckPending int
}

// FeedStreamConfig is the feed stream's configuration: a work queue on file storage without compression, discard-new at its limit,
// its duplicate window, and the transform partitioning its equipment tokens into Groups (T-62, T-64).
func FeedStreamConfig(f Feed) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: f.Stream, Subjects: []string{f.Prefix + ".*"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage,
		Compression: jetstream.NoCompression, MaxBytes: f.MaxBytes, MaxMsgs: -1, MaxMsgsPerSubject: -1, Discard: jetstream.DiscardNew,
		Duplicates: f.Duplicates, Replicas: f.Replicas,
		SubjectTransform: &jetstream.SubjectTransformConfig{
			Source: f.Prefix + ".*", Destination: fmt.Sprintf("%s.{{partition(%d,1)}}.{{wildcard(1)}}", f.Prefix, f.Groups),
		},
	}
}

// FeedConsumer is group n's durable name.
func FeedConsumer(n int) string { return "logfeed-" + strconv.Itoa(n) }

// FeedConsumerConfig is group n's consumer: its partition's subjects, explicit acknowledgements, unlimited deliveries,
// and its state in memory, at the stream's replicas, which a work queue requires (T-64).
func FeedConsumerConfig(f Feed, n int) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: FeedConsumer(n), FilterSubject: fmt.Sprintf("%s.%d.>", f.Prefix, n), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: f.AckWait, MaxAckPending: f.MaxAckPending, MaxDeliver: -1, MemoryStorage: true,
	}
}

// CheckFeed refuses a feed stream named as the traffic or parking stream or whose subjects overlap theirs, the service's preflight (T-60).
func CheckFeed(feed, traffic, parked jetstream.StreamConfig) error {
	for _, other := range []jetstream.StreamConfig{traffic, parked} {
		if feed.Name == other.Name || busconfig.SubjectsOverlap(feed.Subjects, other.Subjects) {
			return fmt.Errorf("%w: %s %v and %s %v", ErrFeedOverlap, feed.Name, feed.Subjects, other.Name, other.Subjects)
		}
	}

	return nil
}

// EnsureFeed creates the feed stream and its group consumers, or brings existing ones to f, after CheckFeed against the traffic and parking streams,
// and deletes the consumers of groups at or above f.Groups.
// A change of the lease groups is refused with ErrFeedNotDrained while the stream holds a message, and a stream an update cannot correct with ErrFeedImmutable.
func EnsureFeed(ctx context.Context, js jetstream.JetStream, f Feed, traffic, parked jetstream.StreamConfig) (jetstream.Stream, error) {
	want := FeedStreamConfig(f)
	if err := CheckFeed(want, traffic, parked); err != nil {
		return nil, err
	}
	s, err := js.CreateStream(ctx, want)
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		if s, err = updateFeed(ctx, js, want); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("bus: feed stream %s: %w", f.Stream, err)
	}
	for n := range f.Groups {
		if _, err := s.CreateOrUpdateConsumer(ctx, FeedConsumerConfig(f, n)); err != nil {
			return nil, fmt.Errorf("bus: feed consumer %s: %w", FeedConsumer(n), err)
		}
	}
	names := s.ConsumerNames(ctx)
	for name := range names.Name() {
		n, err := strconv.Atoi(strings.TrimPrefix(name, "logfeed-"))
		if err != nil || !strings.HasPrefix(name, "logfeed-") || n < f.Groups {
			continue
		}
		if err := s.DeleteConsumer(ctx, name); err != nil {
			return nil, fmt.Errorf("bus: retired feed consumer %s: %w", name, err)
		}
	}
	if err := names.Err(); err != nil {
		return nil, fmt.Errorf("bus: feed consumers: %w", err)
	}

	return s, nil
}

// OpenFeed opens the feed stream and checks it against the traffic and parking streams, as every role does at its start with the feed enabled.
func OpenFeed(ctx context.Context, js jetstream.JetStream, name string, traffic, parked jetstream.StreamConfig) (jetstream.Stream, error) {
	s, err := js.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("bus: feed stream %s: %w", name, err)
	}
	if err := CheckFeed(s.CachedInfo().Config, traffic, parked); err != nil {
		return nil, err
	}

	return s, nil
}

// updateFeed brings the existing feed stream to want: it refuses a change of its transform while it holds a message,
// and a storage, retention or compression an update cannot change in the running store.
func updateFeed(ctx context.Context, js jetstream.JetStream, want jetstream.StreamConfig) (jetstream.Stream, error) {
	s, err := js.Stream(ctx, want.Name)
	if err != nil {
		return nil, fmt.Errorf("bus: feed stream %s: %w", want.Name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("bus: feed stream %s: %w", want.Name, err)
	}
	have := info.Config
	if have.Storage != want.Storage || have.Retention != want.Retention || have.Compression != want.Compression {
		return nil, fmt.Errorf("%w: %s", ErrFeedImmutable, want.Name)
	}
	if regrouped := have.SubjectTransform == nil || *have.SubjectTransform != *want.SubjectTransform; regrouped && info.State.Msgs > 0 {
		return nil, fmt.Errorf("%w: %s holds %d", ErrFeedNotDrained, want.Name, info.State.Msgs)
	}
	if feedDiffers(have, want) {
		if s, err = js.UpdateStream(ctx, want); err != nil {
			return nil, fmt.Errorf("bus: update feed stream %s: %w", want.Name, err)
		}
	}

	return s, nil
}

// feedDiffers reports a stream whose settings migrate owns, and an update can correct, differ from want's.
func feedDiffers(have, want jetstream.StreamConfig) bool {
	return !slices.Equal(have.Subjects, want.Subjects) || have.MaxBytes != want.MaxBytes || have.MaxMsgs != want.MaxMsgs ||
		have.MaxMsgsPerSubject != want.MaxMsgsPerSubject || have.MaxAge != want.MaxAge || have.Discard != want.Discard ||
		have.Duplicates != want.Duplicates || have.Replicas != want.Replicas ||
		have.SubjectTransform == nil || *have.SubjectTransform != *want.SubjectTransform
}
