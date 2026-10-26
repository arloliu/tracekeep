// Package bus is ingest's side of the traffic bus (docs/specs/tracekeep/architecture.md §3.1, parking.md *incarnations*):
// the streams tracekeep migrate ensures, each stream's incarnation in the catalog, and the pull consumers named for it.
package bus

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// Config names the traffic bus's streams.
type Config struct {
	// Stream is the traffic stream; empty takes busconfig.DefaultTrafficStream.
	Stream string
	// SubjectPrefix is the traffic subjects' prefix.
	SubjectPrefix string
	// ParkedStream is the parking stream; empty takes busconfig.DefaultParkedStream.
	ParkedStream string
	// ParkPrefix is the parking subjects' prefix, whose first token differs from SubjectPrefix's;
	// empty takes busconfig.DefaultParkPrefix.
	ParkPrefix string
	// ClaimBucket is the object store large records are put into.
	ClaimBucket string
}

// ConsumerConfig bounds ingest's pull consumer (architecture.md §3.1).
type ConsumerConfig struct {
	// AckWait exceeds the flush interval plus an upload; a held batch's messages are kept with in-progress acks every AckWait/3.
	AckWait time.Duration
	// MaxAckPending exceeds the peak rate times the flush interval plus an upload.
	MaxAckPending int
}

// Ensure creates the traffic stream, the parking stream with the traffic stream's replicas and the claim bucket when they are missing,
// turns the bucket's AllowDirect off (T-28), and returns the two streams: the bus part of tracekeep migrate.
// It refuses a park prefix sharing the traffic prefix's first token and a parking stream CheckParked refuses;
// an existing traffic stream is left as it is, and Preflight reports one whose settings differ.
func Ensure(ctx context.Context, js jetstream.JetStream, cfg Config) (traffic, parked jetstream.Stream, err error) {
	parkPrefix := cmp.Or(cfg.ParkPrefix, busconfig.DefaultParkPrefix)
	if err := busconfig.ValidatePrefixes(cfg.SubjectPrefix, parkPrefix); err != nil {
		return nil, nil, fmt.Errorf("bus: %w", err)
	}
	name := cmp.Or(cfg.Stream, busconfig.DefaultTrafficStream)
	if traffic, err = ensureStream(ctx, js, busconfig.StreamConfig(name, cfg.SubjectPrefix)); err != nil {
		return nil, nil, fmt.Errorf("bus: traffic stream %s: %w", name, err)
	}
	tc := traffic.CachedInfo().Config
	name = cmp.Or(cfg.ParkedStream, busconfig.DefaultParkedStream)
	if parked, err = ensureStream(ctx, js, busconfig.ParkedStreamConfig(name, parkPrefix, tc.Replicas)); err != nil {
		return nil, nil, fmt.Errorf("bus: parking stream %s: %w", name, err)
	}
	if err := CheckParked(tc, parked.CachedInfo().Config); err != nil {
		return nil, nil, err
	}
	if err := ensureClaims(ctx, js, cfg.ClaimBucket); err != nil {
		return nil, nil, err
	}

	return traffic, parked, nil
}

func ensureStream(ctx context.Context, js jetstream.JetStream, c jetstream.StreamConfig) (jetstream.Stream, error) {
	s, err := js.CreateStream(ctx, c)
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		s, err = js.Stream(ctx, c.Name)
	}

	return s, err
}

// CheckParked refuses a parking stream that could hold a parked copy otherwise than until its acknowledgement (parking.md *Parking*):
// subjects a traffic subject could match, fewer replicas than the traffic stream, any age, size or count limit,
// a retention other than work-queue, memory storage, a mirror or sources, or AllowDirect.
func CheckParked(traffic, parked jetstream.StreamConfig) error {
	var why string
	switch {
	case busconfig.SubjectsOverlap(traffic.Subjects, parked.Subjects):
		why = "its subjects overlap the traffic stream's"
	case parked.Replicas < traffic.Replicas:
		why = fmt.Sprintf("%d replicas, fewer than the traffic stream's %d", parked.Replicas, traffic.Replicas)
	case parked.MaxAge > 0 || parked.MaxBytes > 0 || parked.MaxMsgs > 0 || parked.MaxMsgsPerSubject > 0:
		why = "an age, size or count limit"
	case parked.Retention != jetstream.WorkQueuePolicy || parked.Storage != jetstream.FileStorage:
		why = "not a work-queue stream on file storage"
	case parked.Mirror != nil || len(parked.Sources) > 0:
		why = "a mirror or sources"
	case parked.AllowDirect:
		why = "AllowDirect on"
	default:
		return nil
	}

	return fmt.Errorf("bus: parking stream %s refused: %s", parked.Name, why)
}

// ensureClaims creates the claim bucket when it is missing and turns its stream's AllowDirect off.
func ensureClaims(ctx context.Context, js jetstream.JetStream, bucket string) error {
	if _, err := js.CreateObjectStore(ctx, busconfig.ObjectStoreConfig(bucket)); err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return fmt.Errorf("bus: claim bucket %s: %w", bucket, err)
	}
	claims, err := js.Stream(ctx, busconfig.ClaimStreamName(bucket))
	if err != nil {
		return fmt.Errorf("bus: claim bucket %s: %w", bucket, err)
	}
	// nats.go creates the bucket's stream with AllowDirect on; every read must be the leader's own.
	if c := claims.CachedInfo().Config; c.AllowDirect {
		c.AllowDirect = false
		if _, err := js.UpdateStream(ctx, c); err != nil {
			return fmt.Errorf("bus: claim bucket %s: %w", bucket, err)
		}
	}

	return nil
}

// Incarnate returns the incarnation of s, the stream playing role, recording it in the catalog the first time,
// once the stream answers a fresh Info as that incarnation: the same creation time and a last sequence not below one observed.
// Any other answer returns an error wrapping catalog.ErrIncarnationChanged, and the caller fetches nothing.
//
// The last sequence compared is read after the highest observed one, so another replica recording a later one meanwhile
// is never taken for a rewind: a stream's last sequence only grows, unless a restore rewinds it.
// It is compared with the first read's too, so a rewind between the two reads is caught;
// the first read is recorded before the comparison, so a check after a refusal still finds it.
func Incarnate(ctx context.Context, cat *catalog.Catalog, role string, s jetstream.Stream) (catalog.Incarnation, error) {
	info, err := s.Info(ctx)
	if err != nil {
		return catalog.Incarnation{}, fmt.Errorf("bus: stream info: %w", err)
	}
	inc, err := cat.Incarnate(ctx, role, info.Config.Name, info.Created.UnixNano())
	if err != nil {
		return catalog.Incarnation{}, err
	}
	fresh, err := s.Info(ctx)
	if err != nil {
		return catalog.Incarnation{}, fmt.Errorf("bus: stream info: %w", err)
	}
	first, last := int64(info.State.LastSeq), int64(fresh.State.LastSeq) //nolint:gosec // a stream sequence fits
	if err := cat.ObserveLastSeq(ctx, inc.ID, first); err != nil {
		return catalog.Incarnation{}, err
	}
	seen := max(inc.LastSeqSeen, first)
	switch {
	case !fresh.Created.Equal(info.Created):
		return catalog.Incarnation{}, fmt.Errorf("%w: %s was created again", catalog.ErrIncarnationChanged, info.Config.Name)
	case last < seen:
		return catalog.Incarnation{}, fmt.Errorf("%w: last sequence %d below the %d observed", catalog.ErrIncarnationChanged, last, seen)
	}
	if err := cat.ObserveLastSeq(ctx, inc.ID, last); err != nil {
		return catalog.Incarnation{}, err
	}

	return inc, nil
}

// Consumer creates or updates ingest's pull consumer on the incarnation inc, the traffic stream's or the parking stream's, named for it,
// so a handler of a retired incarnation acknowledges nothing on its replacement (parking.md *incarnations*):
// explicit acks, never terminated after N deliveries.
func Consumer(ctx context.Context, s jetstream.Stream, inc catalog.Incarnation, c ConsumerConfig) (jetstream.Consumer, error) {
	cons, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       ConsumerName(inc),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       c.AckWait,
		MaxDeliver:    -1,
		MaxAckPending: c.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("bus: consumer: %w", err)
	}

	return cons, nil
}

// ConsumerName is ingest's durable consumer on incarnation inc: tk-ingest-<id> on the traffic stream, tk-parked-<id> on the parking stream.
func ConsumerName(inc catalog.Incarnation) string {
	if inc.Role == catalog.RoleParking {
		return fmt.Sprintf("%s-parked-%d", consumerPrefix, inc.ID)
	}

	return fmt.Sprintf("%s-ingest-%d", consumerPrefix, inc.ID)
}

// consumerPrefix starts the name of every durable consumer of the service.
const consumerPrefix = "tk"

// RetireConsumers is the bus part of bus adopt-stream (parking.md *incarnations*):
// it deletes every consumer of s the service names, a restored snapshot's retired incarnations' included,
// and verifies that none remains, so no handler of a retired incarnation can acknowledge on the stream.
func RetireConsumers(ctx context.Context, s jetstream.Stream) error {
	names, err := serviceConsumers(ctx, s)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := s.DeleteConsumer(ctx, name); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
			return fmt.Errorf("bus: delete consumer %s: %w", name, err)
		}
	}
	if names, err = serviceConsumers(ctx, s); err != nil {
		return err
	}
	if len(names) > 0 {
		return fmt.Errorf("bus: consumers %v remain after their deletion", names)
	}

	return nil
}

func serviceConsumers(ctx context.Context, s jetstream.Stream) ([]string, error) {
	lister := s.ConsumerNames(ctx)
	var names []string
	for name := range lister.Name() {
		if strings.HasPrefix(name, consumerPrefix+"-") {
			names = append(names, name)
		}
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("bus: list consumers: %w", err)
	}

	return names, nil
}
