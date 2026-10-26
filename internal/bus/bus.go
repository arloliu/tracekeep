// Package bus is ingest's side of the traffic bus (docs/specs/tracekeep/architecture.md §3.1, parking.md *incarnations*):
// the streams tracekeep migrate ensures, the stream's incarnation in the catalog, and the pull consumer named for it.
package bus

import (
	"context"
	"errors"
	"fmt"
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

// Ensure creates the traffic stream and the claim bucket when they are missing, turns the bucket's AllowDirect off (T-28),
// and returns the traffic stream: the bus part of tracekeep migrate.
// An existing stream is left as it is; Preflight reports one whose settings differ.
func Ensure(ctx context.Context, js jetstream.JetStream, cfg Config) (jetstream.Stream, error) {
	name := streamName(cfg)
	s, err := js.CreateStream(ctx, busconfig.StreamConfig(name, cfg.SubjectPrefix))
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		s, err = js.Stream(ctx, name)
	}
	if err != nil {
		return nil, fmt.Errorf("bus: traffic stream %s: %w", name, err)
	}
	if _, err := js.CreateObjectStore(ctx, busconfig.ObjectStoreConfig(cfg.ClaimBucket)); err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return nil, fmt.Errorf("bus: claim bucket %s: %w", cfg.ClaimBucket, err)
	}
	claims, err := js.Stream(ctx, busconfig.ClaimStreamName(cfg.ClaimBucket))
	if err != nil {
		return nil, fmt.Errorf("bus: claim bucket %s: %w", cfg.ClaimBucket, err)
	}
	// nats.go creates the bucket's stream with AllowDirect on; every read must be the leader's own.
	if c := claims.CachedInfo().Config; c.AllowDirect {
		c.AllowDirect = false
		if _, err := js.UpdateStream(ctx, c); err != nil {
			return nil, fmt.Errorf("bus: claim bucket %s: %w", cfg.ClaimBucket, err)
		}
	}

	return s, nil
}

// Incarnate returns the traffic stream's incarnation, recording it in the catalog the first time,
// once the stream answers a fresh Info as that incarnation: the same creation time and a last sequence not below one observed.
// Any other answer returns an error wrapping catalog.ErrIncarnationChanged, and the caller fetches nothing.
//
// The last sequence compared is read after the highest observed one, so another replica recording a later one meanwhile
// is never taken for a rewind: a stream's last sequence only grows, unless a restore rewinds it.
func Incarnate(ctx context.Context, cat *catalog.Catalog, s jetstream.Stream) (catalog.Incarnation, error) {
	info, err := s.Info(ctx)
	if err != nil {
		return catalog.Incarnation{}, fmt.Errorf("bus: stream info: %w", err)
	}
	if hookAfterInfo != nil {
		hookAfterInfo()
	}
	inc, err := cat.Incarnate(ctx, catalog.RoleTraffic, info.Config.Name, info.Created.UnixNano())
	if err != nil {
		return catalog.Incarnation{}, err
	}
	fresh, err := s.Info(ctx)
	if err != nil {
		return catalog.Incarnation{}, fmt.Errorf("bus: stream info: %w", err)
	}
	last := int64(fresh.State.LastSeq) //nolint:gosec // a stream sequence fits
	switch {
	case !fresh.Created.Equal(info.Created):
		return catalog.Incarnation{}, fmt.Errorf("%w: %s was created again", catalog.ErrIncarnationChanged, info.Config.Name)
	case last < inc.LastSeqSeen:
		return catalog.Incarnation{}, fmt.Errorf("%w: last sequence %d below the %d observed", catalog.ErrIncarnationChanged, last, inc.LastSeqSeen)
	}
	if err := cat.ObserveLastSeq(ctx, inc.ID, last); err != nil {
		return catalog.Incarnation{}, err
	}

	return inc, nil
}

// hookAfterInfo is a test seam between the stream's first Info and the catalog's read of the highest observed sequence.
var hookAfterInfo func()

// Consumer creates or updates ingest's pull consumer on the incarnation inc, named for it,
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

// ConsumerName is ingest's durable consumer on incarnation inc.
func ConsumerName(inc catalog.Incarnation) string {
	return fmt.Sprintf("tk-ingest-%d", inc.ID)
}

func streamName(cfg Config) string {
	if cfg.Stream == "" {
		return busconfig.DefaultTrafficStream
	}

	return cfg.Stream
}
