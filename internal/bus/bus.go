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
	// Replicas are the traffic stream's, the parking stream's and the claim bucket's; 0 takes 1 (T-71).
	Replicas int
}

// ConsumerConfig bounds ingest's pull consumer (architecture.md §3.1).
type ConsumerConfig struct {
	// AckWait exceeds the flush interval plus an upload; a held batch's messages are kept with in-progress acks every AckWait/3.
	AckWait time.Duration
	// MaxAckPending exceeds the peak rate times the flush interval plus an upload.
	MaxAckPending int
}

// ErrReplicasAbove refuses a traffic stream, parking stream or claim bucket with more replicas than Config.Replicas:
// migrate raises them and never lowers them, which would drop copies the operator chose to keep (T-71).
var ErrReplicasAbove = errors.New("bus: more replicas than bus.replicas, which migrate never lowers; raise bus.replicas or lower the stream by hand")

// Ensure creates the traffic stream, the parking stream and the claim bucket with cfg.Replicas when they are missing,
// raises an existing one below it, the parking stream before the traffic stream so it never has fewer (CheckParked),
// turns the bucket's AllowDirect off (T-28), and returns the two streams: the bus part of tracekeep migrate.
// It refuses a park prefix sharing the traffic prefix's first token, a parking stream CheckParked refuses,
// and, before changing any, one of the three with more replicas than cfg.Replicas (ErrReplicasAbove).
// An existing stream's other settings are left as they are, and Preflight reports a traffic stream whose settings differ.
// Each request is retried as Retry does, for a cluster just formed.
func Ensure(ctx context.Context, js jetstream.JetStream, cfg Config) (traffic, parked jetstream.Stream, err error) {
	parkPrefix := cmp.Or(cfg.ParkPrefix, busconfig.DefaultParkPrefix)
	if err := busconfig.ValidatePrefixes(cfg.SubjectPrefix, parkPrefix); err != nil {
		return nil, nil, fmt.Errorf("bus: %w", err)
	}
	n := max(cfg.Replicas, 1)
	tname, pname, cname := StreamName(cfg, catalog.RoleTraffic), StreamName(cfg, catalog.RoleParking), busconfig.ClaimStreamName(cfg.ClaimBucket)
	have := map[string]jetstream.Stream{}
	for _, name := range []string{pname, tname, cname} {
		if have[name], err = existing(ctx, js, name, n); err != nil {
			return nil, nil, fmt.Errorf("bus: stream %s: %w", name, err)
		}
	}
	if parked, err = ensureStream(ctx, js, have[pname], busconfig.ParkedStreamConfig(pname, parkPrefix, n)); err != nil {
		return nil, nil, fmt.Errorf("bus: parking stream %s: %w", pname, err)
	}
	tc := busconfig.StreamConfig(tname, cfg.SubjectPrefix)
	tc.Replicas = n
	if traffic, err = ensureStream(ctx, js, have[tname], tc); err != nil {
		return nil, nil, fmt.Errorf("bus: traffic stream %s: %w", tname, err)
	}
	if err := CheckParked(traffic.CachedInfo().Config, parked.CachedInfo().Config); err != nil {
		return nil, nil, err
	}
	if err := ensureClaims(ctx, js, cfg.ClaimBucket, have[cname], n); err != nil {
		return nil, nil, err
	}

	return traffic, parked, nil
}

// StreamName is the name of the stream playing role, the traffic stream's or the parking stream's.
func StreamName(cfg Config, role string) string {
	if role == catalog.RoleParking {
		return cmp.Or(cfg.ParkedStream, busconfig.DefaultParkedStream)
	}

	return cmp.Or(cfg.Stream, busconfig.DefaultTrafficStream)
}

// existing returns the stream name, nil when it is missing, refusing one with more replicas than n.
func existing(ctx context.Context, js jetstream.JetStream, name string, n int) (jetstream.Stream, error) {
	var s jetstream.Stream
	err := Retry(ctx, time.Second, func(ctx context.Context) (err error) {
		s, err = js.Stream(ctx, name)
		return err
	})
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		return nil, nil //nolint:nilnil // a missing stream is not an error
	case err != nil:
		return nil, err
	}
	if r := max(s.CachedInfo().Config.Replicas, 1); r > n {
		return nil, fmt.Errorf("%w: %d replicas, bus.replicas %d", ErrReplicasAbove, r, n)
	}

	return s, nil
}

// ensureStream creates the stream c when s, its existing handle, is nil, and raises it to c's replicas otherwise.
func ensureStream(ctx context.Context, js jetstream.JetStream, s jetstream.Stream, c jetstream.StreamConfig) (jetstream.Stream, error) { //nolint:gocritic // busconfig's value as it comes
	if s != nil {
		return raise(ctx, js, s, c.Replicas, false)
	}
	err := Retry(ctx, time.Second, func(ctx context.Context) (err error) {
		s, err = js.CreateStream(ctx, c)
		return err
	})
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		// Created meanwhile, by another migrate: raised as an existing one.
		if s, err = js.Stream(ctx, c.Name); err == nil {
			return raise(ctx, js, s, c.Replicas, false)
		}
	}

	return s, err
}

// raise updates s to n replicas when it has fewer, and turns its AllowDirect off when directOff is set, its other settings as they are;
// it returns s when there is nothing to change.
// The update returns before the new replicas hold the stream's data, which the servers copy to them meanwhile.
// The stream is read again just before, so a count raised meanwhile by another migrate is refused above n, never written back lower.
// shortcut: two migrates of different bus.replicas updating at once can still race between that read and the update; migrate runs one at a time.
func raise(ctx context.Context, js jetstream.JetStream, s jetstream.Stream, n int, directOff bool) (jetstream.Stream, error) {
	var info *jetstream.StreamInfo
	err := Retry(ctx, time.Second, func(ctx context.Context) (err error) {
		info, err = s.Info(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	c := info.Config
	if r := max(c.Replicas, 1); r > n {
		return nil, fmt.Errorf("%w: %d replicas, bus.replicas %d", ErrReplicasAbove, r, n)
	}
	if max(c.Replicas, 1) >= n && (!directOff || !c.AllowDirect) {
		return s, nil
	}
	c.Replicas = n
	c.AllowDirect = c.AllowDirect && !directOff
	err = Retry(ctx, time.Second, func(ctx context.Context) (err error) {
		s, err = js.UpdateStream(ctx, c)
		return err
	})

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

// ensureClaims creates the claim bucket with n replicas when claims, its stream's existing handle, is nil,
// raises its stream to n replicas otherwise, and turns the stream's AllowDirect off.
func ensureClaims(ctx context.Context, js jetstream.JetStream, bucket string, claims jetstream.Stream, n int) error {
	if claims == nil {
		oc := busconfig.ObjectStoreConfig(bucket)
		oc.Replicas = n
		err := Retry(ctx, time.Second, func(ctx context.Context) error {
			_, err := js.CreateObjectStore(ctx, oc)
			return err
		})
		if err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
			return fmt.Errorf("bus: claim bucket %s: %w", bucket, err)
		}
		err = Retry(ctx, time.Second, func(ctx context.Context) (err error) {
			claims, err = js.Stream(ctx, busconfig.ClaimStreamName(bucket))
			return err
		})
		if err != nil {
			return fmt.Errorf("bus: claim bucket %s: %w", bucket, err)
		}
	}
	// nats.go creates the bucket's stream with AllowDirect on; every read must be the leader's own.
	if _, err := raise(ctx, js, claims, n, true); err != nil {
		return fmt.Errorf("bus: claim bucket %s: %w", bucket, err)
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
