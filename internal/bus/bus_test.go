package bus_test

import (
	"context"
	"errors"
	"slices"
	"sync"
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

// Ensure creates the traffic stream, the parking stream and the claim bucket with AllowDirect off, each of one replica by default,
// so a single server takes them, and running it again changes nothing.
func TestEnsure(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	js := jetStream(t)
	for range 2 {
		s, parked, err := bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.CachedInfo().Config; got.Name != busconfig.DefaultTrafficStream || got.Retention != jetstream.WorkQueuePolicy || got.Replicas != 1 {
			t.Fatalf("traffic stream %+v", got)
		}
		got := parked.CachedInfo().Config
		if got.Name != busconfig.DefaultParkedStream || got.Retention != jetstream.WorkQueuePolicy || got.MaxAge != 0 ||
			got.Replicas != 1 || len(got.Subjects) != 1 || got.Subjects[0] != busconfig.DefaultParkPrefix+".>" {
			t.Fatalf("parking stream %+v", got)
		}
	}
	claims, err := js.Stream(ctx, busconfig.ClaimStreamName(cfg.ClaimBucket))
	if err != nil {
		t.Fatal(err)
	}
	if c := claims.CachedInfo().Config; c.AllowDirect || c.Replicas != 1 {
		t.Errorf("the claim bucket's stream has AllowDirect %t and %d replicas, want off and one", c.AllowDirect, c.Replicas)
	}
}

// On three servers, Ensure creates the three streams with bus.replicas;
// it raises existing ones of one replica, the parking stream before the traffic stream,
// and refuses, changing nothing, a bus.replicas below an existing count (T-71).
func TestEnsureReplicas(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, js := cluster(t)
	names := func(c bus.Config) []string {
		return []string{bus.StreamName(c, catalog.RoleParking), bus.StreamName(c, catalog.RoleTraffic), busconfig.ClaimStreamName(c.ClaimBucket)}
	}
	replicas := func(c bus.Config, want int) {
		t.Helper()
		for _, name := range names(c) {
			var s jetstream.Stream
			testbus.Retry(t, func(ctx context.Context) (err error) {
				s, err = js.Stream(ctx, name)
				return err
			})
			if got := s.CachedInfo().Config; got.Replicas != want || got.AllowDirect {
				t.Errorf("stream %s has %d replicas and AllowDirect %t, want %d and off", name, got.Replicas, got.AllowDirect, want)
			}
		}
	}

	created := bus.Config{Stream: "A", ParkedStream: "A_PARKED", SubjectPrefix: "a", ParkPrefix: "apark", ClaimBucket: "a-claims", Replicas: 3}
	traffic, parked, err := bus.Ensure(ctx, js, created)
	if err != nil {
		t.Fatal(err)
	}
	if traffic.CachedInfo().Config.Replicas != 3 || parked.CachedInfo().Config.Replicas != 3 {
		t.Errorf("Ensure returned streams of %d and %d replicas, want 3", traffic.CachedInfo().Config.Replicas, parked.CachedInfo().Config.Replicas)
	}
	replicas(created, 3)

	raised := bus.Config{Stream: "B", ParkedStream: "B_PARKED", SubjectPrefix: "b", ParkPrefix: "bpark", ClaimBucket: "b-claims"}
	if _, _, err := bus.Ensure(ctx, js, raised); err != nil {
		t.Fatal(err)
	}
	replicas(raised, 1)
	raised.Replicas = 3
	rec := &recording{JetStream: js}
	if traffic, parked, err = bus.Ensure(ctx, rec, raised); err != nil {
		t.Fatal(err)
	}
	if want := names(raised); !slices.Equal(rec.changes, want) {
		t.Errorf("Ensure raising changed %v, want %v in that order", rec.changes, want)
	}
	if traffic.CachedInfo().Config.Replicas != 3 || parked.CachedInfo().Config.Replicas != 3 {
		t.Errorf("Ensure returned streams of %d and %d replicas, want the raised 3", traffic.CachedInfo().Config.Replicas, parked.CachedInfo().Config.Replicas)
	}
	replicas(raised, 3)

	rec = &recording{JetStream: js}
	for _, lower := range []int{1, 2} {
		raised.Replicas = lower
		if _, _, err := bus.Ensure(ctx, rec, raised); !errors.Is(err, bus.ErrReplicasAbove) {
			t.Errorf("Ensure with bus.replicas %d over streams of 3 = %v, want ErrReplicasAbove", lower, err)
		}
	}
	if len(rec.changes) > 0 {
		t.Errorf("a refused Ensure changed %v, want nothing", rec.changes)
	}
	replicas(raised, 3)
}

// recording is a JetStream that records the streams it creates or updates, the claim bucket's by its stream's name.
type recording struct {
	jetstream.JetStream
	changes []string
}

func (r *recording) CreateStream(ctx context.Context, c jetstream.StreamConfig) (jetstream.Stream, error) { //nolint:gocritic // the interface's signature
	r.changes = append(r.changes, c.Name)
	return r.JetStream.CreateStream(ctx, c)
}

func (r *recording) UpdateStream(ctx context.Context, c jetstream.StreamConfig) (jetstream.Stream, error) { //nolint:gocritic // the interface's signature
	r.changes = append(r.changes, c.Name)
	return r.JetStream.UpdateStream(ctx, c)
}

func (r *recording) CreateObjectStore(ctx context.Context, c jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) { //nolint:gocritic // the interface's signature
	r.changes = append(r.changes, busconfig.ClaimStreamName(c.Bucket))
	return r.JetStream.CreateObjectStore(ctx, c)
}

// Ingest's consumer is named for the stream's incarnation, with explicit acks and no delivery limit.
func TestConsumerIsNamedForTheIncarnation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		s, _, err := bus.Ensure(ctx, jetStream(t), cfg)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s)
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
		s, _, err := bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s); err != nil {
			t.Fatal(err)
		}
		if err := js.DeleteStream(ctx, busconfig.DefaultTrafficStream); err != nil {
			t.Fatal(err)
		}
		// Created has a nanosecond resolution; a later creation differs even within the same second.
		if s, _, err = bus.Ensure(ctx, js, cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Incarnate of the recreated stream = %v, want ErrIncarnationChanged", err)
		}
	})
}

// A stream whose last sequence is below one observed was rewound by a restore that kept its creation time.
func TestRewoundStreamIsRefused(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		s, _, err := bus.Ensure(ctx, jetStream(t), cfg)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s)
		if err != nil {
			t.Fatal(err)
		}
		if err := cat.ObserveLastSeq(ctx, inc.ID, 50); err != nil { // as if 50 messages had been seen before the restore
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Incarnate of the rewound stream = %v, want ErrIncarnationChanged", err)
		}
	})
}

// paused holds its first Info's caller after the read until resumed.
type paused struct {
	jetstream.Stream
	once         sync.Once
	read, resume chan struct{}
}

func (p *paused) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	info, err := p.Stream.Info(ctx, opts...)
	p.once.Do(func() {
		close(p.read)
		<-p.resume
	})

	return info, err
}

// Two replicas renewing at once: one reads the stream, the other observes a later sequence and records it,
// then the first compares; a stream that only grew is no rewind.
func TestConcurrentObservationsAreNoRewind(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		js := jetStream(t)
		s, _, err := bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s); err != nil {
			t.Fatal(err)
		}
		first := &paused{Stream: s, read: make(chan struct{}), resume: make(chan struct{})}
		done := make(chan error, 1)
		go func() { _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, first); done <- err }()
		<-first.read // the first replica read the stream's last sequence
		if _, err := js.Publish(ctx, cfg.SubjectPrefix+".T1.x", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s); err != nil { // the second records the later sequence
			t.Fatal(err)
		}
		close(first.resume)
		if err := <-done; err != nil {
			t.Errorf("the first replica's Incarnate = %v, want no rewind", err)
		}
	})
}

// scripted answers Info with each of its answers in turn, the last one ever after.
type scripted struct {
	jetstream.Stream
	answers []*jetstream.StreamInfo
}

func (s *scripted) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	a := s.answers[0]
	if len(s.answers) > 1 {
		s.answers = s.answers[1:]
	}

	return a, nil
}

// A restore between the two reads of one check, which kept the creation time, is a rewind:
// the second read is below the first, though above every sequence recorded before.
func TestRewindBetweenTheReads(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		s, _, err := bus.Ensure(ctx, jetStream(t), cfg)
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		at := func(seq uint64) *jetstream.StreamInfo {
			i := *info
			i.State.LastSeq = seq
			return &i
		}
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, &scripted{Stream: s, answers: []*jetstream.StreamInfo{at(10)}}); err != nil {
			t.Fatal(err)
		}
		rewound := &scripted{Stream: s, answers: []*jetstream.StreamInfo{at(12), at(11)}}
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, rewound); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Incarnate across a rewind = %v, want ErrIncarnationChanged", err)
		}
		// The refused check recorded what it read first: the stream stays refused.
		if _, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, &scripted{Stream: s, answers: []*jetstream.StreamInfo{at(11)}}); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Incarnate after the refusal = %v, want ErrIncarnationChanged", err)
		}
	})
}

// Ensure refuses a park prefix sharing the traffic prefix's first token, though the server would take the two streams,
// and an existing parking stream CheckParked refuses.
func TestEnsureRefusesAParkingStream(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	shared := cfg
	shared.SubjectPrefix, shared.ParkPrefix = "tk.traffic", "tk.park"
	if _, _, err := bus.Ensure(ctx, jetStream(t), shared); err == nil {
		t.Error("Ensure took a park prefix sharing the traffic prefix's first token")
	}
	js := jetStream(t)
	aged := busconfig.ParkedStreamConfig(busconfig.DefaultParkedStream, busconfig.DefaultParkPrefix, 1)
	aged.MaxAge = time.Hour
	if _, err := js.CreateStream(ctx, aged); err != nil {
		t.Fatal(err)
	}
	if _, _, err := bus.Ensure(ctx, js, cfg); err == nil {
		t.Error("Ensure took an existing parking stream with an age limit")
	}
}

// A parking stream is refused when its subjects could match the traffic stream's, when it has fewer replicas,
// or any age, size or count limit, a mirror, sources or AllowDirect: a parked copy must leave it only by acknowledgement.
func TestCheckParked(t *testing.T) {
	t.Parallel()
	traffic := busconfig.StreamConfig("T", "tk")
	traffic.Replicas = 3
	good := busconfig.ParkedStreamConfig("P", "tkpark", 3)
	if err := bus.CheckParked(traffic, good); err != nil {
		t.Fatalf("CheckParked of the settings migrate creates = %v", err)
	}
	for name, change := range map[string]func(c *jetstream.StreamConfig){
		"overlapping subjects": func(c *jetstream.StreamConfig) { c.Subjects = append(c.Subjects, "tk.*.x") },
		"fewer replicas":       func(c *jetstream.StreamConfig) { c.Replicas = 1 },
		"an age limit":         func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
		"a size limit":         func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 30 },
		"a count limit":        func(c *jetstream.StreamConfig) { c.MaxMsgs = 1000 },
		"a per-subject limit":  func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 },
		"limits retention":     func(c *jetstream.StreamConfig) { c.Retention = jetstream.LimitsPolicy },
		"memory storage":       func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage },
		"a mirror":             func(c *jetstream.StreamConfig) { c.Mirror = &jetstream.StreamSource{Name: "X"} },
		"sources":              func(c *jetstream.StreamConfig) { c.Sources = []*jetstream.StreamSource{{Name: "X"}} },
		"AllowDirect":          func(c *jetstream.StreamConfig) { c.AllowDirect = true },
	} {
		c := good
		c.Subjects = append([]string(nil), good.Subjects...)
		change(&c)
		if err := bus.CheckParked(traffic, c); err == nil {
			t.Errorf("CheckParked took a parking stream with %s", name)
		}
	}
}

// The parked consumer is named for the parking stream's incarnation, and the parking incarnation is recorded under its own role.
func TestParkedConsumerIsNamedForItsIncarnation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		s, parked, err := bus.Ensure(ctx, jetStream(t), cfg)
		if err != nil {
			t.Fatal(err)
		}
		traffic, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, cat, catalog.RoleParking, parked)
		if err != nil || inc.ID == traffic.ID || inc.Role != catalog.RoleParking {
			t.Fatalf("parking incarnation %+v, %v", inc, err)
		}
		c, err := bus.Consumer(ctx, parked, inc, bus.ConsumerConfig{AckWait: time.Minute, MaxAckPending: 100})
		if err != nil {
			t.Fatal(err)
		}
		if got := c.CachedInfo().Config.Durable; got != "tk-parked-2" {
			t.Errorf("parked consumer %q, want tk-parked-2", got)
		}
	})
}

// Adoption's bus part deletes every tk- consumer the stream has, a restored snapshot's included, and leaves any other.
func TestRetireConsumers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, _, err := bus.Ensure(ctx, jetStream(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// A work-queue stream takes several consumers only with disjoint filters.
	for _, name := range []string{"tk-ingest-1", "tk-ingest-2", "tk-parked-3", "audit"} {
		c := jetstream.ConsumerConfig{Durable: name, AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: cfg.SubjectPrefix + "." + name + ".>"}
		if _, err := s.CreateConsumer(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := bus.RetireConsumers(ctx, s); err != nil {
		t.Fatal(err)
	}
	var left []string
	for name := range s.ConsumerNames(ctx).Name() {
		left = append(left, name)
	}
	if len(left) != 1 || left[0] != "audit" {
		t.Errorf("consumers left %v, want only audit", left)
	}
}

// deaf answers every consumer deletion with success and deletes nothing.
type deaf struct{ jetstream.Stream }

func (deaf) DeleteConsumer(context.Context, string) error { return nil }

// A consumer that survives its deletion fails the adoption: the stream is never taken as fenced.
func TestRetireConsumersVerifiesNoneRemains(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, _, err := bus.Ensure(ctx, jetStream(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "tk-ingest-1", AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	if err := bus.RetireConsumers(ctx, deaf{s}); err == nil {
		t.Error("RetireConsumers reported success with a consumer left")
	}
}

// A stale handler's late nak racing the acknowledgement of the same original (the gate's case):
// handler A holds a delivery past the ack wait, the message is redelivered to handler B, which acknowledges it, confirmed;
// A's nak then arrives for a sequence the work-queue stream already removed, and brings nothing back.
func TestLateNakAfterTheAcknowledgement(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		ctx := t.Context()
		js := jetStream(t)
		s, _, err := bus.Ensure(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := bus.Incarnate(ctx, cat, catalog.RoleTraffic, s)
		if err != nil {
			t.Fatal(err)
		}
		c, err := bus.Consumer(ctx, s, inc, bus.ConsumerConfig{AckWait: 300 * time.Millisecond, MaxAckPending: 10})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, "tk.T1.original", []byte("x")); err != nil {
			t.Fatal(err)
		}
		fetch := func() jetstream.Msg {
			t.Helper()
			// The redelivery comes once the ack wait has passed; the fetch waits on it, bounded.
			batch, err := c.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			for m := range batch.Messages() {
				return m
			}
			t.Fatal("no delivery")

			return nil
		}
		stale := fetch()   // handler A, which stalls
		current := fetch() // handler B, after the ack wait
		if md, _ := current.Metadata(); md == nil || md.NumDelivered != 2 {
			t.Fatalf("second delivery %+v, want the redelivery of the same message", md)
		}
		if err := current.DoubleAck(ctx); err != nil {
			t.Fatal(err)
		}
		if err := stale.Nak(); err != nil {
			t.Fatal(err)
		}
		if err := js.Conn().Flush(); err != nil {
			t.Fatal(err)
		}
		info, err := s.Info(ctx)
		if err != nil || info.State.Msgs != 0 {
			t.Errorf("stream %+v, %v; want the message gone despite the late nak", info.State, err)
		}
		if batch, err := c.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond)); err == nil {
			for range batch.Messages() {
				t.Error("the late nak brought the message back")
			}
		}
	})
}
