package compact

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// streams is the bus of a copy test: the parking stream, its incarnation, and a consumer that acknowledges.
type streams struct {
	js     jetstream.JetStream
	parked jetstream.Stream
	inc    catalog.Incarnation
	cons   jetstream.Consumer
}

func newStreams(t *testing.T, c *catalog.Catalog) *streams {
	t.Helper()
	ctx := t.Context()
	nc, err := nats.Connect(testbus.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	_, parked, err := bus.Ensure(ctx, js, bus.Config{SubjectPrefix: "tk", ClaimBucket: "claims"})
	if err != nil {
		t.Fatal(err)
	}
	inc, err := bus.Incarnate(ctx, c, catalog.RoleParking, parked)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := parked.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "t", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}

	return &streams{js: js, parked: parked, inc: inc, cons: cons}
}

// copyOf publishes a copy of seq's version to the parking stream and records its copy row, waiting.
func (b *streams) copyOf(t *testing.T, c *catalog.Catalog, capture tracepack.UUID, seq uint64) *catalog.Identity {
	t.Helper()
	ctx := t.Context()
	ack, err := b.js.Publish(ctx, busconfig.DefaultParkPrefix+".T1."+capture.String()+".v", []byte("copy"))
	if err != nil {
		t.Fatal(err)
	}
	orig := &catalog.Identity{Version: catalog.VersionKey{Capture: capture, Seq: seq, Digest: [32]byte{1}}, Incarnation: 99, StreamSeq: int64(seq), Hour: hour}
	if _, err := c.BeginParking(ctx, orig, "unknown-capture", "", 0); err != nil {
		t.Fatal(err)
	}
	cp := *orig
	cp.Incarnation, cp.StreamSeq = b.inc.ID, int64(ack.Sequence) //nolint:gosec // a stream sequence fits
	if _, err := c.RecordCopy(ctx, &cp); err != nil {
		t.Fatal(err)
	}

	return &cp
}

// acknowledge acknowledges the next message of the parking stream, confirmed by the server.
func (b *streams) acknowledge(t *testing.T) {
	t.Helper()
	msgs, err := b.cons.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for m := range msgs.Messages() {
		if err := m.DoubleAck(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func copyState(t *testing.T, c *catalog.Catalog, d *catalog.Identity) string {
	t.Helper()
	vs, err := c.ParkedVersions(t.Context(), &d.Version.Capture)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		for _, k := range v.Copies {
			if k.Incarnation == d.Incarnation && k.StreamSeq == d.StreamSeq {
				return k.State
			}
		}
	}
	t.Fatalf("no copy row %d/%d", d.Incarnation, d.StreamSeq)

	return ""
}

// The reconciliation boundaries (checklist item 3): the acknowledged copy at N is marked acked
// whether nothing was published after it or N+1 was, and a copy still held, or of an unresolved version, is left waiting.
func TestCopiesBoundaries(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		b := newStreams(t, c)
		capture := newID(t)

		first := b.copyOf(t, c, capture, 1)
		b.acknowledge(t) // N acknowledged, nothing published after: N is LastSeq
		if _, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil {
			t.Fatal(err)
		}
		if n, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, b.parked); err != nil || n != 1 {
			t.Fatalf("Copies = %d, %v; want the tail copy marked", n, err)
		}
		if got := copyState(t, c, first); got != "acked" {
			t.Errorf("copy at LastSeq %s, want acked", got)
		}

		unresolved := b.copyOf(t, c, newID(t), 1)
		b.acknowledge(t) // gone from the stream, but its version is unresolved
		other := newID(t)
		second := b.copyOf(t, c, other, 1)
		b.acknowledge(t)
		held := b.copyOf(t, c, other, 2) // N+1 published, still held
		ahead := *held                   // a sequence above LastSeq, which the stream has not accepted as far as the refresh knows
		ahead.StreamSeq += 10
		if _, err := c.RecordCopy(ctx, &ahead); err != nil {
			t.Fatal(err)
		}
		if _, err := c.TerminateParked(ctx, other, nil, "op", "p2"); err != nil {
			t.Fatal(err)
		}
		if n, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, b.parked); err != nil || n != 1 {
			t.Fatalf("Copies = %d, %v; want one marked", n, err)
		}
		for d, want := range map[*catalog.Identity]string{second: "acked", held: "waiting", unresolved: "waiting", &ahead: "waiting"} {
			if got := copyState(t, c, d); got != want {
				t.Errorf("copy %d %s, want %s", d.StreamSeq, got, want)
			}
		}
	})
}

// No row advances on a failed refresh, AllowDirect on, an incarnation mismatch or an unrelated GetMsg error.
func TestCopiesAdvanceNothing(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		b := newStreams(t, c)
		capture := newID(t)
		d := b.copyOf(t, c, capture, 1)
		b.acknowledge(t)
		if _, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil {
			t.Fatal(err)
		}

		if _, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, failing{Stream: b.parked, get: errors.New("unrelated")}); err != nil {
			t.Errorf("Copies with an unrelated GetMsg error = %v, want the copy skipped", err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := f.r.Copies(cancelled, f.lease, catalog.RoleParking, b.parked); err == nil {
			t.Errorf("Copies after a failed refresh = nil, want its error")
		}
		cfg := b.parked.CachedInfo().Config
		cfg.AllowDirect = true
		if _, err := b.js.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, b.parked); !errors.Is(err, ErrAllowDirect) {
			t.Errorf("Copies with AllowDirect on = %v, want ErrAllowDirect", err)
		}
		cfg.AllowDirect = false
		if _, err := b.js.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		// The stream created again, which only the stop-the-world procedure may do: the catalog's incarnation no longer matches.
		if err := b.js.DeleteStream(ctx, cfg.Name); err != nil {
			t.Fatal(err)
		}
		again, err := b.js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, again); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Copies on a mismatched incarnation = %v, want ErrIncarnationChanged", err)
		}
		if got := copyState(t, c, d); got != "waiting" {
			t.Errorf("copy %s, want waiting", got)
		}
	})
}

// failing is a stream whose GetMsg fails with get.
type failing struct {
	jetstream.Stream
	get error
}

func (s failing) GetMsg(context.Context, uint64, ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	return nil, s.get
}
