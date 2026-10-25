package bustest_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/internal/core"
	"github.com/arloliu/tracekeep/producer/internal/link"
)

const (
	prefix = "tpk"
	bucket = "tpk-objects"
)

// bus is an embedded server with the traffic stream and the claim bucket as tracekeep migrate leaves them.
type bus struct {
	srv *testbus.Server
	url string
	js  jetstream.JetStream
}

func newBus(t *testing.T) *bus {
	t.Helper()

	return newBusWith(t, testbus.Options{}, nil, false)
}

// newBusWith starts a server with o and creates the streams, the traffic stream's settings changed by traffic when it is set;
// raw leaves the claim bucket's stream as nats.go creates it, with direct reads on.
func newBusWith(t *testing.T, o testbus.Options, traffic func(*jetstream.StreamConfig), raw bool) *bus {
	t.Helper()
	srv := testbus.StartServer(t, o)
	url := srv.URL()
	nc, err := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(10*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := t.Context()
	sc := busconfig.StreamConfig(busconfig.DefaultTrafficStream, prefix)
	if traffic != nil {
		traffic(&sc)
	}
	if _, err := js.CreateStream(ctx, sc); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if _, err := js.CreateObjectStore(ctx, busconfig.ObjectStoreConfig(bucket)); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	b := &bus{srv: srv, url: url, js: js}
	if raw {
		return b
	}
	s, err := js.Stream(ctx, busconfig.ClaimStreamName(bucket))
	if err != nil {
		t.Fatalf("claim stream: %v", err)
	}
	cfg := s.CachedInfo().Config
	cfg.AllowDirect = false
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		t.Fatalf("turn direct reads off: %v", err)
	}

	return b
}

// fetch takes n messages off the traffic stream and acknowledges them, as a consumer would, waiting up to ten seconds;
// every call reads through one durable consumer, which a restart keeps.
func (b *bus) fetch(t *testing.T, n int) []jetstream.Msg {
	t.Helper()
	ctx := t.Context()
	cons, err := b.js.CreateOrUpdateConsumer(ctx, busconfig.DefaultTrafficStream, jetstream.ConsumerConfig{Durable: "test", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	var out []jetstream.Msg
	deadline := time.Now().Add(10 * time.Second)
	for len(out) < n && time.Now().Before(deadline) {
		batch, err := cons.Fetch(n-len(out), jetstream.FetchMaxWait(time.Until(deadline)))
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		for m := range batch.Messages() {
			if err := m.Ack(); err != nil {
				t.Fatalf("ack: %v", err)
			}
			out = append(out, m)
		}
	}
	if len(out) < n {
		t.Fatalf("fetched %d messages, want %d", len(out), n)
	}

	return out
}

func observe(rec *core.Recorder, payload []byte) {
	wall, mono := core.Now()
	rec.Observe(&core.Observation{
		Wall: wall, Mono: mono, Socket: 1,
		Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact,
		Payload: payload,
	})
}

func frame(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}

	return b
}

// The connection New dials publishes the start, an inline record and a claim-checked one the way a consumer reads them,
// and Preflight finds nothing wrong but the plain nats:// connection.
func TestDefaultConnectionPublishes(t *testing.T) {
	t.Parallel()

	b := newBus(t)
	p, err := producer.New(producer.Config{Servers: []string{b.url}, SubjectPrefix: prefix, ClaimBucket: bucket})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := p.Begin(ctx, producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	rec := link.Recorder(p)
	c := rec.Current()
	small, large := frame(64), frame(1536<<10) // the large one exceeds max_payload − 4096 and spans two chunks
	observe(rec, small)
	observe(rec, large)

	msgs := b.fetch(t, 3)
	for i, m := range msgs {
		h := m.Headers()
		if want, _ := tracepack.BusMsgID(c.ID, uint64(i)); h.Get(jetstream.MsgIDHeader) != want {
			t.Errorf("message %d id %q, want %q", i, h.Get(jetstream.MsgIDHeader), want)
		}
		if got := h.Get(jetstream.ExpectedStreamHeader); got != busconfig.DefaultTrafficStream {
			t.Errorf("message %d expects stream %q", i, got)
		}
		env := tracepack.BusEnvelope{
			Layout:        h.Get(tracepack.BusHeaderLayout),
			MsgID:         h.Get(jetstream.MsgIDHeader),
			Descriptor:    h.Get(tracepack.BusHeaderDescriptor),
			PayloadRef:    h.Get(tracepack.BusHeaderPayloadRef),
			PayloadDigest: h.Get(tracepack.BusHeaderPayloadDigest),
		}
		br, err := tracepack.CheckBusMessage(env, m.Data())
		if err != nil {
			t.Fatalf("message %d: CheckBusMessage: %v", i, err)
		}
		switch i {
		case 1:
			if !bytes.Equal(br.Record.Payload, small) {
				t.Errorf("inline record payload differs")
			}
		case 2:
			if !br.ClaimCheck {
				t.Fatalf("the large record travelled inline")
			}
			os, err := b.js.ObjectStore(ctx, bucket)
			if err != nil {
				t.Fatalf("bucket: %v", err)
			}
			name, _ := tracepack.BusMsgID(c.ID, 2)
			data, err := os.GetBytes(ctx, name)
			if err != nil || !bytes.Equal(data, large) {
				t.Fatalf("claim object: %d bytes, %v", len(data), err)
			}
			if info, err := os.GetInfo(ctx, name); err != nil || info.Chunks < 2 {
				t.Errorf("claim object info %+v, %v; want two chunks or more", info, err)
			}
		default:
		}
	}

	problems, err := p.Preflight(ctx)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if len(problems) != 1 || problems[0].Check != "tls" {
		t.Errorf("Preflight = %v, want only the tls problem", problems)
	}
	if rep := p.End(ctx); !rep.Drained {
		t.Errorf("End = %+v, want drained", rep)
	}
}
