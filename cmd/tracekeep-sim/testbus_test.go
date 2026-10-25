package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// trafficBound is how long a Traffic call may take, the bus up or down: the device never waits on the bus.
const trafficBound = time.Second

// startBus runs an embedded server with the traffic stream and the claim bucket as tracekeep migrate leaves them.
func startBus(t *testing.T) (*testbus.Server, jetstream.JetStream) {
	t.Helper()
	srv := testbus.StartServer(t, testbus.Options{})
	nc, err := nats.Connect(srv.URL(), nats.MaxReconnects(-1), nats.ReconnectWait(10*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if _, err := js.CreateStream(ctx, busconfig.StreamConfig(busconfig.DefaultTrafficStream, "tpk")); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if _, err := js.CreateObjectStore(ctx, busconfig.ObjectStoreConfig("tpk-objects")); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	s, err := js.Stream(ctx, busconfig.ClaimStreamName("tpk-objects"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := s.CachedInfo().Config
	cfg.AllowDirect = false
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}

	return srv, js
}

// The traffic bus goes down and comes back while the tool runs:
// forwarding never slows, and what the stream holds afterwards is a consistent capture with its T3 events.
func TestBusDownAndUpWhileTheToolRuns(t *testing.T) {
	t.Parallel()
	srv, js := startBus(t)
	l := newTestLoopback(t, producer.Config{Servers: []string{srv.URL()}})
	if err := l.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	traffic := func(n int) {
		t.Helper()
		start := time.Now()
		if err := l.Traffic(t.Context(), n); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took > trafficBound {
			t.Errorf("Traffic(%d) took %v", n, took)
		}
	}

	traffic(50)
	srv.Stop()
	traffic(50)
	srv.Restart()
	traffic(50)
	time.Sleep(100 * time.Millisecond) // the tool idles past T3, so the withheld requests time out before the device stops
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	l.Stop(ctx)

	msgs := readStream(t, js)
	if err := producertest.CheckCaptures(msgs); err != nil {
		t.Fatal(err)
	}
	var t3, stops int
	for i := range msgs {
		br := decode(t, &msgs[i])
		if br.Record.Kind != tracepack.KindTransportEvent {
			continue
		}
		ev, err := tracepack.UnmarshalTransportEvent(br.Record.Payload)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case ev.Event == tracepack.EventTimerExpiry && ev.Timer != nil && *ev.Timer == tracepack.TimerT3:
			t3++
		case ev.Event == tracepack.EventCaptureBoundary && *ev.BoundaryKind == tracepack.BoundaryKindStop:
			stops++
		default:
		}
	}
	if t3 == 0 || stops != 1 {
		t.Errorf("%d T3 events and %d stops on the stream; want some and one", t3, stops)
	}
}

// readStream reads every message of the traffic stream as producer messages, by sequence:
// the stream is a work queue, so no consumer may read it alongside ingest's.
func readStream(t *testing.T, js jetstream.JetStream) []producer.Msg {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, busconfig.DefaultTrafficStream)
	if err != nil {
		t.Fatal(err)
	}
	st := s.CachedInfo().State
	msgs := make([]producer.Msg, 0, st.Msgs)
	for seq := st.FirstSeq; seq <= st.LastSeq && st.Msgs > 0; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		h := make(map[string]string, len(m.Header))
		for k, v := range m.Header {
			h[k] = v[0]
		}
		msgs = append(msgs, producer.Msg{Subject: m.Subject, Header: h, MsgID: h[nats.MsgIdHdr], Data: m.Data})
	}

	return msgs
}
