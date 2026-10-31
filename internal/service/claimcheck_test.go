package service

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	tpbus "github.com/arloliu/go-secs/tracepack/bus"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/sim"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer"
)

// The gate's initial claim-check start at max_payload 4140 (tpbus.MinMaxPayload): on a bus that small every record,
// the start included, travels by claim-check, and ingest still admits the capture and stages its start.
func TestClaimCheckStartAtTheMinimumMaxPayload(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		srv := testbus.StartServer(t, testbus.Options{MaxPayload: tpbus.MinMaxPayload})
		nc, err := nats.Connect(srv.URL())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		e := &Env{Config: load(t, srv.URL()), Log: slog.New(slog.DiscardHandler), Catalog: c, NC: nc, JS: js, Store: store.NewMem(), AllowUnversioned: true, Dev: true}
		e.Config.Ingest.FlushInterval, e.Config.Ingest.MaxWait = 200*time.Millisecond, time.Second
		if err := Migrate(ctx, e); err != nil {
			t.Fatal(err)
		}
		put := &putting{ObjectStore: e.Store, put: make(chan string, 16)}
		e.Store = put

		p, err := producer.New(producer.Config{Servers: []string{srv.URL()}, SubjectPrefix: e.Config.Bus.SubjectPrefix, ClaimBucket: e.Config.Bus.ClaimBucket})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		l, err := sim.NewLoopback(p, sim.Options{ToolID: "SIM-TOOL", DeviceName: "eqp", SessionID: 1, T3: 45 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			l.Stop(sctx)
		})
		run, stop := context.WithCancel(ctx)
		forwarding := make(chan struct{})
		go func() {
			defer close(forwarding)
			sim.Forward(run, l, 50, slog.New(slog.DiscardHandler)) // traffic keeps flushes coming
		}()
		done := make(chan error, 1)
		go func() { done <- Ingest(run, e) }()
		for range 2 { // a later flush's put follows the first flush's registration
			select {
			case <-put.put:
			case err := <-done:
				t.Fatalf("ingest ended before two segments: %v", err)
			}
		}
		stop()
		if err := <-done; err != nil {
			t.Fatalf("ingest = %v", err)
		}
		<-forwarding

		got := registered(t, e)
		if len(got.captures) != 1 || got.start != 1 {
			t.Fatalf("registered segments %v hold %d captures and %d starts; want the capture's start", got.keys, len(got.captures), got.start)
		}
		claims, err := js.ObjectStore(ctx, e.Config.Bus.ClaimBucket)
		if err != nil {
			t.Fatal(err)
		}
		for id := range got.captures {
			name, _ := tpbus.MsgID(id, 0)
			if _, err := claims.GetInfo(ctx, name); err != nil {
				t.Errorf("the start's claim object %s: %v; want the start sent by claim-check", name, err)
			}
			if cp, err := c.Capture(ctx, id); err != nil || cp.State != catalog.StateOpen {
				t.Errorf("capture %+v, %v; want it admitted", cp, err)
			}
		}
	})
}
