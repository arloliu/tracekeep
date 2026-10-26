package admin_test

import (
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/testbus"
)

var cfg = bus.Config{SubjectPrefix: "tk", ClaimBucket: "tk-claims"}

func setup(t *testing.T, c *catalog.Catalog) (*admin.Admin, jetstream.JetStream, jetstream.Stream) {
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
	traffic, _, err := bus.Ensure(t.Context(), js, cfg)
	if err != nil {
		t.Fatal(err)
	}

	return &admin.Admin{Catalog: c, Bus: js, Config: cfg, Actor: "op"}, js, traffic
}

func identity(capture tracepack.UUID, seq uint64, inc, streamSeq int64) *catalog.Identity {
	return &catalog.Identity{
		Version: catalog.VersionKey{Capture: capture, Seq: seq, Header: [44]byte{byte(seq)}}, Incarnation: inc, StreamSeq: streamSeq, Hour: 1,
	}
}

// capture terminate writes every unresolved version terminated and an order whose cut-off is the traffic stream's last sequence,
// read before the transaction: a publish accepted after that read is new traffic, never covered.
func TestCaptureTerminate(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		a, js, _ := setup(t, c)
		capture := tracepack.UUID{1}
		for range 3 {
			if _, err := js.Publish(ctx, "tk.T1.x", nil); err != nil {
				t.Fatal(err)
			}
		}
		parked := identity(capture, 1, 1, 1)
		if _, err := c.BeginParking(ctx, parked, "unknown-capture", "", 0); err != nil {
			t.Fatal(err)
		}
		res, err := a.CaptureTerminate(ctx, capture)
		if err != nil || res.Terminated != 1 || res.ActionID == "" {
			t.Fatalf("CaptureTerminate = %+v, %v", res, err)
		}
		show, err := a.CaptureShow(ctx, capture)
		if err != nil || len(show.Orders) != 1 || show.Orders[0].CutOff != 3 || show.Orders[0].ActionID != res.ActionID || show.Orders[0].State != "active" {
			t.Fatalf("CaptureShow = %+v, %v; want the order cut off at the last sequence 3", show, err)
		}
		if len(show.Versions) != 1 || show.Versions[0].State != catalog.VersionTerminated || show.Capture.State != catalog.StatePending {
			t.Errorf("CaptureShow = %+v; want the version terminated, the placeholder kept", show)
		}
		inc := show.Orders[0].Incarnation
		ack, err := js.Publish(ctx, "tk.T1.x", nil) // accepted after the cut-off's read
		if err != nil {
			t.Fatal(err)
		}
		for seq, want := range map[int64]catalog.Resolution{3: catalog.CoveredByOrder, int64(ack.Sequence): catalog.Unresolved} { //nolint:gosec // a stream sequence fits
			r, err := c.Resolve(ctx, &identity(capture, 9, inc, seq).Version, inc, seq)
			if err != nil || r != want {
				t.Errorf("a message at stream seq %d = %v, %v; want %v", seq, r, err, want)
			}
		}
	})
}

// parked list shows each version with its messages; parked terminate ends the named ones under one action.
func TestParked(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		a, _, _ := setup(t, c)
		capture := tracepack.UUID{2}
		for seq := uint64(1); seq <= 2; seq++ {
			if _, err := c.BeginParking(ctx, identity(capture, seq, 1, int64(seq)), "unknown-capture", "", 0); err != nil {
				t.Fatal(err)
			}
		}
		vs, err := a.ParkedList(ctx, &capture)
		if err != nil || len(vs) != 2 || len(vs[0].Copies) != 1 {
			t.Fatalf("ParkedList = %+v, %v", vs, err)
		}
		seq := uint64(2)
		res, err := a.ParkedTerminate(ctx, capture, &seq)
		if err != nil || res.Terminated != 1 {
			t.Fatalf("ParkedTerminate = %+v, %v", res, err)
		}
		vs, _ = a.ParkedList(ctx, &capture)
		if vs[0].State != catalog.VersionParking || vs[1].State != catalog.VersionTerminated || vs[1].TerminatedBy != res.ActionID {
			t.Errorf("versions %+v; want seq 2 terminated under %s", vs, res.ActionID)
		}
	})
}

// bus adopt-stream is refused while a lease is live, and otherwise adopts the stream under a new incarnation
// and deletes every consumer of the service the stream still has.
func TestAdoptStream(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		a, _, traffic := setup(t, c)
		old, err := bus.Incarnate(ctx, c, catalog.RoleTraffic, traffic)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Consumer(ctx, traffic, old, bus.ConsumerConfig{AckWait: time.Minute, MaxAckPending: 10}); err != nil {
			t.Fatal(err)
		}
		cfgID, err := c.ActivateConfig(ctx, []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Register(ctx, "r", []int64{cfgID}, time.Minute); err != nil {
			t.Fatal(err)
		}
		consumers := func() []string {
			var names []string
			for n := range traffic.ConsumerNames(ctx).Name() {
				names = append(names, n)
			}
			return names
		}
		if _, err := a.AdoptStream(ctx, catalog.RoleTraffic); !errors.Is(err, catalog.ErrLeaseLive) {
			t.Fatalf("AdoptStream with a live lease = %v", err)
		}
		if got := consumers(); !slices.Equal(got, []string{"tk-ingest-1"}) {
			t.Fatalf("consumers %v after a refused adoption, want the old one untouched", got)
		}
		if err := c.Leave(ctx, "r"); err != nil {
			t.Fatal(err)
		}
		inc, err := a.AdoptStream(ctx, catalog.RoleTraffic)
		if err != nil || inc.ID == old.ID {
			t.Fatalf("AdoptStream = %+v, %v", inc, err)
		}
		if got := consumers(); len(got) != 0 {
			t.Errorf("consumers %v after the adoption, want none", got)
		}
		if again, err := bus.Incarnate(ctx, c, catalog.RoleTraffic, traffic); err != nil || again.ID != inc.ID {
			t.Errorf("the stream answers as %+v, %v; want the adopted incarnation", again, err)
		}
	})
}

// gate forwards a client's bytes to the server while open; closed, it holds them, as a network delays a publish already sent.
type gate struct {
	mu     sync.Mutex
	open   bool
	opened chan struct{}
	// holding is closed once the closed gate holds bytes.
	holding chan struct{}
	once    sync.Once
}

func (g *gate) set(open bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if open && !g.open {
		close(g.opened)
	} else if !open && g.open {
		g.opened = make(chan struct{})
	}
	g.open = open
}

func (g *gate) wait() {
	g.mu.Lock()
	ch, open := g.opened, g.open
	g.mu.Unlock()
	if !open {
		g.once.Do(func() { close(g.holding) })
		<-ch
	}
}

// proxy listens for one client and relays it to the server at url, its client-to-server bytes through g.
func proxy(t *testing.T, url string, g *gate) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	server := strings.TrimPrefix(url, "nats://")
	go func() {
		client, err := l.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", server)
		if err != nil {
			_ = client.Close()
			return
		}
		go func() { _, _ = io.Copy(client, up) }()
		buf := make([]byte, 32<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				g.wait()
				if _, err := up.Write(buf[:n]); err != nil {
					return
				}
			}
			if err != nil {
				_ = up.Close()
				return
			}
		}
	}()

	return "nats://" + l.Addr().String()
}

// Checklist 4: a publish sent before capture terminate's fresh Info and accepted after it carries a sequence above the cut-off,
// so the order covers it not, and it is handled as new traffic.
func TestPublishAcceptedAfterTheCutOffRead(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		srv := testbus.StartServer(t, testbus.Options{})
		nc, err := nats.Connect(srv.URL())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := bus.Ensure(ctx, js, cfg); err != nil {
			t.Fatal(err)
		}
		a := &admin.Admin{Catalog: c, Bus: js, Config: cfg, Actor: "op"}
		if _, err := js.Publish(ctx, "tk.T1.x", nil); err != nil {
			t.Fatal(err)
		}

		g := &gate{open: true, opened: make(chan struct{}), holding: make(chan struct{})}
		close(g.opened)
		late, err := nats.Connect(proxy(t, srv.URL(), g))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(late.Close)
		ljs, err := jetstream.New(late)
		if err != nil {
			t.Fatal(err)
		}
		g.set(false)
		acked := make(chan *jetstream.PubAck, 1)
		go func() {
			ack, err := ljs.Publish(ctx, "tk.T1.x", nil) // sent now, held on its way to the server
			if err != nil {
				t.Error(err)
			}
			acked <- ack
		}()
		<-g.holding // the publish has left the client
		capture := tracepack.UUID{3}
		res, err := a.CaptureTerminate(ctx, capture)
		if err != nil {
			t.Fatal(err)
		}
		g.set(true)
		ack := <-acked
		show, err := a.CaptureShow(ctx, capture)
		if err != nil || len(show.Orders) != 1 || show.Orders[0].ActionID != res.ActionID {
			t.Fatalf("CaptureShow = %+v, %v", show, err)
		}
		o := show.Orders[0]
		if ack == nil || int64(ack.Sequence) <= o.CutOff { //nolint:gosec // a stream sequence fits
			t.Fatalf("the held publish was accepted at %+v, at or below the cut-off %d", ack, o.CutOff)
		}
		seq := int64(ack.Sequence) //nolint:gosec // a stream sequence fits
		if r, err := c.Resolve(ctx, &identity(capture, 9, o.Incarnation, seq).Version, o.Incarnation, seq); err != nil || r != catalog.Unresolved {
			t.Errorf("the held publish's message = %v, %v; want new traffic", r, err)
		}
	})
}
