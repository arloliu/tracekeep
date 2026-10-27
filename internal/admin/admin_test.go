package admin_test

import (
	"bytes"
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

// gate holds a client's bytes on their way to the server while closed, as a network delays a publish already sent;
// the client's bytes keep being read into it, so a frame split across reads, or behind another frame, is still seen.
type gate struct {
	mu   sync.Mutex
	cond *sync.Cond
	open bool
	// buf holds the bytes read from the client and not yet forwarded; done reports the client gone.
	buf  []byte
	done bool
	// holding is closed once the closed gate holds a publish (PUB or HPUB) frame.
	holding chan struct{}
	once    sync.Once
}

func newGate() *gate {
	g := &gate{open: true, holding: make(chan struct{})}
	g.cond = sync.NewCond(&g.mu)

	return g
}

func (g *gate) set(open bool) {
	g.mu.Lock()
	g.open = open
	g.mu.Unlock()
	g.cond.Broadcast()
}

// push takes bytes read from the client.
func (g *gate) push(b []byte) {
	g.mu.Lock()
	g.buf = append(g.buf, b...)
	if !g.open && bytes.Contains(g.buf, []byte("PUB ")) {
		g.once.Do(func() { close(g.holding) })
	}
	g.mu.Unlock()
	g.cond.Broadcast()
}

// end marks the client gone.
func (g *gate) end() {
	g.mu.Lock()
	g.done = true
	g.mu.Unlock()
	g.cond.Broadcast()
}

// pop returns the bytes to forward once the gate is open, in order; ok is false once the client is gone and nothing remains.
func (g *gate) pop() (b []byte, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for (!g.open || len(g.buf) == 0) && !g.done {
		g.cond.Wait()
	}
	if !g.open || len(g.buf) == 0 {
		return nil, false
	}
	b, g.buf = g.buf, nil

	return b, true
}

func held(g *gate) bool {
	select {
	case <-g.holding:
		return true
	default:
		return false
	}
}

// The gate signals only once the bytes it holds include a publish frame, however the reads split them,
// and forwards everything in order once opened.
func TestGateWaitsForThePublishFrame(t *testing.T) {
	t.Parallel()
	g := newGate()
	g.set(false)
	for _, chunk := range []string{"SUB _INBOX.a 1\r\n", "PU", "B tk.T1.x _INBOX.a.1 0\r\n\r\n"} {
		if held(g) {
			t.Fatalf("holding before %q", chunk)
		}
		g.push([]byte(chunk))
	}
	if !held(g) {
		t.Fatal("not holding once the publish frame arrived")
	}
	got := make(chan []byte)
	go func() {
		b, _ := g.pop()
		got <- b
	}()
	g.set(true)
	if b := <-got; string(b) != "SUB _INBOX.a 1\r\nPUB tk.T1.x _INBOX.a.1 0\r\n\r\n" {
		t.Errorf("forwarded %q, want every held byte in order", b)
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
		go func() {
			defer g.end()
			buf := make([]byte, 32<<10)
			for {
				n, err := client.Read(buf)
				if n > 0 {
					g.push(buf[:n])
				}
				if err != nil {
					return
				}
			}
		}()
		for {
			b, ok := g.pop()
			if !ok {
				_ = up.Close()
				return
			}
			if _, err := up.Write(b); err != nil {
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

		g := newGate()
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
		<-g.holding // the publish has left the client and waits on its way
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

// survivorAndStray leaves c a survivor, tk-claims/x.6 of hour 1, 70 bytes, and an unattributed object, tk-claims/upload.bin.
func survivorAndStray(t *testing.T, c *catalog.Catalog) {
	t.Helper()
	ctx := t.Context()
	lease, ok, err := c.TakeLease(ctx, "compactor", "w", time.Hour)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}
	if _, err := c.ActivateConfig(ctx, []byte("config")); err != nil {
		t.Fatal(err)
	}
	capture := tracepack.UUID{3}
	d := &catalog.Descriptor{Bytes: []byte("d"), ToolID: "T1", RecorderInstanceID: tracepack.UUID{4}, Device: "eqp", OriginNS: 1}
	if _, err := c.Admit(ctx, identity(capture, 0, 1, 1), d); err != nil {
		t.Fatal(err)
	}
	parked := identity(capture, 6, 1, 6)
	if _, err := c.BeginParking(ctx, parked, "claim-conflict", "tk-claims/x.6", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdvanceParked(ctx, &parked.Version); err != nil {
		t.Fatal(err)
	}
	objs := []catalog.Listed{
		{Key: "tk-claims/x.6", Size: 70, Attr: &catalog.ClaimObject{Capture: capture, Seq: 6, Hour: 1}},
		{Key: "tk-claims/upload.bin", Size: 3},
	}
	if _, err := c.RecordListing(ctx, lease, objs, 5); err != nil {
		t.Fatal(err)
	}
	now := int64(3 * time.Hour)
	if _, err := c.RecordRetention(ctx, lease, time.Hour, time.Hour, now); err != nil {
		t.Fatal(err)
	}
	if ev, err := c.EvictHour(ctx, lease, catalog.Scope{Capture: capture, Hour: 1}, now); err != nil || ev.Survivors != 1 {
		t.Fatalf("EvictHour = %+v, %v", ev, err)
	}
}

// object list shows the survivors an eviction kept, with their evicted hour and size, and the unattributed objects.
func TestObjectList(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		a, _, _ := setup(t, c)
		survivorAndStray(t, c)
		got, err := a.ObjectList(ctx, true, false)
		if err != nil || len(got.Survivors) != 1 || len(got.Unattributed) != 0 {
			t.Fatalf("object list --survivors = %+v, %v", got, err)
		}
		if s := got.Survivors[0]; s.Key != "tk-claims/x.6" || s.Size != 70 || s.EvictedHour == nil || *s.EvictedHour != 1 {
			t.Errorf("survivor = %+v, want its key, size and evicted hour", s)
		}
		got, err = a.ObjectList(ctx, false, true)
		if err != nil || len(got.Survivors) != 0 || len(got.Unattributed) != 1 || got.Unattributed[0].Key != "tk-claims/upload.bin" {
			t.Errorf("object list --unattributed = %+v, %v", got, err)
		}
		if got, err = a.ObjectList(ctx, false, false); err != nil || len(got.Survivors) != 1 || len(got.Unattributed) != 1 {
			t.Errorf("object list = %+v, %v, want both", got, err)
		}
	})
}
