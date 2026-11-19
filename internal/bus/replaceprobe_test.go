package bus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// The probe of the feed stream a group plan's step 6: a server holding group streams of one replica lost with its storage,
// replaced by a server of the same name or of another, both with its tag, on three nats-server processes of the module's version.
// TRACEKEEP_PROBE=feed-replace runs it, each step logging what the production functions answer.
const replaceGroups = 6

type replaceCluster struct {
	t       *testing.T
	dir     string
	bin     string
	cmds    [4]*exec.Cmd
	mapping string
}

func TestFeedReplaceProbe(t *testing.T) {
	if os.Getenv("TRACEKEEP_PROBE") != "feed-replace" {
		t.Skip("TRACEKEEP_PROBE=feed-replace runs the lost server's replacement probe")
	}
	t.Run("same-name", func(t *testing.T) { probeReplace(t, false) })
	t.Run("new-name", func(t *testing.T) { probeReplace(t, true) })
}

func probeReplace(t *testing.T, newName bool) {
	c := startReplaceCluster(t)
	nc, js := c.connect("feed", 0)
	f := feed(replaceGroups, 1)
	f.PlacementTags = []string{"pb-0", "pb-1", "pb-2"}
	ctx := t.Context()
	if err := bus.EnsureFeed(ctx, js, f, 0, trafficStream, parkedStream); err != nil {
		t.Fatal(err)
	}
	// One entry each for 300 equipment, never fetched, so every stream holds some.
	for i := range 300 {
		tok, err := tpbus.EquipmentToken(catalogtest.FleetEquipment(i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, "tklog."+tok, []byte(`{}`), jetstream.WithMsgID(fmt.Sprintf("e-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	lost := map[string]bool{}
	for n := range replaceGroups {
		s, err := js.Stream(ctx, bus.FeedStreamName(f, n))
		if err != nil {
			t.Fatal(err)
		}
		in := s.CachedInfo()
		t.Logf("before: %s on %s, %d messages", in.Config.Name, in.Cluster.Leader, in.State.Msgs)
		if in.Cluster.Leader == "pb-1" {
			lost[in.Config.Name] = true
		}
	}
	lostTok := ""
	for i := 0; lostTok == ""; i++ {
		tok, _ := tpbus.EquipmentToken(catalogtest.FleetEquipment(i))
		if a, err := js.Publish(ctx, "tklog."+tok, []byte(`{}`), jetstream.WithMsgID(fmt.Sprintf("e-%d", i))); err == nil && lost[a.Stream] {
			lostTok = tok
		}
	}

	c.kill(1)
	if err := os.RemoveAll(filepath.Join(c.dir, "pb-1")); err != nil {
		t.Fatal(err)
	}
	t.Log("pb-1 killed, its storage removed")
	time.Sleep(3 * time.Second)
	c.observe("outage", nc, js, f, lostTok)

	if newName {
		c.write(3, "pb-3", "pb-1", 1)
		c.start(3)
		c.awaitHealthy(1)
		t.Log("pb-3 started on pb-1's ports with its tag, empty storage")
	} else {
		c.start(1)
		c.awaitHealthy(1)
		t.Log("pb-1 restarted, empty storage")
	}
	time.Sleep(10 * time.Second)
	c.observe("replaced", nc, js, f, lostTok)
	if !newName {
		return
	}

	// The old peer's removal, as an operator runs `nats server raft peer-remove pb-1`.
	snc, _ := c.connect("sys", 0)
	req, _ := json.Marshal(map[string]string{"peer": "pb-1"})
	r, err := snc.Request("$JS.API.SERVER.REMOVE", req, 10*time.Second)
	if err != nil {
		t.Logf("peer-remove pb-1: %v", err)
	} else {
		t.Logf("peer-remove pb-1: %s", r.Data)
	}
	time.Sleep(10 * time.Second)
	c.observe("peer removed", nc, js, f, lostTok)

	for name := range lost {
		dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := js.DeleteStream(dctx, name)
		cancel()
		t.Logf("delete %s: %v", name, err)
	}
	time.Sleep(3 * time.Second)
	c.observe("lost streams deleted", nc, js, f, lostTok)
}

// observe logs what each production function answers, and a publish to a lost group.
func (c *replaceCluster) observe(when string, nc *nats.Conn, js jetstream.JetStream, f bus.Feed, lostTok string) {
	t := c.t
	ctx := t.Context()
	bounded := func(d time.Duration) (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, d) }
	t.Logf("== %s", when)
	cctx, cancel := bounded(30 * time.Second)
	for _, st := range bus.FeedStates(cctx, js, f) {
		t.Logf("%s: state %s msgs %d err %v", when, st.Stream, st.Messages, st.Err)
	}
	cancel()
	for n := range f.Groups {
		cctx, cancel := bounded(10 * time.Second)
		s, err := js.Stream(cctx, bus.FeedStreamName(f, n))
		if err == nil {
			in := s.CachedInfo()
			_, cerr := s.Consumer(cctx, bus.FeedConsumer(n))
			t.Logf("%s: info %s leader %q replicas %v msgs %d consumer err %v", when, in.Config.Name, in.Cluster.Leader, peers(in), in.State.Msgs, cerr)
		}
		cancel()
	}
	start := time.Now()
	cctx, cancel = bounded(60 * time.Second)
	t.Logf("%s: CheckFeedStreams = %v (%s)", when, bus.CheckFeedStreams(cctx, js, f, trafficStream, parkedStream), time.Since(start).Round(time.Millisecond))
	cancel()
	start = time.Now()
	cctx, cancel = bounded(60 * time.Second)
	t.Logf("%s: ProbeFeedMapping = %v (%s)", when, bus.ProbeFeedMapping(cctx, nc, js, f), time.Since(start).Round(time.Millisecond))
	cancel()
	start = time.Now()
	cctx, cancel = bounded(90 * time.Second)
	t.Logf("%s: EnsureFeed = %v (%s)", when, bus.EnsureFeed(cctx, js, f, f.Groups, trafficStream, parkedStream), time.Since(start).Round(time.Millisecond))
	cancel()
	cctx, cancel = bounded(5 * time.Second)
	a, err := js.Publish(cctx, "tklog."+lostTok, []byte(`{}`), jetstream.WithMsgID(fmt.Sprintf("%s-%d", when, time.Now().UnixNano())))
	cancel()
	if err != nil {
		t.Logf("%s: publish to the lost group: %v", when, err)
	} else {
		t.Logf("%s: publish to the lost group: %s seq %d", when, a.Stream, a.Sequence)
	}
}

func peers(in *jetstream.StreamInfo) []string {
	var p []string
	if in.Cluster != nil {
		p = append(p, in.Cluster.Leader)
		for _, r := range in.Cluster.Replicas {
			p = append(p, fmt.Sprintf("%s(current=%v,offline=%v)", r.Name, r.Current, r.Offline))
		}
	}

	return p
}

func startReplaceCluster(t *testing.T) *replaceCluster {
	t.Helper()
	c := &replaceCluster{t: t, dir: t.TempDir(), mapping: fmt.Sprintf(`"tklog.*": "tklog.{{partition(%d,1)}}.{{wildcard(1)}}"`, replaceGroups)}
	c.bin = filepath.Join(c.dir, "nats-server")
	if out, err := exec.Command("go", "build", "-o", c.bin, "github.com/nats-io/nats-server/v2").CombinedOutput(); err != nil {
		t.Fatalf("build nats-server: %v\n%s", err, out)
	}
	for i := range 3 {
		c.write(i, fmt.Sprintf("pb-%d", i), fmt.Sprintf("pb-%d", i), i)
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.cmds {
			c.kill(i)
		}
	})
	c.awaitHealthy(0, 1, 2)

	return c
}

// write is configuration i: a server named name, tagged tag, on port slot slot's ports, its storage under its name.
func (c *replaceCluster) write(i int, name, tag string, slot int) {
	conf := fmt.Sprintf(`server_name: %[1]s
listen: "127.0.0.1:433%[3]d"
http: "127.0.0.1:824%[3]d"
server_tags: [%[2]q]
jetstream { store_dir: %[4]q }
cluster { name: probe, listen: "127.0.0.1:633%[3]d", routes: ["nats-route://127.0.0.1:6330", "nats-route://127.0.0.1:6331", "nats-route://127.0.0.1:6332"] }
accounts {
  FEED { jetstream: enabled, users: [{user: feed, password: feed}], mappings { %[5]s } }
  SYS { users: [{user: sys, password: sys}] }
}
system_account: SYS
`, name, tag, slot, filepath.Join(c.dir, name), c.mapping)
	if err := os.WriteFile(c.conf(i), []byte(conf), 0o600); err != nil {
		c.t.Fatal(err)
	}
}

func (c *replaceCluster) conf(i int) string { return filepath.Join(c.dir, fmt.Sprintf("s-%d.conf", i)) }

func (c *replaceCluster) start(i int) {
	cmd := exec.Command(c.bin, "-c", c.conf(i))
	log, err := os.Create(filepath.Join(c.dir, fmt.Sprintf("s-%d.log", i)))
	if err != nil {
		c.t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.cmds[i] = cmd
}

func (c *replaceCluster) kill(i int) {
	if cmd := c.cmds[i]; cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		c.cmds[i] = nil
	}
}

// awaitHealthy waits for the servers on port slots slots.
func (c *replaceCluster) awaitHealthy(slots ...int) {
	deadline := time.Now().Add(60 * time.Second)
	for _, i := range slots {
		for {
			r, err := http.Get(fmt.Sprintf("http://127.0.0.1:824%d/healthz", i))
			if err == nil {
				_ = r.Body.Close()
				if r.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				c.t.Logf("server on slot %d not healthy within 60 s", i)

				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func (c *replaceCluster) connect(user string, slot int) (*nats.Conn, jetstream.JetStream) {
	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:433%d", slot), nats.UserInfo(user, user), nats.MaxReconnects(-1))
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		c.t.Fatal(err)
	}

	return nc, js
}
