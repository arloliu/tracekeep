package bus_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/testbus"
)

// The probes of the feed stream a group plan's step 1, on three nats-server processes of the module's version:
// TRACEKEEP_PROBE=feed-groups runs them, each logging its outcome.
const (
	probeGroups    = 32
	probeEquipment = 2000
)

// probeCluster is three nats-server processes, each tagged with its name, in a configuration-file account FEED
// whose mapping partitions the feed's subjects, beside an account REF without one.
type probeCluster struct {
	t    *testing.T
	dir  string
	bin  string
	cmds [3]*exec.Cmd
}

func TestFeedGroupProbes(t *testing.T) {
	if os.Getenv("TRACEKEEP_PROBE") != "feed-groups" {
		t.Skip("TRACEKEEP_PROBE=feed-groups runs the feed stream a group probes")
	}
	c := startProbeCluster(t)
	feedJS := c.connect("feed", 0)
	refJS := c.connect("ref", 0)

	t.Run("placement", func(t *testing.T) { probePlacement(t, feedJS) })
	ref := t.Run("mapping", func(t *testing.T) { probeMapping(t, feedJS, refJS) })
	if !ref {
		t.Fatal("the mapping failed: the later probes rest on it")
	}
	t.Run("server-lost", func(t *testing.T) { probeServerLost(t, c, feedJS) })
	t.Run("limit", func(t *testing.T) { probeLimit(t, feedJS) })
}

func startProbeCluster(t *testing.T) *probeCluster {
	t.Helper()
	c := &probeCluster{t: t, dir: t.TempDir()}
	c.bin = filepath.Join(c.dir, "nats-server")
	if out, err := exec.Command("go", "build", "-o", c.bin, "github.com/nats-io/nats-server/v2").CombinedOutput(); err != nil {
		t.Fatalf("build nats-server: %v\n%s", err, out)
	}
	mapping := fmt.Sprintf(`"tklog.*": "tklog.{{partition(%d,1)}}.{{wildcard(1)}}"`, probeGroups)
	for i := range 3 {
		conf := fmt.Sprintf(`server_name: pb-%[1]d
listen: "127.0.0.1:432%[1]d"
http: "127.0.0.1:823%[1]d"
server_tags: ["pb-%[1]d"]
jetstream { store_dir: %[2]q }
cluster { name: probe, listen: "127.0.0.1:632%[1]d", routes: ["nats-route://127.0.0.1:6320", "nats-route://127.0.0.1:6321", "nats-route://127.0.0.1:6322"] }
accounts {
  FEED { jetstream: enabled, users: [{user: feed, password: feed}], mappings { %[3]s } }
  REF { jetstream: enabled, users: [{user: ref, password: ref}] }
  SYS { users: [{user: sys, password: sys}] }
}
system_account: SYS
`, i, filepath.Join(c.dir, fmt.Sprintf("pb-%d", i)), mapping)
		if err := os.WriteFile(c.conf(i), []byte(conf), 0o600); err != nil {
			t.Fatal(err)
		}
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range 3 {
			c.kill(i)
		}
	})
	c.awaitHealthy(0, 1, 2)

	return c
}

func (c *probeCluster) conf(i int) string { return filepath.Join(c.dir, fmt.Sprintf("pb-%d.conf", i)) }

func (c *probeCluster) start(i int) {
	cmd := exec.Command(c.bin, "-c", c.conf(i))
	log, err := os.Create(filepath.Join(c.dir, fmt.Sprintf("pb-%d.log", i)))
	if err != nil {
		c.t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.cmds[i] = cmd
}

// kill stops server i at once, as a lost node does.
func (c *probeCluster) kill(i int) {
	if cmd := c.cmds[i]; cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		c.cmds[i] = nil
	}
}

func (c *probeCluster) awaitHealthy(servers ...int) {
	deadline := time.Now().Add(60 * time.Second)
	for _, i := range servers {
		for {
			r, err := http.Get(fmt.Sprintf("http://127.0.0.1:823%d/healthz", i))
			if err == nil {
				_ = r.Body.Close()
				if r.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				c.t.Fatalf("server pb-%d not healthy", i)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func (c *probeCluster) connect(user string, server int) jetstream.JetStream {
	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:432%d", server), nats.UserInfo(user, user), nats.MaxReconnects(-1))
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		c.t.Fatal(err)
	}

	return js
}

func groupStream(n int) string { return fmt.Sprintf("TRACEKEEP_LOGFEED_%d", n) }

func groupStreamConfig(n int) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: groupStream(n), Subjects: []string{fmt.Sprintf("tklog.%d.*", n)}, Retention: jetstream.WorkQueuePolicy,
		Storage: jetstream.FileStorage, Compression: jetstream.NoCompression, MaxBytes: 1 << 30, MaxMsgs: -1, MaxMsgsPerSubject: -1,
		Discard: jetstream.DiscardNew, Duplicates: 2 * time.Minute, Replicas: 1,
	}
}

func token(t *testing.T, i int) string {
	t.Helper()
	tok, err := tpbus.EquipmentToken(catalogtest.FleetEquipment(i))
	if err != nil {
		t.Fatal(err)
	}

	return tok
}

// leaders counts the group streams by the server leading them.
func leaders(t *testing.T, js jetstream.JetStream) map[string]int {
	t.Helper()
	count := map[string]int{}
	for n := range probeGroups {
		s, err := js.Stream(t.Context(), groupStream(n))
		if err != nil {
			t.Fatal(err)
		}
		info := s.CachedInfo()
		leader := "?"
		if info.Cluster != nil {
			leader = info.Cluster.Leader
		}
		count[leader]++
	}

	return count
}

func createGroups(t *testing.T, js jetstream.JetStream, tagged bool) {
	t.Helper()
	var slowest time.Duration
	for n := range probeGroups {
		cfg := groupStreamConfig(n)
		if tagged {
			cfg.Placement = &jetstream.Placement{Tags: []string{fmt.Sprintf("pb-%d", n%3)}}
		}
		start := time.Now()
		// A cluster just formed may leave a creation unanswered (testbus.Retry); the creation is idempotent.
		var s jetstream.Stream
		testbus.Retry(t, func(ctx context.Context) error {
			var err error
			s, err = js.CreateStream(ctx, cfg)

			return err
		})
		testbus.AwaitStream(t, js, s)
		slowest = max(slowest, time.Since(start))
	}
	t.Logf("32 streams created (tagged %t), the slowest ready in %s", tagged, slowest.Round(time.Millisecond))
}

func deleteGroups(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	for n := range probeGroups {
		if err := js.DeleteStream(t.Context(), groupStream(n)); err != nil {
			t.Fatal(err)
		}
	}
}

// probePlacement counts where 32 streams of one replica land unplaced, in three rounds, and then tagged a server each.
func probePlacement(t *testing.T, js jetstream.JetStream) {
	// Tagged first, on a cluster that never held the names, then unplaced, then tagged again in place of streams just deleted.
	createGroups(t, js, true)
	t.Logf("tagged on a fresh cluster: leaders %v", leaders(t, js))
	deleteGroups(t, js)
	for round := range 3 {
		createGroups(t, js, false)
		t.Logf("unplaced, round %d: leaders %v", round, leaders(t, js))
		deleteGroups(t, js)
	}
	createGroups(t, js, true)
	got := leaders(t, js)
	t.Logf("tagged pb-(n mod 3): leaders %v", got)
	if got["pb-0"] != 11 || got["pb-1"] != 11 || got["pb-2"] != 10 {
		t.Errorf("tagged placement %v, want 11, 11 and 10", got)
	}
}

// probeMapping publishes each fleet token to FEED, whose mapping routes it to a group stream, and to REF's one stream partitioned by the transform
// migrate gives the feed stream today (bus.FeedStreamConfig), and compares the groups; then retries every FEED publish under its message id.
func probeMapping(t *testing.T, feedJS, refJS jetstream.JetStream) {
	ctx := t.Context()
	ref := bus.FeedStreamConfig(bus.Feed{Stream: "REF", Prefix: "tklog", Groups: probeGroups, MaxBytes: 1 << 30, Duplicates: 2 * time.Minute, Replicas: 1})
	rs, err := refJS.CreateStream(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]int, probeEquipment)
	for i := range probeEquipment {
		ack, err := refJS.Publish(ctx, "tklog."+token(t, i), []byte("e"))
		if err != nil {
			t.Fatal(err)
		}
		m, err := rs.GetMsg(ctx, ack.Sequence)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Sscanf(m.Subject, "tklog.%d.", &want[i]); err != nil {
			t.Fatalf("transformed subject %q: %v", m.Subject, err)
		}
	}
	perGroup := make([]int, probeGroups)
	mismatched := 0
	for i := range probeEquipment {
		id := fmt.Sprintf("probe/%s/0/0", token(t, i))
		ack, err := feedJS.Publish(ctx, "tklog."+token(t, i), []byte("e"), jetstream.WithMsgID(id))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if ack.Stream != groupStream(want[i]) {
			mismatched++
		}
		perGroup[want[i]]++
		dup, err := feedJS.Publish(ctx, "tklog."+token(t, i), []byte("e"), jetstream.WithMsgID(id))
		if err != nil || !dup.Duplicate || dup.Sequence != ack.Sequence {
			t.Errorf("retry of %d: %+v, %v; want a duplicate of %d", i, dup, err, ack.Sequence)
		}
	}
	t.Logf("%d tokens: %d in another group than the stream transform's", probeEquipment, mismatched)
	if mismatched > 0 {
		t.Errorf("%d tokens mapped to another group", mismatched)
	}
	t.Logf("equipment a group: min %d, max %d, mean %.1f", slices.Min(perGroup), slices.Max(perGroup), float64(probeEquipment)/probeGroups)
	var stored uint64
	for n := range probeGroups {
		s, err := feedJS.Stream(ctx, groupStream(n))
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if int(info.State.Msgs) != perGroup[n] {
			t.Errorf("stream %d holds %d, want %d", n, info.State.Msgs, perGroup[n])
		}
		stored += info.State.Msgs
		if err := s.Purge(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("stored %d of %d, each once", stored, probeEquipment)
	if err := refJS.DeleteStream(ctx, "REF"); err != nil {
		t.Fatal(err)
	}
}

// probeServerLost kills the server holding one group's stream with acknowledged and unacknowledged entries in it,
// publishes to that group and another meanwhile, restarts it, and checks the stream, its consumer and its entries.
func probeServerLost(t *testing.T, c *probeCluster, js jetstream.JetStream) {
	ctx := t.Context()
	f := bus.Feed{Prefix: "tklog", AckWait: 7 * time.Minute, MaxAckPending: 100_000}
	// A group on pb-1, the server killed, and one on pb-0, the clients' server.
	lostTok, lost := findToken(t, js, func(n int) bool { return n%3 == 1 })
	liveTok, live := findToken(t, js, func(n int) bool { return n%3 == 0 })
	for _, n := range []int{lost, live} {
		s, err := js.Stream(ctx, groupStream(n))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Purge(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CreateOrUpdateConsumer(ctx, bus.FeedConsumerConfig(f, n)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 10 {
		if _, err := js.Publish(ctx, "tklog."+lostTok, []byte("e"), jetstream.WithMsgID(fmt.Sprintf("before/%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	cons, err := js.Consumer(ctx, groupStream(lost), bus.FeedConsumer(lost))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cons.Fetch(10, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for m := range batch.Messages() {
		if got < 4 {
			if err := m.DoubleAck(ctx); err != nil {
				t.Fatal(err)
			}
		}
		got++
	}
	t.Logf("group %d on pb-1: 10 published, %d delivered, 4 acknowledged, 6 held unacknowledged", lost, got)

	c.kill(1)
	killed := time.Now()
	probeOutage(t, js, cons, lost, live, lostTok, liveTok, killed)
	down := time.Since(killed)
	c.start(1)
	restarted := time.Now()
	c.awaitHealthy(1)
	t.Logf("pb-1 down %s, healthy %s after its restart", down.Round(time.Second), time.Since(restarted).Round(time.Millisecond))
	awaitPublish(t, js, lost, lostTok, restarted)
	t.Logf("group %d takes a publish %s after its server's restart", lost, time.Since(restarted).Round(time.Millisecond))
	s, err := js.Stream(ctx, groupStream(lost))
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ci, cerr := s.Consumer(ctx, bus.FeedConsumer(lost))
	t.Logf("after the restart: stream holds %d (6 unacknowledged before + the publishes taken), consumer %v", info.State.Msgs, cerr)
	if cerr != nil {
		t.Fatalf("the consumer is gone after its server's restart: %v", cerr)
	}
	cinfo, err := ci.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("consumer: pending %d, ack pending %d, delivered %d, ack floor %d",
		cinfo.NumPending, cinfo.NumAckPending, cinfo.Delivered.Stream, cinfo.AckFloor.Stream)
	var ids []string
	batch, err = ci.Fetch(100, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for m := range batch.Messages() {
		ids = append(ids, m.Headers().Get(jetstream.MsgIDHeader))
		_ = m.Ack()
	}
	t.Logf("fetched after the restart (the held six redelivered at once, without their ack wait): %v", ids)
	for _, want := range []string{"before/4", "before/9", "after"} {
		if !slices.Contains(ids, want) {
			t.Errorf("%s not delivered after the restart", want)
		}
	}
}

// probeLimit fills one group's stream to its max_bytes and checks the publishes it refuses, and that another group's still succeed.
func probeLimit(t *testing.T, js jetstream.JetStream) {
	ctx := t.Context()
	tokFull, full := findToken(t, js, func(int) bool { return true })
	tokOther, other := findToken(t, js, func(n int) bool { return n != full })
	cfg := groupStreamConfig(full)
	cfg.Placement = &jetstream.Placement{Tags: []string{fmt.Sprintf("pb-%d", full%3)}}
	cfg.MaxBytes = 64 << 10
	s, err := js.UpdateStream(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	payload := []byte(strings.Repeat("x", 1000))
	var refusal error
	n := 0
	for ; n < 1000; n++ {
		if _, err := js.Publish(ctx, "tklog."+tokFull, payload); err != nil {
			refusal = err
			break
		}
	}
	var apiErr *jetstream.APIError
	code := 0
	if errors.As(refusal, &apiErr) {
		code = int(apiErr.ErrorCode)
	}
	t.Logf("group %d at 64 KiB: %d entries of 1000 B taken, then %v (error code %d)", full, n, refusal, code)
	if refusal == nil {
		t.Fatal("a full group stream took every publish")
	}
	if _, err := js.Publish(ctx, "tklog."+tokOther, payload); err != nil {
		t.Errorf("group %d refused while group %d is full: %v", other, full, err)
	} else {
		t.Logf("group %d takes publishes while group %d is full", other, full)
	}
}

// probeOutage publishes to the lost group and a live one, and fetches and deletes as the stage does, while the lost group's server is down.
func probeOutage(t *testing.T, js jetstream.JetStream, cons jetstream.Consumer, lost, live int, lostTok, liveTok string, killed time.Time) {
	ctx := t.Context()
	for i := range 3 {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		start := time.Now()
		_, err := js.Publish(pctx, "tklog."+lostTok, []byte("e"), jetstream.WithMsgID(fmt.Sprintf("during/%d", i)))
		cancel()
		t.Logf("publish to group %d, its server down %s: after %s, %v (no responders: %t)",
			lost, time.Since(killed).Round(time.Second), time.Since(start).Round(time.Millisecond), err, errors.Is(err, nats.ErrNoResponders))
		pctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		start = time.Now()
		_, err = js.Publish(pctx, "tklog."+liveTok, []byte("e"))
		cancel()
		t.Logf("publish to group %d on pb-0 meanwhile: after %s, %v", live, time.Since(start).Round(time.Millisecond), err)
		if err != nil {
			t.Errorf("a live group's publish failed while another server was down: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
	// As the stage fetches: a pull the server ends at its wait, then the batch's own error.
	start := time.Now()
	fb, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err == nil {
		for range fb.Messages() {
		}
		err = fb.Error()
	}
	t.Logf("the lost group's consumer fetching meanwhile: after %s, %v (no responders: %t)",
		time.Since(start).Round(time.Millisecond), err, errors.Is(err, nats.ErrNoResponders))
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = js.DeleteConsumer(dctx, groupStream(lost), "absent")
	cancel()
	t.Logf("deleting a consumer of the lost group's stream meanwhile, as the stage's start does: %v", err)
	ictx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = js.Stream(ictx, groupStream(lost))
	cancel()
	t.Logf("the lost group's stream info meanwhile: %v", err)
}

// findToken publishes fleet tokens until one lands in a group accept takes, and returns it and its group.
func findToken(t *testing.T, js jetstream.JetStream, accept func(n int) bool) (string, int) {
	t.Helper()
	for i := range probeEquipment {
		ack, err := js.Publish(t.Context(), "tklog."+token(t, i), []byte("e"))
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if _, err := fmt.Sscanf(ack.Stream, "TRACEKEEP_LOGFEED_%d", &n); err != nil {
			t.Fatal(err)
		}
		if accept(n) {
			return token(t, i), n
		}
	}
	t.Fatal("no token lands in an accepted group")

	return "", 0
}

// awaitPublish publishes to group n until its stream takes one, at most 2 min after its server's restart.
func awaitPublish(t *testing.T, js jetstream.JetStream, n int, tok string, restarted time.Time) {
	t.Helper()
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		_, err := js.Publish(ctx, "tklog."+tok, []byte("e"), jetstream.WithMsgID("after"))
		cancel()
		if err == nil {
			return
		}
		if time.Since(restarted) > 2*time.Minute {
			t.Fatalf("group %d takes no publish 2 min after its server's restart: %v", n, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
