package service

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/testbus"
)

// The feed-streams mode runs the owner's fleet on the feed's bus alone, at ingest's own times, its groups holding each equipment's entries
// until its flush as the log stage does, to judge how the stream's layout bears it:
// TRACEKEEP_LOAD_STREAMS is 1, the feed's one partitioned stream, or feedGroups, a stream a group, which the servers' subject mapping
// partitions the publishes into (scripts/load-feed.sh BUS_FEED_STREAMS), so the publishers name no stream;
// TRACEKEEP_LOAD_REPLICAS is the streams' replicas, 1 or 3; TRACEKEEP_LOAD_FLUSHES=spread spreads the flushes over F, aligned otherwise;
// TRACEKEEP_LOAD_SETTLE=purge settles each group by purging below its checkpoint instead of acknowledging each entry (feed.purge).
// Streams of one replica are placed on the servers in turn by their tags, and a replicated stream's leader too.
// It needs TRACEKEEP_LOAD_BUS, servers named tb-0, tb-1 and tb-2 tagged with their names, and writes streams.csv under TRACEKEEP_LOAD_SERIES.
func loadFeedStreams(t *testing.T, smoke bool) {
	url := os.Getenv("TRACEKEEP_LOAD_BUS")
	if url == "" {
		t.Skip("TRACEKEEP_LOAD_BUS not set: the mode needs the bus of scripts/load-feed.sh")
	}
	requireFreshBus(t, url)
	streams, _ := strconv.Atoi(cmp.Or(os.Getenv("TRACEKEEP_LOAD_STREAMS"), "1"))
	replicas, _ := strconv.Atoi(cmp.Or(os.Getenv("TRACEKEEP_LOAD_REPLICAS"), "1"))
	if streams != 1 && streams != feedGroups || replicas != 1 && replicas != 3 {
		t.Fatalf("TRACEKEEP_LOAD_STREAMS is 1 or %d, TRACEKEEP_LOAD_REPLICAS 1 or 3", feedGroups)
	}
	ft := feedModeTimes(smoke, !smoke)
	fs := fleetOf(smoke)
	ft.maxAckPending = cmp.Or(fs.ackPending, ft.maxAckPending)
	f := newFeedStreams(t, url, streams, replicas, ft, os.Getenv("TRACEKEEP_LOAD_SETTLE") == "purge")
	f.aligned = os.Getenv("TRACEKEEP_LOAD_FLUSHES") != "spread"
	f.groupBytes, f.maxHeld = 128<<20, ft.maxAckPending // logfeed.group_max_bytes's default and the fleet's max_ack_pending
	ctx := t.Context()
	f.consume(ctx)
	defer f.stop()
	pool, burstPool := newSizedFeedPool(t, fs.body), newSizedFeedPool(t, fs.burstBody)
	start := time.Now()
	burstAt := start.Add(2*ft.flush + ft.flush/2)
	end := start.Add(4 * ft.flush)
	pubs := publishFleet(ctx, url, fs, pool, burstPool, burstAt, ft.dupWindow*3/4)
	defer pubs.stop()
	dir := cmp.Or(os.Getenv(envSeries), t.TempDir())
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(dir, "streams.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	w := csv.NewWriter(out)
	defer w.Flush()
	_ = w.Write([]string{"unix", "made", "published", "failed", "retried", "stored", "msgs", "consumer_acks", "held"})
	var stored []float64 // stored publishes a second
	var last uint64
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for now := range tick.C {
		var seq, msgs uint64
		for _, s := range f.streams {
			if si, err := s.Info(ctx); err == nil {
				seq += si.State.LastSeq
				msgs += si.State.Msgs
			}
		}
		held := 0
		for _, g := range f.groups {
			held += g.heldNow()
		}
		_ = w.Write([]string{strconv.FormatInt(now.Unix(), 10), strconv.FormatUint(pubs.made.Load(), 10), strconv.FormatUint(pubs.acked.Load(), 10),
			strconv.FormatUint(pubs.failed.Load(), 10), strconv.FormatUint(pubs.retried.Load(), 10), strconv.FormatUint(seq, 10), strconv.FormatUint(msgs, 10),
			strconv.FormatUint(f.acks.Load(), 10), strconv.Itoa(held)})
		w.Flush()
		if last != 0 && seq >= last {
			stored = append(stored, float64(seq-last))
		}
		last = max(last, seq)
		if now.After(end) {
			break
		}
	}
	report := streamsReport(fs, stored, pubs, start, burstAt)
	settle := "acknowledged"
	if f.purge {
		settle = "purged: " + f.purged.String()
	}
	t.Logf("feed-streams: %d stream(s) of %d replica(s), flushes %s, settled %s, placement %s\n%s", streams, replicas,
		map[bool]string{true: "aligned", false: "spread"}[f.aligned], settle, f.placement(t), report)
}

// streamsReport is the run's stored rate before, during and after the burst: its mean and its lowest ten seconds, and the publishes' outcome.
func streamsReport(fs fleetShape, stored []float64, pubs *feedPublishers, start, burstAt time.Time) string {
	window := func(from, to time.Duration) (mean, lowest float64) {
		a, b := int(from.Seconds()), min(int(to.Seconds()), len(stored))
		if a >= b {
			return 0, 0
		}
		lowest = -1
		for i := a; i < b; i++ {
			mean += stored[i] / float64(b-a)
			if i+10 <= b {
				var s float64
				for _, v := range stored[i : i+10] {
					s += v / 10
				}
				if lowest < 0 || s < lowest {
					lowest = s
				}
			}
		}

		return mean, lowest
	}
	b := burstAt.Sub(start)
	pm, pl := window(time.Minute, b)
	bm, bl := window(b, b+fs.burstFor)
	am, al := window(b+fs.burstFor, time.Duration(len(stored))*time.Second)
	lat := pubs.latencies(start, start.Add(time.Duration(len(stored))*time.Second))
	slices.Sort(lat)
	var p99 time.Duration
	if len(lat) > 0 {
		p99 = lat[len(lat)*99/100]
	}

	return fmt.Sprintf("| offered entries/s | stored before the burst: mean, lowest 10 s | during it | after it | made | failed | retried | publish p99 |\n"+
		"| %.0f | %.0f, %.0f | %.0f, %.0f | %.0f, %.0f | %d | %d | %d | %s |",
		fs.rate()*2, pm, pl, bm, bl, am, al, pubs.made.Load(), pubs.failed.Load(), pubs.retried.Load(), p99.Round(time.Millisecond))
}

// newFeedStreams creates the feed's streams on url, each of replicas, and a group consumer a group:
// one stream partitioned into the groups, or a stream a group capturing the subjects the servers' mapping gives it.
func newFeedStreams(t *testing.T, url string, streams, replicas int, ft feedTimes, purge bool) *feed {
	t.Helper()
	ctx := t.Context()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	f := &feed{t: t, ft: ft, name: feedStream, prefix: feedPrefix, nc: nc, js: js, purge: purge}
	for n := range streams {
		cfg := jetstream.StreamConfig{
			Subjects: []string{feedPrefix + ".*"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: replicas,
			MaxBytes: feedMaxBytes / int64(streams), Discard: jetstream.DiscardNew, Duplicates: ft.dupWindow, Name: feedStream,
		}
		if purge {
			cfg.Retention = jetstream.LimitsPolicy
		}
		if streams == 1 {
			cfg.SubjectTransform = &jetstream.SubjectTransformConfig{Source: feedPrefix + ".*",
				Destination: fmt.Sprintf("%s.{{partition(%d,1)}}.{{wildcard(1)}}", feedPrefix, feedGroups)}
		} else {
			cfg.Name, cfg.Subjects = fmt.Sprintf("%s_%d", feedStream, n), []string{fmt.Sprintf("%s.%d.*", feedPrefix, n)}
		}
		server := fmt.Sprintf("tb-%d", n%3)
		if replicas == 1 {
			cfg.Placement = &jetstream.Placement{Tags: []string{server}}
		}
		s, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		testbus.AwaitStream(t, js, s)
		if replicas > 1 {
			stepDown(t, nc, "$JS.API.STREAM.LEADER.STEPDOWN."+cfg.Name, server, func() string {
				si, err := s.Info(ctx)
				if err != nil || si.Cluster == nil {
					return ""
				}
				return si.Cluster.Leader
			})
		}
		f.streams = append(f.streams, s)
	}
	f.stream = f.streams[0]
	for n := range feedGroups {
		s := f.streams[n%len(f.streams)]
		filter := fmt.Sprintf("%s.%d.>", feedPrefix, n)
		c, err := s.CreateOrUpdateConsumer(ctx, f.consumerConfig(n, filter))
		if err != nil {
			t.Fatal(err)
		}
		if replicas > 1 {
			// A group's consumer is led where its stream is.
			name := f.consumerConfig(n, filter).Durable
			stepDown(t, nc, "$JS.API.CONSUMER.LEADER.STEPDOWN."+s.CachedInfo().Config.Name+"."+name, fmt.Sprintf("tb-%d", n%len(f.streams)%3), func() string {
				ci, err := c.Info(ctx)
				if err != nil || ci.Cluster == nil {
					return ""
				}
				return ci.Cluster.Leader
			})
		}
		f.consumers = append(f.consumers, c)
		f.groups = append(f.groups, &feedGroup{n: n, f: f, filter: filter, batches: map[string][]jetstream.Msg{}, due: map[string]time.Time{}})
	}

	return f
}

// stepDown asks the leader of the asset subject names to step down for preferred until leader, the asset's current leader, is preferred.
func stepDown(t *testing.T, nc *nats.Conn, subject, preferred string, leader func() string) {
	t.Helper()
	for range 10 {
		if leader() == preferred {
			return
		}
		body, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": preferred}})
		resp, err := nc.Request(subject, body, 10*time.Second)
		if err == nil && !json.Valid(resp.Data) {
			t.Fatalf("%s: %s", subject, resp.Data)
		}
		var r struct {
			Error *apiError `json:"error"`
		}
		if err == nil && json.Unmarshal(resp.Data, &r) == nil && r.Error != nil {
			t.Logf("%s: %s", subject, r.Error.Description)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("%s: the leader did not move to %s", subject, preferred)
}

// apiError is a JetStream API response's error.
type apiError struct {
	Description string `json:"description"`
}

// placement is each stream's leader, as the bus reports it at the run's end.
func (f *feed) placement(t *testing.T) string {
	t.Helper()
	var out []string
	for _, s := range f.streams {
		si, err := s.Info(t.Context())
		if err != nil || si.Cluster == nil {
			out = append(out, "?")
			continue
		}
		out = append(out, si.Cluster.Leader)
	}

	return fmt.Sprint(out)
}
