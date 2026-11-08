package service

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"
	"github.com/arloliu/go-secs/v2/sml"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/testbus"
)

// The feed mode measures the host log feed's stream before its converter exists (the host log feed plan's step 1, last text at 3c11e5e):
// the stream and group consumers of docs/specs/tracekeep/logfeed.md §15.4 on a three-node cluster shared with production's traffic,
// publishers awaiting each publish's acknowledgement, and consumers that hold each entry for the reorder window and its equipment's flush as the converter will.

const (
	feedGroups    = 32
	feedEquipment = 2000
	feedConns     = 16
	// feedInFlight is each publisher connection's publishes awaiting their acknowledgement;
	// feedRetriers are its retries of failed acknowledgements, each one publish awaiting its own.
	feedInFlight = 256
	feedRetriers = 16
	feedStream   = "TRACEKEEP_LOGFEED"
	feedPrefix   = "tklog"
	feedSource   = "load-feed"
	// feedMaxBytes is the main case's limit; feedFullBytes the full-stream control's, enough for several sealed 4 MB blocks.
	feedMaxBytes  = 40 << 30
	feedFullBytes = 64 << 20
	// feedWindows are the judged flush cycles, after one of warm-up; feedControlWindow is a control's window whatever the mode.
	feedWindows       = 3
	feedControlWindow = 10 * time.Second
	// feedP99 is the publish latency budget at the 99th percentile.
	feedP99 = time.Second
	// feedAckTimeout bounds a publish's wait for its acknowledgement, which then fails and is retried once.
	feedAckTimeout = 5 * time.Second
	// feedStaleAcks are the takeover's acknowledgements from the old consumer's handles, of its newest held entries.
	feedStaleAcks = 10
	// errCodeMaxBytes is the stream's refusal of a publish past its max_bytes with discard-new.
	errCodeMaxBytes jetstream.ErrorCode = 10077
)

// feedTimes are a mode's times, rate and budgets.
type feedTimes struct {
	flush, ackWait, reorder, dupWindow time.Duration
	maxAckPending                      int
	rate                               float64
	// traffic is production's traffic beside the feed: its equipment, forwards a second and pending budget.
	k            int
	trafficRate  float64
	trafficLimit int
}

// feedModeTimes are the mode's times: ingest's own, T-61's 1/30 of them, or the latter at a tenth of the rate.
func feedModeTimes(smoke, realTime bool) feedTimes {
	switch {
	case realTime:
		return feedTimes{flush: 5 * time.Minute, ackWait: 7 * time.Minute, reorder: 30 * time.Second, dupWindow: 2 * time.Minute,
			maxAckPending: 100_000, rate: 3200, k: 100, trafficRate: 800, trafficLimit: 600_000}
	case smoke:
		return feedTimes{flush: 10 * time.Second, ackWait: 14 * time.Second, reorder: time.Second, dupWindow: 10 * time.Second,
			maxAckPending: 3_400, rate: 320, k: 2, trafficRate: 20, trafficLimit: 2_000}
	default:
		return feedTimes{flush: 10 * time.Second, ackWait: 14 * time.Second, reorder: time.Second, dupWindow: 2 * time.Minute,
			maxAckPending: 3_400, rate: 3200, k: 100, trafficRate: 800, trafficLimit: 20_000}
	}
}

// loadFeed runs the main case beside production's traffic, the takeover under it, then the controls.
func loadFeed(t *testing.T, smoke, realTime bool) {
	ft := feedModeTimes(smoke, realTime)
	srvs := testbus.StartClusterServers(t, 3)
	urls := make([]string, len(srvs))
	for i, s := range srvs {
		urls[i] = s.ClientURL()
	}
	url := strings.Join(urls, ",")
	pool := newFeedPool(t)
	h := newReportingHarness(t, loadSpec{flush: ft.flush, ackWait: ft.ackWait, probed: true, maxAckPending: ft.trafficLimit, bus: url})
	h.log(fmt.Sprintf("feed on a three-node cluster, catalog %s: flush %s, ack_wait %s, reorder window %s, max_ack_pending %d, %.0f entries/s over %d equipment and %d groups; traffic %.0f fwd/s over K = %d",
		h.engine, ft.flush, ft.ackWait, ft.reorder, ft.maxAckPending, ft.rate, feedEquipment, feedGroups, ft.trafficRate, ft.k))

	t.Run("main", func(t *testing.T) { feedMain(t, h, srvs, url, pool, ft) })
	t.Run("constrained", func(t *testing.T) { feedConstrained(t, h, url, pool, ft) })
	t.Run("full-stream", func(t *testing.T) { feedFull(t, h, srvs, url, pool) })
	t.Run("duplicate-window-memory", func(t *testing.T) { feedDupMemory(t, h, url, pool, ft) })
}

// feedMain runs the feed beside the traffic harness's production case: the feed is judged over its flush cycles, the traffic by its own verdict,
// and one group's consumer is then taken over while the load continues.
func feedMain(t *testing.T, h *loadHarness, srvs []*server.Server, url string, pool *feedPool, ft feedTimes) {
	ctx := t.Context()
	f := newFeed(t, url, feedStream, feedPrefix, ft, feedMaxBytes, true)
	equipment := h.startEquipment(ft.k)
	defer h.stopEquipment(equipment)
	cpu0 := processCPU(t)
	pubs := f.publish(ctx, pool, ft.rate, feedConns, feedInFlight, true)
	f.consume(ctx)
	sampler := f.sample(ctx, srvs, pubs)
	dir := profileDir(t)
	tv, failed, ackPending := h.cycles(equipment, ft.trafficRate, dir)
	writeHeapProfile(t, filepath.Join(dir, "feed-heap.pprof"))
	if span := time.Duration(feedWindows+1) * ft.flush; time.Since(pubs.started) < span+ft.reorder {
		time.Sleep(span + ft.reorder - time.Since(pubs.started))
	}
	samples := sampler.stop()
	judged := afterWarmUp(samples, pubs.started.Add(ft.flush+ft.reorder))
	v := judgeFeed(ft.rate, judged, pubs.latencies(judged[0].at, judged[len(judged)-1].at), ft.flush, ft.maxAckPending)
	secs := samples[len(samples)-1].at.Sub(samples[0].at).Seconds()
	last, first := samples[len(samples)-1], samples[0]
	h.log("", "| offered /s | acknowledged /s | failed | retried | windows | mean backlog slope | queue maxima slope | bound | publish p50 | publish p99 | max pending (busiest group) | max held | sustained |",
		"|---|---|---|---|---|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %.1f | %d | %d | %d | %.2f | %.2f | %.2f | %s | %s | %d | %d | %v %s |", ft.rate, v.acked, last.failed, last.retried, v.windows,
			v.backlogSlope, v.queueSlope, v.bound, v.p50.Round(time.Microsecond), v.p99.Round(time.Microsecond), v.pendingMax, v.heldMax, v.sustained, v.why))
	cycleRows := make([]string, 0, 3+len(v.cycles))
	cycleRows = append(cycleRows, "", "| cycle | acknowledged /s | mean backlog | publisher queue max | busiest consumer's pending |", "|---|---|---|---|---|")
	for i, c := range v.cycles {
		cycleRows = append(cycleRows, fmt.Sprintf("| %d | %.1f | %.0f | %.0f | %d |", i+1, c.acked, c.backlog, c.queue, c.pending))
	}
	h.log(cycleRows...)
	h.log("", "consumers' pending at the end, lowest / median / highest: "+f.pendingSpread(ctx),
		"the store's columns are the stream's directory; a server's JetStream directory also holds its Raft logs and the other streams")
	h.log("", "| acks /s | in-progress acks /s | in-progress / ack | process CPU (cores) | max RSS | disk writes /s | logical bytes (max) | store on disk per server (end) |",
		"|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %.0f | %.3f | %.2f | %s | %s | %s | %s |", float64(last.acks-first.acks)/secs, float64(last.inProgress-first.inProgress)/secs,
			float64(last.inProgress-first.inProgress)/math.Max(1, float64(last.acks-first.acks)), (processCPU(t)-cpu0).Seconds()/secs,
			mib(maxOf(samples, func(s feedSample) uint64 { return s.rss })), mib(uint64(float64(last.written-first.written)/secs)),
			mib(maxOf(samples, func(s feedSample) uint64 { return s.bytes })), last.stores))
	if !v.sustained {
		t.Errorf("the feed is not sustained: %s", v.why)
	}
	if why := productionVerdict(tv, failed, ackPending, ft.trafficLimit, false); why != "" {
		t.Errorf("the traffic beside the feed: %s", why)
	}
	stopTraffic := h.drive(ctx, equipment, ft.trafficRate)
	tk := f.groups[0].takeover(ctx, f, feedStaleAcks)
	lat := pubs.latencies(tk.at, tk.at.Add(tk.took+time.Second))
	stopTraffic()
	h.log("", "| takeover: held | acks lost with the consumer | every held entry redelivered, oldest first | took | old handles' late acks applied | publish p99 meanwhile |", "|---|---|---|---|---|---|",
		fmt.Sprintf("| %d | %d | %v | %s | %d of %d | %s |", tk.held, tk.lost, tk.inOrder, tk.took.Round(time.Millisecond), tk.applied, feedStaleAcks, percentile(lat, 0.99).Round(time.Microsecond)))
	switch {
	case !tk.inOrder:
		t.Errorf("the takeover's first deliveries were not its %d held entries, oldest first", tk.held)
	case tk.took > ft.ackWait:
		t.Errorf("the takeover redelivered its held entries in %s, beyond ack_wait %s", tk.took, ft.ackWait)
	default:
	}
	pubs.stop()
	f.stop()
}

// feedConstrained runs a publisher capped at half the rate, which the rate budget must refuse,
// then one connection with one publish in flight, measured and not judged.
func feedConstrained(t *testing.T, h *loadHarness, url string, pool *feedPool, ft feedTimes) {
	ctx := t.Context()
	ct := ft
	ct.flush, ct.reorder = feedControlWindow, 0
	f := newFeed(t, url, "TRACEKEEP_LOGFEED_CAPPED", "tkcapped", ct, feedMaxBytes, false)
	f.consume(ctx)
	pubs := f.publish(ctx, pool, ft.rate/2, feedConns, feedInFlight, false)
	sampler := f.sample(ctx, nil, pubs)
	time.Sleep(time.Duration(feedWindows+1)*feedControlWindow + 2*loadTick)
	samples := sampler.stop()
	pubs.stop()
	judged := afterWarmUp(samples, pubs.started.Add(feedControlWindow))
	v := judgeFeed(ft.rate, judged, pubs.latencies(judged[0].at, judged[len(judged)-1].at), feedControlWindow, ft.maxAckPending)
	h.log("", fmt.Sprintf("capped at %.0f/s against %.0f/s offered: acknowledged %.1f/s, sustained %v %s", ft.rate/2, ft.rate, v.acked, v.sustained, v.why))
	if v.sustained || !strings.HasPrefix(v.why, "acknowledged") {
		t.Errorf("a publisher capped at half the rate: sustained %v, %s; want the rate budget refusing it", v.sustained, v.why)
	}
	serial := f.publish(ctx, pool, ft.rate, 1, 1, false)
	time.Sleep(feedControlWindow)
	n := serial.acked.Load()
	serial.stop()
	h.log(fmt.Sprintf("one connection, one publish in flight: %.0f acknowledged publishes/s against %.0f offered", float64(n)/feedControlWindow.Seconds(), ft.rate))
	f.stop()
}

// feedFull checks a retried publish stored once, then fills a small stream with its consumer paused:
// publishes fail at the limit and evict nothing, each server's store is reported against the logical bytes, and a drain lets the same publish succeed.
func feedFull(t *testing.T, h *loadHarness, srvs []*server.Server, url string, pool *feedPool) {
	ctx := t.Context()
	ft := feedTimes{flush: time.Minute, ackWait: 2 * time.Minute, dupWindow: 2 * time.Minute, maxAckPending: 1_000_000}
	f := newFeed(t, url, "TRACEKEEP_LOGFEED_FULL", "tkfull", ft, feedFullBytes, false)
	feedLostAck(t, f, pool)
	accepted := uint64(1)
	var refused *nats.Msg
	for i := 1; ; i++ {
		m := pool.message(f.prefix, i%feedEquipment, int64(i), i)
		if _, err := f.js.PublishMsg(ctx, m); err != nil {
			var api *jetstream.APIError
			if !errors.As(err, &api) || api.ErrorCode != errCodeMaxBytes {
				t.Fatalf("full stream: after %d publishes, a failure that is not the stream's limit: %v", accepted, err)
			}
			refused = m
			h.log("", fmt.Sprintf("full stream: %d publishes accepted, %s logical, then refused: %v", accepted, mib(f.info(t).State.Bytes), err))

			break
		}
		accepted++
	}
	info := f.info(t)
	if info.State.Msgs != accepted || info.State.FirstSeq != 1 {
		t.Errorf("full stream holds %d messages from seq %d, want the %d accepted from seq 1", info.State.Msgs, info.State.FirstSeq, accepted)
	}
	rows := make([]string, 0, 3+len(srvs))
	rows = append(rows, "", "| server | blocks on disk | other files | logical bytes |", "|---|---|---|---|")
	for _, s := range srvs {
		st := streamStore(s, f.name)
		rows = append(rows, fmt.Sprintf("| %s | %s | %s | %s |", s.Name(), mib(st.sealed+st.active), mib(st.meta), mib(info.State.Bytes)))
	}
	h.log(rows...)
	feedDrain(t, f)
	if _, err := f.js.PublishMsg(ctx, refused); err != nil {
		t.Errorf("the refused publish failed again after the drain: %v", err)
	}
	f.stop()
}

// feedLostAck publishes an entry whose acknowledgement is lost, stored without one, then retries it under its message id: the stream stores it once.
func feedLostAck(t *testing.T, f *feed, pool *feedPool) {
	t.Helper()
	ctx := t.Context()
	msg := pool.message(f.prefix, 0, 0, 0)
	if err := f.nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	testbus.Retry(t, func(context.Context) error {
		if n := f.info(t).State.Msgs; n != 1 {
			return fmt.Errorf("stored %d: %w", n, context.DeadlineExceeded)
		}

		return nil
	})
	ack, err := f.js.PublishMsg(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if n := f.info(t).State.Msgs; n != 1 || !ack.Duplicate {
		t.Fatalf("a publish retried under its message id: %d messages stored, acknowledged as a duplicate %v; want 1 and true", n, ack.Duplicate)
	}
}

// feedDrain acknowledges every message of f's one consumer within a minute.
func feedDrain(t *testing.T, f *feed) {
	t.Helper()
	cons := f.consumers[0]
	drained := time.Now().Add(time.Minute)
	for f.info(t).State.Msgs > 0 {
		if time.Now().After(drained) {
			t.Fatalf("full stream: %d messages left after a minute's drain", f.info(t).State.Msgs)
		}
		b, err := cons.Fetch(10_000, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		for m := range b.Messages() {
			if err := m.Ack(); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			t.Fatal(err)
		}
	}
}

// feedDupMemory runs the main case's feed, without traffic, once with message ids and once with an inert header of their length in their place,
// and compares the heap and the resident memory at the duplicate window's full population, read from the publishes acknowledged over the window.
func feedDupMemory(t *testing.T, h *loadHarness, url string, pool *feedPool, ft feedTimes) {
	ctx := t.Context()
	type reading struct {
		ids       float64
		heap, rss int64
	}
	run := func(ids bool) reading {
		f := newFeed(t, url, "TRACEKEEP_LOGFEED_DUP", "tkdup", ft, feedMaxBytes, true)
		f.consume(ctx)
		pubs := f.publish(ctx, pool, ft.rate, feedConns, feedInFlight, ids)
		time.Sleep(5 * time.Second)
		from := pubs.acked.Load()
		time.Sleep(ft.dupWindow)
		r := reading{ids: float64(pubs.acked.Load() - from)}
		runtime.GC() //nolint:revive // the heap compared at the window's population, not its garbage
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		r.heap, r.rss = int64(ms.HeapInuse), int64(procStatus("VmRSS:")*1024) //nolint:gosec // sizes
		pubs.stop()
		f.stop()

		return r
	}
	with, without := run(true), run(false)
	signed := func(b int64) string { return fmt.Sprintf("%+.1f MiB", float64(b)/(1<<20)) }
	h.log("", "| duplicate window | ids in the window | heap with ids | heap with an inert header | heap difference, per id | RSS with ids | RSS with an inert header | RSS difference |",
		"|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %s | %.0f | %s | %s | %s, %.0f B | %s | %s | %s |", ft.dupWindow, with.ids, mib(uint64(with.heap)), mib(uint64(without.heap)), //nolint:gosec // sizes
			signed(with.heap-without.heap), float64(with.heap-without.heap)/math.Max(1, with.ids),
			mib(uint64(with.rss)), mib(uint64(without.rss)), signed(with.rss-without.rss))) //nolint:gosec // sizes
}

// feedEntry is one entry of logfeed.md §15.3's field schema.
type feedEntry struct {
	V           int    `json:"v"`
	EquipmentID string `json:"equipment_id"`
	Source      string `json:"source"`
	TZ          string `json:"tz"`
	File        string `json:"file"`
	Offset      int64  `json:"offset"`
	TS          string `json:"ts"`
	Kind        string `json:"kind"`
	Dir         string `json:"dir,omitempty"`
	Device      string `json:"device,omitempty"`
	Header      string `json:"header,omitempty"`
	TID         string `json:"tid,omitempty"`
	SxFy        string `json:"sxfy,omitempty"`
	Text        string `json:"text,omitempty"`
}

// feedPool is header and body entries generated from go-secs messages, sized as the sample's 570 B an entry, which a publish fills in.
type feedPool struct{ entries []feedEntry }

// newFeedPool generates 200 messages, each a header entry and a body entry: event reports, their acknowledgements and status data.
func newFeedPool(t *testing.T) *feedPool {
	t.Helper()
	r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // synthetic data
	p := &feedPool{}
	for i := range 200 {
		s, f, it := feedMessage(r, i)
		m, err := hsms.NewDataMessage(s, f, f%2 == 1, 1, [4]byte{0, 0, byte(i >> 8), byte(i)}, it)
		if err != nil {
			t.Fatal(err)
		}
		hdr := m.HeaderBytes()
		sf := fmt.Sprintf("S%dF%d", s, f)
		p.entries = append(p.entries,
			feedEntry{Kind: "header", Dir: "sent", Device: "1", Header: hex.EncodeToString(hdr[:]), TID: strconv.Itoa(i), SxFy: sf},
			feedEntry{Kind: "body", TID: strconv.Itoa(i), SxFy: sf, Text: sml.MustEncodeMessage(m)})
	}

	return p
}

// feedMessage is message i: an event report, its acknowledgement, or status values.
func feedMessage(r *rand.Rand, i int) (s, f uint8, it secs2.Item) {
	switch i % 4 {
	case 0, 1:
		n := 5 + r.IntN(8)
		reports := make([]secs2.Item, 0, n)
		for k := range n {
			var vals []secs2.Item
			for v := range 4 + r.IntN(8) {
				switch v % 3 {
				case 0:
					vals = append(vals, secs2.NewASCIIItem(fmt.Sprintf("LOT%06d.%02d", r.IntN(999_999), r.IntN(25))))
				case 1:
					vals = append(vals, secs2.NewUintItem(4, r.IntN(100_000)))
				default:
					vals = append(vals, secs2.NewFloatItem(4, r.Float32()*1000))
				}
			}
			reports = append(reports, secs2.NewListItem(secs2.NewUintItem(4, 1000+k), secs2.NewListItem(vals...)))
		}

		return 6, 11, secs2.NewListItem(secs2.NewUintItem(4, i), secs2.NewUintItem(4, 2000+r.IntN(50)), secs2.NewListItem(reports...))
	case 2:
		return 6, 12, secs2.NewBinaryItem(0)
	default:
		var vals []secs2.Item
		for range 5 + r.IntN(10) {
			vals = append(vals, secs2.NewUintItem(4, r.IntN(1<<20)))
		}

		return 1, 4, secs2.NewListItem(vals...)
	}
}

// message is the publish of equipment eq's entry at offset, the pool's entry n, on prefix's subject with its message id.
func (p *feedPool) message(prefix string, eq int, offset int64, n int) *nats.Msg {
	e := p.entries[n%len(p.entries)]
	id := fmt.Sprintf("FEED-%04d", eq)
	e.V, e.EquipmentID, e.Source, e.TZ, e.File, e.Offset = 1, id, feedSource, "Asia/Taipei", "host-20261107.log", offset
	e.TS = time.Now().Format("2006-01-02 15:04:05.000")
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(e) // a struct of strings and integers
	m := nats.NewMsg(prefix + "." + id)
	m.Data = bytes.TrimSuffix(b.Bytes(), []byte("\n"))
	m.Header.Set(jetstream.MsgIDHeader, fmt.Sprintf("%s/%s/%s/%d", feedSource, id, e.File, offset))

	return m
}

// feed is one stream with its group consumers.
type feed struct {
	t      *testing.T
	ft     feedTimes
	name   string
	prefix string
	nc     *nats.Conn
	js     jetstream.JetStream
	stream jetstream.Stream
	// streams are the feed-streams mode's streams, stream the first of them.
	streams   []jetstream.Stream
	consumers []jetstream.Consumer
	groups    []*feedGroup
	// aligned makes an equipment's flush due F after its first entry and each flush, as the log stage's are, not spread over F by its phase;
	// groupBytes and maxHeld bound each group as the log stage's bound does (logfeed.md §15.7): past groupBytes of batched messages,
	// or holding more than half of maxHeld, its largest equipment flushes first; zero bounds nothing.
	aligned             bool
	groupBytes, maxHeld int
	// purge settles a group by purging its stream below its checkpoint instead of acknowledging each entry (the feed-streams mode's
	// TRACEKEEP_LOAD_SETTLE=purge): its streams keep messages by limits, its consumers acknowledge nothing,
	// and its checkpoint is the stream sequence below which it holds nothing; purged times each purge.
	purge  bool
	purged purgeTimes
	// restarted, with the feed's purge, is the restart's record: every group resumed from its checkpoint (feed.restart).
	restarted restartRecord
	acks      atomic.Uint64
	progress  atomic.Uint64
}

// restartRecord is a restart's outcome: when it began, the entries read again (delivered once before it),
// and each group's time until it delivered past what it had before.
type restartRecord struct {
	mu       sync.Mutex
	at       time.Time
	groups   int
	reread   uint64
	caughtUp []time.Duration
}

func (r *restartRecord) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.at.IsZero() {
		return "no restart"
	}
	slowest := time.Duration(0)
	for _, d := range r.caughtUp {
		slowest = max(slowest, d)
	}

	return fmt.Sprintf("restart: %d groups, %d entries read again, %d groups caught up, the slowest after %s",
		r.groups, r.reread, len(r.caughtUp), slowest.Round(time.Millisecond))
}

// restart stops every group, drops what it holds, and resumes each from its checkpoint on a new consumer, as a restarted converter does.
func (f *feed) restart(ctx context.Context) {
	began := time.Now()
	for _, g := range f.groups {
		g.halt()
	}
	f.restarted.mu.Lock()
	f.restarted.at, f.restarted.groups = began, len(f.groups)
	f.restarted.mu.Unlock()
	for _, g := range f.groups {
		g.mu.Lock()
		cp, last := g.checkpoint, g.last
		g.reorder, g.batches, g.due, g.bytes = nil, map[string][]jetstream.Msg{}, map[string]time.Time{}, map[string]int{}
		g.held, g.total, g.last = 0, 0, 0
		g.rereadUpTo, g.restartedAt, g.caught = last, began, false
		g.mu.Unlock()
		s := f.streams[g.n%len(f.streams)]
		cfg := f.consumerConfig(g.n, g.filter)
		if err := s.DeleteConsumer(ctx, cfg.Durable); err != nil {
			f.t.Errorf("restart: group %d: %v", g.n, err)
			continue
		}
		cfg.DeliverPolicy, cfg.OptStartSeq = jetstream.DeliverByStartSequencePolicy, max(cp, 1)
		c, err := s.CreateOrUpdateConsumer(ctx, cfg)
		if err != nil {
			f.t.Errorf("restart: group %d: %v", g.n, err)
			continue
		}
		g.start(ctx, c)
	}
}

// purgeTimes are the purges' durations and failures.
type purgeTimes struct {
	mu     sync.Mutex
	took   []time.Duration
	failed int
}

func (p *purgeTimes) add(took time.Duration, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.failed++
		return
	}
	p.took = append(p.took, took)
}

// String is the purges' count, failures, p50, p99, max and total time.
func (p *purgeTimes) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	took := slices.Clone(p.took)
	slices.Sort(took)
	var total time.Duration
	for _, d := range took {
		total += d
	}
	q := func(f float64) time.Duration {
		if len(took) == 0 {
			return 0
		}
		return took[min(len(took)-1, int(float64(len(took))*f))]
	}

	return fmt.Sprintf("%d purges, %d failed, p50 %s, p99 %s, max %s, total %s", len(took), p.failed,
		q(.5).Round(time.Microsecond), q(.99).Round(time.Microsecond), q(1).Round(time.Microsecond), total.Round(time.Millisecond))
}

// newFeed creates the stream logfeed.md §15.4 specifies under name and prefix, R3, with maxBytes, partitioned into feedGroups when partitioned,
// and its group consumers, deleted when t ends.
func newFeed(t *testing.T, url, name, prefix string, ft feedTimes, maxBytes int64, partitioned bool) *feed {
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
	cfg := jetstream.StreamConfig{
		Name: name, Subjects: []string{prefix + ".*"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: 3,
		MaxBytes: maxBytes, Discard: jetstream.DiscardNew, Duplicates: cmp.Or(ft.dupWindow, 2*time.Minute),
	}
	groups := 1
	if partitioned {
		groups = feedGroups
		cfg.SubjectTransform = &jetstream.SubjectTransformConfig{Source: prefix + ".*", Destination: fmt.Sprintf("%s.{{partition(%d,1)}}.{{wildcard(1)}}", prefix, feedGroups)}
	}
	s, err := js.CreateStream(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	testbus.AwaitStream(t, js, s)
	t.Cleanup(func() { _ = js.DeleteStream(context.WithoutCancel(ctx), name) })
	f := &feed{t: t, ft: ft, name: name, prefix: prefix, nc: nc, js: js, stream: s}
	for n := range groups {
		filter := prefix + ".*"
		if partitioned {
			filter = fmt.Sprintf("%s.%d.>", prefix, n)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, f.consumerConfig(n, filter))
		if err != nil {
			t.Fatal(err)
		}
		f.consumers = append(f.consumers, c)
		f.groups = append(f.groups, &feedGroup{n: n, f: f, filter: filter, batches: map[string][]jetstream.Msg{}, due: map[string]time.Time{}})
	}

	return f
}

// consumerConfig is group n's durable consumer: explicit acknowledgements, the mode's ack_wait and pending limit, unlimited deliveries,
// its state in memory (logfeed.md §15.4, T-64).
func (f *feed) consumerConfig(n int, filter string) jetstream.ConsumerConfig {
	if f.purge {
		return jetstream.ConsumerConfig{Durable: fmt.Sprintf("logfeed-%d", n), FilterSubject: filter, AckPolicy: jetstream.AckNonePolicy, MemoryStorage: true}
	}

	return jetstream.ConsumerConfig{
		Durable: fmt.Sprintf("logfeed-%d", n), FilterSubject: filter, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: cmp.Or(f.ft.ackWait, time.Minute), MaxAckPending: f.ft.maxAckPending, MaxDeliver: -1,
		MemoryStorage: true,
	}
}

func (f *feed) info(t *testing.T) *jetstream.StreamInfo {
	t.Helper()
	info, err := f.stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return info
}

// consume starts every group.
func (f *feed) consume(ctx context.Context) {
	for i, g := range f.groups {
		g.start(ctx, f.consumers[i])
	}
}

// stop stops every group.
func (f *feed) stop() {
	for _, g := range f.groups {
		g.halt()
	}
}

// feedGroup is one group's converter as the load sees it: each entry waits the reorder window, joins its equipment's batch,
// and is acknowledged at its equipment's flush; everything held is kept in progress every ack_wait / 3.
type feedGroup struct {
	n      int
	f      *feed
	filter string
	mu     sync.Mutex
	cc     jetstream.ConsumeContext
	cancel context.CancelFunc
	done   chan struct{}
	// reorder are the entries in their window, in arrival order; batches and due, each equipment's released entries and next flush.
	reorder []feedHeld
	batches map[string][]jetstream.Msg
	due     map[string]time.Time
	held    int
	// bytes are each equipment's batched message bytes, total the group's.
	bytes map[string]int
	total int
	// want, during a takeover, is how many deliveries the new consumer's first ones are recorded in order, and allAt when they all arrived.
	want  int
	order []uint64
	allAt time.Time
	// With the feed's purge: first is each batched equipment's first stream sequence, last the latest delivered,
	// checkpoint the latest one handed to the purger through purges.
	first      map[string]uint64
	last       uint64
	checkpoint uint64
	purges     chan uint64
	// After a restart: deliveries at or below rereadUpTo are read again; caught is set at the first past it.
	rereadUpTo  uint64
	restartedAt time.Time
	caught      bool
}

type feedHeld struct {
	at  time.Time
	eq  string
	m   jetstream.Msg
	seq uint64
}

// start consumes c into g and runs g's loop.
func (g *feedGroup) start(ctx context.Context, c jetstream.Consumer) {
	ctx, g.cancel = context.WithCancel(ctx)
	g.done = make(chan struct{})
	g.subscribe(c)
	if g.f.purge {
		g.first, g.purges = map[string]uint64{}, make(chan uint64, 1)
		go g.purger(ctx)
	}
	go g.loop(ctx)
}

// purger purges g's stream below each checkpoint handed to it, g's subjects alone on a stream it shares.
func (g *feedGroup) purger(ctx context.Context) {
	s := g.f.streams[g.n%len(g.f.streams)]
	for {
		select {
		case <-ctx.Done():
			return
		case cp := <-g.purges:
			opts := []jetstream.StreamPurgeOpt{jetstream.WithPurgeSequence(cp)}
			if len(g.f.streams) == 1 {
				opts = append(opts, jetstream.WithPurgeSubject(g.filter))
			}
			began := time.Now()
			err := s.Purge(ctx, opts...)
			g.f.purged.add(time.Since(began), err)
		}
	}
}

// settle hands the purger g's checkpoint when it moved: the least stream sequence g holds, or the one after the latest delivered.
func (g *feedGroup) settle() {
	cp := g.last + 1
	if len(g.reorder) > 0 {
		cp = min(cp, g.reorder[0].seq)
	}
	for _, seq := range g.first {
		cp = min(cp, seq)
	}
	if g.last == 0 || cp <= g.checkpoint {
		return
	}
	g.checkpoint = cp
	select {
	case <-g.purges:
	default:
	}
	g.purges <- cp
}

func (g *feedGroup) subscribe(c jetstream.Consumer) {
	cc, err := c.Consume(g.add, jetstream.PullMaxMessages(500))
	if err != nil {
		g.f.t.Error(err)
		return
	}
	g.cc = cc
}

// add holds a delivery in the reorder window, counting it toward a takeover's expected redeliveries.
func (g *feedGroup) add(m jetstream.Msg) {
	eq := m.Subject()[strings.LastIndexByte(m.Subject(), '.')+1:]
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.order) < g.want {
		if md, err := m.Metadata(); err == nil {
			g.order = append(g.order, md.Sequence.Stream)
			if len(g.order) == g.want {
				g.allAt = time.Now()
			}
		}
	}
	h := feedHeld{at: time.Now(), eq: eq, m: m}
	if g.f.purge {
		if md, err := m.Metadata(); err == nil {
			h.seq = md.Sequence.Stream
			g.last = max(g.last, h.seq)
			if g.rereadUpTo > 0 && !g.caught {
				if h.seq <= g.rereadUpTo {
					g.f.restarted.mu.Lock()
					g.f.restarted.reread++
					g.f.restarted.mu.Unlock()
				} else {
					g.caught = true
					g.f.restarted.mu.Lock()
					g.f.restarted.caughtUp = append(g.f.restarted.caughtUp, time.Since(g.restartedAt))
					g.f.restarted.mu.Unlock()
				}
			}
		}
	}
	g.reorder = append(g.reorder, h)
	g.held++
}

// loop releases, flushes and heartbeats every 50 ms until ctx ends.
func (g *feedGroup) loop(ctx context.Context) {
	defer close(g.done)
	ft := g.f.ft
	start := time.Now()
	beat := max(ft.ackWait/3, time.Second)
	nextBeat := start.Add(beat)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			g.mu.Lock()
			i := 0
			for ; i < len(g.reorder) && now.Sub(g.reorder[i].at) >= ft.reorder; i++ {
				h := g.reorder[i]
				g.batches[h.eq] = append(g.batches[h.eq], h.m)
				if _, ok := g.first[h.eq]; g.f.purge && !ok {
					g.first[h.eq] = h.seq
				}
				if g.bytes == nil {
					g.bytes = map[string]int{}
				}
				g.bytes[h.eq] += len(h.m.Data())
				g.total += len(h.m.Data())
				if _, ok := g.due[h.eq]; !ok {
					g.due[h.eq] = start.Add(phase(h.eq, ft.flush))
					if g.f.aligned {
						g.due[h.eq] = now.Add(ft.flush)
					}
				}
			}
			g.reorder = slices.Delete(g.reorder, 0, i)
			for eq, at := range g.due {
				if now.Before(at) {
					continue
				}
				g.flushEq(eq, now)
			}
			g.relieve(now)
			if g.f.purge {
				g.settle()
			}
			if !g.f.purge && !now.Before(nextBeat) {
				for _, h := range g.reorder {
					_ = h.m.InProgress()
				}
				for _, b := range g.batches {
					for _, m := range b {
						_ = m.InProgress()
					}
				}
				g.f.progress.Add(uint64(g.held)) //nolint:gosec // a count
				nextBeat = nextBeat.Add(beat)
			}
			g.mu.Unlock()
		}
	}
}

// flushEq acknowledges eq's batch and makes its next flush due: F after now when aligned, else at its phase's next time.
func (g *feedGroup) flushEq(eq string, now time.Time) {
	for _, m := range g.batches[eq] {
		if !g.f.purge {
			_ = m.Ack()
		}
		g.f.acks.Add(1)
	}
	delete(g.first, eq)
	g.held -= len(g.batches[eq])
	g.total -= g.bytes[eq]
	delete(g.batches, eq)
	delete(g.bytes, eq)
	flush := max(g.f.ft.flush, 50*time.Millisecond)
	if g.f.aligned {
		g.due[eq] = now.Add(flush)
		return
	}
	for !g.due[eq].After(now) {
		g.due[eq] = g.due[eq].Add(flush)
	}
}

// relieve flushes g's largest equipment while g holds past its bound.
func (g *feedGroup) relieve(now time.Time) {
	for g.f.groupBytes > 0 && g.total > g.f.groupBytes || g.f.maxHeld > 0 && 2*g.held > g.f.maxHeld {
		largest := ""
		for eq, b := range g.bytes {
			if largest == "" || b > g.bytes[largest] || b == g.bytes[largest] && eq < largest {
				largest = eq
			}
		}
		if largest == "" {
			return
		}
		g.flushEq(largest, now)
	}
}

// phase is equipment eq's flush offset within F, so the equipments' flushes spread over it.
func phase(eq string, flush time.Duration) time.Duration {
	if flush <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(eq))

	return time.Duration(h.Sum64() % uint64(flush)) //nolint:gosec // a positive duration
}

func (g *feedGroup) halt() {
	if g.cc != nil {
		g.cc.Stop()
	}
	if g.cancel != nil {
		g.cancel()
		<-g.done
	}
}

func (g *feedGroup) heldNow() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.held
}

// feedTakeover is a takeover's record.
type feedTakeover struct {
	at                  time.Time
	held, lost, applied int
	// inOrder is the new consumer's first deliveries being every held entry, oldest first; took is from the deletion until the last of them.
	inOrder bool
	took    time.Duration
}

// takeover deletes and re-creates g's consumer while g holds its entries, as a converter taking the group's lease does (logfeed.md §15.5).
// Once the old consumer's deliveries have stopped, the held entries are taken from g; the old handles acknowledge the newest lost of them
// between the deletion and the re-creation, acknowledgements the deletion loses, and once the new consumer has delivered every held entry again
// they send in-progress acknowledgements and acknowledge as many more, which the new consumer applies by stream sequence.
// The new consumer's first deliveries must be every held entry, oldest first, within ack_wait of the deletion.
func (g *feedGroup) takeover(ctx context.Context, f *feed, lost int) feedTakeover {
	g.cc.Stop()
	<-g.cc.Closed()
	g.mu.Lock()
	var old []jetstream.Msg
	for _, h := range g.reorder {
		old = append(old, h.m)
	}
	for _, b := range g.batches {
		old = append(old, b...)
	}
	seqOf := func(m jetstream.Msg) uint64 {
		md, err := m.Metadata()
		if err != nil {
			return 0
		}

		return md.Sequence.Stream
	}
	slices.SortFunc(old, func(a, b jetstream.Msg) int { return cmp.Compare(seqOf(a), seqOf(b)) })
	tk := feedTakeover{at: time.Now(), held: len(old), lost: min(lost, len(old))}
	g.reorder, g.batches, g.held = nil, map[string][]jetstream.Msg{}, 0
	g.want, g.order, g.allAt = len(old), nil, time.Time{}
	g.mu.Unlock()
	if err := f.stream.DeleteConsumer(ctx, f.consumerConfig(g.n, g.filter).Durable); err != nil {
		f.t.Fatal(err)
	}
	for _, m := range old[len(old)-tk.lost:] {
		_ = m.Ack()
	}
	c, err := f.stream.CreateOrUpdateConsumer(ctx, f.consumerConfig(g.n, g.filter))
	if err != nil {
		f.t.Fatal(err)
	}
	f.consumers[g.n] = c
	g.subscribe(c)
	deadline := tk.at.Add(f.ft.ackWait)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		done := !g.allAt.IsZero()
		g.mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	g.mu.Lock()
	order, allAt := g.order, g.allAt
	g.want, g.order = 0, nil
	g.mu.Unlock()
	tk.inOrder = len(order) == len(old)
	for i := range order {
		tk.inOrder = tk.inOrder && order[i] == seqOf(old[i])
	}
	tk.took = time.Since(tk.at)
	if !allAt.IsZero() {
		tk.took = allAt.Sub(tk.at)
	}
	late := old[:max(0, len(old)-tk.lost)]
	late = late[max(0, len(late)-lost):]
	for _, m := range old {
		_ = m.InProgress()
	}
	for _, m := range late {
		if m.DoubleAck(ctx) != nil {
			continue
		}
		// A work queue removes a message once its acknowledgement is applied.
		if _, err := f.stream.GetMsg(ctx, seqOf(m)); errors.Is(err, jetstream.ErrMsgNotFound) {
			tk.applied++
		}
	}

	return tk
}

// feedPublishers are a run's publishers and their counters.
type feedPublishers struct {
	started                      time.Time
	rate                         float64
	made, acked, failed, retried atomic.Uint64
	// burstMade counts the feed-convert mode's entries published by the burst's equipment during it.
	burstMade atomic.Uint64
	mu        sync.Mutex
	lat       []feedLatency
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// feedLatency is one acknowledged publish: when its acknowledgement arrived and how long after its publish.
type feedLatency struct {
	at   time.Time
	took time.Duration
}

// publish starts conns connections publishing rate entries a second together, each with inFlight publishes awaiting their acknowledgement,
// round-robin over the equipment; a failed publish is retried once under its message id. Without ids the message id is replaced by an inert header of its length.
func (f *feed) publish(ctx context.Context, pool *feedPool, rate float64, conns, inFlight int, ids bool) *feedPublishers {
	ctx, cancel := context.WithCancel(ctx)
	p := &feedPublishers{started: time.Now(), rate: rate, cancel: cancel}
	offsets := make([]atomic.Int64, feedEquipment)
	every := time.Duration(float64(time.Second) * float64(conns) / rate)
	for c := range conns {
		nc, err := nats.Connect(f.nc.ConnectedUrl())
		if err != nil {
			f.t.Fatal(err)
		}
		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(inFlight), jetstream.WithPublishAsyncTimeout(feedAckTimeout))
		if err != nil {
			f.t.Fatal(err)
		}
		type waiting struct {
			fut  jetstream.PubAckFuture
			sent time.Time
		}
		pending := make(chan waiting, inFlight)
		p.wg.Go(func() {
			defer nc.Close()
			for w := range pending {
				select {
				case <-w.fut.Ok():
					p.acked.Add(1)
					p.record(time.Since(w.sent))
				case <-w.fut.Err(): //nolint:errcheck // any failure is retried once
					p.retried.Add(1)
					if _, err := js.PublishMsg(context.WithoutCancel(ctx), w.fut.Msg()); err != nil {
						p.failed.Add(1)
						continue
					}
					p.acked.Add(1)
					p.record(time.Since(w.sent))
				}
			}
		})
		p.wg.Go(func() {
			defer close(pending)
			tick := time.NewTicker(every)
			defer tick.Stop()
			n := c
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				eq := n % feedEquipment
				n += conns
				off := offsets[eq].Add(1)
				m := pool.message(f.prefix, eq, off, int(off))
				if !ids {
					id := m.Header.Get(jetstream.MsgIDHeader)
					m.Header.Del(jetstream.MsgIDHeader)
					m.Header.Set("Tk-Inert", strings.Repeat("x", len(id)+len(jetstream.MsgIDHeader)-len("Tk-Inert")))
				}
				p.made.Add(1)
				sent := time.Now()
				if inFlight == 1 {
					if _, err := js.PublishMsg(ctx, m); err != nil {
						p.failed.Add(1)
						continue
					}
					p.acked.Add(1)
					p.record(time.Since(sent))

					continue
				}
				fut, err := js.PublishMsgAsync(m)
				if err != nil {
					p.failed.Add(1)
					continue
				}
				pending <- waiting{fut: fut, sent: sent}
			}
		})
	}

	return p
}

func (p *feedPublishers) record(took time.Duration) {
	p.mu.Lock()
	p.lat = append(p.lat, feedLatency{at: time.Now(), took: took})
	p.mu.Unlock()
}

// latencies are the publish latencies acknowledged within [from, to].
func (p *feedPublishers) latencies(from, to time.Time) []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []time.Duration
	for _, l := range p.lat {
		if !l.at.Before(from) && !l.at.After(to) {
			out = append(out, l.took)
		}
	}
	slices.Sort(out)

	return out
}

func (p *feedPublishers) stop() {
	p.cancel()
	p.wg.Wait()
}

// feedSample is one reading of the feed: the publishers' counters, the stream and its consumers, and the process.
type feedSample struct {
	at                           time.Time
	offered                      float64
	made, acked, failed, retried uint64
	acks, inProgress             uint64
	backlog, bytes               uint64
	pendingMax, heldMax          int
	rss, written                 uint64
	stores                       string
}

type feedSampler struct {
	cancel  context.CancelFunc
	done    chan struct{}
	samples []feedSample
}

// sample reads f every loadTick until stopped; srvs, when not nil, are read for their stores.
func (f *feed) sample(ctx context.Context, srvs []*server.Server, p *feedPublishers) *feedSampler {
	ctx, cancel := context.WithCancel(ctx)
	s := &feedSampler{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		tick := time.NewTicker(loadTick)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			fs := feedSample{at: time.Now(), made: p.made.Load(), acked: p.acked.Load(), failed: p.failed.Load(), retried: p.retried.Load(),
				acks: f.acks.Load(), inProgress: f.progress.Load()}
			fs.offered = p.rate * fs.at.Sub(p.started).Seconds()
			if info, err := f.stream.Info(ctx); err == nil {
				fs.backlog, fs.bytes = info.State.Msgs, info.State.Bytes
			}
			for i, c := range f.consumers {
				if ci, err := c.Info(ctx); err == nil {
					fs.pendingMax = max(fs.pendingMax, ci.NumAckPending)
				}
				fs.heldMax = max(fs.heldMax, f.groups[i].heldNow())
			}
			fs.rss, fs.written = procStatus("VmRSS:")*1024, procIO("write_bytes:")
			var stores []string
			for _, srv := range srvs {
				st := streamStore(srv, f.name)
				stores = append(stores, fmt.Sprintf("%s: sealed %s, active %s, other %s; JetStream %s", srv.Name(), mib(st.sealed), mib(st.active), mib(st.meta), mib(dirSize(srv.JetStreamConfig().StoreDir))))
			}
			fs.stores = strings.Join(stores, "<br>")
			s.samples = append(s.samples, fs)
		}
	}()

	return s
}

func (s *feedSampler) stop() []feedSample {
	s.cancel()
	<-s.done

	return s.samples
}

// afterWarmUp are the samples from from on.
func afterWarmUp(samples []feedSample, from time.Time) []feedSample {
	i := slices.IndexFunc(samples, func(s feedSample) bool { return !s.at.Before(from) })
	if i < 0 {
		return samples[len(samples)-1:]
	}

	return samples[i:]
}

// feedVerdict is the feed judged over its flush cycles.
type feedVerdict struct {
	windows                         int
	acked                           float64
	backlogSlope, queueSlope, bound float64
	p50, p99                        time.Duration
	pendingMax, heldMax             int
	sustained                       bool
	why                             string
	// cycles are each window's acknowledged publishes a second, mean backlog, publisher-queue maximum and busiest consumer's pending maximum.
	cycles []feedCycle
}

type feedCycle struct {
	acked, backlog, queue float64
	pending               int
}

// judgeFeed judges samples over whole windows of window (a flush cycle):
// acknowledged publishes at least 98% of rate, the windows' mean backlog and publisher-queue maxima not rising past 0.5% of a window's entries,
// the traffic harness's bound, the backlog by its means since the held entries never drain to zero as the traffic's do,
// publish latency within feedP99 at the 99th percentile, and every consumer's pending acknowledgements under maxAckPending.
func judgeFeed(rate float64, samples []feedSample, lat []time.Duration, window time.Duration, maxAckPending int) feedVerdict {
	var v feedVerdict
	if len(samples) < 2 {
		v.why = "fewer than two samples"
		return v
	}
	t0 := samples[0].at
	v.windows = int(samples[len(samples)-1].at.Sub(t0) / window)
	if v.windows < feedWindows {
		v.why = fmt.Sprintf("%d whole windows, want %d", v.windows, feedWindows)
		return v
	}
	end := t0.Add(time.Duration(v.windows) * window)
	first := samples[0]
	last := first
	means, counts, maxima := make([]float64, v.windows), make([]float64, v.windows), make([]float64, v.windows)
	for _, s := range samples {
		if s.at.After(end) {
			break
		}
		last = s
		w := min(int(s.at.Sub(t0)/window), v.windows-1)
		means[w] += float64(s.backlog)
		counts[w]++
		maxima[w] = max(maxima[w], s.offered-float64(s.acked+s.failed))
		v.pendingMax, v.heldMax = max(v.pendingMax, s.pendingMax), max(v.heldMax, s.heldMax)
		if len(v.cycles) <= w {
			v.cycles = append(v.cycles, feedCycle{acked: float64(s.acked)})
		}
		v.cycles[w].pending = max(v.cycles[w].pending, s.pendingMax)
		v.cycles[w].queue = maxima[w]
	}
	for w := range v.cycles {
		next := float64(last.acked)
		if w+1 < len(v.cycles) {
			next = v.cycles[w+1].acked
		}
		v.cycles[w].acked = (next - v.cycles[w].acked) / window.Seconds()
	}
	v.acked = float64(last.acked-first.acked) / last.at.Sub(first.at).Seconds()
	for w := range means {
		means[w] /= max(counts[w], 1)
		if w < len(v.cycles) {
			v.cycles[w].backlog = means[w]
		}
	}
	v.backlogSlope, v.queueSlope = slope(means), slope(maxima)
	v.bound = 0.005 * v.acked * window.Seconds()
	v.p50, v.p99 = percentile(lat, 0.5), percentile(lat, 0.99)
	switch {
	case v.acked < 0.98*rate:
		v.why = fmt.Sprintf("acknowledged %.1f/s against %.1f/s offered", v.acked, rate)
	case v.backlogSlope >= v.bound:
		v.why = fmt.Sprintf("the mean backlog rises %.2f a window, the bound %.2f", v.backlogSlope, v.bound)
	case v.queueSlope >= v.bound:
		v.why = fmt.Sprintf("the publishers' queue rises %.2f a window, the bound %.2f", v.queueSlope, v.bound)
	case v.p99 > feedP99:
		v.why = fmt.Sprintf("publish p99 %s, the budget %s", v.p99, feedP99)
	case v.pendingMax >= maxAckPending:
		v.why = fmt.Sprintf("a consumer's pending acknowledgements reached %d, its limit %d", v.pendingMax, maxAckPending)
	default:
		v.sustained = true
	}

	return v
}

// feedStore is one server's store of a stream: its sealed blocks, its active block and the rest.
type feedStore struct {
	sealedBlocks         int
	sealed, active, meta uint64
}

// streamStore reads srv's store of stream name: the highest-numbered block is the active one.
func streamStore(srv *server.Server, name string) feedStore {
	var st feedStore
	var root string
	_ = filepath.WalkDir(srv.JetStreamConfig().StoreDir, func(path string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() && d.Name() == name && filepath.Base(filepath.Dir(path)) == "streams" {
			root = path
			return filepath.SkipAll
		}

		return nil
	})
	if root == "" {
		return st
	}
	type blk struct {
		n    int
		size uint64
	}
	var blks []blk
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // a file removed while walking
		}
		info, err := d.Info()
		if err != nil {
			return nil //nolint:nilerr // a file removed while walking
		}
		size := uint64(info.Size()) //nolint:gosec // a file's size
		if n, err := strconv.Atoi(strings.TrimSuffix(d.Name(), ".blk")); err == nil && strings.HasSuffix(d.Name(), ".blk") {
			blks = append(blks, blk{n: n, size: size})
			return nil
		}
		st.meta += size

		return nil
	})
	slices.SortFunc(blks, func(a, b blk) int { return a.n - b.n })
	for i, b := range blks {
		if i == len(blks)-1 {
			st.active = b.size
			continue
		}
		st.sealed += b.size
		st.sealedBlocks++
	}

	return st
}

// procStatus is the value of field in /proc/self/status, in its unit; 0 when unread.
func procStatus(field string) uint64 { return procField("/proc/self/status", field) }

// procIO is the value of field in /proc/self/io; 0 when unread.
func procIO(field string) uint64 { return procField("/proc/self/io", field) }

func procField(path, field string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), field); ok {
			n, _ := strconv.ParseUint(strings.Fields(rest)[0], 10, 64)
			return n
		}
	}

	return 0
}

// writeHeapProfile writes the process's heap profile to path, for the held entries' and the servers' share of its memory.
func writeHeapProfile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path) //nolint:gosec // the harness's own profile directory
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
}

// dirSize is the bytes of the files under dir.
func dirSize(dir string) uint64 {
	var n uint64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // a file removed while walking
		}
		if info, err := d.Info(); err == nil {
			n += uint64(info.Size()) //nolint:gosec // a file's size
		}

		return nil
	})

	return n
}

// pendingSpread is the group consumers' pending acknowledgements now: the lowest, the median and the highest.
func (f *feed) pendingSpread(ctx context.Context) string {
	var ps []int
	for _, c := range f.consumers {
		if ci, err := c.Info(ctx); err == nil {
			ps = append(ps, ci.NumAckPending)
		}
	}
	if len(ps) == 0 {
		return "unread"
	}
	slices.Sort(ps)

	return fmt.Sprintf("%d / %d / %d", ps[0], ps[len(ps)/2], ps[len(ps)-1])
}

func maxOf(samples []feedSample, f func(feedSample) uint64) uint64 {
	var m uint64
	for _, s := range samples {
		m = max(m, f(s))
	}

	return m
}

func mib(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

// A feed passes only at its rate, with neither its backlog nor its publishers' queue rising, within its latency and pending budgets.
func TestJudgeFeed(t *testing.T) {
	t0 := time.Unix(0, 0)
	steady := func(acked func(i int) uint64, backlog func(i int) uint64) []feedSample {
		out := make([]feedSample, 0, 31)
		for i := range 31 {
			out = append(out, feedSample{at: t0.Add(time.Duration(i) * time.Second), offered: 100 * float64(i), acked: acked(i), backlog: backlog(i), pendingMax: 10})
		}

		return out
	}
	full := func(i int) uint64 { return uint64(100 * i) } //nolint:gosec // a test index
	flat := func(int) uint64 { return 50 }
	lat := []time.Duration{time.Millisecond, 2 * time.Millisecond}
	cases := []struct {
		name    string
		samples []feedSample
		lat     []time.Duration
		pending int
		want    string
	}{
		{"sustained", steady(full, flat), lat, 100, ""},
		{"half the rate", steady(func(i int) uint64 { return uint64(50 * i) }, flat), lat, 100, "acknowledged"},      //nolint:gosec // a test index
		{"backlog rising", steady(full, func(i int) uint64 { return uint64(50 + 20*i) }), lat, 100, "backlog"},       //nolint:gosec // a test index
		{"queue drifting", steady(func(i int) uint64 { return uint64(98.5 * float64(i)) }, flat), lat, 100, "queue"}, //nolint:gosec // a test index
		{"slow publishes", steady(full, flat), []time.Duration{2 * time.Second}, 100, "p99"},
		{"pending at its limit", steady(full, flat), lat, 10, "pending"},
		{"too short", steady(full, flat)[:15], lat, 100, "whole windows"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := judgeFeed(100, c.samples, c.lat, 10*time.Second, c.pending)
			if c.want == "" != v.sustained || !strings.Contains(v.why, c.want) {
				t.Errorf("judgeFeed: sustained %v, why %q; want %q", v.sustained, v.why, c.want)
			}
		})
	}
}
