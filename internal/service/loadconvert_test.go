package service

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/fuda"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"
	"github.com/arloliu/go-secs/v2/sml"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/testbus"
)

// The feed-convert mode is the host log feed's gate load (docs/plans/host-log-feed.md step 10b), at ingest's own times:
// a fleet shaped as the owner sized it, its feed converted by dev's own log stage on the three-node cluster beside production's traffic.
// 1,980 equipment log as the sampled ones do, about 1.2 messages a second of about 6 KB each; 20 heavy ones log 20 GB a day zipped, about 3 MB/s;
// after the warm-up, 200 of them compare their recipes for three minutes, 10 messages of 50 KB a second each, a burst the stream holds for the stage to digest.
// It judges the publishes all stored, the burst digested within two flushes of its end, the traffic's own verdict,
// and every stored entry accounted for exactly once: converted into a registered record or counted with an outcome.

// fleetShape is the fleet's messages a second by kind of equipment, each message a header entry and a body entry, and its burst.
type fleetShape struct {
	equipment, heavy      int
	normalRate, heavyRate float64
	body                  int
	burstEquipment        int
	burstRate             float64
	burstBody             int
	burstFor              time.Duration
	// ackPending is each group consumer's max_ack_pending, above twice its busiest group's entries in a reorder window (logfeed.md §15.10); zero keeps the mode's.
	ackPending int
}

// fleetOf is the mode's fleet: the owner's sizing, or a smoke's tenth of the equipment's rates, a small burst and a short one.
func fleetOf(smoke bool) fleetShape {
	if smoke {
		return fleetShape{equipment: feedEquipment, heavy: 2, normalRate: 0.12, heavyRate: 50, body: 5500, burstEquipment: 20, burstRate: 5, burstBody: 50_000, burstFor: 20 * time.Second}
	}

	return fleetShape{equipment: feedEquipment, heavy: 20, normalRate: 1.2, heavyRate: 500, body: 5500, burstEquipment: 200, burstRate: 10, burstBody: 50_000, burstFor: 3 * time.Minute,
		ackPending: 250_000}
}

// rate is the fleet's messages a second outside the burst.
func (s fleetShape) rate() float64 {
	return float64(s.equipment-s.heavy)*s.normalRate + float64(s.heavy)*s.heavyRate
}

// isHeavy and inBurst spread the heavy equipment and the burst's over the publishers' connections;
// the heavy equipment's IDs hash into 16 of the 32 groups, at most three in one, as random IDs about do.
func (s fleetShape) isHeavy(eq int) bool { return eq%89 == 0 && eq/89 < s.heavy }

func (s fleetShape) inBurst(eq int) bool {
	return !s.isHeavy(eq) && eq%10 == 7 && eq/10 < s.burstEquipment
}

// loadFeedConvert runs the fleet through dev's log stage beside production's traffic, its burst after the warm-up,
// then drains the feed and checks every entry's account.
func loadFeedConvert(t *testing.T, smoke bool) {
	ft := feedModeTimes(smoke, !smoke)
	fs := fleetOf(smoke)
	srvs := testbus.StartClusterServers(t, 3)
	urls := make([]string, len(srvs))
	for i, s := range srvs {
		urls[i] = s.ClientURL()
	}
	url := strings.Join(urls, ",")
	pool, burstPool := newSizedFeedPool(t, fs.body), newSizedFeedPool(t, fs.burstBody)
	h := newReportingHarness(t, loadSpec{flush: ft.flush, ackWait: ft.ackWait, probed: true, maxAckPending: ft.trafficLimit, bus: url,
		feed: func(l *config.Logfeed) {
			l.Stream, l.Prefix, l.LeaseGroups, l.MaxBytes = feedStream, feedPrefix, feedGroups, fuda.ByteSize(feedMaxBytes)
			l.DuplicateWindow, l.MaxAckPending, l.ReorderWindow, l.FlushInterval = ft.dupWindow, cmp.Or(fs.ackPending, ft.maxAckPending), ft.reorder, ft.flush
		}})
	h.log(fmt.Sprintf("a fleet converted by dev's log stage on a three-node cluster, catalog %s: flush %s, ack_wait %s, reorder window %s, %d groups, max_ack_pending %d; "+
		"%d equipment at %.2f msg/s and %d at %.0f msg/s, %.0f msg/s in all, a body of %s and its entries %s a message; "+
		"a burst of %d equipment at %.0f msg/s of %s for %s; traffic %.0f fwd/s over K = %d",
		h.engine, ft.flush, ft.ackWait, ft.reorder, feedGroups, cmp.Or(fs.ackPending, ft.maxAckPending), fs.equipment-fs.heavy, fs.normalRate, fs.heavy, fs.heavyRate, fs.rate(),
		kib(fs.body), kib(pool.pairBytes()), fs.burstEquipment, fs.burstRate, kib(burstPool.pairBytes()), fs.burstFor, ft.trafficRate, ft.k))
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
	s, err := js.Stream(ctx, feedStream)
	if err != nil {
		t.Fatal(err)
	}

	equipment := h.startEquipment(ft.k)
	defer h.stopEquipment(equipment)
	cpu0 := processCPU(t)
	start := time.Now()
	burstAt := start.Add(ft.flush + ft.reorder)
	pubs := publishFleet(ctx, url, fs, pool, burstPool, burstAt)
	backlog := sampleBacklog(ctx, s)
	progress(ctx, t, h, s, pubs)
	tv, failed, ackPending := h.cycles(equipment, ft.trafficRate, profileDir(t))
	burstEnd := burstAt.Add(fs.burstFor)
	if wait := time.Until(burstEnd.Add(2*ft.flush + ft.reorder)); wait > 0 {
		time.Sleep(wait)
	}
	pubs.stop()
	ran := time.Since(pubs.started)
	stopped := time.Now()
	remaining, drained := drainFeed(t, s, 2*ft.flush+ft.reorder+ft.ackWait)
	samples := backlog.stop()
	cpu := (processCPU(t) - cpu0).Seconds() / time.Since(start).Seconds()
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	acc := accountFeed(t, h, info.State.LastSeq, remaining)

	base, peak, digested := digest(samples, burstAt, burstEnd)
	made := float64(pubs.made.Load()) / ran.Seconds()
	lat := pubs.latencies(start, stopped)
	p99 := time.Duration(0)
	if len(lat) > 0 {
		p99 = lat[len(lat)*99/100]
	}
	h.log("", "| offered entries/s | made entries/s | failed | retried | publish p99 | backlog before the burst | burst's peak | digested in | drained in | process CPU (cores) |",
		"|---|---|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %.0f | %d | %d | %s | %s | %s | %s | %s | %.2f |", 2*fs.rate(), made, pubs.failed.Load(), pubs.retried.Load(),
			p99.Round(time.Microsecond), base, peak, digested.Round(time.Second), time.Since(stopped).Round(time.Millisecond), cpu))
	h.log(append([]string{""}, acc.table()...)...)
	if pubs.failed.Load() != 0 || made < 0.98*2*fs.rate() {
		t.Errorf("the publishers: %d failed, %.0f entries made a second of %.0f offered", pubs.failed.Load(), made, 2*fs.rate())
	}
	if digested < 0 || digested > 2*ft.flush {
		t.Errorf("the burst was not digested within two flushes of its end: %s", digested)
	}
	if !drained || remaining != 0 {
		t.Errorf("the feed did not drain within two flushes, the reorder window and ack_wait: %d entries left", remaining)
	}
	if why := productionVerdict(tv, failed, ackPending, ft.trafficLimit, false); why != "" {
		t.Errorf("the traffic beside the converted feed: %s", why)
	}
	// Every message published is a header and its body, consecutive in its file, so a healthy run pairs each and finds none late.
	if o := acc.outcomes; o["stale"]+o["annotation"]+o["unreadable"]+o["beyond-retention"] != 0 {
		t.Errorf("entries not converted into a record: %.0f stale, %.0f annotations, %.0f unreadable, %.0f beyond retention",
			o["stale"], o["annotation"], o["unreadable"], o["beyond-retention"])
	}
	for _, why := range acc.problems {
		t.Error(why)
	}
}

// progress writes a line a minute to stderr, which a test's timeout does not swallow as it does the report.
func progress(ctx context.Context, t *testing.T, h *loadHarness, s jetstream.Stream, p *feedPublishers) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done })
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			var msgs, bytes uint64
			replicas := 0
			if info, err := s.Info(ctx); err == nil {
				msgs, bytes, replicas = info.State.Msgs, info.State.Bytes, info.Config.Replicas
			}
			lo, hi, capped := -1, 0, 0
			for n := range feedGroups {
				ci, err := s.Consumer(ctx, bus.FeedConsumer(n))
				if err != nil {
					continue
				}
				info, err := ci.Info(ctx)
				if err != nil {
					continue
				}
				a := info.NumAckPending
				if lo < 0 || a < lo {
					lo = a
				}
				hi = max(hi, a)
				if a >= info.Config.MaxAckPending {
					capped++
				}
			}
			if dir := os.Getenv("TRACEKEEP_LOAD_PROFILES"); dir != "" {
				if f, err := os.Create(filepath.Join(dir, fmt.Sprintf("feed-heap-%s.pprof", time.Since(p.started).Round(time.Minute)))); err == nil {
					_ = pprof.Lookup("heap").WriteTo(f, 0)
					_ = f.Close()
				}
			}
			converted := 0.0
			if mfs, err := h.d.Env.Metrics.Registry().Gather(); err == nil {
				for _, mf := range mfs {
					if mf.GetName() == "tracekeep_logfeed_entries_total" {
						for _, m := range mf.GetMetric() {
							converted += m.GetCounter().GetValue()
						}
					}
				}
			}
			fmt.Fprintf(os.Stderr, "progress %s: made %d acked %d failed %d retried %d; stream %d msgs %s R%d; settled %.0f; ack pending %d..%d, %d groups capped\n",
				time.Since(p.started).Round(time.Second), p.made.Load(), p.acked.Load(), p.failed.Load(), p.retried.Load(), msgs, kib(int(bytes)), replicas,
				converted, lo, hi, capped)
		}
	}()
}

func kib(b int) string { return fmt.Sprintf("%.1f KiB", float64(b)/1024) }

// newSizedFeedPool is 50 S6F11 messages whose body's SML text is at least body bytes, each a header entry and a body entry.
func newSizedFeedPool(t *testing.T, body int) *feedPool {
	t.Helper()
	r := rand.New(rand.NewPCG(3, uint64(body))) //nolint:gosec // synthetic data
	p := &feedPool{}
	for i := range 50 {
		var reports []secs2.Item
		var m *hsms.DataMessage
		for {
			vals := make([]secs2.Item, 0, 10)
			for v := range 10 {
				if v%2 == 0 {
					vals = append(vals, secs2.NewASCIIItem(fmt.Sprintf("LOT%06d.%02d", r.IntN(999_999), r.IntN(25))))
				} else {
					vals = append(vals, secs2.NewFloatItem(4, r.Float32()*1000))
				}
			}
			reports = append(reports, secs2.NewListItem(secs2.NewUintItem(4, 1000+len(reports)), secs2.NewListItem(vals...)))
			var err error
			m, err = hsms.NewDataMessage(6, 11, true, 1, [4]byte{0, 0, byte(i >> 8), byte(i)},
				secs2.NewListItem(secs2.NewUintItem(4, i), secs2.NewUintItem(4, 2000+r.IntN(50)), secs2.NewListItem(reports...)))
			if err != nil {
				t.Fatal(err)
			}
			if len(sml.MustEncodeMessage(m)) >= body {
				break
			}
		}
		hdr := m.HeaderBytes()
		p.entries = append(p.entries,
			feedEntry{Kind: "header", Dir: "sent", Device: "1", Header: hex.EncodeToString(hdr[:]), TID: strconv.Itoa(i), SxFy: "S6F11"},
			feedEntry{Kind: "body", TID: strconv.Itoa(i), SxFy: "S6F11", Text: sml.MustEncodeMessage(m)})
	}

	return p
}

// pairBytes is a message's two entries' mean size as published.
func (p *feedPool) pairBytes() int {
	n := 0
	for i := range p.entries {
		n += len(p.message(feedPrefix, 0, int64(i), i).Data)
	}

	return 2 * n / len(p.entries)
}

// publishFleet publishes fs's messages from feedConns connections, each its own equipment's, a message being its header entry then its body entry
// at the next two offsets of its equipment's file; the burst's equipment add their burst's messages from burstAt for fs.burstFor.
// Every publish awaits its acknowledgement, at most feedInFlight a connection, and a failed one is retried once.
func publishFleet(ctx context.Context, url string, fs fleetShape, pool, burstPool *feedPool, burstAt time.Time) *feedPublishers {
	ctx, cancel := context.WithCancel(ctx)
	p := &feedPublishers{started: time.Now(), rate: 2 * fs.rate(), cancel: cancel}
	for c := range feedConns {
		p.wg.Go(func() { publishConn(ctx, url, c, fs, pool, burstPool, burstAt, p) })
	}

	return p
}

func publishConn(ctx context.Context, url string, c int, fs fleetShape, pool, burstPool *feedPool, burstAt time.Time, p *feedPublishers) {
	nc, err := nats.Connect(url)
	if err != nil {
		p.failed.Add(1)
		return
	}
	defer nc.Close()
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(feedInFlight), jetstream.WithPublishAsyncTimeout(feedAckTimeout))
	if err != nil {
		p.failed.Add(1)
		return
	}
	type waiting struct {
		fut  jetstream.PubAckFuture
		sent time.Time
	}
	pending := make(chan waiting, feedInFlight)
	var acks sync.WaitGroup
	acks.Go(func() {
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
	defer func() {
		close(pending)
		acks.Wait()
	}()
	var mine []int
	for eq := c; eq < fs.equipment; eq += feedConns {
		mine = append(mine, eq)
	}
	credit, offset, sent := make([]float64, len(mine)), make([]int64, len(mine)), make([]int, len(mine))
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			dt := now.Sub(last).Seconds()
			last = now
			burst := !now.Before(burstAt) && now.Before(burstAt.Add(fs.burstFor))
			for i, eq := range mine {
				rate, from := fs.normalRate, pool
				if fs.isHeavy(eq) {
					rate = fs.heavyRate
				}
				if burst && fs.inBurst(eq) {
					rate, from = rate+fs.burstRate, burstPool
				}
				credit[i] += rate * dt
				for ; credit[i] >= 1; credit[i]-- {
					// A message's header and body come from one pool, as consecutive entries of its file.
					n := 2 * sent[i]
					sent[i]++
					for k := range 2 {
						offset[i]++
						m := from.message(feedPrefix, eq, offset[i], n+k)
						p.made.Add(1)
						fut, err := js.PublishMsgAsync(m)
						if err != nil {
							// A stalled publish is retried once, as one whose acknowledgement failed is.
							p.retried.Add(1)
							if _, err := js.PublishMsg(context.WithoutCancel(ctx), m); err != nil {
								p.failed.Add(1)
							} else {
								p.acked.Add(1)
							}

							continue
						}
						pending <- waiting{fut: fut, sent: time.Now()}
					}
				}
			}
		}
	}
}

// digest reads the burst from samples, in the stream's bytes, which the burst's large messages fill more than its count:
// the backlog's mean over the minute before it, its peak, and how long after its end the backlog came back to within a fifth above that mean,
// -1 when it never did.
func digest(samples []backlogSample, at, end time.Time) (base, peak string, took time.Duration) {
	var sum, n float64
	var top uint64
	took = -1
	for _, s := range samples {
		switch {
		case s.at.Before(at.Add(-time.Minute)):
		case s.at.Before(at):
			sum += float64(s.bytes)
			n++
		case !s.at.After(end):
			top = max(top, s.bytes)
		default:
			top = max(top, s.bytes)
			if took < 0 && n > 0 && float64(s.bytes) <= 1.2*sum/n {
				took = s.at.Sub(end)
			}
		}
	}
	mean := 0.0
	if n > 0 {
		mean = sum / n
	}

	return fmt.Sprintf("%.0f MiB", mean/(1<<20)), fmt.Sprintf("%d MiB", top>>20), took
}

// backlogSample is one reading of the feed stream's bytes.
type backlogSample struct {
	at    time.Time
	bytes uint64
}

// backlogSampler reads the feed stream's messages every loadTick.
type backlogSampler struct {
	cancel  context.CancelFunc
	done    chan struct{}
	samples []backlogSample
}

func sampleBacklog(ctx context.Context, s jetstream.Stream) *backlogSampler {
	ctx, cancel := context.WithCancel(ctx)
	b := &backlogSampler{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(b.done)
		tick := time.NewTicker(loadTick)
		defer tick.Stop()
		for {
			if info, err := s.Info(ctx); err == nil {
				b.samples = append(b.samples, backlogSample{at: time.Now(), bytes: info.State.Bytes})
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()

	return b
}

func (b *backlogSampler) stop() []backlogSample {
	b.cancel()
	<-b.done

	return b.samples
}

// drainFeed waits up to within for the feed stream to hold nothing, every message published being stored and paired,
// and returns what it holds: an equipment's last entries leave their window up to the reorder window after the publishers stop,
// and its flush after them up to F later, so no smaller remainder is final before the deadline.
func drainFeed(t *testing.T, s jetstream.Stream, within time.Duration) (uint64, bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		info, err := s.Info(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		n := info.State.Msgs
		if n == 0 {
			return n, true
		}
		if time.Now().After(deadline) {
			return n, false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// feedAccount is the feed's entries by outcome and the catalog's records, by equipment, and what does not add up.
type feedAccount struct {
	outcomes    map[string]float64
	annotations float64
	records     int64
	problems    []string
}

// accountFeed reads the log stage's counters and each piece of equipment's capture: every stored entry was converted, counted with an outcome,
// or is still held, the stored being the stream's last sequence, every publish retried under its message id stored once;
// and each capture's records are its equipment's converted data records, two entries each, and its annotations.
func accountFeed(t *testing.T, h *loadHarness, stored, held uint64) *feedAccount {
	t.Helper()
	mfs, err := h.d.Env.Metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	type key struct{ equipment, label string }
	entries, annotations := map[key]float64{}, map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			switch mf.GetName() {
			case "tracekeep_logfeed_entries_total":
				entries[key{labels["equipment_id"], labels["outcome"]}] += m.GetCounter().GetValue()
			case "tracekeep_logfeed_annotations_total":
				annotations[labels["equipment_id"]] += m.GetCounter().GetValue()
			default:
			}
		}
	}
	a := &feedAccount{outcomes: map[string]float64{}}
	for k, v := range entries {
		a.outcomes[k.label] += v
	}
	for _, v := range annotations {
		a.annotations += v
	}
	settled := a.outcomes["record"] + a.outcomes["annotation"] + a.outcomes["stale"] + a.outcomes["beyond-retention"] + a.outcomes["unreadable"]
	if settled+float64(held) != float64(stored) {
		a.problems = append(a.problems, fmt.Sprintf("%.0f entries settled and %d held, of %d stored: %+.0f unaccounted", settled, held, stored, float64(stored)-settled-float64(held)))
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for eq := range feedEquipment {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			id := fmt.Sprintf("FEED-%04d", eq)
			cp, ok, err := h.d.Env.Catalog.LogCaptureOf(t.Context(), catalog.LogKey{
				LogFile: catalog.LogFile{Source: feedSource, EquipmentID: id, File: "host-20261107.log"}, Device: "eqp",
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil || !ok || !cp.HasHighestSeq {
				a.problems = append(a.problems, fmt.Sprintf("%s's capture = %v, %v", id, ok, err))
				return
			}
			a.records += cp.HighestSeq
			if want := entries[key{id, "record"}]/2 + annotations[id]; float64(cp.HighestSeq) != want {
				a.problems = append(a.problems, fmt.Sprintf("%s's capture holds %d records, the stage converted %.0f", id, cp.HighestSeq, want))
			}
		})
	}
	wg.Wait()

	return a
}

func (a *feedAccount) table() []string {
	o := a.outcomes

	return []string{
		"| record entries | annotation entries | duplicate | stale | beyond retention | unreadable | annotations | registered records |",
		"|---|---|---|---|---|---|---|---|",
		fmt.Sprintf("| %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %d |", o["record"], o["annotation"], o["duplicate"], o["stale"],
			o["beyond-retention"], o["unreadable"], a.annotations, a.records),
	}
}
