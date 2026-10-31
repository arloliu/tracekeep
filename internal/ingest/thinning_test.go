package ingest

import (
	"bytes"
	"context"
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/sim"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/producertest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// thinnedTraffic runs the simulator with every third poll kept for n polls and returns its records as the bus carried them,
// with the capture's descriptor: the SDK's own thinned records and their declaration, not a test's imitation.
func thinnedTraffic(t *testing.T, n int) (tracepack.UUID, []byte, []tracepack.Record) {
	t.Helper()
	bus := producertest.NewBus()
	p, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects"}, producer.WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	l, err := sim.NewLoopback(p, sim.Options{
		ToolID: "SIM-TOOL", DeviceName: "eqp", SessionID: 1, T3: time.Second,
		Thinning: &producer.SamplingRule{KeepEveryN: 3, KeepInterval: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if err := l.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := l.Traffic(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var (
		capture    tracepack.UUID
		descriptor []byte
		recs       []tracepack.Record
	)
	for i, data := 1, 0; data < 2*n; i++ {
		msgs, err := bus.WaitMessages(ctx, i)
		if err != nil {
			t.Fatalf("%d data records on the bus: %v", data, err)
		}
		m := &msgs[i-1]
		br, err := tpbus.CheckMessage(tpbus.Envelope{
			Layout: m.Header[tpbus.HeaderLayout], MsgID: m.MsgID, Descriptor: m.Header[tpbus.HeaderDescriptor],
		}, m.Data)
		if err != nil {
			t.Fatal(err)
		}
		if br.Descriptor != nil {
			capture, descriptor = br.CaptureID, br.Descriptor
		}
		if br.Record.Kind == tracepack.KindData {
			data++
		}
		recs = append(recs, br.Record)
	}
	l.Stop(t.Context())

	return capture, descriptor, recs
}

// The gate's thinned path (masking-keys.md §9, producer.md §4.6): under a policy whose rules match S1F3 and S1F4,
// a record the SDK thinned is stored unobserved, unmasked and with no entry, and a kept one is masked;
// the hour merges without a false conflict, a thinned record is not restorable, a kept one restores,
// and the capture's listing names the sampling rule its descriptor declares.
// A wrong unobserved argument to Mask shows either way: a thinned record masked, or a kept one stored in clear.
func TestThinnedRecordsThroughMaskingAndTheMerge(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		pol := policy.Baseline()
		pol.ItemRules = append(pol.ItemRules,
			policy.ItemRule{Stream: 1, Function: 3, Path: "1", Domain: "poll"}, policy.ItemRule{Stream: 1, Function: 4, Path: "1", Domain: "poll"})
		k := activeMaskingOf(t, p, &pol)
		capture, descriptor, recs := thinnedTraffic(t, 12)

		feedInTwoFlushes(t, p, capture, descriptor, recs)

		sent, data := map[uint64]*tracepack.Record{}, 0
		for i := range recs {
			sent[recs[i].Seq] = &recs[i]
			if recs[i].Kind == tracepack.KindData {
				data++
			}
		}
		thinned, kept := checkThinnedStorage(t, p, sent)
		if len(thinned) == 0 || len(kept) == 0 || len(thinned)+len(kept) != data {
			t.Fatalf("%d thinned and %d kept data records stored, want both and every one of the %d sent", len(thinned), len(kept), data)
		}

		last := recs[len(recs)-1].TSUTCNs
		mergeToTheArchives(t, c, p, recs)
		if th, kp := checkThinnedStorage(t, p, sent); !maps.Equal(th, thinned) || !maps.Equal(kp, kept) {
			t.Errorf("the archive holds %d thinned and %d kept data records, want the segments' %d and %d", len(th), len(kp), len(thinned), len(kept))
		}

		restoreThinned(t, served(t, p, k, slog.New(slog.DiscardHandler)), capture, sent, thinned, kept)

		listed, err := c.ListCaptures(t.Context(), catalog.Selector{ToolID: "SIM-TOOL", Capture: &capture, From: last - int64(time.Hour), To: last + int64(time.Hour)}, time.Minute)
		if err != nil || len(listed) != 1 || len(listed[0].Thinning) != 2 {
			t.Fatalf("ListCaptures = %+v, %v; want the capture with its S1F3 and S1F4 entries", listed, err)
		}
		for _, th := range listed[0].Thinning {
			if th.KeepEveryN == nil || *th.KeepEveryN != 3 || th.KeepIntervalMs == nil || *th.KeepIntervalMs != 3600000 {
				t.Errorf("listed thinning %+v, want keep every 3 and an hour", th)
			}
		}
	})
}

// mergeToTheArchives merges every hour recs fall in, with no conflict, then deletes the segments,
// so one archive for each of those hours is all p stores; the simulator's records carry the wall clock, so they may span two hours.
func mergeToTheArchives(t *testing.T, c *catalog.Catalog, p *pipeline, recs []tracepack.Record) {
	t.Helper()
	hours := map[int64]bool{}
	for i := range recs {
		hours[HourOf(recs[i].TSUTCNs)] = true
	}
	last := recs[len(recs)-1].TSUTCNs
	lease, ok, err := c.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	at := time.Unix(0, last).Add(3 * time.Hour)
	r := &compact.Compactor{
		Catalog: c, Store: p.st, HorizonHour: func() int64 { return HourOf(last) - 24 }, Log: slog.New(slog.DiscardHandler),
		WorkDir: t.TempDir(), Writer: "tracekeep-test", QuietPeriod: 15 * time.Minute, DeleteDelay: time.Minute,
		Now: func() time.Time { return at },
	}
	if rep, err := r.Conflicts(t.Context(), lease); err != nil || rep.Confirmed+rep.Failed+rep.Changed != 0 {
		t.Fatalf("Conflicts = %+v, %v; want no conflict", rep, err)
	}
	if rep, err := r.Merges(t.Context(), lease); err != nil || len(rep.Merged) == 0 || len(rep.Failed)+len(rep.Conflicted)+len(rep.Stale) != 0 {
		t.Fatalf("Merges = %+v, %v; want the hour merged", rep, err)
	}
	at = time.Now().Add(time.Hour) // the commit fixes delete_at by the catalog's own clock
	if err := r.Deletions(t.Context(), lease, &compact.RetentionReport{}); err != nil {
		t.Fatal(err)
	}
	stored := storedKeys(t, p.st)
	for _, k := range stored {
		if !strings.HasPrefix(k, "archive/") {
			t.Errorf("%s stored after the deletion, want archives alone", k)
		}
	}
	if len(stored) != len(hours) {
		t.Fatalf("%d objects after the deletion, want an archive for each of the %d hours", len(stored), len(hours))
	}
}

// feedInTwoFlushes feeds recs, inline and by claim-check by turns, flushing halfway and at the end,
// so the hour holds two segments for the merge.
func feedInTwoFlushes(t *testing.T, p *pipeline, capture tracepack.UUID, descriptor []byte, recs []tracepack.Record) {
	t.Helper()
	for i := range recs {
		var d []byte
		if recs[i].Seq == 0 {
			d = descriptor
		}
		m := inline(t, capture, recs[i], d)
		if i%2 == 1 {
			m = claimCheck(t, p.cl, capture, recs[i], d)
		}
		p.feed(t, m)
		if i == len(recs)/2 {
			p.flushAll(t)
		}
	}
	p.flushAll(t)
}

// restoreThinned checks a thinned record is not restorable and a kept one restores to the record as sent.
func restoreThinned(t *testing.T, cl tracekeepv1connect.MachineServiceClient, capture tracepack.UUID, sent map[uint64]*tracepack.Record, thinned, kept map[uint64]bool) {
	t.Helper()
	restore := func(seq uint64) (*connect.Response[tkv1.RestoreResponse], error) {
		return cl.Restore(t.Context(), bearer(&tkv1.RestoreRequest{
			CaptureId: capture.String(), Seq: seq, Hour: timestamppb.New(time.Unix(HourOf(sent[seq].TSUTCNs)*3600, 0)),
		}, restorerToken))
	}
	for seq := range thinned {
		if _, err := restore(seq); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("restore of thinned seq %d = %v, want not restorable", seq, err)
		}
	}
	for seq := range kept {
		if resp, err := restore(seq); err != nil || !bytes.Equal(resp.Msg.GetPayload(), sent[seq].Payload) {
			t.Errorf("restore of kept seq %d = %v; want the record as sent", seq, err)
		}
	}
}

// checkThinnedStorage reads every stored pack and returns the data seqs stored thinned and kept:
// a thinned one is the SDK's record, unobserved, unredacted and without entry; a kept one is redacted with an entry of each policy rule's domain.
func checkThinnedStorage(t *testing.T, p *pipeline, sent map[uint64]*tracepack.Record) (thinned, kept map[uint64]bool) {
	t.Helper()
	thinned, kept = map[uint64]bool{}, map[uint64]bool{}
	for _, key := range storedKeys(t, p.st) {
		entries := map[uint64]int{}
		for _, e := range header(t, p, key).Meta.Redaction {
			entries[e.Seq]++
		}
		for _, r := range records(t, p.st, key) {
			if r.Kind != tracepack.KindData {
				continue
			}
			in := sent[r.Seq]
			switch {
			case in.Quality.HasAny(tracepack.QualityUnobserved):
				thinned[r.Seq] = true
				if !bytes.Equal(r.Payload, in.Payload) || r.Quality.HasAny(tracepack.QualityRedacted) || !r.Quality.HasAny(tracepack.QualityUnobserved) || entries[r.Seq] != 0 {
					t.Errorf("%s: thinned seq %d stored with quality %v and %d entries, or changed", key, r.Seq, r.Quality, entries[r.Seq])
				}
			default:
				kept[r.Seq] = true
				if !r.Quality.HasAny(tracepack.QualityRedacted) || entries[r.Seq] == 0 || bytes.Equal(r.Payload, in.Payload) {
					t.Errorf("%s: kept seq %d stored with quality %v and %d entries, or in clear", key, r.Seq, r.Quality, entries[r.Seq])
				}
			}
		}
	}

	return thinned, kept
}

// activeMaskingOf stages and activates the masking configuration of policy pol in p's catalog, under d-1 and m-1,
// beside the pipeline's policy-less one, so the captures admitted after bind it; it returns the keys p masks with.
func activeMaskingOf(t *testing.T, p *pipeline, pol *policy.Policy) *keys.Keys {
	t.Helper()
	var plain int64
	for id := range p.x.Configs {
		plain = id
	}
	mcfg := maskingConfig(t)
	mcfg.Policy = pol
	var err error
	if mcfg.Masking, err = pol.Compile("d-1", "m-1"); err != nil {
		t.Fatal(err)
	}
	id := stageMasking(t, p.cat, &mcfg)
	p.x.Configs[id], p.f.Configs[id] = mcfg, mcfg
	k := maskingKeys(t, p.cat)
	p.x.Keys = k
	if _, err := p.cat.Register(t.Context(), "r", []int64{plain, id}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := p.cat.ActivateStaged(t.Context(), id, nil, nil); err != nil {
		t.Fatal(err)
	}

	return k
}
