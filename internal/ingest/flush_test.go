package ingest

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// msg records what was done to one message.
type msg struct {
	mu   sync.Mutex
	acks []string
}

func (m *msg) do(s string) error {
	m.mu.Lock()
	m.acks = append(m.acks, s)
	m.mu.Unlock()

	return nil
}

func (m *msg) Ack() error                         { return m.do("ack") }
func (m *msg) DoubleAck(context.Context) error    { return m.do("double-ack") }
func (m *msg) NakWithDelay(d time.Duration) error { return m.do("nak " + d.String()) }
func (m *msg) InProgress() error                  { return m.do("in-progress") }
func (m *msg) Term() error                        { return m.do("term") }
func (m *msg) done() []string                     { m.mu.Lock(); defer m.mu.Unlock(); return slices.Clone(m.acks) }

// pipeline is one replica's executor, batcher and flusher over a catalog and a memory store.
type pipeline struct {
	x   *Executor
	b   *Batcher
	f   *Flusher
	st  *store.Mem
	cl  *claims
	cat *catalog.Catalog
}

func newPipeline(t testing.TB, c *catalog.Catalog) *pipeline {
	t.Helper()
	cl := newClaims()
	x := executor(t, c, cl)
	st := store.NewMem()
	f := &Flusher{
		Catalog: c, Store: st, WorkDir: t.TempDir(), Configs: x.Configs, Interval: time.Second,
		HorizonHour: func() int64 { return HourOf(x.Horizon()) }, Writer: "tracekeep-test", Now: func() time.Time { return time.Unix(0, now) },
	}

	return &pipeline{x: x, b: NewBatcher(time.Second, 1<<20), f: f, st: st, cl: cl, cat: c}
}

// feed disposes m and batches it, failing unless it is batched.
func (p *pipeline) feed(t testing.TB, m Message) *msg {
	t.Helper()
	d, dec := dispose(t, p.x, m)
	if dec.Action != ActBatch {
		t.Fatalf("seq %d disposed %+v, want batched", d.Raw.Version.Seq, dec)
	}
	h := &msg{}
	p.b.Add(Entry{D: d, Msg: h}, time.Unix(0, now))

	return h
}

// flushAll flushes every open batch.
func (p *pipeline) flushAll(t testing.TB) []Flushed {
	t.Helper()
	batches := p.b.All()
	out := make([]Flushed, 0, len(batches))
	for _, b := range batches {
		fl, err := p.f.Flush(t.Context(), b)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fl)
	}

	return out
}

// records reads every record of the object under key.
func records(t *testing.T, st store.ObjectStore, key string) []tracepack.Record {
	t.Helper()
	obj, err := st.Open(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	r, err := tracepack.Open(t.Context(), obj, obj.Size(), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var out []tracepack.Record
	collect := func(it *tracepack.Item) error {
		// An Item is valid only during the call: its payload is the reader's pooled block buffer, so it is kept as a copy.
		r := it.Record
		r.Payload = slices.Clone(r.Payload)
		out = append(out, r)
		return nil
	}
	if _, err := r.Iterate(t.Context(), tracepack.Query{Payloads: true}, collect); err != nil {
		t.Fatal(err)
	}

	return out
}

// verify checks the object under key in full, as the reconciler would before adopting it (storage.md §5.4).
func verify(t *testing.T, st store.ObjectStore, key string) {
	t.Helper()
	obj, err := st.Open(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	rep, err := tracepack.Verify(t.Context(), obj, obj.Size(), tracepack.VerifyOptions{})
	if err != nil || rep.Outcome != tracepack.VerifyOutcomeFinalizedConsistent || len(rep.WriterDefects) != 0 {
		t.Errorf("Verify = %v, %v, writer defects %v", rep.Outcome, err, rep.WriterDefects)
	}
}

func storedKeys(t testing.TB, st store.ObjectStore) []string {
	t.Helper()
	var ks []string
	if err := st.List(t.Context(), "", func(i store.ObjectInfo) error { ks = append(ks, i.Key); return nil }); err != nil {
		t.Fatal(err)
	}
	slices.Sort(ks)

	return ks
}

// A batch is written sorted by seq into one segment, registered, and only then is each message acknowledged.
func TestFlushWritesRegistersThenAcks(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		capture := tracepack.UUID{1}
		held := make([]*msg, 0, 4)
		held = append(held, p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp"))))
		for _, seq := range []uint64{3, 1, 2} { // out of order
			held = append(held, p.feed(t, inline(t, capture, dataRecord(seq, now+int64(seq)), nil)))
		}
		var inProgress int
		p.b.Held(func(e Entry) { inProgress++; _ = e.Msg.InProgress() })
		if inProgress != 4 {
			t.Errorf("%d held messages kept in progress, want 4", inProgress)
		}
		fl := p.flushAll(t)
		if len(fl) != 1 || len(fl[0].Packs) != 1 {
			t.Fatalf("flushed %+v, want one segment", fl)
		}
		ks := storedKeys(t, p.st)
		if len(ks) != 1 {
			t.Fatalf("objects %v, want one segment", ks)
		}
		verify(t, p.st, ks[0])
		recs := records(t, p.st, ks[0])
		seqs := make([]uint64, 0, len(recs))
		for _, r := range recs {
			seqs = append(seqs, r.Seq)
		}
		if !slices.Equal(seqs, []uint64{0, 1, 2, 3}) {
			t.Errorf("segment seqs %v, want 0 to 3 in order", seqs)
		}
		for i, h := range held {
			if got := h.done(); got[len(got)-1] != "ack" {
				t.Errorf("message %d: %v, want acknowledged last", i, got)
			}
		}
		if got, _ := c.Capture(t.Context(), capture); got.HighestSeq != 3 {
			t.Errorf("highest seq %d, want 3", got.HighestSeq)
		}
	})
}

// Equal copies of a record are written once and every copy is acknowledged;
// another version of a seq goes into a conflict segment the catalog records as confirmed (ingest.md §6.2).
func TestFlushDeduplicatesAndSplitsConflicts(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		capture := tracepack.UUID{2}
		p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		a := p.feed(t, inline(t, capture, dataRecord(1, now), nil))
		copyOfA := p.feed(t, inline(t, capture, dataRecord(1, now), nil))
		other := dataRecord(1, now)
		other.Payload = append(bytes.Clone(other.Payload), 0xff)
		b := p.feed(t, inline(t, capture, other, nil))
		fl := p.flushAll(t)
		if len(fl) != 1 || len(fl[0].Packs) != 2 {
			t.Fatalf("flushed %+v, want a segment and a conflict segment", fl)
		}
		for name, h := range map[string]*msg{"first": a, "its copy": copyOfA, "the other version": b} {
			if got := h.done(); len(got) == 0 || got[len(got)-1] != "ack" {
				t.Errorf("%s: %v, want acknowledged", name, got)
			}
		}
		cs, err := c.Conflicts(t.Context(), capture)
		if err != nil || len(cs) != 1 || cs[0].State != "confirmed" || cs[0].SeqFrom != 1 || cs[0].Other != fl[0].Packs[0] {
			t.Errorf("conflicts %+v, %v; want seq 1 confirmed against the batch's segment", cs, err)
		}
	})
}

// A claim-check record's message is acknowledged with a confirmed ack, and its receipt names the object.
func TestFlushClaimCheckDoubleAcks(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		capture := tracepack.UUID{3}
		p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		h := p.feed(t, claimCheck(t, p.cl, capture, dataRecord(1, now), nil))
		p.flushAll(t)
		if got := h.done(); !slices.Equal(got, []string{"double-ack"}) {
			t.Errorf("claim-check message: %v, want one confirmed ack", got)
		}
		v := &catalog.VersionKey{Capture: capture, Seq: 1}
		d, _ := dispose(t, p.x, claimCheck(t, p.cl, capture, dataRecord(1, now), nil))
		v.Header, v.Digest = d.Raw.Version.Header, d.Raw.Version.Digest
		if m, err := c.Receipt(t.Context(), v); err != nil || m != catalog.ReceiptEqual {
			t.Errorf("receipt %v, %v; want the record's", m, err)
		}
	})
}

// A claim-check copy of a record also delivered inline gets its receipt, whichever copy the segment holds (ingest.md §6.3).
func TestFlushReceiptOfAnEqualCopy(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		for i, inlineFirst := range []bool{true, false} {
			p := newPipeline(t, c)
			capture := tracepack.UUID{4, byte(i)}
			p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
			cc := claimCheck(t, p.cl, capture, dataRecord(1, now), nil)
			copies := []Message{inline(t, capture, dataRecord(1, now), nil), cc}
			if !inlineFirst {
				copies[0], copies[1] = copies[1], copies[0]
			}
			var hs []*msg
			for _, m := range copies {
				hs = append(hs, p.feed(t, m))
			}
			p.flushAll(t)
			for j, h := range hs {
				if got := h.done(); len(got) != 1 {
					t.Errorf("inline first %v, copy %d: %v, want acknowledged once", inlineFirst, j, got)
				}
			}
			d, _ := dispose(t, p.x, cc)
			v := &catalog.VersionKey{Capture: capture, Seq: 1, Header: d.Raw.Version.Header, Digest: d.Raw.Version.Digest}
			if m, err := c.Receipt(t.Context(), v); err != nil || m != catalog.ReceiptEqual {
				t.Errorf("inline first %v: receipt %v, %v; want the claim-check copy's", inlineFirst, m, err)
			}
		}
	})
}

// While a flush runs, the heartbeat keeps in progress the messages of the batch it writes, of the batches still to flush
// and of those the batcher holds (architecture.md §3.1).
func TestHeartbeatDuringAFlush(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		start := func(capture tracepack.UUID) *msg {
			return p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		}
		flushing := start(tracepack.UUID{5})
		batches := p.b.All()
		waiting := start(tracepack.UUID{6})
		batches = append(batches, p.b.All()...)
		held := start(tracepack.UUID{7})

		tick := make(chan time.Time)
		ctx, cancel := context.WithCancel(t.Context())
		stopped := make(chan struct{})
		go func() { g.heartbeats(ctx, tick); close(stopped) }()
		p.f.hooks[hookAfterPut] = func() error {
			p.f.hooks[hookAfterPut] = nil
			tick <- time.Time{}
			tick <- time.Time{} // taken once the first beat is done
			return nil
		}
		err := g.flush(t.Context(), batches)
		cancel()
		<-stopped
		if err != nil {
			t.Fatal(err)
		}
		for name, h := range map[string]*msg{"the flushing batch's": flushing, "a waiting batch's": waiting, "a held batch's": held} {
			if got := h.done(); len(got) == 0 || got[0] != "in-progress" {
				t.Errorf("%s message: %v, want kept in progress during the flush", name, got)
			}
		}
	})
}

// The batches a flush takes are the heartbeat's in the same step the batcher lets them go: no tick finds them in neither place.
func TestHeartbeatHoldsTakenBatches(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		h := p.feed(t, inline(t, tracepack.UUID{8}, startRecord(t, now), descriptorOf(t, "eqp")))
		taken := g.take(p.b.All)
		g.beat() // a tick between the take and the flush
		if got := h.done(); !slices.Equal(got, []string{"in-progress"}) {
			t.Errorf("taken message: %v, want kept in progress", got)
		}
		if err := g.flush(t.Context(), taken); err != nil {
			t.Fatal(err)
		}
	})
}

// addRecord batches an empty record of capture at ts, arriving at at.
func addRecord(b *Batcher, capture tracepack.UUID, ts int64, at time.Time) {
	b.Add(Entry{D: &Delivery{Raw: Raw{Version: Version{Capture: capture}, TS: ts}}, Msg: &msg{}}, at)
}

// batchKeys are the keys of bs, in their order.
func batchKeys(bs []*Batch) []Key {
	ks := make([]Key, len(bs))
	for i, b := range bs {
		ks[i] = b.Key
	}

	return ks
}

// Records of two hours are two batches of a capture, due together at its earliest opening plus F:
// the hour's change makes nothing due, and each capture keeps its own flush time across it.
func TestBatcherKeepsCadenceAcrossTheHour(t *testing.T) {
	t.Parallel()
	const f = time.Minute
	b := NewBatcher(f, 1<<20)
	t0, hour := time.Unix(0, now), HourOf(now)
	one, two := tracepack.UUID{1}, tracepack.UUID{2}
	addRecord(b, one, now, t0)
	addRecord(b, two, now, t0.Add(20*time.Second))
	change := t0.Add(30 * time.Second) // both captures' first records of the next hour
	addRecord(b, one, now+int64(time.Hour), change)
	addRecord(b, two, now+int64(time.Hour), change)
	if due := b.Due(change); len(due) != 0 {
		t.Fatalf("due at the hour's change %v, want none", batchKeys(due))
	}
	if due := b.Due(t0.Add(f)); !slices.Equal(batchKeys(due), []Key{{one, hour}, {one, hour + 1}}) {
		t.Fatalf("due at capture 1's flush time %v, want both its hours", batchKeys(due))
	}
	if due := b.Due(t0.Add(20*time.Second + f)); !slices.Equal(batchKeys(due), []Key{{two, hour}, {two, hour + 1}}) {
		t.Fatalf("due at capture 2's flush time %v, want both its hours", batchKeys(due))
	}
	if due := b.Due(change.Add(f)); len(due) != 0 {
		t.Errorf("due F after the hour's change %v, want none: no wave follows the hour", batchKeys(due))
	}
}

// Due and All return their batches ordered by capture and hour, so the ingester registers a capture's earlier hour first.
func TestBatcherReturnsCapturesAndHoursInOrder(t *testing.T) {
	t.Parallel()
	b := NewBatcher(time.Minute, 1<<20)
	at, hour := time.Unix(0, now), HourOf(now)
	want := make([]Key, 0, 8*3)
	for c := range 8 {
		for h := range int64(3) {
			want = append(want, Key{Capture: tracepack.UUID{byte(c + 1)}, Hour: hour + h})
		}
	}
	fill := func() {
		for i := len(want) - 1; i >= 0; i-- {
			addRecord(b, want[i].Capture, now+(want[i].Hour-hour)*int64(time.Hour), at)
		}
	}
	fill()
	if got := batchKeys(b.Due(at.Add(time.Minute))); !slices.Equal(got, want) {
		t.Errorf("Due returned %v, want %v", got, want)
	}
	fill()
	if got := batchKeys(b.All()); !slices.Equal(got, want) {
		t.Errorf("All returned %v, want %v", got, want)
	}
}

// No batch waits more than F after its own opening, which every reader's bound assumes,
// and none leaves before its capture's due time, its earliest open batch's opening plus F, whatever hours its records are of.
func TestBatcherDueWithinF(t *testing.T) {
	t.Parallel()
	const f = time.Minute
	b := NewBatcher(f, 1<<20)
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a reproducible schedule, not a secret
	at := time.Unix(0, now)
	for range 2000 {
		at = at.Add(time.Duration(rng.Int64N(int64(f / 4))))
		earliest := map[tracepack.UUID]time.Time{}
		for k, bt := range b.batches {
			if e, ok := earliest[k.Capture]; !ok || bt.opened.Before(e) {
				earliest[k.Capture] = bt.opened
			}
		}
		for _, bt := range b.Due(at) {
			if waited := at.Sub(earliest[bt.Key.Capture]); waited < f {
				t.Fatalf("%+v due %s after its capture's earliest opening, before F (%s)", bt.Key, waited, f)
			}
		}
		for k, bt := range b.batches {
			if waited := at.Sub(bt.opened); waited >= f {
				t.Fatalf("%+v held %s after its opening, F is %s", k, waited, f)
			}
		}
		addRecord(b, tracepack.UUID{byte(rng.IntN(4) + 1)}, now+rng.Int64N(3)*int64(time.Hour), at)
	}
}

// The working files a crashed process left are removed at start; a flush leaves none behind.
func TestWorkDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	left := filepath.Join(dir, "segment-123.tpk")
	if err := os.WriteFile(left, []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dir, "other")
	if err := os.WriteFile(keep, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CleanWorkDir(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(left); !os.IsNotExist(err) {
		t.Error("a crashed flush's working file is still there")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("CleanWorkDir removed a file that is not a working file")
	}
}

// Two claim digests at one seq (the gate's case): the claim object, named for the seq, holds one payload,
// so the version staged first keeps its receipt and the other, whose digest the object no longer matches,
// is deferred as a claim conflict with an alert, never staged with another version's payload (compactor.md §8.4).
func TestTwoClaimDigestsAtOneSeq(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		capture := tracepack.UUID{5}
		p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		first := claimCheck(t, p.cl, capture, dataRecord(1, now), nil)
		p.feed(t, first)
		p.flushAll(t)

		other := dataRecord(1, now)
		other.Payload = append(bytes.Clone(other.Payload), 0xee)
		second := claimCheck(t, newClaims(), capture, other, nil) // its object never reaches this bucket, which keeps the first's
		if second.Env.PayloadDigest == first.Env.PayloadDigest {
			t.Fatal("the two versions share a digest")
		}
		_, dec := dispose(t, p.x, second)
		if dec.Action != ActNak || dec.Reason != ReasonClaimConflict || !dec.Alert {
			t.Errorf("the second digest's decision %+v, want a claim conflict nak'd with an alert", dec)
		}
		d, _ := dispose(t, p.x, first)
		v := d.versionKey()
		if m, err := c.Receipt(t.Context(), v); err != nil || m != catalog.ReceiptEqual {
			t.Errorf("the first version's receipt %v, %v; want it kept", m, err)
		}
	})
}
