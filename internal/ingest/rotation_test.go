package ingest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/classify"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/store"
)

// versioned is a key provider with versions: material by reference and version, the current version answering an unpinned read.
type versioned struct {
	mu      sync.Mutex
	mat     map[string]map[string][]byte
	current map[string]string
}

func (v *versioned) Material(_ context.Context, ref, version string) ([]byte, string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if version == "" {
		version = v.current[ref]
	}
	m, ok := v.mat[ref][version]
	if !ok {
		return nil, "", &keys.ProviderError{Provider: "fake", Op: "read", Reason: "missing"}
	}

	return slices.Clone(m), version, nil
}

// put writes fresh material for ref as its next version, which becomes current.
func (v *versioned) put(ref string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.mat[ref] == nil {
		v.mat[ref] = map[string][]byte{}
	}
	m := make([]byte, 32)
	_, _ = rand.Read(m)
	version := fmt.Sprint(len(v.mat[ref]) + 1)
	v.mat[ref][version], v.current[ref] = m, version
}

// rotationWorld is what outlives a process: the catalog, the store, the claim bucket, the key provider and the declarations.
type rotationWorld struct {
	c        *catalog.Catalog
	st       *store.Mem
	cl       *claims
	prov     *versioned
	declared map[string]keys.Declaration
	lease    *catalog.Lease
}

func newRotationWorld(c *catalog.Catalog) *rotationWorld {
	w := &rotationWorld{
		c: c, st: store.NewMem(), cl: newClaims(),
		prov:     &versioned{mat: map[string]map[string][]byte{}, current: map[string]string{}},
		declared: map[string]keys.Declaration{},
	}
	w.declare("d-1", catalog.KindDigest)
	w.declare("m-1", catalog.KindMaster)

	return w
}

func (w *rotationWorld) declare(id, kind string) {
	w.prov.put(id)
	w.declared[id] = keys.Declaration{Provider: "fake", Ref: id, Kind: kind}
}

// keys is a fresh process's keys, its cache empty.
func (w *rotationWorld) keys(t *testing.T) *keys.Keys {
	t.Helper()
	k, err := keys.New(keys.Config{Declared: w.declared, Providers: map[string]keys.Provider{"fake": w.prov}}, w.c)
	if err != nil {
		t.Fatal(err)
	}

	return k
}

// stage stages and activates the masking configuration of p under the two key ids, binding them, as config stage and config activate do.
func (w *rotationWorld) stage(t *testing.T, p *policy.Policy, digest, encryption string) int64 {
	t.Helper()
	ctx := t.Context()
	k := w.keys(t)
	cfg := Config{Name: classify.Name, MaxFrameLen: maxFrameLen, Policy: p}
	for _, ref := range []struct {
		to   *KeyRef
		id   string
		kind string
	}{{&cfg.DigestKey, digest, catalog.KindDigest}, {&cfg.EncryptionKey, encryption, catalog.KindMaster}} {
		fp, err := k.Fingerprint(ctx, ref.id, ref.kind)
		if err != nil {
			t.Fatal(err)
		}
		*ref.to = KeyRef{ID: ref.id, Fingerprint: fp}
	}
	body, _ := p.Canonical()
	id, err := w.c.StageConfig(ctx, catalog.Staging{
		Content: cfg.Content(), Policy: &catalog.StoredPolicy{ID: p.ID, Version: int64(p.Version), Body: body}, //nolint:gosec // a parsed version fits
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.c.ActivateStaged(ctx, id, nil, nil); err != nil {
		t.Fatal(err)
	}

	return id
}

// process is a fresh ingest process outside dev: its keys, its configurations rebuilt from the catalog, its membership, its pipeline.
func (w *rotationWorld) process(t *testing.T) *pipeline {
	t.Helper()
	ctx := t.Context()
	b, err := namedConfig(classify.Name, maxFrameLen, true)
	if err != nil {
		t.Fatal(err)
	}
	k := w.keys(t)
	configs, err := Executable(ctx, w.c, &b, k, false)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, len(configs))
	for id := range configs {
		ids = append(ids, id)
	}
	if _, err := w.c.Register(ctx, "r", ids, time.Minute); err != nil {
		t.Fatal(err)
	}
	x := &Executor{
		Catalog: w.c, Claims: w.cl, Horizon: func() int64 { return now - int64(24*time.Hour) }, Configs: configs, Keys: k,
		Bindable: func(id int64) bool { cfg, ok := configs[id]; return ok && cfg.Policy != nil },
	}
	f := &Flusher{
		Catalog: w.c, Store: w.st, WorkDir: t.TempDir(), Configs: configs, Interval: time.Second,
		HorizonHour: func() int64 { return HourOf(x.Horizon()) }, Writer: "tracekeep-test", Now: func() time.Time { return time.Unix(0, now) },
	}

	return &pipeline{x: x, b: NewBatcher(time.Second, 1<<20), f: f, st: w.st, cl: w.cl, cat: w.c}
}

// maskedTraffic is a start and every baseline carrier, inline or by claim-check, as published.
func maskedTraffic(t *testing.T, w *rotationWorld, capture tracepack.UUID, claimCheckAll bool) []Message {
	t.Helper()
	b := policy.Baseline()
	carriers, err := maskingtest.Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}
	msgs := []Message{inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp"))}
	for i := range carriers {
		r := carrierRecord(&carriers[i], uint64(i+1))
		if claimCheckAll {
			msgs = append(msgs, claimCheck(t, w.cl, capture, r, nil))
		} else {
			msgs = append(msgs, inline(t, capture, r, nil))
		}
	}

	return msgs
}

// rotations change the keys or the policy between two deliveries of one record (phase 3.6's gate case, owned by 3.3),
// each returning the configuration it staged and activated, or 0 when it staged none.
var rotations = map[string]func(t *testing.T, w *rotationWorld) int64{
	"none": func(*testing.T, *rotationWorld) int64 { return 0 },
	"digest key": func(t *testing.T, w *rotationWorld) int64 {
		w.declare("d-2", catalog.KindDigest)
		return w.stage(t, new(policy.Baseline()), "d-2", "m-1")
	},
	"master key": func(t *testing.T, w *rotationWorld) int64 {
		w.declare("m-2", catalog.KindMaster)
		return w.stage(t, new(policy.Baseline()), "d-1", "m-2")
	},
	"provider version": func(_ *testing.T, w *rotationWorld) int64 {
		w.prov.put("d-1")
		w.prov.put("m-1")
		return 0
	},
	"policy": func(t *testing.T, w *rotationWorld) int64 {
		p := policy.Baseline()
		p.Version = 2
		p.ItemRules = append(slices.Clone(p.ItemRules), newRule)
		return w.stage(t, &p, "d-1", "m-1")
	},
}

// newRule is the policy rotation's rule: a second path on the S7F3 carrier, its identifier,
// so a redelivery bound to the new policy would write one entry more than the first delivery.
var newRule = policy.ItemRule{Stream: 7, Function: 3, Path: "1", Domain: "ppbody"}

// capturingPut fails every put, as a crash before the object exists would, after keeping the bytes it was given:
// the first attempt's serialization, which its retry must reproduce.
type capturingPut struct {
	store.ObjectStore
	got *[][]byte
}

func (c capturingPut) Put(_ context.Context, _ string, r io.ReadSeeker, _ int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	*c.got = append(*c.got, b)

	return errCrash
}

// crash is the first process's flush ending at a crash point of ingest.md §6.3.
type crash struct {
	// stop ends the flush and asserts the state it leaves:
	// the bytes a put was given that no object holds, and how many of the first messages it acknowledged, which are not redelivered.
	stop func(t *testing.T, w *rotationWorld, p *pipeline, hs []*msg) (attempted [][]byte, acked int)
	// after, when set, runs once the redelivery is flushed.
	after func(t *testing.T, w *rotationWorld)
}

// crashes are the first process's flush ending at each crash point of ingest.md §6.3.
var crashes = map[string]crash{
	"before the put": {stop: func(t *testing.T, w *rotationWorld, p *pipeline, hs []*msg) ([][]byte, int) {
		var got [][]byte
		p.f.Store = capturingPut{ObjectStore: w.st, got: &got}
		if _, err := p.flushOne(t); !errors.Is(err, errCrash) {
			t.Fatalf("Flush = %v, want the crash", err)
		}
		if len(got) != 1 || len(storedKeys(t, w.st)) != 0 {
			t.Fatalf("%d attempted puts, objects %v; want one attempt and nothing stored", len(got), storedKeys(t, w.st))
		}
		untouched(t, hs)

		return got, 0
	}},
	"after the put, the orphan adopted": {stop: func(t *testing.T, w *rotationWorld, p *pipeline, hs []*msg) ([][]byte, int) {
		crashAfterPut(t, p, hs)
		adopt(t, w)

		return nil, 0
	}},
	"after the put, the orphan adopted after the redelivery": {
		stop: func(t *testing.T, _ *rotationWorld, p *pipeline, hs []*msg) ([][]byte, int) {
			crashAfterPut(t, p, hs)
			return nil, 0
		},
		after: adopt,
	},
	"during the acknowledgements": {stop: func(t *testing.T, _ *rotationWorld, p *pipeline, hs []*msg) ([][]byte, int) {
		// The flush acknowledges in seq order: the start, then the first carrier, a masked record; the crash follows it.
		acks := 0
		p.f.hooks[hookAfterAck] = func() error {
			if acks++; acks == 2 {
				return errCrash
			}
			return nil
		}
		if _, err := p.flushOne(t); !errors.Is(err, errCrash) {
			t.Fatalf("Flush = %v, want the crash", err)
		}
		for i, h := range hs {
			if acked := len(h.done()) == 1; acked != (i < 2) {
				t.Fatalf("message %d: %v; want the start and the first carrier acknowledged, nothing else", i, h.done())
			}
		}

		return nil, 2
	}},
	"after every acknowledgement": {stop: func(t *testing.T, _ *rotationWorld, p *pipeline, hs []*msg) ([][]byte, int) {
		if _, err := p.flushOne(t); err != nil {
			t.Fatal(err)
		}
		for i, h := range hs {
			if len(h.done()) != 1 {
				t.Fatalf("message %d: %v, want acknowledged", i, h.done())
			}
		}

		return nil, 0
	}},
}

// crashAfterPut ends the flush once its segment is put, before the registration: an orphan, its messages untouched.
func crashAfterPut(t *testing.T, p *pipeline, hs []*msg) {
	t.Helper()
	p.f.hooks[hookAfterPut] = func() error { return errCrash }
	if _, err := p.flushOne(t); !errors.Is(err, errCrash) {
		t.Fatalf("Flush = %v, want the crash", err)
	}
	untouched(t, hs)
}

// adopt runs the compactor's orphan passes until the one orphan is adopted, its records registered.
func adopt(t *testing.T, w *rotationWorld) {
	t.Helper()
	r := &compact.Compactor{Catalog: w.c, Store: w.st, HorizonHour: func() int64 { return HourOf(now) - 24 }, Log: slog.New(slog.DiscardHandler)}
	lease := w.compactorLease(t)
	adopted := make([]string, 0, 1)
	for range 2 { // an orphan is adopted on the second pass that finds it
		rep, err := r.Orphans(t.Context(), lease)
		if err != nil {
			t.Fatal(err)
		}
		adopted = append(adopted, rep.Adopted...)
	}
	if len(adopted) != 1 {
		t.Fatalf("adopted %v, want the orphan", adopted)
	}
	if cp, err := w.c.Capture(t.Context(), tracepack.UUID{0x71}); err != nil || !cp.HasHighestSeq {
		t.Fatalf("the capture after the adoption = %+v, %v; want the orphan's records registered", cp, err)
	}
}

// compactorLease is the one compactor's lease, taken at its first use.
func (w *rotationWorld) compactorLease(t *testing.T) catalog.Lease {
	t.Helper()
	if w.lease == nil {
		l, ok, err := w.c.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
		if err != nil || !ok {
			t.Fatalf("TakeLease = %v, %v", ok, err)
		}
		w.lease = &l
	}

	return *w.lease
}

// Rotation between two deliveries of one record: whatever key or policy rotated, at whatever crash point the first process stopped,
// inline or by claim-check, a fresh process redelivering what was not acknowledged, in other batches,
// writes byte-identical records and redaction entries under the same pack-level redaction policy, classifiers and frame limits,
// since the capture keeps its configuration;
// a capture admitted after the rotation is bound to the configuration it staged, and the compactor merges every copy without a false conflict.
func TestRotationBetweenDeliveries(t *testing.T) {
	t.Parallel()
	for rotation, rotate := range rotations {
		for name, cr := range crashes {
			for _, claimCheckAll := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/claim-check %v", rotation, name, claimCheckAll), func(t *testing.T) {
					t.Parallel()
					catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
						rotationCase(t, c, rotate, cr, claimCheckAll, rotation == "policy")
					})
				})
			}
		}
	}
}

func rotationCase(t *testing.T, c *catalog.Catalog, rotate func(*testing.T, *rotationWorld) int64, cr crash, claimCheckAll, newPolicy bool) {
	ctx := t.Context()
	w := newRotationWorld(c)
	first := w.stage(t, new(policy.Baseline()), "d-1", "m-1")
	capture := tracepack.UUID{0x71}
	msgs := maskedTraffic(t, w, capture, claimCheckAll)
	a := w.process(t)
	attempted, acked := cr.stop(t, w, a, a.deliver(t, msgs))
	staged := rotate(t, w)
	b := w.process(t)
	// The redelivery carries what was not acknowledged, in other batches: its first records, then the rest.
	rest := msgs[acked:]
	half := len(rest) / 2
	b.deliver(t, rest[:half])
	b.flushAll(t)
	b.deliver(t, rest[half:])
	b.flushAll(t)
	if cr.after != nil {
		cr.after(t, w)
	}
	if cp, err := c.Capture(ctx, capture); err != nil || cp.ConfigID != first {
		t.Fatalf("the capture = %+v, %v; want still bound to %d", cp, err, first)
	}
	if staged != 0 {
		newCapture(t, w, b, staged, claimCheckAll, newPolicy)
	}
	sameCopies(t, w, capture, attempted, acked)
	mergeClean(t, w, attempted == nil)
}

// newCapture feeds a capture admitted after the rotation its start and the S7F3 carrier:
// it is bound to the staged configuration, and under a rotated policy its carrier carries the new rule's entry.
func newCapture(t *testing.T, w *rotationWorld, p *pipeline, staged int64, claimCheckAll, newPolicy bool) {
	t.Helper()
	next := tracepack.UUID{0x72}
	b := policy.Baseline()
	carriers, err := maskingtest.Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}
	r := carrierRecord(&carriers[slices.IndexFunc(carriers, func(c maskingtest.Carrier) bool { return c.Name == "S7F3" })], 1)
	p.feed(t, inline(t, next, startRecord(t, now), descriptorOf(t, "eqp")))
	if claimCheckAll {
		p.feed(t, claimCheck(t, w.cl, next, r, nil))
	} else {
		p.feed(t, inline(t, next, r, nil))
	}
	p.flushAll(t)
	if cp, err := w.c.Capture(t.Context(), next); err != nil || cp.ConfigID != staged {
		t.Fatalf("a capture admitted after the rotation = %+v, %v; want bound to %d", cp, err, staged)
	}
	if !newPolicy {
		return
	}
	packs := packsOf(t, w, next, nil)
	if len(packs) != 1 {
		t.Fatalf("%d packs of the new capture, want one", len(packs))
	}
	if es := packs[0].h.Meta.Redaction; !slices.ContainsFunc(es, func(e tracepack.RedactionEntry) bool {
		return e.Seq == 1 && e.ItemPath != nil && *e.ItemPath == newRule.Path
	}) {
		t.Errorf("the new capture's entries %s; want one at the new rule's path", entriesKey(es))
	}
}

// pack is one pack read back: its header and its records.
type pack struct {
	h    *tracepack.PackHeader
	recs []tracepack.Record
}

// packsOf reads every pack of capture: the attempted puts' bytes first, then every stored object.
func packsOf(t *testing.T, w *rotationWorld, capture tracepack.UUID, attempted [][]byte) []pack {
	t.Helper()
	datas := slices.Clone(attempted)
	for _, k := range storedKeys(t, w.st) {
		obj, err := w.st.Open(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size()))
		_ = obj.Close()
		if err != nil {
			t.Fatal(err)
		}
		datas = append(datas, b)
	}
	var out []pack
	for _, data := range datas {
		rd, err := tracepack.Open(t.Context(), bytes.NewReader(data), int64(len(data)), tracepack.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if rd.Header().CaptureID != capture {
			continue
		}
		p := pack{h: rd.Header()}
		collect := func(it *tracepack.Item) error {
			p.recs = append(p.recs, it.Record)
			return nil
		}
		if _, err := rd.Iterate(t.Context(), tracepack.Query{Payloads: true}, collect); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}

	return out
}

// sameCopies checks that every pack of capture, stored or only attempted, names the first pack's redaction policy, classifiers and frame limits,
// that every copy of each seq is byte-identical, its redaction entries included and a carrier's never empty,
// and that each seq has the first delivery's copy and, unless it was among the acked first messages, the redelivery's;
// the traffic's i-th message holds seq i.
func sameCopies(t *testing.T, w *rotationWorld, capture tracepack.UUID, attempted [][]byte, acked int) {
	t.Helper()
	type copyOf struct {
		rec     tracepack.Record
		entries []tracepack.RedactionEntry
	}
	packs := packsOf(t, w, capture, attempted)
	if len(packs) == 0 {
		t.Fatal("no copy")
	}
	m0 := packs[0].h.Meta
	if m0.RedactionPolicy == nil {
		t.Fatal("the first pack names no redaction policy")
	}
	bySeq := map[uint64][]copyOf{}
	for _, p := range packs {
		m := p.h.Meta
		if !reflect.DeepEqual(m.RedactionPolicy, m0.RedactionPolicy) || !slices.Equal(m.Classifiers, m0.Classifiers) || !slices.Equal(m.MaxFrameLens, m0.MaxFrameLens) {
			t.Errorf("pack %v: redaction policy %+v, classifiers %v, frame limits %v; want the first pack's %+v, %v, %v",
				p.h.PackID, m.RedactionPolicy, m.Classifiers, m.MaxFrameLens, m0.RedactionPolicy, m0.Classifiers, m0.MaxFrameLens)
		}
		for _, r := range p.recs {
			var es []tracepack.RedactionEntry
			for _, e := range m.Redaction {
				if e.Seq == r.Seq {
					es = append(es, e)
				}
			}
			if r.Seq >= 1 && len(es) == 0 {
				t.Errorf("pack %v seq %d: no redaction entry, want the carrier masked", p.h.PackID, r.Seq)
			}
			bySeq[r.Seq] = append(bySeq[r.Seq], copyOf{r, es})
		}
	}
	for seq, cs := range bySeq {
		want := 2
		if seq < uint64(acked) { //nolint:gosec // a message count
			want = 1
		}
		if len(cs) != want {
			t.Errorf("seq %d: %d copies, want %d: the first delivery's, and the redelivery's unless acknowledged first", seq, len(cs), want)
		}
		for _, c := range cs[1:] {
			if !sameRecord(&c.rec, &cs[0].rec) || !bytes.Equal(c.rec.Payload, cs[0].rec.Payload) {
				t.Errorf("seq %d: two stored copies differ", seq)
			}
			if entriesKey(c.entries) != entriesKey(cs[0].entries) {
				t.Errorf("seq %d: two copies' redaction entries differ", seq)
			}
		}
	}
}

// mergeClean runs the compactor's conflict and merge passes once the hour is quiet: no conflict, and every scope merged;
// when the first delivery stored a copy, the redelivery's copies are suspects the conflict pass clears.
func mergeClean(t *testing.T, w *rotationWorld, stored bool) {
	t.Helper()
	r := &compact.Compactor{
		Catalog: w.c, Store: w.st, HorizonHour: func() int64 { return HourOf(now) - 24 }, Log: slog.New(slog.DiscardHandler),
		WorkDir: t.TempDir(), Writer: "tracekeep-test", QuietPeriod: 15 * time.Minute,
		Now: func() time.Time { return time.Unix(0, now).Add(3 * time.Hour) },
	}
	lease := w.compactorLease(t)
	if rep, err := r.Conflicts(t.Context(), lease); err != nil || rep.Confirmed+rep.Failed+rep.Changed != 0 || stored && rep.Cleared == 0 {
		t.Fatalf("Conflicts = %+v, %v; want every suspect cleared", rep, err)
	}
	rep, err := r.Merges(t.Context(), lease)
	if err != nil || len(rep.Merged) == 0 || len(rep.Conflicted)+len(rep.Failed)+len(rep.Stale) != 0 {
		t.Errorf("Merges = %+v, %v; want every scope merged", rep, err)
	}
}

// entriesKey spells entries by value, an item path by the path it points to.
func entriesKey(entries []tracepack.RedactionEntry) string {
	var b bytes.Buffer
	for _, e := range entries {
		path := "<whole>"
		if e.ItemPath != nil {
			path = *e.ItemPath
		}
		fmt.Fprintf(&b, "%d %s %v %s %x %x %v;", e.Seq, path, e.MaskedRanges, e.Domain, e.Digest, e.SIV, e.HasSIV)
	}

	return b.String()
}
