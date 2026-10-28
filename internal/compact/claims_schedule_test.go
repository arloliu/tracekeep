package compact

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// version is a parked version of a claim-check seq: its original delivery and its copy on the parking stream.
type version struct{ orig, copy *catalog.Identity }

// claimSeq is the claim-check seq whose versions the schedules below park.
const claimSeq = 6

// parkVersion parks the version of capture's claimSeq with digest d, naming the claim object key, and publishes and records its copy on bs.
func (f *fixture) parkVersion(t *testing.T, bs *streams, capture tracepack.UUID, d byte, key string) version {
	t.Helper()
	ctx := t.Context()
	orig := &catalog.Identity{
		Version: catalog.VersionKey{Capture: capture, Seq: claimSeq, Digest: [32]byte{d}}, Incarnation: 99, StreamSeq: claimSeq*10 + int64(d), Hour: hour,
	}
	if _, err := f.c.BeginParking(ctx, orig, "claim-conflict", key, 0); err != nil {
		t.Fatal(err)
	}
	ack, err := bs.js.Publish(ctx, fmt.Sprintf("%s.T1.%s.v%d", busconfig.DefaultParkPrefix, capture, d), []byte("copy"))
	if err != nil {
		t.Fatal(err)
	}
	cp := *orig
	cp.Incarnation, cp.StreamSeq = bs.inc.ID, int64(ack.Sequence) //nolint:gosec // a stream sequence fits
	if _, err := f.c.RecordCopy(ctx, &cp); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.AdvanceParked(ctx, &orig.Version); err != nil {
		t.Fatal(err)
	}

	return version{orig: orig, copy: &cp}
}

// stageVersion registers a segment of v's seq with v's copy as its delivery evidence, which stages the version.
func (f *fixture) stageVersion(t *testing.T, v version) {
	t.Helper()
	b := writeSegment(t, v.orig.Version.Capture, []uint64{v.orig.Version.Seq}, noAdjust)
	f.put(t, b.key, b.data)
	p, reason, err := f.r.read(t.Context(), store.ObjectInfo{Key: b.key, Size: int64(len(b.data))})
	if err != nil || reason != "" {
		t.Fatalf("read = %q, %v", reason, err)
	}
	ev := &catalog.Evidence{Deliveries: []catalog.Identity{*v.copy}}
	if out, err := f.c.RegisterPack(t.Context(), p, catalog.Registration{Evidence: ev, HorizonHour: f.horizon}); err != nil || out != catalog.Registered {
		t.Fatalf("RegisterPack = %v, %v", out, err)
	}
}

// restart replaces f's compactor with a new one of the same configuration, as a restarted process builds.
func (f *fixture) restart() {
	o := f.r
	f.r = &Compactor{
		Catalog: o.Catalog, Store: o.Store, KeyPrefix: o.KeyPrefix, HorizonHour: o.HorizonHour, Log: o.Log, JS: o.JS, Bus: o.Bus,
		Holder: o.Holder, LeaseTTL: o.LeaseTTL, Interval: o.Interval, WorkDir: o.WorkDir, Writer: o.Writer, QuietPeriod: o.QuietPeriod,
		Now: o.Now, Inactivity: o.Inactivity, FlushInterval: o.FlushInterval, Retention: o.Retention, DeleteDelay: o.DeleteDelay,
		Claims: o.Claims, Metrics: o.Metrics,
	}
}

func claimRows(t *testing.T, c *catalog.Catalog) map[string]string {
	t.Helper()
	objs, err := c.ClaimObjects(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, o := range objs {
		out[o.Key] = o.State
	}

	return out
}

// Gate case 21 (i): one claim object two versions of a seq name, both staged before the eviction:
// the hour is evicted normally and the object released with it; its bucket deletion fails,
// and a restarted compactor's next cycle attributes the object again, evicts the scope-less hour and deletes it.
func TestSharedObjectOfTwoStagedVersions(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := f.retaining(t)
		bs := newStreams(t, c)
		capture := f.admit(t)
		obj := sdkObject(t, capture, 6, hour*nsPerHour+10)
		b.put(obj)
		key := "claims/" + obj.Name
		for _, d := range []byte{1, 2} {
			f.stageVersion(t, f.parkVersion(t, bs, capture, d, key))
		}
		b.failDeletes = true
		if rep := f.retain(t); rep.Evicted != 1 || rep.Survivors != 0 || rep.Failed != 1 {
			t.Fatalf("Retain = %+v, want the hour evicted, its object released and the deletion failed", rep)
		}
		if got := b.names(); !slices.Equal(got, []string{obj.Name}) {
			t.Fatalf("bucket = %v, want the object the failed deletion left", got)
		}
		b.failDeletes = false
		f.restart()
		if rep := f.retain(t); rep.Attributed != 1 || rep.Evicted != 1 || rep.Survivors != 0 {
			t.Errorf("the restarted Retain = %+v, want the object attributed and its hour evicted again", rep)
		}
		if got := b.names(); len(got) != 0 {
			t.Errorf("bucket = %v, want empty", got)
		}
		if rows := claimRows(t, c); len(rows) != 0 {
			t.Errorf("claim rows %v, want none", rows)
		}
	})
}

// Gate case 21 (ii): one claim object of a staged version A and an unresolved version B, staged first or parked first:
// the eviction keeps the object as a survivor, through repeated cycles and restarts; B terminated, the object stays while B's copy waits;
// the copies acknowledged and reconciled, the release runs, and a crash before or after its bucket deletion is resumed by the next cycle.
func TestSharedObjectSurvivorReleased(t *testing.T) {
	t.Parallel()
	for _, stagedFirst := range []bool{true, false} {
		for _, crashAfter := range []bool{false, true} {
			name := fmt.Sprintf("%s, crash %s the bucket delete",
				map[bool]string{true: "staged first", false: "unresolved first"}[stagedFirst], map[bool]string{false: "before", true: "after"}[crashAfter])
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
					ctx := t.Context()
					f := newFixture(t, c)
					b := f.retaining(t)
					bs := newStreams(t, c)
					capture := f.admit(t)
					obj := sdkObject(t, capture, 6, hour*nsPerHour+10)
					b.put(obj)
					key := "claims/" + obj.Name
					if stagedFirst {
						f.stageVersion(t, f.parkVersion(t, bs, capture, 1, key))
						f.parkVersion(t, bs, capture, 2, key)
					} else {
						f.parkVersion(t, bs, capture, 2, key)
						f.stageVersion(t, f.parkVersion(t, bs, capture, 1, key))
					}
					if rep := f.retain(t); rep.Evicted != 1 || rep.Survivors != 1 {
						t.Fatalf("Retain = %+v, want the hour evicted and the object a survivor", rep)
					}
					for range 2 {
						f.restart()
						if rep := f.retain(t); rep.Released != 0 || !slices.Equal(b.names(), []string{obj.Name}) {
							t.Fatalf("a later cycle = %+v, bucket %v; want the survivor kept", rep, b.names())
						}
					}
					seq := uint64(6)
					if n, err := c.TerminateParked(ctx, capture, &seq, "op", "a1"); err != nil || n != 1 {
						t.Fatalf("TerminateParked = %d, %v; want B terminated", n, err)
					}
					if rep := f.retain(t); rep.Released != 0 {
						t.Fatalf("Retain while B's copy waits = %+v, want the survivor kept", rep)
					}
					for range 2 {
						bs.acknowledge(t)
					}
					if n, err := f.r.Copies(ctx, f.lease, catalog.RoleParking, bs.parked); err != nil || n != 2 {
						t.Fatalf("Copies = %d, %v; want both copies marked acked", n, err)
					}
					b.failDeletes, b.crashAfterDelete = !crashAfter, crashAfter
					if rep := f.retain(t); rep.Released != 0 || rep.Failed != 1 {
						t.Fatalf("the crashed release = %+v, want it failed", rep)
					}
					if rows := claimRows(t, c); rows[key] != "deletable" {
						t.Fatalf("claim rows %v, want the survivor deletable", rows)
					}
					if got := len(b.names()); got != map[bool]int{false: 1, true: 0}[crashAfter] {
						t.Fatalf("bucket %v after the crash", b.names())
					}
					b.failDeletes, b.crashAfterDelete = false, false
					f.restart()
					if rep := f.retain(t); rep.Released != 1 || len(b.names()) != 0 {
						t.Errorf("the resumed release = %+v, bucket %v; want the survivor released", rep, b.names())
					}
					if rows := claimRows(t, c); len(rows) != 0 {
						t.Errorf("claim rows %v, want none", rows)
					}
				})
			})
		}
	}
}

// adoptStream replaces the parking stream's incarnation as bus adopt-stream does once the compactor's lease is released,
// marking its waiting copies lost, and takes the lease again.
func (f *fixture) adoptStream(t *testing.T, bs *streams) {
	t.Helper()
	ctx := t.Context()
	if err := f.c.ReleaseLease(ctx, f.lease); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Leave(ctx, "r"); err != nil { // the member f.admit registered, stopped with the rest of the world
		t.Fatal(err)
	}
	if _, err := f.c.AdoptStream(ctx, catalog.RoleParking, bs.inc.Stream, bs.inc.CreatedNS+1, "op"); err != nil {
		t.Fatal(err)
	}
	lease, ok, err := f.c.TakeLease(ctx, "compactor", "reconciler", time.Hour)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}
	f.lease = lease
}

// Gate case 22: a survivor whose version's every copy bus adopt-stream marked lost.
// Terminated first, its waiting copy lost by the adoption, it is released by the next cycle;
// unresolved, it is kept through cycles and a restart until the operator terminates it, and then released.
func TestSurvivorWithLostCopies(t *testing.T) {
	t.Parallel()
	for _, terminatedFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "terminated, then adopted", false: "adopted while unresolved"}[terminatedFirst], func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				ctx := t.Context()
				f := newFixture(t, c)
				b := f.retaining(t)
				bs := newStreams(t, c)
				capture := f.admit(t)
				obj := sdkObject(t, capture, 6, hour*nsPerHour+10)
				b.put(obj)
				v := f.parkVersion(t, bs, capture, 1, "claims/"+obj.Name)
				if rep := f.retain(t); rep.Survivors != 1 {
					t.Fatalf("Retain = %+v, want a survivor", rep)
				}
				seq := uint64(6)
				terminate := func() {
					t.Helper()
					if n, err := c.TerminateParked(ctx, capture, &seq, "op", "a1"); err != nil || n != 1 {
						t.Fatalf("TerminateParked = %d, %v", n, err)
					}
				}
				if terminatedFirst {
					terminate()
					if rep := f.retain(t); rep.Released != 0 {
						t.Fatalf("Retain with the copy waiting = %+v, want kept", rep)
					}
				}
				f.adoptStream(t, bs)
				if got := copyState(t, c, v.copy); got != "lost" {
					t.Fatalf("copy %s, want lost by the adoption", got)
				}
				if !terminatedFirst {
					for range 2 {
						f.restart()
						if rep := f.retain(t); rep.Released != 0 || len(b.names()) != 1 {
							t.Fatalf("Retain of an unresolved version's survivor = %+v, want kept until the operator decides", rep)
						}
					}
					terminate()
				}
				if rep := f.retain(t); rep.Released != 1 || len(b.names()) != 0 {
					t.Errorf("Retain once terminated with its copy lost = %+v, bucket %v; want released", rep, b.names())
				}
			})
		})
	}
}

// passesInsideRetention runs, over f's merged hour still inside retention, a merge, the deletion of its inputs, input among them,
// and a retention pass: a period long enough that the hour outlives the merge's deletion time, which the catalog's clock sets.
func passesInsideRetention(t *testing.T, f *fixture, input *catalog.Pack) {
	t.Helper()
	f.r.Retention = 1_000_000 * time.Hour
	if rep := f.merges(t); len(rep.Merged) != 1 {
		t.Fatalf("Merges = %+v", rep)
	}
	f.retain(t) // records the period
	deleteAt := time.Unix(0, deadline(t, f.c, input, f.r.DeleteDelay)).Add(time.Second)
	f.r.Now = func() time.Time { return deleteAt }
	var rep RetentionReport
	if err := f.r.Deletions(t.Context(), f.lease, &rep); err != nil || rep.Deleted != 2 {
		t.Fatalf("Deletions = %+v, %v", rep, err)
	}
	if rep := f.retain(t); rep.Evicted != 0 {
		t.Fatalf("Retain inside retention = %+v, want nothing evicted", rep)
	}
}

// Gate case 7: a conflicting claim version stays available until eviction: merges, deletions and a retention pass short of the hour's
// deadline keep its object in the bucket and the version parked; once the deadline passes, the eviction keeps the object a survivor.
func TestConflictingClaimVersionKeptUntilEviction(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		f.merging(t)
		b := f.retaining(t)
		bs := newStreams(t, c)
		capture := f.admit(t)
		a := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		f.stage(t, writeSegment(t, capture, []uint64{2, 3}, noAdjust))
		obj := sdkObject(t, capture, 6, hour*nsPerHour+10)
		b.put(obj)
		f.parkVersion(t, bs, capture, 2, "claims/"+obj.Name)
		passesInsideRetention(t, f, a)
		if got := b.names(); !slices.Equal(got, []string{obj.Name}) {
			t.Errorf("bucket = %v, want the conflicting version's object", got)
		}
		if vs, err := c.ParkedVersions(ctx, &capture); err != nil || len(vs) != 1 || vs[0].State != catalog.VersionParked {
			t.Errorf("versions %+v, %v; want the conflicting version parked", vs, err)
		}
		f.r.Retention = month // shortened: the hour is evicted once the shortening's hold ends
		f.retain(t)
		later := f.r.now().Add(f.r.DeleteDelay + time.Minute)
		f.r.Now = func() time.Time { return later }
		if rep := f.retain(t); rep.Evicted != 1 || rep.Survivors != 1 || len(b.names()) != 1 {
			t.Errorf("Retain past the deadline = %+v, bucket %v; want the object kept as a survivor", rep, b.names())
		}
	})
}

// Gate case 5: a claim-check record's receipt and object outlive every pass short of its hour's eviction:
// staged with its receipt, its pack merged and deleted, and a retention pass inside retention, a late copy's lookup still finds
// its receipt, and its object is in the bucket under a live row.
func TestLateClaimReferenceInsideRetention(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		f.merging(t)
		b := newBucket()
		f.r.Claims, f.r.Bus, f.r.DeleteDelay = b, bus.Config{ClaimBucket: "claims"}, time.Hour
		capture := f.admit(t)
		obj := sdkObject(t, capture, 1, hour*nsPerHour+10)
		b.put(obj)
		in := writeSegment(t, capture, []uint64{0, 1}, noAdjust)
		f.put(t, in.key, in.data)
		p, reason, err := f.r.read(ctx, store.ObjectInfo{Key: in.key, Size: int64(len(in.data))})
		if err != nil || reason != "" {
			t.Fatalf("read = %q, %v", reason, err)
		}
		v := catalog.VersionKey{Capture: capture, Seq: 1, Header: [44]byte{1}, Digest: [32]byte{1}}
		rc := catalog.Receipt{Seq: 1, Header: v.Header, PayloadSHA256: v.Digest, ObjectKey: "claims/" + obj.Name, ObjectSize: 64}
		ev := &catalog.Evidence{Receipts: []catalog.Receipt{rc}}
		if out, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: ev, HorizonHour: f.horizon}); err != nil || out != catalog.Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		f.stage(t, writeSegment(t, capture, []uint64{2, 3}, noAdjust))
		passesInsideRetention(t, f, p)
		if m, err := c.Receipt(ctx, &v); err != nil || m != catalog.ReceiptEqual {
			t.Errorf("Receipt = %v, %v; want the late copy's receipt found", m, err)
		}
		if got := b.names(); !slices.Equal(got, []string{obj.Name}) {
			t.Errorf("bucket = %v, want the object", got)
		}
		if rows := claimRows(t, c); rows[rc.ObjectKey] != "live" {
			t.Errorf("claim rows %v, want the object's row live", rows)
		}
	})
}
