package compact

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer"
)

const month = 720 * time.Hour

// memBucket is a claim bucket in memory.
type memBucket struct {
	mu   sync.Mutex
	objs map[string]ClaimInfo
	// failDeletes makes every deletion fail; crashAfterDelete makes it fail once the object is deleted.
	failDeletes, crashAfterDelete bool
}

func newBucket() *memBucket { return &memBucket{objs: map[string]ClaimInfo{}} }

func (b *memBucket) List(context.Context) ([]ClaimInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return slices.Collect(maps.Values(b.objs)), nil
}

func (b *memBucket) Delete(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failDeletes {
		return errors.New("delete refused")
	}
	delete(b.objs, name)
	if b.crashAfterDelete {
		return errors.New("crash after the delete")
	}

	return nil
}

func (b *memBucket) put(info ClaimInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objs[info.Name] = info
}

func (b *memBucket) names() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	names := slices.Collect(maps.Keys(b.objs))
	slices.Sort(names)

	return names
}

// sdkObject is the claim object the SDK writes for capture's seq at ts: its name and the metadata of its record header and time.
func sdkObject(t *testing.T, capture tracepack.UUID, seq uint64, ts int64) ClaimInfo {
	t.Helper()
	br := tpbus.NewRecord(&tracepack.Record{Seq: seq, TSUTCNs: ts, Kind: tracepack.KindData, Payload: bytes.Repeat([]byte{1}, 64)})
	br.ClaimCheck, br.Bucket = true, "claims"
	body, err := tpbus.EncodeBody(&br)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != headerLen {
		t.Fatalf("a claim-check body is %d bytes, want the header alone", len(body))
	}

	name, err := tpbus.MsgID(capture, seq)
	if err != nil {
		t.Fatal(err)
	}

	return ClaimInfo{Name: name, Size: 64, Metadata: map[string]string{
		producer.ObjectMetaRecordHeader: hex.EncodeToString(body),
		producer.ObjectMetaTSUTCNs:      strconv.FormatInt(ts, 16),
	}}
}

// An object is attributed only when its name, header seq and time metadata agree; the hour is its record time's.
func TestAttribute(t *testing.T) {
	t.Parallel()
	capture := newID(t)
	ts := hour*nsPerHour + 5
	good := sdkObject(t, capture, 7, ts)
	if got := attribute(good); got == nil || *got != (catalog.ClaimObject{Capture: capture, Seq: 7, Hour: hour}) {
		t.Fatalf("attribute = %+v", got)
	}
	bad := map[string]func(*ClaimInfo){
		"no metadata":     func(o *ClaimInfo) { o.Metadata = nil },
		"another seq":     func(o *ClaimInfo) { o.Name = capture.String() + ".8" },
		"a foreign name":  func(o *ClaimInfo) { o.Name = "upload.bin" },
		"another time":    func(o *ClaimInfo) { o.Metadata[producer.ObjectMetaTSUTCNs] = strconv.FormatInt(ts+1, 16) },
		"a short header":  func(o *ClaimInfo) { o.Metadata[producer.ObjectMetaRecordHeader] = "00" },
		"no time":         func(o *ClaimInfo) { delete(o.Metadata, producer.ObjectMetaTSUTCNs) },
		"not hexadecimal": func(o *ClaimInfo) { o.Metadata[producer.ObjectMetaRecordHeader] = "zz" },
	}
	for name, spoil := range bad {
		o := good
		o.Metadata = maps.Clone(good.Metadata)
		spoil(&o)
		if got := attribute(o); got != nil {
			t.Errorf("%s: attribute = %+v, want nil", name, got)
		}
	}
}

// retaining readies f's compactor for deletion and retention, at a time when hour is a month and a minute past its end.
func (f *fixture) retaining(t *testing.T) *memBucket {
	t.Helper()
	b := newBucket()
	f.r.Claims, f.r.Bus, f.r.Retention, f.r.DeleteDelay = b, bus.Config{ClaimBucket: "claims"}, month, time.Hour
	f.r.Now = func() time.Time { return time.Unix(0, (hour+1)*nsPerHour).Add(month + time.Minute) }

	return b
}

func (f *fixture) retain(t *testing.T) RetentionReport {
	t.Helper()
	var rep RetentionReport
	if err := f.r.Retain(t.Context(), f.lease, &rep); err != nil {
		t.Fatal(err)
	}

	return rep
}

// A merged input is deleted D after its merge, its object after the commit; a deletion whose object removal failed
// leaves a tombstoned key the orphan pass deletes.
func TestDeletions(t *testing.T) {
	t.Parallel()
	for _, failing := range []bool{false, true} {
		catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
			f := newFixture(t, c)
			f.merging(t)
			f.r.DeleteDelay = time.Hour
			capture := f.admit(t)
			a := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
			b := f.stage(t, writeSegment(t, capture, []uint64{2, 3}, noAdjust))
			if rep := f.merges(t); len(rep.Merged) != 1 {
				t.Fatalf("Merges = %+v", rep)
			}
			archives := f.archives(t)
			deletions := func(at time.Time) RetentionReport {
				f.r.Now = func() time.Time { return at }
				var rep RetentionReport
				if err := f.r.Deletions(t.Context(), f.lease, &rep); err != nil {
					t.Fatal(err)
				}

				return rep
			}
			if rep := deletions(time.Now().Add(30 * time.Minute)); rep.Deleted != 0 {
				t.Fatalf("Deletions before D = %+v", rep)
			}
			if failing {
				f.r.Store = failingDelete{f.s}
			}
			if rep := deletions(time.Now().Add(2 * time.Hour)); rep.Deleted != 2 || rep.Tombstoned != 2 {
				t.Fatalf("Deletions after D = %+v, want both inputs", rep)
			}
			f.r.Store = f.s
			if failing {
				if !f.present(t, a.Key) || !f.present(t, b.Key) {
					t.Fatal("a failed removal removed an object")
				}
				f.pass(t)
			}
			if f.present(t, a.Key) || f.present(t, b.Key) {
				t.Error("a deleted input's object is still stored")
			}
			if keys := f.archives(t); !slices.Equal(keys, archives) {
				t.Errorf("archives = %v, want %v kept", keys, archives)
			}
		})
	}
}

// failingDelete is a store whose deletions fail.
type failingDelete struct{ *store.Mem }

func (failingDelete) Delete(context.Context, string) error { return errors.New("delete refused") }

// Retention lists the bucket, records its period and evicts the hour past its deadline:
// the hour's pack and the claim object nothing needs removed after the commit, the unattributed object kept for the operator,
// and the object an unresolved parked version names kept as a survivor until its version is terminated.
func TestRetain(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := f.retaining(t)
		capture := f.admit(t)
		seg := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		ts := hour*nsPerHour + 10
		released, needed := sdkObject(t, capture, 5, ts), sdkObject(t, capture, 6, ts)
		b.put(released)
		b.put(needed)
		b.put(ClaimInfo{Name: "upload.bin", Size: 3})
		v := &catalog.Identity{Version: catalog.VersionKey{Capture: capture, Seq: 6, Digest: [32]byte{6}}, Incarnation: 99, StreamSeq: 6, Hour: hour}
		if _, err := c.BeginParking(t.Context(), v, "claim-conflict", "claims/"+needed.Name, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := c.AdvanceParked(t.Context(), &v.Version); err != nil {
			t.Fatal(err)
		}

		rep := f.retain(t)
		if rep.Evicted != 1 || rep.Survivors != 1 || rep.Attributed != 2 || rep.Unattributed != 1 || rep.Tombstoned != 1 || rep.Failed != 0 {
			t.Fatalf("Retain = %+v", rep)
		}
		if f.present(t, seg.Key) {
			t.Error("the evicted hour's pack is still stored")
		}
		if got := b.names(); !slices.Equal(got, []string{needed.Name, "upload.bin"}) {
			t.Errorf("bucket = %v, want the survivor and the unattributed object", got)
		}
		r, err := c.Retention(t.Context())
		if err != nil || !r.Known || r.Period != month {
			t.Errorf("recorded retention = %+v, %v", r, err)
		}
		if rep := f.retain(t); rep.Released != 0 || len(b.names()) != 2 {
			t.Errorf("a survivor of an unresolved version was released: %+v", rep)
		}
		seq := uint64(6)
		if _, err := c.TerminateParked(t.Context(), capture, &seq, "op", "a1"); err != nil {
			t.Fatal(err)
		}
		if rep := f.retain(t); rep.Released != 1 {
			t.Errorf("Retain after the termination = %+v, want the survivor released", rep)
		}
		if got := b.names(); !slices.Equal(got, []string{"upload.bin"}) {
			t.Errorf("bucket = %v, want only the unattributed object", got)
		}
		if objs, err := c.ClaimObjects(t.Context()); err != nil || len(objs) != 0 {
			t.Errorf("claim objects = %v, %v, want none", objs, err)
		}
	})
}

// A crash between an eviction's commit and its bucket deletions: the next listing attributes the objects to the now scope-less hour,
// which is evicted again; a survivor whose deletion failed stays deletable and is removed by a later cycle.
func TestRetainConvergesAfterFailedDeletions(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := f.retaining(t)
		capture := f.admit(t)
		f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		obj := sdkObject(t, capture, 5, hour*nsPerHour+10)
		b.put(obj)
		b.failDeletes = true
		if rep := f.retain(t); rep.Evicted != 1 || rep.Failed != 1 {
			t.Fatalf("Retain = %+v", rep)
		}
		if got := b.names(); !slices.Equal(got, []string{obj.Name}) {
			t.Fatalf("bucket = %v, want the object a failed deletion left", got)
		}
		b.failDeletes = false
		if rep := f.retain(t); rep.Attributed != 1 || rep.Evicted != 1 {
			t.Errorf("the next Retain = %+v, want the object attributed and its hour evicted again", rep)
		}
		if got := b.names(); len(got) != 0 {
			t.Errorf("bucket = %v, want empty", got)
		}
	})
}

// A claim object of a capture not admitted is kept, whatever its hour; once the capture is admitted, it is evicted.
func TestRetainKeepsAnUnknownCapturesObject(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := f.retaining(t)
		capture := newID(t)
		obj := sdkObject(t, capture, 5, hour*nsPerHour+10)
		b.put(obj)
		if rep := f.retain(t); rep.Attributed != 1 || rep.Evicted != 0 {
			t.Fatalf("Retain = %+v, want attributed and kept", rep)
		}
		if _, err := c.Reject(t.Context(), &catalog.Identity{Version: catalog.VersionKey{Capture: capture}, Incarnation: 1, StreamSeq: 1, Hour: hour}, []byte("d")); err != nil {
			t.Fatal(err)
		}
		if rep := f.retain(t); rep.Evicted != 0 || len(b.names()) != 1 {
			t.Fatalf("Retain of a rejected capture's object = %+v", rep)
		}
		d := &catalog.Identity{Version: catalog.VersionKey{Capture: capture}, Incarnation: 1, StreamSeq: 2, Hour: hour}
		if _, err := f.c.ActivateConfig(t.Context(), []byte(`{"classifier":"go-secs/v2.8.0","max_frame_len":0}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Admit(t.Context(), d, &catalog.Descriptor{Bytes: []byte("d"), ToolID: tool, RecorderInstanceID: newID(t), Device: "eqp", OriginNS: 1}); err != nil {
			t.Fatal(err)
		}
		if rep := f.retain(t); rep.Evicted != 1 || len(b.names()) != 0 {
			t.Errorf("Retain once admitted = %+v, bucket %v", rep, b.names())
		}
	})
}

// The object store bucket lists the SDK's metadata and deletes an absent object without an error.
func TestObjectBucket(t *testing.T) {
	t.Parallel()
	js := connect(t, testbus.Start(t))
	os, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "claims"})
	if err != nil {
		t.Fatal(err)
	}
	obj := sdkObject(t, newID(t), 3, hour*nsPerHour)
	if _, err := os.Put(t.Context(), jetstream.ObjectMeta{Name: obj.Name, Metadata: obj.Metadata}, bytes.NewReader(make([]byte, 64))); err != nil {
		t.Fatal(err)
	}
	b := ObjectBucket{Store: os}
	got, err := b.List(t.Context())
	if err != nil || len(got) != 1 || got[0].Name != obj.Name || got[0].Size != 64 || attribute(got[0]) == nil {
		t.Fatalf("List = %+v, %v", got, err)
	}
	if err := b.Delete(t.Context(), obj.Name); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(t.Context(), obj.Name); err != nil {
		t.Errorf("Delete of an absent object = %v", err)
	}
	if got, err := b.List(t.Context()); err != nil || len(got) != 0 {
		t.Errorf("List of an empty bucket = %v, %v", got, err)
	}
}

// Deletion and retention time their tasks, count tombstones and read the claim gauges; a failed removal stamps no success.
func TestRetentionPassMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := f.retaining(t)
		s := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(s); err != nil {
			t.Fatal(err)
		}
		capture := f.admit(t)
		f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		needed := sdkObject(t, capture, 6, hour*nsPerHour+10)
		b.put(needed)
		v := &catalog.Identity{Version: catalog.VersionKey{Capture: capture, Seq: 6, Digest: [32]byte{6}}, Incarnation: 99, StreamSeq: 6, Hour: hour}
		if _, err := c.BeginParking(t.Context(), v, "claim-conflict", "claims/"+needed.Name, 0); err != nil {
			t.Fatal(err)
		}
		if err := f.r.retentionPass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		got := scrape(t, s)
		for _, want := range []string{
			`tracekeep_compact_cycle_seconds_count{task="delete"} 1`, `tracekeep_compact_cycle_seconds_count{task="retention"} 1`,
			`tracekeep_compact_last_success_timestamp{task="delete"}`, `tracekeep_compact_last_success_timestamp{task="retention"}`,
			"tracekeep_compact_tombstones_total 1", "tracekeep_compact_deletions_total 0",
			"tracekeep_compact_claim_objects 1", "tracekeep_compact_claim_object_bytes 64", "tracekeep_compact_claim_objects_survivors 1",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("/metrics lacks %s", want)
			}
		}
		g := metrics.NewServer()
		if f.r.Metrics, err = metrics.NewCompact(g); err != nil {
			t.Fatal(err)
		}
		f.stage(t, writeSegment(t, f.admit(t), []uint64{0}, noAdjust))
		f.r.Store = failingDelete{f.s}
		if err := f.r.retentionPass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		if got := scrape(t, g); strings.Contains(got, `last_success_timestamp{task="retention"}`) {
			t.Error("a retention task with a failed removal stamped a success")
		}
	})
}

// A retention shortened across a restart evicts nothing early: the first compactor records a month,
// a second one configured with a day evicts no hour the month still keeps until D has passed since it recorded the day.
func TestRetentionShortenedAcrossARestart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := f.retaining(t)
		capture := f.admit(t)
		seg := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		t0 := time.Unix(0, (hour+1)*nsPerHour).Add(2 * 24 * time.Hour) // two days after the hour: a month keeps it, a day does not
		at := func(when time.Time) func() time.Time { return func() time.Time { return when } }
		f.r.Now = at(t0)
		if rep := f.retain(t); rep.Evicted != 0 {
			t.Fatalf("Retain under a month = %+v", rep)
		}
		restarted := &Compactor{
			Catalog: c, Store: f.s, Log: f.r.Log, Claims: b, Bus: f.r.Bus, Retention: 24 * time.Hour, DeleteDelay: time.Hour,
			Now: at(t0.Add(10 * time.Minute)),
		}
		retain := func(r *Compactor) RetentionReport {
			var rep RetentionReport
			if err := r.Retain(t.Context(), f.lease, &rep); err != nil {
				t.Fatal(err)
			}

			return rep
		}
		if rep := retain(restarted); rep.Evicted != 0 || !f.present(t, seg.Key) {
			t.Fatalf("Retain right after shortening = %+v, want nothing evicted", rep)
		}
		restarted.Now = at(t0.Add(10*time.Minute + time.Hour - time.Second))
		if rep := retain(restarted); rep.Evicted != 0 {
			t.Fatalf("Retain just inside D = %+v, want nothing evicted", rep)
		}
		restarted.Now = at(t0.Add(10*time.Minute + time.Hour))
		if rep := retain(restarted); rep.Evicted != 1 || f.present(t, seg.Key) {
			t.Errorf("Retain once D passed = %+v, want the hour evicted", rep)
		}
	})
}

// A survivor's release whose bucket deletion fails leaves the row deletable and the object in place, counted as failed;
// the next cycle resumes it and removes both.
func TestSurvivorReleaseResumes(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		b := f.retaining(t)
		capture := f.admit(t)
		needed := sdkObject(t, capture, 6, hour*nsPerHour+10)
		b.put(needed)
		v := &catalog.Identity{Version: catalog.VersionKey{Capture: capture, Seq: 6, Digest: [32]byte{6}}, Incarnation: 99, StreamSeq: 6, Hour: hour}
		if _, err := c.BeginParking(t.Context(), v, "claim-conflict", "claims/"+needed.Name, 0); err != nil {
			t.Fatal(err)
		}
		if rep := f.retain(t); rep.Survivors != 1 {
			t.Fatalf("Retain = %+v, want a survivor", rep)
		}
		seq := uint64(6)
		if _, err := c.TerminateParked(t.Context(), capture, &seq, "op", "a1"); err != nil {
			t.Fatal(err)
		}
		b.failDeletes = true
		if rep := f.retain(t); rep.Released != 0 || rep.Failed != 1 {
			t.Fatalf("Retain with a failing bucket = %+v, want the release failed", rep)
		}
		objs, err := c.ClaimObjects(t.Context())
		if err != nil || len(objs) != 1 || objs[0].State != "deletable" || len(b.names()) != 1 {
			t.Fatalf("after the failed release: rows %+v, %v, bucket %v", objs, err, b.names())
		}
		b.failDeletes = false
		if rep := f.retain(t); rep.Released != 1 || len(b.names()) != 0 {
			t.Errorf("the resumed release = %+v, bucket %v", rep, b.names())
		}
	})
}

// A crash between an eviction's commit and its store deletions: the orphan pass deletes the tombstoned keys.
func TestEvictedPacksDeletedByTheOrphanPass(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.retaining(t)
		capture := f.admit(t)
		seg := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		f.r.Store = failingDelete{f.s}
		if rep := f.retain(t); rep.Evicted != 1 || rep.Failed != 1 {
			t.Fatalf("Retain = %+v, want the hour evicted and its removal failed", rep)
		}
		f.r.Store = f.s
		if !f.present(t, seg.Key) {
			t.Fatal("the failed removal removed the pack")
		}
		f.pass(t)
		if f.present(t, seg.Key) {
			t.Error("the orphan pass left an evicted pack's tombstoned key")
		}
	})
}

// An eviction a guard refuses, another hour's row naming one of its keys, is left in place and counted every cycle.
func TestRefusedEvictionIsCounted(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.retaining(t)
		capture := f.admit(t)
		seg := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		twin := *seg
		twin.ID, twin.Hour = newID(t), seg.Hour+5000
		if _, err := c.RegisterPack(t.Context(), &twin, catalog.Registration{Evidence: &catalog.Evidence{}}); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if rep := f.retain(t); rep.Refused != 1 || rep.Evicted != 0 {
				t.Fatalf("Retain = %+v, want the eviction refused", rep)
			}
		}
		if !f.present(t, seg.Key) {
			t.Error("a refused eviction removed the pack")
		}
	})
}

// A lease lost between an eviction's commit and its removals ends the pass with the loss, removes nothing and stamps no success;
// the tombstoned key is the orphan pass's to delete.
func TestLeaseLostBeforeAnEvictionsRemovals(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.retaining(t)
		s := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(s); err != nil {
			t.Fatal(err)
		}
		seg := f.stage(t, writeSegment(t, f.admit(t), []uint64{0, 1}, noAdjust))
		f.r.hooks[hookEvicted] = func() {
			if err := c.ReleaseLease(t.Context(), f.lease); err != nil {
				t.Error(err)
			}
			if _, ok, err := c.TakeLease(t.Context(), "compactor/2", "other", time.Hour); err != nil || !ok {
				t.Errorf("TakeLease = %v, %v", ok, err)
			}
		}
		if err := f.r.retentionPass(t.Context(), f.lease); !errors.Is(err, catalog.ErrLeaseLost) {
			t.Fatalf("retentionPass = %v, want ErrLeaseLost", err)
		}
		if !f.present(t, seg.Key) {
			t.Error("a worker that lost its lease removed an object")
		}
		if got := scrape(t, s); strings.Contains(got, `last_success_timestamp{task="retention"}`) {
			t.Error("a retention task that lost its lease stamped a success")
		}
	})
}

// The retention pass prunes the change cursor's events older than the period, a seq prefix only:
// a cursor in the pruned prefix then reads stale.
func TestRetainPrunesEvents(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.retaining(t)
		nowNs := f.r.Now().UnixNano()
		write, _ := c.Pools()
		capture := newID(t)
		for seq, at := range []int64{0, nowNs - int64(month) - 1, nowNs - int64(time.Minute), 0} {
			_, err := write.ExecContext(t.Context(), `INSERT INTO scope_events (seq, capture_id, hour, event, revision, at) VALUES (?, ?, 1, 'created', 1, ?)`,
				seq+1, capture[:], at)
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := write.ExecContext(t.Context(), `UPDATE counters SET value = 4 WHERE name = 'scope_events'`); err != nil {
			t.Fatal(err)
		}
		if rep := f.retain(t); rep.Pruned != 2 {
			t.Fatalf("Retain = %+v, want seqs 1 and 2 pruned", rep)
		}
		sel := catalog.Selector{ToolID: tool}
		for after, stale := range map[int64]bool{1: true, 2: false} {
			got, err := c.ListScopeChanges(t.Context(), catalog.Cursor{Seq: after}, sel, 10)
			if err != nil || got.Stale != stale {
				t.Errorf("cursor at %d: stale %v, %v; want %v", after, got.Stale, err, stale)
			}
		}
	})
}
