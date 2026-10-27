package compact

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/producer"
)

// ClaimBucket is the claim bucket as the compactor reads it (compactor.md §8.4).
type ClaimBucket interface {
	// List returns every object the bucket holds, with its metadata.
	List(ctx context.Context) ([]ClaimInfo, error)
	// Delete deletes the object named name; an absent object is no error.
	Delete(ctx context.Context, name string) error
}

// ClaimInfo is one object of the claim bucket: its bare name, size and metadata.
type ClaimInfo struct {
	Name     string
	Size     int64
	Metadata map[string]string
}

// ObjectBucket is the claim bucket over a NATS object store.
type ObjectBucket struct{ Store jetstream.ObjectStore }

// List implements ClaimBucket.
func (o ObjectBucket) List(ctx context.Context) ([]ClaimInfo, error) {
	infos, err := o.Store.List(ctx)
	if errors.Is(err, jetstream.ErrNoObjectsFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]ClaimInfo, 0, len(infos))
	for _, in := range infos {
		out = append(out, ClaimInfo{Name: in.Name, Size: int64(in.Size), Metadata: in.Metadata}) //nolint:gosec // an object's size fits
	}

	return out, nil
}

// Delete implements ClaimBucket.
func (o ObjectBucket) Delete(ctx context.Context, name string) error {
	if err := o.Store.Delete(ctx, name); err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
		return err
	}

	return nil
}

// headerLen is the length of a record header, the body of a claim-check message.
const headerLen = 44

// attribute attributes the claim object info of bucket by its name, <capture_id>.<seq>, and the SDK's metadata:
// the record's 44-byte header, whose seq and time must match the name and the time metadata, and the hour of that time.
// It returns nil for an object the SDK did not write, which only an operator deletes (compactor.md §8.4).
func attribute(info ClaimInfo) *catalog.ClaimObject {
	capture, seq, err := tracepack.ParseBusMsgID(info.Name)
	if err != nil {
		return nil
	}
	header, err := hex.DecodeString(info.Metadata[producer.ObjectMetaRecordHeader])
	if err != nil || len(header) != headerLen || binary.LittleEndian.Uint64(header[:8]) != seq {
		return nil
	}
	ts, err := strconv.ParseInt(info.Metadata[producer.ObjectMetaTSUTCNs], 16, 64)
	if err != nil || ts != int64(binary.LittleEndian.Uint64(header[8:16])) { //nolint:gosec // ts_utc_ns is signed little-endian, header bytes 8–15
		return nil
	}

	return &catalog.ClaimObject{Capture: capture, Seq: seq, Hour: floorHour(ts)}
}

// claimKey is the bucket-qualified key of the object named name, as a message's Tpk-Payload-Ref names it.
func (r *Compactor) claimKey(name string) string { return r.Bus.ClaimBucket + "/" + name }

// claimName is the bare name of the bucket-qualified key, false for a key of another bucket.
func (r *Compactor) claimName(key string) (string, bool) {
	return strings.CutPrefix(key, r.Bus.ClaimBucket+"/")
}

// RetentionReport is what a deletion and retention cycle did.
type RetentionReport struct {
	// Deleted counts the merged inputs deleted, Evicted the hours evicted and Tombstoned the keys their transactions tombstoned.
	Deleted, Evicted, Tombstoned int
	// Attributed and Unattributed count the listing's objects without a row, by whether the SDK's metadata attributes them;
	// Survivors counts the claim objects an eviction kept, and Released the survivors deleted.
	Attributed, Unattributed, Survivors, Released int
	// Failed counts the steps that failed and were left for the next cycle.
	Failed int
}

// Deletions deletes the merged inputs past their merge's delete_at (compactor.md §8.3): each by DeletePack, its object removed after the commit;
// a failed removal is left to the orphan pass, which deletes a tombstoned key again.
func (r *Compactor) Deletions(ctx context.Context, lease catalog.Lease, rep *RetentionReport) error {
	nowNs := r.now().UnixNano()
	cands, err := r.Catalog.DeleteCandidates(ctx, nowNs)
	if err != nil {
		return err
	}
	for _, d := range cands {
		ok, err := r.Catalog.DeletePack(ctx, lease, d, nowNs)
		if err := r.stepFailed(ctx, err, "delete a merged input", d.Key, rep); err != nil {
			return err
		}
		if !ok {
			continue
		}
		rep.Deleted++
		rep.Tombstoned++
		r.removeObject(ctx, lease, d.Key)
	}

	return nil
}

// Retain runs retention (compactor.md §8.3–§8.4): the claim bucket listed and recorded, the configured period recorded,
// every hour past its deadline evicted with its objects removed after the commit, then the survivors §8.4's predicate releases.
func (r *Compactor) Retain(ctx context.Context, lease catalog.Lease, rep *RetentionReport) error {
	listed, err := r.Claims.List(ctx)
	if err != nil {
		return err
	}
	objs := make([]catalog.Listed, 0, len(listed))
	for _, in := range listed {
		objs = append(objs, catalog.Listed{Key: r.claimKey(in.Name), Size: in.Size, Attr: attribute(in)})
	}
	added, err := r.Catalog.RecordListing(ctx, lease, objs, r.now().UnixNano())
	if err != nil {
		return err
	}
	rep.Attributed += added
	unattributed, err := r.Catalog.UnattributedObjects(ctx)
	if err != nil {
		return err
	}
	rep.Unattributed = len(unattributed)
	nowNs := r.now().UnixNano()
	if _, err := r.Catalog.RecordRetention(ctx, lease, r.Retention, r.DeleteDelay, nowNs); err != nil {
		return err
	}
	cands, err := r.Catalog.EvictCandidates(ctx, nowNs)
	if err != nil {
		return err
	}
	for _, s := range cands {
		ev, err := r.Catalog.EvictHour(ctx, lease, s, nowNs)
		if err := r.stepFailed(ctx, err, "evict an hour", s.Capture.String(), rep); err != nil {
			return err
		}
		if !ev.Evicted {
			continue
		}
		rep.Evicted++
		rep.Tombstoned += len(ev.Packs)
		rep.Survivors += ev.Survivors
		for _, key := range ev.Packs {
			r.removeObject(ctx, lease, key)
		}
		for _, key := range ev.Claims {
			r.removeClaim(ctx, lease, key)
		}
	}

	return r.releaseSurvivors(ctx, lease, rep)
}

// releaseSurvivors deletes every survivor §8.4's predicate now frees: marked deletable, its object deleted, its row removed,
// each step resumed by the next cycle after a failure.
func (r *Compactor) releaseSurvivors(ctx context.Context, lease catalog.Lease, rep *RetentionReport) error {
	survivors, err := r.Catalog.ClaimObjects(ctx, "survivor", "deletable")
	if err != nil {
		return err
	}
	for _, o := range survivors {
		ok, err := r.Catalog.MarkSurvivorDeletable(ctx, lease, o.Key)
		if err := r.stepFailed(ctx, err, "release a survivor", o.Key, rep); err != nil {
			return err
		}
		if !ok || !r.removeClaim(ctx, lease, o.Key) {
			continue
		}
		err = r.Catalog.RemoveClaimObject(ctx, lease, o.Key)
		if err := r.stepFailed(ctx, err, "remove a released survivor", o.Key, rep); err != nil {
			return err
		}
		if err == nil {
			rep.Released++
		}
	}

	return nil
}

// stepFailed returns a lost lease or a done context, and logs and counts any other failure of one step, which the next cycle retries.
func (r *Compactor) stepFailed(ctx context.Context, err error, what, key string, rep *RetentionReport) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, catalog.ErrLeaseLost) || ctx.Err() != nil {
		return err
	}
	r.Log.Error("compact: "+what, "key", key, "error", err)
	rep.Failed++

	return nil
}

// removeObject removes the tombstoned pack key from the store while lease is held; a failure is logged and left to the orphan pass.
func (r *Compactor) removeObject(ctx context.Context, lease catalog.Lease, key string) {
	if err := r.Catalog.VerifyLease(ctx, lease); err != nil {
		return
	}
	if err := r.Store.Delete(ctx, key); err != nil {
		r.Log.Warn("compact: delete a tombstoned pack", "key", key, "error", err)
	}
}

// removeClaim deletes the claim object of key from the bucket while lease is held and reports whether it is gone;
// a failure is logged, and the next listing attributes the object again.
func (r *Compactor) removeClaim(ctx context.Context, lease catalog.Lease, key string) bool {
	name, ok := r.claimName(key)
	if !ok {
		r.Log.Warn("compact: a claim object of another bucket", "key", key)
		return false
	}
	if err := r.Catalog.VerifyLease(ctx, lease); err != nil {
		return false
	}
	if err := r.Claims.Delete(ctx, name); err != nil {
		r.Log.Warn("compact: delete a claim object", "key", key, "error", err)
		return false
	}

	return true
}
