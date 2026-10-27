// Package compact holds the compactor's work under its catalog lease (docs/specs/tracekeep/compactor.md):
// the reconciler, which adopts orphaned packs (storage.md §5.4) and reconciles parked copy rows (parking.md *resolution*),
// and the merge of settled scopes (compactor.md §8.1).
package compact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

const nsPerHour = int64(time.Hour)

// Compactor holds the compactor lease and runs the compactor's passes under it:
// the reconciler's, which adopt the packs the catalog does not know and reconcile parked copy rows, and the merge.
// One Compactor runs at a time, the lease's holder; its methods are not safe for concurrent use.
type Compactor struct {
	Catalog *catalog.Catalog
	Store   store.ObjectStore
	// KeyPrefix is the pack keys' prefix, as ingest writes them.
	KeyPrefix string
	// HorizonHour returns the oldest hour retention keeps.
	HorizonHour func() int64
	Log         *slog.Logger
	// JS and Bus name the traffic and parking streams whose incarnations the reconciler checks and whose copy rows it reconciles;
	// each check takes a handle of its own, so no handle is shared between goroutines.
	JS  jetstream.JetStream
	Bus bus.Config
	// Holder names this process in the compactor lease, LeaseTTL is how long a take or a renewal holds it,
	// and Interval is the time between two cycles of Run; a lease that lapses between them passes to a standby, harmlessly.
	Holder   string
	LeaseTTL time.Duration
	Interval time.Duration
	// WorkDir holds the merge's working files; Writer is the service's writer tag, which every archive carries;
	// QuietPeriod is Q, how long after its hour ends a scope becomes a merge candidate (compactor.md §8.1);
	// Now is the clock that judges it, nil for time.Now.
	WorkDir     string
	Writer      string
	QuietPeriod time.Duration
	Now         func() time.Time

	// lease is the compactor lease while this process holds it.
	lease *catalog.Lease
	// hooks are the package's test seams.
	hooks [numHooks]func()
	// Metrics records the reconciler's metrics; nil records none.
	Metrics *metrics.Compact
	// holdErr is the last failure to read the lease's state, nil once it was read; Ready reports it.
	holdMu    sync.Mutex
	holdErr   error
	holdKnown bool
	// unknown holds the keys the last orphan pass found unknown, which this pass may adopt.
	unknown map[string]bool
}

// Quarantine is an orphan the reconciler will never register or delete: an operator decides.
type Quarantine struct {
	Key    string
	Reason string
}

// Unadmitted is an orphan of a capture no start has admitted; it stays unregistered until one does.
type Unadmitted struct {
	Key     string
	Capture tracepack.UUID
}

// OrphanReport is what an orphan pass did.
type OrphanReport struct {
	// Adopted are the keys registered as adopted packs.
	Adopted []string
	// Deleted are the keys deleted: tombstoned, beyond the horizon, or lost to a termination.
	Deleted []string
	// Tombstoned are the keys this pass tombstoned, beyond the horizon or lost to a termination, then deleted.
	Tombstoned []string
	// Quarantined and Unadmitted are reported, never deleted.
	Quarantined []Quarantine
	Unadmitted  []Unadmitted
	// Waiting are the unknown keys first seen by this pass, which the next pass examines.
	Waiting []string
}

// Orphans lists the pack keys under staging/ and archive/ and settles each the catalog does not register (storage.md §5.4).
// A tombstoned key is deleted again. An unknown key is examined only when the previous pass also found it unknown,
// since a healthy flush's key is unknown between its put and its registration.
// An examined key is verified in full and read for its exact seq set, its name checked against the pack,
// and registered as adopted under lease; the registration's own guards decide, not this listing.
// A lost lease ends the pass with ErrLeaseLost; an error on one key is logged and leaves the key for the next pass.
func (r *Compactor) Orphans(ctx context.Context, lease catalog.Lease) (OrphanReport, error) {
	var rep OrphanReport
	seen := map[string]bool{}
	for _, area := range []string{"staging/", "archive/"} {
		prefix := area
		if r.KeyPrefix != "" {
			prefix = r.KeyPrefix + "/" + area
		}
		err := r.Store.List(ctx, prefix, func(info store.ObjectInfo) error {
			err := r.settle(ctx, lease, info, seen, &rep)
			if errors.Is(err, catalog.ErrLeaseLost) || ctx.Err() != nil {
				return err
			}
			if err != nil {
				r.Log.Warn("compact: orphan", "key", info.Key, "error", err)
			}

			return nil
		})
		if err != nil {
			return rep, err
		}
	}
	r.unknown = seen

	return rep, nil
}

// settle handles one listed key.
func (r *Compactor) settle(ctx context.Context, lease catalog.Lease, info store.ObjectInfo, seen map[string]bool, rep *OrphanReport) error {
	standing, err := r.Catalog.KeyStanding(ctx, info.Key)
	if err != nil {
		return err
	}
	switch standing {
	case catalog.KeyRegistered:
		return nil
	case catalog.KeyTombstoned:
		// A delayed upload after retention or a refused registration: never registered.
		return r.delete(ctx, lease, info.Key, rep)
	case catalog.KeyUnknown:
	default:
		return fmt.Errorf("compact: unknown key standing %d", standing)
	}
	if !r.unknown[info.Key] {
		seen[info.Key] = true
		rep.Waiting = append(rep.Waiting, info.Key)

		return nil
	}
	// Examined now, so a key that fails is examined again by the next pass.
	seen[info.Key] = true
	p, reason, err := r.read(ctx, info)
	if err != nil {
		return err
	}
	if reason != "" {
		rep.Quarantined = append(rep.Quarantined, Quarantine{Key: info.Key, Reason: reason})
		r.Log.Error("compact: orphan quarantined", "key", info.Key, "reason", reason)

		return nil
	}

	return r.adopt(ctx, lease, p, rep)
}

// adopt registers p as adopted under lease and acts on the outcome.
func (r *Compactor) adopt(ctx context.Context, lease catalog.Lease, p *catalog.Pack, rep *OrphanReport) error {
	out, err := r.Catalog.RegisterPack(ctx, p, catalog.Registration{Lease: &lease, Adopted: true, HorizonHour: r.HorizonHour()})
	if errors.Is(err, catalog.ErrInvalidRegistration) {
		rep.Quarantined = append(rep.Quarantined, Quarantine{Key: p.Key, Reason: err.Error()})
		r.Log.Error("compact: orphan quarantined", "key", p.Key, "reason", err.Error())

		return nil
	}
	if err != nil {
		return err
	}
	switch out {
	case catalog.Registered:
		rep.Adopted = append(rep.Adopted, p.Key)
		r.Log.Info("compact: orphan adopted", "key", p.Key)
	case catalog.AlreadyRegistered:
		// Its registration committed after the listing's look.
	case catalog.BeyondHorizon, catalog.LostToTermination:
		// The key is now tombstoned, so the object goes; an orphan's messages were never acknowledged and are decided again.
		rep.Tombstoned = append(rep.Tombstoned, p.Key)
		return r.delete(ctx, lease, p.Key, rep)
	case catalog.Tombstoned:
		return r.delete(ctx, lease, p.Key, rep)
	case catalog.UnadmittedCapture:
		rep.Unadmitted = append(rep.Unadmitted, Unadmitted{Key: p.Key, Capture: p.Capture})
		r.Log.Warn("compact: orphan of an unadmitted capture", "key", p.Key, "capture", p.Capture)
	case catalog.FoundAdopted:
		return fmt.Errorf("compact: adoption of %s found it adopted, an outcome only evidence gets", p.Key)
	default:
		return fmt.Errorf("compact: unknown registration outcome %d", out)
	}

	return nil
}

// delete deletes the tombstoned key while this process holds lease: a worker that lost it does nothing further (compactor.md §8.1).
// The lease may be lost between the check and the deletion, which is harmless: a tombstoned key is never registered again.
func (r *Compactor) delete(ctx context.Context, lease catalog.Lease, key string, rep *OrphanReport) error {
	if err := r.Catalog.VerifyLease(ctx, lease); err != nil {
		return err
	}
	if err := r.Store.Delete(ctx, key); err != nil {
		return err
	}
	rep.Deleted = append(rep.Deleted, key)

	return nil
}

// read verifies the object of info in full and reads the pack it holds;
// a non-empty reason quarantines it, and an error leaves it for the next pass.
func (r *Compactor) read(ctx context.Context, info store.ObjectInfo) (*catalog.Pack, string, error) {
	opened, err := r.Store.Open(ctx, info.Key)
	if err != nil {
		return nil, "", err
	}
	defer opened.Close()
	obj := reading{opened}
	vr, err := tracepack.Verify(ctx, obj, obj.Size(), tracepack.VerifyOptions{})
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, errStoreRead) || errors.Is(err, store.ErrNotFound) {
			return nil, "", err
		}
		// A pack Verify cannot open is no finalized pack.
		return nil, fmt.Sprintf("verify: %v", err), nil
	}
	if reason := judge(&vr); reason != "" {
		return nil, reason, nil
	}
	rd, err := tracepack.Open(ctx, obj, obj.Size(), tracepack.ReaderOptions{})
	if err != nil {
		return nil, "", err
	}
	p, reason := describe(rd, r.KeyPrefix, info)
	if reason != "" {
		return nil, reason, nil
	}
	if err := records(ctx, rd, p); err != nil {
		return nil, "", err
	}
	if len(p.Ranges) == 0 {
		return nil, "the pack holds no record", nil
	}

	return p, "", nil
}

// errStoreRead marks a failure of the store's own read, which the next pass retries;
// any other error of Verify is the object's, which is quarantined.
var errStoreRead = errors.New("compact: store read")

// reading tags the read errors of an object, its end apart, as the store's.
type reading struct{ store.Object }

func (o reading) ReadAt(b []byte, off int64) (int, error) {
	n, err := o.Object.ReadAt(b, off)
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("%w: %w", errStoreRead, err)
	}

	return n, err
}

// judge is the quarantine rule of storage.md §5.4 on a verification: anything but finalized-consistent, or any writer defect.
func judge(vr *tracepack.VerifyReport) string {
	switch {
	case vr.Outcome != tracepack.OutcomeFinalizedConsistent:
		return fmt.Sprintf("verification outcome %v", vr.Outcome)
	case len(vr.WriterDefects) > 0:
		return fmt.Sprintf("%d writer defects", len(vr.WriterDefects))
	default:
		return ""
	}
}

// describe reads the pack's identity from its header and checks that the key spells it ([STO §3]):
// a segment's capture, first seq and pack id, an archive's hour, capture and pack id.
func describe(rd *tracepack.Reader, keyPrefix string, info store.ObjectInfo) (*catalog.Pack, string) {
	h := rd.Header()
	m := h.Meta
	if m == nil {
		return nil, "the pack has no metadata"
	}
	// shortcut: a close (seq_start 2^63-1, the close sentinel, which no segment's registration takes) is left to the compactor that publishes it;
	// adopt a close's orphan here, as a close holding no range, once the compactor writes closes.
	if m.SeqStart == math.MaxInt64 {
		return nil, "a close, which only its publication registers"
	}
	p := &catalog.Pack{
		ID: h.PackID, Capture: h.CaptureID, ToolID: m.ToolID, Hour: floorHour(m.PeriodStart), Key: info.Key, Size: info.Size,
		SeqStart: m.SeqStart, FormatMinor: int(h.FormatMinor),
	}
	var key string
	var err error
	switch m.PackRole {
	case tracepack.PackRoleSegment:
		p.Role = catalog.RoleSegment
		key, err = tracepack.SegmentKey(keyPrefix, m.ToolID, h.CaptureID, m.SeqStart, h.PackID)
	case tracepack.PackRoleArchive:
		p.Role = catalog.RoleArchive
		key, err = tracepack.ArchiveKey(keyPrefix, m.ToolID, h.CaptureID, p.Hour, h.PackID)
	case tracepack.PackRoleUnknown, tracepack.PackRoleExtract, tracepack.PackRoleRepair:
		return nil, fmt.Sprintf("pack role %v, which no tracekeep key holds", m.PackRole)
	default:
		return nil, fmt.Sprintf("pack role %v", m.PackRole)
	}
	if err != nil {
		return nil, fmt.Sprintf("key: %v", err)
	}
	if key != info.Key {
		return nil, fmt.Sprintf("the pack's key is %s", key)
	}
	for _, cv := range m.Coverage {
		if cv.CaptureID == nil || *cv.CaptureID == h.CaptureID {
			p.Losses = append(p.Losses, catalog.Loss{SeqFrom: cv.SeqFirst, SeqTo: cv.SeqLast, TSFrom: cv.TimeStart, TSTo: cv.TimeEnd})
		}
	}

	return p, ""
}

func floorHour(ns int64) int64 {
	h := ns / nsPerHour
	if ns%nsPerHour < 0 {
		h--
	}

	return h
}

// records reads every record of rd: the exact seq runs with the times of their endpoints, and a stop boundary.
func records(ctx context.Context, rd *tracepack.Reader, p *catalog.Pack) error {
	type point struct {
		seq uint64
		ts  int64
	}
	var pts []point
	// A stop is told by its transport event's payload, which Iterate reads only on request.
	res, err := rd.Iterate(ctx, tracepack.Query{Payloads: true}, func(it *tracepack.Item) error {
		rec := &it.Record
		pts = append(pts, point{rec.Seq, rec.TSUTCNs})
		if isStop(rec) {
			p.Stop = true
		}

		return nil
	})
	if err != nil {
		return err
	}
	for _, d := range res.Incomplete {
		// The pack's own coverage entries and crash gaps say what it never held; any other defect is a fault of this read,
		// since Verify validated every block, and the next pass reads again.
		if d.Reason != tracepack.ReasonCoverage && d.Reason != tracepack.ReasonCrashGap {
			return fmt.Errorf("compact: read of %s incomplete: %v", p.Key, d.Reason)
		}
	}
	slices.SortFunc(pts, func(a, b point) int {
		switch {
		case a.seq < b.seq:
			return -1
		case a.seq > b.seq:
			return 1
		default:
			return 0
		}
	})
	for _, pt := range pts {
		if n := len(p.Ranges); n > 0 && p.Ranges[n-1].SeqTo >= pt.seq {
			continue // an equal copy of a seq already counted
		} else if n > 0 && p.Ranges[n-1].SeqTo+1 == pt.seq {
			p.Ranges[n-1].SeqTo, p.Ranges[n-1].TSTo = pt.seq, pt.ts
			continue
		}
		p.Ranges = append(p.Ranges, catalog.Range{SeqFrom: pt.seq, SeqTo: pt.seq, TSFrom: pt.ts, TSTo: pt.ts})
	}

	return nil
}

// isStop reports a capture-boundary transport event of kind stop.
func isStop(rec *tracepack.Record) bool {
	if rec.Kind != tracepack.KindTransportEvent {
		return false
	}
	ev, err := tracepack.UnmarshalTransportEvent(rec.Payload)

	return err == nil && ev.Event == tracepack.EventCaptureBoundary && ev.BoundaryKind != nil && *ev.BoundaryKind == tracepack.BoundaryKindStop
}
