package ingest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
)

// workFilePattern names a flush's working file under the working directory.
const workFilePattern = "segment-*.tpk"

// nsPerHour is one hour in nanoseconds, the span of a scope.
const nsPerHour = int64(time.Hour)

// Flusher writes a batch's segments, puts and registers them, then acknowledges each message (ingest.md §6.2, §6.3).
type Flusher struct {
	Catalog *catalog.Catalog
	Store   store.ObjectStore
	// WorkDir holds the working files; each holds masked bytes only and lives until its put returns (storage.md §5.1).
	WorkDir string
	// KeyPrefix prefixes every key the store holds; empty for none.
	KeyPrefix string
	// Configs are the processing configurations this replica executes, by id.
	Configs map[int64]Config
	// Interval is F, recorded in every segment as its flush interval.
	Interval time.Duration
	// HorizonHour returns the oldest hour retention keeps.
	HorizonHour func() int64
	// Writer names this writer in every segment's metadata.
	Writer string
	// Now returns the time a segment's writer_start is stamped with.
	Now func() time.Time

	// hooks are the package's test seams, each between two side effects of a flush; an error is a crash there.
	hooks [numFlushHooks]func() error
}

// Flush hooks: the crash points of ingest.md §6.3.
const (
	// hookWrite runs before a segment's records are appended; its error is the Writer's refusal.
	hookWrite = iota
	// hookAfterPut runs once a segment is put, before its registration.
	hookAfterPut
	// hookAfterRegister runs once every segment is registered, before the first acknowledgement.
	hookAfterRegister
	// hookAfterAck runs after each acknowledgement.
	hookAfterAck
	numFlushHooks
)

func (f *Flusher) hook(id int) error {
	if h := f.hooks[id]; h != nil {
		return h()
	}

	return nil
}

// Flushed is what a flush did.
type Flushed struct {
	// Packs are the segments registered, the batch's first and then its conflict segments.
	Packs []tracepack.UUID
	// BeyondHorizon reports that the batch's hour passed the retention horizon before its registration:
	// nothing was registered, and the caller terminates its messages as beyond-retention.
	BeyondHorizon bool
	// Acked counts the batch's entries acknowledged, in the order of b.Entries after the flush;
	// a flush that fails during the acknowledgements leaves the rest for the caller.
	Acked int
}

// segment is one pack a flush writes: the batch's first versions, or one rank of further versions.
type segment struct {
	id   tracepack.UUID
	role string
	// entries are the versions the segment holds, one entry each.
	entries []Entry
	// copies are every entry whose version the segment holds, equal copies included: each is delivery evidence.
	copies []Entry
	// others names, for each seq of a conflict segment, the pack holding the version it conflicts with.
	others []catalog.Version
}

// working is a segment written to its working file, ready to be put.
type working struct {
	file  *os.File
	size  int64
	minor uint16
}

// Flush writes b's records as segments, puts each under its key and registers it with delivery evidence,
// then acknowledges every message of the batch; the messages of a batch that fails anywhere are left unacknowledged.
// Equal copies of a record are written once, and every further version of a seq goes into a conflict segment (ingest.md §6.2).
// Every segment is written before the first put, so a batch the Writer refuses registers nothing,
// and one retention horizon judges every segment of the batch.
func (f *Flusher) Flush(ctx context.Context, b *Batch) (Flushed, error) {
	cfg, ok := f.Configs[b.ConfigID]
	if !ok {
		return Flushed{}, fmt.Errorf("ingest: configuration %d is not executed here", b.ConfigID)
	}
	capture, err := f.Catalog.Capture(ctx, b.Key.Capture)
	if err != nil {
		return Flushed{}, err
	}
	segs := split(b)
	files := make([]working, 0, len(segs))
	defer func() {
		for _, w := range files {
			_ = w.file.Close()
			_ = os.Remove(w.file.Name())
		}
	}()
	for i := range segs {
		w, err := f.writeFile(b, &capture, &cfg, &segs[i])
		if err != nil {
			return Flushed{}, err
		}
		files = append(files, w)
	}
	horizon := f.HorizonHour()
	var out Flushed
	for i := range segs {
		outcome, err := f.register(ctx, b, &capture, &segs[i], &files[i], horizon)
		if err != nil {
			return Flushed{}, err
		}
		if outcome == catalog.BeyondHorizon {
			// Only the first segment can meet it: every segment is judged by the same horizon and hour.
			return Flushed{BeyondHorizon: true}, nil
		}
		out.Packs = append(out.Packs, segs[i].id)
	}
	if err := f.hook(hookAfterRegister); err != nil {
		return out, err
	}
	for _, e := range b.Entries {
		if err := ack(ctx, e); err != nil {
			return out, err // the rest are redelivered, written again and deduplicated by the merge
		}
		out.Acked++
		if err := f.hook(hookAfterAck); err != nil {
			return out, err
		}
	}

	return out, nil
}

// CleanWorkDir removes the working files a crashed process left: their messages were never acknowledged (storage.md §5.1).
func CleanWorkDir(dir string) error {
	names, err := filepath.Glob(filepath.Join(dir, workFilePattern))
	if err != nil {
		return err
	}
	for _, n := range names {
		if err := os.Remove(n); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	return nil
}

// split sorts b's entries by seq and parts them into segments:
// the first version of each seq in the batch's segment, the n-th further version of a seq in the n-th conflict segment;
// an equal copy of a version is written once.
func split(b *Batch) []segment {
	slices.SortStableFunc(b.Entries, func(x, y Entry) int { return cmpSeq(x.D.Raw.Version.Seq, y.D.Raw.Version.Seq) })
	segs := []segment{{id: newPackID(), role: catalog.RoleSegment}}
	for i := 0; i < len(b.Entries); {
		seq := b.Entries[i].D.Raw.Version.Seq
		var versions []Entry
		for ; i < len(b.Entries) && b.Entries[i].D.Raw.Version.Seq == seq; i++ {
			e := b.Entries[i]
			rank := slices.IndexFunc(versions, func(v Entry) bool { return v.D.Raw.Version == e.D.Raw.Version })
			if rank < 0 {
				rank = len(versions)
				versions = append(versions, e)
				for len(segs) <= rank {
					segs = append(segs, segment{id: newPackID(), role: catalog.RoleConflict})
				}
				segs[rank].entries = append(segs[rank].entries, e)
				if rank > 0 {
					segs[rank].others = append(segs[rank].others, catalog.Version{Seq: seq, Pack: segs[0].id})
				}
			}
			segs[rank].copies = append(segs[rank].copies, e)
		}
	}

	return segs
}

// writeFile writes seg to a working file.
func (f *Flusher) writeFile(b *Batch, capture *catalog.Capture, cfg *Config, seg *segment) (working, error) {
	tmp, err := os.CreateTemp(f.WorkDir, workFilePattern)
	if err != nil {
		return working{}, fmt.Errorf("ingest: working file: %w", err)
	}
	minor, err := f.write(tmp, b, capture, cfg, seg)
	if err == nil {
		var size int64
		if size, err = tmp.Seek(0, io.SeekEnd); err == nil {
			return working{file: tmp, size: size, minor: minor}, nil
		}
	}
	_ = tmp.Close()
	_ = os.Remove(tmp.Name())

	return working{}, err
}

// register puts seg's working file under its key and registers it, judged against the retention horizon hour.
func (f *Flusher) register(ctx context.Context, b *Batch, capture *catalog.Capture, seg *segment, w *working, horizon int64) (catalog.PackOutcome, error) {
	first := seg.entries[0].D.Raw.Version.Seq
	key, err := tracepack.SegmentKey(f.KeyPrefix, capture.ToolID, b.Key.Capture, first, seg.id)
	if err != nil {
		return 0, fmt.Errorf("ingest: segment key: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if err := f.Store.Put(ctx, key, w.file, w.size); err != nil {
		return 0, err
	}
	if err := f.hook(hookAfterPut); err != nil {
		return 0, err
	}
	p := &catalog.Pack{
		ID: seg.id, Capture: b.Key.Capture, ToolID: capture.ToolID, Hour: b.Key.Hour, Role: seg.role, Key: key, Size: w.size,
		SeqStart: first, FormatMinor: int(w.minor), Ranges: ranges(seg.entries), Stop: hasStop(seg.entries), Conflicts: seg.others,
	}
	outcome, err := f.Catalog.RegisterPack(ctx, p, catalog.Registration{Evidence: evidence(seg.copies), HorizonHour: horizon})
	if err != nil {
		return 0, err
	}
	switch outcome {
	case catalog.Registered:
		return outcome, nil
	case catalog.BeyondHorizon:
		// The key is tombstoned; the object goes, and so would a put delayed across eviction.
		return outcome, f.Store.Delete(ctx, key)
	case catalog.AlreadyRegistered, catalog.Tombstoned, catalog.UnadmittedCapture:
		// A fresh pack id meets no earlier registration or tombstone, and its capture is admitted.
		return outcome, fmt.Errorf("ingest: registration of the fresh pack %s returned outcome %d", seg.id, outcome)
	case catalog.FoundAdopted:
		// The reconciler adopted the put object while its registration was in flight (ingest.md §6.3):
		// it wrote no receipts, so the batch is not acknowledged and its redelivery registers it again.
		return outcome, fmt.Errorf("ingest: the pack %s was adopted before its registration", seg.id)
	default:
		return outcome, fmt.Errorf("ingest: unknown registration outcome %d", outcome)
	}
}

// write writes seg's records with a Writer to w and returns the pack's format_minor:
// the producer assigned the seqs and detected the clock steps, so the Writer does neither ([SEM §4]).
func (f *Flusher) write(w *os.File, b *Batch, capture *catalog.Capture, cfg *Config, seg *segment) (uint16, error) {
	meta, err := tracepack.DecodeDescriptor(capture.Descriptor)
	if err != nil {
		return 0, fmt.Errorf("ingest: descriptor of %s: %w", b.Key.Capture, err)
	}
	interval := uint64(f.Interval.Nanoseconds()) //nolint:gosec // a positive duration
	meta.Writer = f.Writer
	meta.PackRole = tracepack.PackRoleSegment
	meta.SeqStart = seg.entries[0].D.Raw.Version.Seq
	meta.PeriodStart, meta.PeriodEnd = b.Key.Hour*nsPerHour, (b.Key.Hour+1)*nsPerHour
	meta.FlushIntervalNs = &interval
	meta.ScopeGeneration = new(uint64) // 0: a segment, which no merge produced
	meta.Classifiers, meta.MaxFrameLens = []string{cfg.Name}, []uint64{cfg.MaxFrameLen}
	tw, err := tracepack.NewWriter(w, tracepack.WriterOptions{
		Meta: meta, Facts: facts(seg.entries), PackID: seg.id, CaptureID: b.Key.Capture, Sync: w, Now: f.Now,
	})
	if err != nil {
		return 0, refusal(b, err)
	}
	if err := f.hook(hookWrite); err != nil {
		return 0, refusal(b, err)
	}
	for _, e := range seg.entries {
		r := e.D.Record.Record
		if err := tw.Append(&r); err != nil {
			return 0, refusal(b, fmt.Errorf("append seq %d: %w", r.Seq, err))
		}
	}
	if _, err := tw.Close(); err != nil {
		return 0, refusal(b, err)
	}
	size, err := w.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	r, err := tracepack.Open(context.Background(), w, size, tracepack.ReaderOptions{})
	if err != nil {
		return 0, fmt.Errorf("ingest: reopen the working file: %w", err)
	}

	return r.Header().FormatMinor, nil
}

// errWrite marks a batch the Writer refused: deterministic, unlike a failed put or a failed working file (ingest.md §6.1 `write-failure`).
var errWrite = errors.New("ingest: write")

// refusalError is the Writer's refusal of a batch, named by the batch's capture, hour and configuration:
// a live capture's redelivered batch gains records between tries, so its messages do not name it.
type refusalError struct {
	batch string
	err   error
}

func (e *refusalError) Error() string { return fmt.Sprintf("%v: %v", errWrite, e.err) }

func (e *refusalError) Unwrap() []error { return []error{errWrite, e.err} }

// refusals are the Writer's validation errors; any other error, such as the working file's, is transient.
var refusals = []error{
	tracepack.ErrSeqOrder, tracepack.ErrPayloadTooLarge, tracepack.ErrFieldValidity, tracepack.ErrMetadataCommitment,
	tracepack.ErrValidation, tracepack.ErrScopeBreach, tracepack.ErrInvalidRecord, tracepack.ErrMergeLimit,
	tracepack.ErrRequiredTag, tracepack.ErrFieldValue, tracepack.ErrFieldLength, tracepack.ErrAnnotationText, tracepack.ErrReservedTag,
}

// refusal returns err as a refusal of b when it is one of the Writer's validation errors, and as a plain error otherwise.
func refusal(b *Batch, err error) error {
	if slices.ContainsFunc(refusals, func(r error) bool { return errors.Is(err, r) }) {
		return &refusalError{batch: batchName(b), err: err}
	}

	return fmt.Errorf("ingest: write: %w", err)
}

// batchName names b's capture, hour and configuration.
func batchName(b *Batch) string {
	return fmt.Sprintf("%s/%d/%d", b.Key.Capture, b.Key.Hour, b.ConfigID)
}

// facts are what the segment's metadata is validated against.
func facts(entries []Entry) tracepack.PackFacts {
	f := tracepack.PackFacts{DurableRecorder: true}
	for _, e := range entries {
		ds := e.D.Record.Record.DecodeStatus
		if ds != tracepack.DecodeStatusNotAttempted && ds != tracepack.DecodeStatusNotApplicable {
			f.AnyClassified = true
		}
		if ds == tracepack.DecodeStatusOversized {
			f.AnyOversized = true
		}
	}

	return f
}

// ranges are the exact seq runs of sorted entries, with the times of each run's first and last record.
func ranges(entries []Entry) []catalog.Range {
	var rs []catalog.Range
	for _, e := range entries {
		seq, ts := e.D.Raw.Version.Seq, e.D.Raw.TS
		if n := len(rs); n > 0 && rs[n-1].SeqTo+1 == seq {
			rs[n-1].SeqTo, rs[n-1].TSTo = seq, ts
			continue
		}
		rs = append(rs, catalog.Range{SeqFrom: seq, SeqTo: seq, TSFrom: ts, TSTo: ts})
	}

	return rs
}

// hasStop reports a stop boundary among the entries.
func hasStop(entries []Entry) bool {
	return slices.ContainsFunc(entries, func(e Entry) bool {
		r := &e.D.Record.Record
		if r.Kind != tracepack.KindTransportEvent {
			return false
		}
		ev, err := tracepack.UnmarshalTransportEvent(r.Payload)

		return err == nil && ev.Event == tracepack.EventCaptureBoundary && ev.BoundaryKind != nil && *ev.BoundaryKind == tracepack.BoundaryKindStop
	})
}

// evidence is the delivery evidence of the entries: a receipt for each record that travelled by claim-check.
func evidence(entries []Entry) *catalog.Evidence {
	ev := &catalog.Evidence{}
	for _, e := range entries {
		if !e.D.Raw.ClaimCheck {
			continue
		}
		v := e.D.Raw.Version
		// The object's key is its Tpk-Payload-Ref, "<bucket>/<Nats-Msg-Id>".
		ev.Receipts = append(ev.Receipts, catalog.Receipt{
			Seq: v.Seq, Header: v.Header, PayloadSHA256: v.Digest, ObjectKey: e.D.Msg.Env.PayloadRef, ObjectSize: int64(len(e.D.Claim)),
		})
	}

	return ev
}

// ack acknowledges e's message once a segment holding its record is registered; a claim-check message's acknowledgement is confirmed,
// since its object's deletion depends on it (architecture.md §3.1).
func ack(ctx context.Context, e Entry) error {
	if e.D.Raw.ClaimCheck {
		return e.Msg.DoubleAck(ctx)
	}

	return e.Msg.Ack()
}

func cmpSeq(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func newPackID() tracepack.UUID {
	// A UUIDv7: the time first, so pack ids sort by creation.
	var id tracepack.UUID
	_, _ = rand.Read(id[:])
	ms := uint64(time.Now().UnixMilli()) //nolint:gosec // a time after 1970
	for i := range 6 {
		id[i] = byte(ms >> (40 - 8*i))
	}
	id[6] = id[6]&0x0f | 0x70
	id[8] = id[8]&0x3f | 0x80

	return id
}
