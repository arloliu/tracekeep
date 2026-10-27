package compact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
)

// workFilePattern names a merge's working file under the working directory.
const workFilePattern = "archive-*.tpk"

// CleanWorkDir removes the merges' and closes' working files a crashed process left (storage.md §5.1):
// their scopes are merged again, and their closes written again from their intents, next cycle. It removes nothing else.
func CleanWorkDir(dir string) error {
	for _, pattern := range []string{workFilePattern, closeFilePattern} {
		names, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return err
		}
		for _, n := range names {
			if err := os.Remove(n); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}

	return nil
}

// MergeReport is what a merge pass did, scope by scope.
type MergeReport struct {
	// Merged scopes are settled.
	Merged []catalog.Scope
	// Stale scopes changed between their snapshot and their commit; the next pass merges them again,
	// and the archive put is left to the reconciler.
	Stale []catalog.Scope
	// Conflicted scopes hold records that differ between their inputs; the merge fails until the conflict is resolved.
	Conflicted []catalog.Scope
	// Failed scopes could not be merged (a damaged input, a failed read or put); each stays as it was, and an operator acts.
	Failed []catalog.Scope
}

// Merges merges every scope whose hour has ended and whose quiet period has passed, under lease (compactor.md §8.1):
// a snapshot of the scope's revision and staged inputs, tracepack.Merge into a working file, the archive put under its key,
// then one transaction guarded by the revision and the lease. A rolled-back merge never deletes its archive: the reconciler owns it.
// A failure of one scope is logged and leaves it as it was; a lost lease ends the pass with ErrLeaseLost.
func (r *Compactor) Merges(ctx context.Context, lease catalog.Lease) (MergeReport, error) {
	var rep MergeReport
	ready := floorHour(r.now().Add(-r.QuietPeriod).UnixNano())
	scopes, err := r.Catalog.MergeCandidates(ctx, ready)
	if err != nil {
		return rep, err
	}
	for _, s := range scopes {
		_, err := r.attempt(ctx, lease, s, nil, &rep)
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
		if errors.Is(err, catalog.ErrLeaseLost) {
			return rep, err
		}
	}

	return rep, nil
}

// attempt merges s, for request when it is not nil, and adds the attempt to rep by its outcome; a failure is logged and added as failed.
func (r *Compactor) attempt(ctx context.Context, lease catalog.Lease, s catalog.Scope, request *tracepack.UUID, rep *MergeReport) (mergeOutcome, error) {
	out, err := r.merge(ctx, lease, s, request)
	if errors.Is(err, catalog.ErrLeaseLost) || ctx.Err() != nil {
		return out, err
	}
	switch {
	case err != nil:
		r.Log.Error("compact: merge failed", "capture", s.Capture, "hour", s.Hour, "error", err)
		rep.Failed = append(rep.Failed, s)
	case out == merged:
		rep.Merged = append(rep.Merged, s)
	case out == stale:
		r.Log.Info("compact: merge stale, retried next cycle", "capture", s.Capture, "hour", s.Hour)
		rep.Stale = append(rep.Stale, s)
	case out == conflicted:
		r.Log.Warn("compact: merge found conflicting records", "capture", s.Capture, "hour", s.Hour)
		rep.Conflicted = append(rep.Conflicted, s)
	default:
		// The snapshot found the scope no longer eligible.
	}

	return out, err
}

// add adds other's attempts to rep.
func (rep *MergeReport) add(other MergeReport) {
	rep.Merged = append(rep.Merged, other.Merged...)
	rep.Stale = append(rep.Stale, other.Stale...)
	rep.Conflicted = append(rep.Conflicted, other.Conflicted...)
	rep.Failed = append(rep.Failed, other.Failed...)
}

// mergeOutcome is what the merge of one scope did.
type mergeOutcome int

const (
	ineligible mergeOutcome = iota
	merged
	stale
	conflicted
)

// merge merges one scope, for request when it is not nil, whose commit then ends the request.
func (r *Compactor) merge(ctx context.Context, lease catalog.Lease, s catalog.Scope, request *tracepack.UUID) (mergeOutcome, error) {
	snap, err := r.Catalog.MergeSnapshot(ctx, s)
	if err != nil || !snap.Eligible {
		return ineligible, err
	}
	r.hook(hookMergeSnapshot)
	tmp, err := os.CreateTemp(r.WorkDir, workFilePattern)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	report, found, err := r.write(ctx, tmp, lease, &snap)
	if found != nil {
		return conflicted, r.mergeConflicts(ctx, lease, s, found)
	}
	if err != nil {
		return 0, err
	}
	archive, err := r.describeArchive(ctx, tmp, &snap, report.PackID)
	if err != nil {
		return 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if err := r.Store.Put(ctx, archive.Key, tmp, archive.Size); err != nil {
		return 0, err
	}
	r.hook(hookMergePut)
	m := &catalog.Merge{Scope: s, Revision: snap.Revision, Archive: archive, Request: request, DeleteDelay: r.DeleteDelay}
	for _, in := range snap.Inputs {
		m.Inputs = append(m.Inputs, in.ID)
	}
	out, err := r.Catalog.CommitMerge(ctx, lease, m)
	if err != nil {
		return 0, err
	}
	r.hook(hookMergeCommitted)
	switch out {
	case catalog.Merged, catalog.ArchiveRegistered:
		// ArchiveRegistered: this merge's own commit, whose result was lost.
		return merged, nil
	case catalog.MergeStale, catalog.ArchiveAdopted, catalog.ArchiveTombstoned:
		return stale, nil
	default:
		return 0, fmt.Errorf("compact: unknown merge outcome %d", out)
	}
}

// write merges snap's inputs into w; found holds the conflicts of a merge that failed on them, never nil then.
func (r *Compactor) write(ctx context.Context, w *os.File, lease catalog.Lease, snap *catalog.Snapshot) (tracepack.MergeReport, []tracepack.Conflict, error) {
	inputs := make([]tracepack.MergeInput, 0, len(snap.Inputs))
	var opened []store.Object
	defer func() {
		for _, obj := range opened {
			_ = obj.Close()
		}
	}()
	for _, in := range snap.Inputs {
		obj, err := r.Store.Open(ctx, in.Key)
		if err != nil {
			return tracepack.MergeReport{}, nil, fmt.Errorf("compact: open input %s: %w", in.Key, err)
		}
		opened = append(opened, obj)
		inputs = append(inputs, tracepack.MergeInput{ReaderAt: reading{obj}, Size: obj.Size()})
	}
	conflicts := []tracepack.Conflict{}
	report, err := tracepack.Merge(ctx, w, inputs, tracepack.MergeOptions{
		Writer: r.Writer,
		// The revision the commit gives the scope, and the lease's fence token (compactor.md §8.1 step 4).
		ScopeGeneration: uint64(snap.Revision + 1), //nolint:gosec // a revision is positive
		PublisherEpoch:  uint64(lease.Fence),       //nolint:gosec // a fence token is positive
		Now:             r.now,
		OnConflict:      func(c tracepack.Conflict) { conflicts = append(conflicts, c) },
		Sync:            w,
	})
	if errors.Is(err, tracepack.ErrMergeConflict) {
		return report, conflicts, nil
	}
	if err != nil {
		return report, nil, fmt.Errorf("compact: merge: %w", err)
	}

	return report, nil, nil
}

// describeArchive reads the archive in w back as the pack to register: its key from its identity, its exact ranges and its losses.
func (r *Compactor) describeArchive(ctx context.Context, w *os.File, snap *catalog.Snapshot, id tracepack.UUID) (*catalog.Pack, error) {
	info, err := w.Stat()
	if err != nil {
		return nil, err
	}
	key, err := tracepack.ArchiveKey(r.KeyPrefix, snap.ToolID, snap.Capture, snap.Hour, id)
	if err != nil {
		return nil, err
	}
	rd, err := tracepack.Open(ctx, w, info.Size(), tracepack.ReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("compact: open archive: %w", err)
	}
	p, reason := describe(rd, r.KeyPrefix, store.ObjectInfo{Key: key, Size: info.Size()})
	if reason != "" {
		return nil, fmt.Errorf("compact: archive %s: %s", key, reason)
	}
	if err := records(ctx, rd, p); err != nil {
		return nil, err
	}

	return p, nil
}

// now is the compactor's clock.
func (r *Compactor) now() time.Time {
	if r.Now == nil {
		return time.Now()
	}

	return r.Now()
}
