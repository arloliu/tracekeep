package compact

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
)

// ConflictReport is what a conflict pass did.
type ConflictReport struct {
	// Cleared and Confirmed count the suspects recorded each way.
	Cleared, Confirmed int
	// Changed counts the suspects whose row changed while they were compared; the next pass compares them again.
	Changed int
	// Failed counts the suspects whose packs could not be compared; nothing was recorded for them.
	Failed int
}

// Conflicts compares every suspect's two packs and records the result under lease (ingest.md §6.2):
// the overlapping records by their bytes and their redaction entries, cleared when every copy is equal, confirmed otherwise,
// each only while the row still names exactly the packs compared.
// A comparison that fails records nothing; a lost lease ends the pass with ErrLeaseLost.
func (r *Compactor) Conflicts(ctx context.Context, lease catalog.Lease) (ConflictReport, error) {
	var rep ConflictReport
	suspects, err := r.Catalog.Suspects(ctx)
	if err != nil {
		return rep, err
	}
	for _, s := range suspects {
		differ, err := r.compare(ctx, &s)
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
		if err != nil {
			r.Log.Warn("compact: compare a suspect", "capture", s.Capture, "seq_from", s.SeqFrom, "seq_to", s.SeqTo, "error", err)
			rep.Failed++

			continue
		}
		r.hook(hookCompared)
		out, err := r.Catalog.RecordConflict(ctx, lease, s, differ)
		if err != nil {
			return rep, err
		}
		switch {
		case out == catalog.ConflictChanged:
			rep.Changed++
		case differ:
			rep.Confirmed++
			r.Log.Warn("compact: conflict confirmed", "capture", s.Capture, "seq_from", s.SeqFrom, "seq_to", s.SeqTo)
		default:
			rep.Cleared++
		}
	}

	return rep, nil
}

// compare reports whether s's two packs hold different versions of a seq of its run: different record bytes,
// or, where both carry a redaction policy, different redaction entries.
func (r *Compactor) compare(ctx context.Context, s *catalog.Suspect) (bool, error) {
	var readers []*tracepack.Reader
	var opened []store.Object
	defer func() {
		for _, obj := range opened {
			_ = obj.Close()
		}
	}()
	for _, p := range []catalog.ConflictPack{s.Pack, s.Other} {
		obj, err := r.Store.Open(ctx, p.Key)
		if err != nil {
			return false, err
		}
		opened = append(opened, obj)
		rd, err := tracepack.Open(ctx, reading{obj}, obj.Size(), tracepack.ReaderOptions{})
		if err != nil {
			return false, fmt.Errorf("compact: open %s: %w", p.Key, err)
		}
		readers = append(readers, rd)
	}
	in := func(seq uint64) bool { return seq >= s.SeqFrom && seq <= s.SeqTo }
	res, err := tracepack.MergeIterate(ctx, readers, tracepack.Query{}, tracepack.MergeIterateOptions{Order: tracepack.OrderCapture},
		func(*tracepack.Item) error { return nil })
	if err != nil {
		return false, err
	}
	for _, d := range res.Incomplete {
		if d.Reason != tracepack.ReasonCoverage && d.Reason != tracepack.ReasonCrashGap {
			return false, fmt.Errorf("compact: comparison incomplete: %v", d.Reason)
		}
	}
	for _, c := range res.Conflicts {
		if c.CaptureID == s.Capture && in(c.Seq) {
			return true, nil
		}
	}
	a, b := readers[0].Header().Meta, readers[1].Header().Meta
	if a == nil || b == nil || a.RedactionPolicy == nil || b.RedactionPolicy == nil {
		return false, nil
	}

	return !reflect.DeepEqual(entries(a.Redaction, in), entries(b.Redaction, in)), nil
}

// entries are the redaction entries of the seqs in selects, in stored order.
func entries(all []tracepack.RedactionEntry, selects func(uint64) bool) []tracepack.RedactionEntry {
	var out []tracepack.RedactionEntry
	for _, e := range all {
		if selects(e.Seq) {
			out = append(out, e)
		}
	}

	return out
}

// mergeConflicts records the conflicts a merge of s failed on, the packs of each version named by the merge.
func (r *Compactor) mergeConflicts(ctx context.Context, lease catalog.Lease, s catalog.Scope, found []tracepack.Conflict) error {
	out := make([]catalog.MergeConflict, 0, len(found))
	for _, c := range found {
		out = append(out, catalog.MergeConflict{Seq: c.Seq, Packs: c.Versions})
	}
	if err := r.Catalog.RecordMergeConflicts(ctx, lease, s, out); err != nil {
		return err
	}
	if len(found) == 0 {
		return errors.New("compact: the merge failed on conflicts it did not report")
	}

	return nil
}
