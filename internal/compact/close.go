package compact

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// closeFilePattern names a close's working file under the working directory.
const closeFilePattern = "close-*.tpk"

// CloseReport is what a close pass did.
type CloseReport struct {
	// Intended counts the captures closed by an intent, by trigger.
	Intended map[string]int
	// Published and Abandoned count the intents that ended each way;
	// Failed counts the candidates whose intent failed and the intents whose publication failed, each left for the next cycle.
	Published, Abandoned, Failed int
}

// Closes closes the captures left open under lease (compactor.md §8.2): every candidate of the scan becomes an intent,
// whose transaction re-reads its eligibility, then every pending intent, this cycle's and earlier ones', is written by
// tracepack.WriteClose from exactly its stored options, put under its staging/ key and published.
// A failure of one capture is logged and leaves its intent pending; a lost lease ends the pass with ErrLeaseLost.
func (r *Compactor) Closes(ctx context.Context, lease catalog.Lease) (CloseReport, error) {
	rep := CloseReport{Intended: map[string]int{}}
	now := r.now()
	cands, err := r.Catalog.CloseCandidates(ctx, now.UnixNano(), r.Inactivity, r.QuietPeriod)
	if err != nil {
		return rep, err
	}
	for _, cand := range cands {
		atNs := cand.TriggeredAt
		if cand.Trigger != catalog.CloseSuccessor {
			atNs = now.UnixNano()
		}
		out, err := r.intend(ctx, lease, cand.Capture, cand.Trigger, atNs, nil)
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
		if errors.Is(err, catalog.ErrLeaseLost) {
			return rep, err
		}
		if err != nil {
			r.Log.Error("compact: close intent", "capture", cand.Capture, "error", err)
			rep.Failed++

			continue
		}
		if out == catalog.Intended {
			rep.Intended[cand.Trigger]++
			r.Log.Info("compact: capture closed", "capture", cand.Capture, "trigger", cand.Trigger)
		}
	}
	pending, err := r.Catalog.PendingCloses(ctx)
	if err != nil {
		return rep, err
	}
	for i := range pending {
		state, err := r.publishClose(ctx, lease, &pending[i])
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
		if errors.Is(err, catalog.ErrLeaseLost) {
			return rep, err
		}
		switch {
		case err != nil:
			r.Log.Error("compact: publish a close", "capture", pending[i].Capture, "error", err)
			rep.Failed++
		case state == "published":
			rep.Published++
		case state == "abandoned":
			rep.Abandoned++
			r.Log.Warn("compact: close abandoned", "capture", pending[i].Capture)
		default:
			// Ended by another holder's cycle before this one: nothing to count.
		}
	}

	return rep, nil
}

// intend builds the close of capture by trigger at at, every byte-affecting input fixed, and records its intent:
// the capture's descriptor with the values a segment of the capture carries, in a period of the hour of at.
func (r *Compactor) intend(ctx context.Context, lease catalog.Lease, capture tracepack.UUID, trigger string, at int64,
	request *tracepack.UUID,
) (catalog.CloseOutcome, error) {
	cp, err := r.Catalog.Capture(ctx, capture)
	if err != nil {
		return 0, err
	}
	meta, err := tracepack.DecodeDescriptor(cp.Descriptor)
	if err != nil {
		return 0, fmt.Errorf("compact: descriptor of %s: %w", capture, err)
	}
	if meta.ClockStepToleranceNs == nil || *meta.ClockStepToleranceNs > math.MaxInt64 {
		return 0, fmt.Errorf("compact: the descriptor of %s has no clock step tolerance", capture)
	}
	interval := uint64(r.FlushInterval.Nanoseconds()) //nolint:gosec // a positive duration
	h := floorHour(at)
	meta.Writer, meta.ScopeGeneration, meta.FlushIntervalNs = r.Writer, new(uint64), &interval
	meta.PeriodStart, meta.PeriodEnd = h*nsPerHour, (h+1)*nsPerHour
	b, err := meta.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("compact: the close's metadata: %w", err)
	}
	var id tracepack.UUID
	if _, err := rand.Read(id[:]); err != nil {
		return 0, err
	}
	id[6], id[8] = id[6]&0x0f|0x40, id[8]&0x3f|0x80 // a version 4 UUID
	in := &catalog.Intent{
		Capture: capture, Trigger: trigger, TriggeredAt: at, Now: r.now().UnixNano(), Inactivity: r.Inactivity, Quiet: r.QuietPeriod,
		PackID: id, WriterTime: r.now().UnixNano(), Meta: b, ToleranceNS: int64(*meta.ClockStepToleranceNs), Request: request,
	}

	return r.Catalog.CloseIntent(ctx, lease, in)
}

// publishClose writes the close of c from its intent, puts it under its key and publishes it; an abandoned close's object is deleted.
func (r *Compactor) publishClose(ctx context.Context, lease catalog.Lease, c *catalog.Close) (string, error) {
	tmp, err := os.CreateTemp(r.WorkDir, closeFilePattern)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	p, err := r.writeClose(ctx, tmp, c)
	if err != nil {
		return "", err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	if err := r.Store.Put(ctx, p.Key, tmp, p.Size); err != nil {
		// A put whose result was lost: the object stands only if it holds exactly the bytes written (compactor.md §8.2 step 2).
		if same, serr := r.stored(ctx, p.Key, tmp, p.Size); serr != nil || !same {
			return "", err
		}
	}
	r.hook(hookClosePut)
	state, err := r.Catalog.PublishClose(ctx, lease, p, r.HorizonHour())
	if err != nil {
		return "", err
	}
	if state == "abandoned" {
		// The key is tombstoned, so the object goes; the reconciler deletes it again if this fails.
		if err := r.Catalog.VerifyLease(ctx, lease); err != nil {
			return state, err
		}
		if err := r.Store.Delete(ctx, p.Key); err != nil {
			r.Log.Warn("compact: delete an abandoned close", "key", p.Key, "error", err)
		}
	}

	return state, nil
}

// stored reports whether the object under key holds exactly the size bytes of w, compared by their SHA-256 digests.
func (r *Compactor) stored(ctx context.Context, key string, w *os.File, size int64) (bool, error) {
	info, err := r.Store.Stat(ctx, key)
	if err != nil || info.Size != size {
		return false, err
	}
	obj, err := r.Store.Open(ctx, key)
	if err != nil {
		return false, err
	}
	defer obj.Close()
	want, got := sha256.New(), sha256.New()
	if _, err := io.Copy(want, io.NewSectionReader(w, 0, size)); err != nil {
		return false, err
	}
	if _, err := io.Copy(got, io.NewSectionReader(obj, 0, size)); err != nil {
		return false, err
	}

	return bytes.Equal(want.Sum(nil), got.Sum(nil)), nil
}

// writeClose writes the close of c to w from exactly its intent's options and returns the pack to register.
func (r *Compactor) writeClose(ctx context.Context, w *os.File, c *catalog.Close) (*catalog.Pack, error) {
	meta, err := tracepack.UnmarshalPackMeta(c.Meta)
	if err != nil {
		return nil, fmt.Errorf("compact: the intent's metadata: %w", err)
	}
	writer := time.Unix(0, c.WriterTime)
	_, err = tracepack.WriteClose(w, tracepack.CloseOptions{
		Meta: meta, CaptureID: c.Capture, Trigger: time.Unix(0, c.TriggeredAt), HighestSeqTS: c.HighestSeqTS,
		PackID: c.PackID, Sync: w, Now: func() time.Time { return writer },
	})
	if err != nil {
		return nil, fmt.Errorf("compact: write the close of %s: %w", c.Capture, err)
	}
	info, err := w.Stat()
	if err != nil {
		return nil, err
	}
	rd, err := tracepack.Open(ctx, w, info.Size(), tracepack.ReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("compact: open the close: %w", err)
	}
	key, err := tracepack.SegmentKey(r.KeyPrefix, c.ToolID, c.Capture, math.MaxInt64, c.PackID)
	if err != nil {
		return nil, err
	}

	return &catalog.Pack{
		ID: c.PackID, Capture: c.Capture, ToolID: c.ToolID, Hour: floorHour(c.TriggeredAt), Role: catalog.RoleClose, Key: key,
		Size: info.Size(), SeqStart: math.MaxInt64, FormatMinor: int(rd.Header().FormatMinor),
	}, nil
}
