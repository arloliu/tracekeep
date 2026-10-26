package sim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// ReplayPack sends every data frame of the pack at path through l, in seq order,
// at the pack's pace multiplied by speed, or as fast as the pair takes them when speed is 0.
// The pack must be synthetic or masked (docs/ROADMAP.md, phase 0); a thinned record has no frame to send and is skipped.
// A frame go-secs cannot decode, which a wire-exact record may hold, is skipped.
// It returns how many frames it sent and how many it skipped.
func ReplayPack(ctx context.Context, l *Loopback, path string, speed float64) (sent, skipped int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	r, err := tracepack.Open(ctx, f, st.Size(), tracepack.ReaderOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("%s: %w", path, err)
	}
	start := time.Now()
	var first int64
	_, err = r.Iterate(ctx, tracepack.Query{Payloads: true}, func(it *tracepack.Item) error {
		rec := &it.Record
		if rec.Kind != tracepack.KindData || r.Unobserved(rec) {
			return nil
		}
		if sent+skipped == 0 {
			first = rec.TSUTCNs
		}
		if speed > 0 {
			due := start.Add(time.Duration(float64(rec.TSUTCNs-first) / speed))
			if err := SleepUntil(ctx, due); err != nil {
				return err
			}
		}
		err := l.Replay(ctx, rec.Dir, rec.Payload)
		switch {
		case errors.Is(err, errMalformed):
			skipped++
		case err != nil:
			return fmt.Errorf("seq %d: %w", rec.Seq, err)
		default:
			sent++
		}

		return nil
	})

	return sent, skipped, err
}

// SleepUntil waits until t or the end of ctx, whichever comes first, returning ctx's error in the second case.
func SleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Forward relays requests at rate per second until ctx ends.
func Forward(ctx context.Context, l *Loopback, rate float64, log *slog.Logger) {
	tick := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := l.Forward(ctx); err != nil && ctx.Err() == nil {
				log.Warn("sim: forward failed", "error", err)
			}
		}
	}
}
