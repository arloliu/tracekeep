package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// replayPack sends every data frame of the pack at path through l, in seq order,
// at the pack's pace multiplied by speed, or as fast as the pair takes them when speed is 0.
// The pack must be synthetic or masked (docs/ROADMAP.md, phase 0); a thinned record has no frame to send and is skipped.
// It returns how many frames it sent.
func replayPack(ctx context.Context, l *Loopback, path string, speed float64) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	r, err := tracepack.Open(ctx, f, st.Size(), tracepack.ReaderOptions{})
	if err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	start := time.Now()
	var first int64
	n := 0
	_, err = r.Iterate(ctx, tracepack.Query{Payloads: true}, func(it *tracepack.Item) error {
		rec := &it.Record
		if rec.Kind != tracepack.KindData || r.Unobserved(rec) {
			return nil
		}
		if n == 0 {
			first = rec.TSUTCNs
		}
		if speed > 0 {
			due := start.Add(time.Duration(float64(rec.TSUTCNs-first) / speed))
			if err := sleepUntil(ctx, due); err != nil {
				return err
			}
		}
		if err := l.Replay(ctx, rec.Dir, rec.Payload); err != nil {
			return fmt.Errorf("seq %d: %w", rec.Seq, err)
		}
		n++

		return nil
	})

	return n, err
}

func sleepUntil(ctx context.Context, t time.Time) error {
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
