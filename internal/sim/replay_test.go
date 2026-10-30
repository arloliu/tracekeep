package sim

import (
	"context"
	"encoding/binary"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// frameOf builds a data frame of the replayed session.
func frameOf(t *testing.T, stream, function uint8, w bool, sb uint32, item secs2.Item) []byte {
	t.Helper()
	var sys [4]byte
	binary.BigEndian.PutUint32(sys[:], sb)
	m, err := hsms.NewDataMessage(stream, function, w, 1, sys, item)
	if err != nil {
		t.Fatal(err)
	}

	return m.ToBytes()
}

// writePack records frames as one synthetic capture and returns the path of its segment.
func writePack(t *testing.T, frames []tracepack.Dir, payloads [][]byte) string {
	t.Helper()
	ctx := t.Context()
	root := t.TempDir()
	sink, err := tracepack.NewDirSink(root, "")
	if err != nil {
		t.Fatal(err)
	}
	origin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w, err := tracepack.NewSegmentWriter(ctx, tracepack.SegmentWriterOptions{
		Capture: tracepack.CaptureDescriptor{
			ToolID: "SYN-TOOL", Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
			Vantage: tracepack.VantageHost, Recorder: "synthetic/1", RecorderInstanceID: tracepack.UUID{1},
			TimeSource: tracepack.TimeSourceCaptureClock, CaptureOrigin: origin,
			LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		},
		Sink: sink,
		Now:  func() time.Time { return origin.Add(time.Minute) },
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range payloads {
		if err := w.AppendFrame(ctx, origin.Add(time.Duration(i+1)*10*time.Millisecond), frames[i], 1, f); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var paths []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == ".partial" {
			return filepath.SkipDir
		}
		if !d.IsDir() {
			paths = append(paths, path)
		}

		return nil
	})
	if len(paths) != 1 {
		t.Fatalf("%d segments, want 1", len(paths))
	}

	return paths[0]
}

// Every data frame of the pack crosses the pair from the side that sent it and is recorded as it was;
// the request the pack never answered times out on the replay too.
func TestReplaySendsThePackThroughThePair(t *testing.T) {
	t.Parallel()
	h2e, e2h := tracepack.DirHostToEquipment, tracepack.DirEquipmentToHost
	dirs := []tracepack.Dir{h2e, e2h, e2h, h2e, h2e, h2e}
	frames := [][]byte{
		frameOf(t, 1, 3, true, 0x100, secs2.L(secs2.U4(1))),
		frameOf(t, 1, 4, false, 0x100, secs2.L(secs2.U4(2))),
		frameOf(t, 6, 11, true, 0x200, secs2.L(secs2.U4(3))),
		frameOf(t, 6, 12, false, 0x200, secs2.B(0)),
		frameOf(t, 1, 3, true, 0x101, secs2.L()), // never answered
		{0, 0, 0, 2, 0xff, 0xff},                 // malformed, as a wire-exact record may be
	}
	pack := writePack(t, dirs, frames)
	frames = frames[:len(frames)-1]

	bus := producertest.NewBus()
	opts := testOptions()
	opts.Replay = true
	// A replayed request is never marked, so the second S1F3, which a marked poll would lose, keeps its text.
	opts.Thinning = &producer.SamplingRule{KeepEveryN: 1000, KeepInterval: time.Hour}
	l := newTestLoopbackWith(t, producer.Config{}, opts, producer.WithBus(bus))
	if err := l.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	n, skipped, err := ReplayPack(t.Context(), l, pack, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(frames) || skipped != 1 {
		t.Fatalf("replayed %d frames and skipped %d, want %d and 1", n, skipped, len(frames))
	}

	// Wait for the frames and the T3 of the unanswered request.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var got [][]byte
	t3 := 0
	for i := 1; len(got) < len(frames) || t3 == 0; i++ {
		msgs, err := bus.WaitMessages(ctx, i)
		if err != nil {
			t.Fatalf("%d frames and %d T3 on the bus: %v", len(got), t3, err)
		}
		r := decode(t, &msgs[i-1]).Record
		switch {
		case r.Kind == tracepack.KindData:
			got = append(got, r.Payload)
		case r.Kind == tracepack.KindTransportEvent && isT3(t, r.Payload):
			t3++
		default:
		}
	}
	slices.SortFunc(got, slices.Compare)
	want := slices.Clone(frames)
	slices.SortFunc(want, slices.Compare)
	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("recorded frames differ from the pack's")
	}
	l.Stop(t.Context())
}

func isT3(t *testing.T, payload []byte) bool {
	t.Helper()
	ev, err := tracepack.UnmarshalTransportEvent(payload)
	if err != nil {
		t.Fatal(err)
	}

	return ev.Event == tracepack.EventTimerExpiry && ev.Timer != nil && *ev.Timer == tracepack.TimerT3
}

func TestReplayKeepsThePacksPace(t *testing.T) {
	t.Parallel()
	dirs := []tracepack.Dir{tracepack.DirEquipmentToHost, tracepack.DirEquipmentToHost, tracepack.DirEquipmentToHost}
	frames := [][]byte{
		frameOf(t, 6, 11, false, 1, secs2.L()), frameOf(t, 6, 11, false, 2, secs2.L()), frameOf(t, 6, 11, false, 3, secs2.L()),
	}
	pack := writePack(t, dirs, frames) // 10 ms apart
	opts := testOptions()
	opts.Replay = true
	l := newTestLoopbackWith(t, producer.Config{}, opts, producer.WithBus(producertest.NewBus()))
	if err := l.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, _, err := ReplayPack(t.Context(), l, pack, 0.5); err != nil {
		t.Fatal(err)
	}
	// Half speed stretches the 20 ms between the first and the last frame to 40 ms.
	if took := time.Since(start); took < 40*time.Millisecond {
		t.Errorf("replay at half speed took %v, want at least 40ms", took)
	}
	l.Stop(t.Context())
}

func TestReplayRefusesAMissingPack(t *testing.T) {
	t.Parallel()
	l := newTestLoopbackWith(t, producer.Config{}, testOptions(), producer.WithBus(producertest.NewBus()))
	if _, _, err := ReplayPack(t.Context(), l, filepath.Join(t.TempDir(), "none"), 0); !os.IsNotExist(err) {
		t.Fatalf("replayPack = %v, want not-exist", err)
	}
}
