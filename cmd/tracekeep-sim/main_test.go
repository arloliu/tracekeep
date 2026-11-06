package main

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"

	"github.com/arloliu/tracekeep/internal/sim"
)

// noBus is a bus no server answers on: the SDK keeps trying to connect while the device runs.
const noBus = "nats://127.0.0.1:1"

func simConfig(mode string) *config {
	return &config{
		mode: mode, servers: noBus, prefix: "tpk", bucket: "tpk-objects", source: "test-source", speed: 1, rate: 10,
		opts: sim.Options{EquipmentID: "SIM-EQP", Device: "eqp", SessionID: 1, T3: time.Second},
	}
}

// A rate the simulator cannot tick at is refused before the device starts.
func TestRunRefusesARateItCannotTick(t *testing.T) {
	t.Parallel()
	for _, rate := range []float64{1e300, 1e-300} {
		c := simConfig("loopback")
		c.rate, c.duration = rate, time.Millisecond
		if err := run(t.Context(), c); err == nil || !strings.Contains(err.Error(), "rate") {
			t.Errorf("run at rate %v = %v, want an error naming the rate", rate, err)
		}
	}
}

// A replay its own -duration ends is a success: the run did what it was asked.
func TestTimedReplayEndsInSuccess(t *testing.T) {
	t.Parallel()
	c := simConfig("replay")
	// The frames are 10 ms apart, which a millionth of the pace stretches to hours, long after the duration ends the replay.
	c.pack, c.speed, c.duration = writePack(t), 1e-6, 100*time.Millisecond
	if err := run(t.Context(), c); err != nil {
		t.Errorf("run = %v, want nil when the duration ends the replay", err)
	}
}

func TestTimedReplayKeepsAnUnrelatedFailure(t *testing.T) {
	t.Parallel()
	c := simConfig("replay")
	// The duration has ended before the replay starts, so a missing pack fails with the duration's cause set.
	c.pack, c.duration = filepath.Join(t.TempDir(), "missing.tp"), time.Nanosecond
	if err := run(t.Context(), c); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("run = %v, want the missing pack's error though the duration ended", err)
	}
}

// writePack records two synthetic equipment frames 10 ms apart as one capture and returns the path of its segment.
func writePack(t *testing.T) string {
	t.Helper()
	ctx := t.Context()
	root := t.TempDir()
	sink, err := tracepack.NewDirSink(root, "", tracepack.DirSinkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	origin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w, err := tracepack.NewSegmentWriter(ctx, tracepack.SegmentWriterOptions{
		Capture: tracepack.CaptureDescriptor{
			EquipmentID: "SYN-EQP", Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
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
	for i := range 2 {
		m, err := hsms.NewDataMessage(6, 11, false, 1, [4]byte{0, 0, 0, byte(i + 1)}, secs2.L())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.AppendFrame(ctx, origin.Add(time.Duration(i)*10*time.Millisecond), tracepack.DirEquipmentToHost, 1, m.ToBytes(), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
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
