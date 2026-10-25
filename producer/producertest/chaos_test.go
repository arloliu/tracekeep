package producertest_test

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/core"
	"github.com/arloliu/tracekeep/producer/internal/link"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// recDevice is a device whose traffic goes straight to the producer's recorder, as its observers would feed it.
type recDevice struct {
	p      *producer.Producer
	rec    *core.Recorder
	reload atomic.Uint64
}

func newRecDevice(t *testing.T, bus *producertest.Bus, tun producer.Tunables) producertest.Device {
	t.Helper()
	p, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects", Tunables: tun}, producer.WithBus(bus))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	return &recDevice{p: p, rec: link.Recorder(p)}
}

func (d *recDevice) descriptor() producer.CaptureDescriptor {
	id := d.reload.Load()
	return producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp", DeviceID: &id}
}

func (d *recDevice) Start(ctx context.Context) error { return d.p.Begin(ctx, d.descriptor()) }

func (d *recDevice) HotReload(ctx context.Context) error {
	d.reload.Add(1)
	return d.p.Rotate(ctx, d.descriptor())
}

func (d *recDevice) Stop(ctx context.Context) { d.p.End(ctx) }

func (d *recDevice) Traffic(_ context.Context, n int) error {
	f := []byte{0, 0, 0, 12, 0, 1, 0x01, 1, 0, 0, 0, 0, 0, 7, 0x01, 0x00}
	for range n {
		wall, mono := core.Now()
		d.rec.Observe(&core.Observation{
			Wall: wall, Mono: mono, Socket: 1,
			Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact,
			Payload: f,
		})
	}

	return nil
}

func TestRunChaos(t *testing.T) {
	t.Parallel()
	producertest.RunChaos(t, newRecDevice)
}

func TestCheckCapturesFindsAMissingStart(t *testing.T) {
	t.Parallel()
	bus := producertest.NewBus()
	d := newRecDevice(t, bus, producer.Tunables{})
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := d.Traffic(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	msgs, err := bus.WaitMessages(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := producertest.CheckCaptures(msgs); err != nil {
		t.Fatalf("CheckCaptures of a start and a record: %v", err)
	}
	if err := producertest.CheckCaptures(msgs[1:]); err == nil {
		t.Fatalf("CheckCaptures passed a record without its start")
	}
}

// failures records what a scenario reports instead of failing the test.
type failures struct {
	testing.TB
	mu     sync.Mutex
	errors []string
}

func (f *failures) Helper() {}

func (f *failures) Error(args ...any) { f.add(fmt.Sprint(args...)) }

func (f *failures) Errorf(format string, args ...any) { f.add(fmt.Sprintf(format, args...)) }

func (f *failures) add(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors, s)
}

// brokenDevice wires the SDK wrongly: hotReload and traffic say whether those verbs reach it.
type brokenDevice struct {
	producertest.Device
	hotReload, traffic bool
}

func (d *brokenDevice) HotReload(ctx context.Context) error {
	if d.hotReload {
		return d.Device.HotReload(ctx)
	}
	return nil
}

func (d *brokenDevice) Traffic(ctx context.Context, n int) error {
	if d.traffic {
		return d.Device.Traffic(ctx, n)
	}
	return nil
}

// The harness fails a device whose HotReload or traffic never reaches the SDK.
func TestRunChaosFailsBrokenWiring(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name               string
		hotReload, traffic bool
		failing            []string
	}{
		{"no hot reload", false, true, []string{"bus down and up", "hot reload under traffic", "injected faults"}},
		{"no traffic", true, false, []string{"bus down and up", "hot reload under traffic", "stop under a stalled publisher", "injected faults"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			got := map[string]*failures{}
			producertest.RunChaosReporting(t, func(t *testing.T, bus *producertest.Bus, tun producer.Tunables) producertest.Device {
				return &brokenDevice{Device: newRecDevice(t, bus, tun), hotReload: tt.hotReload, traffic: tt.traffic}
			}, func(st *testing.T) testing.TB {
				f := &failures{TB: st}
				mu.Lock()
				got[path.Base(st.Name())] = f
				mu.Unlock()
				return f
			})
			for _, sc := range tt.failing {
				if f := got[strings.ReplaceAll(sc, " ", "_")]; f == nil || len(f.errors) == 0 {
					t.Errorf("scenario %q passed", sc)
				}
			}
		})
	}
}
