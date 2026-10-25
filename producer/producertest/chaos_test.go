package producertest_test

import (
	"context"
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
