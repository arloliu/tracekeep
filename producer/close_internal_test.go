package producer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// closeBound is how long Close may take once the wait it interrupts began; every wait below would last longer uncancelled.
const closeBound = defaultLifecycleTimeout - time.Second

// waitBus is a recBus whose synchronous publishes, when holdPublish is set, and object operations wait for their context's end;
// waiting receives the name of each operation that begins waiting.
type waitBus struct {
	*recBus
	holdPublish bool
	waiting     chan string
}

type waitObjects struct{ b *waitBus }

func newWaitBus(holdPublish bool) *waitBus {
	return &waitBus{recBus: newRecBus(), holdPublish: holdPublish, waiting: make(chan string, 16)}
}

func (b *waitBus) wait(ctx context.Context, op string) error {
	b.waiting <- op
	<-ctx.Done()

	return ctx.Err()
}

func (b *waitBus) Publish(ctx context.Context, m *Msg) (Ack, error) {
	if b.holdPublish {
		return Ack{}, b.wait(ctx, "publish")
	}

	return b.recBus.Publish(ctx, m)
}

func (b *waitBus) Objects(context.Context, string) (Objects, error) { return waitObjects{b}, nil }

func (o waitObjects) GetInfo(ctx context.Context, _ string) (ObjectInfo, error) {
	return ObjectInfo{}, o.b.wait(ctx, "info")
}

func (o waitObjects) Put(ctx context.Context, _ ObjectMeta, _ []byte) (ObjectInfo, error) {
	return ObjectInfo{}, o.b.wait(ctx, "put")
}

func (o waitObjects) Get(ctx context.Context, _ string) ([]byte, error) {
	return nil, o.b.wait(ctx, "get")
}

func (o waitObjects) Delete(ctx context.Context, _ string) error { return o.b.wait(ctx, "delete") }

// closeWithin closes p and fails the test if Close took closeBound or longer.
func closeWithin(t *testing.T, p *Producer) {
	t.Helper()
	start := time.Now()
	_ = p.Close()
	if took := time.Since(start); took >= closeBound {
		t.Errorf("Close took %v: it waited out the wait it should cancel", took)
	}
}

// Close cancels the publisher's synchronous publish instead of waiting out τ.
func TestCloseCancelsAPublish(t *testing.T) {
	t.Parallel()
	bus := newWaitBus(true)
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Tunables: Tunables{PublishTimeout: time.Minute}}, WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	if op := <-bus.waiting; op != "publish" {
		t.Fatalf("the publisher waits in %s, want the start's publish", op)
	}
	closeWithin(t, p)
}

// Close cancels the publisher's claim-object operation instead of waiting out τ.
func TestCloseCancelsAClaimPut(t *testing.T) {
	t.Parallel()
	bus := newWaitBus(false)
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Tunables: Tunables{PublishTimeout: time.Minute}}, WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	bus.waitFor(t, 1) // the start
	wall, mono := core.Now()
	p.rec.Observe(&core.Observation{Wall: wall, Mono: mono, Socket: 1, Kind: tracepack.KindData, Payload: make([]byte, bus.MaxPayload())})
	if op := <-bus.waiting; op != "info" {
		t.Fatalf("the publisher waits in %s, want the claim object's inspection", op)
	}
	closeWithin(t, p)
}

// Close cancels a fault rotation waiting for the lifecycle lock, and it installs nothing.
func TestCloseCancelsAFaultRotationsLockWait(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	if !p.lockLife(time.Now().Add(10*time.Second), nil) {
		t.Fatal("lifecycle lock")
	}
	c.Fault()
	p.rec.SignalFault(c)
	for len(p.rec.Faults()) > 0 { // barrier: the lifecycle goroutine took the signal and goes for the lock this test holds
		time.Sleep(time.Millisecond)
	}
	closeWithin(t, p)
	p.unlockLife()
	if cur := p.rec.Current(); cur != c {
		t.Errorf("current capture %v after Close, want the faulted one left in place", cur)
	}
}

// Close cancels a seq-limit rotation looping on a capture lock a callback holds;
// the rotation abandons before its commit point, so it installs nothing.
func TestCloseCancelsARotationsCaptureLockWait(t *testing.T) {
	t.Parallel()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, func(o *options) { o.noPublisher, o.rotateAt = true, 1 })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	var entered atomic.Int32
	held, release, trying := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	p.rec.SetHook(core.HookLocked, func() {
		if entered.Add(1) == 2 {
			close(held)
			<-release
		}
	})
	p.rec.SetHook(core.HookTryLockFailed, func() { trying <- struct{}{} })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()

	// The lifecycle lock keeps the rotation the first observation asks for waiting until the second holds the capture's lock.
	if !p.lockLife(time.Now().Add(10*time.Second), nil) {
		t.Fatal("lifecycle lock")
	}
	observe := func() {
		wall, mono := core.Now()
		p.rec.Observe(&core.Observation{Wall: wall, Mono: mono, Socket: 1, Kind: tracepack.KindData, Payload: []byte{1}})
	}
	observe() // seq 1 reaches rotateAt
	done := make(chan struct{})
	go func() {
		defer close(done)
		observe()
	}()
	<-held
	p.unlockLife()
	<-trying // the rotation loops on the capture lock

	closeWithin(t, p)
	close(release)
	<-done
	if cur := p.rec.Current(); cur != c {
		t.Errorf("current capture %v after Close, want the rotation to have installed nothing", cur)
	}
}
