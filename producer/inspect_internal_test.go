package producer

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// observeAsync runs one observation on its own goroutine; done closes when the callback returns.
func observeAsync(p *Producer, payload []byte) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wall, mono := core.Now()
		p.rec.Observe(&core.Observation{Wall: wall, Mono: mono, Socket: 1, Kind: tracepack.KindData, Payload: payload})
	}()

	return done
}

// returns fails the test unless done closes within a generous bound.
func returns(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// The publisher's read of a capture's reservation state holds the capture's lock, the one wait T-52 admits:
// a callback that goes for the lock during the read waits for it, as for another callback, and proceeds on its release.
func TestCallbackWaitsOnlyForThePublishersInspection(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	var (
		mu       sync.Mutex
		order    []string
		armed    atomic.Bool
		watching atomic.Bool
	)
	note := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}
	inspecting, release, loaded := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	p.rec.SetHook(core.HookInspect, func() {
		if armed.CompareAndSwap(true, false) {
			close(inspecting)
			<-release
			note("inspection released")
		}
	})
	p.rec.SetHook(core.HookLoaded, func() {
		if watching.Load() {
			loaded <- struct{}{}
		}
	})
	p.rec.SetHook(core.HookLocked, func() {
		if watching.Load() {
			note("callback locked")
		}
	})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	bus.waitFor(t, 1)

	armed.Store(true)
	<-inspecting // the next pass holds its read
	watching.Store(true)
	done := observeAsync(p, []byte{1})
	<-loaded // the callback goes for the lock the read holds
	select {
	case <-done:
		t.Fatal("the callback returned while the publisher's read held the capture's lock")
	default:
	}
	close(release)
	returns(t, done, "the callback")
	mu.Lock()
	got := slices.Clone(order)
	mu.Unlock()
	if want := []string{"inspection released", "callback locked"}; !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
	bus.waitFor(t, 2) // the callback's record is published
}

// While the publisher waits outside the lock, on a publish or a claim-object operation, no callback waits on it.
func TestPublishersExternalWaitsHoldNoCallback(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		publish bool
		want    string
	}{
		{name: "publish", publish: true, want: "publish"},
		{name: "claim object", want: "info"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bus := newWaitBus(tt.publish)
			p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Tunables: Tunables{PublishTimeout: time.Minute}}, WithBus(bus))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			if err := p.Begin(t.Context(), testDescriptor()); err != nil {
				t.Fatal(err)
			}
			if !tt.publish {
				bus.waitFor(t, 1) // the start
				returns(t, observeAsync(p, make([]byte, bus.MaxPayload())), "the large record's callback")
			}
			if op := <-bus.waiting; op != tt.want {
				t.Fatalf("the publisher waits in %s, want %s", op, tt.want)
			}

			for range 3 {
				returns(t, observeAsync(p, []byte{1}), "a callback while the publisher waits")
			}
		})
	}
}
