package keys

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// The adapter redact.Mask is given answers as the module does, for the use's context.
func TestForUseServesRedactKeys(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x61, 32)
	k := newKeys(t, maskingKeys(oneVersion(m)), cat)
	keys := k.ForUse(t.Context())
	if got, err := keys.DigestKey("d-1"); err != nil || !bytes.Equal(got, m) {
		t.Errorf("DigestKey = %x, %v", got, err)
	}
	want, err := k.Encryption(t.Context(), testCapture, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := keys.EncryptionKey(testCapture, "m-1"); err != nil || !bytes.Equal(got, want) {
		t.Errorf("EncryptionKey = %x, %v; want %x", got, err, want)
	}
	if _, err := keys.DigestKey("m-1"); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("DigestKey of a master key id = %v, want ErrUnknownKey", err)
	}
}

// A warm cache serves bound material through a provider outage; a cold one fails closed as unavailable.
func TestOutageWarmAndCold(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x62, 32)
	p := oneVersion(m)
	warm := newKeys(t, maskingKeys(p), cat)
	if _, err := warm.Digest(t.Context(), "d-1"); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.fail = 1 << 20
	p.mu.Unlock()
	if got, err := warm.ForUse(t.Context()).DigestKey("d-1"); err != nil || !bytes.Equal(got, m) {
		t.Errorf("a warm cache during the outage = %x, %v; want the bound material", got, err)
	}
	if n := p.callCount(); n != 1 {
		t.Errorf("provider calls = %d, want 1: the warm cache asked nothing", n)
	}
	cold := newKeys(t, maskingKeys(p), cat)
	if got, err := cold.ForUse(t.Context()).DigestKey("d-1"); !errors.Is(err, errProviderDown) || got != nil {
		t.Errorf("a cold cache during the outage = %x, %v; want no key and the provider's failure", got, err)
	}
}

// A use whose context is cancelled gets no material.
func TestCancelledUseGetsNothing(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	k := newKeys(t, maskingKeys(oneVersion(material(0x63, 32))), cat)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := k.ForUse(ctx).EncryptionKey(testCapture, "m-1"); !errors.Is(err, context.Canceled) || got != nil {
		t.Errorf("EncryptionKey under a cancelled context = %x, %v", got, err)
	}
}

// Concurrent uses of one key id share one resolution; one use cancelled while it waits stops waiting with nothing,
// and the resolution goes on for the other, which gets the material.
func TestConcurrentUsesShareOneResolution(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x64, 32)
	p := oneVersion(m)
	p.gate = newGate()
	k := newKeys(t, maskingKeys(p), cat)
	joined := joining(k)
	ctxA, cancelA := context.WithCancel(t.Context())
	type result struct {
		key []byte
		err error
	}
	resA, resB := make(chan result, 1), make(chan result, 1)
	go func() { key, err := k.ForUse(ctxA).DigestKey("d-1"); resA <- result{key, err} }()
	<-p.gate.arrived
	// B asks while A's resolution is held at the provider.
	go func() { key, err := k.ForUse(t.Context()).DigestKey("d-1"); resB <- result{key, err} }()
	<-joined
	cancelA()
	if a := <-resA; !errors.Is(a.err, context.Canceled) || a.key != nil {
		t.Fatalf("the cancelled use = %x, %v; want nothing", a.key, a.err)
	}
	close(p.gate.release)
	if b := <-resB; b.err != nil || !bytes.Equal(b.key, m) {
		t.Errorf("the other use = %x, %v; want the material", b.key, b.err)
	}
	if n := p.callCount(); n != 1 {
		t.Errorf("provider calls = %d, want one shared resolution", n)
	}
	if got, err := k.Digest(t.Context(), "d-1"); err != nil || !bytes.Equal(got, m) {
		t.Errorf("a later use = %x, %v; want the cached material", got, err)
	}
}

// A shared resolution's failure reaches every waiter and caches nothing.
func TestSharedFailureCachesNothing(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x65, 32)
	p := oneVersion(m)
	p.fail, p.gate = 1, newGate()
	k := newKeys(t, maskingKeys(p), cat)
	joined := joining(k)
	errs := make(chan error, 2)
	go func() { _, err := k.Digest(t.Context(), "d-1"); errs <- err }()
	<-p.gate.arrived
	go func() { _, err := k.Digest(t.Context(), "d-1"); errs <- err }()
	<-joined
	close(p.gate.release)
	for range 2 {
		if err := <-errs; !errors.Is(err, errProviderDown) {
			t.Errorf("a waiter = %v, want the shared failure", err)
		}
	}
	p.mu.Lock()
	p.gate = nil
	p.mu.Unlock()
	if got, err := k.Digest(t.Context(), "d-1"); err != nil || !bytes.Equal(got, m) {
		t.Errorf("the next use = %x, %v; want a fresh resolution", got, err)
	}
	if n := p.callCount(); n != 2 {
		t.Errorf("provider calls = %d, want 2", n)
	}
}

// joining makes k signal on the returned channel each time a use joins a resolution already in flight.
func joining(k *Keys) <-chan struct{} {
	ch := make(chan struct{}, 4)
	k.joined = func() { ch <- struct{}{} }

	return ch
}

// A cancelled use gets nothing from a warm cache either, through both adapter methods.
func TestCancelledUseGetsNothingWarm(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	k := newKeys(t, maskingKeys(oneVersion(material(0x66, 32))), cat)
	if _, err := k.Digest(t.Context(), "d-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Encryption(t.Context(), testCapture, "m-1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	use := k.ForUse(ctx)
	if got, err := use.DigestKey("d-1"); !errors.Is(err, context.Canceled) || got != nil {
		t.Errorf("DigestKey from a warm cache, cancelled = %x, %v", got, err)
	}
	if got, err := use.EncryptionKey(testCapture, "m-1"); !errors.Is(err, context.Canceled) || got != nil {
		t.Errorf("EncryptionKey from a warm cache, cancelled = %x, %v", got, err)
	}
}

// A waiter cancelled before its shared resolution completes gets nothing, whichever of the two its select sees first;
// the leader's use, not cancelled, gets the key.
func TestCancelledWaiterAsTheResolutionCompletes(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x67, 32)
	p := oneVersion(m)
	p.gate = newGate()
	k := newKeys(t, maskingKeys(p), cat)
	joined := joining(k)
	leader, waiter := make(chan []byte, 1), make(chan error, 1)
	go func() { got, _ := k.Digest(t.Context(), "d-1"); leader <- got }()
	<-p.gate.arrived
	ctx, cancel := context.WithCancel(t.Context())
	var waiterKey []byte
	go func() {
		got, err := k.Digest(ctx, "d-1")
		waiterKey = got
		waiter <- err
	}()
	<-joined
	cancel()
	close(p.gate.release)
	if got := <-leader; !bytes.Equal(got, m) {
		t.Errorf("the leader = %x, want the material", got)
	}
	if err := <-waiter; !errors.Is(err, context.Canceled) || waiterKey != nil {
		t.Errorf("the cancelled waiter = %x, %v; want nothing", waiterKey, err)
	}
}
