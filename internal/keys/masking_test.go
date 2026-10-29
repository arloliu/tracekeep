package keys

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// fakeProvider is a versioned provider: material by version, the current one answering an empty request.
// When gate is set, every call reports its arrival on gate.arrived and waits on gate.release.
type fakeProvider struct {
	mu       sync.Mutex
	byVer    map[string][]byte
	current  string
	fail     int // the number of calls still to fail
	calls    int
	versions []string // the versions asked for, in order
	gate     *gate
}

type gate struct {
	arrived chan struct{}
	release chan struct{}
}

func newGate() *gate { return &gate{arrived: make(chan struct{}, 1), release: make(chan struct{})} }

var errProviderDown = errors.New("fake provider: unreachable")

func (f *fakeProvider) Material(ctx context.Context, _, version string) ([]byte, string, error) {
	f.mu.Lock()
	f.calls++
	f.versions = append(f.versions, version)
	g := f.gate
	failing := f.fail > 0
	if failing {
		f.fail--
	}
	f.mu.Unlock()
	if g != nil {
		g.arrived <- struct{}{}
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	if failing {
		return nil, "", errProviderDown
	}
	if version == "" {
		version = f.current
	}
	m, ok := f.byVer[version]
	if !ok {
		return nil, "", errors.New("fake provider: no such version")
	}

	return m, version, nil
}

func (f *fakeProvider) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

// oneVersion is a fake provider whose only and current version, "1", holds m.
func oneVersion(m []byte) *fakeProvider {
	return &fakeProvider{byVer: map[string][]byte{"1": m}, current: "1"}
}

// maskingKeys is a configuration declaring d-1 as a digest key id and m-1 as a master key id, both on p.
func maskingKeys(p Provider) Config {
	return Config{
		Declared: map[string]Declaration{
			"d-1": {Provider: "fake", Ref: "secret/d-1", Kind: catalog.KindDigest},
			"m-1": {Provider: "fake", Ref: "secret/m-1", Kind: catalog.KindMaster},
		},
		Providers: map[string]Provider{"fake": p},
	}
}

// specHKDF is RFC 5869's HKDF-SHA-256, written here from the RFC rather than taken from the code under test.
func specHKDF(ikm, salt, info []byte, n int) []byte {
	ext := hmac.New(sha256.New, salt)
	ext.Write(ikm)
	prk := ext.Sum(nil)
	var okm, t []byte
	for i := byte(1); len(okm) < n; i++ {
		h := hmac.New(sha256.New, prk)
		h.Write(t)
		h.Write(info)
		h.Write([]byte{i})
		t = h.Sum(nil)
		okm = append(okm, t...)
	}

	return okm[:n]
}

var testCapture = tracepack.UUID{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}

// EncryptionKey is §9's recipe: HKDF-SHA-256 over the master material, salt "tracekeep/capture-key/v1",
// info the capture id's 16 raw bytes then the key id's UTF-8, 64 bytes.
func TestEncryptionKeyFollowsTheRecipe(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x21, 32)
	k := newKeys(t, maskingKeys(oneVersion(m)), cat)
	got, err := k.Encryption(t.Context(), testCapture, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	// Computed independently of this repository (RFC 5869 by hand) for these inputs.
	const vector = "c729352867dd5896942c4fc69a1d14905d493a560b39c4a532176bd69fe8e8c6e29cea49223e1c724648bed4db20259bfbe7c0abe091a8fddeba2709b26675d5"
	if hex.EncodeToString(got) != vector {
		t.Errorf("EncryptionKey = %x, want the vector", got)
	}
	if want := specHKDF(m, []byte("tracekeep/capture-key/v1"), append(testCapture[:], "m-1"...), 64); !bytes.Equal(got, want) {
		t.Errorf("EncryptionKey = %x, want %x", got, want)
	}
	other := testCapture
	other[15] ^= 1
	if o, err := k.Encryption(t.Context(), other, "m-1"); err != nil || bytes.Equal(o, got) || len(o) != 64 {
		t.Errorf("another capture's key = %x, %v; want 64 other bytes", o, err)
	}
}

// The key id is part of the derivation: one master material under two key ids gives two keys.
func TestEncryptionKeyNamesItsKeyID(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x22, 32)
	cfg := maskingKeys(oneVersion(m))
	cfg.Declared["m-2"] = Declaration{Provider: "fake", Ref: "secret/m-2", Kind: catalog.KindMaster}
	k := newKeys(t, cfg, cat)
	a, errA := k.Encryption(t.Context(), testCapture, "m-1")
	b, errB := k.Encryption(t.Context(), testCapture, "m-2")
	if errA != nil || errB != nil || bytes.Equal(a, b) {
		t.Errorf("keys under m-1 and m-2 = %x, %x (%v, %v); want two keys", a, b, errA, errB)
	}
}

// A key id's UTF-8 bytes, not another encoding, follow the capture id in the info.
func TestEncryptionKeyNonASCIIKeyID(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	cfg := maskingKeys(oneVersion(material(0x21, 32)))
	cfg.Declared["主鍵-1"] = Declaration{Provider: "fake", Ref: "secret/m-x", Kind: catalog.KindMaster}
	got, err := newKeys(t, cfg, cat).Encryption(t.Context(), testCapture, "主鍵-1")
	// Computed independently of this repository (RFC 5869 by hand) for these inputs.
	const vector = "64c5c9ee76d117cedb9da21ad5aa435fb8058f639598eb3d04a4183b0cde58c6a7a9db1cae43ca216295dd72e4d00fccb0f8b74da49eefd2a93a0a5c0be766c6"
	if err != nil || hex.EncodeToString(got) != vector {
		t.Errorf("EncryptionKey under a non-ASCII key id = %x, %v; want the vector", got, err)
	}
}

// DigestKey is the bound material itself, one key whatever the capture.
func TestDigestKeyIsTheBoundMaterial(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x23, 32)
	k := newKeys(t, maskingKeys(oneVersion(m)), cat)
	for range 2 {
		if got, err := k.Digest(t.Context(), "d-1"); err != nil || !bytes.Equal(got, m) {
			t.Errorf("DigestKey = %x, %v; want the material", got, err)
		}
	}
	b, ok, err := cat.KeyBinding(t.Context(), "d-1")
	if !ok || err != nil || b.Kind != catalog.KindDigest || b.ProviderVersion != "1" || b.Fingerprint != specFingerprint(m) {
		t.Errorf("binding = %+v, %v, %v", b, ok, err)
	}
}

// A key id serves one purpose: a request under any other fails closed, before any provider call when the declaration already says so.
func TestMaskingPurposesStayDistinct(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	p := oneVersion(material(0x24, 32))
	cfg := maskingKeys(p)
	cfg.Declared["dl-1"] = Declaration{Provider: "fake", Ref: "secret/dl-1"}
	cfg.Download = Purpose{Issuing: "dl-1", Accepted: []string{"dl-1"}}
	k := newKeys(t, cfg, cat)
	for name, call := range map[string]func() error{
		"a master key id as digest":   func() error { _, err := k.Digest(t.Context(), "m-1"); return err },
		"a digest key id as master":   func() error { _, err := k.Encryption(t.Context(), testCapture, "d-1"); return err },
		"a download key id as digest": func() error { _, err := k.Digest(t.Context(), "dl-1"); return err },
		"a download key id as master": func() error { _, err := k.Encryption(t.Context(), testCapture, "dl-1"); return err },
		"a digest key id as download": func() error { _, err := k.Accepted(t.Context(), catalog.KindDownload, "d-1"); return err },
		"an undeclared key id":        func() error { _, err := k.Digest(t.Context(), "d-9"); return err },
	} {
		if err := call(); !errors.Is(err, ErrUnknownKey) {
			t.Errorf("%s = %v, want ErrUnknownKey", name, err)
		}
	}
	if n := p.callCount(); n != 0 {
		t.Errorf("the provider was called %d times, want none", n)
	}
	// A binding under another kind, made by another configuration, fails closed too, still before the provider.
	if _, err := cat.BindKey(t.Context(), catalog.KeyBinding{KeyID: "m-1", Kind: catalog.KindDigest, ProviderRef: "fake:secret/m-1", Fingerprint: specFingerprint(material(0x24, 32))}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Encryption(t.Context(), testCapture, "m-1"); !errors.Is(err, catalog.ErrKeyBindingMismatch) {
		t.Errorf("a master key id bound as digest = %v, want a binding mismatch", err)
	}
	if n := p.callCount(); n != 0 {
		t.Errorf("the provider was called %d times, want none", n)
	}
}

func TestNewChecksMaskingDeclarations(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	p := oneVersion(material(0x25, 32))
	digestAccepted := maskingKeys(p)
	digestAccepted.Download = Purpose{Issuing: "d-1", Accepted: []string{"d-1"}}
	unknownKind := maskingKeys(p)
	unknownKind.Declared["x-1"] = Declaration{Provider: "fake", Ref: "x", Kind: "cookie"}
	shadowed := maskingKeys(p)
	shadowed.Providers = map[string]Provider{ProviderFile: p}
	for name, cfg := range map[string]Config{
		"a digest key id accepted for downloads": digestAccepted,
		"an unknown kind":                        unknownKind,
		"a provider named like a built-in one":   shadowed,
	} {
		if _, err := New(cfg, cat); err == nil {
			t.Errorf("New with %s succeeded", name)
		}
	}
}

// Short material is refused for every purpose, and nothing is bound.
func TestShortMaskingMaterialRefused(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x26, 31)
	k := newKeys(t, maskingKeys(oneVersion(m)), cat)
	_, errD := k.Digest(t.Context(), "d-1")
	_, errM := k.Encryption(t.Context(), testCapture, "m-1")
	if errD == nil || errM == nil {
		t.Fatalf("31 bytes of material: digest %v, master %v; want both refused", errD, errM)
	}
	noMaterial(t, errD, m)
	noMaterial(t, errM, m)
	for _, id := range []string{"d-1", "m-1"} {
		if _, ok, _ := cat.KeyBinding(t.Context(), id); ok {
			t.Errorf("short material bound %s", id)
		}
	}
}

// A failed resolution caches nothing: the next use asks the provider again and succeeds.
func TestFailedResolutionCachesNothing(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x27, 32)
	p := oneVersion(m)
	p.fail = 1
	k := newKeys(t, maskingKeys(p), cat)
	if _, err := k.Encryption(t.Context(), testCapture, "m-1"); !errors.Is(err, errProviderDown) {
		t.Fatalf("Encryption with the provider down = %v", err)
	}
	if _, ok, _ := cat.KeyBinding(t.Context(), "m-1"); ok {
		t.Error("a failed resolution bound the key id")
	}
	if got, err := k.Encryption(t.Context(), testCapture, "m-1"); err != nil || len(got) != 64 {
		t.Errorf("Encryption once the provider is back = %x, %v", got, err)
	}
	if n := p.callCount(); n != 2 {
		t.Errorf("provider calls = %d, want 2: the failure cached nothing", n)
	}
}

// Two real Keys instances resolve different material for one master key id before either binds:
// the first to bind wins, and the loser returns, caches and derives nothing.
func TestFirstUseRaceDifferentMaterial(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		mA, mB := material(0x31, 32), material(0x32, 32)
		pA, pB := oneVersion(mA), oneVersion(mB)
		pA.gate, pB.gate = newGate(), newGate()
		kA, kB := newKeys(t, maskingKeys(pA), cat), newKeys(t, maskingKeys(pB), cat)
		type result struct {
			key []byte
			err error
		}
		resA, resB := make(chan result, 1), make(chan result, 1)
		go func() { key, err := kA.Encryption(t.Context(), testCapture, "m-1"); resA <- result{key, err} }()
		go func() { key, err := kB.Encryption(t.Context(), testCapture, "m-1"); resB <- result{key, err} }()
		// Both have their material before either binds.
		<-pA.gate.arrived
		<-pB.gate.arrived
		close(pA.gate.release)
		a := <-resA
		if a.err != nil || !bytes.Equal(a.key, specHKDF(mA, []byte("tracekeep/capture-key/v1"), append(testCapture[:], "m-1"...), 64)) {
			t.Fatalf("the first to bind = %x, %v; want its derived key", a.key, a.err)
		}
		close(pB.gate.release)
		b := <-resB
		if !errors.Is(b.err, catalog.ErrKeyBindingMismatch) || b.key != nil {
			t.Fatalf("the loser = %x, %v; want no key and a binding mismatch", b.key, b.err)
		}
		noMaterial(t, b.err, mB)
		// The loser cached nothing: its next use asks its provider again, and fails again.
		pB.gate = nil
		if key, err := kB.Encryption(t.Context(), testCapture, "m-1"); !errors.Is(err, catalog.ErrKeyBindingMismatch) || key != nil {
			t.Errorf("the loser's next use = %x, %v; want a binding mismatch", key, err)
		}
		if n := pB.callCount(); n != 2 {
			t.Errorf("the loser's provider calls = %d, want 2", n)
		}
		if bnd, _, _ := cat.KeyBinding(t.Context(), "m-1"); bnd.Fingerprint != specFingerprint(mA) {
			t.Errorf("binding fingerprint = %s, want the winner's", bnd.Fingerprint)
		}
	})
}

// Two real Keys instances resolve equal material at different provider versions before either binds:
// one binding wins, both serve the key, and a fresh process follows the bound version.
func TestFirstUseRaceEqualMaterialOtherVersion(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		m := material(0x33, 32)
		pA := &fakeProvider{byVer: map[string][]byte{"3": m}, current: "3", gate: newGate()}
		pB := &fakeProvider{byVer: map[string][]byte{"3": m, "4": m}, current: "4", gate: newGate()}
		kA, kB := newKeys(t, maskingKeys(pA), cat), newKeys(t, maskingKeys(pB), cat)
		errA, errB := make(chan error, 1), make(chan error, 1)
		go func() { _, err := kA.Digest(t.Context(), "d-1"); errA <- err }()
		go func() { _, err := kB.Digest(t.Context(), "d-1"); errB <- err }()
		<-pA.gate.arrived
		<-pB.gate.arrived
		close(pA.gate.release)
		if err := <-errA; err != nil {
			t.Fatal(err)
		}
		close(pB.gate.release)
		if err := <-errB; err != nil {
			t.Fatalf("equal material at another version = %v, want the binding accepted", err)
		}
		if bnd, _, _ := cat.KeyBinding(t.Context(), "d-1"); bnd.ProviderVersion != "3" {
			t.Fatalf("bound version = %q, want the winner's 3", bnd.ProviderVersion)
		}
		// A fresh process asks for the bound version, never the current one.
		pC := &fakeProvider{byVer: map[string][]byte{"3": m, "4": material(0x34, 32)}, current: "4"}
		if got, err := newKeys(t, maskingKeys(pC), cat).Digest(t.Context(), "d-1"); err != nil || !bytes.Equal(got, m) {
			t.Errorf("a fresh process's digest key = %x, %v; want the bound version's material", got, err)
		}
		if len(pC.versions) != 1 || pC.versions[0] != "3" {
			t.Errorf("a fresh process asked for versions %q, want [3]", pC.versions)
		}
	})
}
