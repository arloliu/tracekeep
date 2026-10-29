package keys

import (
	"errors"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// key verify re-resolves at the binding's version past the cache, reports a mismatch without failing, and binds nothing.
func TestVerify(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(0x71, 32)
	p := &fakeProvider{byVer: map[string][]byte{"1": m}, current: "1"}
	k := newKeys(t, maskingKeys(p), cat)
	if _, err := k.Verify(t.Context(), "d-1"); !errors.Is(err, ErrNotBound) {
		t.Errorf("Verify of an unbound key id = %v, want ErrNotBound", err)
	}
	if _, ok, _ := cat.KeyBinding(t.Context(), "d-1"); ok {
		t.Error("Verify bound a key id")
	}
	if _, err := k.Digest(t.Context(), "d-1"); err != nil {
		t.Fatal(err)
	}
	v, err := k.Verify(t.Context(), "d-1")
	if err != nil || !v.Match || v.Bound != specFingerprint(m) || v.Resolved != v.Bound || v.ProviderVersion != "1" || v.Kind != catalog.KindDigest {
		t.Errorf("Verify of intact material = %+v, %v", v, err)
	}
	// The material at the pinned version changed, and a newer version exists: verify reads the pinned one, past the cache.
	p.mu.Lock()
	p.byVer["1"], p.byVer["2"], p.current = material(0x72, 32), m, "2"
	p.mu.Unlock()
	calls := p.callCount()
	v, err = k.Verify(t.Context(), "d-1")
	if err != nil || v.Match || v.Resolved != specFingerprint(material(0x72, 32)) {
		t.Errorf("Verify of changed material = %+v, %v; want a reported mismatch", v, err)
	}
	if p.callCount() != calls+1 || p.versions[len(p.versions)-1] != "1" {
		t.Errorf("Verify asked versions %q, want one more read of version 1", p.versions)
	}
	p.mu.Lock()
	p.fail = 1
	p.mu.Unlock()
	if _, err := k.Verify(t.Context(), "d-1"); !errors.Is(err, errProviderDown) {
		t.Errorf("Verify with the provider down = %v", err)
	}
}
