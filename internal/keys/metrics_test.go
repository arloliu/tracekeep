package keys

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// recorder keeps what the module records, each call as its labels joined.
type recorder struct {
	mu    sync.Mutex
	calls []string
	until time.Time
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, s)
}

func (r *recorder) Resolution(kind, outcome string) { r.add("resolution " + kind + " " + outcome) }

func (r *recorder) ProviderCall(provider, op, reason string, _ time.Duration) {
	r.add("call " + provider + " " + op + " " + reason)
}

func (r *recorder) CredentialExpiry(provider string, at time.Time) {
	r.mu.Lock()
	r.until = at
	r.mu.Unlock()
	r.add("expiry " + provider)
}

func (r *recorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.calls)
}

// Each use is counted by kind and outcome: cached, resolved, unavailable or refused.
func TestResolutionOutcomes(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	p := oneVersion(material(0x91, 32))
	p.fail = 1
	r := &recorder{}
	cfg := maskingKeys(p)
	cfg.Declared["m-short"] = Declaration{Provider: "fake", Ref: "short", Kind: catalog.KindMaster}
	cfg.Metrics = r
	k := newKeys(t, cfg, cat)
	_, _ = k.Digest(t.Context(), "d-1")                      // the provider down: unavailable
	_, _ = k.Digest(t.Context(), "d-1")                      // resolved
	_, _ = k.Digest(t.Context(), "d-1")                      // cached
	_, _ = k.Digest(t.Context(), "m-1")                      // another kind: refused
	_, _ = k.Encryption(t.Context(), testCapture, "d-9")     // undeclared: refused
	p.byVer["1"] = material(0x92, 16)                        // short material for the next key id read
	_, _ = k.Encryption(t.Context(), testCapture, "m-short") // short: refused
	want := []string{
		"resolution digest unavailable", "resolution digest resolved", "resolution digest cached",
		"resolution digest refused", "resolution master refused", "resolution master refused",
	}
	if got := r.recorded(); !slices.Equal(got, want) {
		t.Errorf("recorded %q, want %q", got, want)
	}
}

func TestRefusedClasses(t *testing.T) {
	t.Parallel()
	for err, refused := range map[error]bool{
		catalog.ErrKeyBindingMismatch:         true,
		ErrUnknownKey:                         true,
		errRefusedMaterial:                    true,
		&ProviderError{Reason: "missing"}:     true,
		&ProviderError{Reason: "malformed"}:   true,
		&ProviderError{Reason: "auth"}:        false,
		&ProviderError{Reason: "deadline"}:    false,
		&ProviderError{Reason: "unreachable"}: false,
		&ProviderError{Reason: "server"}:      false,
		errProviderDown:                       false,
	} {
		if got := Refused(errors.Join(errors.New("wrapped"), err)); got != refused {
			t.Errorf("Refused(%v) = %v, want %v", err, got, refused)
		}
	}
}

// The vault provider records each call by operation and reason and its token's expiry; no label holds a credential, a key id or a reference.
func TestVaultMetrics(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	f.put("tk/d-1", material(0x93, 32))
	r := &recorder{}
	cfg := f.appRole()
	cfg.Metrics = r
	v := mustVault(t, cfg)
	now := time.Unix(1_800_000_000, 0)
	v.now = func() time.Time { return now }
	if _, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil {
		t.Fatal(err)
	}
	_, _, _ = v.Material(t.Context(), "tk/absent", "")
	want := []string{"call vault login ", "expiry vault", "call vault read ", "call vault read missing"}
	if got := r.recorded(); !slices.Equal(got, want) {
		t.Errorf("recorded %q, want %q", got, want)
	}
	if !r.until.Equal(now.Add(30 * time.Second)) {
		t.Errorf("expiry = %v, want the login's TTL from now", r.until)
	}
	for _, c := range r.recorded() {
		for _, s := range []string{"TOKEN-SENTINEL", sentinelSecretID, "tk/", "d-1"} {
			if strings.Contains(c, s) {
				t.Errorf("a label holds %s: %q", s, c)
			}
		}
	}
}
