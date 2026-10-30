package keys

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
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
		"call fake read unreachable", "resolution digest unavailable",
		"call fake read ", "resolution digest resolved", "resolution digest cached",
		"resolution digest refused", "resolution master refused",
		"call fake read ", "resolution master refused",
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
	// A renewal answered 200 with no token is a malformed renewal, counted as a failure, then replaced by a login.
	now = now.Add(21 * time.Second)
	f.mu.Lock()
	f.renewBody = `{"auth":null}`
	f.mu.Unlock()
	if _, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil {
		t.Fatal(err)
	}
	// Reads are the keys module's to record; the provider records its logins and renewals once validated.
	want := []string{"call vault login ", "expiry vault", "call vault renew malformed", "call vault login ", "expiry vault"}
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

// File and env failures are refusals or unavailable by reason, through real Keys uses, and their reads are recorded with that reason.
func TestFileFailuresClassified(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad")
	if err := os.WriteFile(bad, []byte("not base64!"), 0o600); err != nil {
		t.Fatal(err)
	}
	open := keyFile(t, material(0x94, 32), 0o644)
	r := &recorder{}
	cfg := Config{
		Declared: map[string]Declaration{
			"d-absent": {Provider: ProviderFile, Ref: filepath.Join(dir, "absent"), Kind: catalog.KindDigest},
			"d-bad":    {Provider: ProviderFile, Ref: bad, Kind: catalog.KindDigest},
			"d-open":   {Provider: ProviderFile, Ref: open, Kind: catalog.KindDigest},
		},
		Metrics: r,
	}
	k := newKeys(t, cfg, cat)
	for id, reason := range map[string]string{"d-absent": "missing", "d-bad": "malformed", "d-open": "malformed"} {
		_, err := k.Digest(t.Context(), id)
		if !Refused(err) {
			t.Errorf("%s = %v, want refused", id, err)
		}
		if !slices.Contains(r.recorded(), "call file read "+reason) {
			t.Errorf("%s: recorded %q, want a %s read", id, r.recorded(), reason)
		}
	}
	if n := strings.Count(strings.Join(r.recorded(), ","), "resolution digest refused"); n != 3 {
		t.Errorf("refusals counted %d, want 3", n)
	}
}

// A key file's text, a key file of the wrong mode and an environment variable's value stay out of the error and the metrics.
func TestFileAndEnvFailuresHoldNoSecrets(t *testing.T) {
	const sentinel = "KEY-TEXT-SENTINEL" // a hyphen is outside base64's alphabet
	text := filepath.Join(t.TempDir(), "text")
	if err := os.WriteFile(text, []byte(sentinel), 0o600); err != nil {
		t.Fatal(err)
	}
	m := material(0x95, 32)
	t.Setenv("TK_TEST_ENV_KEY", sentinel)
	srv := metrics.NewServer()
	km, err := metrics.NewKeys(srv)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Dev: true,
		Declared: map[string]Declaration{
			"d-text": {Provider: ProviderFile, Ref: text, Kind: catalog.KindDigest},
			"d-open": {Provider: ProviderFile, Ref: keyFile(t, m, 0o644), Kind: catalog.KindDigest},
			"d-env":  {Provider: ProviderEnv, Ref: "TK_TEST_ENV_KEY", Kind: catalog.KindDigest},
		},
		Metrics: km,
	}
	k := newKeys(t, cfg, catalogtest.Open(t, catalog.EngineSQLite))
	for _, id := range []string{"d-text", "d-open", "d-env"} {
		_, err := k.Digest(t.Context(), id)
		if !Refused(err) {
			t.Errorf("%s = %v, want refused", id, err)
		}
		if err != nil && strings.Contains(err.Error(), sentinel) {
			t.Errorf("%s: error %q holds the key's text", id, err)
		}
		noMaterial(t, err, m)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	scrape := rec.Body.String()
	for _, want := range []string{
		`tracekeep_key_provider_errors_total{provider="file",reason="malformed"} 2`,
		`tracekeep_key_provider_errors_total{provider="env",reason="malformed"} 1`,
		`tracekeep_key_resolutions_total{kind="digest",outcome="refused"} 3`,
	} {
		if !strings.Contains(scrape, want) {
			t.Errorf("no %s in\n%s", want, scrape)
		}
	}
	for _, s := range []string{sentinel, string(m), base64.StdEncoding.EncodeToString(m)} {
		if strings.Contains(scrape, s) {
			t.Errorf("the metrics hold %q", s)
		}
	}
}
