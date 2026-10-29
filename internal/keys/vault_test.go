package keys

import (
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// Synthetic secrets: none may appear in an error.
const (
	sentinelSecretID = "SECRET-ID-SENTINEL"
	sentinelBody     = "BODY-SENTINEL"
)

// fakeVault is an httptest Vault: AppRole and Kubernetes logins, renew-self, and KV v2 reads under mount secret.
// Each login issues a fresh token whose name holds a sentinel, so a leaked token is detectable.
type fakeVault struct {
	t   *testing.T
	srv *httptest.Server
	ca  string

	mu        sync.Mutex
	logins    []map[string]string
	renewals  int
	reads     []string // the request URIs of reads
	tokens    map[string]bool
	ttl       int64 // seconds a login grants
	renewTTL  int64 // seconds a renewal grants; 0 grants ttl
	renewable bool
	renewFail bool
	loginFail int // HTTP status of logins, 0 for success
	forbid    map[string]bool
	readFail  int // HTTP status of reads, 0 for success
	stall     chan struct{}
	secrets   map[string]map[uint64]any // path -> version -> material field (string, or another JSON value)
	deleted   map[string]map[uint64]bool
	current   map[string]uint64
	raw       map[string]string // path -> a raw response body instead
}

func newFakeVault(t *testing.T) *fakeVault {
	t.Helper()
	f := &fakeVault{
		t: t, tokens: map[string]bool{}, ttl: 30, renewable: true, forbid: map[string]bool{},
		secrets: map[string]map[uint64]any{}, deleted: map[string]map[uint64]bool{}, current: map[string]uint64{}, raw: map[string]string{},
	}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	f.ca = filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(f.ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	return f
}

// put stores m as path's next version.
func (f *fakeVault) put(path string, m []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.secrets[path] == nil {
		f.secrets[path] = map[uint64]any{}
	}
	f.current[path]++
	f.secrets[path][f.current[path]] = base64.StdEncoding.EncodeToString(m)
}

func (f *fakeVault) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	stall := f.stall
	f.mu.Unlock()
	if stall != nil {
		select {
		case <-stall:
		case <-r.Context().Done():
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	body := map[string]string{}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	token := r.Header.Get("X-Vault-Token")
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/login"):
		f.logins = append(f.logins, body)
		if f.loginFail != 0 {
			http.Error(w, `{"errors":["`+sentinelBody+`"]}`, f.loginFail)
			return
		}
		tok := fmt.Sprintf("hvs.TOKEN-SENTINEL-%d", len(f.logins))
		f.tokens[tok] = true
		f.auth(w, tok, f.ttl)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/auth/token/renew-self":
		f.renewals++
		if f.renewFail || !f.tokens[token] {
			http.Error(w, `{"errors":["`+sentinelBody+`"]}`, http.StatusInternalServerError)
			return
		}
		ttl := f.renewTTL
		if ttl == 0 {
			ttl = f.ttl
		}
		f.auth(w, token, ttl)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/secret/data/"):
		f.serveRead(w, r, token)
	default:
		http.NotFound(w, r)
	}
}

// serveRead answers a KV v2 read; f.mu is held.
func (f *fakeVault) serveRead(w http.ResponseWriter, r *http.Request, token string) {
	f.reads = append(f.reads, r.URL.RequestURI())
	if !f.tokens[token] || f.forbid[token] {
		http.Error(w, `{"errors":["permission denied `+sentinelBody+`"]}`, http.StatusForbidden)
		return
	}
	if f.readFail != 0 {
		http.Error(w, `{"errors":["`+sentinelBody+`"]}`, f.readFail)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/secret/data/")
	if raw, ok := f.raw[path]; ok {
		_, _ = w.Write([]byte(raw))
		return
	}
	version := f.current[path]
	if q := r.URL.Query().Get("version"); q != "" {
		_, _ = fmt.Sscan(q, &version)
	}
	m, ok := f.secrets[path][version]
	if !ok {
		http.Error(w, `{"errors":[]}`, http.StatusNotFound)
		return
	}
	meta := map[string]any{"version": version, "deletion_time": "", "destroyed": false}
	var data any = map[string]any{"material": m}
	status := http.StatusOK
	if f.deleted[path][version] {
		// KV v2 answers a deleted version with 404, its metadata and no data.
		meta["deletion_time"], data, status = "2026-10-29T00:00:00Z", nil, http.StatusNotFound
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data, "metadata": meta}})
}

func (f *fakeVault) auth(w http.ResponseWriter, tok string, ttl int64) {
	_ = json.NewEncoder(w).Encode(map[string]any{"auth": map[string]any{"client_token": tok, "lease_duration": ttl, "renewable": f.renewable}})
}

// history copies the logins' bodies and the reads' URIs.
func (f *fakeVault) history() ([]map[string]string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]map[string]string(nil), f.logins...), append([]string(nil), f.reads...)
}

func (f *fakeVault) counts() (logins, renewals, reads int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.logins), f.renewals, len(f.reads)
}

// appRole is an AppRole configuration for f.
func (f *fakeVault) appRole() VaultConfig {
	return VaultConfig{
		Address: f.srv.URL, CAFile: f.ca, Mount: "secret", Timeout: 5 * time.Second,
		Method: VaultAppRole, RoleID: "role-1", SecretID: sentinelSecretID,
	}
}

func mustVault(t *testing.T, cfg VaultConfig) *Vault {
	t.Helper()
	v, err := NewVault(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

// noSecret fails when err's text holds a token, the secret id, a response body or m.
func noSecret(t *testing.T, err error, m []byte) {
	t.Helper()
	if err == nil {
		return
	}
	for _, s := range []string{"TOKEN-SENTINEL", sentinelSecretID, sentinelBody} {
		if strings.Contains(err.Error(), s) {
			t.Errorf("error %q holds %s", err, s)
		}
	}
	if len(m) > 0 {
		noMaterial(t, err, m)
	}
}

func TestVaultReadsAtTheVersionAsked(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	m1, m2 := material(0x41, 32), material(0x42, 32)
	f.put("tk/m-1", m1)
	f.put("tk/m-1", m2)
	v := mustVault(t, f.appRole())
	if got, ver, err := v.Material(t.Context(), "tk/m-1", "1"); err != nil || ver != "1" || string(got) != string(m1) {
		t.Errorf("Material at version 1 = %x, %q, %v", got, ver, err)
	}
	if got, ver, err := v.Material(t.Context(), "tk/m-1", ""); err != nil || ver != "2" || string(got) != string(m2) {
		t.Errorf("Material at the current version = %x, %q, %v", got, ver, err)
	}
	logins, reads := f.history()
	if len(reads) != 2 || reads[0] != "/v1/secret/data/tk/m-1?version=1" || reads[1] != "/v1/secret/data/tk/m-1" {
		t.Errorf("reads = %q", reads)
	}
	if len(logins) != 1 || logins[0]["role_id"] != "role-1" || logins[0]["secret_id"] != sentinelSecretID {
		t.Errorf("logins = %v, want one AppRole login for both reads", logins)
	}
}

// Kubernetes auth reads the service account token at every login, since the file rotates.
func TestVaultKubernetesReadsTheTokenAtEachLogin(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	f.put("tk/d-1", material(0x43, 32))
	jwt := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(jwt, []byte("jwt-one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := f.appRole()
	cfg.Method, cfg.RoleID, cfg.SecretID, cfg.Role, cfg.TokenFile = VaultKubernetes, "", "", "tracekeep", jwt
	v := mustVault(t, cfg)
	if _, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jwt, []byte("jwt-two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.forbid["hvs.TOKEN-SENTINEL-1"] = true
	f.mu.Unlock()
	if _, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil {
		t.Fatal(err)
	}
	if logins, _ := f.history(); len(logins) != 2 || logins[0]["jwt"] != "jwt-one" || logins[1]["jwt"] != "jwt-two" || logins[1]["role"] != "tracekeep" {
		t.Errorf("Kubernetes logins = %v; want each with the file's token then", logins)
	}
}

// The token is renewed past two thirds of its TTL; a failed renewal, a token that does not renew,
// and a renewal whose TTL shrank below a third of the login's (its maximum TTL near) each log in afresh.
func TestVaultRenewsAndReplacesItsToken(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		setup       func(f *fakeVault)
		wantLogins  int
		wantRenewed int
	}{
		"renewed":           {func(*fakeVault) {}, 1, 1},
		"renewal failed":    {func(f *fakeVault) { f.renewFail = true }, 2, 1},
		"not renewable":     {func(*fakeVault) {}, 2, 0},
		"maximum TTL near":  {func(f *fakeVault) { f.renewTTL = 9 }, 2, 1},
		"before two thirds": {nil, 1, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFakeVault(t)
			f.put("tk/d-1", material(0x44, 32))
			f.renewable = name != "not renewable"
			v := mustVault(t, f.appRole())
			now := time.Unix(1_800_000_000, 0)
			v.now = func() time.Time { return now }
			if _, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil {
				t.Fatal(err)
			}
			if tc.setup == nil {
				now = now.Add(19 * time.Second)
			} else {
				f.mu.Lock()
				tc.setup(f)
				f.mu.Unlock()
				now = now.Add(21 * time.Second)
			}
			if _, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil {
				t.Fatal(err)
			}
			if logins, renewals, _ := f.counts(); logins != tc.wantLogins || renewals != tc.wantRenewed {
				t.Errorf("logins %d, renewals %d; want %d, %d", logins, renewals, tc.wantLogins, tc.wantRenewed)
			}
		})
	}
}

// A read refused as forbidden logs in afresh and retries once; a second refusal is an unavailable auth failure.
func TestVaultForbiddenReadRelogsInOnce(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	m := material(0x45, 32)
	f.put("tk/d-1", m)
	v := mustVault(t, f.appRole())
	if _, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil {
		t.Fatal(err)
	}
	// The token is revoked: one login replaces it.
	f.mu.Lock()
	delete(f.tokens, "hvs.TOKEN-SENTINEL-1")
	f.mu.Unlock()
	if got, _, err := v.Material(t.Context(), "tk/d-1", ""); err != nil || string(got) != string(m) {
		t.Fatalf("Material after a revocation = %x, %v", got, err)
	}
	// Every token is refused: one login, one retry, then the failure.
	f.mu.Lock()
	f.forbid["hvs.TOKEN-SENTINEL-2"], f.forbid["hvs.TOKEN-SENTINEL-3"] = true, true
	f.mu.Unlock()
	_, _, err := v.Material(t.Context(), "tk/d-1", "")
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "auth") {
		t.Errorf("Material with every token refused = %v, want an unavailable auth failure", err)
	}
	noSecret(t, err, m)
	if logins, _, reads := f.counts(); logins != 3 || reads != 5 {
		t.Errorf("logins %d, reads %d; want 3 and 5", logins, reads)
	}
}

// A deleted pinned version is missing, a refusal, even beside a newer version: nothing falls back.
func TestVaultDeletedPinnedVersionIsMissing(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	f.put("tk/m-1", material(0x46, 32))
	f.put("tk/m-1", material(0x47, 32))
	f.mu.Lock()
	f.deleted["tk/m-1"] = map[uint64]bool{1: true}
	f.mu.Unlock()
	v := mustVault(t, f.appRole())
	_, _, err := v.Material(t.Context(), "tk/m-1", "1")
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.Reason != "missing" || errors.Is(err, ErrUnavailable) {
		t.Errorf("Material of a deleted version = %v, want missing, not unavailable", err)
	}
	if _, reads := f.history(); len(reads) != 1 || reads[0] != "/v1/secret/data/tk/m-1?version=1" {
		t.Errorf("reads = %q, want only version 1", reads)
	}
	if _, _, err := v.Material(t.Context(), "tk/absent", ""); !errors.As(err, &pe) || pe.Reason != "missing" {
		t.Errorf("Material of an absent secret = %v, want missing", err)
	}
	// A server answering a deleted or destroyed version with 200, its data still present, is read as missing too.
	f.mu.Lock()
	f.raw["tk/soft"] = `{"data":{"data":{"material":"` + base64.StdEncoding.EncodeToString(material(0x48, 32)) + `"},"metadata":{"version":1,"deletion_time":"2026-10-29T00:00:00Z"}}}`
	f.raw["tk/gone"] = `{"data":{"data":{"material":"` + base64.StdEncoding.EncodeToString(material(0x48, 32)) + `"},"metadata":{"version":1,"destroyed":true}}}`
	f.mu.Unlock()
	for _, ref := range []string{"tk/soft", "tk/gone"} {
		if _, _, err := v.Material(t.Context(), ref, "1"); !errors.As(err, &pe) || pe.Reason != "missing" {
			t.Errorf("Material of %s = %v, want missing", ref, err)
		}
	}
}

func TestVaultMalformedResponses(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	f.raw["tk/not-json"] = "not json " + sentinelBody
	f.raw["tk/no-field"] = `{"data":{"data":{"other":"x"},"metadata":{"version":1}}}`
	f.raw["tk/not-base64"] = `{"data":{"data":{"material":"!!` + sentinelBody + `"},"metadata":{"version":1}}}`
	f.raw["tk/not-string"] = `{"data":{"data":{"material":42},"metadata":{"version":1}}}`
	f.raw["tk/wrong-version"] = `{"data":{"data":{"material":"` + base64.StdEncoding.EncodeToString(material(1, 32)) + `"},"metadata":{"version":2}}}`
	v := mustVault(t, f.appRole())
	for _, c := range []struct{ ref, version string }{
		{"tk/not-json", ""}, {"tk/no-field", ""}, {"tk/not-base64", ""}, {"tk/not-string", ""}, {"tk/wrong-version", "1"}, {"tk/x", "latest"},
	} {
		_, _, err := v.Material(t.Context(), c.ref, c.version)
		var pe *ProviderError
		if !errors.As(err, &pe) || pe.Reason != "malformed" || errors.Is(err, ErrUnavailable) {
			t.Errorf("%s at %q = %v, want malformed", c.ref, c.version, err)
		}
		noSecret(t, err, nil)
	}
}

// A stalled server fails the call at its deadline, unavailable; the error holds no secret.
func TestVaultStalledServerMeetsTheDeadline(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	f.put("tk/d-1", material(0x48, 32))
	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) })
	f.stall = stall
	cfg := f.appRole()
	cfg.Timeout = 50 * time.Millisecond
	_, _, err := mustVault(t, cfg).Material(t.Context(), "tk/d-1", "")
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.Reason != "deadline" || pe.Op != "login" || !errors.Is(err, ErrUnavailable) {
		t.Errorf("Material against a stalled server = %v, want a login deadline", err)
	}
}

// Server and login failures are unavailable and carry the status, never the body, the token or the secret id.
func TestVaultFailuresHoldNoSecrets(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	m := material(0x49, 32)
	f.put("tk/d-1", m)
	f.loginFail = http.StatusBadRequest
	v := mustVault(t, f.appRole())
	_, _, err := v.Material(t.Context(), "tk/d-1", "")
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "400") {
		t.Errorf("a refused login = %v", err)
	}
	noSecret(t, err, m)
	f.mu.Lock()
	f.loginFail, f.readFail = 0, http.StatusServiceUnavailable
	f.mu.Unlock()
	_, _, err = v.Material(t.Context(), "tk/d-1", "")
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "503") {
		t.Errorf("a sealed server = %v", err)
	}
	noSecret(t, err, m)
}

// A redirect is never followed, so the token never leaves for another host.
func TestVaultRefusesRedirects(t *testing.T) {
	t.Parallel()
	var hit bool
	elsewhere := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit = true }))
	t.Cleanup(elsewhere.Close)
	f := newFakeVault(t)
	redirect := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	redirect.TLS = f.srv.TLS
	redirect.StartTLS()
	t.Cleanup(redirect.Close)
	cfg := f.appRole()
	cfg.Address = redirect.URL
	if _, _, err := mustVault(t, cfg).Material(t.Context(), "tk/d-1", ""); err == nil || hit {
		t.Errorf("Material through a redirect = %v, followed %v", err, hit)
	}
}

func TestNewVaultChecksItsConfiguration(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	for name, change := range map[string]func(c *VaultConfig){
		"an http address":            func(c *VaultConfig) { c.Address = "http://vault.example:8200" },
		"no mount":                   func(c *VaultConfig) { c.Mount = "" },
		"no deadline":                func(c *VaultConfig) { c.Timeout = 0 },
		"an unknown method":          func(c *VaultConfig) { c.Method = "userpass" },
		"approle without a secret":   func(c *VaultConfig) { c.SecretID = "" },
		"kubernetes without a role":  func(c *VaultConfig) { c.Method, c.TokenFile = VaultKubernetes, "/x" },
		"a CA file with no PEM":      func(c *VaultConfig) { c.CAFile = os.DevNull },
		"a CA file that is not here": func(c *VaultConfig) { c.CAFile = filepath.Join(t.TempDir(), "absent") },
	} {
		cfg := f.appRole()
		change(&cfg)
		if _, err := NewVault(cfg); err == nil {
			t.Errorf("NewVault with %s succeeded", name)
		}
	}
}

// Through the keys module: a vault key id binds at the version read, and a fresh process reads that version, not the current one.
func TestVaultKeyBindsItsVersion(t *testing.T) {
	t.Parallel()
	f := newFakeVault(t)
	m := material(0x4a, 32)
	f.put("tk/d-1", m)
	cfg := Config{
		Declared:  map[string]Declaration{"d-1": {Provider: ProviderVault, Ref: "tk/d-1", Kind: "digest"}},
		Providers: map[string]Provider{ProviderVault: mustVault(t, f.appRole())},
	}
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	if got, err := newKeys(t, cfg, cat).Digest(t.Context(), "d-1"); err != nil || string(got) != string(m) {
		t.Fatalf("Digest = %x, %v", got, err)
	}
	f.put("tk/d-1", material(0x4b, 32))
	cfg.Providers[ProviderVault] = mustVault(t, f.appRole())
	if got, err := newKeys(t, cfg, cat).Digest(t.Context(), "d-1"); err != nil || string(got) != string(m) {
		t.Errorf("a fresh process's Digest = %x, %v; want version 1's material", got, err)
	}
	if _, reads := f.history(); reads[len(reads)-1] != "/v1/secret/data/tk/d-1?version=1" {
		last := reads[len(reads)-1]
		t.Errorf("a fresh process read %s, want version 1", last)
	}
}
