package keys

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ProviderVault names the vault provider in a declaration.
const ProviderVault = "vault"

// Vault auth methods.
const (
	VaultKubernetes = "kubernetes"
	VaultAppRole    = "approle"
)

// maxVaultBody bounds a Vault response's read.
const maxVaultBody = 1 << 20

// ErrUnavailable marks a provider failure a retry may cure: the server unreachable, its deadline passed,
// its credential refused or expired, or a server error (masking-keys.md §9's unavailable class).
var ErrUnavailable = errors.New("keys: provider unavailable")

// ProviderError is a provider call's failure: the operation, a reason from a bounded set and the server's status.
// It never holds a response body, a token, a secret id or material.
type ProviderError struct {
	Provider string
	// Op is login, renew or read.
	Op string
	// Reason is unreachable, deadline, auth, missing, malformed or server.
	Reason string
	// Status is the HTTP status, 0 when no response came.
	Status int
	// Detail, when set, says more and holds nothing secret, such as a file's mode.
	Detail string
}

func (e *ProviderError) Error() string {
	msg := fmt.Sprintf("%s provider: %s: %s", e.Provider, e.Op, e.Reason)
	if e.Status != 0 {
		msg += fmt.Sprintf(" (status %d)", e.Status)
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}

	return msg
}

// Is makes an unreachable, deadline, auth or server failure ErrUnavailable; missing and malformed material are refusals.
func (e *ProviderError) Is(target error) bool {
	return target == ErrUnavailable && e.Reason != "missing" && e.Reason != "malformed"
}

// VaultConfig is the vault provider's configuration (masking-keys.md §9, the configuration guide's keys.vault).
type VaultConfig struct {
	// Address is the server's https URL; CAFile, when set, verifies it beside the system's roots.
	Address, CAFile string
	// Mount is the KV v2 mount the references are under.
	Mount string
	// Timeout bounds every call to the server.
	Timeout time.Duration
	// Method is kubernetes or approle; AuthMount defaults to the method's name; Role is the role logged in as.
	Method, AuthMount, Role string
	// TokenFile is the service account token kubernetes logs in with, read at each login.
	TokenFile string
	// RoleID and SecretID are approle's credentials.
	RoleID, SecretID string
	// Metrics records each call and the token's expiry; nil records nothing.
	Metrics Metrics
}

// Vault is the vault provider: KV v2 reads at a version, under a token it logs in for, renews and replaces.
// A reference is the secret's path under the mount; the material is its material field, base64 text.
type Vault struct {
	cfg    VaultConfig
	client *http.Client
	now    func() time.Time

	// mu serializes logins and renewals; reads run outside it.
	mu        sync.Mutex
	token     string
	loginTTL  time.Duration
	renewAt   time.Time
	renewable bool
}

// NewVault checks cfg and returns its provider; it calls nothing, so a start never waits on Vault.
func NewVault(cfg VaultConfig) (*Vault, error) {
	var errs []error
	if u, err := url.Parse(cfg.Address); err != nil || u.Scheme != "https" || u.Host == "" {
		errs = append(errs, fmt.Errorf("vault: address %q must be an https URL", cfg.Address))
	}
	if cfg.Mount == "" {
		errs = append(errs, errors.New("vault: no KV v2 mount"))
	}
	if cfg.Timeout <= 0 {
		errs = append(errs, errors.New("vault: the call deadline must be positive"))
	}
	switch cfg.Method {
	case VaultKubernetes:
		if cfg.Role == "" || cfg.TokenFile == "" {
			errs = append(errs, errors.New("vault: kubernetes auth needs a role and a token file"))
		}
	case VaultAppRole:
		if cfg.RoleID == "" || cfg.SecretID == "" {
			errs = append(errs, errors.New("vault: approle auth needs a role id and a secret id"))
		}
	default:
		errs = append(errs, fmt.Errorf("vault: unknown auth method %q", cfg.Method))
	}
	if cfg.AuthMount == "" {
		cfg.AuthMount = cfg.Method
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			errs = append(errs, fmt.Errorf("vault: CA file: %w", err))
		} else if !pool.AppendCertsFromPEM(pem) {
			errs = append(errs, fmt.Errorf("vault: CA file %s holds no certificate", cfg.CAFile))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		// A redirect could carry the token elsewhere; none is followed.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	if cfg.Metrics == nil {
		cfg.Metrics = noMetrics{}
	}

	return &Vault{cfg: cfg, client: client, now: time.Now}, nil
}

// Material implements Provider: the material at ref and version, or at the current version when version is empty.
// A read refused as forbidden logs in afresh and is retried once.
func (v *Vault) Material(ctx context.Context, ref, version string) ([]byte, string, error) {
	if version != "" {
		if n, err := strconv.ParseUint(version, 10, 64); err != nil || n == 0 {
			return nil, "", &ProviderError{Provider: ProviderVault, Op: "read", Reason: "malformed"}
		}
	}
	token, err := v.currentToken(ctx)
	if err != nil {
		return nil, "", err
	}
	m, got, err := v.read(ctx, token, ref, version)
	var pe *ProviderError
	if errors.As(err, &pe) && pe.Reason == "auth" {
		if token, err = v.relogin(ctx, token); err != nil {
			return nil, "", err
		}
		m, got, err = v.read(ctx, token, ref, version)
	}

	return m, got, err
}

// currentToken returns a token, logging in when there is none and renewing it, or replacing it, past two thirds of its TTL.
func (v *Vault) currentToken(ctx context.Context) (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch {
	case v.token == "":
		return v.loginLocked(ctx)
	case v.renewAt.IsZero() || v.now().Before(v.renewAt):
		return v.token, nil
	case !v.renewable:
		return v.loginLocked(ctx)
	}
	g, err := v.auth(ctx, "renew", "auth/token/renew-self", v.token, struct{}{})
	// A renewal that fails, or whose TTL has shrunk below a third of the login's because the maximum TTL is near, is replaced by a login.
	if err != nil || g.ttl < v.loginTTL/3 {
		return v.loginLocked(ctx)
	}
	v.renewable = g.renewable
	v.setRenewAt(g.ttl)

	return v.token, nil
}

// relogin replaces stale, the token a read was refused with, unless another caller has replaced it already;
// a replacement that failed left no token, so this caller logs in itself.
func (v *Vault) relogin(ctx context.Context, stale string) (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.token != stale && v.token != "" {
		return v.token, nil
	}

	return v.loginLocked(ctx)
}

func (v *Vault) loginLocked(ctx context.Context) (string, error) {
	v.token = ""
	var body map[string]string
	switch v.cfg.Method {
	case VaultKubernetes:
		jwt, err := os.ReadFile(v.cfg.TokenFile)
		if err != nil {
			return "", &ProviderError{Provider: ProviderVault, Op: "login", Reason: "auth"}
		}
		body = map[string]string{"role": v.cfg.Role, "jwt": strings.TrimSpace(string(jwt))}
	default:
		body = map[string]string{"role_id": v.cfg.RoleID, "secret_id": v.cfg.SecretID}
	}
	g, err := v.auth(ctx, "login", "auth/"+v.cfg.AuthMount+"/login", "", body)
	if err != nil {
		return "", err
	}
	v.token, v.loginTTL, v.renewable = g.token, g.ttl, g.renewable
	v.setRenewAt(g.ttl)

	return g.token, nil
}

// setRenewAt schedules the renewal at two thirds of ttl, and records the token's expiry; a token without a TTL is never renewed.
func (v *Vault) setRenewAt(ttl time.Duration) {
	v.renewAt = time.Time{}
	if ttl > 0 {
		now := v.now()
		v.renewAt = now.Add(ttl * 2 / 3)
		v.cfg.Metrics.CredentialExpiry(ProviderVault, now.Add(ttl))
	}
}

// auth runs a login or a renewal and returns the token, its TTL and whether it renews;
// the call is recorded once its response is validated, so a malformed answer counts as a failure.
func (v *Vault) auth(ctx context.Context, op, path, token string, body any) (grant, error) {
	start := time.Now()
	data, err := v.call(ctx, op, http.MethodPost, path, token, body)
	var g grant
	if err == nil {
		g, err = parseAuth(op, data)
	}
	reason := ""
	var pe *ProviderError
	if errors.As(err, &pe) {
		reason = pe.Reason
	}
	v.cfg.Metrics.ProviderCall(ProviderVault, op, reason, time.Since(start))

	return g, err
}

// grant is a login's or a renewal's token, its TTL and whether it renews.
type grant struct {
	token     string
	ttl       time.Duration
	renewable bool
}

// parseAuth reads a login's or a renewal's response: its token, the token's TTL and whether it renews.
// Its error holds the operation and a reason, never the response.
func parseAuth(op string, data []byte) (grant, error) {
	var out authResponse
	if json.Unmarshal(data, &out) != nil || out.Auth == nil || out.Auth.ClientToken == "" ||
		out.Auth.LeaseDuration < 0 || out.Auth.LeaseDuration > maxLease {
		return grant{}, &ProviderError{Provider: ProviderVault, Op: op, Reason: "malformed"}
	}

	return grant{token: out.Auth.ClientToken, ttl: time.Duration(out.Auth.LeaseDuration) * time.Second, renewable: out.Auth.Renewable}, nil
}

// maxLease bounds a token's TTL, in seconds, so no lease overflows a time.Duration: a century.
const maxLease = 100 * 366 * 24 * 3600

// read returns the material at ref and version and the version read; a deleted or destroyed version is missing, never another one.
// The keys module records the read.
func (v *Vault) read(ctx context.Context, token, ref, version string) ([]byte, string, error) {
	segs := strings.Split(strings.Trim(ref, "/"), "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	path := v.cfg.Mount + "/data/" + strings.Join(segs, "/")
	if version != "" {
		path += "?version=" + version
	}
	data, err := v.call(ctx, "read", http.MethodGet, path, token, nil)
	if err != nil {
		return nil, "", err
	}

	return parseKV(data, version)
}

// parseKV reads a KV v2 read's response: the material field's material and the version read, which must be version when one was asked for.
// A deleted or destroyed version is missing; its error holds a reason, never the response.
func parseKV(data []byte, version string) ([]byte, string, error) {
	missing := &ProviderError{Provider: ProviderVault, Op: "read", Reason: "missing"}
	malformed := &ProviderError{Provider: ProviderVault, Op: "read", Reason: "malformed"}
	var out kvResponse
	if json.Unmarshal(data, &out) != nil || out.Data == nil {
		return nil, "", malformed
	}
	md := out.Data.Metadata
	if out.Data.Data == nil || md.Destroyed || md.DeletionTime != "" {
		return nil, "", missing
	}
	got := strconv.FormatUint(md.Version, 10)
	if md.Version == 0 || (version != "" && got != version) {
		return nil, "", malformed
	}
	text, ok := out.Data.Data["material"].(string)
	if !ok {
		return nil, "", malformed
	}
	m, err := decode(text)
	if err != nil {
		return nil, "", malformed
	}

	return m, got, nil
}

// call sends one request under the call deadline and returns a 2xx response's body.
// Its error holds the operation, a reason and the status, never the body.
func (v *Vault) call(ctx context.Context, op, method, path, token string, body any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, v.cfg.Timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: "malformed"}
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(v.cfg.Address, "/")+"/v1/"+path, rd)
	if err != nil {
		return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: "malformed"}
	}
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: transportReason(ctx)}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxVaultBody+1))
	if err != nil {
		return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: transportReason(ctx), Status: resp.StatusCode}
	}
	switch s := resp.StatusCode; {
	case s == http.StatusUnauthorized || s == http.StatusForbidden:
		return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: "auth", Status: s}
	case s == http.StatusNotFound:
		return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: "missing", Status: s}
	case s < 200 || s >= 300:
		return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: "server", Status: s}
	case len(data) > maxVaultBody:
		return nil, &ProviderError{Provider: ProviderVault, Op: op, Reason: "malformed", Status: s}
	}

	return data, nil
}

// transportReason is a failed exchange's reason: deadline when the call's deadline passed, else unreachable.
func transportReason(ctx context.Context) string {
	if ctx.Err() != nil {
		return "deadline"
	}

	return "unreachable"
}

// authResponse is a login's or a renewal's response.
type authResponse struct {
	Auth *authToken `json:"auth"`
}

type authToken struct {
	ClientToken   string `json:"client_token"`
	LeaseDuration int64  `json:"lease_duration"`
	Renewable     bool   `json:"renewable"`
}

// kvResponse is a KV v2 read's response.
type kvResponse struct {
	Data *kvSecret `json:"data"`
}

type kvSecret struct {
	Data     map[string]any `json:"data"`
	Metadata kvMetadata     `json:"metadata"`
}

type kvMetadata struct {
	Version      uint64 `json:"version"`
	DeletionTime string `json:"deletion_time"`
	Destroyed    bool   `json:"destroyed"`
}
