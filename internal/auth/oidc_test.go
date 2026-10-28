package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const audience = "tracekeep"

// clock is a settable time.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: time.Date(2026, 10, 28, 8, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// idp is a synthetic IdP serving discovery and a JWKS of the keys it holds.
type idp struct {
	srv                *httptest.Server
	mu                 sync.Mutex
	keys               []jose.JSONWebKey
	issuer             string // the issuer its discovery document names; the server's URL unless set
	discovery, jwks    atomic.Int32
	down               atomic.Bool
	block, jwksEntered chan struct{}
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	p := &idp{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			p.discovery.Add(1)
			p.mu.Lock()
			iss := p.issuer
			p.mu.Unlock()
			if iss == "" {
				iss = p.srv.URL
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": iss, "jwks_uri": p.srv.URL + "/jwks"})
		case "/jwks":
			p.jwks.Add(1)
			if p.jwksEntered != nil {
				p.jwksEntered <- struct{}{}
			}
			if p.block != nil {
				<-p.block
			}
			p.mu.Lock()
			set := jose.JSONWebKeySet{Keys: slices.Clone(p.keys)}
			p.mu.Unlock()
			_ = json.NewEncoder(w).Encode(set)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.srv.Close)

	return p
}

// signer is a private key the IdP publishes under kid.
type signer struct {
	key any
	alg jose.SignatureAlgorithm
	kid string
}

func (p *idp) newKey(t *testing.T, kid string) signer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p.publish(kid, &k.PublicKey, jose.ES256)

	return signer{key: k, alg: jose.ES256, kid: kid}
}

func (p *idp) publish(kid string, pub any, alg jose.SignatureAlgorithm) {
	p.mu.Lock()
	p.keys = append(p.keys, jose.JSONWebKey{Key: pub, KeyID: kid, Algorithm: string(alg), Use: "sig"})
	p.mu.Unlock()
}

// claims are a valid token's claims at now.
func (p *idp) claims(now time.Time) map[string]any {
	return map[string]any{
		"iss": p.srv.URL, "aud": audience, "sub": "ve-generator", "exp": now.Add(5 * time.Minute).Unix(),
		"iat": now.Unix(), "roles": []string{"ve-generation"},
	}
}

func sign(t *testing.T, s signer, claims map[string]any) string {
	t.Helper()
	var key any = jose.JSONWebKey{Key: s.key, KeyID: s.kid}
	if s.kid == "" {
		key = s.key
	}
	sg, err := jose.NewSigner(jose.SigningKey{Algorithm: s.alg, Key: key}, (&jose.SignerOptions{}).WithType("JWT"))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwt.Signed(sg).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}

	return tok
}

func (p *idp) verifier(c *clock) *Verifier {
	return NewVerifier(OIDCConfig{
		Issuer: p.srv.URL, Audience: audience,
		Roles: map[string][]string{"ve-generation": {RolePackReader}, "scoring": {RoleScorer}},
		TTL:   time.Hour, MinInterval: 10 * time.Second, Leeway: time.Minute, Now: c.Now,
	})
}

func reason(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}

	return ""
}

func TestVerifierFetchesOnFirstUse(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	v := p.verifier(c)
	if n := p.discovery.Load() + p.jwks.Load(); n != 0 {
		t.Fatalf("construction made %d requests", n)
	}
	got, err := v.Verify(t.Context(), sign(t, s, p.claims(c.Now())))
	if err != nil || got.Subject != "ve-generator" || !slices.Equal(got.Roles, []string{RolePackReader}) || got.Via != ViaOIDC {
		t.Fatalf("Verify = %+v, %v", got, err)
	}
	if _, err := v.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Fatal(err)
	}
	if d, j := p.discovery.Load(), p.jwks.Load(); d != 1 || j != 1 {
		t.Errorf("requests: %d discoveries and %d JWKS fetches, want one of each, the second token from the cache", d, j)
	}
}

func TestVerifierChecks(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	stranger := newIDP(t).newKey(t, "k1") // the same kid under a key the IdP never published
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p.publish("r1", &rsaKey.PublicKey, jose.RS256)
	now := c.Now()
	with := func(k string, v any) map[string]any {
		m := p.claims(now)
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}

		return m
	}
	v := p.verifier(c)
	for _, tc := range []struct {
		name, token, reason string
		roles               []string
	}{
		{"valid", sign(t, s, p.claims(now)), "", []string{RolePackReader}},
		{"rs256", sign(t, signer{rsaKey, jose.RS256, "r1"}, p.claims(now)), "", []string{RolePackReader}},
		{"roles as a string", sign(t, s, with("roles", "scoring")), "", []string{RoleScorer}},
		{"unmapped roles", sign(t, s, with("roles", []string{"other"})), "", nil},
		{"no roles claim", sign(t, s, with("roles", nil)), "", nil},
		{"expired within the leeway", sign(t, s, with("exp", now.Add(-30*time.Second).Unix())), "", []string{RolePackReader}},
		{"expired", sign(t, s, with("exp", now.Add(-2*time.Minute).Unix())), ReasonExpired, nil},
		{"no exp", sign(t, s, with("exp", nil)), ReasonMalformed, nil},
		{"not yet valid", sign(t, s, with("nbf", now.Add(2*time.Minute).Unix())), ReasonNotYetValid, nil},
		{"issued in the future", sign(t, s, with("iat", now.Add(2*time.Minute).Unix())), ReasonNotYetValid, nil},
		{"another issuer", sign(t, s, with("iss", "https://idp.example.invalid")), ReasonIssuer, nil},
		{"another audience", sign(t, s, with("aud", []string{"other"})), ReasonAudience, nil},
		{"one of its audiences", sign(t, s, with("aud", []string{"other", audience})), "", []string{RolePackReader}},
		{"a key it never published", sign(t, stranger, p.claims(now)), ReasonSignature, nil},
		{"a symmetric alg", sign(t, signer{[]byte("0123456789abcdef0123456789abcdef"), jose.HS256, "k1"}, p.claims(now)), ReasonAlg, nil},
		{"no kid", sign(t, signer{s.key, s.alg, ""}, p.claims(now)), ReasonMalformed, nil},
		{"garbage", "a.b.c", ReasonMalformed, nil},
	} {
		got, err := v.Verify(t.Context(), tc.token)
		if reason(err) != tc.reason || (tc.reason == "" && (err != nil || !slices.Equal(got.Roles, tc.roles))) {
			t.Errorf("%s: Verify = %+v, %v; want reason %q, roles %v", tc.name, got, err, tc.reason, tc.roles)
		}
	}
}

// An unknown kid refreshes the JWKS once, when the minimum interval allows, and is then rejected.
func TestUnknownKidRefreshesOnce(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	v := p.verifier(c)
	if _, err := v.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Minute)
	rotated := p.newKey(t, "k2")
	if _, err := v.Verify(t.Context(), sign(t, rotated, p.claims(c.Now()))); err != nil {
		t.Fatalf("Verify of a rotated kid = %v, want it found by one refresh", err)
	}
	if j := p.jwks.Load(); j != 2 {
		t.Errorf("%d JWKS fetches, want 2", j)
	}
	c.advance(time.Minute)
	bogus := signer{s.key, s.alg, "k9"}
	if _, err := v.Verify(t.Context(), sign(t, bogus, p.claims(c.Now()))); reason(err) != ReasonUnknownKid {
		t.Errorf("Verify of an unknown kid = %v, want unknown-kid", err)
	}
	if j := p.jwks.Load(); j != 3 {
		t.Errorf("%d JWKS fetches, want the one refresh, 3", j)
	}
	// Inside the minimum interval an unknown kid fetches nothing, so a flood of them cannot drive the IdP.
	for range 5 {
		if _, err := v.Verify(t.Context(), sign(t, bogus, p.claims(c.Now()))); reason(err) != ReasonUnknownKid {
			t.Errorf("Verify of an unknown kid again = %v, want unknown-kid", err)
		}
	}
	if j := p.jwks.Load(); j != 3 {
		t.Errorf("%d JWKS fetches after the flood, want still 3", j)
	}
	if _, err := v.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Errorf("a known kid after the flood = %v", err)
	}
}

// Concurrent requests needing the JWKS wait on the one fetch in flight.
func TestOneFetchInFlight(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	p.block, p.jwksEntered = make(chan struct{}), make(chan struct{}, 8)
	v := p.verifier(c)
	waiting := make(chan struct{}, 8)
	v.onWait = func() { waiting <- struct{}{} }
	tok := sign(t, s, p.claims(c.Now()))
	const n = 8
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := v.Verify(t.Context(), tok)
			errs <- err
		}()
	}
	<-p.jwksEntered
	for range n - 1 {
		<-waiting
	}
	if j := p.jwks.Load(); j != 1 {
		t.Errorf("%d JWKS fetches in flight, want 1", j)
	}
	close(p.block)
	for range n {
		if err := <-errs; err != nil {
			t.Errorf("Verify = %v", err)
		}
	}
	if j := p.jwks.Load(); j != 1 {
		t.Errorf("%d JWKS fetches, want 1", j)
	}
}

// Cached keys verify through an outage until their TTL ends; with none cached a request is rejected as idp-unavailable,
// and the IdP's return is noticed once the minimum interval passed.
func TestVerifierThroughAnOutage(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	cold := p.verifier(c)
	warm := p.verifier(c)
	if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Fatal(err)
	}
	p.down.Store(true)
	if _, err := cold.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
		t.Errorf("cold Verify during the outage = %v, want idp-unavailable", err)
	}
	c.advance(59 * time.Minute)
	if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Errorf("warm Verify inside the TTL during the outage = %v", err)
	}
	c.advance(2 * time.Minute)
	if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
		t.Errorf("Verify past the TTL during the outage = %v, want idp-unavailable", err)
	}
	p.down.Store(false)
	if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
		t.Errorf("Verify inside the minimum interval after the outage = %v, want idp-unavailable, no fetch yet", err)
	}
	c.advance(10 * time.Second)
	if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Errorf("Verify after the outage = %v", err)
	}
}

func TestDiscoveryOfAnotherIssuer(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	p.issuer = "https://idp.example.invalid"
	if _, err := p.verifier(c).Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable || p.jwks.Load() != 0 {
		t.Errorf("Verify against a discovery naming another issuer = %v after %d JWKS fetches; want idp-unavailable and none", err, p.jwks.Load())
	}
}
