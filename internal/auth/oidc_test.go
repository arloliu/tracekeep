package auth

import (
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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

// idp is a synthetic IdP serving discovery and a JWKS of the keys it holds, over TLS.
type idp struct {
	srv  *httptest.Server
	mu   sync.Mutex
	keys []jose.JSONWebKey
	// issuer and jwksURI are what its discovery document names, the server's own unless set;
	// jwksBody, when set, is served in place of the key set.
	issuer, jwksURI, jwksBody string
	// redirectTo is where /redirect sends its client.
	redirectTo      string
	discovery, jwks atomic.Int32
	// down refuses every request; failJWKS refuses the JWKS once a blocked fetch is released;
	// stallDiscovery holds discovery unanswered until its client gives up.
	down, failJWKS, stallDiscovery atomic.Bool
	block, jwksEntered             chan struct{}
	// endpoints, when set, replace what discovery names for the authorization and token endpoints ("-" names none).
	authEndpoint, tokenEndpoint string
	// grants are the codes the token endpoint exchanges, each once; tokenEntered and tokenBlock hold an exchange.
	grants                   map[string]grant
	tokens                   atomic.Int32
	tokenEntered, tokenBlock chan struct{}
	tokenBody                string
}

// grant is what a code was issued for: the PKCE challenge and the ID token's claims.
type grant struct {
	challenge string
	idToken   string
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	p := &idp{}
	p.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		p.mu.Lock()
		iss, uri, body, to := cmp.Or(p.issuer, p.srv.URL), cmp.Or(p.jwksURI, p.srv.URL+"/jwks"), p.jwksBody, p.redirectTo
		authz, token := cmp.Or(p.authEndpoint, p.srv.URL+"/authorize"), cmp.Or(p.tokenEndpoint, p.srv.URL+"/token")
		set := jose.JSONWebKeySet{Keys: slices.Clone(p.keys)}
		p.mu.Unlock()
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			p.discovery.Add(1)
			if p.stallDiscovery.Load() {
				<-r.Context().Done()
				return
			}
			doc := map[string]string{"issuer": iss, "jwks_uri": uri}
			if authz != "-" {
				doc["authorization_endpoint"] = authz
			}
			if token != "-" {
				doc["token_endpoint"] = token
			}
			_ = json.NewEncoder(w).Encode(doc)
		case "/token":
			p.token(w, r)
		case "/redirect":
			http.Redirect(w, r, to, http.StatusFound)
		case "/jwks":
			p.jwks.Add(1)
			if p.jwksEntered != nil {
				p.jwksEntered <- struct{}{}
			}
			if p.block != nil {
				<-p.block
			}
			if p.failJWKS.Load() {
				http.Error(w, "failing", http.StatusInternalServerError)
				return
			}
			if body != "" {
				_, _ = io.WriteString(w, body)
				return
			}
			_ = json.NewEncoder(w).Encode(set)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.srv.Close)

	return p
}

func (p *idp) set(f func()) {
	p.mu.Lock()
	f()
	p.mu.Unlock()
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

func (p *idp) verifier(c *clock) *Verifier { return p.verifierWith(c, p.srv.Client()) }

func (p *idp) verifierWith(c *clock, client *http.Client) *Verifier {
	return NewVerifier(OIDCConfig{
		Issuer: p.srv.URL, Audience: audience, Client: client,
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

// fetches counts the fetches v ends, each sent on the channel it returns once published; v is closed with the test.
func fetches(t *testing.T, v *Verifier) chan struct{} {
	t.Helper()
	ch := make(chan struct{}, 64)
	v.onFetched = func() { ch <- struct{}{} }
	t.Cleanup(func() { _ = v.Close() })

	return ch
}

// attempted is when v last started a fetch, and whether one is in flight.
func attempted(v *Verifier) (time.Time, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.attempted, v.inflight != nil
}

// Cached keys verify through an outage until their TTL ends; with none cached a request is rejected as idp-unavailable,
// and the IdP's return is noticed once the minimum interval passed.
// Through the TTL's second half each request is answered from the cache and starts one fetch per minimum interval,
// none of whose failures extends the cache's expiry.
func TestVerifierThroughAnOutage(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	cold := p.verifier(c)
	warm := p.verifier(c)
	fetched := fetches(t, warm)
	if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Fatal(err)
	}
	<-fetched
	p.down.Store(true)
	if _, err := cold.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
		t.Errorf("cold Verify during the outage = %v, want idp-unavailable", err)
	}
	c.advance(30 * time.Minute)
	for range 3 {
		if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
			t.Errorf("warm Verify inside the TTL during the outage = %v", err)
		}
		started, _ := attempted(warm)
		if !started.Equal(c.Now()) {
			t.Fatalf("the request past half the TTL started no fetch: last at %v, now %v", started, c.Now())
		}
		<-fetched
		c.advance(5 * time.Second)
		if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
			t.Errorf("warm Verify inside the minimum interval = %v", err)
		}
		if again, inflight := attempted(warm); !again.Equal(started) || inflight {
			t.Errorf("a request inside the minimum interval started a fetch at %v", again)
		}
		c.advance(5 * time.Second)
	}
	// The failures left the expiry as the first fetch set it: an hour after it, never extended.
	c.advance(29*time.Minute + 30*time.Second)
	if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
		t.Errorf("Verify at the first fetch's TTL during the outage = %v, want idp-unavailable", err)
	}
	<-fetched
	c.advance(time.Minute)
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
	p.set(func() { p.issuer = "https://idp.example.invalid" })
	if _, err := p.verifier(c).Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable || p.jwks.Load() != 0 {
		t.Errorf("Verify against a discovery naming another issuer = %v after %d JWKS fetches; want idp-unavailable and none", err, p.jwks.Load())
	}
}

// Keys come over https only: a discovery naming a plain-text JWKS, or a redirect to one, is a failed fetch,
// though the plain-text server would serve the keys.
func TestKeysOverHTTPSOnly(t *testing.T) {
	t.Parallel()
	for _, redirect := range []bool{false, true} {
		p, c := newIDP(t), newClock()
		s := p.newKey(t, "k1")
		plain := httptest.NewServer(p.srv.Config.Handler)
		t.Cleanup(plain.Close)
		p.set(func() {
			p.jwksURI, p.redirectTo = plain.URL+"/jwks", plain.URL+"/jwks"
			if redirect {
				p.jwksURI = p.srv.URL + "/redirect"
			}
		})
		if _, err := p.verifier(c).Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable || p.jwks.Load() != 0 {
			t.Errorf("redirect %v: Verify = %v after %d JWKS fetches; want idp-unavailable and none", redirect, err, p.jwks.Load())
		}
	}
	plain := NewVerifier(OIDCConfig{Issuer: "http://idp.example.invalid", Audience: audience, TTL: time.Hour, MinInterval: time.Second})
	p, c := newIDP(t), newClock()
	if _, err := plain.Verify(t.Context(), sign(t, p.newKey(t, "k1"), p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
		t.Errorf("Verify against a plain-text issuer = %v, want idp-unavailable", err)
	}
}

// A JWKS without a usable key is a failed fetch: a warm cache keeps verifying with its keys, and a cold one refuses as idp-unavailable.
func TestMalformedJWKSKeepsTheCache(t *testing.T) {
	t.Parallel()
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ed, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	set := func(k jose.JSONWebKey) string {
		b, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{k}})
		if err != nil {
			t.Fatal(err)
		}

		return string(b)
	}
	for _, body := range []string{
		"{}", "null", `{"keys":null}`, `{"keys":[]}`, `{"keys":[{"kty":"oct","k":"c2VjcmV0"}]}`,
		set(jose.JSONWebKey{Key: &ec.PublicKey, Use: "sig"}),                                 // no kid
		set(jose.JSONWebKey{Key: ed, KeyID: "k1", Use: "sig"}),                               // EdDSA, outside the allowlist
		set(jose.JSONWebKey{Key: &ec.PublicKey, KeyID: "k1", Algorithm: string(jose.HS256)}), // an alg outside the allowlist
		set(jose.JSONWebKey{Key: &ec.PublicKey, KeyID: "k1", Algorithm: string(jose.RS256)}), // an allowed alg the key's type cannot verify
		set(jose.JSONWebKey{Key: &ec.PublicKey, KeyID: "k1", Algorithm: string(jose.ES384)}), // an EC alg of another curve
		set(jose.JSONWebKey{Key: &rsaKey.PublicKey, KeyID: "k1", Algorithm: string(jose.ES256)}),
	} {
		p, c := newIDP(t), newClock()
		s := p.newKey(t, "k1")
		warm := p.verifier(c)
		if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
			t.Fatal(err)
		}
		p.set(func() { p.jwksBody = body })
		c.advance(time.Minute)
		if _, err := warm.Verify(t.Context(), sign(t, signer{s.key, s.alg, "k9"}, p.claims(c.Now()))); reason(err) != ReasonUnknownKid || p.jwks.Load() != 2 {
			t.Errorf("%s: an unknown kid = %v after %d fetches; want unknown-kid after its refresh", body, err, p.jwks.Load())
		}
		if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
			t.Errorf("%s: the cached kid after a malformed refresh = %v", body, err)
		}
		// The failed refresh left the cache's TTL as the first fetch set it: an hour after that fetch, not after the refresh.
		c.advance(59*time.Minute + 30*time.Second)
		if _, err := warm.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
			t.Errorf("%s: the cached kid past the first fetch's TTL = %v, want idp-unavailable", body, err)
		}
		if _, err := p.verifier(c).Verify(t.Context(), sign(t, s, p.claims(c.Now()))); reason(err) != ReasonIDPUnavailable {
			t.Errorf("%s: a cold verifier = %v, want idp-unavailable", body, err)
		}
	}
}

// Waiters on a fetch that fails are refused as idp-unavailable once it ends, none starting a fetch of its own.
func TestWaitersOfAFailedFetch(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	p.block, p.jwksEntered = make(chan struct{}), make(chan struct{}, 8)
	p.failJWKS.Store(true)
	v := p.verifier(c)
	waiting := make(chan struct{}, 8)
	v.onWait = func() { waiting <- struct{}{} }
	tok := sign(t, s, p.claims(c.Now()))
	const n = 4
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
	close(p.block)
	for range n {
		if err := <-errs; reason(err) != ReasonIDPUnavailable {
			t.Errorf("Verify = %v, want idp-unavailable", err)
		}
	}
	if j := p.jwks.Load(); j != 1 {
		t.Errorf("%d JWKS fetches, want 1", j)
	}
}

// A cancelled waiter returns at once without ending the fetch, and a cancelled requester's fetch still serves the others.
func TestCancelledRequestsLeaveTheFetch(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	p.block, p.jwksEntered = make(chan struct{}), make(chan struct{}, 8)
	v := p.verifier(c)
	waiting := make(chan struct{}, 8)
	v.onWait = func() { waiting <- struct{}{} }
	tok := sign(t, s, p.claims(c.Now()))
	starter, cancelStarter := context.WithCancel(t.Context())
	starterErr := make(chan error, 1)
	go func() {
		_, err := v.Verify(starter, tok)
		starterErr <- err
	}()
	<-p.jwksEntered
	cancelStarter()
	live := make(chan error, 1)
	go func() {
		_, err := v.Verify(t.Context(), tok)
		live <- err
	}()
	<-waiting
	gone, cancelGone := context.WithCancel(t.Context())
	goneErr := make(chan error, 1)
	go func() {
		_, err := v.Verify(gone, tok)
		goneErr <- err
	}()
	<-waiting
	cancelGone()
	if err := <-goneErr; reason(err) != ReasonIDPUnavailable {
		t.Errorf("a cancelled waiter = %v, want idp-unavailable before the fetch ends", err)
	}
	close(p.block)
	if err := <-live; err != nil {
		t.Errorf("a waiter on a cancelled requester's fetch = %v", err)
	}
	if err := <-starterErr; err != nil {
		t.Errorf("the cancelled requester = %v, its fetch completing", err)
	}
	if j := p.jwks.Load(); j != 1 {
		t.Errorf("%d JWKS fetches, want 1", j)
	}
}

// panicky is a transport that panics while armed.
type panicky struct {
	rt    http.RoundTripper
	armed atomic.Bool
}

func (p *panicky) RoundTrip(r *http.Request) (*http.Response, error) {
	if p.armed.Load() {
		panic("transport")
	}

	return p.rt.RoundTrip(r)
}

// A fetch that panics still ends: a later request, past the minimum interval, fetches again rather than waiting forever.
func TestPanickingFetchEnds(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	tr := &panicky{rt: p.srv.Client().Transport}
	tr.armed.Store(true)
	v := p.verifierWith(c, &http.Client{Transport: tr})
	tok := sign(t, s, p.claims(c.Now()))
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the armed transport did not panic")
			}
		}()
		_, _ = v.Verify(t.Context(), tok)
	}()
	tr.armed.Store(false)
	c.advance(10 * time.Second)
	done := make(chan error, 1)
	go func() {
		_, err := v.Verify(t.Context(), tok)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Verify after the panicking fetch = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Verify after the panicking fetch waits forever on it")
	}
}

// token is the token endpoint: a POST of an authorization code grant from the client, its code exchanged once,
// its verifier matching the code's S256 challenge.
func (p *idp) token(w http.ResponseWriter, r *http.Request) {
	p.tokens.Add(1)
	p.mu.Lock()
	entered, block := p.tokenEntered, p.tokenBlock
	p.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		case <-r.Context().Done():
			return
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-r.Context().Done():
			return
		}
	}
	id, secret, ok := r.BasicAuth()
	if r.Method != http.MethodPost || !ok || id != peopleClient || secret != peopleSecret || r.PostFormValue("grant_type") != "authorization_code" {
		http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
		return
	}
	p.mu.Lock()
	g, found := p.grants[r.PostFormValue("code")]
	delete(p.grants, r.PostFormValue("code"))
	body := p.tokenBody
	p.mu.Unlock()
	sum := sha256.Sum256([]byte(r.PostFormValue("code_verifier")))
	if !found || base64.RawURLEncoding.EncodeToString(sum[:]) != g.challenge {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
		return
	}
	if body != "" {
		_, _ = io.WriteString(w, body)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "at-sentinel", "id_token": g.idToken, "token_type": "Bearer"})
}

// grantCode issues a code for verifier's challenge, which exchanges once for idToken.
func (p *idp) grantCode(verifier, idToken string) string {
	sum := sha256.Sum256([]byte(verifier))
	code := "code-" + rand.Text()
	p.mu.Lock()
	if p.grants == nil {
		p.grants = map[string]grant{}
	}
	p.grants[code] = grant{challenge: base64.RawURLEncoding.EncodeToString(sum[:]), idToken: idToken}
	p.mu.Unlock()

	return code
}

// within waits for ch, failing the test after a bound that only a hung run reaches.
func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not happen", what)
		var zero T

		return zero
	}
}

// grantChallenge issues a code for a login's S256 challenge, which exchanges once for idToken.
func (p *idp) grantChallenge(challenge, idToken string) string {
	code := "code-" + rand.Text()
	p.mu.Lock()
	if p.grants == nil {
		p.grants = map[string]grant{}
	}
	p.grants[code] = grant{challenge: challenge, idToken: idToken}
	p.mu.Unlock()

	return code
}

// lasting are a valid token's claims at now that stay valid for two hours, across the clock moves of a test.
func (p *idp) lasting(now time.Time) map[string]any {
	m := p.claims(now)
	m["exp"] = now.Add(2 * time.Hour).Unix()

	return m
}

// A request past half the TTL is answered from the cache while the fetch it started in the background is held at the IdP,
// and another request meanwhile starts none; the fetch, once done, moves the expiry an hour past itself.
func TestBackgroundRefreshPastHalfTheTTL(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	v := p.verifier(c)
	fetched := fetches(t, v)
	tok := sign(t, s, p.lasting(c.Now()))
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	<-fetched
	c.advance(29 * time.Minute)
	if _, err := v.Verify(t.Context(), tok); err != nil || p.jwks.Load() != 1 {
		t.Fatalf("Verify before half the TTL = %v after %d fetches, want the cache alone", err, p.jwks.Load())
	}
	p.block, p.jwksEntered = make(chan struct{}), make(chan struct{}, 8)
	c.advance(2 * time.Minute)
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatalf("Verify past half the TTL = %v, want the cache's answer", err)
	}
	<-p.jwksEntered
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Errorf("Verify while the background fetch is held = %v", err)
	}
	close(p.block)
	<-fetched
	if j := p.jwks.Load(); j != 2 {
		t.Errorf("%d JWKS fetches, want the one background fetch beside the first", j)
	}
	// Past the first fetch's TTL, before half the second's: the cache answers and nothing is fetched.
	c.advance(29*time.Minute + 30*time.Second)
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Errorf("Verify past the first fetch's TTL = %v, want the refreshed keys", err)
	}
	if _, inflight := attempted(v); inflight || p.jwks.Load() != 2 {
		t.Errorf("a fetch started before half the refreshed TTL (%d fetches)", p.jwks.Load())
	}
}

// A request for an unknown kid, and a cold one whose cache has expired, share the background fetch in flight.
func TestRequestsShareTheBackgroundFetch(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	v := p.verifier(c)
	fetched := fetches(t, v)
	known := sign(t, s, p.lasting(c.Now()))
	if _, err := v.Verify(t.Context(), known); err != nil {
		t.Fatal(err)
	}
	<-fetched
	rotated := sign(t, p.newKey(t, "k2"), p.lasting(c.Now()))
	waiting := make(chan struct{}, 8)
	v.onWait = func() { waiting <- struct{}{} }
	p.block, p.jwksEntered = make(chan struct{}), make(chan struct{}, 8)
	c.advance(31 * time.Minute)
	if _, err := v.Verify(t.Context(), known); err != nil {
		t.Fatal(err)
	}
	<-p.jwksEntered
	errs := make(chan error, 2)
	go func() {
		_, err := v.Verify(t.Context(), rotated)
		errs <- err
	}()
	<-waiting
	c.advance(30 * time.Minute) // past the TTL: the next request is cold
	go func() {
		_, err := v.Verify(t.Context(), known)
		errs <- err
	}()
	<-waiting
	close(p.block)
	for range 2 {
		if err := <-errs; err != nil {
			t.Errorf("a request sharing the background fetch = %v", err)
		}
	}
	if j := p.jwks.Load(); j != 2 {
		t.Errorf("%d JWKS fetches, want the background one shared", j)
	}
}

// Close cancels the fetch held at the IdP and returns once it ended; a request after it starts none, and a second Close returns at once.
func TestCloseEndsTheFetch(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	v := p.verifier(c)
	fetched := fetches(t, v)
	tok := sign(t, s, p.lasting(c.Now()))
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	<-fetched
	p.block, p.jwksEntered = make(chan struct{}), make(chan struct{}, 8)
	t.Cleanup(func() { close(p.block) })
	c.advance(31 * time.Minute)
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	<-p.jwksEntered
	if err := v.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	select {
	case <-fetched:
	default:
		t.Error("Close returned before the fetch it cancelled ended")
	}
	last, _ := attempted(v)
	c.advance(time.Minute)
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Errorf("Verify from the cache after Close = %v", err)
	}
	c.advance(time.Hour)
	if _, err := v.Verify(t.Context(), tok); reason(err) != ReasonIDPUnavailable {
		t.Errorf("Verify past the TTL after Close = %v, want idp-unavailable", err)
	}
	if again, inflight := attempted(v); !again.Equal(last) || inflight || p.jwks.Load() != 2 {
		t.Errorf("a request after Close started a fetch (%d fetches)", p.jwks.Load())
	}
	if err := v.Close(); err != nil {
		t.Errorf("a second Close = %v", err)
	}
}

// A refresh racing Close, and two Closes racing each other, leave no fetch in flight once every call returned.
func TestRefreshRacingClose(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	v := p.verifier(c)
	tok := sign(t, s, p.lasting(c.Now()))
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	c.advance(31 * time.Minute)
	var wg sync.WaitGroup
	wg.Go(func() { _, _ = v.Verify(t.Context(), tok) })
	wg.Go(func() { _ = v.Close() })
	wg.Go(func() { _ = v.Close() })
	wg.Wait()
	if err := v.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	if _, inflight := attempted(v); inflight {
		t.Error("a fetch is in flight after Close returned")
	}
}

// A failed fetch is logged by the fields the verifier builds: the endpoint's scheme, host and path, the HTTP status and a class;
// the user information and query of the JWKS URL discovery names appear neither in the line nor in the refusal.
func TestFetchFailureLogged(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		class, status string
		setup         func(p *idp)
		timeout       time.Duration
	}{
		{class: "status", status: `"status":500`, setup: func(p *idp) { p.failJWKS.Store(true) }},
		{class: "malformed", setup: func(p *idp) { p.jwksBody = "not json" }},
		{class: "timeout", setup: func(p *idp) { p.block = make(chan struct{}) }, timeout: time.Second},
	} {
		t.Run(tc.class, func(t *testing.T) {
			t.Parallel()
			p, c := newIDP(t), newClock()
			s := p.newKey(t, "k1")
			host := p.srv.Listener.Addr().String()
			p.set(func() { p.jwksURI = "https://user-sentinel:pw-sentinel@" + host + "/jwks?sig=query-sentinel" })
			tc.setup(p)
			if p.block != nil {
				t.Cleanup(func() { close(p.block) })
			}
			client := p.srv.Client()
			client.Timeout = tc.timeout
			v := p.verifierWith(c, client)
			var buf syncBuffer
			v.cfg.Log = slog.New(slog.NewJSONHandler(&buf, nil))
			_, err := v.Verify(t.Context(), sign(t, s, p.claims(c.Now())))
			if reason(err) != ReasonIDPUnavailable {
				t.Fatalf("Verify = %v, want idp-unavailable", err)
			}
			line := buf.String()
			if strings.Count(line, "\n") != 1 || !strings.Contains(line, `"class":"`+tc.class+`"`) || !strings.Contains(line, `"path":"/jwks"`) ||
				!strings.Contains(line, `"scheme":"https"`) || !strings.Contains(line, `"host":"`+host+`"`) || !strings.Contains(line, tc.status) {
				t.Errorf("logged %q, want one line with the class %s, the endpoint and %s", line, tc.class, tc.status)
			}
			for _, leak := range []string{"sentinel", "sig="} {
				if strings.Contains(line, leak) || strings.Contains(err.Error(), leak) {
					t.Errorf("%q leaks into the line %q or the refusal %q", leak, line, err)
				}
			}
		})
	}
}

// A background fetch that panics is recovered and logged as one error line without the panic's value.
func TestBackgroundPanicRecovered(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	tr := &panicky{rt: p.srv.Client().Transport}
	v := p.verifierWith(c, &http.Client{Transport: tr})
	var buf syncBuffer
	v.cfg.Log = slog.New(slog.NewJSONHandler(&buf, nil))
	fetched := fetches(t, v)
	tok := sign(t, s, p.lasting(c.Now()))
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	<-fetched
	tr.armed.Store(true)
	c.advance(31 * time.Minute)
	if _, err := v.Verify(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	<-fetched
	if err := v.Close(); err != nil { // waits for the background fetch, its recovery included
		t.Fatal(err)
	}
	if line := buf.String(); strings.Count(line, "\n") != 1 || !strings.Contains(line, `"level":"ERROR"`) || strings.Contains(line, "transport") {
		t.Errorf("logged %q, want one error line without the panic's value", line)
	}
}

// syncBuffer is a buffer a log may write to while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}
