package auth

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// The people's client at the test IdP; the secret is a sentinel no log, error or answer may hold.
const (
	peopleClient = "tracekeep-web"
	peopleSecret = "client-secret-sentinel-4b1d"
)

// people is a sign-in over p's IdP and c's clock, with one session key id.
func (p *idp) people(c *clock) *People {
	v := p.verifier(c)
	return NewPeople(PeopleConfig{
		ClientID: peopleClient, ClientSecret: peopleSecret, PublicURL: "https://tk.example.invalid",
		RolesClaim: "groups", Roles: map[string][]string{"tk-viewers": {RoleViewer}, "tk-export": {RoleExporter}},
		SessionLifetime: 8 * time.Hour, LoginLifetime: 10 * time.Minute, ExchangeTimeout: 10 * time.Second,
		Client: p.srv.Client(), Now: c.Now,
	}, v, &Cookies{Keys: newSessionKeys("s-1", "s-1"), Leeway: time.Minute, Now: c.Now})
}

// idClaims are a valid ID token's claims for nonce at now.
func (p *idp) idClaims(now time.Time, nonce string) map[string]any {
	return map[string]any{
		"iss": p.srv.URL, "aud": peopleClient, "sub": "person-1", "exp": now.Add(5 * time.Minute).Unix(),
		"iat": now.Unix(), "nonce": nonce, "groups": []string{"tk-viewers", "unmapped"},
	}
}

func TestSignInExchangesTheCodeOnce(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	pp := p.people(c)
	l := Login{Verifier: "verifier-1", State: "state-1", Nonce: "nonce-1", Return: "/", Expiry: c.Now().Add(10 * time.Minute)}
	code := p.grantCode(l.Verifier, sign(t, s, p.idClaims(c.Now(), l.Nonce)))
	got, err := pp.signIn(t.Context(), code, l)
	if err != nil {
		t.Fatal(err)
	}
	if got.Subject != "person-1" || !slices.Equal(got.Roles, []string{RoleViewer}) || !got.Expiry.Equal(c.Now().Add(8*time.Hour)) {
		t.Errorf("session = %+v, want person-1 as viewer for 8 h", got)
	}
	// The IdP refuses a second exchange of the code, so a replayed callback signs nobody in.
	if _, err := pp.signIn(t.Context(), code, l); reason(err) != ReasonExchange {
		t.Errorf("a second exchange of the code = %v, want refused as exchange", err)
	}
}

func TestSignInRefusals(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	pp := p.people(c)
	l := Login{Verifier: "verifier-1", Nonce: "nonce-1", Expiry: c.Now().Add(10 * time.Minute)}
	with := func(edit func(map[string]any)) string {
		cl := p.idClaims(c.Now(), l.Nonce)
		edit(cl)
		return sign(t, s, cl)
	}
	for name, tc := range map[string]struct {
		code, want string
	}{
		"another nonce":         {p.grantCode(l.Verifier, with(func(m map[string]any) { m["nonce"] = "nonce-2" })), ReasonNonce},
		"no nonce":              {p.grantCode(l.Verifier, with(func(m map[string]any) { delete(m, "nonce") })), ReasonNonce},
		"the service audience":  {p.grantCode(l.Verifier, with(func(m map[string]any) { m["aud"] = audience })), ReasonAudience},
		"two audiences, no azp": {p.grantCode(l.Verifier, with(func(m map[string]any) { m["aud"] = []string{peopleClient, "other"} })), ReasonAudience},
		"another azp":           {p.grantCode(l.Verifier, with(func(m map[string]any) { m["azp"] = "other" })), ReasonAudience},
		"no subject":            {p.grantCode(l.Verifier, with(func(m map[string]any) { delete(m, "sub") })), ReasonMalformed},
		"another issuer":        {p.grantCode(l.Verifier, with(func(m map[string]any) { m["iss"] = "https://other.example.invalid" })), ReasonIssuer},
		"expired":               {p.grantCode(l.Verifier, with(func(m map[string]any) { m["exp"] = c.Now().Add(-time.Hour).Unix() })), ReasonExpired},
		"another verifier":      {p.grantCode("verifier-2", with(func(map[string]any) {})), ReasonExchange},
		"an unknown code":       {"code-unknown", ReasonExchange},
	} {
		_, err := pp.signIn(t.Context(), tc.code, l)
		if reason(err) != tc.want {
			t.Errorf("%s: signIn = %v, want refused as %s", name, err, tc.want)
		}
		if err != nil && (strings.Contains(err.Error(), peopleSecret) || strings.Contains(err.Error(), tc.code)) {
			t.Errorf("%s: the refusal %q holds the secret or the code", name, err)
		}
	}
	// Two audiences with azp the client are admitted.
	code := p.grantCode(l.Verifier, with(func(m map[string]any) { m["aud"] = []string{peopleClient, "other"}; m["azp"] = peopleClient }))
	if _, err := pp.signIn(t.Context(), code, l); err != nil {
		t.Errorf("two audiences with azp the client: %v", err)
	}
	// An answer without an ID token, or not JSON, is the exchange's refusal.
	for _, body := range []string{`{"access_token":"at"}`, "not json"} {
		p.set(func() { p.tokenBody = body })
		code := p.grantCode(l.Verifier, with(func(map[string]any) {}))
		if _, err := pp.signIn(t.Context(), code, l); reason(err) != ReasonExchange {
			t.Errorf("token answer %q: signIn = %v, want refused as exchange", body, err)
		}
	}
}

// A token endpoint that does not answer within the exchange's deadline refuses the sign-in as exchange.
func TestSignInExchangeDeadline(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	pp := p.people(c)
	pp.cfg.ExchangeTimeout = 50 * time.Millisecond
	release := make(chan struct{})
	p.set(func() { p.tokenBlock = release })
	defer close(release)
	l := Login{Verifier: "verifier-1", Nonce: "nonce-1", Expiry: c.Now().Add(10 * time.Minute)}
	code := p.grantCode(l.Verifier, sign(t, s, p.idClaims(c.Now(), l.Nonce)))
	if _, err := pp.signIn(t.Context(), code, l); reason(err) != ReasonExchange {
		t.Errorf("a stalled token endpoint: signIn = %v, want refused as exchange", err)
	}
}

// The endpoints come from discovery, lazily, https only, and keep serving from the cache while the IdP is down.
func TestEndpointsFromDiscovery(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	p.newKey(t, "k1")
	v := p.verifier(c)
	if n := p.discovery.Load(); n != 0 {
		t.Fatalf("construction made %d requests", n)
	}
	e, err := v.Endpoints(t.Context())
	if err != nil || e.Authorization != p.srv.URL+"/authorize" || e.Token != p.srv.URL+"/token" {
		t.Fatalf("Endpoints = %+v, %v", e, err)
	}
	p.down.Store(true)
	if _, err := v.Endpoints(t.Context()); err != nil {
		t.Errorf("Endpoints from the cache while the IdP is down: %v", err)
	}
	// Cold and down: idp-unavailable, without hanging.
	cold := p.verifier(newClock())
	if _, err := cold.Endpoints(t.Context()); reason(err) != ReasonIDPUnavailable {
		t.Errorf("cold Endpoints with the IdP down = %v, want idp-unavailable", err)
	}
	p.down.Store(false)
	for name, edit := range map[string]func(){
		"no token endpoint": func() { p.tokenEndpoint = "-" },
		"a plain-http one":  func() { p.tokenEndpoint = "http://idp.example.invalid/token" },
		"no authorization":  func() { p.authEndpoint = "-" },
		"an http one":       func() { p.authEndpoint = "http://idp.example.invalid/authorize" },
	} {
		p.set(func() { p.authEndpoint, p.tokenEndpoint = "", ""; edit() })
		if _, err := p.verifier(newClock()).Endpoints(t.Context()); reason(err) != ReasonIDPUnavailable {
			t.Errorf("%s: Endpoints = %v, want idp-unavailable", name, err)
		}
	}
	// Service tokens keep verifying from a discovery document without endpoints.
	p.set(func() { p.authEndpoint, p.tokenEndpoint = "-", "-" })
	sv := p.verifier(newClock())
	if _, err := sv.Verify(t.Context(), sign(t, p.newKey(t, "k2"), p.claims(newClock().Now()))); err != nil {
		t.Errorf("a service token with no endpoints discovered: %v", err)
	}
}

// An ID token never passes as a service token: one whose aud holds people's client id is refused, even with the service audience.
func TestIDTokenIsNoServiceToken(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	v := NewVerifier(OIDCConfig{
		Issuer: p.srv.URL, Audience: audience, PeopleClient: peopleClient, Client: p.srv.Client(),
		Roles: map[string][]string{"ve-generation": {RolePackReader}}, TTL: time.Hour, MinInterval: 10 * time.Second, Leeway: time.Minute, Now: c.Now,
	})
	cl := p.claims(c.Now())
	cl["aud"] = []string{audience, peopleClient}
	if _, err := v.Verify(t.Context(), sign(t, s, cl)); reason(err) != ReasonAudience {
		t.Errorf("a token for both audiences = %v, want refused as audience", err)
	}
	if _, err := v.Verify(t.Context(), sign(t, s, p.claims(c.Now()))); err != nil {
		t.Errorf("a service token: %v", err)
	}
}
