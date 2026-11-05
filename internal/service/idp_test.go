package service

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/devidp"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/metrics"
)

// The api role over HTTPS through an outage of the IdP (api.md §10.2 *People's sign-in*):
// started with the IdP down, a login is refused as unavailable, and so is the callback of a login another replica started,
// both counted on /metrics; the IdP back, a person signs in once the fetch interval has passed;
// down again, the session cookie still reads ListDevices while a new login's callback fails at the exchange;
// and the session key's file gone after start, the cookie is still accepted, its material resolved at start.
func TestAPIRoleThroughAnIdPOutage(t *testing.T) {
	t.Parallel()
	const public, client, secret = "https://tk.example.invalid", "tk-web", "people-secret"
	var provider atomic.Pointer[devidp.Provider]
	var down atomic.Bool
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		provider.Load().Handler().ServeHTTP(w, r)
	}))
	srv.StartTLS()
	t.Cleanup(srv.Close)
	p, err := devidp.New(devidp.Config{Issuer: srv.URL, ClientID: client, ClientSecret: secret, RedirectURL: public + "/auth/callback", RolesClaim: "groups", Roles: []string{"viewers"}})
	if err != nil {
		t.Fatal(err)
	}
	provider.Store(p)
	ca := filepath.Join(t.TempDir(), "idp-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	sessionKey := filepath.Join(t.TempDir(), "s-1")
	if err := os.WriteFile(sessionKey, []byte(base64.StdEncoding.EncodeToString([]byte(rand.Text()+rand.Text()))), 0o600); err != nil {
		t.Fatal(err)
	}
	const interval = 100 * time.Millisecond
	// replica is an api role signing people in at the IdP under the shared session key, with metrics of its own.
	replica := func() *Env {
		e := devEnv(t)
		e.Metrics = metrics.NewServer()
		e.Config.API.Listen = "127.0.0.1:0"
		e.Config.API.TLSCertFile, e.Config.API.TLSKeyFile = selfSigned(t)
		e.Config.API.PublicURL = public
		e.Config.Keys.Declared["s-1"] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: sessionKey, Kind: catalog.KindSession}
		e.Config.Keys.Session = config.KeyPurpose{Issuing: "s-1", Accepted: []string{"s-1"}}
		e.Config.Auth.OIDC = config.OIDC{Issuer: srv.URL, Audience: "tk-services", JWKSTTL: time.Hour, FetchInterval: interval, CAFile: ca}
		e.Config.Auth.People = config.People{ClientID: client, ClientSecret: secret, RolesClaim: "groups", Roles: map[string][]string{"viewers": {auth.RoleViewer}},
			SessionLifetime: 8 * time.Hour, LoginLifetime: 10 * time.Minute, ExchangeTimeout: 10 * time.Second}
		serveAPI(t, e)

		return e
	}
	tk := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // the test's own certificate
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: 30 * time.Second}
	metricsHold := func(e *Env, want ...string) {
		t.Helper()
		m := httptest.NewRecorder()
		e.Metrics.Handler().ServeHTTP(m, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
		for _, w := range want {
			if !strings.Contains(m.Body.String(), w) {
				t.Errorf("/metrics holds no %s", w)
			}
		}
	}

	// Started with the IdP down: the login is refused as unavailable, and /metrics counts it.
	down.Store(true)
	e := replica()
	base := "https://" + e.APIAddr()
	if resp := do(t, tk, http.MethodGet, base+"/auth/login", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a login with the IdP down at start = %d, want 503", resp.StatusCode)
	}
	metricsHold(e, `tracekeep_api_logins_total{outcome="unavailable",step="start"} 1`, `tracekeep_api_auth_failures_total{reason="idp-unavailable"} 1`)

	// The callback of a login another replica started while the IdP was up, reaching this replica with it down again:
	// this replica has never fetched from the IdP, so the callback is refused as unavailable, not as a failed exchange.
	down.Store(false)
	other := replica()
	started := do(t, tk, http.MethodGet, "https://"+other.APIAddr()+"/auth/login", nil)
	if started.StatusCode != http.StatusFound {
		t.Fatalf("a login on the other replica with the IdP up = %d, want 302", started.StatusCode)
	}
	authorized := authorize(t, p, started)
	// Within the fetch interval or past it, this replica has no endpoints: its fetch is refused or not yet due.
	down.Store(true)
	if resp := do(t, tk, http.MethodGet, base+authorized, func(r *http.Request) { r.AddCookie(cookie(started, auth.LoginCookie)) }); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a cold callback with the IdP down = %d, want 503", resp.StatusCode)
	}
	metricsHold(e, `tracekeep_api_logins_total{outcome="unavailable",step="callback"} 1`)

	// The IdP back: once the fetch interval has passed, a login reaches it and the person signs in.
	down.Store(false)
	var login *http.Response
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(interval) {
		if login = do(t, tk, http.MethodGet, base+"/auth/login", nil); login.StatusCode == http.StatusFound || time.Now().After(deadline) {
			break
		}
	}
	session := callback(t, tk, base, p, login)
	if session.StatusCode != http.StatusSeeOther || cookie(session, auth.SessionCookie) == nil {
		t.Fatalf("the sign-in once the IdP is back = %d, want 303 with a session cookie", session.StatusCode)
	}
	sc := cookie(session, auth.SessionCookie)

	// Down again: the session still reads ListDevices, and a new login's callback fails at the token endpoint.
	down.Store(true)
	listDevices := func(what string) {
		t.Helper()
		resp := do(t, tk, http.MethodPost, base+"/tracekeep.v1.MachineService/ListDevices", func(r *http.Request) {
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Origin", public)
			r.AddCookie(sc)
			r.Body = io.NopCloser(strings.NewReader(`{"selector":{"toolId":"SIM-TOOL","from":"2026-11-01T00:00:00Z","to":"2026-11-01T01:00:00Z"}}`))
		})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("ListDevices by the session cookie, %s = %d, want 200", what, resp.StatusCode)
		}
	}
	listDevices("the IdP down")
	login = do(t, tk, http.MethodGet, base+"/auth/login", nil)
	if login.StatusCode != http.StatusFound {
		t.Fatalf("a login from the cached endpoints with the IdP down = %d, want 302", login.StatusCode)
	}
	if resp := callback(t, tk, base, p, login); resp.StatusCode != http.StatusBadGateway {
		t.Errorf("a callback with the IdP down = %d, want 502, the exchange failed", resp.StatusCode)
	}

	// The session key's provider gone after start: the material resolved at start still opens the cookie.
	if err := os.Remove(sessionKey); err != nil {
		t.Fatal(err)
	}
	listDevices("the session key's file gone")
}

// serveAPI runs the api role of e until the test ends, once its listener is bound.
func serveAPI(t *testing.T, e *Env) {
	t.Helper()
	bound := make(chan struct{})
	e.apiBound = func() { close(bound) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- API(ctx, e) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done: //nolint:errcheck // the role's end, its result not checked here
		case <-time.After(30 * time.Second):
			t.Errorf("the api role did not stop")
		}
	})
	select {
	case <-bound:
	case err := <-done:
		t.Fatalf("API = %v", err)
	case <-time.After(30 * time.Second): // bounds a hung start only
		t.Fatalf("the api role never bound")
	}
}

// do sends one request, shaped by set, and returns its answer with the body read and closed.
func do(t *testing.T, c *http.Client, method, target string, set func(*http.Request)) *http.Response {
	t.Helper()
	r, err := http.NewRequestWithContext(t.Context(), method, target, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if set != nil {
		set(r)
	}
	resp, err := c.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	return resp
}

// authorize has the IdP's authorization page sign a viewer in for login, a redirect to it,
// and returns the path and query of the callback it sends the browser to.
func authorize(t *testing.T, p *devidp.Provider, login *http.Response) string {
	t.Helper()
	authz, err := url.Parse(login.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	form := authz.Query()
	form.Set("subject", "person-1")
	form.Set("role", "viewers")
	r := httptest.NewRequest(http.MethodPost, "/authorize", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	p.Handler().ServeHTTP(w, r)
	back, err := url.Parse(w.Header().Get("Location"))
	if err != nil || w.Code != http.StatusFound {
		t.Fatalf("the IdP's authorization = %d %s", w.Code, w.Body)
	}

	return back.Path + "?" + back.RawQuery
}

// callback authorizes login at the IdP and returns the api's answer to its callback, the login cookie sent.
func callback(t *testing.T, c *http.Client, base string, p *devidp.Provider, login *http.Response) *http.Response {
	t.Helper()

	return do(t, c, http.MethodGet, base+authorize(t, p, login), func(r *http.Request) { r.AddCookie(cookie(login, auth.LoginCookie)) })
}

// cookie is the cookie name resp sets, nil when it sets none.
func cookie(resp *http.Response, name string) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == name && c.MaxAge >= 0 {
			return c
		}
	}

	return nil
}

// The api role closes its verifier on every exit once it is built: a start failing after it cancels the fetch begun meanwhile,
// held at the IdP, and joins it, and a request after the role returned starts no fetch.
func TestAPIClosesItsVerifier(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 4)
	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		entered <- struct{}{}
		<-r.Context().Done() // held until its client gives up
		// No answer: one returned here can reach the client between its TLS close_notify and its close,
		// and the cancelled fetch would then read it as a malformed document.
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)
	ca := filepath.Join(t.TempDir(), "idp-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	e := devEnv(t)
	// No certificate: the start fails after the verifier is built.
	e.Config.API.TLSCertFile, e.Config.API.TLSKeyFile = "", ""
	e.Config.Auth.OIDC.Issuer, e.Config.Auth.OIDC.CAFile = srv.URL, ca
	enc := base64.RawURLEncoding.EncodeToString
	tok := enc([]byte(`{"alg":"ES256","kid":"k1"}`)) + "." + enc([]byte(`{}`)) + "." + enc([]byte("signature"))
	var v *auth.Verifier
	verified := make(chan error, 1)
	e.verifierBuilt = func(built *auth.Verifier) {
		v = built
		go func() {
			_, err := built.Verify(context.Background(), tok)
			verified <- err
		}()
		<-entered
	}
	if err := API(t.Context(), e); err == nil {
		t.Fatal("API started without its certificate")
	}
	// Cancelled by Close, the fetch failed unreachable, not by the client's timeout.
	var ae *auth.Error
	if err := <-verified; !errors.As(err, &ae) || ae.Reason != auth.ReasonIDPUnavailable || !strings.Contains(err.Error(), "unreachable") {
		t.Errorf("the request whose fetch was held = %v, want idp-unavailable, its fetch cancelled", err)
	}
	if _, err := v.Verify(t.Context(), tok); !errors.As(err, &ae) || ae.Reason != auth.ReasonIDPUnavailable {
		t.Errorf("a request after the role returned = %v, want idp-unavailable", err)
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("the IdP saw %d requests, want the one fetch begun before the role returned", n)
	}
}
