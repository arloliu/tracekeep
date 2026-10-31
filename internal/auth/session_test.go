package auth

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// signedIn signs a person in at r through the callback and returns the session cookie's value.
func (p *idp) signedIn(t *testing.T, c *clock, r *replica) string {
	t.Helper()
	s := p.newKey(t, "k1")
	b := newBrowser()
	q := startLogin(t, b, r.people.Handler())
	code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
	if w := b.do(r.people.Handler(), http.MethodGet, "/auth/callback?code="+code+"&state="+q.Get("state"), nil); w.Code != http.StatusSeeOther {
		t.Fatalf("callback = %d %s", w.Code, w.Body)
	}

	return b.cookies[SessionCookie]
}

// cookieRequest is a request carrying session as its cookie, from origin when given.
func cookieRequest(method, origin, session string) *http.Request {
	r := httptest.NewRequest(method, "/tracekeep.v1.MachineService/ListCaptures", http.NoBody)
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	r.AddCookie(&http.Cookie{Name: SessionCookie, Value: session})

	return r
}

// A session cookie admits a same-origin POST of the browser's operations, under the person's roles, and nothing else.
func TestSessionCookieCredential(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	r := p.replica(c, newSessionKeys("s-1", "s-1"), nil)
	session := p.signedIn(t, c, r)
	f := &failures{}
	var logs bytes.Buffer
	a := NewAuthenticator(AuthenticatorConfig{People: r.people, Failures: f, Log: slog.New(slog.NewTextHandler(&logs, nil))})
	got, err := a.Check(cookieRequest(http.MethodPost, publicURL, session), OpListCaptures)
	if err != nil || got.Subject != "person-1" || got.Via != ViaSession {
		t.Fatalf("Check(ListCaptures) = %+v, %v; want person-1 by session", got, err)
	}
	for name, tc := range map[string]struct {
		r          *http.Request
		op, reason string
	}{
		"a role the person lacks":     {cookieRequest(http.MethodPost, publicURL, session), OpListPacks, ReasonForbidden},
		"PackURL":                     {cookieRequest(http.MethodPost, publicURL, session), OpPackURL, ReasonSessionScope},
		"Restore":                     {cookieRequest(http.MethodPost, publicURL, session), OpRestore, ReasonSessionScope},
		"Digest":                      {cookieRequest(http.MethodPost, publicURL, session), OpDigest, ReasonSessionScope},
		"Admin":                       {cookieRequest(http.MethodPost, publicURL, session), OpAdmin, ReasonSessionScope},
		"Export":                      {cookieRequest(http.MethodPost, publicURL, session), OpExport, ReasonSessionScope},
		"a GET":                       {cookieRequest(http.MethodGet, publicURL, session), OpListCaptures, ReasonCSRF},
		"no Origin":                   {cookieRequest(http.MethodPost, "", session), OpListCaptures, ReasonCSRF},
		"another origin":              {cookieRequest(http.MethodPost, "https://evil.example.invalid", session), OpListCaptures, ReasonCSRF},
		"another scheme":              {cookieRequest(http.MethodPost, "http://tk.example.invalid", session), OpListCaptures, ReasonCSRF},
		"a null Origin":               {cookieRequest(http.MethodPost, "null", session), OpListCaptures, ReasonCSRF},
		"a cookie that does not open": {cookieRequest(http.MethodPost, publicURL, session[:len(session)-2]+"AA"), OpListCaptures, ReasonSession},
	} {
		if _, err := a.Check(tc.r, tc.op); reason(err) != tc.reason {
			t.Errorf("%s: Check = %v, want refused as %s", name, err, tc.reason)
		}
	}
	// One credential, no fallback: a refused bearer token beside a valid cookie refuses the request.
	withBearer := cookieRequest(http.MethodPost, publicURL, session)
	withBearer.Header.Set("Authorization", "Bearer not-a-token")
	if p, err := a.Check(withBearer, OpListCaptures); err == nil || p.Via == ViaSession {
		t.Errorf("a refused bearer token beside a valid cookie = %+v, %v; want refused", p, err)
	}
	if !strings.Contains(logs.String(), ReasonCSRF) || strings.Contains(logs.String(), session) || strings.Contains(logs.String(), session[:len(session)-2]) {
		t.Errorf("the refusals' log names no reason, or holds the cookie:\n%s", logs.String())
	}
	// A header present but empty is still the chosen credential: no fallback to the cookie.
	empty := cookieRequest(http.MethodPost, publicURL, session)
	empty.Header["Authorization"] = []string{""}
	if p, err := a.Check(empty, OpListCaptures); reason(err) != ReasonMalformed || p.Via == ViaSession {
		t.Errorf("an empty Authorization header beside a valid cookie = %+v, %v; want refused as malformed", p, err)
	}
	// Without people's sign-in no cookie is read.
	none := NewAuthenticator(AuthenticatorConfig{})
	if _, err := none.Check(cookieRequest(http.MethodPost, publicURL, session), OpListCaptures); reason(err) != ReasonMissing {
		t.Errorf("a cookie with no sign-in configured = %v, want missing", err)
	}
}

// A verified client certificate is chosen before a session cookie.
func TestCertificateBeforeCookie(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	r := p.replica(c, newSessionKeys("s-1", "s-1"), nil)
	session := p.signedIn(t, c, r)
	a := NewAuthenticator(AuthenticatorConfig{MTLS: map[string][]string{generatorURI: {RolePackReader}}, People: r.people})
	leaf, err := x509.ParseCertificate(newCA(t).client(t, []string{generatorURI}, nil).Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	req := cookieRequest(http.MethodPost, publicURL, session)
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
	if got, err := a.Authenticate(req); err != nil || got.Via != ViaMTLS {
		t.Errorf("a certificate and a cookie = %+v, %v; want the certificate's principal", got, err)
	}
}

// CSRF is checked before the cookie is opened: a refused method or Origin asks for no key, whatever the cookie holds.
func TestCSRFBeforeTheCookieOpens(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	keys := newSessionKeys("s-1", "s-1")
	r := p.replica(c, keys, nil)
	a := NewAuthenticator(AuthenticatorConfig{People: r.people})
	for name, req := range map[string]*http.Request{
		"a GET with a cookie that does not open":          cookieRequest(http.MethodGet, publicURL, "AQIAA3MtMQ"),
		"another origin with a cookie that does not open": cookieRequest(http.MethodPost, "https://evil.example.invalid", "AQIAA3MtMQ"),
	} {
		if _, err := a.Check(req, OpListCaptures); reason(err) != ReasonCSRF {
			t.Errorf("%s: Check = %v, want csrf", name, err)
		}
	}
	keys.mu.Lock()
	defer keys.mu.Unlock()
	if len(keys.asked) != 0 {
		t.Errorf("keys asked for %v before the CSRF check refused", keys.asked)
	}
}

// Origins compare by scheme, host and effective port, a default port written or not.
func TestSameOrigin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		public, origin string
		want           bool
	}{
		{"https://tk.example.invalid", "https://tk.example.invalid", true},
		{"https://tk.example.invalid:443", "https://tk.example.invalid", true},
		{"https://tk.example.invalid/", "https://TK.example.invalid", true},
		{"https://tk.example.invalid:8443", "https://tk.example.invalid:8443", true},
		{"https://tk.example.invalid:8443", "https://tk.example.invalid", false},
		{"https://tk.example.invalid", "http://tk.example.invalid", false},
		{"https://tk.example.invalid", "https://tk.example.invalid.evil.example", false},
		{"https://tk.example.invalid", "null", false},
		{"https://tk.example.invalid", "", false},
	} {
		r := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		if got := SameOrigin(r, tc.public); got != tc.want {
			t.Errorf("SameOrigin(%q, public %q) = %t, want %t", tc.origin, tc.public, got, tc.want)
		}
	}
}
