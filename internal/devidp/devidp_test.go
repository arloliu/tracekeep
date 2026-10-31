package devidp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const (
	issuer   = "https://127.0.0.1:8444"
	client   = "tk-web"
	secret   = "s3cret"
	redirect = "https://127.0.0.1:8080/auth/callback"
	verifier = "a-verifier-of-enough-length-for-the-test"
)

// clock is a settable clock.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newProvider(t *testing.T) (*Provider, *clock) {
	t.Helper()
	c := &clock{now: time.Unix(1_800_000_000, 0)}
	p, err := New(Config{
		Issuer: issuer, ClientID: client, ClientSecret: secret, RedirectURL: redirect, RolesClaim: "roles",
		Roles: []string{"viewer", "exporter"}, DefaultSubject: "dev", DefaultRoles: []string{"viewer"}, Now: c.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	return p, c
}

func challenge(v string) string {
	sum := sha256.Sum256([]byte(v))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// authorization is a valid authorization request's parameters.
func authorization() url.Values {
	return url.Values{
		"response_type": {"code"}, "client_id": {client}, "redirect_uri": {redirect}, "scope": {"openid profile"},
		"state": {"st"}, "nonce": {"nc"}, "code_challenge": {challenge(verifier)}, "code_challenge_method": {"S256"},
	}
}

func serve(p *Provider, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	p.Handler().ServeHTTP(w, r)

	return w
}

// signIn posts the authorization form for subject with roles and returns the code the redirect carries.
func signIn(t *testing.T, p *Provider, subject string, roles ...string) string {
	t.Helper()
	form := authorization()
	form.Set("subject", subject)
	form["role"] = roles
	r := httptest.NewRequest(http.MethodPost, issuer+"/authorize", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := serve(p, r)
	if w.Code != http.StatusFound {
		t.Fatalf("authorize = %d %s, want a redirect", w.Code, w.Body)
	}
	loc, err := url.Parse(w.Header().Get("Location"))
	if err != nil || !strings.HasPrefix(loc.String(), redirect+"?") || loc.Query().Get("state") != "st" {
		t.Fatalf("authorize redirected to %v, want the redirect URL with the state", loc)
	}

	return loc.Query().Get("code")
}

// exchange posts the token request and returns the status and the decoded answer.
func exchange(t *testing.T, p *Provider, id, sec string, form url.Values) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, issuer+"/token", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetBasicAuth(url.QueryEscape(id), url.QueryEscape(sec))
	w := serve(p, r)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("token answer %q: %v", w.Body, err)
	}

	return w.Code, body
}

func tokenForm(code string) url.Values {
	return url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {redirect}, "code_verifier": {verifier}}
}

// A code is exchanged once for an ES256 ID token naming the chosen subject and roles, the nonce and the client,
// verified against the published key; the same code again is refused.
func TestCodeExchangedOnceForTheIDToken(t *testing.T) {
	t.Parallel()
	p, c := newProvider(t)
	code := signIn(t, p, "alice", "viewer", "exporter")
	status, body := exchange(t, p, client, secret, tokenForm(code))
	if status != http.StatusOK {
		t.Fatalf("token = %d %v", status, body)
	}
	raw, _ := body["id_token"].(string)
	tok, err := jwt.ParseSigned(raw, []jose.SignatureAlgorithm{jose.ES256})
	if err != nil {
		t.Fatal(err)
	}
	var keys jose.JSONWebKeySet
	w := serve(p, httptest.NewRequest(http.MethodGet, issuer+"/jwks", nil))
	if err := json.Unmarshal(w.Body.Bytes(), &keys); err != nil || len(keys.Keys) != 1 {
		t.Fatalf("jwks = %s, %v", w.Body, err)
	}
	if tok.Headers[0].KeyID != keys.Keys[0].KeyID {
		t.Errorf("kid %q, want the published %q", tok.Headers[0].KeyID, keys.Keys[0].KeyID)
	}
	var std jwt.Claims
	var extra struct {
		Nonce string   `json:"nonce"`
		Roles []string `json:"roles"`
	}
	if err := tok.Claims(keys.Keys[0].Key, &std, &extra); err != nil {
		t.Fatal(err)
	}
	if err := std.ValidateWithLeeway(jwt.Expected{Issuer: issuer, AnyAudience: jwt.Audience{client}, Time: c.now}, 0); err != nil {
		t.Error(err)
	}
	if std.Subject != "alice" || extra.Nonce != "nc" || strings.Join(extra.Roles, ",") != "viewer,exporter" || std.IssuedAt == nil {
		t.Errorf("claims = %+v %+v", std, extra)
	}
	if status, _ := exchange(t, p, client, secret, tokenForm(code)); status != http.StatusBadRequest {
		t.Errorf("the code again = %d, want refused", status)
	}
}

// A token request is refused for another client or secret, another verifier, another redirect URL, or a code past its minute;
// a refused exchange spends its code.
func TestTokenRefusals(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		id, secret string
		edit       func(url.Values)
		advance    time.Duration
		status     int
	}{
		"another secret":       {client, "other", nil, 0, http.StatusUnauthorized},
		"another client":       {"other", secret, nil, 0, http.StatusUnauthorized},
		"another verifier":     {client, secret, func(f url.Values) { f.Set("code_verifier", "not-it") }, 0, http.StatusBadRequest},
		"another redirect URL": {client, secret, func(f url.Values) { f.Set("redirect_uri", "https://127.0.0.1:1/cb") }, 0, http.StatusBadRequest},
		"a code past its life": {client, secret, nil, time.Minute, http.StatusBadRequest},
		"another grant":        {client, secret, func(f url.Values) { f.Set("grant_type", "client_credentials") }, 0, http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, c := newProvider(t)
			code := signIn(t, p, "alice", "viewer")
			c.now = c.now.Add(tc.advance)
			form := tokenForm(code)
			if tc.edit != nil {
				tc.edit(form)
			}
			if status, body := exchange(t, p, tc.id, tc.secret, form); status != tc.status {
				t.Errorf("token = %d %v, want %d", status, body, tc.status)
			}
			if tc.status == http.StatusBadRequest && tc.advance == 0 && name != "another grant" {
				if status, _ := exchange(t, p, client, secret, tokenForm(code)); status != http.StatusBadRequest {
					t.Errorf("the code after a refused exchange = %d, want spent", status)
				}
			}
		})
	}
}

// The authorization page and form accept only the one client, its redirect URL, PKCE S256, a state and a nonce,
// known roles and a subject, and a form posted from no other origin.
func TestAuthorizationRefusals(t *testing.T) {
	t.Parallel()
	p, _ := newProvider(t)
	for name, edit := range map[string]func(url.Values){
		"another client":       func(q url.Values) { q.Set("client_id", "other") },
		"another redirect URL": func(q url.Values) { q.Set("redirect_uri", "https://evil.example.invalid/cb") },
		"no PKCE":              func(q url.Values) { q.Del("code_challenge") },
		"plain PKCE":           func(q url.Values) { q.Set("code_challenge_method", "plain") },
		"no nonce":             func(q url.Values) { q.Del("nonce") },
		"no openid":            func(q url.Values) { q.Set("scope", "profile") },
	} {
		q := authorization()
		edit(q)
		if w := serve(p, httptest.NewRequest(http.MethodGet, issuer+"/authorize?"+q.Encode(), nil)); w.Code != http.StatusBadRequest {
			t.Errorf("%s: the page = %d, want refused", name, w.Code)
		}
	}
	w := serve(p, httptest.NewRequest(http.MethodGet, issuer+"/authorize?"+authorization().Encode(), nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `value="dev"`) || !strings.Contains(w.Header().Get("Content-Security-Policy"), "form-action 'self' https://127.0.0.1:8080") {
		t.Errorf("the page = %d %v", w.Code, w.Header())
	}
	for name, tc := range map[string]struct {
		edit   func(url.Values)
		origin string
		status int
	}{
		"an unknown role":  {func(f url.Values) { f["role"] = []string{"admin"} }, "", http.StatusBadRequest},
		"no subject":       {func(f url.Values) { f.Set("subject", " ") }, "", http.StatusBadRequest},
		"another origin":   {nil, "https://evil.example.invalid", http.StatusForbidden},
		"another redirect": {func(f url.Values) { f.Set("redirect_uri", "https://evil.example.invalid/cb") }, "", http.StatusBadRequest},
	} {
		f := authorization()
		f.Set("subject", "alice")
		if tc.edit != nil {
			tc.edit(f)
		}
		r := httptest.NewRequest(http.MethodPost, issuer+"/authorize", strings.NewReader(f.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		if w := serve(p, r); w.Code != tc.status {
			t.Errorf("%s: the form = %d, want %d", name, w.Code, tc.status)
		}
	}
}
