// Package devidp is tracekeep dev's test IdP (api.md §10.3 *Development*):
// an OpenID provider for one client that signs in whoever asks, with the subject and roles they choose,
// by the authorization code flow with PKCE that people's sign-in uses.
// Only tracekeep dev and tests import it; it must never be reachable from another host.
package devidp

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// Lifetimes of a code and of an ID token.
const (
	codeLifetime  = time.Minute
	tokenLifetime = 10 * time.Minute
)

// Config is the IdP's one client.
type Config struct {
	// Issuer is the IdP's https base, as the service and the browser reach it.
	Issuer string
	// ClientID, ClientSecret and RedirectURL are the one client it accepts, and the one place it sends codes.
	ClientID, ClientSecret, RedirectURL string
	// RolesClaim names the ID token's claim holding the chosen roles.
	RolesClaim string
	// Roles are offered on the authorization page; DefaultSubject and DefaultRoles are preselected.
	Roles          []string
	DefaultSubject string
	DefaultRoles   []string
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// Provider is the test IdP: it serves discovery, its JWKS, the authorization page and the token endpoint.
type Provider struct {
	cfg    Config
	key    *ecdsa.PrivateKey
	kid    string
	signer jose.Signer

	mu    sync.Mutex
	codes map[string]grant
}

// grant is what an issued code stands for.
type grant struct {
	challenge, nonce, subject string
	roles                     []string
	expiry                    time.Time
}

// New returns the IdP of cfg with a fresh ES256 signing key.
func New(cfg Config) (*Provider, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("devidp: %w", err)
	}
	kid := rand.Text()[:16]
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader(jose.HeaderKey("kid"), kid))
	if err != nil {
		return nil, fmt.Errorf("devidp: %w", err)
	}

	return &Provider{cfg: cfg, key: key, kid: kid, signer: signer, codes: map[string]grant{}}, nil
}

// Handler serves the IdP's endpoints under its issuer's path.
func (p *Provider) Handler() http.Handler {
	base := ""
	if u, err := url.Parse(p.cfg.Issuer); err == nil {
		base = strings.TrimSuffix(u.Path, "/")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+base+"/.well-known/openid-configuration", p.discovery)
	mux.HandleFunc("GET "+base+"/jwks", p.jwks)
	mux.HandleFunc("GET "+base+"/authorize", p.authorizePage)
	mux.HandleFunc("POST "+base+"/authorize", p.authorize)
	mux.HandleFunc("POST "+base+"/token", p.token)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func (p *Provider) endpoint(path string) string { return strings.TrimSuffix(p.cfg.Issuer, "/") + path }

func (p *Provider) discovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer": p.cfg.Issuer, "jwks_uri": p.endpoint("/jwks"),
		"authorization_endpoint": p.endpoint("/authorize"), "token_endpoint": p.endpoint("/token"),
		"response_types_supported": []string{"code"}, "subject_types_supported": []string{"public"},
		"id_token_signing_alg_values_supported": []string{"ES256"}, "code_challenge_methods_supported": []string{"S256"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_basic"},
	})
}

func (p *Provider) jwks(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
		{Key: &p.key.PublicKey, KeyID: p.kid, Algorithm: string(jose.ES256), Use: "sig"},
	}})
}

// request is an authorization request's parameters, checked against the one client.
type request struct {
	State, Nonce, Challenge string
}

// check admits an authorization request of the one client, to its one redirect URL, by PKCE S256 with a state and a nonce.
func (p *Provider) check(q url.Values) (request, string) {
	switch {
	case q.Get("response_type") != "code":
		return request{}, "response_type must be code"
	case q.Get("client_id") != p.cfg.ClientID:
		return request{}, "unknown client"
	case q.Get("redirect_uri") != p.cfg.RedirectURL:
		return request{}, "unregistered redirect_uri"
	case !slices.Contains(strings.Fields(q.Get("scope")), "openid"):
		return request{}, "scope must hold openid"
	case q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "":
		return request{}, "PKCE S256 is required"
	case q.Get("state") == "" || q.Get("nonce") == "":
		return request{}, "state and nonce are required"
	}

	return request{State: q.Get("state"), Nonce: q.Get("nonce"), Challenge: q.Get("code_challenge")}, ""
}

var page = template.Must(template.New("authorize").Parse(`<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>tracekeep dev sign-in</title></head>
<body style="font-family: sans-serif; max-width: 32rem; margin: 3rem auto">
<h1>tracekeep dev sign-in</h1>
<p>A test IdP: whoever reaches it signs in as the subject and roles chosen here.</p>
<form method="post">
{{range $k, $v := .Hidden}}<input type="hidden" name="{{$k}}" value="{{$v}}">
{{end}}<p><label>Subject <input name="subject" value="{{.Subject}}" required></label></p>
<fieldset><legend>Roles</legend>
{{range .Roles}}<label><input type="checkbox" name="role" value="{{.Name}}"{{if .Checked}} checked{{end}}> {{.Name}}</label><br>
{{end}}</fieldset>
<p><button type="submit">Sign in</button></p>
</form></body></html>
`))

type roleOption struct {
	Name    string
	Checked bool
}

func (p *Provider) authorizePage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if _, why := p.check(q); why != "" {
		http.Error(w, "Refused: "+why+".", http.StatusBadRequest)
		return
	}
	hidden := map[string]string{}
	for _, k := range []string{"response_type", "client_id", "redirect_uri", "scope", "state", "nonce", "code_challenge", "code_challenge_method"} {
		hidden[k] = q.Get(k)
	}
	var roles []roleOption
	for _, name := range p.cfg.Roles {
		roles = append(roles, roleOption{Name: name, Checked: slices.Contains(p.cfg.DefaultRoles, name)})
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// A browser applies form-action to the redirect that answers the form too, so it names the redirect URL's origin.
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; style-src 'unsafe-inline'; form-action 'self' "+origin(p.cfg.RedirectURL)+"; frame-ancestors 'none'")
	_ = page.Execute(w, map[string]any{"Hidden": hidden, "Subject": p.cfg.DefaultSubject, "Roles": roles})
}

// authorize signs in the chosen subject with the chosen roles and sends the browser back with a code and the state.
func (p *Provider) authorize(w http.ResponseWriter, r *http.Request) {
	if o := r.Header.Get("Origin"); o != "" && o != origin(p.cfg.Issuer) {
		http.Error(w, "Refused: another origin.", http.StatusForbidden)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Refused: a form that does not parse.", http.StatusBadRequest)
		return
	}
	req, why := p.check(r.PostForm)
	subject := strings.TrimSpace(r.PostForm.Get("subject"))
	if why == "" && subject == "" {
		why = "a subject is required"
	}
	var roles []string
	for _, role := range r.PostForm["role"] {
		if !slices.Contains(p.cfg.Roles, role) {
			why = "unknown role " + role
		}
		roles = append(roles, role)
	}
	if why != "" {
		http.Error(w, "Refused: "+why+".", http.StatusBadRequest)
		return
	}
	code := rand.Text() + rand.Text()
	p.mu.Lock()
	now := p.cfg.Now()
	for c, g := range p.codes {
		if !now.Before(g.expiry) {
			delete(p.codes, c)
		}
	}
	p.codes[code] = grant{challenge: req.Challenge, nonce: req.Nonce, subject: subject, roles: roles, expiry: now.Add(codeLifetime)}
	p.mu.Unlock()
	back, _ := url.Parse(p.cfg.RedirectURL)
	v := back.Query()
	v.Set("code", code)
	v.Set("state", req.State)
	back.RawQuery = v.Encode()
	http.Redirect(w, r, back.String(), http.StatusFound)
}

// token exchanges a code once, within its lifetime, for the client by HTTP Basic, with the redirect URL and the PKCE verifier.
func (p *Provider) token(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	if ok {
		id, _ = url.QueryUnescape(id)
		secret, _ = url.QueryUnescape(secret)
	}
	if !ok || id != p.cfg.ClientID || subtle.ConstantTimeCompare([]byte(secret), []byte(p.cfg.ClientSecret)) != 1 {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_client"})
		return
	}
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "authorization_code" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type"})
		return
	}
	code := r.PostForm.Get("code")
	p.mu.Lock()
	g, found := p.codes[code]
	delete(p.codes, code) // a code is spent by its first exchange, whatever the outcome
	p.mu.Unlock()
	now := p.cfg.Now()
	sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	if !found || !now.Before(g.expiry) || r.PostForm.Get("redirect_uri") != p.cfg.RedirectURL ||
		subtle.ConstantTimeCompare([]byte(challenge), []byte(g.challenge)) != 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant"})
		return
	}
	claims := jwt.Claims{
		Issuer: p.cfg.Issuer, Subject: g.subject, Audience: jwt.Audience{p.cfg.ClientID},
		IssuedAt: jwt.NewNumericDate(now), Expiry: jwt.NewNumericDate(now.Add(tokenLifetime)),
	}
	roles := g.roles
	if roles == nil {
		roles = []string{}
	}
	raw, err := jwt.Signed(p.signer).Claims(claims).Claims(map[string]any{"nonce": g.nonce, p.cfg.RolesClaim: roles}).Serialize()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "server_error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": rand.Text(), "token_type": "Bearer", "expires_in": int(tokenLifetime / time.Second), "id_token": raw,
	})
}

// origin is the scheme and host of an absolute URL, as a browser's Origin header names it.
func origin(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}

	return u.Scheme + "://" + u.Host
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
