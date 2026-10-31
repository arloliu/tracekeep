package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// The cookies of people's sign-in (api.md §10.2 *People's sign-in*).
const (
	LoginCookie   = "__Host-tk_login"
	SessionCookie = "__Host-tk_session"
)

// The steps and outcomes logins_total counts.
const (
	StepStart          = "start"
	StepCallback       = "callback"
	OutcomeOK          = "ok"
	OutcomeRefused     = "refused"
	OutcomeUnavailable = "unavailable"
)

// Logins counts each login started and each callback answered; *metrics.API is one.
type Logins interface {
	Login(step, outcome string)
}

// Handler serves people's sign-in under /auth/: login, callback, logout and the signed-in principal.
// Every answer carries Cache-Control: no-store.
func (p *People) Handler() http.Handler { return p.handler }

func (p *People) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /auth/login", p.login)
	mux.HandleFunc("GET /auth/callback", p.callback)
	mux.HandleFunc("POST /auth/logout", p.logout)
	mux.HandleFunc("GET /auth/me", p.me)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

// login starts a login: a PKCE verifier, a state and a nonce sealed in the login cookie, and a redirect to the IdP.
// It changes no server state.
func (p *People) login(w http.ResponseWriter, r *http.Request) {
	e, err := p.idp.Endpoints(r.Context())
	if err != nil {
		p.refuse(w, StepStart, err)
		return
	}
	authz, err := url.Parse(e.Authorization)
	if err != nil {
		p.refuse(w, StepStart, refuse(ReasonIDPUnavailable, errors.New("the authorization endpoint does not parse")))
		return
	}
	l := Login{
		Verifier: randomText(), State: randomText(), Nonce: randomText(),
		Return: ReturnPath(r.URL.Query().Get("return")), Expiry: p.cfg.Now().Add(p.cfg.LoginLifetime),
	}
	v, err := p.cookies.SealLogin(r.Context(), l)
	if err != nil {
		p.fail(w, StepStart, err)
		return
	}
	sum := sha256.Sum256([]byte(l.Verifier))
	q := authz.Query()
	q.Set("response_type", "code")
	q.Set("client_id", p.cfg.ClientID)
	q.Set("redirect_uri", p.redirectURL())
	q.Set("scope", p.scope())
	q.Set("state", l.State)
	q.Set("nonce", l.Nonce)
	q.Set("code_challenge", base64.RawURLEncoding.EncodeToString(sum[:]))
	q.Set("code_challenge_method", "S256")
	authz.RawQuery = q.Encode()
	http.SetCookie(w, p.cookie(LoginCookie, v, p.cfg.LoginLifetime))
	p.count(StepStart, OutcomeOK)
	http.Redirect(w, r, authz.String(), http.StatusFound)
}

// callback finishes a login: the login cookie, always cleared, must hold the state the IdP returns;
// the code is exchanged, the ID token verified, and the session cookie set.
func (p *People) callback(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.SetCookie(w, p.cookie(LoginCookie, "", -1))
	q := r.URL.Query()
	c, err := r.Cookie(LoginCookie)
	if err != nil {
		p.refuse(w, StepCallback, refuse(ReasonLogin, errors.New("no login cookie")))
		return
	}
	l, err := p.cookies.OpenLogin(r.Context(), c.Value)
	switch {
	case err != nil:
		p.refuse(w, StepCallback, refuse(ReasonLogin, err))
		return
	case q.Get("error") != "":
		p.refuse(w, StepCallback, refuse(ReasonLogin, errors.New("the IdP refused the login")))
		return
	case subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(l.State)) != 1:
		p.refuse(w, StepCallback, refuse(ReasonLogin, errors.New("another login's state")))
		return
	case q.Get("code") == "":
		p.refuse(w, StepCallback, refuse(ReasonLogin, errors.New("no code")))
		return
	}
	s, err := p.signIn(r.Context(), q.Get("code"), l)
	if err != nil {
		p.refuse(w, StepCallback, err)
		return
	}
	v, err := p.cookies.SealSession(r.Context(), s)
	if errors.Is(err, ErrCookieTooLong) {
		p.refuse(w, StepCallback, &Error{Reason: ReasonLogin, Subject: s.Subject, Err: err})
		return
	} else if err != nil {
		p.fail(w, StepCallback, err)
		return
	}
	http.SetCookie(w, p.cookie(SessionCookie, v, p.cfg.SessionLifetime))
	p.count(StepCallback, OutcomeOK)
	p.cfg.Log.Info("auth: signed in", "subject", s.Subject, "roles", s.Roles)
	// The return path was checked when the login began, and is sent as it was kept.
	w.Header().Set("Location", l.Return)
	w.WriteHeader(http.StatusSeeOther)
}

// logout clears the session cookie of a same-origin POST; it ends nothing at the IdP.
func (p *People) logout(w http.ResponseWriter, r *http.Request) {
	if !SameOrigin(r, p.cfg.PublicURL) {
		_ = p.refused(&Error{Reason: ReasonCSRF, Err: errors.New("a logout from another origin")})
		http.Error(w, "refused", http.StatusForbidden)
		return
	}
	http.SetCookie(w, p.cookie(SessionCookie, "", -1))
	w.WriteHeader(http.StatusNoContent)
}

// me answers the signed-in principal, or 401.
func (p *People) me(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(SessionCookie)
	if err != nil {
		http.Error(w, "not signed in", http.StatusUnauthorized)
		return
	}
	s, err := p.cookies.OpenSession(r.Context(), c.Value)
	if err != nil {
		_ = p.refused(&Error{Reason: ReasonSession, Err: err})
		http.Error(w, "not signed in", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"subject": s.Subject, "roles": s.Roles, "expiry": s.Expiry.UTC().Format(time.RFC3339)})
}

// Session returns the session r's session cookie holds, refused as session; it is the authenticator's cookie branch.
func (p *People) Session(r *http.Request) (Session, error) {
	c, err := r.Cookie(SessionCookie)
	if err != nil {
		return Session{}, refuse(ReasonMissing, errors.New("no credential"))
	}
	s, err := p.cookies.OpenSession(r.Context(), c.Value)
	if err != nil {
		return Session{}, refuse(ReasonSession, err)
	}

	return s, nil
}

// cookie is a cookie of people's sign-in: Secure, HttpOnly, SameSite=Lax, Path=/, no Domain; a negative maxAge clears it.
func (p *People) cookie(name, value string, maxAge time.Duration) *http.Cookie {
	c := &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode}
	if maxAge < 0 {
		c.MaxAge = -1
	} else {
		c.MaxAge = int(maxAge / time.Second)
	}

	return c
}

// scope is openid and the configured scopes, each once.
func (p *People) scope() string {
	scopes := []string{"openid"}
	for _, s := range p.cfg.Scopes {
		if s != "" && !strings.Contains(" "+strings.Join(scopes, " ")+" ", " "+s+" ") {
			scopes = append(scopes, s)
		}
	}

	return strings.Join(scopes, " ")
}

// refuse answers a refused login step with a fixed page naming its reason, counting and logging it.
func (p *People) refuse(w http.ResponseWriter, step string, err error) {
	e := p.refused(err)
	status, outcome := http.StatusBadRequest, OutcomeRefused
	switch e.Reason {
	case ReasonExchange:
		status = http.StatusBadGateway
	case ReasonIDPUnavailable:
		status, outcome = http.StatusServiceUnavailable, OutcomeUnavailable
	default: // the login's own refusals and the ID token's
	}
	p.count(step, outcome)
	http.Error(w, "Sign-in failed: "+e.Reason+".", status)
}

// fail answers a login step that failed on tracekeep's side, such as a session key it could not use.
func (p *People) fail(w http.ResponseWriter, step string, err error) {
	p.count(step, OutcomeUnavailable)
	p.cfg.Log.Error("auth: sign-in failed", "step", step, "error", err)
	http.Error(w, "Sign-in is unavailable.", http.StatusServiceUnavailable)
}

// refused counts and logs a refusal, with its subject when one is known and never a credential, and returns it as an *Error.
func (p *People) refused(err error) *Error {
	var e *Error
	if !errors.As(err, &e) {
		e = &Error{Reason: ReasonLogin, Err: err}
	}
	if p.cfg.Failures != nil {
		p.cfg.Failures.AuthFailure(e.Reason)
	}
	p.cfg.Log.Warn("auth: refused", "reason", e.Reason, "subject", e.Subject, "error", e.Err)

	return e
}

func (p *People) count(step, outcome string) {
	if p.cfg.Logins != nil {
		p.cfg.Logins.Login(step, outcome)
	}
}

// ReturnPath is raw when it is a path on this origin, and / otherwise (api.md §10.2 *People's sign-in*):
// printable ASCII without a space, a backslash or a control, starting with one / not followed by another,
// and parsing as a URL with no scheme and no host.
func ReturnPath(raw string) string {
	if len(raw) == 0 || len(raw) > 2048 || raw[0] != '/' || len(raw) > 1 && raw[1] == '/' {
		return "/"
	}
	for i := range len(raw) {
		if c := raw[i]; c <= ' ' || c > '~' || c == '\\' {
			return "/"
		}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "" || u.Host != "" || u.User != nil {
		return "/"
	}

	return raw
}

// SameOrigin reports whether r's Origin is publicURL's origin: its scheme, host and port.
func SameOrigin(r *http.Request, publicURL string) bool {
	o := r.Header.Get("Origin")
	u, err := url.Parse(publicURL)
	if o == "" || err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}

	return strings.EqualFold(o, u.Scheme+"://"+u.Host)
}

// randomText is 32 random bytes, base64url.
func randomText() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // never fails (crypto/rand)

	return base64.RawURLEncoding.EncodeToString(b)
}
