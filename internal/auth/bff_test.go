package auth

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const publicURL = "https://tk.example.invalid"

// browser carries the cookies a browser keeps between requests to one origin, across the replicas that serve it.
type browser struct {
	cookies map[string]string
}

func newBrowser() *browser { return &browser{cookies: map[string]string{}} }

// do sends a request to h with b's cookies and keeps the cookies the answer sets or clears.
func (b *browser) do(h http.Handler, method, target string, header http.Header) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, http.NoBody)
	for k, v := range header {
		r.Header[k] = v
	}
	for name, v := range b.cookies {
		r.AddCookie(&http.Cookie{Name: name, Value: v})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	for _, c := range w.Result().Cookies() {
		if c.MaxAge < 0 {
			delete(b.cookies, c.Name)
		} else {
			b.cookies[c.Name] = c.Value
		}
	}

	return w
}

// replica is one api replica's sign-in: its own verifier and cookies over the shared IdP and session keys.
type replica struct {
	people   *People
	failures *failureLog
	logins   *loginLog
}

type failureLog struct {
	mu      sync.Mutex
	reasons []string
}

func (f *failureLog) AuthFailure(reason string) {
	f.mu.Lock()
	f.reasons = append(f.reasons, reason)
	f.mu.Unlock()
}

type loginLog struct {
	mu    sync.Mutex
	steps []string
}

func (l *loginLog) Login(step, outcome string) {
	l.mu.Lock()
	l.steps = append(l.steps, step+"/"+outcome)
	l.mu.Unlock()
}

func (p *idp) replica(c *clock, keys *sessionKeys, log *slog.Logger) *replica {
	r := &replica{failures: &failureLog{}, logins: &loginLog{}}
	r.people = NewPeople(PeopleConfig{
		ClientID: peopleClient, ClientSecret: peopleSecret, PublicURL: publicURL, Scopes: []string{"groups"},
		RolesClaim: "groups", Roles: map[string][]string{"tk-viewers": {RoleViewer}},
		SessionLifetime: 8 * time.Hour, LoginLifetime: 10 * time.Minute, ExchangeTimeout: 10 * time.Second,
		Client: p.srv.Client(), Now: c.Now, Failures: r.failures, Logins: r.logins, Log: log,
	}, p.verifier(c), &Cookies{Keys: keys, Leeway: time.Minute, Now: c.Now})

	return r
}

// startLogin starts a login on h and returns the authorization request's parameters.
func startLogin(t *testing.T, b *browser, h http.Handler) url.Values {
	t.Helper()
	w := b.do(h, http.MethodGet, "/auth/login", nil)
	if w.Code != http.StatusFound {
		t.Fatalf("login = %d %s", w.Code, w.Body)
	}
	loc, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}

	return loc.Query()
}

func TestSignInAcrossReplicas(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	keys := newSessionKeys("s-1", "s-1")
	a, b := p.replica(c, keys, nil), p.replica(c, keys, nil)
	br := newBrowser()
	w := br.do(a.people.Handler(), http.MethodGet, "/auth/login?return=%2Fcaptures%3Ftool%3DT1", nil)
	if w.Code != http.StatusFound || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("login = %d, Cache-Control %q", w.Code, w.Header().Get("Cache-Control"))
	}
	checkLoginCookie(t, w.Result().Cookies()[0])
	q := checkAuthorization(t, w.Header().Get("Location"))
	// The callback reaches the other replica, which holds nothing of the login but its cookie.
	code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
	w = br.do(b.people.Handler(), http.MethodGet, "/auth/callback?code="+code+"&state="+q.Get("state"), nil)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/captures?tool=T1" || w.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("callback = %d to %q, %s", w.Code, w.Header().Get("Location"), w.Body)
	}
	if _, ok := br.cookies[LoginCookie]; ok {
		t.Error("the login cookie outlived the callback")
	}
	w = br.do(a.people.Handler(), http.MethodGet, "/auth/me", nil)
	var me struct {
		Subject string   `json:"subject"`
		Roles   []string `json:"roles"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &me); err != nil || me.Subject != "person-1" || !slices.Equal(me.Roles, []string{RoleViewer}) {
		t.Errorf("me = %d %s", w.Code, w.Body)
	}
	if !slices.Equal(b.logins.steps, []string{"callback/ok"}) || !slices.Equal(a.logins.steps, []string{"start/ok"}) {
		t.Errorf("logins counted %v on A and %v on B", a.logins.steps, b.logins.steps)
	}
}

// checkLoginCookie fails unless c is the login cookie with its flags and lifetime.
func checkLoginCookie(t *testing.T, c *http.Cookie) {
	t.Helper()
	if c.Name != LoginCookie || !c.Secure || !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || c.Domain != "" || c.MaxAge != 600 {
		t.Errorf("login cookie = %+v, want __Host- form, Secure, HttpOnly, Lax, Path /, no Domain, 10 min", c)
	}
}

// checkAuthorization fails unless location is the authorization request of a login, and returns its parameters.
func checkAuthorization(t *testing.T, location string) url.Values {
	t.Helper()
	loc, err := url.Parse(location)
	if err != nil {
		t.Fatal(err)
	}
	q := loc.Query()
	if loc.Path != "/authorize" || q.Get("response_type") != "code" || q.Get("client_id") != peopleClient ||
		q.Get("redirect_uri") != publicURL+"/auth/callback" || q.Get("scope") != "openid groups" || q.Get("code_challenge_method") != "S256" ||
		len(q.Get("state")) < 43 || len(q.Get("nonce")) < 43 || q.Get("code_challenge") == "" {
		t.Errorf("authorization request = %s", loc)
	}

	return q
}

// Logout clears the session of a same-origin POST, and refuses one from another origin or none, and a GET.
func TestLogout(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	a := p.replica(c, newSessionKeys("s-1", "s-1"), nil)
	br := newBrowser()
	q := startLogin(t, br, a.people.Handler())
	code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
	if w := br.do(a.people.Handler(), http.MethodGet, "/auth/callback?code="+code+"&state="+q.Get("state"), nil); w.Code != http.StatusSeeOther {
		t.Fatalf("callback = %d", w.Code)
	}
	// Logout: refused from another origin or none, done from the API's own.
	for _, origin := range []string{"", "https://evil.example.invalid", "http://tk.example.invalid"} {
		h := http.Header{}
		if origin != "" {
			h.Set("Origin", origin)
		}
		if w := br.do(a.people.Handler(), http.MethodPost, "/auth/logout", h); w.Code != http.StatusForbidden {
			t.Errorf("logout from %q = %d, want 403", origin, w.Code)
		}
	}
	if w := br.do(a.people.Handler(), http.MethodGet, "/auth/logout", nil); w.Code == http.StatusNoContent {
		t.Error("a GET logged out")
	}
	if w := br.do(a.people.Handler(), http.MethodPost, "/auth/logout", http.Header{"Origin": {publicURL}}); w.Code != http.StatusNoContent {
		t.Errorf("logout = %d", w.Code)
	}
	if w := br.do(a.people.Handler(), http.MethodGet, "/auth/me", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("me after logout = %d, want 401", w.Code)
	}
}

func TestCallbackRefusals(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	r := p.replica(c, newSessionKeys("s-1", "s-1"), nil)
	h := r.people.Handler()
	callback := func(edit func(q url.Values, b *browser) string) int {
		b := newBrowser()
		q := startLogin(t, b, h)
		code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
		target := edit(url.Values{"code": {code}, "state": {q.Get("state")}}, b)

		return b.do(h, http.MethodGet, target, nil).Code
	}
	for name, tc := range map[string]struct {
		edit func(q url.Values, b *browser) string
		want int
	}{
		"no login cookie": {func(q url.Values, b *browser) string {
			delete(b.cookies, LoginCookie)
			return "/auth/callback?" + q.Encode()
		}, 400},
		"another state": {func(q url.Values, _ *browser) string {
			q.Set("state", q.Get("state")+"x")
			return "/auth/callback?" + q.Encode()
		}, 400},
		"the IdP's error": {func(q url.Values, _ *browser) string {
			q.Set("error", "access_denied")
			return "/auth/callback?" + q.Encode()
		}, 400},
		"no code": {func(q url.Values, _ *browser) string { q.Del("code"); return "/auth/callback?" + q.Encode() }, 400},
		"another code": {func(q url.Values, _ *browser) string {
			q.Set("code", "code-unknown")
			return "/auth/callback?" + q.Encode()
		}, 502},
		"a swapped login cookie": {func(q url.Values, b *browser) string {
			other := newBrowser()
			startLogin(t, other, h)
			b.cookies[LoginCookie] = other.cookies[LoginCookie]
			return "/auth/callback?" + q.Encode()
		}, 400},
		"a session cookie as the login cookie": {func(q url.Values, b *browser) string {
			v, _ := r.people.cookies.SealSession(t.Context(), Session{Subject: "p", Expiry: c.Now().Add(time.Hour)})
			b.cookies[LoginCookie] = v
			return "/auth/callback?" + q.Encode()
		}, 400},
	} {
		if got := callback(tc.edit); got != tc.want {
			t.Errorf("%s: callback = %d, want %d", name, got, tc.want)
		}
	}
	// A login left open past its lifetime and the leeway is refused.
	// Its ID token is still valid, so only the login cookie's expiry refuses it, before any exchange.
	b := newBrowser()
	q := startLogin(t, b, h)
	claims := p.idClaims(c.Now(), q.Get("nonce"))
	claims["exp"] = c.Now().Add(time.Hour).Unix()
	code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, claims))
	late := p.replica(&clock{now: c.Now().Add(12 * time.Minute)}, newSessionKeys("s-1", "s-1"), nil)
	exchanges := p.tokens.Load()
	if w := b.do(late.people.Handler(), http.MethodGet, "/auth/callback?code="+code+"&state="+q.Get("state"), nil); w.Code != 400 ||
		!slices.Equal(late.failures.reasons, []string{ReasonLogin}) || p.tokens.Load() != exchanges {
		t.Errorf("an expired login: callback = %d, reasons %v, %d exchanges; want 400, login, none", w.Code, late.failures.reasons, p.tokens.Load()-exchanges)
	}
	// A replayed callback: the IdP refuses the code's second exchange.
	b = newBrowser()
	q = startLogin(t, b, h)
	code = p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
	target := "/auth/callback?code=" + code + "&state=" + q.Get("state")
	saved := b.cookies[LoginCookie]
	if w := b.do(h, http.MethodGet, target, nil); w.Code != http.StatusSeeOther {
		t.Fatalf("callback = %d", w.Code)
	}
	b.cookies[LoginCookie] = saved
	if w := b.do(h, http.MethodGet, target, nil); w.Code != http.StatusBadGateway {
		t.Errorf("a replayed callback = %d, want 502", w.Code)
	}
}

// Two callbacks carrying one code, on two replicas at once: the IdP exchanges the code once, so exactly one signs in.
func TestTwoCallbacksOfOneCode(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	keys := newSessionKeys("s-1", "s-1")
	a, b := p.replica(c, keys, nil), p.replica(c, keys, nil)
	br := newBrowser()
	q := startLogin(t, br, a.people.Handler())
	// Warm both replicas' discovery so the race is at the token endpoint.
	for _, r := range []*replica{a, b} {
		if _, err := r.people.idp.Endpoints(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	p.set(func() { p.tokenEntered, p.tokenBlock = entered, release })
	target := "/auth/callback?code=" + code + "&state=" + q.Get("state")
	codes := make(chan int, 2)
	for _, r := range []*replica{a, b} {
		cookie := br.cookies[LoginCookie]
		go func() {
			own := &browser{cookies: map[string]string{LoginCookie: cookie}}
			codes <- own.do(r.people.Handler(), http.MethodGet, target, nil).Code
		}()
	}
	within(t, entered, "the first exchange")
	within(t, entered, "the second exchange")
	once.Do(func() { close(release) })
	got := []int{within(t, codes, "the first callback"), within(t, codes, "the second callback")}
	slices.Sort(got)
	if !slices.Equal(got, []int{http.StatusSeeOther, http.StatusBadGateway}) {
		t.Errorf("two callbacks of one code = %v, want one signed in and one refused", got)
	}
}

// A login started under the old session key id finishes on a replica that issues under the new one.
func TestRotationBetweenLoginAndCallback(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	before := newSessionKeys("s-0", "s-0", "s-1")
	after := newSessionKeys("s-1", "s-0", "s-1")
	after.material = before.material
	a, b := p.replica(c, before, nil), p.replica(c, after, nil)
	br := newBrowser()
	q := startLogin(t, br, a.people.Handler())
	code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
	if w := br.do(b.people.Handler(), http.MethodGet, "/auth/callback?code="+code+"&state="+q.Get("state"), nil); w.Code != http.StatusSeeOther {
		t.Fatalf("callback after the switch = %d %s", w.Code, w.Body)
	}
	if w := br.do(a.people.Handler(), http.MethodGet, "/auth/me", nil); w.Code != http.StatusOK {
		t.Errorf("the new session on a replica still issuing under the old id = %d", w.Code)
	}
}

// The IdP down before the first login refuses it as unavailable without hanging; down after a sign-in, the session keeps working.
func TestIdPDownColdAndWarm(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	r := p.replica(c, newSessionKeys("s-1", "s-1"), nil)
	h := r.people.Handler()
	p.down.Store(true)
	if w := newBrowser().do(h, http.MethodGet, "/auth/login", nil); w.Code != http.StatusServiceUnavailable {
		t.Errorf("login with the IdP down cold = %d, want 503", w.Code)
	}
	if !slices.Contains(r.failures.reasons, ReasonIDPUnavailable) || !slices.Contains(r.logins.steps, "start/unavailable") {
		t.Errorf("counted %v and %v", r.failures.reasons, r.logins.steps)
	}
	p.down.Store(false)
	c.advance(time.Minute) // past the fetch interval
	b := newBrowser()
	q := startLogin(t, b, h)
	code := p.grantChallenge(q.Get("code_challenge"), sign(t, s, p.idClaims(c.Now(), q.Get("nonce"))))
	if w := b.do(h, http.MethodGet, "/auth/callback?code="+code+"&state="+q.Get("state"), nil); w.Code != http.StatusSeeOther {
		t.Fatalf("callback = %d", w.Code)
	}
	p.down.Store(true)
	if w := b.do(h, http.MethodGet, "/auth/me", nil); w.Code != http.StatusOK {
		t.Errorf("a session with the IdP down = %d, want it still served", w.Code)
	}
	// A login started from the cached endpoints reaches a token endpoint that is down: refused as exchange.
	q = startLogin(t, b, h)
	if w := b.do(h, http.MethodGet, "/auth/callback?code=code-x&state="+q.Get("state"), nil); w.Code != http.StatusBadGateway {
		t.Errorf("callback with the IdP down warm = %d, want 502", w.Code)
	}
}

func TestReturnPath(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]string{
		"":                       "/",
		"/":                      "/",
		"/captures?tool=T1#x":    "/captures?tool=T1#x",
		"//evil.example":         "/",
		"/\\evil.example":        "/",
		"/\t/evil.example":       "/",
		"/ /evil":                "/",
		"https://evil.example/":  "/",
		"evil":                   "/",
		"/%09/evil":              "/%09/evil",
		"/café":                  "/",
		"/x\r\nLocation: //evil": "/",
	} {
		if got := ReturnPath(raw); got != want {
			t.Errorf("ReturnPath(%q) = %q, want %q", raw, got, want)
		}
	}
}

// Nothing of the code, the client secret, the tokens or a cookie reaches the log or an answer's body.
func TestSignInWritesNoSecret(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	var buf bytes.Buffer
	r := p.replica(c, newSessionKeys("s-1", "s-1"), slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	h := r.people.Handler()
	const described = "error-description-sentinel-7f2e"
	bodies := make([]string, 0, 5)
	secrets := make([]string, 0, 43)
	secrets = append(secrets, peopleSecret, "at-sentinel", described)
	// A sign-in, then each refusal: another state, the IdP's error, an exchange refused, an ID token refused.
	for _, path := range []string{"ok", "state", "idp-error", "exchange", "nonce"} {
		b := newBrowser()
		q := startLogin(t, b, h)
		login, err := r.people.cookies.OpenLogin(t.Context(), b.cookies[LoginCookie])
		if err != nil {
			t.Fatal(err)
		}
		nonce := q.Get("nonce")
		if path == "nonce" {
			nonce += "x"
		}
		idToken := sign(t, s, p.idClaims(c.Now(), nonce))
		code := p.grantChallenge(q.Get("code_challenge"), idToken)
		v := url.Values{"code": {code}, "state": {q.Get("state")}}
		switch path {
		case "state":
			v.Set("state", q.Get("state")+"x")
		case "idp-error":
			v.Set("error", "access_denied")
			v.Set("error_description", described)
		case "exchange":
			v.Set("code", code+"x")
		default: // a sign-in, or the nonce's refusal, which its ID token carries
		}
		secrets = append(secrets, code, idToken, q.Get("nonce"), q.Get("state"), q.Get("code_challenge"), b.cookies[LoginCookie], login.Verifier)
		w := b.do(h, http.MethodGet, "/auth/callback?"+v.Encode(), nil)
		bodies = append(bodies, w.Body.String())
		secrets = append(secrets, b.cookies[SessionCookie])
	}
	r.logins.mu.Lock()
	labels := strings.Join(r.logins.steps, " ")
	r.logins.mu.Unlock()
	r.failures.mu.Lock()
	labels += " " + strings.Join(r.failures.reasons, " ")
	r.failures.mu.Unlock()
	text := buf.String() + strings.Join(bodies, "\n") + labels
	for _, want := range []string{"auth: refused", "auth: signed in", ReasonExchange, ReasonNonce, ReasonLogin} {
		if !strings.Contains(text, want) {
			t.Fatalf("the log, bodies and labels hold no %q: %s", want, text)
		}
	}
	for _, sec := range secrets {
		if sec != "" && strings.Contains(text, sec) {
			t.Errorf("the log, a body or a metric label holds %q", sec)
		}
	}
}

// Every answer under /auth/ is no-store, the callback's with no referrer, a refused method included;
// me without a session counts one missing credential.
func TestAuthAnswersProtected(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	r := p.replica(c, newSessionKeys("s-1", "s-1"), nil)
	h := r.people.Handler()
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/auth/callback"}, {http.MethodGet, "/auth/callback?code=c&state=s"}, {http.MethodGet, "/auth/unknown"},
		{http.MethodPut, "/auth/logout"}, {http.MethodGet, "/auth/me"},
	} {
		w := newBrowser().do(h, tc.method, tc.path, nil)
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s %s = %d without no-store", tc.method, tc.path, w.Code)
		}
		if strings.HasPrefix(tc.path, "/auth/callback") && w.Header().Get("Referrer-Policy") != "no-referrer" {
			t.Errorf("%s %s = %d without no-referrer", tc.method, tc.path, w.Code)
		}
	}
	r.failures.mu.Lock()
	defer r.failures.mu.Unlock()
	if n := slices.Index(r.failures.reasons, ReasonMissing); n < 0 || slices.Contains(r.failures.reasons[n+1:], ReasonMissing) {
		t.Errorf("me without a session counted %v, want one missing", r.failures.reasons)
	}
}
