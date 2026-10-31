package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// PeopleConfig is people's sign-in (api.md §10.2 *People's sign-in*): a client of its own at the service tokens' IdP.
type PeopleConfig struct {
	ClientID, ClientSecret string
	// PublicURL is the https base the browser reaches the API at; the redirect URL is under it.
	PublicURL string
	// Scopes are asked for beside openid, which is always sent.
	Scopes []string
	// RolesClaim names the ID token claim holding a person's group values, mapped to roles by Roles.
	RolesClaim string
	Roles      map[string][]string
	// SessionLifetime bounds a session, LoginLifetime a started login, ExchangeTimeout the token exchange.
	SessionLifetime, LoginLifetime, ExchangeTimeout time.Duration
	// Client exchanges codes, over https only and following no redirect; nil takes a default client.
	Client *http.Client
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// People is the backend-for-frontend's sign-in: it exchanges a login's code at the IdP, verifies the ID token
// and seals the session, with nothing kept between requests.
type People struct {
	cfg     PeopleConfig
	idp     *Verifier
	cookies *Cookies
	client  *http.Client
}

// NewPeople returns the sign-in of cfg at idp, sealing its cookies with cookies; it fetches nothing.
func NewPeople(cfg PeopleConfig, idp *Verifier, cookies *Cookies) *People {
	client := &http.Client{}
	if cfg.Client != nil {
		*client = *cfg.Client
	}
	// A token request is a POST carrying the code and the secret: it follows no redirect.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &People{cfg: cfg, idp: idp, cookies: cookies, client: client}
}

// redirectURL is where the IdP sends the browser back.
func (p *People) redirectURL() string {
	return strings.TrimSuffix(p.cfg.PublicURL, "/") + "/auth/callback"
}

// signIn exchanges code for l's ID token and returns the session it grants:
// the token's subject, the roles its claim maps to, until the session lifetime from now.
func (p *People) signIn(ctx context.Context, code string, l Login) (Session, error) {
	e, err := p.idp.Endpoints(ctx)
	if err != nil {
		return Session{}, err
	}
	raw, err := p.exchange(ctx, e.Token, code, l.Verifier)
	if err != nil {
		return Session{}, err
	}
	tok, err := p.idp.VerifyIDToken(ctx, raw, p.cfg.ClientID, l.Nonce)
	if err != nil {
		return Session{}, err
	}
	now := p.cfg.Now().Truncate(time.Second)

	return Session{
		Subject: tok.Subject, Roles: mapRoles(tok.Claims[p.cfg.RolesClaim], p.cfg.Roles),
		Issued: now, Expiry: now.Add(p.cfg.SessionLifetime),
	}, nil
}

// exchange posts the authorization code grant to tokenURL, the client authenticated by HTTP Basic,
// within the exchange's deadline, and returns the answer's id_token; every failure is refused as exchange,
// in words that hold neither the code nor the secret.
func (p *People) exchange(ctx context.Context, tokenURL, code, verifier string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.ExchangeTimeout)
	defer cancel()
	form := url.Values{
		"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {p.redirectURL()}, "code_verifier": {verifier},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil || req.URL.Scheme != "https" {
		return "", refuse(ReasonExchange, errors.New("the token endpoint is not an https URL"))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	// RFC 6749 §2.3.1: the id and secret are form-encoded before Basic encodes them.
	req.SetBasicAuth(url.QueryEscape(p.cfg.ClientID), url.QueryEscape(p.cfg.ClientSecret))
	resp, err := p.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", refuse(ReasonExchange, errors.New("the token endpoint did not answer in time"))
		}

		return "", refuse(ReasonExchange, errors.New("the token endpoint could not be reached"))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", refuse(ReasonExchange, fmt.Errorf("the token endpoint answered %d", resp.StatusCode))
	}
	var body struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxDocument)).Decode(&body); err != nil || body.IDToken == "" {
		return "", refuse(ReasonExchange, errors.New("the token endpoint's answer holds no id_token"))
	}

	return body.IDToken, nil
}
