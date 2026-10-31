package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// signatureAlgs are the algorithms a service token may name (api.md §10.2): asymmetric only.
var signatureAlgs = []jose.SignatureAlgorithm{
	jose.RS256, jose.RS384, jose.RS512, jose.PS256, jose.PS384, jose.PS512, jose.ES256, jose.ES384, jose.ES512,
}

// parseAlgs are the algorithms a token is parsed under, so that a refused one is told apart from a malformed token.
var parseAlgs = append(slices.Clone(signatureAlgs), jose.HS256, jose.HS384, jose.HS512, jose.EdDSA)

// maxDocument bounds a discovery document's or a JWKS's read, and fetchTimeout each fetch.
const (
	maxDocument  = 1 << 20
	fetchTimeout = 10 * time.Second
)

// OIDCConfig is the IdP a service token is validated against (api.md §10.2).
type OIDCConfig struct {
	Issuer, Audience string
	// PeopleClient is people's client id, when they sign in: a service token whose aud holds it is refused,
	// so an ID token never passes as a service token.
	PeopleClient string
	// RolesClaim names the claim holding the token's role values, a string or a list of strings; empty is "roles".
	RolesClaim string
	// Roles maps a role value to the roles it grants; a value it does not name grants none.
	Roles map[string][]string
	// TTL is how long a fetched JWKS verifies, MinInterval the least time between two fetches' starts,
	// and Leeway the clock tolerance of exp, nbf and iat.
	TTL, MinInterval, Leeway time.Duration
	// Client fetches discovery and the JWKS, over https only; nil, or a zero timeout, takes a 10 s timeout.
	Client *http.Client
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// Verifier validates service tokens against the IdP's JWKS, fetched lazily and cached with a TTL:
// one fetch in flight, fetches started at least MinInterval apart, an unknown kid refreshing once.
type Verifier struct {
	cfg OIDCConfig

	mu sync.Mutex
	// keys verify until until; inflight, while a fetch runs, is closed when it ends.
	keys  []jose.JSONWebKey
	until time.Time
	// endpoints serve logins until endpointsUntil; a fetch whose discovery names no usable pair keeps them, never extended.
	endpoints      Endpoints
	endpointsUntil time.Time
	attempted      time.Time
	tried          bool
	lastErr        error
	inflight       chan struct{}
	// onWait, set only by the package's tests, runs when a request starts waiting on the fetch in flight.
	onWait func()
}

// NewVerifier returns a verifier of cfg; it fetches nothing, so startup never waits on the IdP.
func NewVerifier(cfg OIDCConfig) *Verifier {
	if cfg.RolesClaim == "" {
		cfg.RolesClaim = "roles"
	}
	// The client is a copy: keys come over https only, every redirect included, and a fetch is bounded in time.
	client := &http.Client{}
	if cfg.Client != nil {
		*client = *cfg.Client
	}
	if client.Timeout == 0 {
		client.Timeout = fetchTimeout
	}
	prior := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme != "https" {
			return fmt.Errorf("a redirect to %s, not https", req.URL.Redacted())
		}
		if prior != nil {
			return prior(req, via)
		}
		if len(via) >= 10 {
			return errors.New("too many redirects")
		}

		return nil
	}
	cfg.Client = client
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &Verifier{cfg: cfg}
}

// Verify validates the service token raw and returns its principal, or an *Error naming the reason.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	claims, extra, err := v.verifyJWS(ctx, raw, v.cfg.Audience)
	if err != nil {
		return Principal{}, err
	}
	if v.cfg.PeopleClient != "" && slices.Contains(claims.Audience, v.cfg.PeopleClient) {
		return Principal{}, &Error{Reason: ReasonAudience, Subject: claims.Subject, Err: errors.New("an audience of people's client")}
	}

	return Principal{Subject: claims.Subject, Roles: mapRoles(extra[v.cfg.RolesClaim], v.cfg.Roles), Via: ViaOIDC}, nil
}

// IDToken is a verified ID token's subject and claims.
type IDToken struct {
	Subject string
	Claims  map[string]any
}

// VerifyIDToken validates the ID token raw of a login (api.md §10.2 *People's sign-in*):
// signed as a service token is, by the same cached keys, with the issuer and times checked alike,
// its aud holding clientID, its azp clientID (required when aud holds more than one value), its nonce the login's and its sub not empty.
func (v *Verifier) VerifyIDToken(ctx context.Context, raw, clientID, nonce string) (IDToken, error) {
	claims, extra, err := v.verifyJWS(ctx, raw, clientID)
	if err != nil {
		return IDToken{}, err
	}
	azpClaim, hasAZP := extra["azp"]
	azp, isString := azpClaim.(string)
	switch {
	case (hasAZP || len(claims.Audience) > 1) && (!isString || azp != clientID):
		return IDToken{}, &Error{Reason: ReasonAudience, Subject: claims.Subject, Err: errors.New("azp is not the client id")}
	case claims.Subject == "":
		return IDToken{}, refuse(ReasonMalformed, errors.New("no sub"))
	}
	got, _ := extra["nonce"].(string)
	if nonce == "" || subtle.ConstantTimeCompare([]byte(got), []byte(nonce)) != 1 {
		return IDToken{}, &Error{Reason: ReasonNonce, Subject: claims.Subject, Err: errors.New("not the login's nonce")}
	}

	return IDToken{Subject: claims.Subject, Claims: extra}, nil
}

// verifyJWS checks raw's form, algorithm, key and signature, then its issuer, its audience holding audience, and its times,
// and returns its claims.
func (v *Verifier) verifyJWS(ctx context.Context, raw, audience string) (jwt.Claims, map[string]any, error) {
	tok, err := jwt.ParseSigned(raw, parseAlgs)
	if err != nil {
		return jwt.Claims{}, nil, refuse(ReasonMalformed, errors.New("not a compact JWS"))
	}
	if len(tok.Headers) != 1 {
		return jwt.Claims{}, nil, refuse(ReasonMalformed, errors.New("not one signature"))
	}
	h := tok.Headers[0]
	if !slices.Contains(signatureAlgs, jose.SignatureAlgorithm(h.Algorithm)) {
		return jwt.Claims{}, nil, refuse(ReasonAlg, fmt.Errorf("alg %q", h.Algorithm))
	}
	if h.KeyID == "" {
		return jwt.Claims{}, nil, refuse(ReasonMalformed, errors.New("no kid"))
	}
	key, err := v.key(ctx, h.KeyID)
	if err != nil {
		return jwt.Claims{}, nil, err
	}
	if key.Algorithm != "" && key.Algorithm != h.Algorithm {
		return jwt.Claims{}, nil, refuse(ReasonSignature, fmt.Errorf("kid %s is a %s key, the token names %s", h.KeyID, key.Algorithm, h.Algorithm))
	}
	if err := tok.Claims(key.Key); err != nil {
		return jwt.Claims{}, nil, refuse(ReasonSignature, fmt.Errorf("kid %s: the signature does not verify", h.KeyID))
	}
	var claims jwt.Claims
	var extra map[string]any
	if err := tok.UnsafeClaimsWithoutVerification(&claims, &extra); err != nil { // verified just above
		return jwt.Claims{}, nil, refuse(ReasonMalformed, errors.New("claims"))
	}
	if claims.Expiry == nil {
		return jwt.Claims{}, nil, &Error{Reason: ReasonMalformed, Subject: claims.Subject, Err: errors.New("no exp")}
	}
	err = claims.ValidateWithLeeway(jwt.Expected{Issuer: v.cfg.Issuer, AnyAudience: jwt.Audience{audience}, Time: v.cfg.Now()}, v.cfg.Leeway)
	if err != nil {
		return jwt.Claims{}, nil, &Error{Reason: claimReason(err), Subject: claims.Subject, Err: err}
	}

	return claims, extra, nil
}

func claimReason(err error) string {
	switch {
	case errors.Is(err, jwt.ErrInvalidIssuer):
		return ReasonIssuer
	case errors.Is(err, jwt.ErrInvalidAudience):
		return ReasonAudience
	case errors.Is(err, jwt.ErrExpired):
		return ReasonExpired
	case errors.Is(err, jwt.ErrNotValidYet), errors.Is(err, jwt.ErrIssuedInTheFuture):
		return ReasonNotYetValid
	default:
		return ReasonMalformed
	}
}

// mapRoles maps a role claim's values, a string or a list of strings, to the roles roles grants them, each once.
func mapRoles(claim any, roles map[string][]string) []string {
	var values []string
	switch c := claim.(type) {
	case string:
		values = []string{c}
	case []any:
		for _, e := range c {
			if s, ok := e.(string); ok {
				values = append(values, s)
			}
		}
	default: // absent, or of another type: no role values
	}
	var out []string
	for _, val := range values {
		for _, r := range roles[val] {
			if !slices.Contains(out, r) {
				out = append(out, r)
			}
		}
	}

	return out
}

// key returns kid's key: from the cache while its TTL lasts, else after one fetch, started by this request or waited on.
func (v *Verifier) key(ctx context.Context, kid string) (jose.JSONWebKey, error) {
	var k jose.JSONWebKey
	err := v.await(ctx, func() bool {
		var ok bool
		k, ok = v.cached(kid)

		return ok
	}, func() error { return v.miss(kid) })

	return k, err
}

// Endpoints are the authorization and token endpoints a login uses, from the discovery document.
type Endpoints struct {
	Authorization, Token string
}

// Endpoints returns the IdP's authorization and token endpoints: from the cache while its TTL lasts, else after one fetch,
// under the same rules as a key; a discovery document naming no https endpoint for either refuses a login as idp-unavailable.
func (v *Verifier) Endpoints(ctx context.Context) (Endpoints, error) {
	var e Endpoints
	err := v.await(ctx, func() bool {
		e = v.endpoints

		return v.cfg.Now().Before(v.endpointsUntil) && e.Authorization != "" && e.Token != ""
	}, func() error {
		err := v.lastErr
		if err == nil {
			err = errors.New("discovery names no authorization or token endpoint")
		}

		return refuse(ReasonIDPUnavailable, err)
	})

	return e, err
}

// await returns once have holds under v.mu: at once, or after one fetch, started by this request or waited on,
// and otherwise with miss's refusal; a fetch is started only MinInterval after the previous one started.
func (v *Verifier) await(ctx context.Context, have func() bool, miss func() error) error {
	v.mu.Lock()
	if have() {
		v.mu.Unlock()
		return nil
	}
	wait := v.inflight
	if wait == nil {
		now := v.cfg.Now()
		if v.tried && now.Sub(v.attempted) < v.cfg.MinInterval {
			err := miss()
			v.mu.Unlock()

			return err
		}
		v.tried, v.attempted = true, now
		wait = make(chan struct{})
		v.inflight = wait
		v.mu.Unlock()
		v.refresh(ctx, wait)
	} else {
		v.mu.Unlock()
		if v.onWait != nil {
			v.onWait()
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return refuse(ReasonIDPUnavailable, ctx.Err())
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if have() {
		return nil
	}

	return miss()
}

// cached returns kid's key while the cache's TTL lasts; v.mu is held.
func (v *Verifier) cached(kid string) (jose.JSONWebKey, bool) {
	if !v.cfg.Now().Before(v.until) {
		return jose.JSONWebKey{}, false
	}
	for _, k := range v.keys {
		if k.KeyID == kid {
			return k, true
		}
	}

	return jose.JSONWebKey{}, false
}

// miss is the refusal of a kid no cached key has: unknown while the cache lasts, the IdP unavailable once it has expired; v.mu is held.
func (v *Verifier) miss(kid string) error {
	if v.cfg.Now().Before(v.until) {
		return refuse(ReasonUnknownKid, fmt.Errorf("kid %s", kid))
	}
	err := v.lastErr
	if err == nil {
		err = errors.New("no keys cached")
	}

	return refuse(ReasonIDPUnavailable, err)
}

// refresh runs the fetch this request started and publishes its outcome, closing done.
// The fetch outlives the request that started it, since others wait on it.
func (v *Verifier) refresh(ctx context.Context, done chan struct{}) {
	var keys []jose.JSONWebKey
	var endpoints Endpoints
	err := errors.New("the fetch did not complete")
	// The fetch in flight ends, and its waiters wake, however the fetch returns.
	defer func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		if err == nil {
			v.keys, v.until, v.lastErr = keys, v.cfg.Now().Add(v.cfg.TTL), nil
			if endpoints.Authorization != "" && endpoints.Token != "" {
				v.endpoints, v.endpointsUntil = endpoints, v.until
			}
		} else {
			v.lastErr = err
		}
		v.inflight = nil
		close(done)
	}()
	keys, endpoints, err = v.fetch(context.WithoutCancel(ctx))
}

// fetch reads the issuer's discovery document, which must name the configured issuer, and the public signing keys of its JWKS,
// both over https, with the authorization and token endpoints the document names over https;
// a key of a type it cannot read is skipped, and a JWKS without a usable key is a failed fetch,
// which keeps the keys and endpoints cached before it.
func (v *Verifier) fetch(ctx context.Context) ([]jose.JSONWebKey, Endpoints, error) {
	var doc struct {
		Issuer        string `json:"issuer"`
		JWKSURI       string `json:"jwks_uri"`
		Authorization string `json:"authorization_endpoint"`
		Token         string `json:"token_endpoint"`
	}
	if err := v.get(ctx, strings.TrimSuffix(v.cfg.Issuer, "/")+"/.well-known/openid-configuration", &doc); err != nil {
		return nil, Endpoints{}, fmt.Errorf("discovery: %w", err)
	}
	if doc.Issuer != v.cfg.Issuer || doc.JWKSURI == "" {
		return nil, Endpoints{}, fmt.Errorf("discovery names issuer %q and jwks_uri %q", doc.Issuer, doc.JWKSURI)
	}
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := v.get(ctx, doc.JWKSURI, &set); err != nil {
		return nil, Endpoints{}, fmt.Errorf("jwks: %w", err)
	}
	var keys []jose.JSONWebKey
	for _, raw := range set.Keys {
		var k jose.JSONWebKey
		if err := k.UnmarshalJSON(raw); err != nil || !usable(k) {
			continue
		}
		keys = append(keys, k)
	}

	if len(keys) == 0 {
		return nil, Endpoints{}, errors.New("jwks: no usable signing key")
	}

	return keys, Endpoints{Authorization: httpsOnly(doc.Authorization), Token: httpsOnly(doc.Token)}, nil
}

// httpsOnly is u when it is an https URL with a host, and empty otherwise.
func httpsOnly(u string) string {
	p, err := url.Parse(u)
	if err != nil || p.Scheme != "https" || p.Host == "" {
		return ""
	}

	return u
}

// usable reports whether k can verify a service token: a valid public RSA or EC signing key with a kid,
// naming, if any, an algorithm of the allowlist that its type and curve verify.
func usable(k jose.JSONWebKey) bool {
	if !k.Valid() || !k.IsPublic() || k.KeyID == "" || (k.Use != "" && k.Use != "sig") {
		return false
	}
	var algs []jose.SignatureAlgorithm
	switch pub := k.Key.(type) {
	case *rsa.PublicKey:
		algs = []jose.SignatureAlgorithm{jose.RS256, jose.RS384, jose.RS512, jose.PS256, jose.PS384, jose.PS512}
	case *ecdsa.PublicKey:
		algs = []jose.SignatureAlgorithm{map[string]jose.SignatureAlgorithm{"P-256": jose.ES256, "P-384": jose.ES384, "P-521": jose.ES512}[pub.Curve.Params().Name]}
	default:
		return false
	}

	return k.Algorithm == "" || slices.Contains(algs, jose.SignatureAlgorithm(k.Algorithm))
}

func (v *Verifier) get(ctx context.Context, target string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
	if err != nil {
		return err
	}
	if req.URL.Scheme != "https" {
		return fmt.Errorf("%s is not https", req.URL.Redacted())
	}
	resp, err := v.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", target, resp.Status)
	}

	return json.NewDecoder(io.LimitReader(resp.Body, maxDocument)).Decode(dst)
}
