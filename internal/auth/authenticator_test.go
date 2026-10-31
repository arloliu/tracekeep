package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"
)

// ca is a synthetic certificate authority issuing client certificates.
type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) ca {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	return ca{cert: cert, key: key}
}

// client issues a client certificate naming uris and dns.
func (c ca) client(t *testing.T, uris []string, dns []string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "client"}, DNSNames: dns,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = append(tmpl.URIs, p)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// failures records the reasons counted.
type failures struct {
	mu      sync.Mutex
	reasons []string
}

func (f *failures) AuthFailure(reason string) {
	f.mu.Lock()
	f.reasons = append(f.reasons, reason)
	f.mu.Unlock()
}

func (f *failures) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.reasons)
}

const (
	generatorURI = "spiffe://cluster.example/ns/ve/sa/generator"
	scorerDNS    = "scorer.ve.svc.cluster.example"
)

// mtlsServer serves the principal Check finds for op as JSON, over TLS verifying a client certificate of authority when one is given.
func mtlsServer(t *testing.T, a *Authenticator, authority ca, op string) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := a.Check(r, op)
		if err != nil {
			http.Error(w, reason(err), http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(wirePrincipal(p))
	}))
	pool := x509.NewCertPool()
	pool.AddCert(authority.cert)
	srv.TLS = &tls.Config{ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pool, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return srv
}

// wirePrincipal is a principal as mtlsServer serves it.
type wirePrincipal struct {
	Subject string   `json:"subject"`
	Roles   []string `json:"roles"`
	Via     string   `json:"via"`
}

func clientWith(srv *httptest.Server, certs ...tls.Certificate) *http.Client {
	tr := srv.Client().Transport.(*http.Transport).Clone() //nolint:errcheck // httptest's client has an *http.Transport
	tr.TLSClientConfig.Certificates = certs

	return &http.Client{Transport: tr}
}

// The mTLS principal is the verified certificate's first URI name, else its first DNS name, mapped to roles;
// a certificate of another authority fails the handshake, and an identity the configuration does not name is refused.
func TestMTLSPrincipal(t *testing.T) {
	t.Parallel()
	authority := newCA(t)
	f := &failures{}
	a := NewAuthenticator(AuthenticatorConfig{
		MTLS:     map[string][]string{generatorURI: {RolePackReader}, scorerDNS: {RoleScorer}},
		Failures: f,
	})
	srv := mtlsServer(t, a, authority, OpListPacks)
	get := func(c *http.Client) (int, Principal) {
		t.Helper()
		resp, err := c.Get(srv.URL)
		if err != nil {
			return 0, Principal{}
		}
		defer resp.Body.Close()
		var p wirePrincipal
		_ = json.NewDecoder(resp.Body).Decode(&p)

		return resp.StatusCode, Principal(p)
	}
	code, p := get(clientWith(srv, authority.client(t, []string{generatorURI}, []string{scorerDNS})))
	if code != http.StatusOK || p.Subject != generatorURI || p.Via != ViaMTLS || !slices.Equal(p.Roles, []string{RolePackReader}) {
		t.Errorf("URI-named client = %d, %+v; want the URI's principal", code, p)
	}
	srvScorer := mtlsServer(t, a, authority, OpDigest)
	resp, err := clientWith(srvScorer, authority.client(t, nil, []string{scorerDNS})).Get(srvScorer.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("DNS-named scorer = %d, want its DNS name's principal", resp.StatusCode)
	}
	if code, _ := get(clientWith(srv, authority.client(t, []string{"spiffe://cluster.example/ns/other/sa/x"}, nil))); code != http.StatusForbidden {
		t.Errorf("an unnamed identity = %d, want refused", code)
	}
	if code, _ := get(clientWith(srv, newCA(t).client(t, []string{generatorURI}, nil))); code != 0 {
		t.Errorf("a certificate of another authority = %d, want the handshake refused", code)
	}
	if code, _ := get(clientWith(srv)); code != http.StatusForbidden {
		t.Errorf("no certificate and no token = %d, want refused", code)
	}
	if got := f.got(); !slices.Equal(got, []string{ReasonCertificate, ReasonMissing}) {
		t.Errorf("failures counted %v, want certificate then missing", got)
	}
}

// A certificate the handshake did not verify is no credential.
func TestUnverifiedCertificateIgnored(t *testing.T) {
	t.Parallel()
	a := NewAuthenticator(AuthenticatorConfig{MTLS: map[string][]string{generatorURI: {RolePackReader}}})
	leaf, err := x509.ParseCertificate(newCA(t).client(t, []string{generatorURI}, nil).Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	if _, err := a.Authenticate(r); reason(err) != ReasonMissing {
		t.Errorf("Authenticate with an unverified certificate = %v, want missing", err)
	}
}

func bearer(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}

	return r
}

const staticToken = "a-static-token-of-the-standalone-host"

func staticTokens() []StaticToken {
	return []StaticToken{{Token: staticToken, Subject: "standalone-reader", Roles: []string{RolePackReader}}}
}

func TestStaticTokens(t *testing.T) {
	t.Parallel()
	on := NewAuthenticator(AuthenticatorConfig{StaticEnabled: true, Static: staticTokens()})
	p, err := on.Authenticate(bearer(staticToken))
	if err != nil || p.Subject != "standalone-reader" || p.Via != ViaStatic || !slices.Equal(p.Roles, []string{RolePackReader}) {
		t.Errorf("Authenticate with the static token = %+v, %v", p, err)
	}
	if _, err := on.Authenticate(bearer(staticToken + "x")); reason(err) != ReasonStatic {
		t.Errorf("Authenticate with another token and no IdP = %v, want static", err)
	}
	if _, err := on.Authenticate(bearer("")); reason(err) != ReasonMissing {
		t.Errorf("Authenticate without a token = %v, want missing", err)
	}
	r := bearer("")
	r.Header.Set("Authorization", "Basic "+staticToken)
	if _, err := on.Authenticate(r); reason(err) != ReasonMalformed {
		t.Errorf("Authenticate with another scheme = %v, want malformed", err)
	}
}

// The static-token provider is never activated by a failure: with its flag off and the IdP down,
// a static token is refused like any other, and a valid service token is refused as idp-unavailable.
func TestNoStaticFallback(t *testing.T) {
	t.Parallel()
	p, c := newIDP(t), newClock()
	s := p.newKey(t, "k1")
	p.down.Store(true)
	f := &failures{}
	a := NewAuthenticator(AuthenticatorConfig{OIDC: p.verifier(c), Static: staticTokens(), Failures: f})
	if got, err := a.Check(bearer(staticToken), OpListPacks); err == nil {
		t.Errorf("Check with a static token and the flag off = %+v, want refused", got)
	}
	if _, err := a.Check(bearer(sign(t, s, p.claims(c.Now()))), OpListPacks); reason(err) != ReasonIDPUnavailable {
		t.Errorf("Check with a service token and the IdP down = %v, want idp-unavailable", err)
	}
	if got := f.got(); len(got) != 2 || got[0] != ReasonMalformed || got[1] != ReasonIDPUnavailable {
		t.Errorf("failures counted %v", got)
	}
	// With the flag on, a token that matches no static token goes on to the IdP.
	on := NewAuthenticator(AuthenticatorConfig{OIDC: p.verifier(c), StaticEnabled: true, Static: staticTokens()})
	p.down.Store(false)
	if got, err := on.Authenticate(bearer(sign(t, s, p.claims(c.Now())))); err != nil || got.Via != ViaOIDC {
		t.Errorf("Authenticate with a service token and the flag on = %+v, %v", got, err)
	}
}

func TestAuthorize(t *testing.T) {
	t.Parallel()
	admits := map[string][]string{
		OpListDevices: {RolePackReader, RoleViewer}, OpListCaptures: {RolePackReader, RoleViewer}, OpListScopeChanges: {RolePackReader, RoleViewer},
		OpListPacks: {RolePackReader}, OpPackURL: {RolePackReader},
		OpDigest: {RoleScorer}, OpRestore: {RoleRestorer}, OpExport: {RoleExporter}, OpAdmin: {RoleAdmin},
		OpQueryRecords: {RoleViewer}, OpFindTransaction: {RoleViewer},
	}
	roles := []string{RolePackReader, RoleScorer, RoleViewer, RoleExporter, RoleRestorer, RoleAdmin}
	for op, want := range admits {
		for _, role := range roles {
			err := Authorize(Principal{Subject: "s", Roles: []string{role}}, op)
			if (err == nil) != slices.Contains(want, role) || (err != nil && reason(err) != ReasonForbidden) {
				t.Errorf("Authorize(%s, %s) = %v", role, op, err)
			}
		}
		if err := Authorize(Principal{Subject: "s"}, op); reason(err) != ReasonForbidden {
			t.Errorf("Authorize(no role, %s) = %v", op, err)
		}
	}
	if err := Authorize(Principal{Subject: "s", Roles: roles}, "Unknown"); reason(err) != ReasonForbidden {
		t.Errorf("Authorize of an operation the table does not name = %v, want forbidden", err)
	}
}

func TestCheckCountsForbidden(t *testing.T) {
	t.Parallel()
	f := &failures{}
	a := NewAuthenticator(AuthenticatorConfig{StaticEnabled: true, Static: staticTokens(), Failures: f})
	if _, err := a.Check(bearer(staticToken), OpAdmin); reason(err) != ReasonForbidden {
		t.Errorf("Check of admin as a pack reader = %v, want forbidden", err)
	}
	if p, err := a.Check(bearer(staticToken), OpListPacks); err != nil || p.Subject != "standalone-reader" {
		t.Errorf("Check of ListPacks = %+v, %v", p, err)
	}
	if got := f.got(); !slices.Equal(got, []string{ReasonForbidden}) {
		t.Errorf("failures counted %v, want forbidden", got)
	}
}
