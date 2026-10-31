package service

import (
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/devidp"
	"github.com/arloliu/tracekeep/internal/keys"
)

// Dev's sign-in (api.md §10.3 *Development*): its certificate, its test IdP's client and its session key, under its directory.
const (
	DevCertFile    = "tls/cert.pem"
	devCertKey     = "tls/key.pem"
	devCertLife    = 365 * 24 * time.Hour
	devSecretFile  = "idp-client-secret" //nolint:gosec // a file name, not a credential
	devSessionFile = "session-key"
	devSessionID   = "dev-session-1"
	devAudience    = "tracekeep-dev-api"
	devClientID    = "tracekeep-dev-web"
)

// devHosts are the names dev's certificate holds, and so the only hosts dev serves or names (its test IdP signs in whoever asks).
var devHosts = []string{"localhost", "127.0.0.1", "::1"}

// devRoles are every role, each offered by the test IdP and granted under its own name.
var devRoles = []string{auth.RoleViewer, auth.RoleExporter, auth.RolePackReader, auth.RoleScorer, auth.RoleRestorer, auth.RoleAdmin}

// devListen binds addr, whose host must be one of devHosts, and returns the listener and the address to name it by:
// the host as given, with the bound port, so port 0 resolves.
func devListen(flag, addr string) (net.Listener, string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || !slices.Contains(devHosts, host) {
		return nil, "", fmt.Errorf("dev: --%s %s: dev serves only localhost, 127.0.0.1 or ::1", flag, addr)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", fmt.Errorf("dev: --%s: %w", flag, err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	return ln, net.JoinHostPort(host, port), nil
}

// devSignIn configures cfg for people's sign-in at dev's test IdP, which it starts on idpLn:
// the API over TLS on its certificate, the IdP as auth.oidc trusted through auth.oidc.ca_file, a people client and a session key.
func (d *Dev) devSignIn(cfg *config.Config, apiAddr string, idpLn net.Listener, idpAddr string) error {
	dir := d.opts.Dir
	cert, key := filepath.Join(dir, DevCertFile), filepath.Join(dir, devCertKey)
	if err := devCertificate(cert, key, time.Now()); err != nil {
		return err
	}
	public := "https://" + apiAddr
	if d.opts.PublicURL != "" {
		u, err := url.Parse(d.opts.PublicURL)
		if err != nil || u.Scheme != "https" || !slices.Contains(devHosts, u.Hostname()) || strings.Trim(u.Path, "/") != "" || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("dev: --public-url %q must be an https URL on localhost, 127.0.0.1 or ::1, without a path", d.opts.PublicURL)
		}
		// One form for the IdP's redirect URL and the service's, which exact matching compares.
		public = "https://" + u.Host
	}
	secretFile := filepath.Join(dir, devSecretFile)
	if err := generate(secretFile, func() []byte { return []byte(rand.Text() + rand.Text()) }); err != nil {
		return err
	}
	secret, err := os.ReadFile(secretFile)
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	sessionKey := filepath.Join(dir, devSessionFile)
	if err := generate(sessionKey, func() []byte { return []byte(base64.StdEncoding.EncodeToString([]byte(rand.Text() + rand.Text()))) }); err != nil {
		return err
	}
	issuer := "https://" + idpAddr
	idp, err := devidp.New(devidp.Config{
		Issuer: issuer, ClientID: devClientID, ClientSecret: string(secret), RedirectURL: public + "/auth/callback",
		RolesClaim: "roles", Roles: devRoles, DefaultSubject: "dev", DefaultRoles: []string{auth.RoleViewer, auth.RoleExporter},
	})
	if err != nil {
		return err
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	d.idp = &http.Server{
		Handler: idp.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12},
		ErrorLog:  slog.NewLogLogger(d.opts.Log.Handler(), slog.LevelWarn),
	}
	srv := d.idp
	go func() { _ = srv.ServeTLS(idpLn, "", "") }()

	identity := map[string][]string{}
	for _, r := range devRoles {
		identity[r] = []string{r}
	}
	cfg.API.TLSCertFile, cfg.API.TLSKeyFile, cfg.API.PublicURL = cert, key, public
	cfg.Auth.OIDC.Issuer, cfg.Auth.OIDC.Audience, cfg.Auth.OIDC.CAFile = issuer, devAudience, cert
	cfg.Auth.OIDC.RolesClaim, cfg.Auth.OIDC.Roles = "roles", identity
	p := &cfg.Auth.People
	p.ClientID, p.ClientSecretRef, p.ClientSecret, p.RolesClaim, p.Roles = devClientID, "file://"+secretFile, string(secret), "roles", identity
	cfg.Keys.Declared[devSessionID] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: sessionKey, Kind: catalog.KindSession}
	cfg.Keys.Session = config.KeyPurpose{Issuing: devSessionID, Accepted: []string{devSessionID}}

	return nil
}

// closeIDP shuts the test IdP down within d.idpShutdown, then closes the connections left, which ends their requests.
func (d *Dev) closeIDP() error {
	if d.idp == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cmp.Or(d.idpShutdown, 5*time.Second))
	defer cancel()
	if err := d.idp.Shutdown(ctx); err != nil {
		return errors.Join(fmt.Errorf("dev: idp: shutdown: %w", err), d.idp.Close())
	}

	return nil
}

// devCertificate writes a self-signed ECDSA P-256 certificate for devHosts and its key, mode 0600,
// unless a certificate is there that loads with its key and is valid at now.
func devCertificate(certFile, keyFile string, now time.Time) error {
	if pair, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil && pair.Leaf != nil &&
		now.After(pair.Leaf.NotBefore) && now.Before(pair.Leaf.NotAfter) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(certFile), 0o750); err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "tracekeep dev"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(devCertLife),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("dev: %w", err)
	}
	// The key first: a certificate without its key would not load, and is made again.
	if err := writePEM(keyFile, "PRIVATE KEY", keyDER, 0o600); err != nil {
		return err
	}

	return writePEM(certFile, "CERTIFICATE", der, 0o644)
}

// writePEM replaces path with one PEM block, through a temporary file renamed over it.
func writePEM(path, kind string, der []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	err := os.WriteFile(tmp, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), mode)
	if err == nil {
		err = os.Chmod(tmp, mode)
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("dev: %w", err), os.Remove(tmp))
	}

	return nil
}
