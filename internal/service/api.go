package service

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/api"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

// apiShutdown bounds how long the requests in flight may finish when the api role stops, before their connections are closed.
const apiShutdown = 10 * time.Second

// API runs the api role until ctx ends (architecture.md §3.1): the machine API and its downloads on api.listen,
// over TLS with client certificates verified when given, or over HTTP/2 without TLS under tracekeep dev.
// It is ready once its listener is bound and the catalog answers (api.md §10.1 *Downloads*).
func API(ctx context.Context, e *Env) error {
	e.ready("api", failing(errStarting))
	defer e.ready("api", failing(errStopped))
	c := e.Config
	var m *metrics.API
	if e.Metrics != nil {
		var err error
		if m, err = metrics.NewAPI(e.Metrics); err != nil {
			return err
		}
	}
	log := e.Log.With("role", "api")
	idp := verifier(&c.Auth)
	s := &api.Server{
		Catalog: e.Catalog, QuietPeriod: c.Compact.QuietPeriod, Store: e.Store, Metrics: m, Log: log,
		Auth: authenticator(&c.Auth, idp, nil, m, log),
		Downloads: api.Downloads{
			PublicURL: c.API.PublicURL, Lifetime: c.Store.DownloadLifetime, Margin: c.API.DownloadMargin, Floor: c.API.DownloadFloor,
			DeleteDelay: c.Compact.DeleteDelay, Now: e.Now,
		},
		DigestLimit: &api.DigestLimit{Rate: c.API.DigestRate, Burst: c.API.DigestBurst},
	}
	// Every handler is counted, so the role returns only once none runs: the catalog and the store outlive them all.
	var inFlight sync.WaitGroup
	handler := s.Handler()
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			inFlight.Add(1)
			defer inFlight.Done()
			handler.ServeHTTP(w, r)
		}),
		ReadHeaderTimeout: 10 * time.Second, Protocols: new(http.Protocols),
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	srv.Protocols.SetHTTP1(true)
	if c.API.Plaintext {
		srv.Protocols.SetUnencryptedHTTP2(true)
	} else {
		srv.Protocols.SetHTTP2(true)
		var err error
		if srv.TLSConfig, err = serverTLS(&c.API, &c.Auth.MTLS, log); err != nil {
			return err
		}
	}
	ln, err := net.Listen("tcp", c.API.Listen)
	if err != nil {
		return fmt.Errorf("service: api: %w", err)
	}
	defer ln.Close()
	e.apiAddr.Store(ln.Addr().String())
	if e.apiBound != nil {
		e.apiBound()
	}
	if e.listening != nil {
		e.listeningOnce.Do(func() { close(e.listening) })
	}
	if c.API.Plaintext && s.Downloads.PublicURL == "" {
		// tracekeep dev's downloads are served where it listens.
		s.Downloads.PublicURL = "http://" + ln.Addr().String()
	}
	if err := e.downloads(ctx, &s.Downloads, m); err != nil {
		return err
	}
	if err := e.sessions(ctx); err != nil {
		return err
	}
	if c.Auth.People.ClientID != "" {
		if s.People, err = e.people(idp, m, log); err != nil {
			return err
		}
		// The handler reads s.Auth only once the server serves, after this.
		s.Auth = authenticator(&c.Auth, idp, s.People, m, log)
	}
	// The admin commands' keys, after downloads named what a missing download key costs; Restore and Digest share them.
	if s.Admin, err = e.Admin(""); err != nil {
		return err
	}
	if s.Keys, err = e.Keys(); err != nil {
		return err
	}
	e.ready("api", e.Catalog.Ping)
	log.Info("api: serving", "listen", ln.Addr().String(), "tls", !c.API.Plaintext, "presigned", s.Downloads.Presigner != nil)
	served := make(chan error, 1)
	go func() {
		if c.API.Plaintext {
			served <- srv.Serve(ln)
		} else {
			served <- srv.ServeTLS(ln, "", "")
		}
	}()
	select {
	case err := <-served:
		return fmt.Errorf("service: api: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cmp.Or(e.apiShutdown, apiShutdown))
	defer cancel()
	err = srv.Shutdown(sctx)
	if err != nil {
		// Past the bound, the connections close, which cancels their requests' contexts, and the handlers end.
		err = errors.Join(fmt.Errorf("service: api: shutdown: %w", err), srv.Close())
	}
	inFlight.Wait()
	if err != nil {
		return err
	}

	return ctx.Err()
}

// verifier is the IdP's verifier of a, shared by service tokens and people's sign-in, or nil without an issuer.
func verifier(a *config.Auth) *auth.Verifier {
	o := a.OIDC
	if o.Issuer == "" {
		return nil
	}

	return auth.NewVerifier(auth.OIDCConfig{
		Issuer: o.Issuer, Audience: o.Audience, PeopleClient: a.People.ClientID, RolesClaim: o.RolesClaim, Roles: o.Roles,
		TTL: o.JWKSTTL, MinInterval: o.FetchInterval, Leeway: o.Leeway,
	})
}

// people is the configured people's sign-in at idp under the session keys (api.md §10.2 *People's sign-in*).
func (e *Env) people(idp *auth.Verifier, m *metrics.API, log *slog.Logger) (*auth.People, error) {
	p := e.Config.Auth.People
	if idp == nil {
		return nil, errors.New("service: api: auth.people needs auth.oidc.issuer")
	}
	k, err := e.Keys()
	if err != nil {
		return nil, err
	}
	cfg := auth.PeopleConfig{
		ClientID: p.ClientID, ClientSecret: p.ClientSecret, PublicURL: e.Config.API.PublicURL, Scopes: p.Scopes,
		RolesClaim: p.RolesClaim, Roles: p.Roles,
		SessionLifetime: p.SessionLifetime, LoginLifetime: p.LoginLifetime, ExchangeTimeout: p.ExchangeTimeout,
		Now: e.Now, Log: log,
	}
	if m != nil {
		cfg.Failures, cfg.Logins = m, m
	}

	return auth.NewPeople(cfg, idp, &auth.Cookies{Keys: k, Leeway: e.Config.Auth.OIDC.Leeway, Now: e.Now}), nil
}

// authenticator builds the checks of a over idp and people's sign-in, nil for none, counting refusals in m.
func authenticator(a *config.Auth, idp *auth.Verifier, people *auth.People, m *metrics.API, log *slog.Logger) *auth.Authenticator {
	cfg := auth.AuthenticatorConfig{MTLS: a.MTLS.Subjects, StaticEnabled: a.Static.Enabled, People: people, Log: log}
	if m != nil {
		cfg.Failures = m
	}
	if idp != nil {
		cfg.OIDC = idp
	}
	for _, t := range a.Static.Tokens {
		cfg.Static = append(cfg.Static, auth.StaticToken{Token: t.Token, Subject: t.Subject, Roles: t.Roles})
	}

	return auth.NewAuthenticator(cfg)
}

// downloads readies dl: presigned URLs where the store presigns and presigning is on, once a presigned GET is shown to work,
// and otherwise download tokens under the download keys, which need a public URL.
func (e *Env) downloads(ctx context.Context, dl *api.Downloads, m *metrics.API) error {
	c := e.Config
	if p, ok := e.Store.(store.Presigner); ok && c.Store.Presign {
		if err := checkPresign(ctx, p); err != nil {
			return err
		}
		dl.Presigner = p

		return nil
	}
	if dl.PublicURL == "" || c.Keys.Download.Issuing == "" {
		return errors.New("service: api: downloads go through the API (the store does not presign, or store.presign is off), " +
			"which needs api.public_url and a keys.download issuing key id")
	}
	k, err := e.Keys()
	if err != nil {
		return err
	}
	dl.Tokens = &auth.Downloads{Keys: k, Now: e.Now}
	if m != nil {
		dl.Tokens.Failures = m
	}
	// The issuing key resolves and binds now, so a bad key fails the start rather than the first URL.
	_, _, err = k.Issuing(ctx, catalog.KindDownload)

	return err
}

// sessions resolves and binds every accepted session key id (masking-keys.md §9, T-48), so this replica validates every cookie
// without the provider once it is ready, and a key it cannot resolve fails the start.
func (e *Env) sessions(ctx context.Context) error {
	if len(e.Config.Keys.Session.Accepted) == 0 {
		return nil
	}
	k, err := e.Keys()
	if err != nil {
		return err
	}
	for _, id := range e.Config.Keys.Session.Accepted {
		if _, err := k.Accepted(ctx, catalog.KindSession, id); err != nil {
			return fmt.Errorf("service: api: session key id %s: %w", id, err)
		}
	}

	return nil
}

// checkPresign presigns a GET of a key that does not exist and fetches it (storage.md §5.2 *Downloads*):
// not-found shows the store accepts the signature, and anything else, a redirect included, fails the start.
// Its errors never carry the signed URL.
func checkPresign(ctx context.Context, p store.Presigner) error {
	u, _, err := p.PresignGet(ctx, "tracekeep-presign-check/"+rand.Text(), time.Now().Add(time.Minute))
	if err != nil {
		return fmt.Errorf("service: api: presign: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return errors.New("service: api: presign: the store's presigned URL does not parse")
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("service: api: the presign check's GET %s; turn store.presign off if readers cannot use the store's presigned URLs", failure(err))
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		// The status code alone: a reason phrase is the store's text, which can quote the signed URL.
		return fmt.Errorf("service: api: the store answers a presigned GET %d, not 404: turn store.presign off if readers cannot use its presigned URLs", resp.StatusCode)
	}

	return nil
}

// failure classifies a failed request in words of its own, since the error's text can quote the signed URL,
// a redirect's Location included.
func failure(err error) string {
	var nerr net.Error
	var terr *tls.CertificateVerificationError
	var oerr *net.OpError
	switch {
	case errors.As(err, &nerr) && nerr.Timeout():
		return "timed out"
	case errors.As(err, &terr):
		return "failed: the store's certificate does not verify"
	case errors.As(err, &oerr) && oerr.Op == "dial":
		return "failed: the store does not answer"
	default:
		return "failed"
	}
}

// APIListening returns a channel closed once the api role's listener is bound, which an Env made by Open has.
func (e *Env) APIListening() <-chan struct{} { return e.listening }

// APIAddr is the address the api role listens on, empty before it is bound.
func (e *Env) APIAddr() string {
	a, _ := e.apiAddr.Load().(string)

	return a
}

// Admin is the admin commands' operations in this process, actor naming the operator in their audit rows.
func (e *Env) Admin(actor string) (*admin.Admin, error) {
	k, err := e.Keys()
	if err != nil {
		return nil, err
	}
	builtin, err := ingest.Builtin(e.Config.Ingest.MaxFrameLen, e.AllowUnversioned)
	if err != nil {
		return nil, err
	}

	return &admin.Admin{
		Catalog: e.Catalog, Bus: e.JS, Config: e.Bus(), Actor: actor, Keys: k, Classifier: builtin.Name, MaxFrameLen: builtin.MaxFrameLen, Dev: e.Dev,
	}, nil
}

// Keys is the process's keys, those this configuration declares over the catalog, built at the first call and shared by every role;
// it resolves nothing until a key is used.
func (e *Env) Keys() (*keys.Keys, error) {
	e.keysMu.Lock()
	defer e.keysMu.Unlock()
	if e.keys != nil {
		return e.keys, nil
	}
	var m keys.Metrics
	if e.Metrics != nil {
		km, err := metrics.NewKeys(e.Metrics)
		if err != nil {
			return nil, err
		}
		m = km
	}
	kc, err := keysConfig(e.Config.Keys, e.Dev, m)
	if err != nil {
		return nil, err
	}
	if e.keys, err = keys.New(kc, e.Catalog); err != nil {
		return nil, err
	}

	return e.keys, nil
}

// keysConfig is the keys module's configuration from the service's: the declarations, the download purpose
// and, when a key id is declared with vault, the vault provider, which calls nothing until a key is first used.
func keysConfig(c config.Keys, dev bool, m keys.Metrics) (keys.Config, error) {
	kc := keys.Config{
		Dev: dev, Declared: map[string]keys.Declaration{}, Metrics: m,
		Download: keys.Purpose{Issuing: c.Download.Issuing, Accepted: c.Download.Accepted},
		Session:  keys.Purpose{Issuing: c.Session.Issuing, Accepted: c.Session.Accepted},
	}
	vault := false
	for id, d := range c.Declared {
		kc.Declared[id] = keys.Declaration{Provider: d.Provider, Ref: d.Ref, Version: d.Version, Kind: d.Kind}
		vault = vault || d.Provider == keys.ProviderVault
	}
	if vault {
		v := c.Vault
		p, err := keys.NewVault(keys.VaultConfig{
			Address: v.Address, CAFile: v.CAFile, Mount: v.Mount, Timeout: v.Timeout,
			Method: v.Auth.Method, AuthMount: v.Auth.Mount, Role: v.Auth.Role, TokenFile: v.Auth.TokenFile,
			RoleID: v.Auth.RoleID, SecretID: v.Auth.SecretID, Metrics: m,
		})
		if err != nil {
			return keys.Config{}, err
		}
		kc.Providers = map[string]keys.Provider{keys.ProviderVault: p}
	}

	return kc, nil
}
