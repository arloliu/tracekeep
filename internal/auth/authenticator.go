package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
)

// Operations, as the role table names them (api.md §10.2 *Roles*).
const (
	OpListDevices      = "ListDevices"
	OpListCaptures     = "ListCaptures"
	OpListPacks        = "ListPacks"
	OpListScopeChanges = "ListScopeChanges"
	OpPackURL          = "PackURL"
	OpDigest           = "Digest"
	OpRestore          = "Restore"
	OpExport           = "Export"
	OpAdmin            = "Admin"
)

// operationRoles are the roles any one of which admits each operation (api.md §10.2, T-41);
// an operation it does not name is refused.
var operationRoles = map[string][]string{
	OpListDevices: {RolePackReader, RoleViewer}, OpListCaptures: {RolePackReader, RoleViewer}, OpListScopeChanges: {RolePackReader, RoleViewer},
	OpListPacks: {RolePackReader}, OpPackURL: {RolePackReader},
	OpDigest: {RoleScorer}, OpRestore: {RoleRestorer}, OpExport: {RoleExporter}, OpAdmin: {RoleAdmin},
}

// Authorize admits p to op when p holds one of the roles the table names for op, and refuses it as forbidden otherwise.
func Authorize(p Principal, op string) error {
	for _, r := range operationRoles[op] {
		if slices.Contains(p.Roles, r) {
			return nil
		}
	}

	return &Error{Reason: ReasonForbidden, Subject: p.Subject, Err: errors.New("no role admits " + op)}
}

// StaticToken is a static token of a standalone host without an IdP, with the principal it authenticates.
type StaticToken struct {
	Token, Subject string
	Roles          []string
}

// Failures counts refused requests by reason; a *metrics.API is one.
type Failures interface {
	AuthFailure(reason string)
}

// AuthenticatorConfig is how services authenticate (api.md §10.2).
type AuthenticatorConfig struct {
	// OIDC validates service tokens; nil when no IdP is configured.
	OIDC *Verifier
	// MTLS maps a verified client certificate's identity to its roles; nil when mTLS is not configured.
	MTLS map[string][]string
	// StaticEnabled is the explicit flag that alone admits Static's tokens, whatever the IdP's state.
	StaticEnabled bool
	Static        []StaticToken
	// Failures counts refusals and Log logs them, each with its reason and subject, never the credential; nil for neither.
	Failures Failures
	Log      *slog.Logger
}

// Authenticator authenticates a service's request to a principal.
type Authenticator struct {
	cfg AuthenticatorConfig
	// static holds the static tokens' digests, compared in constant time.
	static [][sha256.Size]byte
}

// NewAuthenticator returns the authenticator of cfg; it fetches nothing.
func NewAuthenticator(cfg AuthenticatorConfig) *Authenticator {
	a := &Authenticator{cfg: cfg}
	if cfg.StaticEnabled {
		for _, s := range cfg.Static {
			a.static = append(a.static, sha256.Sum256([]byte(s.Token)))
		}
	}

	return a
}

// Check authenticates r and authorizes it for op, counting and logging a refusal.
func (a *Authenticator) Check(r *http.Request, op string) (Principal, error) {
	p, err := a.Authenticate(r)
	if err == nil {
		err = Authorize(p, op)
	}
	if err != nil {
		a.refused(err, op)
		return Principal{}, err
	}

	return p, nil
}

func (a *Authenticator) refused(err error, op string) {
	var e *Error
	if !errors.As(err, &e) {
		e = &Error{Reason: ReasonMalformed, Err: err}
	}
	if a.cfg.Failures != nil {
		a.cfg.Failures.AuthFailure(e.Reason)
	}
	if a.cfg.Log != nil {
		a.cfg.Log.Warn("auth: refused", "reason", e.Reason, "subject", e.Subject, "operation", op, "error", e.Err)
	}
}

// Authenticate returns the principal of r's one credential:
// the client certificate the TLS handshake verified, when mTLS is configured, and otherwise its bearer token,
// matched against the static tokens under their flag and else validated against the IdP.
func (a *Authenticator) Authenticate(r *http.Request) (Principal, error) {
	if a.cfg.MTLS != nil && r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		return a.certificate(r)
	}
	h := r.Header.Get("Authorization")
	if h == "" {
		return Principal{}, refuse(ReasonMissing, errors.New("no credential"))
	}
	scheme, token, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return Principal{}, refuse(ReasonMalformed, errors.New("not a bearer token"))
	}
	if p, ok := a.matchStatic(token); ok {
		return p, nil
	}
	if a.cfg.OIDC == nil {
		return Principal{}, refuse(ReasonStatic, errors.New("no static token matches and no IdP is configured"))
	}

	return a.cfg.OIDC.Verify(r.Context(), token)
}

// certificate is the principal of r's verified client certificate: its first URI name, else its first DNS name.
func (a *Authenticator) certificate(r *http.Request) (Principal, error) {
	leaf := r.TLS.VerifiedChains[0][0]
	var id string
	switch {
	case len(leaf.URIs) > 0:
		id = leaf.URIs[0].String()
	case len(leaf.DNSNames) > 0:
		id = leaf.DNSNames[0]
	default:
		return Principal{}, refuse(ReasonCertificate, errors.New("a client certificate with no URI or DNS name"))
	}
	roles, ok := a.cfg.MTLS[id]
	if !ok {
		return Principal{}, &Error{Reason: ReasonCertificate, Subject: id, Err: errors.New("an identity the configuration does not name")}
	}

	return Principal{Subject: id, Roles: slices.Clone(roles), Via: ViaMTLS}, nil
}

// matchStatic compares token with every static token in constant time, by their digests; there is none while the flag is off.
func (a *Authenticator) matchStatic(token string) (Principal, bool) {
	sum := sha256.Sum256([]byte(token))
	match := -1
	for i, s := range a.static {
		if subtle.ConstantTimeCompare(sum[:], s[:]) == 1 && match < 0 {
			match = i
		}
	}
	if match < 0 {
		return Principal{}, false
	}
	s := a.cfg.Static[match]

	return Principal{Subject: s.Subject, Roles: slices.Clone(s.Roles), Via: ViaStatic}, true
}
