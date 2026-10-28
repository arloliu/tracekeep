// Package auth authenticates a service's request to a principal and authorizes an operation by its role (api.md §10.2):
// a service token validated against the IdP's JWKS, a client certificate the TLS handshake verified,
// or, behind an explicit flag, a static token; and the download tokens of api.md §10.1 *Downloads*.
// It serves nothing itself: the API puts it in front of every request.
package auth

import "fmt"

// Roles (api.md §10.2), granted separately.
const (
	RolePackReader = "pack-reader"
	RoleScorer     = "scorer"
	RoleViewer     = "viewer"
	RoleExporter   = "exporter"
	RoleRestorer   = "restorer"
	RoleAdmin      = "admin"
)

// How a principal was authenticated.
const (
	ViaOIDC   = "oidc"
	ViaMTLS   = "mtls"
	ViaStatic = "static"
)

// Reasons of a refused request, the label of auth_failures_total (api.md §10.2 *Failures*).
const (
	ReasonMissing        = "missing"
	ReasonMalformed      = "malformed"
	ReasonAlg            = "alg"
	ReasonUnknownKid     = "unknown-kid"
	ReasonSignature      = "signature"
	ReasonIssuer         = "issuer"
	ReasonAudience       = "audience"
	ReasonExpired        = "expired"
	ReasonNotYetValid    = "not-yet-valid"
	ReasonIDPUnavailable = "idp-unavailable"
	ReasonCertificate    = "certificate"
	ReasonStatic         = "static"
	ReasonForbidden      = "forbidden"
	ReasonDownload       = "download"
)

// Principal is an authenticated caller: its subject, its roles and how it was authenticated.
type Principal struct {
	Subject string
	Roles   []string
	Via     string
}

// Error is a refused request: its reason, the subject when one is known, and the cause.
// Its text never holds the credential.
type Error struct {
	Reason  string
	Subject string
	Err     error
}

func (e *Error) Error() string {
	if e.Subject != "" {
		return fmt.Sprintf("auth: %s (subject %s): %v", e.Reason, e.Subject, e.Err)
	}

	return fmt.Sprintf("auth: %s: %v", e.Reason, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

func refuse(reason string, err error) *Error { return &Error{Reason: reason, Err: err} }
