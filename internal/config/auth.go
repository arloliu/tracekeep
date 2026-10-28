package config

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"

	"github.com/arloliu/tracekeep/internal/auth"
)

// minStaticToken is the least length of a static token.
const minStaticToken = 32

// Auth is how services authenticate to the API (api.md §10.2); the api role builds its checks from it.
type Auth struct {
	OIDC   OIDC   `yaml:"oidc"`
	MTLS   MTLS   `yaml:"mtls"`
	Static Static `yaml:"static"`
}

// OIDC is the IdP service tokens are validated against; an empty issuer configures none.
type OIDC struct {
	Issuer   string `yaml:"issuer" env:"TRACEKEEP_AUTH_OIDC_ISSUER"`
	Audience string `yaml:"audience" env:"TRACEKEEP_AUTH_OIDC_AUDIENCE"`
	// RolesClaim names the claim holding a token's role values, and Roles maps each value to the roles it grants.
	RolesClaim string              `yaml:"roles_claim" default:"roles" env:"TRACEKEEP_AUTH_OIDC_ROLES_CLAIM"`
	Roles      map[string][]string `yaml:"roles"`
	// JWKSTTL is how long a fetched JWKS verifies, FetchInterval the least time between two fetches, Leeway the clock tolerance.
	JWKSTTL       time.Duration `yaml:"jwks_ttl" default:"1h" env:"TRACEKEEP_AUTH_OIDC_JWKS_TTL" validate:"gt=0"`
	FetchInterval time.Duration `yaml:"fetch_interval" default:"10s" env:"TRACEKEEP_AUTH_OIDC_FETCH_INTERVAL" validate:"gt=0"`
	Leeway        time.Duration `yaml:"leeway" default:"1m" env:"TRACEKEEP_AUTH_OIDC_LEEWAY" validate:"gte=0"`
}

// MTLS is the in-cluster alternative: client certificates verified against a CA, their identities mapped to roles.
type MTLS struct {
	ClientCAFile string              `yaml:"client_ca_file" env:"TRACEKEEP_AUTH_MTLS_CLIENT_CA_FILE"`
	Subjects     map[string][]string `yaml:"subjects"`
}

// Static is the static-token provider of a standalone host without an IdP, admitted only by its explicit flag.
type Static struct {
	Enabled bool          `yaml:"enabled" env:"TRACEKEEP_AUTH_STATIC_ENABLED"`
	Tokens  []StaticToken `yaml:"tokens"`
}

// StaticToken is one static token, read by reference, with the principal it authenticates.
type StaticToken struct {
	Subject  string   `yaml:"subject"`
	TokenRef string   `yaml:"token_ref"`
	Token    string   `yaml:"token" refFrom:"TokenRef"`
	Roles    []string `yaml:"roles"`
}

// Keys is the keys the service declares and the purposes they serve (masking-keys.md §9).
type Keys struct {
	Declared map[string]KeyDeclaration `yaml:"declared"`
	Download KeyPurpose                `yaml:"download"`
}

// KeyDeclaration names a key id's provider, file or env, its reference there and, optionally, its version.
type KeyDeclaration struct {
	Provider string `yaml:"provider"`
	Ref      string `yaml:"ref"`
	Version  string `yaml:"version"`
}

// KeyPurpose is a purpose's issuing key id and the key ids it accepts.
type KeyPurpose struct {
	Issuing  string   `yaml:"issuing"`
	Accepted []string `yaml:"accepted"`
}

func (a *Auth) check() []error {
	var errs []error
	roles := func(key string, rs []string) {
		for _, r := range rs {
			if !auth.KnownRole(r) {
				errs = append(errs, fmt.Errorf("%s: %q is no role", key, r))
			}
		}
	}
	if o := a.OIDC; o.Issuer != "" {
		if u, err := url.Parse(o.Issuer); err != nil || u.Scheme != "https" || u.Host == "" {
			errs = append(errs, fmt.Errorf("auth.oidc.issuer %q must be an https URL", o.Issuer))
		}
		if o.Audience == "" {
			errs = append(errs, errors.New("auth.oidc.audience is required with an issuer"))
		}
		for v, rs := range o.Roles {
			roles("auth.oidc.roles."+v, rs)
		}
	}
	if (a.MTLS.ClientCAFile == "") != (len(a.MTLS.Subjects) == 0) {
		errs = append(errs, errors.New("auth.mtls.client_ca_file and auth.mtls.subjects go together"))
	}
	for id, rs := range a.MTLS.Subjects {
		roles("auth.mtls.subjects."+id, rs)
	}
	if a.Static.Enabled {
		if len(a.Static.Tokens) == 0 {
			errs = append(errs, errors.New("auth.static.tokens: the flag is on and no token is configured"))
		}
		for i, s := range a.Static.Tokens {
			key := fmt.Sprintf("auth.static.tokens[%d]", i)
			if s.Subject == "" || len(s.Roles) == 0 {
				errs = append(errs, fmt.Errorf("%s needs a subject and roles", key))
			}
			if len(s.Token) < minStaticToken {
				errs = append(errs, fmt.Errorf("%s: a token holds at least %d characters", key, minStaticToken))
			}
			roles(key+".roles", s.Roles)
		}
	}

	return errs
}

// check refuses a provider other than file and env, env outside development,
// and a purpose whose issuing key id it does not accept or whose accepted key ids are not declared.
func (k *Keys) check(dev bool) []error {
	var errs []error
	for id, d := range k.Declared {
		switch {
		case d.Provider == "env" && !dev:
			errs = append(errs, fmt.Errorf("keys.declared.%s: the env provider is for development only", id))
		case d.Provider != "file" && d.Provider != "env":
			errs = append(errs, fmt.Errorf("keys.declared.%s: unknown provider %q", id, d.Provider))
		case d.Ref == "":
			errs = append(errs, fmt.Errorf("keys.declared.%s: no ref", id))
		default:
		}
	}
	if p := k.Download; p.Issuing != "" || len(p.Accepted) > 0 {
		if !slices.Contains(p.Accepted, p.Issuing) {
			errs = append(errs, fmt.Errorf("keys.download.issuing %q must be one of keys.download.accepted", p.Issuing))
		}
		for _, id := range p.Accepted {
			if _, ok := k.Declared[id]; !ok {
				errs = append(errs, fmt.Errorf("keys.download.accepted: key id %s is not declared", id))
			}
		}
	}

	return errs
}
