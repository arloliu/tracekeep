package config

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/arloliu/tracekeep/internal/auth"
)

// minStaticToken is the least length of a static token.
const minStaticToken = 32

// Auth is how services authenticate to the API (api.md §10.2); the api role builds its checks from it.
type Auth struct {
	OIDC   OIDC   `yaml:"oidc"`
	MTLS   MTLS   `yaml:"mtls"`
	Static Static `yaml:"static"`
	People People `yaml:"people"`
}

// People is people's sign-in (api.md §10.2 *People's sign-in*): a client of its own at auth.oidc's IdP; an empty client id configures none.
type People struct {
	ClientID string `yaml:"client_id" env:"TRACEKEEP_AUTH_PEOPLE_CLIENT_ID"`
	// ClientSecretRef names where the client's secret is read from; the secret is never written in the file.
	ClientSecretRef string `yaml:"client_secret_ref" env:"TRACEKEEP_AUTH_PEOPLE_CLIENT_SECRET_REF"`
	ClientSecret    string `yaml:"-" refFrom:"ClientSecretRef"`
	// Scopes are asked for beside openid.
	Scopes []string `yaml:"scopes"`
	// RolesClaim names the ID token claim holding a person's group values, and Roles maps each value to the roles it grants.
	RolesClaim string              `yaml:"roles_claim" default:"groups" env:"TRACEKEEP_AUTH_PEOPLE_ROLES_CLAIM"`
	Roles      map[string][]string `yaml:"roles"`
	// SessionLifetime bounds a session, LoginLifetime a started login, ExchangeTimeout the token exchange.
	SessionLifetime time.Duration `yaml:"session_lifetime" default:"8h" env:"TRACEKEEP_AUTH_PEOPLE_SESSION_LIFETIME" validate:"gt=0"`
	LoginLifetime   time.Duration `yaml:"login_lifetime" default:"10m" env:"TRACEKEEP_AUTH_PEOPLE_LOGIN_LIFETIME" validate:"gt=0"`
	ExchangeTimeout time.Duration `yaml:"exchange_timeout" default:"10s" env:"TRACEKEEP_AUTH_PEOPLE_EXCHANGE_TIMEOUT" validate:"gt=0"`
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
	Subject  string `yaml:"subject"`
	TokenRef string `yaml:"token_ref"`
	// Token is read from TokenRef only: the file cannot hold it.
	Token string   `yaml:"-" refFrom:"TokenRef"`
	Roles []string `yaml:"roles"`
}

// Keys is the keys the service declares and the purposes they serve (masking-keys.md §9).
type Keys struct {
	Declared map[string]KeyDeclaration `yaml:"declared"`
	Download KeyPurpose                `yaml:"download"`
	// Session is the session and login cookies' purpose (api.md §10.2).
	Session KeyPurpose `yaml:"session"`
	Vault   Vault      `yaml:"vault"`
}

// Vault is the vault key provider's server and login (masking-keys.md §9); read only when a key id is declared with vault.
type Vault struct {
	// Address is the server's https URL; CAFile verifies it beside the system's roots.
	Address string `yaml:"address" env:"TRACEKEEP_KEYS_VAULT_ADDRESS"`
	CAFile  string `yaml:"ca_file" env:"TRACEKEEP_KEYS_VAULT_CA_FILE"`
	// Mount is the KV v2 mount the references are under.
	Mount string `yaml:"mount" default:"secret" env:"TRACEKEEP_KEYS_VAULT_MOUNT"`
	// Timeout bounds every call to the server.
	Timeout time.Duration `yaml:"timeout" default:"10s" env:"TRACEKEEP_KEYS_VAULT_TIMEOUT" validate:"gt=0"`
	Auth    VaultAuth     `yaml:"auth"`
}

// VaultAuth is how the service logs in to Vault: kubernetes with its service account token, or approle.
type VaultAuth struct {
	Method string `yaml:"method" env:"TRACEKEEP_KEYS_VAULT_AUTH_METHOD"`
	// Mount is the auth mount; empty takes the method's name.
	Mount string `yaml:"mount" env:"TRACEKEEP_KEYS_VAULT_AUTH_MOUNT"`
	Role  string `yaml:"role" env:"TRACEKEEP_KEYS_VAULT_AUTH_ROLE"`
	// TokenFile is the service account token kubernetes logs in with, read at each login.
	TokenFile string `yaml:"token_file" default:"/var/run/secrets/kubernetes.io/serviceaccount/token" env:"TRACEKEEP_KEYS_VAULT_AUTH_TOKEN_FILE"`
	RoleID    string `yaml:"role_id" env:"TRACEKEEP_KEYS_VAULT_AUTH_ROLE_ID"`
	// SecretIDRef names where approle's secret id is read from; the secret id is never written in the file.
	SecretIDRef string `yaml:"secret_id_ref" env:"TRACEKEEP_KEYS_VAULT_AUTH_SECRET_ID_REF"`
	SecretID    string `yaml:"-" refFrom:"SecretIDRef"`
}

// KeyDeclaration names a key id's provider, its reference there and, optionally, its version,
// and its kind, which a digest or master key id names (masking-keys.md §9).
type KeyDeclaration struct {
	Provider string `yaml:"provider"`
	Ref      string `yaml:"ref"`
	Version  string `yaml:"version"`
	Kind     string `yaml:"kind"`
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
	for v, rs := range a.People.Roles {
		roles("auth.people.roles."+v, rs)
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
			if s.TokenRef == "" {
				errs = append(errs, fmt.Errorf("%s needs a token_ref: a token is read by reference, never written in the file", key))
			} else if len(s.Token) < minStaticToken {
				errs = append(errs, fmt.Errorf("%s: the token its reference names holds at least %d bytes", key, minStaticToken))
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
		if err := k.checkDeclaration(id, d, dev); err != nil {
			errs = append(errs, err)
		}
	}
	errs = append(errs, k.checkPurpose("download", k.Download)...)

	return append(errs, k.checkPurpose("session", k.Session)...)
}

// checkPurpose refuses a purpose whose issuing key id it does not accept or whose accepted key ids are not declared.
func (k *Keys) checkPurpose(name string, p KeyPurpose) []error {
	if p.Issuing == "" && len(p.Accepted) == 0 {
		return nil
	}
	var errs []error
	if !slices.Contains(p.Accepted, p.Issuing) {
		errs = append(errs, fmt.Errorf("keys.%s.issuing %q must be one of keys.%s.accepted", name, p.Issuing, name))
	}
	for _, id := range p.Accepted {
		if _, ok := k.Declared[id]; !ok {
			errs = append(errs, fmt.Errorf("keys.%s.accepted: key id %s is not declared", name, id))
		}
	}

	return errs
}

// checkDeclaration checks one key id's declaration.
func (k *Keys) checkDeclaration(id string, d KeyDeclaration, dev bool) error {
	switch {
	case d.Provider == "env" && !dev:
		return fmt.Errorf("keys.declared.%s: the env provider is for development only", id)
	case d.Provider != "file" && d.Provider != "env" && d.Provider != "vault":
		return fmt.Errorf("keys.declared.%s: unknown provider %q", id, d.Provider)
	case d.Provider == "vault" && k.Vault.Address == "":
		return fmt.Errorf("keys.declared.%s: the vault provider needs keys.vault.address", id)
	case d.Kind != "" && d.Kind != "digest" && d.Kind != "master" && d.Kind != "download" && d.Kind != "session":
		return fmt.Errorf("keys.declared.%s: unknown kind %q", id, d.Kind)
	case d.Ref == "":
		return fmt.Errorf("keys.declared.%s: no ref", id)
	case utf8.RuneCountInString(id) > 128 || utf8.RuneCountInString(d.Provider+":"+d.Ref) > 255 || utf8.RuneCountInString(d.Version) > 64:
		// A binding records them whole (masking-keys.md §9), in columns of these lengths.
		return fmt.Errorf("keys.declared.%s: a key id above 128 characters, a provider and ref above 255 or a version above 64", id)
	default:
	}

	return nil
}
