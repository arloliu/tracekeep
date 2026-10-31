package config

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const authKeys = `auth:
  oidc:
    issuer: https://idp.example.invalid/realms/ve
    audience: tracekeep
    roles:
      ve-generation: [pack-reader]
  mtls:
    client_ca_file: /etc/tracekeep/client-ca.pem
    subjects:
      spiffe://cluster.example/ns/ve/sa/generator: [pack-reader]
keys:
  declared:
    dl-1: {provider: file, ref: /run/keys/dl-1}
  download:
    issuing: dl-1
    accepted: [dl-1]
`

func TestAuthAndKeys(t *testing.T) {
	t.Parallel()
	c, err := Load(write(t, minimal+authKeys))
	if err != nil {
		t.Fatal(err)
	}
	o := c.Auth.OIDC
	if o.Issuer != "https://idp.example.invalid/realms/ve" || o.Audience != "tracekeep" || o.RolesClaim != "roles" ||
		o.JWKSTTL != time.Hour || o.FetchInterval != 10*time.Second || o.Leeway != time.Minute ||
		!slices.Equal(o.Roles["ve-generation"], []string{"pack-reader"}) {
		t.Errorf("auth.oidc = %+v", o)
	}
	if c.Auth.Static.Enabled {
		t.Error("the static-token flag is on by default")
	}
	if !slices.Equal(c.Auth.MTLS.Subjects["spiffe://cluster.example/ns/ve/sa/generator"], []string{"pack-reader"}) {
		t.Errorf("auth.mtls = %+v", c.Auth.MTLS)
	}
	if d := c.Keys.Declared["dl-1"]; d.Provider != "file" || d.Ref != "/run/keys/dl-1" || c.Keys.Download.Issuing != "dl-1" {
		t.Errorf("keys = %+v", c.Keys)
	}
}

// A static token is read by reference, and admitted only under its flag.
func TestStaticTokenByReference(t *testing.T) {
	t.Parallel()
	secret := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(secret, []byte("a-static-token-of-the-standalone-host"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(write(t, minimal+"auth:\n  static:\n    enabled: true\n    tokens:\n      - subject: reader\n        token_ref: file://"+secret+"\n        roles: [pack-reader]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if tok := c.Auth.Static.Tokens; len(tok) != 1 || tok[0].Token != "a-static-token-of-the-standalone-host" || tok[0].Subject != "reader" {
		t.Errorf("auth.static.tokens = %+v", tok)
	}
}

// The file cannot hold a static token: one written beside its reference is ignored, the reference's token read,
// and a reference that does not resolve fails the load.
func TestInlineStaticTokenIgnored(t *testing.T) {
	t.Parallel()
	secret := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(secret, []byte("the-token-the-reference-names-0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	inline := "the-inline-token-in-the-file-0123456789"
	entry := func(ref string) string {
		return minimal + "auth:\n  static:\n    enabled: true\n    tokens:\n      - subject: reader\n        token: " + inline +
			"\n        token_ref: " + ref + "\n        roles: [pack-reader]\n"
	}
	c, err := Load(write(t, entry("file://"+secret)))
	if err != nil {
		t.Fatal(err)
	}
	if tok := c.Auth.Static.Tokens[0].Token; tok != "the-token-the-reference-names-0123456789" {
		t.Errorf("token %q, want the reference's", tok)
	}
	if _, err := Load(write(t, entry("file://"+filepath.Join(t.TempDir(), "absent")))); err == nil {
		t.Error("Load with an inline token and a reference that does not resolve succeeded")
	}
}

func TestAuthAndKeysValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ref := func(token string) string {
		path := filepath.Join(dir, rand.Text())
		if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
			t.Fatal(err)
		}

		return "file://" + path
	}
	long := strings.Repeat("x", 32)
	for name, tc := range map[string]struct{ extra, want string }{
		"an inline static token":        {"auth:\n  static:\n    enabled: true\n    tokens:\n      - {subject: s, token: " + long + ", roles: [viewer]}\n", "token_ref"},
		"a long key ref":                {"keys:\n  declared:\n    dl-1: {provider: file, ref: /" + strings.Repeat("k", 250) + "}\n", "255"},
		"issuer, no audience":           {"auth:\n  oidc:\n    issuer: https://idp.example.invalid\n", "auth.oidc.audience"},
		"plain-text issuer":             {"auth:\n  oidc:\n    issuer: http://idp.example.invalid\n    audience: tk\n", "auth.oidc.issuer"},
		"an unknown role":               {"auth:\n  oidc:\n    issuer: https://idp.example.invalid\n    audience: tk\n    roles:\n      x: [reader]\n", `"reader"`},
		"subjects, no CA":               {"auth:\n  mtls:\n    subjects:\n      spiffe://c.example/x: [viewer]\n", "auth.mtls.client_ca_file"},
		"static on, no token":           {"auth:\n  static:\n    enabled: true\n", "auth.static.tokens"},
		"a short static token":          {"auth:\n  static:\n    enabled: true\n    tokens:\n      - {subject: s, token_ref: \"" + ref("short") + "\", roles: [viewer]}\n", "32"},
		"a static token, no role":       {"auth:\n  static:\n    enabled: true\n    tokens:\n      - {subject: s, token_ref: \"" + ref(long) + "\"}\n", "roles"},
		"env outside dev":               {"keys:\n  declared:\n    dl-1: {provider: env, ref: TK_DL}\n", "development"},
		"an unknown provider":           {"keys:\n  declared:\n    dl-1: {provider: transit, ref: kv/dl}\n", "provider"},
		"vault, no address":             {"keys:\n  declared:\n    d-1: {provider: vault, ref: tk/d-1, kind: digest}\n", "keys.vault.address"},
		"an unknown kind":               {"keys:\n  declared:\n    d-1: {provider: file, ref: /k, kind: cookie}\n", "kind"},
		"issuing not accepted":          {"keys:\n  declared:\n    dl-1: {provider: file, ref: /k}\n  download:\n    issuing: dl-1\n    accepted: [dl-0]\n", "keys.download.issuing"},
		"accepted not declared":         {"keys:\n  declared:\n    dl-1: {provider: file, ref: /k}\n  download:\n    issuing: dl-1\n    accepted: [dl-1, dl-0]\n", "dl-0"},
		"session issuing not accepted":  {"keys:\n  declared:\n    s-1: {provider: file, ref: /k, kind: session}\n  session:\n    issuing: s-1\n    accepted: [s-0]\n", "keys.session.issuing"},
		"session accepted not declared": {"keys:\n  declared:\n    s-1: {provider: file, ref: /k, kind: session}\n  session:\n    issuing: s-1\n    accepted: [s-1, s-0]\n", "keys.session.accepted"},
	} {
		if _, err := Load(write(t, minimal+tc.extra)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Load = %v, want an error naming %s", name, err, tc.want)
		}
	}
}

// People's sign-in with its defaults, its secret read by reference.
func TestPeopleSignIn(t *testing.T) {
	t.Parallel()
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("client-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(write(t, minimal+people(secret)))
	if err != nil {
		t.Fatal(err)
	}
	p := c.Auth.People
	if p.ClientID != "tk-web" || p.ClientSecret != "client-secret" || p.RolesClaim != "groups" || p.SessionLifetime != 8*time.Hour ||
		p.LoginLifetime != 10*time.Minute || p.ExchangeTimeout != 10*time.Second || p.Roles["tk-viewers"][0] != "viewer" {
		t.Errorf("auth.people = %+v", p)
	}
}

// people is a configuration of people's sign-in whose client secret is at secret.
func people(secret string) string {
	return "api:\n  public_url: https://tk.example.invalid\n" +
		"auth:\n  oidc:\n    issuer: https://idp.example.invalid\n    audience: tk-services\n" +
		"  people:\n    client_id: tk-web\n    client_secret_ref: \"file://" + secret + "\"\n    roles:\n      tk-viewers: [viewer]\n" +
		"keys:\n  declared:\n    s-1: {provider: file, ref: /k, kind: session}\n  session:\n    issuing: s-1\n    accepted: [s-1]\n"
}

func TestPeopleSignInValidation(t *testing.T) {
	t.Parallel()
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("client-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := people(secret)
	for name, tc := range map[string]struct{ yaml, want string }{
		"no issuer":            {strings.Replace(base, "    issuer: https://idp.example.invalid\n    audience: tk-services\n", "", 1), "auth.oidc.issuer"},
		"the service audience": {strings.Replace(base, "audience: tk-services", "audience: tk-web", 1), "auth.oidc.audience"},
		"no secret":            {strings.Replace(base, "    client_secret_ref: \"file://"+secret+"\"\n", "", 1), "client_secret_ref"},
		"no public URL":        {strings.Replace(base, "api:\n  public_url: https://tk.example.invalid\n", "", 1), "api.public_url"},
		"no session key":       {strings.Replace(base, "  session:\n    issuing: s-1\n    accepted: [s-1]\n", "", 1), "keys.session"},
		"an unknown role":      {strings.Replace(base, "tk-viewers: [viewer]", "tk-viewers: [reader]", 1), `"reader"`},
		"a scope with a space": {strings.Replace(base, "    client_id: tk-web\n", "    client_id: tk-web\n    scopes: [\"groups email\"]\n", 1), "auth.people.scopes"},
	} {
		if _, err := Load(write(t, minimal+tc.yaml)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Load = %v, want an error naming %s", name, err, tc.want)
		}
	}
}

// The session purpose names its key ids as the download purpose does, and a key id may be declared of kind session.
func TestSessionKeys(t *testing.T) {
	t.Parallel()
	c, err := Load(write(t, minimal+"keys:\n  declared:\n    s-0: {provider: file, ref: /k0, kind: session}\n    s-1: {provider: file, ref: /k1, kind: session}\n"+
		"  session:\n    issuing: s-1\n    accepted: [s-0, s-1]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Keys.Session; s.Issuing != "s-1" || len(s.Accepted) != 2 || c.Keys.Declared["s-0"].Kind != "session" {
		t.Errorf("keys = %+v, want the session purpose s-1 of s-0 and s-1", c.Keys)
	}
}

// The vault provider's keys, with their defaults, and a secret id read by reference.
func TestVaultKeys(t *testing.T) {
	t.Parallel()
	secret := filepath.Join(t.TempDir(), "secret-id")
	if err := os.WriteFile(secret, []byte("synthetic-secret-id"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(write(t, minimal+"keys:\n  declared:\n    m-1: {provider: vault, ref: tk/m-1, version: \"3\", kind: master}\n"+
		"  vault:\n    address: https://vault.example.invalid:8200\n    auth:\n      method: approle\n      role_id: r-1\n      secret_id_ref: file://"+secret+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	v := c.Keys.Vault
	if v.Mount != "secret" || v.Timeout != 10*time.Second || v.Auth.TokenFile != "/var/run/secrets/kubernetes.io/serviceaccount/token" ||
		v.Auth.RoleID != "r-1" || v.Auth.SecretID != "synthetic-secret-id" {
		t.Errorf("keys.vault = %+v", v)
	}
	if d := c.Keys.Declared["m-1"]; d.Kind != "master" || d.Version != "3" {
		t.Errorf("keys.declared.m-1 = %+v", d)
	}
}
