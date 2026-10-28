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
		"an inline static token":  {"auth:\n  static:\n    enabled: true\n    tokens:\n      - {subject: s, token: " + long + ", roles: [viewer]}\n", "token_ref"},
		"a long key ref":          {"keys:\n  declared:\n    dl-1: {provider: file, ref: /" + strings.Repeat("k", 250) + "}\n", "255"},
		"issuer, no audience":     {"auth:\n  oidc:\n    issuer: https://idp.example.invalid\n", "auth.oidc.audience"},
		"plain-text issuer":       {"auth:\n  oidc:\n    issuer: http://idp.example.invalid\n    audience: tk\n", "auth.oidc.issuer"},
		"an unknown role":         {"auth:\n  oidc:\n    issuer: https://idp.example.invalid\n    audience: tk\n    roles:\n      x: [reader]\n", `"reader"`},
		"subjects, no CA":         {"auth:\n  mtls:\n    subjects:\n      spiffe://c.example/x: [viewer]\n", "auth.mtls.client_ca_file"},
		"static on, no token":     {"auth:\n  static:\n    enabled: true\n", "auth.static.tokens"},
		"a short static token":    {"auth:\n  static:\n    enabled: true\n    tokens:\n      - {subject: s, token_ref: \"" + ref("short") + "\", roles: [viewer]}\n", "32"},
		"a static token, no role": {"auth:\n  static:\n    enabled: true\n    tokens:\n      - {subject: s, token_ref: \"" + ref(long) + "\"}\n", "roles"},
		"env outside dev":         {"keys:\n  declared:\n    dl-1: {provider: env, ref: TK_DL}\n", "development"},
		"an unknown provider":     {"keys:\n  declared:\n    dl-1: {provider: vault, ref: kv/dl}\n", "provider"},
		"issuing not accepted":    {"keys:\n  declared:\n    dl-1: {provider: file, ref: /k}\n  download:\n    issuing: dl-1\n    accepted: [dl-0]\n", "keys.download.issuing"},
		"accepted not declared":   {"keys:\n  declared:\n    dl-1: {provider: file, ref: /k}\n  download:\n    issuing: dl-1\n    accepted: [dl-1, dl-0]\n", "dl-0"},
	} {
		if _, err := Load(write(t, minimal+tc.extra)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Load = %v, want an error naming %s", name, err, tc.want)
		}
	}
}
