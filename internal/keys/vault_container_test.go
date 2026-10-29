package keys

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// The dev-mode Vault server scripts/test-vault.sh starts: its address, the CA of its certificate and its root token.
const (
	envVaultAddr  = "TRACEKEEP_VAULT_ADDR"
	envVaultCA    = "TRACEKEEP_VAULT_CA_FILE"
	envVaultToken = "TRACEKEEP_VAULT_TOKEN"
)

// devVault is a dev-mode Vault server, reached as its root to set up what the provider then reads as an AppRole.
type devVault struct {
	t      *testing.T
	addr   string
	ca     string
	root   string
	client *http.Client
	// prefix is this test's own path under the KV v2 mount and the name of its policy and role.
	prefix string
}

func newDevVault(t *testing.T) *devVault {
	t.Helper()
	addr := os.Getenv(envVaultAddr)
	if addr == "" {
		t.Skipf("%s not set: no Vault server to test against (make test-vault)", envVaultAddr)
	}
	v := mustVault(t, VaultConfig{Address: addr, CAFile: os.Getenv(envVaultCA), Mount: "secret", Timeout: 5 * time.Second, Method: VaultAppRole, RoleID: "x", SecretID: "x"})
	d := &devVault{t: t, addr: addr, ca: os.Getenv(envVaultCA), root: os.Getenv(envVaultToken), client: v.client, prefix: "tk-" + strings.ToLower(rand.Text()[:12])}
	// Enabling AppRole a second time is refused; the first test to ask enables it.
	d.call(http.MethodPost, "sys/auth/approle", map[string]any{"type": "approle"}, nil, http.StatusBadRequest)
	d.call(http.MethodPut, "sys/policies/acl/"+d.prefix, map[string]any{"policy": `path "secret/data/` + d.prefix + `/*" { capabilities = ["read"] }`}, nil)
	d.call(http.MethodPost, "auth/approle/role/"+d.prefix, map[string]any{"token_policies": d.prefix, "token_ttl": "60s", "token_max_ttl": "120s"}, nil)

	return d
}

// call runs one request as root and decodes its response into out; a status in also is accepted beside 2xx.
func (d *devVault) call(method, path string, body, out any, also ...int) {
	d.t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		d.t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(d.t.Context(), method, d.addr+"/v1/"+path, bytes.NewReader(b))
	if err != nil {
		d.t.Fatal(err)
	}
	req.Header.Set("X-Vault-Token", d.root)
	resp, err := d.client.Do(req)
	if err != nil {
		d.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 && !slices.Contains(also, resp.StatusCode) {
		d.t.Fatalf("%s %s: status %d", method, path, resp.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			d.t.Fatal(err)
		}
	}
}

// put writes m as the secret name's next version.
func (d *devVault) put(name string, m []byte) {
	d.call(http.MethodPost, "secret/data/"+d.prefix+"/"+name, map[string]any{"data": map[string]string{"material": base64.StdEncoding.EncodeToString(m)}}, nil)
}

// appRole returns a provider configuration logging in as this test's role, with a fresh secret id.
func (d *devVault) appRole() VaultConfig {
	var roleID, secretID struct {
		Data map[string]any `json:"data"`
	}
	d.call(http.MethodGet, "auth/approle/role/"+d.prefix+"/role-id", nil, &roleID)
	d.call(http.MethodPost, "auth/approle/role/"+d.prefix+"/secret-id", map[string]any{}, &secretID)

	return VaultConfig{
		Address: d.addr, CAFile: d.ca, Mount: "secret", Timeout: 5 * time.Second,
		Method: VaultAppRole, RoleID: fmt.Sprint(roleID.Data["role_id"]), SecretID: fmt.Sprint(secretID.Data["secret_id"]),
	}
}

func TestDevVaultReadsVersions(t *testing.T) {
	t.Parallel()
	d := newDevVault(t)
	m1, m2 := material(0x51, 32), material(0x52, 32)
	d.put("m-1", m1)
	d.put("m-1", m2)
	v := mustVault(t, d.appRole())
	ref := d.prefix + "/m-1"
	if got, ver, err := v.Material(t.Context(), ref, "1"); err != nil || ver != "1" || !bytes.Equal(got, m1) {
		t.Errorf("Material at version 1 = %x, %q, %v", got, ver, err)
	}
	if got, ver, err := v.Material(t.Context(), ref, ""); err != nil || ver != "2" || !bytes.Equal(got, m2) {
		t.Errorf("Material at the current version = %x, %q, %v", got, ver, err)
	}
	// A deleted pinned version is missing beside the current one, and a destroyed one likewise.
	d.call(http.MethodPost, "secret/delete/"+ref, map[string]any{"versions": []int{1}}, nil)
	d.call(http.MethodPut, "secret/destroy/"+ref, map[string]any{"versions": []int{2}}, nil)
	for _, ver := range []string{"1", "2"} {
		_, _, err := v.Material(t.Context(), ref, ver)
		var pe *ProviderError
		if !errors.As(err, &pe) || pe.Reason != "missing" {
			t.Errorf("Material of removed version %s = %v, want missing", ver, err)
		}
	}
	// A path outside the role's policy is refused after one fresh login.
	_, _, err := v.Material(t.Context(), "elsewhere/m-1", "")
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "auth") {
		t.Errorf("Material outside the policy = %v, want an auth failure", err)
	}
}

// Through the keys module on both catalog engines: a vault master key id binds at the version read,
// and a fresh process derives the same capture key from that version after a newer one is written.
func TestDevVaultKeyBindsItsVersion(t *testing.T) {
	t.Parallel()
	d := newDevVault(t)
	catalogtest.Engines(t, func(t *testing.T, cat *catalog.Catalog) {
		name := fmt.Sprintf("m-%s", strings.ToLower(rand.Text()[:8]))
		d.put(name, material(0x53, 32))
		cfg := Config{
			Declared:  map[string]Declaration{"m-1": {Provider: ProviderVault, Ref: d.prefix + "/" + name, Kind: catalog.KindMaster}},
			Providers: map[string]Provider{ProviderVault: mustVault(t, d.appRole())},
		}
		first, err := newKeys(t, cfg, cat).Encryption(t.Context(), testCapture, "m-1")
		if err != nil {
			t.Fatal(err)
		}
		if b, _, _ := cat.KeyBinding(t.Context(), "m-1"); b.ProviderVersion != "1" || b.ProviderRef != ProviderVault+":"+d.prefix+"/"+name {
			t.Errorf("binding = %+v, want version 1 at the declared reference", b)
		}
		d.put(name, material(0x54, 32))
		cfg.Providers[ProviderVault] = mustVault(t, d.appRole())
		if again, err := newKeys(t, cfg, cat).Encryption(t.Context(), testCapture, "m-1"); err != nil || !bytes.Equal(again, first) {
			t.Errorf("a fresh process's key = %x, %v; want the first", again, err)
		}
	})
}
