package service

import (
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/keys"
)

// A key id declared with vault brings the vault provider, built from keys.vault; none declared, none is built.
func TestKeysConfigBuildsVaultOnlyWhenDeclared(t *testing.T) {
	t.Parallel()
	c := config.Keys{
		Declared: map[string]config.KeyDeclaration{"m-1": {Provider: keys.ProviderVault, Ref: "tk/m-1", Kind: "master"}},
		Vault: config.Vault{
			Address: "https://vault.example.invalid:8200", Mount: "secret", Timeout: 10 * time.Second,
			Auth: config.VaultAuth{Method: keys.VaultAppRole, RoleID: "r-1", SecretID: "s-1"},
		},
	}
	kc, err := keysConfig(c, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := kc.Providers[keys.ProviderVault]; !ok || kc.Declared["m-1"].Kind != "master" {
		t.Errorf("keys config = %+v, want the vault provider and m-1 as master", kc)
	}
	c.Vault.Auth.SecretID = ""
	if _, err := keysConfig(c, false); err == nil {
		t.Error("keysConfig with approle and no secret id succeeded")
	}
	c.Declared["m-1"] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: "/k", Kind: "master"}
	if kc, err := keysConfig(c, false); err != nil || kc.Providers != nil {
		t.Errorf("keysConfig without a vault key id = %+v, %v; want no vault provider", kc.Providers, err)
	}
}
