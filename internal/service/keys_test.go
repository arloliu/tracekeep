package service

import (
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/metrics"
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
	kc, err := keysConfig(c, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := kc.Providers[keys.ProviderVault]; !ok || kc.Declared["m-1"].Kind != "master" {
		t.Errorf("keys config = %+v, want the vault provider and m-1 as master", kc)
	}
	c.Vault.Auth.SecretID = ""
	if _, err := keysConfig(c, false, nil); err == nil {
		t.Error("keysConfig with approle and no secret id succeeded")
	}
	c.Declared["m-1"] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: "/k", Kind: "master"}
	if kc, err := keysConfig(c, false, nil); err != nil || kc.Providers != nil {
		t.Errorf("keysConfig without a vault key id = %+v, %v; want no vault provider", kc.Providers, err)
	}
}

// Every role of a process shares one keys module, and its metrics are registered once.
func TestEnvKeysShared(t *testing.T) {
	t.Parallel()
	e := devEnv(t)
	e.Metrics = metrics.NewServer()
	a, err := e.Keys()
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.Keys()
	if err != nil || a != b {
		t.Errorf("a second Keys = %p, %v; want the first, %p", b, err, a)
	}
}

// Outside dev an ingest start stages its own configuration and activates nothing (rollouts.md §6.4); under dev it activates its own.
func TestIngestStartStagesOutsideDev(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		e := env(t, c)
		e.Dev = false
		gs, err := ingesters(t.Context(), e)
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range gs {
			e.leave(g.Replica)
		}
		cs, err := c.Configs(t.Context())
		if err != nil || len(cs) != 1 || cs[0].State() != catalog.ConfigStaged {
			t.Fatalf("after a start outside dev: %+v, %v; want one staged configuration", cs, err)
		}
		e.Dev = true
		if gs, err = ingesters(t.Context(), e); err != nil {
			t.Fatal(err)
		}
		for _, g := range gs {
			e.leave(g.Replica)
		}
		if cs, err = c.Configs(t.Context()); err != nil || len(cs) != 1 || cs[0].State() != catalog.ConfigActive {
			t.Errorf("after a dev start: %+v, %v; want it active", cs, err)
		}
	})
}
