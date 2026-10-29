package admin_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/policy"
)

// stager is an Admin that stages configurations: d-1 a digest key id and m-1 a master key id, both in files, this binary's classifier.
func stager(t *testing.T, c *catalog.Catalog, dev bool) *admin.Admin {
	t.Helper()
	dir := t.TempDir()
	declared := map[string]keys.Declaration{}
	for id, kind := range map[string]string{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster} {
		path := filepath.Join(dir, id)
		writeMaterial(t, path, byte(len(id)+len(kind)))
		declared[id] = keys.Declaration{Provider: keys.ProviderFile, Ref: path, Kind: kind}
	}
	k, err := keys.New(keys.Config{Declared: declared}, c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ingest.Builtin(1<<20, true)
	if err != nil {
		t.Fatal(err)
	}

	return &admin.Admin{Catalog: c, Actor: "config-op", Keys: k, Classifier: b.Name, MaxFrameLen: b.MaxFrameLen, Dev: dev}
}

func run(t *testing.T, a *admin.Admin, words ...string) (any, error) {
	t.Helper()

	return admin.Run(t.Context(), a, words)
}

// config stage stores the baseline and stages a masking configuration of this binary, both key ids bound;
// config list, policy list and policy show read them back; each command is audited.
func TestConfigStage(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		a := stager(t, c, false)
		out, err := run(t, a, "config", "stage", "d-1", "m-1")
		staged, ok := out.(admin.Staged)
		if err != nil || !ok || staged.Policy.ID != "baseline" || staged.Policy.Version != 1 || staged.DigestKey.ID != "d-1" ||
			staged.EncryptionKey == nil || staged.EncryptionKey.ID != "m-1" || staged.DigestKey.Fingerprint == "" {
			t.Fatalf("config stage = %+v, %v", out, err)
		}
		for _, id := range []string{"d-1", "m-1"} {
			if b, ok, _ := c.KeyBinding(t.Context(), id); !ok {
				t.Errorf("%s not bound by staging: %+v", id, b)
			}
		}
		if again, err := run(t, a, "config", "stage", "d-1", "m-1"); err != nil || as[admin.Staged](t, again).ConfigID != staged.ConfigID {
			t.Errorf("staging again = %+v, %v; want the same configuration", again, err)
		}
		readBack(t, a, &staged)
		auditActions(t, c, "config stage,config stage,config list,policy list,policy show")
	})
}

// A policy file lacking a baseline rule is refused outside dev and staged under it; another body under a stored id and version is refused;
// a key id undeclared, or declared with another kind, is refused before anything is stored.
func TestConfigStageRefuses(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		dir := t.TempDir()
		partial := filepath.Join(dir, "partial.yaml")
		if err := os.WriteFile(partial, []byte("policy_id: site\nversion: 1\nitem_rules:\n  - {stream: 7, function: 3, path: \"2\", domain: ppbody}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		a := stager(t, c, false)
		if _, err := run(t, a, "config", "stage", "d-1", "--policy", partial); !errors.Is(err, admin.ErrIncompletePolicy) {
			t.Errorf("a partial policy outside dev = %v, want ErrIncompletePolicy", err)
		}
		if _, err := run(t, a, "config", "stage", "d-9"); !errors.Is(err, keys.ErrUnknownKey) {
			t.Errorf("an undeclared key id = %v, want ErrUnknownKey", err)
		}
		if _, err := run(t, a, "config", "stage", "m-1"); !errors.Is(err, keys.ErrUnknownKey) {
			t.Errorf("a master key id as the digest key = %v, want ErrUnknownKey", err)
		}
		if cs, _ := c.Configs(t.Context()); len(cs) != 0 {
			t.Errorf("refused stagings stored %d configurations", len(cs))
		}
		dev := stager(t, c, true)
		if _, err := run(t, dev, "config", "stage", "d-1", "--policy", partial); err != nil {
			t.Errorf("a partial policy under dev = %v", err)
		}
		changed := filepath.Join(dir, "changed.yaml")
		if err := os.WriteFile(changed, []byte("policy_id: site\nversion: 1\nitem_rules:\n  - {stream: 7, function: 6, path: \"2\", domain: ppbody}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := run(t, dev, "config", "stage", "d-1", "--policy", changed); !errors.Is(err, catalog.ErrPolicyConflict) {
			t.Errorf("another body under site version 1 = %v, want ErrPolicyConflict", err)
		}
		for _, words := range [][]string{
			{"config", "stage"}, {"config", "stage", "d-1", "m-1", "x"}, {"config", "stage", "d-1", "--policy"},
			{"config", "stage", "--policy", partial}, {"policy", "show", "site", "x"}, {"config", "activate", "x"}, {"config", "list", "x"},
		} {
			if _, err := run(t, dev, words...); !errors.Is(err, admin.ErrUsage) {
				t.Errorf("%q = %v, want a usage error", words, err)
			}
		}
	})
}

// config activate activates a staged policy-less configuration, leaves an active one as it is, refuses an unknown or retired one,
// and refuses a masking one while ingest does not mask; each answered activation is audited.
func TestConfigActivate(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		a := stager(t, c, false)
		b, err := ingest.Builtin(a.MaxFrameLen, true)
		if err != nil {
			t.Fatal(err)
		}
		own, err := ingest.StageBuiltin(t.Context(), c, &b, false)
		if err != nil {
			t.Fatal(err)
		}
		out, err := run(t, a, "config", "activate", itoa(own))
		if act, ok := out.(admin.Activation); err != nil || !ok || !act.Changed || act.ConfigID != own {
			t.Fatalf("config activate = %+v, %v", out, err)
		}
		if out, err := run(t, a, "config", "activate", itoa(own)); err != nil || as[admin.Activation](t, out).Changed {
			t.Errorf("activating an active one = %+v, %v; want unchanged", out, err)
		}
		if _, err := run(t, a, "config", "activate", "999"); !errors.Is(err, catalog.ErrUnknownConfig) {
			t.Errorf("an unknown configuration = %v", err)
		}
		staged, err := run(t, a, "config", "stage", "d-1", "m-1")
		if err != nil {
			t.Fatal(err)
		}
		masking := as[admin.Staged](t, staged).ConfigID
		if _, err := run(t, a, "config", "activate", itoa(masking)); !errors.Is(err, admin.ErrMaskingNotExecuted) {
			t.Errorf("activating a masking configuration = %v, want ErrMaskingNotExecuted", err)
		}
		if cs, _ := c.Configs(t.Context()); cs[1].State() != catalog.ConfigStaged {
			t.Errorf("the masking configuration is %s, want still staged", cs[1].State())
		}
		if err := c.RetireConfig(t.Context(), own, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := run(t, a, "config", "activate", itoa(own)); !errors.Is(err, catalog.ErrConfigRetired) {
			t.Errorf("a retired configuration = %v", err)
		}
		rows, _ := c.AuditByActor(t.Context(), "config-op")
		n := 0
		for _, r := range rows {
			if r.Action == "config activate" {
				n++
			}
		}
		if n != 2 {
			t.Errorf("%d config activate rows, want the two answered activations", n)
		}
	})
}

// readBack checks what config list, policy list and policy show answer after staged.
func readBack(t *testing.T, a *admin.Admin, staged *admin.Staged) {
	t.Helper()
	list, err := run(t, a, "config", "list")
	cs, _ := list.([]admin.ConfigSummary)
	if err != nil || len(cs) != 1 || cs[0].ID != staged.ConfigID || cs[0].State != catalog.ConfigStaged || cs[0].Policy == nil ||
		cs[0].Policy.SHA256 != staged.Policy.SHA256 || cs[0].MaxFrameLen != 1<<20 || cs[0].EncryptionKey == nil {
		t.Errorf("config list = %+v, %v", list, err)
	}
	pl, err := run(t, a, "policy", "list")
	ps, _ := pl.([]admin.PolicySummary)
	if err != nil || len(ps) != 1 || ps[0].ID != "baseline" || ps[0].ItemRules != 24 || ps[0].AnnotationRules != 3 {
		t.Errorf("policy list = %+v, %v", pl, err)
	}
	shown, err := run(t, a, "policy", "show", "baseline", "1")
	if p, ok := shown.(policy.Policy); err != nil || !ok || len(p.Missing(new(policy.Baseline()))) != 0 {
		t.Errorf("policy show = %+v, %v", shown, err)
	}
}

// as is v as a T, failing the test otherwise.
func as[T any](t *testing.T, v any) T {
	t.Helper()
	x, ok := v.(T)
	if !ok {
		t.Fatalf("%+v is a %T", v, v)
	}

	return x
}

// auditActions checks the actions config-op's audit rows name, in order.
func auditActions(t *testing.T, c *catalog.Catalog, want string) {
	t.Helper()
	rows, err := c.AuditByActor(t.Context(), "config-op")
	actions := make([]string, 0, len(rows))
	for _, r := range rows {
		actions = append(actions, r.Action)
	}
	if err != nil || strings.Join(actions, ",") != want {
		t.Errorf("audit = %q, %v; want %s", actions, err, want)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
