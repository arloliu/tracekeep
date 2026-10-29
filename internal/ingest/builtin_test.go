package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/procconfig"
)

// The built-in configuration is named for its classifier's implementation and version;
// a build that cannot name the version is refused, except where the caller accepts it (dev).
func TestBuiltin(t *testing.T) {
	t.Parallel()
	c, err := namedConfig("go-secs/v2.8.0", 1<<20, false)
	if err != nil || c.Name != "go-secs/v2.8.0" || c.MaxFrameLen != 1<<20 || c.Classifier.Name() != c.Name || c.Classifier.MaxFrameLen() != 1<<20 {
		t.Errorf("builtin = %+v, %v; want the classifier named for its version", c, err)
	}
	if _, err := namedConfig(unversioned, 0, false); !errors.Is(err, ErrUnversionedBuild) {
		t.Errorf("an unversioned build = %v, want ErrUnversionedBuild", err)
	}
	if c, err := namedConfig(unversioned, 0, true); err != nil || c.Name != unversioned {
		t.Errorf("an unversioned build accepted = %+v, %v", c, err)
	}
}

// Activate stores and activates configs, as tracekeep dev does for its own, and returns them by id.
func Activate(ctx context.Context, cat *catalog.Catalog, configs []Config) (map[int64]Config, error) {
	byID := make(map[int64]Config, len(configs))
	for _, c := range configs {
		id, err := cat.ActivateConfig(ctx, c.Content())
		if err != nil {
			return nil, err
		}
		byID[id] = c
	}

	return byID, nil
}

// declared is a Keys declaring key ids by kind.
type declared map[string]string

func (d declared) Declares(keyID, kind string) bool { return d[keyID] == kind }

// start runs what an ingest start does with its configurations under limit: stage its own (activated under dev), rebuild what it executes,
// and register executing them.
func start(t *testing.T, c *catalog.Catalog, limit uint64, dev bool) (map[int64]Config, error) {
	t.Helper()
	b, err := namedConfig("go-secs/v2.8.0", limit, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := StageBuiltin(t.Context(), c, &b, dev); err != nil {
		t.Fatal(err)
	}
	byID, err := Executable(t.Context(), c, &b, declared{}, dev)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	_, err = c.Register(t.Context(), "r", ids, time.Minute)

	return byID, err
}

func states(t *testing.T, c *catalog.Catalog) map[string]int {
	t.Helper()
	cs, err := c.Configs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	n := map[string]int{}
	for i := range cs {
		n[cs[i].State()]++
	}

	return n
}

// A policy-less configuration's stored bytes are the classifier and the frame limit alone, as before masking existed,
// so a deployment's existing row is the one a start finds.
func TestPolicyLessContentUnchanged(t *testing.T) {
	t.Parallel()
	b, err := namedConfig("go-secs/v2.8.0", 4096, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b.Content()); got != `{"classifier":"go-secs/v2.8.0","max_frame_len":4096}` {
		t.Errorf("content = %s", got)
	}
}

// A start stages its configuration and activates nothing; dev activates its own. A changed frame limit is staged beside the active one,
// executed and listed, and bound only once activated; changing it back joins again.
func TestStartStagesWithoutActivating(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		if _, err := start(t, c, 1, false); err != nil {
			t.Fatal(err)
		}
		if n := states(t, c); n[catalog.ConfigStaged] != 1 || n[catalog.ConfigActive] != 0 {
			t.Fatalf("after a start: %v, want one staged and none active", n)
		}
		_, err := c.Admit(ctx, &catalog.Identity{Version: catalog.VersionKey{Capture: tracepack.UUID{1}}, Incarnation: 1, StreamSeq: 1},
			&catalog.Descriptor{Bytes: []byte("d"), ToolID: "T1", RecorderInstanceID: tracepack.UUID{8}, Device: "eqp", OriginNS: 1})
		if !errors.Is(err, catalog.ErrNoBindableConfig) {
			t.Errorf("an admission with only a staged configuration = %v, want ErrNoBindableConfig", err)
		}
		if _, err := start(t, c, 1, true); err != nil {
			t.Fatal(err)
		}
		if n := states(t, c); n[catalog.ConfigActive] != 1 || n[catalog.ConfigStaged] != 0 {
			t.Errorf("after a dev start: %v, want its own activated", n)
		}
		byID, err := start(t, c, 2, false)
		if err != nil {
			t.Fatalf("a start under another frame limit = %v", err)
		}
		if len(byID) != 2 {
			t.Errorf("executed %d configurations, want the active one and the staged one", len(byID))
		}
		if n := states(t, c); n[catalog.ConfigActive] != 1 || n[catalog.ConfigStaged] != 1 {
			t.Errorf("after a changed frame limit: %v, want one active and one staged", n)
		}
		if _, err := start(t, c, 1, false); err != nil {
			t.Errorf("changing the limit back = %v", err)
		}
	})
}

// A required configuration of another classifier refuses the start; a staged one is skipped, so an older replica keeps running.
func TestForeignConfigurations(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		foreign := []byte(`{"classifier":"other","max_frame_len":0}`)
		if _, err := c.StageConfig(ctx, catalog.Staging{Content: foreign}); err != nil {
			t.Fatal(err)
		}
		if _, err := start(t, c, 1, true); err != nil {
			t.Errorf("a start beside another classifier's staged configuration = %v", err)
		}
		if _, err := c.ActivateConfig(ctx, foreign); err != nil {
			t.Fatal(err)
		}
		if _, err := start(t, c, 1, true); !errors.Is(err, ErrForeignConfiguration) {
			t.Errorf("a start beside another classifier's active configuration = %v, want ErrForeignConfiguration", err)
		}
	})
}

// Retiring a configuration never removes the ability to execute it (rollouts.md §6.4):
// a capture bound to a retired configuration still has it executed after a restart under another frame limit.
func TestExecutableKeepsARetiredBoundConfiguration(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		first, err := start(t, c, 1, true)
		if err != nil {
			t.Fatal(err)
		}
		capture := tracepack.UUID{9}
		res, err := c.Admit(ctx, &catalog.Identity{Version: catalog.VersionKey{Capture: capture}, Incarnation: 1, StreamSeq: 1},
			&catalog.Descriptor{Bytes: []byte("d"), ToolID: "T1", RecorderInstanceID: tracepack.UUID{8}, Device: "eqp", OriginNS: 1})
		if err != nil || first[res.ConfigID].MaxFrameLen != 1 {
			t.Fatalf("Admit = %+v, %v; want the capture bound to the first limit", res, err)
		}
		if _, err := start(t, c, 2, true); err != nil {
			t.Fatal(err)
		}
		if err := c.RetireConfig(ctx, res.ConfigID, []tracepack.UUID{capture}); err != nil {
			t.Fatal(err)
		}
		byID, err := start(t, c, 2, true)
		if got, ok := byID[res.ConfigID]; err != nil || !ok || got.MaxFrameLen != 1 {
			t.Errorf("after the retirement, configuration %d = %+v, %v (%v); want the bound capture's still executed", res.ConfigID, got, ok, err)
		}
	})
}

// masking is a masking configuration of the go-secs/v2.8.0 classifier under p, with d-1 and m-1 bound in c.
func masking(t *testing.T, c *catalog.Catalog, p *policy.Policy) Config {
	t.Helper()
	for id, kind := range map[string]string{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster} {
		if _, err := c.BindKey(t.Context(), catalog.KeyBinding{KeyID: id, Kind: kind, ProviderRef: "file:/k/" + id, Fingerprint: "fp-" + id}); err != nil {
			t.Fatal(err)
		}
	}

	return Config{Name: "go-secs/v2.8.0", MaxFrameLen: 1, Policy: p, DigestKey: KeyRef{ID: "d-1", Fingerprint: "fp-d-1"}, EncryptionKey: KeyRef{ID: "m-1", Fingerprint: "fp-m-1"}}
}

func stageMasking(t *testing.T, c *catalog.Catalog, m *Config) int64 {
	t.Helper()
	body, _ := m.Policy.Canonical()
	id, err := c.StageConfig(t.Context(), catalog.Staging{
		Content: m.Content(), Policy: &catalog.StoredPolicy{ID: m.Policy.ID, Version: int64(m.Policy.Version), Body: body}, //nolint:gosec // a parsed version fits
	})
	if err != nil {
		t.Fatal(err)
	}

	return id
}

// A masking configuration is rebuilt from its stored bytes, its policy and its keys' bindings alone, and compiled.
func TestRebuildMasking(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := masking(t, c, new(policy.Baseline()))
		id := stageMasking(t, c, &m)
		if _, err := c.ActivateStaged(t.Context(), id, nil); err != nil {
			t.Fatal(err)
		}
		b, err := namedConfig("go-secs/v2.8.0", 1, false)
		if err != nil {
			t.Fatal(err)
		}
		byID, err := Executable(t.Context(), c, &b, declared{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster}, false)
		if err != nil {
			t.Fatal(err)
		}
		got := byID[id]
		if got.Policy == nil || got.Masking == nil || got.DigestKey.ID != "d-1" || got.EncryptionKey.ID != "m-1" || got.Policy.ID != "baseline" {
			t.Errorf("rebuilt = %+v", got)
		}
		if string(got.Content()) != string(m.Content()) {
			t.Errorf("the rebuilt configuration's content differs:\n%s\n%s", got.Content(), m.Content())
		}
		cc, err := procconfig.Decode(m.Content())
		if err != nil || cc.FormatMinor != 1 || cc.Policy.Version != 1 {
			t.Errorf("content = %s (%v); want format_minor 1 and the policy's version", m.Content(), err)
		}
	})
}

// Whatever does not match refuses an active masking configuration's start, and skips a staged one:
// an undeclared or wrong-kind key id, a fingerprint not its binding's, an unbound key id, a changed policy hash,
// and, outside dev, a policy lacking a baseline rule.
func TestRebuildRefuses(t *testing.T) {
	t.Parallel()
	ok := declared{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster}
	partial := policy.Baseline()
	partial.ID, partial.ItemRules = "partial", partial.ItemRules[1:]
	for name, tc := range map[string]struct {
		keys   declared
		change func(m *Config)
		dev    bool
	}{
		"an undeclared digest key":        {keys: declared{"m-1": catalog.KindMaster}},
		"a master key declared as digest": {keys: declared{"d-1": catalog.KindDigest, "m-1": catalog.KindDigest}},
		"another fingerprint":             {keys: ok, change: func(m *Config) { m.DigestKey.Fingerprint = "other" }},
		"an unbound key":                  {keys: declared{"d-1": catalog.KindDigest, "m-9": catalog.KindMaster}, change: func(m *Config) { m.EncryptionKey.ID = "m-9" }},
		"a policy lacking the baseline":   {keys: ok, change: func(m *Config) { m.Policy = &partial }},
	} {
		catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
			m := masking(t, c, new(policy.Baseline()))
			if tc.change != nil {
				tc.change(&m)
			}
			id := stageMasking(t, c, &m)
			b, err := namedConfig("go-secs/v2.8.0", 1, false)
			if err != nil {
				t.Fatal(err)
			}
			if byID, err := Executable(t.Context(), c, &b, tc.keys, tc.dev); err != nil || len(byID) != 0 {
				t.Errorf("%s, staged: %d executed, %v; want it skipped", name, len(byID), err)
			}
			if _, err := c.ActivateStaged(t.Context(), id, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := Executable(t.Context(), c, &b, tc.keys, tc.dev); !errors.Is(err, ErrCannotExecute) {
				t.Errorf("%s, active: Executable = %v, want ErrCannotExecute", name, err)
			}
		})
	}
	// Under dev, a policy lacking a baseline rule is executed.
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := masking(t, c, &partial)
		id := stageMasking(t, c, &m)
		if _, err := c.ActivateStaged(t.Context(), id, nil); err != nil {
			t.Fatal(err)
		}
		b, _ := namedConfig("go-secs/v2.8.0", 1, false)
		if byID, err := Executable(t.Context(), c, &b, ok, true); err != nil || byID[id].Masking == nil {
			t.Errorf("under dev, a partial policy = %v, %v", byID, err)
		}
	})
	// A policy changed under its id and version cannot be stored; one whose stored hash differs from the configuration's is refused.
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := masking(t, c, new(policy.Baseline()))
		content := strings.Replace(string(m.Content()), `"sha256":"`, `"sha256":"00`, 1)
		body, _ := m.Policy.Canonical()
		if _, err := c.ActivateConfig(t.Context(), []byte(content)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.StageConfig(t.Context(), catalog.Staging{Content: []byte("x"), Policy: &catalog.StoredPolicy{ID: "baseline", Version: 1, Body: body}}); err != nil {
			t.Fatal(err)
		}
		b, _ := namedConfig("go-secs/v2.8.0", 1, false)
		if _, err := Executable(t.Context(), c, &b, ok, false); !errors.Is(err, ErrCannotExecute) {
			t.Errorf("a configuration naming another hash = %v, want ErrCannotExecute", err)
		}
	})
}
