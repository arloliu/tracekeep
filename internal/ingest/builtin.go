package ingest

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/arloliu/go-secs/tracepack/classify"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/procconfig"
)

// unversioned is what classify.Name reports when the build names no go-secs version, as a workspace build does.
const unversioned = "go-secs/unknown"

var (
	// ErrUnversionedBuild refuses a build whose classifier cannot name its version:
	// two such builds would share one processing configuration whatever their classifiers (rollouts.md §6.4).
	ErrUnversionedBuild = errors.New("ingest: the build names no classifier version")
	// ErrForeignConfiguration refuses a replica that cannot execute a configuration it must, one of another classifier:
	// active, which the activation gate requires, or bound by a capture the catalog holds (rollouts.md §6.4).
	ErrForeignConfiguration = errors.New("ingest: a required configuration of another classifier")
	// ErrCannotExecute refuses a configuration this replica cannot rebuild from its stored bytes:
	// its policy missing or changed, a key id undeclared or of another kind, a fingerprint not its binding's,
	// or, outside tracekeep dev, a policy lacking a baseline rule.
	ErrCannotExecute = errors.New("ingest: the replica cannot execute the configuration")
)

// KeyRef and PolicyRef are what a configuration records of its keys and policy.
type (
	KeyRef    = procconfig.KeyRef
	PolicyRef = procconfig.PolicyRef
)

// Content returns c's stored bytes (procconfig.Content).
func (c *Config) Content() []byte {
	cc := procconfig.Content{Classifier: c.Name, MaxFrameLen: c.MaxFrameLen, FormatMinor: c.FormatMinor}
	if c.Policy != nil {
		_, sum := c.Policy.Canonical()
		cc.Policy = &PolicyRef{ID: c.Policy.ID, Version: int64(c.Policy.Version), SHA256: hex.EncodeToString(sum[:])} //nolint:gosec // policy.Parse bounds a version to int64
		cc.DigestKey = &c.DigestKey
		if c.EncryptionKey.ID != "" {
			cc.EncryptionKey = &c.EncryptionKey
		}
		cc.FormatMinor = max(cc.FormatMinor, procconfig.MaskingFormatMinor)
		cc.Masker = cmp.Or(c.Masker, procconfig.Masker)
	}

	return cc.Encode()
}

// Builtin is the policy-less processing configuration this binary executes: the go-secs classifier, named for its implementation and version
// (classify.Name), under maxFrameLen.
// An unversioned build is refused unless allowUnversioned, which only dev passes.
func Builtin(maxFrameLen uint64, allowUnversioned bool) (Config, error) {
	return namedConfig(classify.Name(), maxFrameLen, allowUnversioned)
}

func namedConfig(name string, maxFrameLen uint64, allowUnversioned bool) (Config, error) {
	if name == unversioned && !allowUnversioned {
		return Config{}, fmt.Errorf("%w: %s", ErrUnversionedBuild, name)
	}

	return Config{Classifier: classify.NewNamed(name, maxFrameLen), Name: name, MaxFrameLen: maxFrameLen}, nil
}

// StageBuiltin stores builtin as a staged configuration, as every start does, and returns its id;
// with activate, which only tracekeep dev passes, it is activated too. A start activates nothing else (rollouts.md §6.4).
func StageBuiltin(ctx context.Context, cat *catalog.Catalog, builtin *Config, activate bool) (int64, error) {
	if activate {
		return cat.ActivateConfig(ctx, builtin.Content())
	}

	return cat.StageConfig(ctx, catalog.Staging{Content: builtin.Content()})
}

// Keys is what rebuilding a masking configuration checks of its key ids, with no provider call:
// whether one is declared with a kind; *keys.Keys is one.
type Keys interface {
	Declares(keyID, kind string) bool
}

// Executable rebuilds, by id, every configuration this replica executes (rollouts.md §6.4):
// each active one and each one a capture is bound to, a start refused when it cannot execute one,
// and each staged one it can execute, the others skipped, so an older replica never stops on a newer one's staged configuration.
// A required configuration of another classifier is refused with ErrForeignConfiguration; any of builtin's classifier runs under its own frame limit.
func Executable(ctx context.Context, cat *catalog.Catalog, builtin *Config, keys Keys, dev bool) (map[int64]Config, error) {
	infos, err := cat.Configs(ctx)
	if err != nil {
		return nil, err
	}
	byID := map[int64]Config{}
	for i := range infos {
		info := &infos[i]
		required := info.State() == catalog.ConfigActive || info.Bound
		if !required && info.State() != catalog.ConfigStaged {
			continue // retired and bound by no capture
		}
		c, err := rebuild(ctx, cat, builtin, keys, info.Content, dev)
		switch {
		case err == nil:
			byID[info.ID] = c
		case required:
			return nil, fmt.Errorf("configuration %d: %w", info.ID, err)
		default: // a staged one this replica cannot execute
		}
	}

	return byID, nil
}

// rebuild is a configuration from its stored bytes, with no provider call:
// its classifier this replica's under the stored frame limit, and, for a masking one, its policy read back and its hash compared,
// both key ids declared with their kinds and their fingerprints their bindings', and the policy compiled under them.
func rebuild(ctx context.Context, cat *catalog.Catalog, builtin *Config, keys Keys, content []byte, dev bool) (Config, error) {
	cc, err := procconfig.Decode(content)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrCannotExecute, err)
	}
	if cc.Classifier != builtin.Name {
		return Config{}, fmt.Errorf("%w: %s, this replica's classifier being %s", ErrForeignConfiguration, cc.Classifier, builtin.Name)
	}
	if cc.FormatMinor < 0 || cc.FormatMinor > procconfig.NewestFormatMinor {
		return Config{}, fmt.Errorf("%w: format_minor %d, this release writing at most %d", ErrCannotExecute, cc.FormatMinor, procconfig.NewestFormatMinor)
	}
	c, err := namedConfig(builtin.Name, cc.MaxFrameLen, true)
	if err != nil {
		return Config{}, err
	}
	c.FormatMinor, c.Masker = cc.FormatMinor, cc.Masker
	if cc.Policy == nil {
		if cc.Masker != "" || cc.DigestKey != nil || cc.EncryptionKey != nil {
			return Config{}, fmt.Errorf("%w: a masker or key without a policy", ErrCannotExecute)
		}

		return c, nil
	}
	if cc.FormatMinor < procconfig.MaskingFormatMinor {
		return Config{}, fmt.Errorf("%w: a policy under format_minor %d", ErrCannotExecute, cc.FormatMinor)
	}
	stored, ok, err := cat.Policy(ctx, cc.Policy.ID, cc.Policy.Version)
	switch {
	case err != nil:
		return Config{}, err
	case !ok || hex.EncodeToString(stored.Hash) != cc.Policy.SHA256:
		return Config{}, fmt.Errorf("%w: policy %s version %d missing or changed", ErrCannotExecute, cc.Policy.ID, cc.Policy.Version)
	}
	p, err := policy.FromCanonical(stored.Body)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrCannotExecute, err)
	}
	if missing := p.Missing(new(policy.Baseline())); len(missing) > 0 && !dev {
		return Config{}, fmt.Errorf("%w: policy %s version %d lacks baseline rules %q", ErrCannotExecute, p.ID, p.Version, missing)
	}
	if cc.DigestKey == nil {
		return Config{}, fmt.Errorf("%w: a policy and no digest key", ErrCannotExecute)
	}
	if err := checkKey(ctx, cat, keys, cc.DigestKey, catalog.KindDigest); err != nil {
		return Config{}, err
	}
	// A pack masked at write encrypts, so a masking configuration names its encryption key (the tracepack format specification §5).
	if cc.EncryptionKey == nil {
		return Config{}, fmt.Errorf("%w: a policy and no encryption key", ErrCannotExecute)
	}
	if err := checkKey(ctx, cat, keys, cc.EncryptionKey, catalog.KindMaster); err != nil {
		return Config{}, err
	}
	c.EncryptionKey = *cc.EncryptionKey
	if c.Masking, err = p.Compile(cc.DigestKey.ID, cc.EncryptionKey.ID); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrCannotExecute, err)
	}
	c.Policy, c.DigestKey = &p, *cc.DigestKey

	return c, nil
}

// checkKey checks a configuration's key id: declared with kind, and bound under kind with the fingerprint the configuration records.
func checkKey(ctx context.Context, cat *catalog.Catalog, keys Keys, k *KeyRef, kind string) error {
	if !keys.Declares(k.ID, kind) {
		return fmt.Errorf("%w: key id %s is not declared as %s", ErrCannotExecute, k.ID, kind)
	}
	b, bound, err := cat.KeyBinding(ctx, k.ID)
	switch {
	case err != nil:
		return err
	case !bound || b.Kind != kind || b.Fingerprint != k.Fingerprint:
		return fmt.Errorf("%w: key id %s is not bound as %s with the configuration's fingerprint", ErrCannotExecute, k.ID, kind)
	}

	return nil
}
