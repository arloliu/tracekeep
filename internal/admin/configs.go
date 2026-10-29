package admin

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/procconfig"
)

var (
	// ErrIncompletePolicy refuses to stage, outside tracekeep dev, a policy lacking a baseline rule (masking-keys.md §9).
	ErrIncompletePolicy = errors.New("admin: the policy lacks baseline rules")
	// ErrMaskingNotExecuted refuses to activate a masking configuration in a release whose ingest does not mask (rollouts.md §6.4).
	ErrMaskingNotExecuted = errors.New("admin: this release's ingest does not mask, so a configuration naming a policy cannot be activated")
)

// PolicySummary is one stored policy as policy list prints it.
type PolicySummary struct {
	ID              string
	Version         int64
	SHA256          string
	ItemRules       int
	AnnotationRules int
	StoredAt        time.Time
}

// PolicyList is policy list: every stored policy by id and version.
func (a *Admin) PolicyList(ctx context.Context) ([]PolicySummary, error) {
	stored, err := a.Catalog.Policies(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]PolicySummary, 0, len(stored))
	for i := range stored {
		p, err := policy.FromCanonical(stored[i].Body)
		if err != nil {
			return nil, fmt.Errorf("admin: policy %s version %d: %w", stored[i].ID, stored[i].Version, err)
		}
		out = append(out, PolicySummary{
			ID: stored[i].ID, Version: stored[i].Version, SHA256: hex.EncodeToString(stored[i].Hash),
			ItemRules: len(p.ItemRules), AnnotationRules: len(p.AnnotationRules), StoredAt: time.Unix(0, stored[i].CreatedAt).UTC(),
		})
	}

	return out, nil
}

// PolicyShow is policy show: a stored policy's canonical form.
func (a *Admin) PolicyShow(ctx context.Context, id string, version int64) (policy.Policy, error) {
	stored, ok, err := a.Catalog.Policy(ctx, id, version)
	switch {
	case err != nil:
		return policy.Policy{}, err
	case !ok:
		return policy.Policy{}, fmt.Errorf("admin: no policy %s version %d", id, version)
	}

	return policy.FromCanonical(stored.Body)
}

// Staged is what config stage did: its action id, the configuration staged and what it names.
type Staged struct {
	ActionID      string
	ConfigID      int64
	Policy        procconfig.PolicyRef
	DigestKey     procconfig.KeyRef
	EncryptionKey procconfig.KeyRef
}

// ConfigStage is config stage: the policy at file, or the embedded baseline when file is empty, parsed and, outside dev, checked for the baseline;
// both key ids resolved under their kinds, which binds them, the encryption key being required since a pack masked at write encrypts; and the configuration of this binary's classifier and frame limit
// with the policy and the key ids' fingerprints stored as staged, with the policy and the audit row, in one transaction (rollouts.md §6.4).
// The file is read by the process that runs the command.
func (a *Admin) ConfigStage(ctx context.Context, digestKeyID, encryptionKeyID, file string) (Staged, error) {
	if a.Keys == nil || a.Classifier == "" {
		return Staged{}, errors.New("admin: this process stages no configuration")
	}
	p := policy.Baseline()
	if file != "" {
		doc, err := os.ReadFile(file)
		if err != nil {
			return Staged{}, fmt.Errorf("admin: policy file: %w", err)
		}
		if p, err = policy.Parse(doc); err != nil {
			return Staged{}, err
		}
	}
	if missing := p.Missing(new(policy.Baseline())); len(missing) > 0 && !a.Dev {
		return Staged{}, fmt.Errorf("%w: %s version %d lacks %q", ErrIncompletePolicy, p.ID, p.Version, missing)
	}
	// The policy compiles under the key ids it will mask with: with an encryption key no two rules may overlap.
	if _, err := p.Compile(digestKeyID, encryptionKeyID); err != nil {
		return Staged{}, err
	}
	body, sum := p.Canonical()
	c := procconfig.Content{
		Classifier: a.Classifier, MaxFrameLen: a.MaxFrameLen, FormatMinor: procconfig.MaskingFormatMinor, Masker: procconfig.Masker,
		Policy: &procconfig.PolicyRef{ID: p.ID, Version: int64(p.Version), SHA256: hex.EncodeToString(sum[:])}, //nolint:gosec // policy.Parse bounds a version to int64
	}
	digest, err := a.keyRef(ctx, digestKeyID, catalog.KindDigest)
	if err != nil {
		return Staged{}, err
	}
	c.DigestKey = &digest
	enc, err := a.keyRef(ctx, encryptionKeyID, catalog.KindMaster)
	if err != nil {
		return Staged{}, err
	}
	c.EncryptionKey = &enc
	action := actionID()
	id, err := a.Catalog.StageConfig(ctx, catalog.Staging{
		Content: c.Encode(),
		Policy:  &catalog.StoredPolicy{ID: p.ID, Version: int64(p.Version), Body: body}, //nolint:gosec // policy.Parse bounds a version to int64
		Audit: &catalog.AuditEntry{Actor: a.Actor, Action: "config stage", ActionID: action,
			Detail: fmt.Sprintf("policy %s version %d, digest key %s, encryption key %s", p.ID, p.Version, digestKeyID, encryptionKeyID)},
	})
	if err != nil {
		return Staged{}, err
	}

	return Staged{ActionID: action, ConfigID: id, Policy: *c.Policy, DigestKey: *c.DigestKey, EncryptionKey: *c.EncryptionKey}, nil
}

// keyRef resolves keyID under kind, binding it at its first use, and names it with its fingerprint.
func (a *Admin) keyRef(ctx context.Context, keyID, kind string) (procconfig.KeyRef, error) {
	fp, err := a.Keys.Fingerprint(ctx, keyID, kind)
	if err != nil {
		return procconfig.KeyRef{}, err
	}

	return procconfig.KeyRef{ID: keyID, Fingerprint: fp}, nil
}

// ConfigSummary is one processing configuration as config list prints it.
type ConfigSummary struct {
	ID            int64
	State         string
	Bound         bool
	Classifier    string
	MaxFrameLen   uint64
	Policy        *procconfig.PolicyRef `json:",omitempty"`
	DigestKey     *procconfig.KeyRef    `json:",omitempty"`
	EncryptionKey *procconfig.KeyRef    `json:",omitempty"`
}

// ConfigList is config list: every processing configuration with its state and what it names.
func (a *Admin) ConfigList(ctx context.Context) ([]ConfigSummary, error) {
	infos, err := a.Catalog.Configs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ConfigSummary, 0, len(infos))
	for i := range infos {
		cc, err := content(infos[i].Content)
		if err != nil {
			return nil, fmt.Errorf("admin: configuration %d: %w", infos[i].ID, err)
		}
		out = append(out, ConfigSummary{
			ID: infos[i].ID, State: infos[i].State(), Bound: infos[i].Bound, Classifier: cc.Classifier, MaxFrameLen: cc.MaxFrameLen,
			Policy: cc.Policy, DigestKey: cc.DigestKey, EncryptionKey: cc.EncryptionKey,
		})
	}

	return out, nil
}

// Activation is what config activate did: its action id, the configuration, and whether it was staged until now.
type Activation struct {
	ActionID string
	ConfigID int64
	Changed  bool
}

// ConfigActivate is config activate: a staged configuration activated under the membership serialization, an active one left as it is,
// an unknown or retired one refused, and, while ingest does not mask, a masking one refused (rollouts.md §6.4).
func (a *Admin) ConfigActivate(ctx context.Context, id int64) (Activation, error) {
	// Judged inside the activation's transaction, so no configuration staged meanwhile escapes it.
	check := func(b []byte) error {
		cc, err := content(b)
		if err != nil {
			return fmt.Errorf("admin: configuration %d: %w", id, err)
		}
		if cc.Policy != nil && !procconfig.Masks {
			return fmt.Errorf("%w: configuration %d names policy %s version %d", ErrMaskingNotExecuted, id, cc.Policy.ID, cc.Policy.Version)
		}

		return nil
	}
	action := actionID()
	changed, err := a.Catalog.ActivateStaged(ctx, id, check, &catalog.AuditEntry{
		Actor: a.Actor, Action: "config activate", ActionID: action, Detail: "config activate " + strconv.FormatInt(id, 10),
	})

	return Activation{ActionID: action, ConfigID: id, Changed: changed}, err
}

func content(b []byte) (procconfig.Content, error) { return procconfig.Decode(b) }
