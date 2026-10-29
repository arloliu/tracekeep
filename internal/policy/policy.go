// Package policy is the redaction policy of masking-keys.md §9: its YAML form, its canonical bytes and their hash,
// the embedded baseline, and the redact.Policy a configuration compiles from it with its key ids.
package policy

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/redact"
	"go.yaml.in/yaml/v3"
)

// maxID bounds a policy id, the length of its catalog column.
const maxID = 128

//go:embed baseline.yaml
var baselineYAML []byte

// ErrInvalid reports a policy that is not one: a malformed document, an unknown field, an empty id, a zero version or a rule redact refuses.
var ErrInvalid = errors.New("policy: invalid")

// Policy is a redaction policy's rules under its id and version; its key ids are not part of it (masking-keys.md §9).
type Policy struct {
	ID              string           `json:"policy_id" yaml:"policy_id"`
	Version         uint64           `json:"version" yaml:"version"`
	ItemRules       []ItemRule       `json:"item_rules" yaml:"item_rules"`
	AnnotationRules []AnnotationRule `json:"annotation_rules" yaml:"annotation_rules"`
}

// ItemRule masks the items Path selects in the data records of Stream and Function, digesting them in Domain.
type ItemRule struct {
	Stream   uint8  `json:"stream" yaml:"stream"`
	Function uint8  `json:"function" yaml:"function"`
	Path     string `json:"path" yaml:"path"`
	Domain   string `json:"domain" yaml:"domain"`
}

// AnnotationRule masks the annotation records of Kind, by its name in the tracepack format specification, digesting them in Domain.
type AnnotationRule struct {
	Kind   string `json:"kind" yaml:"kind"`
	Domain string `json:"domain" yaml:"domain"`
}

// Parse reads a policy from its YAML form, refusing any field the form does not name, and checks it as redact would compile it.
func Parse(doc []byte) (Policy, error) {
	dec := yaml.NewDecoder(bytes.NewReader(doc))
	dec.KnownFields(true)
	var p Policy
	if err := dec.Decode(&p); err != nil {
		return Policy{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return Policy{}, fmt.Errorf("%w: more than one document", ErrInvalid)
	}
	if err := p.check(); err != nil {
		return Policy{}, err
	}

	return p, nil
}

// FromCanonical reads a policy back from its canonical bytes, as the catalog stores them.
func FromCanonical(b []byte) (Policy, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var p Policy
	if err := dec.Decode(&p); err != nil {
		return Policy{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if err := p.check(); err != nil {
		return Policy{}, err
	}

	return p, nil
}

// Baseline is the embedded default: the baseline of the tracepack semantics specification §8.
func Baseline() Policy {
	p, err := Parse(baselineYAML)
	if err != nil {
		panic(fmt.Sprintf("policy: the embedded baseline: %v", err))
	}

	return p
}

// Canonical returns p's canonical bytes, JSON with its rules in their written order, and their SHA-256.
func (p *Policy) Canonical() ([]byte, [32]byte) {
	b, err := json.Marshal(p)
	if err != nil {
		panic(fmt.Sprintf("policy: canonical form: %v", err)) // a Policy holds only strings and integers
	}

	return b, sha256.Sum256(b)
}

// Redact returns p's rules with the key ids a configuration gives, as redact compiles them;
// an empty encryptionKeyID makes the masked bytes zeros.
func (p *Policy) Redact(digestKeyID, encryptionKeyID string) redact.Policy {
	r := redact.Policy{ID: p.ID, Version: p.Version, DigestKeyID: digestKeyID}
	if encryptionKeyID != "" {
		r.Encryption = &redact.Encryption{KeyID: encryptionKeyID}
	}
	for _, it := range p.ItemRules {
		r.ItemRules = append(r.ItemRules, redact.ItemRule{Stream: it.Stream, Function: it.Function, Path: it.Path, Domain: it.Domain})
	}
	for _, a := range p.AnnotationRules {
		k, _ := annotationKind(a.Kind) // check refused any other name
		r.AnnotationRules = append(r.AnnotationRules, redact.AnnotationRule{Kind: k, Domain: a.Domain})
	}

	return r
}

// Compile compiles p under the key ids a configuration gives.
func (p *Policy) Compile(digestKeyID, encryptionKeyID string) (*redact.Compiled, error) {
	c, err := redact.Compile(p.Redact(digestKeyID, encryptionKeyID))
	if err != nil {
		return nil, fmt.Errorf("%w: %s version %d: %w", ErrInvalid, p.ID, p.Version, err)
	}

	return c, nil
}

// Missing returns the rules of base that p lacks, by stream, function, path and domain, or by kind and domain;
// none means p holds every rule of base and possibly more.
func (p *Policy) Missing(base *Policy) []string {
	var missing []string
	for _, r := range base.ItemRules {
		if !slices.Contains(p.ItemRules, r) {
			missing = append(missing, fmt.Sprintf("S%dF%d %q in %s", r.Stream, r.Function, r.Path, r.Domain))
		}
	}
	for _, r := range base.AnnotationRules {
		if !slices.Contains(p.AnnotationRules, r) {
			missing = append(missing, fmt.Sprintf("%s in %s", r.Kind, r.Domain))
		}
	}

	return missing
}

// check refuses an empty or overlong id, a zero version, an unknown annotation kind, and any rule redact refuses without encryption;
// overlapping rules, which only encryption forbids, are refused when a configuration with an encryption key compiles p.
func (p *Policy) check() error {
	switch {
	case p.ID == "" || utf8.RuneCountInString(p.ID) > maxID || !utf8.ValidString(p.ID) || strings.TrimSpace(p.ID) != p.ID:
		return fmt.Errorf("%w: policy_id %q: 1 to %d characters, no surrounding space", ErrInvalid, p.ID, maxID)
	case p.Version == 0 || p.Version > math.MaxInt64:
		return fmt.Errorf("%w: %s: version must be positive and fit the catalog's column", ErrInvalid, p.ID)
	}
	for i, a := range p.AnnotationRules {
		if _, ok := annotationKind(a.Kind); !ok {
			return fmt.Errorf("%w: %s: annotation rule %d: unknown kind %q", ErrInvalid, p.ID, i, a.Kind)
		}
	}
	if _, err := redact.Compile(p.Redact("check", "")); err != nil {
		return fmt.Errorf("%w: %s version %d: %w", ErrInvalid, p.ID, p.Version, err)
	}

	return nil
}

// annotationKind is the annotation kind named name in the tracepack format specification's registry, unknown excepted.
func annotationKind(name string) (tracepack.AnnotationKind, bool) {
	for k := tracepack.AnnotationKindNote; k <= tracepack.AnnotationKindUnrecognisedLine; k++ {
		if k.String() == name {
			return k, true
		}
	}

	return tracepack.AnnotationKindUnknown, false
}
