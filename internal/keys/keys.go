// Package keys is the keys module of masking-keys.md §9: it resolves a key id's material from its provider,
// checks it against the key id's binding by insert-or-compare, and only then caches it and hands it out.
// A provider only returns material at a reference and version; it derives, compares and caches nothing.
// It serves the download purpose (tracekeep-decisions.md T-41) and masking's digest and master purposes;
// the session purpose joins with people's sign-in.
package keys

import (
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// Providers.
const (
	ProviderFile = "file"
	ProviderEnv  = "env"
)

// minMaterial is the least material a digest, master or download key holds (masking-keys.md §9).
const minMaterial = 32

// captureKeySalt and captureKeyLen are EncryptionKey's HKDF salt and output length (masking-keys.md §9):
// the aes-siv-cmac-512 key redact expects.
const (
	captureKeySalt = "tracekeep/capture-key/v1"
	captureKeyLen  = 64
)

// fingerprintMessage is the message the fingerprint's HMAC is keyed over (masking-keys.md §9).
const fingerprintMessage = "tracekeep/key-fingerprint/v1"

// ErrUnknownKey marks a key id the purpose does not accept: an unknown or a dropped one, which fails closed.
var ErrUnknownKey = errors.New("keys: key id not accepted")

// Provider returns a key's material at a reference (masking-keys.md §9).
type Provider interface {
	// Material returns the material at ref and at version, or at the current version when version is empty,
	// and the version it returned, empty for a provider without versions.
	Material(ctx context.Context, ref, version string) ([]byte, string, error)
}

// Declaration is a key id's declaration in configuration: its provider, reference and, optionally, version,
// and its kind, which a digest or master key id must name; a download key id may leave it empty.
type Declaration struct {
	Provider, Ref, Version, Kind string
}

// Purpose is the key ids of one purpose: the one credentials are issued under and those they are accepted under.
type Purpose struct {
	Issuing  string
	Accepted []string
}

// Config is the keys the service declares.
type Config struct {
	// Dev admits the env provider, which every command but tracekeep dev refuses.
	Dev      bool
	Declared map[string]Declaration
	Download Purpose
	// Providers are the providers beside file and env, by the name declarations give, such as vault.
	Providers map[string]Provider
}

// Binder is the catalog's key bindings; a *catalog.Catalog is one.
type Binder interface {
	KeyBinding(ctx context.Context, keyID string) (catalog.KeyBinding, bool, error)
	BindKey(ctx context.Context, b catalog.KeyBinding) (catalog.KeyBinding, error)
}

// Keys resolves the declared keys of the purposes it serves.
type Keys struct {
	binder    Binder
	declared  map[string]Declaration
	purposes  map[string]Purpose
	providers map[string]Provider

	mu sync.Mutex
	// cache holds the material of every key id whose check passed, by kind and key id.
	cache map[[2]string][]byte
}

// New checks cfg and returns its keys over binder; it resolves nothing.
func New(cfg Config, binder Binder) (*Keys, error) {
	k := &Keys{
		binder: binder, declared: cfg.Declared, purposes: map[string]Purpose{},
		providers: map[string]Provider{ProviderFile: File{}}, cache: map[[2]string][]byte{},
	}
	// A role that issues no download URL declares no download purpose.
	if cfg.Download.Issuing != "" || len(cfg.Download.Accepted) > 0 {
		k.purposes[catalog.KindDownload] = cfg.Download
	}
	// Outside development env is no provider at all, so neither a declaration nor a binding made in development reaches it.
	if cfg.Dev {
		k.providers[ProviderEnv] = Env{}
	}
	var errs []error
	for name, p := range cfg.Providers {
		if name == ProviderFile || name == ProviderEnv {
			errs = append(errs, fmt.Errorf("keys: provider %q is built in", name))
			continue
		}
		k.providers[name] = p
	}
	for id, d := range cfg.Declared {
		if _, ok := k.providers[d.Provider]; d.Provider == ProviderEnv && !cfg.Dev {
			errs = append(errs, fmt.Errorf("keys: key id %s: the env provider is for development only", id))
		} else if !ok {
			errs = append(errs, fmt.Errorf("keys: key id %s: unknown provider %q", id, d.Provider))
		}
		switch d.Kind {
		case "", catalog.KindDigest, catalog.KindMaster, catalog.KindDownload:
		default:
			errs = append(errs, fmt.Errorf("keys: key id %s: unknown kind %q", id, d.Kind))
		}
	}
	for kind, p := range k.purposes {
		if p.Issuing == "" {
			errs = append(errs, fmt.Errorf("keys: %s: no issuing key id", kind))
		} else if !slices.Contains(p.Accepted, p.Issuing) {
			errs = append(errs, fmt.Errorf("keys: %s: the issuing key id %s is not accepted", kind, p.Issuing))
		}
		for _, id := range p.Accepted {
			if d, ok := cfg.Declared[id]; !ok {
				errs = append(errs, fmt.Errorf("keys: %s: accepted key id %s is not declared", kind, id))
			} else if d.Kind != "" && d.Kind != kind {
				errs = append(errs, fmt.Errorf("keys: %s: accepted key id %s is declared as %s", kind, id, d.Kind))
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return k, nil
}

// Issuing returns the issuing key id of kind's purpose and its checked material.
func (k *Keys) Issuing(ctx context.Context, kind string) (string, []byte, error) {
	p, ok := k.purposes[kind]
	if !ok {
		return "", nil, fmt.Errorf("keys: no %s purpose", kind)
	}
	m, err := k.resolve(ctx, kind, p.Issuing)

	return p.Issuing, m, err
}

// Accepted returns the checked material of keyID, which kind's purpose must accept.
func (k *Keys) Accepted(ctx context.Context, kind, keyID string) ([]byte, error) {
	p, ok := k.purposes[kind]
	if !ok {
		return nil, fmt.Errorf("keys: no %s purpose", kind)
	}
	if !slices.Contains(p.Accepted, keyID) {
		return nil, fmt.Errorf("%w: %s under %s", ErrUnknownKey, keyID, kind)
	}

	return k.resolve(ctx, kind, keyID)
}

// Digest returns the digest key of keyID, a declared digest key id: its bound material, unchanged, one key across captures.
func (k *Keys) Digest(ctx context.Context, keyID string) ([]byte, error) {
	return k.declaredAs(ctx, catalog.KindDigest, keyID)
}

// Encryption returns capture's encryption key under keyID, a declared master key id:
// HKDF-SHA-256 over the bound master material, salted by the recipe's version, its info the capture id's 16 raw bytes then keyID, 64 bytes.
// Only the master material is cached; the capture key is derived at each call.
func (k *Keys) Encryption(ctx context.Context, capture tracepack.UUID, keyID string) ([]byte, error) {
	m, err := k.declaredAs(ctx, catalog.KindMaster, keyID)
	if err != nil {
		return nil, err
	}
	key, err := hkdf.Key(sha256.New, m, []byte(captureKeySalt), string(capture[:])+keyID, captureKeyLen)
	if err != nil {
		return nil, fmt.Errorf("keys: key id %s: %w", keyID, err)
	}

	return key, nil
}

// declaredAs resolves keyID, which must be declared as kind; any other key id fails closed before its binding or provider is read.
func (k *Keys) declaredAs(ctx context.Context, kind, keyID string) ([]byte, error) {
	if d, ok := k.declared[keyID]; !ok || d.Kind != kind {
		return nil, fmt.Errorf("%w: %s under %s", ErrUnknownKey, keyID, kind)
	}

	return k.resolve(ctx, kind, keyID)
}

// resolve runs §9's order for keyID under kind: the binding read, the material resolved at the binding's reference and version
// (the declaration's while unbound), its fingerprint inserted or compared, and only then the cache published.
func (k *Keys) resolve(ctx context.Context, kind, keyID string) ([]byte, error) {
	k.mu.Lock()
	m, ok := k.cache[[2]string{kind, keyID}]
	k.mu.Unlock()
	if ok {
		return m, nil
	}
	b, bound, err := k.binder.KeyBinding(ctx, keyID)
	if err != nil {
		return nil, err
	}
	d := k.declared[keyID]
	ref := d.Provider + ":" + d.Ref
	version := d.Version
	if bound {
		if b.Kind != kind {
			return nil, fmt.Errorf("%w: key %s is bound as %s, requested as %s", catalog.ErrKeyBindingMismatch, keyID, b.Kind, kind)
		}
		ref, version = b.ProviderRef, b.ProviderVersion
	}
	provider, at, _ := strings.Cut(ref, ":")
	p, ok := k.providers[provider]
	if !ok {
		return nil, fmt.Errorf("keys: key id %s: unknown provider %q", keyID, provider)
	}
	asked := version
	m, version, err = p.Material(ctx, at, version)
	if err != nil {
		return nil, fmt.Errorf("keys: %s key id %s at %s version %q: %w", kind, keyID, ref, asked, err)
	}
	if len(m) < minMaterial {
		return nil, fmt.Errorf("keys: key id %s: material of %d bytes, below %d", keyID, len(m), minMaterial)
	}
	bind := catalog.KeyBinding{KeyID: keyID, Kind: kind, ProviderRef: ref, ProviderVersion: version, Fingerprint: Fingerprint(m)}
	if _, err := k.binder.BindKey(ctx, bind); err != nil {
		return nil, err
	}
	k.mu.Lock()
	k.cache[[2]string{kind, keyID}] = m
	k.mu.Unlock()

	return m, nil
}

// Fingerprint is material's fingerprint (masking-keys.md §9): HMAC-SHA-256 keyed by the material over a fixed message, in lowercase hex.
func Fingerprint(material []byte) string {
	h := hmac.New(sha256.New, material)
	h.Write([]byte(fingerprintMessage))

	return hex.EncodeToString(h.Sum(nil))
}
