package keys

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// material is synthetic key material of n bytes, each b.
func material(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

// keyFile writes m as base64 text, with a trailing newline, to a file of mode perm and returns its path.
func keyFile(t *testing.T, m []byte, perm os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key")
	writeKey(t, path, m, perm)

	return path
}

func writeKey(t *testing.T, path string, m []byte, perm os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(m)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, perm); err != nil {
		t.Fatal(err)
	}
}

// specFingerprint is masking-keys.md §9's fingerprint, computed here from the spec's text: HMAC-SHA-256 keyed by the material over the fixed message, lowercase hex.
func specFingerprint(m []byte) string {
	h := hmac.New(sha256.New, m)
	h.Write([]byte("tracekeep/key-fingerprint/v1"))

	return hex.EncodeToString(h.Sum(nil))
}

// downloadOnly is a configuration whose download purpose issues and accepts key id dl-1, declared at path.
func downloadOnly(path string) Config {
	return Config{
		Declared: map[string]Declaration{"dl-1": {Provider: ProviderFile, Ref: path}},
		Download: Purpose{Issuing: "dl-1", Accepted: []string{"dl-1"}},
	}
}

func newKeys(t *testing.T, cfg Config, cat *catalog.Catalog) *Keys {
	t.Helper()
	k, err := New(cfg, cat)
	if err != nil {
		t.Fatal(err)
	}

	return k
}

// noMaterial fails when err's text holds m, raw or as base64.
func noMaterial(t *testing.T, err error, m []byte) {
	t.Helper()
	if err != nil && (strings.Contains(err.Error(), string(m)) || strings.Contains(err.Error(), base64.StdEncoding.EncodeToString(m))) {
		t.Errorf("error %q holds the material", err)
	}
}

func TestFileProvider(t *testing.T) {
	t.Parallel()
	m := material(1, 32)
	got, version, err := File{}.Material(t.Context(), keyFile(t, m, 0o600), "")
	if err != nil || !bytes.Equal(got, m) || version != "" {
		t.Errorf("Material of a 0600 file = %x, %q, %v", got, version, err)
	}
	for _, perm := range []os.FileMode{0o640, 0o604, 0o644, 0o400, 0o700} {
		if _, _, err := (File{}).Material(t.Context(), keyFile(t, m, perm), ""); err == nil {
			t.Errorf("Material of a %o file succeeded", perm)
		}
	}
	if _, _, err := (File{}).Material(t.Context(), filepath.Join(t.TempDir(), "absent"), ""); err == nil {
		t.Error("Material of an absent file succeeded")
	}
	bad := filepath.Join(t.TempDir(), "bad")
	if err := os.WriteFile(bad, []byte("not base64!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (File{}).Material(t.Context(), bad, ""); err == nil {
		t.Error("Material of a file holding no base64 succeeded")
	}
	if _, _, err := (File{}).Material(t.Context(), keyFile(t, m, 0o600), "3"); err == nil {
		t.Error("Material at a version of a provider without versions succeeded")
	}
}

// A key file above 64 KiB is refused whole, never read as its prefix, and binds nothing.
func TestOversizedKeyFileRefused(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	path := filepath.Join(t.TempDir(), "key")
	// Valid base64 whole, and its first 64 KiB plus one byte would be too: so only the size refuses it.
	text := append(append(bytes.Repeat([]byte("A"), 65536), '\n'), "AAAA"...)
	if err := os.WriteFile(path, text, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (File{}).Material(t.Context(), path, ""); err == nil {
		t.Error("Material of a 65,541-byte file succeeded")
	}
	if _, _, err := newKeys(t, downloadOnly(path), cat).Issuing(t.Context(), catalog.KindDownload); err == nil {
		t.Error("Issuing from an oversized file succeeded")
	}
	if _, ok, _ := cat.KeyBinding(t.Context(), "dl-1"); ok {
		t.Error("an oversized file was bound")
	}
}

func TestEnvProvider(t *testing.T) {
	m := material(2, 32)
	t.Setenv("TK_TEST_KEY", " "+base64.StdEncoding.EncodeToString(m)+"\n")
	got, version, err := Env{}.Material(t.Context(), "TK_TEST_KEY", "")
	if err != nil || !bytes.Equal(got, m) || version != "" {
		t.Errorf("Material = %x, %q, %v", got, version, err)
	}
	if _, _, err := (Env{}).Material(t.Context(), "TK_TEST_KEY_UNSET", ""); err == nil {
		t.Error("Material of an unset variable succeeded")
	}
}

func TestNewChecksTheConfiguration(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	path := keyFile(t, material(1, 32), 0o600)
	env := downloadOnly(path)
	env.Declared["dl-1"] = Declaration{Provider: ProviderEnv, Ref: "TK_KEY"}
	for name, cfg := range map[string]Config{
		"env outside development": env,
		"issuing not accepted": {
			Declared: map[string]Declaration{"dl-0": {Provider: ProviderFile, Ref: path}, "dl-1": {Provider: ProviderFile, Ref: path}},
			Download: Purpose{Issuing: "dl-1", Accepted: []string{"dl-0"}},
		},
		"accepted not declared": {Declared: downloadOnly(path).Declared, Download: Purpose{Issuing: "dl-1", Accepted: []string{"dl-1", "dl-0"}}},
		"unknown provider":      {Declared: map[string]Declaration{"dl-1": {Provider: "vault", Ref: "x"}}, Download: downloadOnly(path).Download},
		"no issuing key id":     {Declared: downloadOnly(path).Declared, Download: Purpose{Accepted: []string{"dl-1"}}},
	} {
		if _, err := New(cfg, cat); err == nil {
			t.Errorf("New with %s succeeded", name)
		}
	}
	env.Dev = true
	if _, err := New(env, cat); err != nil {
		t.Errorf("New with env in development = %v", err)
	}
}

// §9's order of a resolution: the binding read, the material resolved, its fingerprint inserted or compared, and only then cached.
func TestResolutionBindsThenCaches(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(3, 32)
	path := keyFile(t, m, 0o600)
	k := newKeys(t, downloadOnly(path), cat)
	id, got, err := k.Issuing(t.Context(), catalog.KindDownload)
	if err != nil || id != "dl-1" || !bytes.Equal(got, m) {
		t.Fatalf("Issuing = %q, %x, %v", id, got, err)
	}
	b, ok, err := cat.KeyBinding(t.Context(), "dl-1")
	if !ok || err != nil || b.Kind != catalog.KindDownload || b.Fingerprint != specFingerprint(m) || b.ProviderRef != "file:"+path || b.ProviderVersion != "" {
		t.Errorf("binding = %+v, %v, %v; want download, file:%s, the material's fingerprint", b, ok, err, path)
	}
	// The cache answers once the check passed: the file's later change is not read.
	writeKey(t, path, material(4, 32), 0o600)
	if got, err := k.Accepted(t.Context(), catalog.KindDownload, "dl-1"); err != nil || !bytes.Equal(got, m) {
		t.Errorf("Accepted from the cache = %x, %v; want the bound material", got, err)
	}
}

// Changed material fails closed against the binding and caches nothing: once the file is restored, the next resolution succeeds.
func TestChangedMaterialFailsClosed(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(5, 32)
	path := keyFile(t, m, 0o600)
	if _, _, err := newKeys(t, downloadOnly(path), cat).Issuing(t.Context(), catalog.KindDownload); err != nil {
		t.Fatal(err)
	}
	other := material(6, 32)
	writeKey(t, path, other, 0o600)
	k := newKeys(t, downloadOnly(path), cat)
	_, _, err := k.Issuing(t.Context(), catalog.KindDownload)
	if !errors.Is(err, catalog.ErrKeyBindingMismatch) {
		t.Fatalf("Issuing of changed material = %v, want a binding mismatch", err)
	}
	noMaterial(t, err, other)
	writeKey(t, path, m, 0o600)
	if _, got, err := k.Issuing(t.Context(), catalog.KindDownload); err != nil || !bytes.Equal(got, m) {
		t.Errorf("Issuing after the file was restored = %x, %v; want the bound material, nothing cached by the failure", got, err)
	}
}

// A bound key id is resolved at its binding's reference, whatever its declaration says now.
func TestBoundKeyResolvedAtItsBinding(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(7, 32)
	path := keyFile(t, m, 0o600)
	if _, _, err := newKeys(t, downloadOnly(path), cat).Issuing(t.Context(), catalog.KindDownload); err != nil {
		t.Fatal(err)
	}
	moved := downloadOnly(keyFile(t, material(8, 32), 0o600))
	if _, got, err := newKeys(t, moved, cat).Issuing(t.Context(), catalog.KindDownload); err != nil || !bytes.Equal(got, m) {
		t.Errorf("Issuing after the declaration moved = %x, %v; want the material at the binding's reference", got, err)
	}
}

// A key id has one purpose: a request under another kind, or a key id bound under another, fails closed without its material.
func TestPurposesStayDistinct(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(9, 32)
	k := newKeys(t, downloadOnly(keyFile(t, m, 0o600)), cat)
	if _, err := k.Accepted(t.Context(), catalog.KindSession, "dl-1"); err == nil {
		t.Error("Accepted under the session kind succeeded")
	}
	if _, _, err := k.Issuing(t.Context(), catalog.KindDigest); err == nil {
		t.Error("Issuing under the digest kind succeeded")
	}
	if _, err := cat.BindKey(t.Context(), catalog.KeyBinding{KeyID: "dl-1", Kind: catalog.KindSession, ProviderRef: "file:x", Fingerprint: specFingerprint(m)}); err != nil {
		t.Fatal(err)
	}
	_, _, err := k.Issuing(t.Context(), catalog.KindDownload)
	if !errors.Is(err, catalog.ErrKeyBindingMismatch) {
		t.Errorf("Issuing of a key id bound as session = %v, want a binding mismatch", err)
	}
	noMaterial(t, err, m)
}

// The session purpose issues and accepts its own key ids; a session key id serves no download, nor a download key id a session.
func TestSessionPurpose(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	sm, dm := material(12, 32), material(13, 32)
	cfg := downloadOnly(keyFile(t, dm, 0o600))
	cfg.Declared["s-1"] = Declaration{Provider: ProviderFile, Ref: keyFile(t, sm, 0o600), Kind: catalog.KindSession}
	cfg.Session = Purpose{Issuing: "s-1", Accepted: []string{"s-1"}}
	k := newKeys(t, cfg, cat)
	if id, got, err := k.Issuing(t.Context(), catalog.KindSession); err != nil || id != "s-1" || !bytes.Equal(got, sm) {
		t.Errorf("Issuing(session) = %s, %x, %v; want s-1 and its material", id, got, err)
	}
	if b, bound, err := cat.KeyBinding(t.Context(), "s-1"); err != nil || !bound || b.Kind != catalog.KindSession {
		t.Errorf("s-1's binding = %+v, %t, %v; want bound as session", b, bound, err)
	}
	if _, err := k.Accepted(t.Context(), catalog.KindDownload, "s-1"); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("Accepted(download, s-1) = %v, want ErrUnknownKey", err)
	}
	if _, err := k.Accepted(t.Context(), catalog.KindSession, "dl-1"); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("Accepted(session, dl-1) = %v, want ErrUnknownKey", err)
	}
	cfg.Download.Accepted = []string{"dl-1", "s-1"}
	if _, err := New(cfg, cat); err == nil {
		t.Error("New with a session key id accepted for downloads succeeded")
	}
}

func TestAcceptedKeyIDs(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	old, cur := material(10, 32), material(11, 32)
	cfg := Config{
		Declared: map[string]Declaration{
			"dl-0": {Provider: ProviderFile, Ref: keyFile(t, old, 0o600)},
			"dl-1": {Provider: ProviderFile, Ref: keyFile(t, cur, 0o600)},
		},
		Download: Purpose{Issuing: "dl-1", Accepted: []string{"dl-0", "dl-1"}},
	}
	k := newKeys(t, cfg, cat)
	if got, err := k.Accepted(t.Context(), catalog.KindDownload, "dl-0"); err != nil || !bytes.Equal(got, old) {
		t.Errorf("Accepted(dl-0) = %x, %v", got, err)
	}
	if _, err := k.Accepted(t.Context(), catalog.KindDownload, "dl-9"); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("Accepted of an undeclared key id = %v, want ErrUnknownKey", err)
	}
	cfg.Download.Accepted = []string{"dl-1"}
	if _, err := newKeys(t, cfg, cat).Accepted(t.Context(), catalog.KindDownload, "dl-0"); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("Accepted of a dropped key id = %v, want ErrUnknownKey", err)
	}
}

func TestShortMaterialRefused(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	m := material(12, 31)
	_, _, err := newKeys(t, downloadOnly(keyFile(t, m, 0o600)), cat).Issuing(t.Context(), catalog.KindDownload)
	if err == nil {
		t.Fatal("Issuing of 31 bytes of material succeeded")
	}
	noMaterial(t, err, m)
	if _, ok, _ := cat.KeyBinding(t.Context(), "dl-1"); ok {
		t.Error("short material was bound")
	}
}
