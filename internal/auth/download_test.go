package auth

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
)

const packKey = "packs/t1/2026/10/28/08/0001.tpk"

// keyDir holds synthetic download keys dl-0, dl-1 and dl-2, each a file of mode 0600.
func keyDir(t *testing.T) map[string]keys.Declaration {
	t.Helper()
	dir := t.TempDir()
	out := map[string]keys.Declaration{}
	for i, id := range []string{"dl-0", "dl-1", "dl-2"} {
		path := filepath.Join(dir, id)
		text := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{byte(i + 1)}, 32))
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		out[id] = keys.Declaration{Provider: keys.ProviderFile, Ref: path}
	}

	return out
}

// downloads is a replica's download tokens under the issuing key id and the accepted ones, over cat.
func downloads(t *testing.T, cat *catalog.Catalog, declared map[string]keys.Declaration, issuing string, accepted ...string) (*Downloads, *failures) {
	t.Helper()
	k, err := keys.New(keys.Config{Declared: declared, Download: keys.Purpose{Issuing: issuing, Accepted: accepted}}, cat)
	if err != nil {
		t.Fatal(err)
	}
	f := &failures{}

	return &Downloads{Keys: k, Now: newClock().Now, Failures: f}, f
}

func TestDownloadToken(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	d, f := downloads(t, cat, keyDir(t), "dl-1", "dl-1")
	now := d.Now()
	tok, err := d.Sign(t.Context(), packKey, now.Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(tok, "+/= ") {
		t.Errorf("token %q is not URL-safe", tok)
	}
	if err := d.Verify(t.Context(), tok, packKey); err != nil {
		t.Errorf("Verify = %v", err)
	}
	payload, mac, _ := strings.Cut(tok, ".")
	flipped := []byte(payload)
	flipped[len(flipped)-1] ^= 1 // the object key's last byte
	expired, err := d.Sign(t.Context(), packKey, now)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct{ token, key string }{
		"another object":    {tok, packKey + "x"},
		"at its expiry":     {expired, packKey},
		"a changed payload": {string(flipped) + "." + mac, packKey},
		"a changed mac":     {payload + "." + strings.Repeat("A", len(mac)), packKey},
		"no mac":            {payload, packKey},
		"empty":             {"", packKey},
		"not base64":        {"!!.!!", packKey},
		"too long":          {strings.Repeat("A", 8192) + "." + mac, packKey},
	} {
		if err := d.Verify(t.Context(), c.token, c.key); reason(err) != ReasonDownload {
			t.Errorf("Verify of %s = %v, want refused as download", name, err)
		}
	}
	if got := f.got(); len(got) != 8 || slices.IndexFunc(got, func(r string) bool { return r != ReasonDownload }) >= 0 {
		t.Errorf("failures counted %v, want eight download refusals", got)
	}
}

// A token under a key id the replica does not accept, or one bound to another purpose, is refused.
func TestDownloadTokenKeyIDs(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	declared := keyDir(t)
	other, _ := downloads(t, cat, declared, "dl-2", "dl-2")
	tok, err := other.Sign(t.Context(), packKey, other.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	d, _ := downloads(t, cat, declared, "dl-1", "dl-1", "dl-0")
	if err := d.Verify(t.Context(), tok, packKey); reason(err) != ReasonDownload {
		t.Errorf("Verify under a key id not accepted = %v, want refused", err)
	}
	if _, err := cat.BindKey(t.Context(), catalog.KeyBinding{KeyID: "dl-0", Kind: catalog.KindSession, ProviderRef: "file:x", Fingerprint: "00"}); err != nil {
		t.Fatal(err)
	}
	session, _ := downloads(t, cat, declared, "dl-0", "dl-0")
	if _, err := session.Sign(t.Context(), packKey, session.Now().Add(time.Minute)); err == nil {
		t.Error("Sign under a key id bound as session succeeded")
	}
}

// Rotation's three steps (masking-keys.md §9): the new key id accepted everywhere, issuance switched to it, the old one dropped,
// after which a token issued under the old id is refused and one under the new id still verifies.
func TestDownloadKeyRotation(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	declared := keyDir(t)
	before, _ := downloads(t, cat, declared, "dl-0", "dl-0")
	old, err := before.Sign(t.Context(), packKey, before.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	accepting, _ := downloads(t, cat, declared, "dl-0", "dl-0", "dl-1")
	if err := accepting.Verify(t.Context(), old, packKey); err != nil {
		t.Errorf("step 1: Verify of the old token = %v", err)
	}
	switched, _ := downloads(t, cat, declared, "dl-1", "dl-0", "dl-1")
	cur, err := switched.Sign(t.Context(), packKey, switched.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for name, tok := range map[string]string{"old": old, "new": cur} {
		if err := switched.Verify(t.Context(), tok, packKey); err != nil {
			t.Errorf("step 2: Verify of the %s token = %v", name, err)
		}
		if err := accepting.Verify(t.Context(), tok, packKey); err != nil {
			t.Errorf("step 2: a replica still at step 1 refuses the %s token: %v", name, err)
		}
	}
	dropped, _ := downloads(t, cat, declared, "dl-1", "dl-1")
	if err := dropped.Verify(t.Context(), old, packKey); reason(err) != ReasonDownload {
		t.Errorf("step 3: Verify of the old token = %v, want refused", err)
	}
	if err := dropped.Verify(t.Context(), cur, packKey); err != nil {
		t.Errorf("step 3: Verify of the new token = %v", err)
	}
}

// hostile is a token of payload under an arbitrary MAC.
func hostile(payload ...byte) string {
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
}

// hostileTokens are payloads whose lengths lie: no length, a truncated or overflowing uvarint, a key id longer than the payload,
// a truncated expiry.
var hostileTokens = map[string]string{
	"no length":           hostile(1),
	"truncated uvarint":   hostile(1, 0x80),
	"overflowing uvarint": hostile(1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02),
	"the largest uvarint": hostile(1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01),
	"a huge key id":       hostile(1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 'd'),
	"a key id too long":   hostile(1, 5, 'd', 'l'),
	"a truncated expiry":  hostile(1, 4, 'd', 'l', '-', '1', 0, 0, 0),
	"another version":     hostile(2, 4, 'd', 'l', '-', '1', 0, 0, 0, 0, 0, 0, 0, 0),
}

func TestDownloadTokenHostileLengths(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	d, _ := downloads(t, cat, keyDir(t), "dl-1", "dl-1")
	for name, tok := range hostileTokens {
		if err := d.Verify(t.Context(), tok, packKey); reason(err) != ReasonDownload {
			t.Errorf("Verify of %s = %v, want refused", name, err)
		}
	}
}

// The key id is under the MAC: a token's key id changed to another accepted one, even one holding the same material, is refused.
func TestDownloadTokenKeyIDAuthenticated(t *testing.T) {
	t.Parallel()
	cat := catalogtest.Open(t, catalog.EngineSQLite)
	declared := keyDir(t)
	declared["dl-0"] = declared["dl-1"] // one material under two key ids
	d, _ := downloads(t, cat, declared, "dl-1", "dl-0", "dl-1")
	tok, err := d.Sign(t.Context(), packKey, d.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	p, m, _ := strings.Cut(tok, ".")
	payload, err := base64.RawURLEncoding.DecodeString(p)
	if err != nil {
		t.Fatal(err)
	}
	swapped := bytes.Replace(payload, []byte("dl-1"), []byte("dl-0"), 1)
	if err := d.Verify(t.Context(), base64.RawURLEncoding.EncodeToString(swapped)+"."+m, packKey); reason(err) != ReasonDownload {
		t.Errorf("Verify with the key id swapped = %v, want refused", err)
	}
}

// fixedKeys is a download purpose of one key id, dl-1, for the fuzz target, which runs without a catalog.
type fixedKeys struct{}

var fixedMaterial = bytes.Repeat([]byte{7}, 32)

func (fixedKeys) Issuing(context.Context, string) (string, []byte, error) {
	return "dl-1", fixedMaterial, nil
}

func (fixedKeys) Accepted(_ context.Context, _, keyID string) ([]byte, error) {
	if keyID != "dl-1" {
		return nil, errors.New("not accepted")
	}

	return fixedMaterial, nil
}

// FuzzDownloadToken: no input panics the parser, and the only token admitted is the one issued.
func FuzzDownloadToken(f *testing.F) {
	d := &Downloads{Keys: fixedKeys{}, Now: newClock().Now}
	valid, err := d.Sign(context.Background(), packKey, d.Now().Add(time.Minute))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	for _, tok := range hostileTokens {
		f.Add(tok)
	}
	f.Fuzz(func(t *testing.T, tok string) {
		if err := d.Verify(context.Background(), tok, packKey); err == nil && tok != valid {
			t.Errorf("Verify admitted %q, not the token issued", tok)
		}
	})
}
