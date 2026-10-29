package admin_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
)

// digestKey writes synthetic material for key id d-1 to a file and returns keys declaring it as a digest key id, and the file.
func digestKey(t *testing.T, c *catalog.Catalog, b byte) (*keys.Keys, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "d-1")
	writeMaterial(t, path, b)
	k, err := keys.New(keys.Config{Declared: map[string]keys.Declaration{"d-1": {Provider: keys.ProviderFile, Ref: path, Kind: catalog.KindDigest}}}, c)
	if err != nil {
		t.Fatal(err)
	}

	return k, path
}

func writeMaterial(t *testing.T, path string, b byte) {
	t.Helper()
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))), 0o600); err != nil {
		t.Fatal(err)
	}
}

// key verify reports the binding against the material resolved now, audited as a read; an unbound key id fails and writes nothing.
func TestKeyVerify(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		k, path := digestKey(t, c, 0x81)
		a := &admin.Admin{Catalog: c, Actor: "key-op", Keys: k}
		if _, err := admin.Run(ctx, a, []string{"key", "verify", "d-1"}); !errors.Is(err, keys.ErrNotBound) {
			t.Fatalf("key verify of an unbound key id = %v, want ErrNotBound", err)
		}
		if _, err := k.Digest(ctx, "d-1"); err != nil {
			t.Fatal(err)
		}
		out, err := admin.Run(ctx, a, []string{"key", "verify", "d-1"})
		if v, ok := out.(keys.Verification); err != nil || !ok || !v.Match || v.Kind != catalog.KindDigest {
			t.Errorf("key verify = %+v, %v; want a match", out, err)
		}
		writeMaterial(t, path, 0x82)
		out, err = admin.Run(ctx, a, []string{"key", "verify", "d-1"})
		if v, ok := out.(keys.Verification); err != nil || !ok || v.Match || v.Resolved == v.Bound {
			t.Errorf("key verify of changed material = %+v, %v; want a reported mismatch", out, err)
		}
		rows, err := c.AuditByActor(ctx, "key-op")
		if err != nil || len(rows) != 2 || rows[0].Action != "key verify" || rows[1].Detail != "key verify d-1" {
			t.Errorf("audit = %+v, %v; want a row per answered verify", rows, err)
		}
		if _, err := admin.Run(ctx, a, []string{"key", "verify"}); !errors.Is(err, admin.ErrUsage) {
			t.Errorf("key verify without a key id = %v, want a usage error", err)
		}
	})
}
