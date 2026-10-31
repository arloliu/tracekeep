//go:build webdist

package web

import (
	"io/fs"
	"strings"
	"testing"
)

// With the tag the build is embedded, never the fallback: how a release refuses to ship it.
func TestBuildWithTheTag(t *testing.T) {
	t.Parallel()
	b, err := fs.ReadFile(FS(), "index.html")
	if err != nil || strings.Contains(string(b), "frontend was not built") {
		t.Fatalf("index.html = %v; want the build's", err)
	}
	if assets, err := fs.ReadDir(FS(), "assets"); err != nil || len(assets) == 0 {
		t.Errorf("assets = %v, %v; want the build's scripts", assets, err)
	}
	if !Built() {
		t.Error("Built() with the tag, want true")
	}
}
