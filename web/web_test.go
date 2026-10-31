//go:build !webdist

package web

import (
	"io/fs"
	"strings"
	"testing"
)

// Without the tag the fallback is embedded: one page saying the frontend was not built.
func TestFallbackWithoutTheTag(t *testing.T) {
	t.Parallel()
	b, err := fs.ReadFile(FS(), "index.html")
	if err != nil || !strings.Contains(string(b), "frontend was not built") {
		t.Fatalf("index.html = %q, %v; want the fallback", b, err)
	}
	if Built() {
		t.Error("Built() without the tag, want false")
	}
}
