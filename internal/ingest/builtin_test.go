package ingest

import (
	"errors"
	"testing"
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
