package ingest

import (
	"errors"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
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

// A replica executes every unretired configuration of its own classifier, whatever its frame limit,
// so a limit changed and changed back still joins; a configuration of another classifier is refused before anything is activated.
func TestExecutable(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		join := func(limit uint64) error {
			b, err := namedConfig("go-secs/v2.8.0", limit, false)
			if err != nil {
				return err
			}
			cs, err := Executable(ctx, c, b)
			if err != nil {
				return err
			}
			byID, err := Activate(ctx, c, cs)
			if err != nil {
				return err
			}
			ids := make([]int64, 0, len(byID))
			for id := range byID {
				ids = append(ids, id)
			}
			_, err = c.Register(ctx, "r", ids, time.Minute)

			return err
		}
		for _, limit := range []uint64{1, 2, 1} {
			if err := join(limit); err != nil {
				t.Fatalf("join with frame limit %d = %v", limit, err)
			}
		}
		if n := unretired(t, c); n != 2 {
			t.Errorf("%d unretired configurations, want the two limits", n)
		}
		if _, err := c.ActivateConfig(ctx, []byte(`{"classifier":"other","max_frame_len":0}`)); err != nil {
			t.Fatal(err)
		}
		if err := join(3); !errors.Is(err, ErrForeignConfiguration) {
			t.Errorf("join beside another classifier's configuration = %v, want ErrForeignConfiguration", err)
		}
		if n := unretired(t, c); n != 3 {
			t.Errorf("%d unretired configurations after the refusal, want nothing activated", n)
		}
	})
}

func unretired(t *testing.T, c *catalog.Catalog) int {
	t.Helper()
	cs, err := c.UnretiredConfigs(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return len(cs)
}
