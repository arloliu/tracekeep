package catalog

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// An error of a call that names an object key does not repeat the key in its text, and keeps its class:
// a caller logs it, and an object key never reaches a log (operations.md §10.4).
func TestKeyedCallsErrorWithoutTheKey(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		const key = "claims/key-sentinel"
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		for name, call := range map[string]func() error{
			"MarkSurvivorDeletable": func() error { _, err := c.MarkSurvivorDeletable(ctx, l, key); return err },
			"RemoveClaimObject":     func() error { return c.RemoveClaimObject(ctx, l, key) },
			"KeyStanding":           func() error { _, err := c.KeyStanding(ctx, key); return err },
		} {
			err := call()
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s under a cancelled context = %v, want its cancellation", name, err)
			}
			if err != nil && strings.Contains(err.Error(), "key-sentinel") {
				t.Errorf("%s's error %q names the key", name, err)
			}
		}
	})
}
