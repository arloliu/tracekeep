package catalog

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// This file lends the package's seams and fixtures to its external tests, which drive other packages' passes over a catalog.

// FailFirstCommit makes the next commit whose statements succeeded fail with err, which is retried like a deadlock.
func FailFirstCommit(c *Catalog, err error) {
	var done atomic.Bool
	c.testRetryable = err
	c.testCommit = func() error {
		if done.CompareAndSwap(false, true) {
			return err
		}

		return nil
	}
}

// FailCommits makes every commit whose statements succeeded fail with err, which is not retried.
func FailCommits(c *Catalog, err error) {
	c.testCommit = func() error { return err }
}

// MergedInputs merges two segments of a fresh capture under l with delete delay d, returning the inputs and the archive.
func MergedInputs(t *testing.T, c *Catalog, l Lease, d time.Duration) (inputs []*Pack, archive *Pack) {
	t.Helper()
	_, inputs, archive = mergedScope(t, c, l, d)

	return inputs, archive
}

// StateOf is pack id's state.
func StateOf(t *testing.T, c *Catalog, id tracepack.UUID) string {
	t.Helper()

	return packState(t, c, id)
}

// HasTombstone reports whether key has a tombstone.
func HasTombstone(t *testing.T, c *Catalog, key string) bool {
	t.Helper()

	return count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, key) > 0
}
