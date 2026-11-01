package catalog_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/store"
)

// deletesRecorded is a store that records the keys deleted from it.
type deletesRecorded struct {
	*store.Mem
	mu      sync.Mutex
	deleted []string
}

func (s *deletesRecorded) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deleted = append(s.deleted, key)
	s.mu.Unlock()

	return s.Mem.Delete(ctx, key)
}

// A deletion whose first commit fails retryably is decided by its retry alone:
// with the archive excluded before the retry, the retry's guard refuses, and the compactor removes no object,
// tombstones no key and keeps the inputs merged; without, the retry commits and the inputs are deleted.
func TestDeletionRetriedOverChangedRows(t *testing.T) {
	t.Parallel()
	for _, excluded := range []bool{true, false} {
		name := map[bool]string{true: "archive excluded", false: "rows unchanged"}[excluded]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				ctx := t.Context()
				l, ok, err := c.TakeLease(ctx, "compactor/2", "w", time.Hour)
				if err != nil || !ok {
					t.Fatalf("TakeLease = %v, %v", ok, err)
				}
				inputs, archive := catalog.MergedInputs(t, c, l, time.Hour)
				s := &deletesRecorded{Mem: store.NewMem()}
				for _, p := range append(slices.Clone(inputs), archive) {
					if err := s.Put(ctx, p.Key, bytes.NewReader([]byte("x")), 1); err != nil {
						t.Fatal(err)
					}
				}
				catalog.FailFirstCommit(c, errors.New("commit failed retryably"))
				c.OnRetry(func(string) {
					if excluded {
						if _, err := c.ExcludeInput(ctx, archive.ID, "operator", "act-1"); err != nil {
							t.Error(err)
						}
					}
				})
				r := &compact.Compactor{Catalog: c, Store: s, Log: slog.New(slog.DiscardHandler), DeleteDelay: time.Hour,
					Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
				var rep compact.RetentionReport
				if err := r.Deletions(ctx, l, &rep); err != nil {
					t.Fatal(err)
				}
				if !excluded {
					if rep.Deleted != 2 || len(s.deleted) != 2 {
						t.Errorf("Deletions = %+v, store deletes %v; want both inputs deleted", rep, s.deleted)
					}
					for _, p := range inputs {
						if st := catalog.StateOf(t, c, p.ID); st != "deleted" || !catalog.HasTombstone(t, c, p.Key) {
							t.Errorf("input %s is %s, tombstoned %v; want deleted and tombstoned", p.Key, st, catalog.HasTombstone(t, c, p.Key))
						}
					}

					return
				}
				if rep.Deleted != 0 || rep.Refused != 2 || len(s.deleted) != 0 {
					t.Errorf("Deletions = %+v, store deletes %v; want both refused and no object removed", rep, s.deleted)
				}
				for _, p := range inputs {
					if st := catalog.StateOf(t, c, p.ID); st != "merged" || catalog.HasTombstone(t, c, p.Key) {
						t.Errorf("input %s is %s, tombstoned %v; want merged, without tombstone", p.Key, st, catalog.HasTombstone(t, c, p.Key))
					}
				}
			})
		})
	}
}
