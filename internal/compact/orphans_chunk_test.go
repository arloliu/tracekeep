package compact

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkrecords "github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
)

// errListing is a listing's failure.
var errListing = errors.New("listing failed")

// listing is a store whose listings can list a key twice, fail, and whose opens are counted.
type listing struct {
	store.ObjectStore
	// repeat, when set, is listed first and again after every other key of its listing.
	repeat string
	// failArea and failAfter fail the listing of the prefix ending in failArea after failAfter keys, or after its last.
	failArea  string
	failAfter int
	mu        sync.Mutex
	opened    []string
}

func (s *listing) List(ctx context.Context, prefix string, fn func(store.ObjectInfo) error) error {
	var infos []store.ObjectInfo
	err := s.ObjectStore.List(ctx, prefix, func(info store.ObjectInfo) error {
		infos = append(infos, info)

		return nil
	})
	if err != nil {
		return err
	}
	if i := slices.IndexFunc(infos, func(info store.ObjectInfo) bool { return info.Key == s.repeat }); i >= 0 {
		r := infos[i]
		infos = append(append([]store.ObjectInfo{r}, slices.Delete(infos, i, i+1)...), r)
	}
	fails := s.failArea != "" && strings.HasSuffix(prefix, s.failArea)
	for n, info := range infos {
		if fails && n == s.failAfter {
			return errListing
		}
		if err := fn(info); err != nil {
			return err
		}
	}
	if fails {
		return errListing
	}

	return nil
}

func (s *listing) Open(ctx context.Context, key string) (store.Object, error) {
	s.mu.Lock()
	s.opened = append(s.opened, key)
	s.mu.Unlock()

	return s.ObjectStore.Open(ctx, key)
}

// chunks counts the chunks an orphan pass of f looks at.
func (f *fixture) chunks() *int {
	n := new(int)
	f.r.hooks[hookStandings] = func() { *n++ }

	return n
}

// orphans puts n unknown segments of one seq each of capture and returns their keys.
func (f *fixture) orphans(t *testing.T, capture tracepack.UUID, seq0 uint64, n int) []string {
	t.Helper()
	keys := make([]string, n)
	for i := range n {
		b := segment(t, capture, []uint64{seq0 + uint64(i)}, false) //nolint:gosec // a test's small count
		f.put(t, b.key, b.data)
		keys[i] = b.key
	}

	return keys
}

// registered registers b's one seq as ingest would, with evidence of its delivery.
func registered(t *testing.T, c *catalog.Catalog, capture tracepack.UUID, b built, seq uint64) {
	t.Helper()
	p := &catalog.Pack{
		ID: b.id, Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleSegment, Key: b.key, Size: int64(len(b.data)),
		SeqStart: seq, FormatMinor: 1, Ranges: []catalog.Range{{SeqFrom: seq, SeqTo: seq}},
	}
	ev := &catalog.Evidence{Deliveries: []catalog.Identity{{Version: catalog.VersionKey{Capture: capture, Seq: seq}, Incarnation: 1, StreamSeq: int64(seq) + 1}}}
	if out, err := c.RegisterPack(t.Context(), p, catalog.Registration{Evidence: ev, HorizonHour: hour - 24}); err != nil || out != catalog.Registered {
		t.Fatalf("RegisterPack = %v, %v; want registered", out, err)
	}
}

func sorted(keys []string) []string {
	out := slices.Clone(keys)
	slices.Sort(out)

	return out
}

// The pass looks at at most 128 keys a chunk: 127 and 128 keys are one look, 129 two,
// and each unknown key is adopted only on its second sighting.
func TestOrphanChunks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ keys, chunks int }{{127, 1}, {128, 1}, {129, 2}} {
		t.Run(fmt.Sprintf("keys=%d", tc.keys), func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				keys := f.orphans(t, f.admit(t), 1, tc.keys)
				n := f.chunks()
				if rep := f.pass(t); !slices.Equal(sorted(rep.Waiting), sorted(keys)) || len(rep.Adopted) != 0 || *n != tc.chunks {
					t.Fatalf("first pass over %d keys = %d waiting, %d adopted in %d chunks; want all waiting in %d", tc.keys, len(rep.Waiting), len(rep.Adopted), *n, tc.chunks)
				}
				*n = 0
				if rep := f.pass(t); !slices.Equal(sorted(rep.Adopted), sorted(keys)) || *n != tc.chunks {
					t.Fatalf("second pass over %d keys = %d adopted in %d chunks; want all adopted in %d", tc.keys, len(rep.Adopted), *n, tc.chunks)
				}
			})
		})
	}
}

// Registered, tombstoned and unknown keys mixed across two chunks are each settled by their standing:
// the tombstoned deleted, the unknown waiting, and no registered key opened.
func TestOrphanStandingsMixed(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		capture := f.admit(t)
		reg, gone := make([]string, 0, 50), make([]string, 0, 50)
		for i := range 50 {
			b := segment(t, capture, []uint64{uint64(10 + i)}, false) //nolint:gosec // a test's small count
			registered(t, c, capture, b, uint64(10+i))                //nolint:gosec // a test's small count
			f.put(t, b.key, b.data)
			reg = append(reg, b.key)
			d := segment(t, capture, []uint64{2}, false)
			tombstone(t, c, capture, d)
			f.put(t, d.key, d.data)
			gone = append(gone, d.key)
		}
		unknown := f.orphans(t, capture, 100, 50)
		s := &listing{ObjectStore: f.s}
		f.r.Store = s
		n := f.chunks()
		rep := f.pass(t)
		if !slices.Equal(sorted(rep.Deleted), sorted(gone)) || !slices.Equal(sorted(rep.Waiting), sorted(unknown)) || len(rep.Adopted)+len(rep.Quarantined) != 0 || *n != 2 {
			t.Fatalf("pass = %d deleted, %d waiting, %d adopted, %d quarantined in %d chunks; want 50 deleted and 50 waiting in 2", len(rep.Deleted), len(rep.Waiting), len(rep.Adopted), len(rep.Quarantined), *n)
		}
		rep = f.pass(t)
		if !slices.Equal(sorted(rep.Adopted), sorted(unknown)) || len(rep.Deleted) != 0 {
			t.Fatalf("second pass = %d adopted, %d deleted; want the 50 unknown adopted", len(rep.Adopted), len(rep.Deleted))
		}
		for _, k := range s.opened {
			if slices.Contains(reg, k) || slices.Contains(gone, k) {
				t.Errorf("%s opened, a key whose standing settles it", k)
			}
		}
		if len(s.opened) != len(unknown) {
			t.Errorf("%d keys opened, want the %d examined", len(s.opened), len(unknown))
		}
	})
}

// A key listed again in a later chunk is judged once: never its own second sighting, never deleted twice.
func TestOrphanRepeatedKey(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		capture := f.admit(t)
		keys := f.orphans(t, capture, 1, 130)
		s := &listing{ObjectStore: f.s, repeat: keys[0]}
		f.r.Store = s
		n := f.chunks()
		rep := f.pass(t)
		if len(rep.Adopted) != 0 || !slices.Equal(sorted(rep.Waiting), sorted(keys)) || *n != 2 {
			t.Fatalf("first pass = %d adopted, %d waiting of %d keys in %d chunks; want each waiting once, in 2", len(rep.Adopted), len(rep.Waiting), len(keys), *n)
		}
		if rep := f.pass(t); !slices.Equal(sorted(rep.Adopted), sorted(keys)) {
			t.Fatalf("second pass = %d adopted, want each of %d once", len(rep.Adopted), len(keys))
		}

		d := segment(t, capture, []uint64{2}, false)
		tombstone(t, c, capture, d)
		f.put(t, d.key, d.data)
		s.repeat = d.key
		if rep := f.pass(t); !slices.Equal(rep.Deleted, []string{d.key}) {
			t.Errorf("a tombstoned key listed twice = %v deleted, want it once", rep.Deleted)
		}
	})
}

// A failed listing, in the first or the second area and in the middle of a chunk, ends the pass with the store's class,
// keeps the previous pass's unknown keys and never makes a key first listed by it a second sighting.
func TestOrphanListingFails(t *testing.T) {
	t.Parallel()
	for _, area := range []string{"staging/", "archive/"} {
		t.Run(area, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				capture := f.admit(t)
				before := f.orphans(t, capture, 1, 3)
				f.pass(t)
				later := f.orphans(t, capture, 10, 3)
				f.r.Store = &listing{ObjectStore: f.s, failArea: area, failAfter: 2}
				if rep, err := f.r.Orphans(t.Context(), f.lease); !errors.Is(err, tkrecords.ErrStore) || len(rep.Adopted) != 0 {
					t.Fatalf("a pass whose %s listing fails = %d adopted, %v; want nothing and its error", area, len(rep.Adopted), err)
				}
				f.r.Store = f.s
				rep := f.pass(t)
				if !slices.Equal(sorted(rep.Adopted), sorted(before)) || !slices.Equal(sorted(rep.Waiting), sorted(later)) {
					t.Errorf("the next pass = %d adopted, %d waiting; want the 3 sighted before the failure adopted, the 3 put after it waiting", len(rep.Adopted), len(rep.Waiting))
				}
			})
		})
	}
}

// A key registered or tombstoned between the chunk's look and the act is settled by the registration's own guards:
// ingest's registration wins over the adoption, and a tombstoned key is deleted, never registered.
func TestOrphanStandingChangesAfterTheLook(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		capture := f.admit(t)
		ingested := segment(t, capture, []uint64{5}, false)
		lost := segment(t, capture, []uint64{2}, false)
		f.put(t, ingested.key, ingested.data)
		f.put(t, lost.key, lost.data)
		f.pass(t)
		f.r.hooks[hookStandings] = func() {
			f.r.hooks[hookStandings] = nil
			registered(t, c, capture, ingested, 5)
			tombstone(t, c, capture, lost)
		}
		rep := f.pass(t)
		if len(rep.Adopted) != 0 || !slices.Equal(rep.Deleted, []string{lost.key}) {
			t.Fatalf("pass = %d adopted, %v deleted; want nothing adopted and the tombstoned key deleted", len(rep.Adopted), rep.Deleted)
		}
		if got, err := c.KeyStanding(t.Context(), ingested.key); err != nil || got != catalog.KeyRegistered || !f.present(t, ingested.key) {
			t.Errorf("ingest's key = %v, %v, stored %v; want registered and kept", got, err, f.present(t, ingested.key))
		}
		if f.present(t, lost.key) {
			t.Errorf("the tombstoned key is still stored")
		}
	})
}

// A lease lost after a chunk's look ends the pass with ErrLeaseLost: nothing of that chunk or a later one is acted on,
// and the next holder's pass still adopts the keys sighted before.
func TestOrphanLeaseLostAfterTheLook(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		f := newFixture(t, c)
		capture := f.admit(t)
		keys := f.orphans(t, capture, 1, 130)
		f.pass(t)
		gone := segment(t, capture, []uint64{2}, false)
		tombstone(t, c, capture, gone)
		f.put(t, gone.key, gone.data)
		var other catalog.Lease
		looks := 0
		f.r.hooks[hookStandings] = func() {
			looks++
			if looks > 1 {
				return
			}
			if err := c.ReleaseLease(ctx, f.lease); err != nil {
				t.Fatal(err)
			}
			l, ok, err := c.TakeLease(ctx, leaseName, "other", time.Hour)
			if err != nil || !ok {
				t.Fatalf("TakeLease = %v, %v", ok, err)
			}
			other = l
		}
		rep, err := f.r.Orphans(ctx, f.lease)
		if !errors.Is(err, catalog.ErrLeaseLost) || len(rep.Adopted)+len(rep.Deleted) != 0 || looks != 1 {
			t.Fatalf("pass after the lease was lost = %d adopted, %d deleted after %d looks, %v; want nothing after one look and ErrLeaseLost", len(rep.Adopted), len(rep.Deleted), looks, err)
		}
		if !f.present(t, gone.key) {
			t.Errorf("the stale holder deleted the tombstoned key")
		}
		f.r.hooks[hookStandings] = nil
		f.lease = other
		rep = f.pass(t)
		if !slices.Equal(sorted(rep.Adopted), sorted(keys)) || !slices.Equal(rep.Deleted, []string{gone.key}) {
			t.Errorf("the next holder's pass = %d adopted, %v deleted; want the %d sighted adopted and the tombstoned key deleted", len(rep.Adopted), rep.Deleted, len(keys))
		}
	})
}
