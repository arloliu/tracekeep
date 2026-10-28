package catalog

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// race runs first, holds it on the capture row, starts second, which waits on that row, and releases first.
func race(t *testing.T, c *Catalog, first, second func()) {
	t.Helper()
	held, release := holdAt(c, hookLocked)
	a := run(first)
	<-held
	waits := c.write.Stats().WaitCount
	b := run(second)
	waitBlocked(t, c, waits, "INSERT INTO captures", 1)
	close(release)
	<-a
	<-b
}

// A merged input's deletion and a redelivered registration of its pack serialize on the capture row, in either order:
// the registration finds the pack registered and writes nothing, and the deletion leaves the pack deleted with its tombstone.
func TestRegistrationRacingADeletion(t *testing.T) {
	t.Parallel()
	for _, deleteFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "deletion first", false: "registration first"}[deleteFirst], func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				l := compactorLease(t, c, "w")
				capture, inputs, archive := mergedScope(t, c, l, 0)
				in := inputs[0]
				var out PackOutcome
				var deleted bool
				var errR, errD error
				register := func() { out, errR = c.RegisterPack(t.Context(), in, Registration{Evidence: &Evidence{}}) }
				del := func() {
					deleted, errD = c.DeletePack(t.Context(), l, Deletable{Pack: in.ID, Capture: capture, Key: in.Key}, deleteAt(t, c, archive), time.Hour)
				}
				if deleteFirst {
					race(t, c, del, register)
				} else {
					race(t, c, register, del)
				}
				if errR != nil || errD != nil || out != AlreadyRegistered || !deleted {
					t.Fatalf("RegisterPack = %v, %v; DeletePack = %v, %v", out, errR, deleted, errD)
				}
				if packState(t, c, in.ID) != "deleted" || count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, in.Key) != 1 {
					t.Error("the deletion's outcome did not stand")
				}
			})
		})
	}
}

// A deletion and a registration of another pack naming the input's key serialize on the capture row, their outcomes exclusive:
// the deletion first tombstones the key and the registration is refused for it; the registration first makes the deletion find
// another row naming the key and refuse, the input left merged.
func TestSharedKeyRegistrationRacingADeletion(t *testing.T) {
	t.Parallel()
	for _, deleteFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "deletion first", false: "registration first"}[deleteFirst], func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				l := compactorLease(t, c, "w")
				capture, inputs, archive := mergedScope(t, c, l, 0)
				in := inputs[0]
				twin := segment(t, capture, 3, 40, 49)
				twin.Key = in.Key
				var out PackOutcome
				var deleted bool
				var errR, errD error
				register := func() { out, errR = c.RegisterPack(t.Context(), twin, Registration{Evidence: &Evidence{}}) }
				del := func() {
					deleted, errD = c.DeletePack(t.Context(), l, Deletable{Pack: in.ID, Capture: capture, Key: in.Key}, deleteAt(t, c, archive), time.Hour)
				}
				if deleteFirst {
					race(t, c, del, register)
				} else {
					race(t, c, register, del)
				}
				if errR != nil || errD != nil {
					t.Fatalf("RegisterPack = %v, DeletePack = %v", errR, errD)
				}
				tombs := count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, in.Key)
				twins := count(t, c, `SELECT COUNT(*) FROM packs WHERE pack_id = ?`, twin.ID[:])
				if deleteFirst {
					if !deleted || out != Tombstoned || tombs != 1 || twins != 0 || packState(t, c, in.ID) != "deleted" {
						t.Errorf("deletion first: deleted %v, registration %v, tombstones %d, twin rows %d", deleted, out, tombs, twins)
					}

					return
				}
				if deleted || out != Registered || tombs != 0 || twins != 1 || packState(t, c, in.ID) != "merged" {
					t.Errorf("registration first: deleted %v, registration %v, tombstones %d, twin rows %d", deleted, out, tombs, twins)
				}
			})
		})
	}
}

// An eviction and a registration into its hour by an ingest whose horizon is still older serialize on the capture row:
// a registration first is evicted with the hour; one after the eviction meets the hour's evicted ranges and is refused,
// its key tombstoned and nothing registered, so the hour is not a candidate again.
func TestRegistrationRacingAnEviction(t *testing.T) {
	t.Parallel()
	for _, evictFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "eviction first", false: "registration first"}[evictFirst], func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				l := compactorLease(t, c, "w")
				capture := admitted(t, c)
				old, late := segment(t, capture, 1, 0, 9), segment(t, capture, 1, 10, 19)
				register(t, c, old, Registration{Evidence: &Evidence{}})
				recordRetention(t, c, l, month, 0)
				var out PackOutcome
				var ev Eviction
				var errR, errE error
				reg := func() { out, errR = c.RegisterPack(t.Context(), late, Registration{Evidence: &Evidence{}}) }
				evict := func() { ev, errE = c.EvictHour(t.Context(), l, Scope{capture, 1}, hourEvictedNow) }
				if evictFirst {
					race(t, c, evict, reg)
				} else {
					race(t, c, reg, evict)
				}
				want := map[bool]PackOutcome{true: BeyondHorizon, false: Registered}[evictFirst]
				if errR != nil || errE != nil || out != want || !ev.Evicted {
					t.Fatalf("RegisterPack = %v, %v; EvictHour = %+v, %v; want %v", out, errR, ev, errE, want)
				}
				left := evictCandidates(t, c, hourEvictedNow)
				if evictFirst {
					if !slices.Equal(ev.Packs, []string{old.Key}) || len(left) != 0 {
						t.Errorf("evicted %v, candidates %v; want the old pack evicted and no candidate", ev.Packs, left)
					}
					if n := count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, late.Key); n != 1 {
						t.Errorf("%d tombstones of the refused key, want 1", n)
					}
					if n := count(t, c, `SELECT COUNT(*) FROM packs`); n != 0 {
						t.Errorf("%d packs after the refused registration, want none", n)
					}

					return
				}
				slices.Sort(ev.Packs)
				keys := []string{old.Key, late.Key}
				slices.Sort(keys)
				if !slices.Equal(ev.Packs, keys) || len(left) != 0 {
					t.Errorf("evicted %v, candidates %v; want both packs evicted and no candidate", ev.Packs, left)
				}
			})
		})
	}
}

// Parking and an eviction of its hour, which holds a registered pack, serialize on the capture row, the parking's horizon read before either and still keeping the hour:
// parking first leaves an unresolved version, which the eviction keeps the object for (a version still parking is not moved);
// an eviction first releases the object, and the parking meets the hour's evicted ranges, finds it beyond retention and parks nothing.
func TestParkingRacingAnEviction(t *testing.T) {
	t.Parallel()
	for _, evictFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "eviction first", false: "parking first"}[evictFirst], func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				l := compactorLease(t, c, "w")
				capture := admitted(t, c)
				claimRow(t, c, capture, 12, 1, "claims/12")
				old := segment(t, capture, 1, 0, 9)
				register(t, c, old, Registration{Evidence: &Evidence{}})
				recordRetention(t, c, l, month, 0)
				d := identity(capture, 12, 9)
				d.Hour = 1
				var b Begun
				var ev Eviction
				var errP, errE error
				// The moment before the horizon passed the hour.
				parkIt := func() { b, errP = c.BeginParking(t.Context(), d, "claim-conflict", "claims/12", d.Hour) }
				evict := func() { ev, errE = c.EvictHour(t.Context(), l, Scope{capture, 1}, hourEvictedNow) }
				if evictFirst {
					race(t, c, evict, parkIt)
				} else {
					race(t, c, parkIt, evict)
				}
				if errP != nil || errE != nil || !ev.Evicted {
					t.Fatalf("BeginParking = %+v, %v; EvictHour = %+v, %v", b, errP, ev, errE)
				}
				if !slices.Equal(ev.Packs, []string{old.Key}) {
					t.Errorf("evicted %v, want the hour's pack", ev.Packs)
				}
				if evictFirst {
					if !b.BeyondRetention || !slices.Equal(ev.Claims, []string{"claims/12"}) || count(t, c, `SELECT COUNT(*) FROM parked`) != 0 {
						t.Errorf("parking %+v, eviction %+v; want beyond retention and the object released", b, ev)
					}

					return
				}
				if b.State != VersionParking || ev.Survivors != 1 || len(ev.Claims) != 0 {
					t.Errorf("parking %+v, eviction %+v; want the version parking and the object a survivor", b, ev)
				}
				if s, _ := versionState(t, c, d); s != "parking" {
					t.Errorf("the version is %s, want parking: eviction moves only parked versions", s)
				}
			})
		})
	}
}

// An object two versions of one (capture, seq) name is kept until the last satisfies the predicate:
// one staged and one parked keeps it; the parked one terminated with a current copy waiting still keeps it; acknowledged, it goes.
func TestSurvivorOfTwoVersions(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		capture := admitted(t, c)
		staged, other := identity(capture, 12, 9), identity(capture, 12, 10)
		other.Version.Digest = [32]byte{2}
		for _, d := range []*Identity{staged, other} {
			d.Hour = 1
		}
		park(t, c, staged, "staged")
		park(t, c, other, "parked")
		claimRow(t, c, capture, 12, 1, "claims/12")
		recordRetention(t, c, l, month, 0)
		if ev := evict(t, c, l, Scope{capture, 1}, hourEvictedNow); ev.Survivors != 1 {
			t.Fatalf("EvictHour = %+v, want the object a survivor", ev)
		}
		lapse(t, c, `INSERT INTO bus_streams (incarnation_id, role, stream_name, created_ns, last_seq_seen) VALUES (1, 'traffic', 's', 0, 0)`)
		setState(t, c, other, "terminated")
		v := &other.Version
		lapse(t, c, `INSERT INTO parked_copies (incarnation_id, stream_seq, capture_id, seq, header, payload_digest, state)
			VALUES (1, 10, X'`+hexOf(capture)+`', 12, X'`+hexBytes(v.Header[:])+`', X'`+hexBytes(v.Digest[:])+`', 'waiting')`)
		if ok, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/12"); err != nil || ok {
			t.Fatalf("with a current copy waiting: MarkSurvivorDeletable = %v, %v, want kept", ok, err)
		}
		lapse(t, c, `UPDATE parked_copies SET state = 'acked'`)
		if ok, err := c.MarkSurvivorDeletable(t.Context(), l, "claims/12"); err != nil || !ok {
			t.Fatalf("once both versions are satisfied: MarkSurvivorDeletable = %v, %v", ok, err)
		}
	})
}

// A lease taken over while a deletion or an eviction holds the capture row, before its lease check: nothing is written.
func TestLeaseTakeoverBeforeADeletionOrAnEviction(t *testing.T) {
	t.Parallel()
	for _, evicting := range []bool{false, true} {
		engines(t, func(t *testing.T, c *Catalog) {
			if c.d.name == EngineSQLite {
				t.Skip("SQLite serializes whole transactions")
			}
			l := compactorLease(t, c, "old")
			capture, inputs, archive := mergedScope(t, c, l, 0)
			recordRetention(t, c, l, month, 0)
			held, release := holdAt(c, hookLocked)
			var err error
			done := run(func() {
				if evicting {
					_, err = c.EvictHour(t.Context(), l, Scope{capture, 1}, hourEvictedNow)

					return
				}
				_, err = c.DeletePack(t.Context(), l, Deletable{Pack: inputs[0].ID, Capture: capture, Key: inputs[0].Key}, deleteAt(t, c, archive), time.Hour)
			})
			<-held
			lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
			compactorLease(t, c, "new")
			close(release)
			<-done
			if !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("evicting %v: %v, want ErrLeaseLost", evicting, err)
			}
			if count(t, c, `SELECT COUNT(*) FROM tombstones`) != 0 || packState(t, c, inputs[0].ID) != "merged" ||
				count(t, c, `SELECT COUNT(*) FROM evicted_ranges`) != 0 {
				t.Error("a refused transaction wrote")
			}
		})
	}
}
