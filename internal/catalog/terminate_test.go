package catalog

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// identity is a delivery of capture's seq on incarnation 1 at streamSeq, its record in hour 5.
func identity(capture tracepack.UUID, seq uint64, streamSeq int64) *Identity {
	return &Identity{
		Version:     VersionKey{Capture: capture, Seq: seq, Header: [44]byte{byte(seq)}, Digest: [32]byte{1}},
		Incarnation: 1, StreamSeq: streamSeq, Hour: 5,
	}
}

// start is a delivery of capture's seq 0, at stream seq 1.
func start(capture tracepack.UUID) *Identity { return identity(capture, 0, 1) }

// order writes the active termination order action of capture on incarnation 1, cut off at stream seq 10, as capture terminate would.
func order(t *testing.T, c *Catalog, capture tracepack.UUID, action string) {
	t.Helper()
	_, err := c.write.ExecContext(t.Context(), `INSERT INTO termination_orders (action_id, capture_id, incarnation_id, cut_off, state)
		VALUES (?, ?, 1, 10, 'active')`, action, capture[:])
	if err != nil {
		t.Fatal(err)
	}
}

// park writes the version of d in state, as parking would.
func park(t *testing.T, c *Catalog, d *Identity, state string) {
	t.Helper()
	v := &d.Version
	_, err := c.write.ExecContext(t.Context(), `INSERT INTO parked (capture_id, seq, header, payload_digest, state, reason, hour, parked_at)
		VALUES (?, ?, ?, ?, ?, 'x', ?, 0)`, v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:], state, d.Hour)
	if err != nil {
		t.Fatal(err)
	}
}

// versionState is the state of d's version and the action that terminated it.
func versionState(t *testing.T, c *Catalog, d *Identity) (state, by string) {
	t.Helper()
	v := &d.Version
	var row struct {
		State string  `db:"state"`
		By    *string `db:"terminated_by"`
	}
	err := c.read.GetContext(t.Context(), &row, `SELECT state, terminated_by FROM parked WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ?`,
		v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
	if err != nil {
		t.Fatal(err)
	}
	if row.By != nil {
		by = *row.By
	}

	return row.State, by
}

// An admission or a rejection re-checks the delivery's resolution and the capture's orders under the row (T-32):
// a terminated version, or one an order covers, returns Terminated and makes no admission effect.
func TestAdmissionRechecksTermination(t *testing.T) {
	t.Parallel()
	ops := []struct {
		name   string
		normal Outcome
		run    func(c *Catalog, d *Identity) (Result, error)
	}{
		{"admit", Admitted, func(c *Catalog, d *Identity) (Result, error) { return c.Admit(t.Context(), d, descriptor(t, "d")) }},
		{"reject", Rejected, func(c *Catalog, d *Identity) (Result, error) { return c.Reject(t.Context(), d, []byte("d")) }},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				gate(t, c)

				// A terminated version: nothing at all is written, the prologue's row included.
				dead := identity(newID(t), 0, 1)
				park(t, c, dead, "terminated")
				if res, err := op.run(c, dead); err != nil || res.Outcome != Terminated {
					t.Fatalf("on a terminated version = %+v, %v; want terminated", res, err)
				}
				if _, err := c.Capture(t.Context(), dead.Version.Capture); !errors.Is(err, ErrUnknownCapture) {
					t.Errorf("a terminated version left a capture row: %v", err)
				}

				// A version an order covers: the order is applied, and the capture is neither admitted nor rejected.
				covered := identity(newID(t), 0, 10)
				order(t, c, covered.Version.Capture, "a1")
				if res, err := op.run(c, covered); err != nil || res.Outcome != Terminated {
					t.Fatalf("on a covered version = %+v, %v; want terminated", res, err)
				}
				if state, by := versionState(t, c, covered); state != "terminated" || by != "a1" {
					t.Errorf("covered version = %s by %q, want terminated by a1", state, by)
				}
				if got, err := c.Capture(t.Context(), covered.Version.Capture); err != nil || got.State != StatePending {
					t.Errorf("covered capture = %+v, %v; want pending", got, err)
				}

				// Outside the order, on another incarnation, or of a staged version: the normal outcome.
				for i, d := range []*Identity{identity(newID(t), 0, 11), {Version: VersionKey{Capture: newID(t)}, Incarnation: 2, StreamSeq: 5}} {
					order(t, c, d.Version.Capture, fmt.Sprintf("o%d", i))
					if res, err := op.run(c, d); err != nil || res.Outcome != op.normal {
						t.Errorf("at stream seq %d on incarnation %d = %+v, %v; want %v", d.StreamSeq, d.Incarnation, res, err, op.normal)
					}
				}
				staged := identity(newID(t), 0, 1)
				park(t, c, staged, "staged")
				if res, err := op.run(c, staged); err != nil || res.Outcome != op.normal {
					t.Errorf("on a staged version = %+v, %v; want %v", res, err, op.normal)
				}
			})
		})
	}
}

// An order's application terminates the unresolved version under the order's action, with its copy row, its audit row
// and a terminated event on the record's scope (T-33); it never undoes a resolution.
func TestApplyOrder(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		order(t, c, capture, "a1")

		d := identity(capture, 3, 10)
		if r, err := c.ApplyOrder(ctx, d); err != nil || r != ResolvedTerminated {
			t.Fatalf("ApplyOrder = %v, %v; want terminated", r, err)
		}
		if state, by := versionState(t, c, d); state != "terminated" || by != "a1" {
			t.Errorf("version = %s by %q, want terminated by a1", state, by)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked_copies WHERE incarnation_id = 1 AND stream_seq = 10 AND seq = 3 AND state = 'waiting'`); n != 1 {
			t.Errorf("%d copy rows of the delivery, want 1 waiting", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM audit_log WHERE action_id = 'a1' AND capture_id = ?`, capture[:]); n != 1 {
			t.Errorf("%d audit rows of the order, want 1", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM scopes WHERE capture_id = ? AND hour = 5 AND revision = 1`, capture[:]); n != 1 {
			t.Errorf("the terminated hour's scope was not created at revision 1")
		}
		// Applied again: already resolved, nothing more written.
		if r, err := c.ApplyOrder(ctx, d); err != nil || r != ResolvedTerminated {
			t.Errorf("ApplyOrder again = %v, %v; want terminated", r, err)
		}

		// A parked version moves to terminated by compare-and-set; the scope's revision advances.
		parked := identity(capture, 4, 8)
		park(t, c, parked, "parked")
		if r, err := c.ApplyOrder(ctx, parked); err != nil || r != ResolvedTerminated {
			t.Fatalf("ApplyOrder of a parked version = %v, %v", r, err)
		}
		if state, by := versionState(t, c, parked); state != "terminated" || by != "a1" {
			t.Errorf("parked version = %s by %q, want terminated by a1", state, by)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM scopes WHERE capture_id = ? AND hour = 5 AND revision = 2`, capture[:]); n != 1 {
			t.Errorf("the scope's revision did not advance")
		}

		// A staged version stays staged; a message above the cut-off is not covered.
		staged := identity(capture, 5, 9)
		park(t, c, staged, "staged")
		if r, err := c.ApplyOrder(ctx, staged); err != nil || r != ResolvedStaged {
			t.Errorf("ApplyOrder of a staged version = %v, %v; want staged", r, err)
		}
		if r, err := c.ApplyOrder(ctx, identity(capture, 6, 11)); err != nil || r != Unresolved {
			t.Errorf("ApplyOrder above the cut-off = %v, %v; want unresolved", r, err)
		}

		if got := events(t, c); !slices.Equal(got, []string{"terminated@5", "terminated@5"}) {
			t.Errorf("events = %v, want a terminated event per terminating application in hour 5", got)
		}
	})
}

// An order's application in an hour retention evicted gets no scope and no event: its seqs read from evicted_ranges and parked.
func TestApplyOrderInAnEvictedHour(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		order(t, c, capture, "a1")
		evicted := identity(capture, 7, 7)
		evicted.Hour = 2
		if _, err := c.write.ExecContext(ctx, `INSERT INTO evicted_ranges (capture_id, hour, seq_from, seq_to, ts_from, ts_to) VALUES (?, 2, 0, 9, 0, 9)`, capture[:]); err != nil {
			t.Fatal(err)
		}
		if r, err := c.ApplyOrder(ctx, evicted); err != nil || r != ResolvedTerminated {
			t.Fatalf("ApplyOrder in an evicted hour = %v, %v", r, err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM scopes WHERE capture_id = ? AND hour = 2`, capture[:]); n != 0 {
			t.Errorf("an evicted hour's scope was recreated")
		}
		if got := events(t, c); len(got) != 0 {
			t.Errorf("events = %v, want none", got)
		}
	})
}

// A registration with evidence aborts and tombstones its key when a version of the batch is terminated
// or a message of it is covered by an order (T-32), and stages the batch's parked versions.
func TestRegisterPackRechecksTermination(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		order(t, c, capture, "a1")
		for _, tc := range []struct {
			name string
			d    *Identity
		}{{"terminated", identity(capture, 3, 30)}, {"covered", identity(capture, 4, 10)}} {
			if tc.name == "terminated" {
				park(t, c, tc.d, "terminated")
			}
			p := segment(t, capture, 5, 0, 9)
			ev := &Evidence{Deliveries: []Identity{*identity(capture, 2, 20), *tc.d}}
			if out := register(t, c, p, Registration{Evidence: ev}); out != VersionTerminated {
				t.Errorf("%s: RegisterPack = %v, want version terminated", tc.name, out)
			}
			if n := count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ? AND reason = 'terminated'`, p.Key); n != 1 {
				t.Errorf("%s: the key is not tombstoned", tc.name)
			}
		}
		if n := count(t, c, `SELECT COUNT(*) FROM packs`); n != 0 {
			t.Errorf("%d packs registered", n)
		}

		// The batch's parking and parked versions become staged; an evicted-hour one is never staged.
		parking, parked := identity(capture, 5, 50), identity(capture, 6, 60)
		park(t, c, parking, "parking")
		park(t, c, parked, "parked")
		ev := &Evidence{Deliveries: []Identity{*parking, *parked, *identity(capture, 7, 70)}}
		if out := register(t, c, segment(t, capture, 5, 0, 9), Registration{Evidence: ev}); out != Registered {
			t.Fatalf("RegisterPack = %v, want registered", out)
		}
		for _, d := range []*Identity{parking, parked} {
			if state, _ := versionState(t, c, d); state != "staged" {
				t.Errorf("seq %d = %s, want staged", d.Version.Seq, state)
			}
		}
		if n := count(t, c, `SELECT COUNT(*) FROM parked WHERE state = 'staged' AND resolved_at IS NULL`); n != 0 {
			t.Errorf("%d staged versions without a resolution time", n)
		}
	})
}
