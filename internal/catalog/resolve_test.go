package catalog

import (
	"testing"
)

func TestResolve(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		v := &VersionKey{Capture: newID(t), Seq: 7, Header: [44]byte{1}, Digest: [32]byte{2}}
		if r, err := c.Resolve(ctx, v, 1, 100); err != nil || r != Unresolved {
			t.Fatalf("Resolve with no rows = %v, %v; want unresolved", r, err)
		}
		park := func(state string) {
			_, err := c.write.ExecContext(ctx, `INSERT INTO parked (capture_id, seq, header, payload_digest, state, reason, hour, parked_at)
				VALUES (?, ?, ?, ?, ?, 'x', 0, 0)`, v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:], state)
			if err != nil {
				t.Fatal(err)
			}
		}
		// An order of the capture covers a message at or below its cut-off on its incarnation only.
		if _, err := c.write.ExecContext(ctx, `INSERT INTO termination_orders (action_id, capture_id, incarnation_id, cut_off, state) VALUES ('a1', ?, 1, 100, 'active')`, v.Capture[:]); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			inc, seq int64
			want     Resolution
		}{{1, 100, CoveredByOrder}, {1, 101, Unresolved}, {2, 50, Unresolved}} {
			if r, err := c.Resolve(ctx, v, tc.inc, tc.seq); err != nil || r != tc.want {
				t.Errorf("Resolve on incarnation %d at %d = %v, %v; want %v", tc.inc, tc.seq, r, err, tc.want)
			}
		}
		park("staged")
		if r, err := c.Resolve(ctx, v, 1, 100); err != nil || r != ResolvedStaged {
			t.Errorf("Resolve of a staged version = %v, %v; want staged, which an order never undoes", r, err)
		}
	})
}

func TestReceipt(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		v := &VersionKey{Capture: newID(t), Seq: 3, Header: [44]byte{1}, Digest: [32]byte{2}}
		if m, err := c.Receipt(ctx, v); err != nil || m != ReceiptNone {
			t.Fatalf("Receipt with none = %v, %v", m, err)
		}
		other := *v
		other.Digest = [32]byte{9}
		insert := func(k *VersionKey) {
			_, err := c.write.ExecContext(ctx, `INSERT INTO claim_records (capture_id, seq, header, payload_sha256, hour, first_pack_id) VALUES (?, ?, ?, ?, 0, ?)`,
				k.Capture[:], int64(k.Seq), k.Header[:], k.Digest[:], k.Capture[:])
			if err != nil {
				t.Fatal(err)
			}
		}
		insert(&other)
		if m, err := c.Receipt(ctx, v); err != nil || m != ReceiptOther {
			t.Errorf("Receipt with another version's = %v, %v; want other", m, err)
		}
		insert(v)
		if m, err := c.Receipt(ctx, v); err != nil || m != ReceiptEqual {
			t.Errorf("Receipt with this version's = %v, %v; want equal", m, err)
		}
	})
}

func TestAudit(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		id := newID(t)
		if err := c.Audit(t.Context(), AuditEntry{Actor: "ingest", Action: "beyond-retention", Capture: &id, Detail: "seq 4"}); err != nil {
			t.Fatal(err)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM audit_log WHERE action = 'beyond-retention' AND capture_id = ?`, id[:]); n != 1 {
			t.Errorf("%d audit rows, want one", n)
		}
	})
}
