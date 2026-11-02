package catalog

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// A standalone audit write waits for its connection at most a second within its caller's deadline,
// fails with ErrAudit when the wait ends, and writes no row later;
// each attempt records one wait with its outcome: acquired, deadline, canceled or error.
func TestAuditWithin(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		var outcomes []string
		c.OnAuditWait(func(outcome string, wait time.Duration) {
			if wait < 0 {
				t.Errorf("a %s wait of %s", outcome, wait)
			}
			outcomes = append(outcomes, outcome)
		})
		e := AuditEntry{Actor: "oidc:audit-within", Action: "pack-url"}
		rows := func() int {
			return count(t, c, `SELECT COUNT(*) FROM audit_log WHERE actor = ?`, e.Actor)
		}
		if err := c.AuditWithin(ctx, e); err != nil {
			t.Fatalf("AuditWithin = %v", err)
		}
		want := []string{"acquired"}
		if c.Engine() == EngineMariaDB {
			// The insert, not the connection, waits on a table another session locks: acquired, then expired.
			// A statement already waiting on a lock when its client gives up may still run once the lock is released,
			// so its row is its own actor's, never counted below: a row for an answer not given, never an answer without its row.
			locker, err := c.write.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := locker.ExecContext(ctx, `LOCK TABLES audit_log WRITE`); err != nil {
				t.Fatal(err)
			}
			if err := c.AuditWithin(ctx, AuditEntry{Actor: "oidc:audit-locked", Action: "pack-url"}); !errors.Is(err, ErrAudit) {
				t.Errorf("AuditWithin on a locked table = %v, want ErrAudit", err)
			}
			if _, err := locker.ExecContext(ctx, `UNLOCK TABLES`); err != nil {
				t.Fatal(err)
			}
			_ = locker.Close()
			want = append(want, "acquired")
		}
		// SQLite's writer is one connection; MariaDB's pool is made one, so holding it holds the writer.
		c.write.SetMaxOpenConns(1)
		held, err := c.write.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := c.AuditWithin(ctx, e); !errors.Is(err, ErrAudit) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("AuditWithin on a held writer = %v, want ErrAudit past its deadline", err)
		}
		if waited := time.Since(start); waited < auditDeadline {
			t.Errorf("AuditWithin on a held writer gave up after %s, before its deadline of %s", waited, auditDeadline)
		}
		parent, cancel := context.WithTimeout(ctx, auditDeadline/20)
		defer cancel()
		if err := c.AuditWithin(parent, e); !errors.Is(err, ErrAudit) || !errors.Is(err, context.DeadlineExceeded) || parent.Err() == nil {
			t.Errorf("AuditWithin past an earlier parent deadline = %v (parent %v), want ErrAudit at the parent's deadline", err, parent.Err())
		}
		gone, cancelGone := context.WithCancel(ctx)
		cancelGone()
		if err := c.AuditWithin(gone, e); !errors.Is(err, ErrAudit) || !errors.Is(err, context.Canceled) {
			t.Errorf("AuditWithin of a cancelled caller = %v, want ErrAudit, cancelled", err)
		}
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}
		if n := rows(); n != 1 {
			t.Errorf("%d audit rows once the writer is released, want only the first", n)
		}
		if err := c.write.Close(); err != nil {
			t.Fatal(err)
		}
		if err := c.AuditWithin(ctx, e); !errors.Is(err, ErrAudit) {
			t.Errorf("AuditWithin on a closed catalog = %v, want ErrAudit", err)
		}
		want = append(want, "deadline", "deadline", "canceled", "error")
		if !slices.Equal(outcomes, want) {
			t.Errorf("the waits' outcomes = %v, want %v", outcomes, want)
		}
	})
}
