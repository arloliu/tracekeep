package api

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// waits collects the outcomes of c's audit waits, each sent where it is recorded.
func waits(c *catalog.Catalog) <-chan string {
	ch := make(chan string, 8)
	c.OnAuditWait(func(outcome string, _ time.Duration) { ch <- outcome })

	return ch
}

// outcomes drains what waits sent so far, once the requests that recorded them answered.
func outcomes(ch <-chan string) []string {
	var out []string
	for {
		select {
		case o := <-ch:
			out = append(out, o)
		default:
			return out
		}
	}
}

// A URL whose audit row finds the writer held is not issued:
// its read and signature done, the audit gives up at its deadline, the request answers internal,
// and no row is written once the writer is released; the wait is recorded once, as past its deadline.
func TestPackURLAuditOnAHeldWriter(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		w := newWorld(t, c)
		now := deadlineOf().Add(-24 * time.Hour)
		w.urls(t, &now, true)
		p := w.stage(t, w.admit(t))
		ch := waits(c)
		write, _ := c.Pools()
		held := make(chan *sql.Conn, 1)
		w.srv.stall = func(stage string) {
			if stage != "signed" {
				return
			}
			// SQLite's writer is one connection, and MariaDB's pool is made one, so holding it holds the writer.
			write.SetMaxOpenConns(1)
			conn, err := write.Conn(context.Background())
			if err != nil {
				t.Error(err)
			}
			held <- conn
		}
		start := time.Now()
		got, err := packURL(t, w, p.ID.String())
		if connect.CodeOf(err) != connect.CodeInternal || errMessage(err) != msgAudit || got != nil {
			t.Errorf("PackURL with the writer held = %v, %v; want internal with the audit's message and no URL", got, err)
		}
		if waited := time.Since(start); waited < time.Second {
			t.Errorf("PackURL answered after %s, before its audit's deadline", waited)
		}
		if conn := <-held; conn != nil {
			_ = conn.Close()
		}
		if audit, err := c.AuditEntries(t.Context(), p.Capture); err != nil || len(audit) != 0 {
			t.Errorf("audit once the writer is released = %+v, %v; want no row", audit, err)
		}
		if got := outcomes(ch); len(got) != 1 || got[0] != "deadline" {
			t.Errorf("the audit's waits = %v, want one past its deadline", got)
		}
	})
}

// An operator read through AdminService runs, then its audit row, finding the writer held, gives up at its deadline:
// the answer is internal with the audit's message and carries no result.
// On SQLite the writer's one connection is held; on MariaDB, whose reads share the write pool, a session locks the audit table,
// so the connection is acquired and the insert expires.
func TestAdminReadAuditOnAHeldWriter(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ch := waits(c)
		s := &Server{Catalog: c, Auth: testAuthenticator(nil), Admin: &admin.Admin{Catalog: c}}
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		cl := tracekeepv1connect.NewAdminServiceClient(srv.Client(), srv.URL)
		write, _ := c.Pools()
		held, err := write.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := "deadline"
		if c.Engine() == catalog.EngineMariaDB {
			if _, err := held.ExecContext(t.Context(), `LOCK TABLES audit_log WRITE`); err != nil {
				t.Fatal(err)
			}
			want = "acquired"
		}
		start := time.Now()
		got, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: []string{"parked", "list"}}, adminToken))
		if connect.CodeOf(err) != connect.CodeInternal || errMessage(err) != msgAudit || got != nil {
			t.Errorf("parked list with the writer held = %v, %v; want internal with the audit's message and no result", got, err)
		}
		if waited := time.Since(start); waited < time.Second {
			t.Errorf("parked list answered after %s, before its audit's deadline", waited)
		}
		if c.Engine() == catalog.EngineMariaDB {
			if _, err := held.ExecContext(t.Context(), `UNLOCK TABLES`); err != nil {
				t.Fatal(err)
			}
		}
		_ = held.Close()
		// On MariaDB the insert that waited on the lock may still land once it is released (catalog's TestAuditWithin), so only SQLite's rows are counted.
		if rows, err := c.AuditByActor(t.Context(), "operator"); c.Engine() == catalog.EngineSQLite && (err != nil || len(rows) != 0) {
			t.Errorf("audit once the writer is released = %+v, %v; want no row", rows, err)
		}
		if got := outcomes(ch); len(got) != 1 || got[0] != want {
			t.Errorf("the audit's waits = %v, want one %s", got, want)
		}
	})
}
