package catalog

import (
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// waiting is a catalog holding an open capture of T1 with a pack, one deferred version and one resolved,
// and a pending capture with two deferred versions.
func waiting(t *testing.T, c *Catalog) (open, pending tracepack.UUID) {
	t.Helper()
	ctx := t.Context()
	open = admitted(t, c)
	register(t, c, segment(t, open, 1, 0, 9), Registration{Evidence: &Evidence{}})
	pending = newID(t)
	begin(t, c, identity(pending, 1, 1), 0)
	begin(t, c, identity(pending, 2, 2), 0)
	if _, err := c.BeginParking(ctx, identity(open, 12, 3), "awaiting-config", "", 0); err != nil {
		t.Fatal(err)
	}
	park(t, c, identity(open, 13, 4), "staged") // resolved: no longer deferred

	return open, pending
}

// Status lists the pending captures by id outside any tool, then each tool's last record, open captures and deferred versions.
func TestStatus(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		_, pending := waiting(t, c)
		st, err := c.Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Pending) != 1 || st.Pending[0].Capture != pending || st.Pending[0].SinceNS == 0 {
			t.Errorf("pending = %+v, want the parked unknown capture", st.Pending)
		}
		if len(st.Tools) != 1 || st.Tools[0].ToolID != "T1" || st.Tools[0].OpenCaptures != 1 || st.Tools[0].Deferred != 1 ||
			st.Tools[0].LastRecordNS == nil || *st.Tools[0].LastRecordNS != 9 {
			t.Errorf("tools = %+v, want T1 with one open capture, one deferred version and its last record at 9", st.Tools)
		}
	})
}

// Deferred groups the unresolved parked versions by capture and reason, with the oldest's parking time.
func TestDeferred(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		open, pending := waiting(t, c)
		groups, err := c.Deferred(ctx, nil)
		if err != nil || len(groups) != 2 {
			t.Fatalf("Deferred = %+v, %v; want a group per capture and reason", groups, err)
		}
		for _, g := range groups {
			switch g.Capture {
			case pending:
				if g.Reason != "unknown-capture" || g.Count != 2 || g.ToolID != "" || g.OldestNS == 0 {
					t.Errorf("the pending capture's group = %+v", g)
				}
			case open:
				if g.Reason != "awaiting-config" || g.Count != 1 || g.ToolID != "T1" {
					t.Errorf("the open capture's group = %+v", g)
				}
			default:
				t.Errorf("a group of another capture: %+v", g)
			}
		}
		if only, err := c.Deferred(ctx, &open); err != nil || len(only) != 1 || only[0].Capture != open {
			t.Errorf("Deferred of one capture = %+v, %v", only, err)
		}
	})
}
