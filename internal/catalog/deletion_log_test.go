package catalog_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/store"
)

// errDSNFault is a catalog failure naming a DSN with a password and a query, as a driver's text might.
var errDSNFault = errors.New("dial mysql://u:pw-sentinel@db.invalid/tk?token=q-sentinel: connection refused")

// recorded is a slog handler keeping each record as its fields, the message under "msg".
type recorded struct {
	mu   sync.Mutex
	recs []map[string]string
}

func (h *recorded) Enabled(context.Context, slog.Level) bool { return true }

func (h *recorded) Handle(_ context.Context, r slog.Record) error {
	rec := map[string]string{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool {
		rec[a.Key] = a.Value.Resolve().String()
		return true
	})
	h.mu.Lock()
	h.recs = append(h.recs, rec)
	h.mu.Unlock()

	return nil
}

func (h *recorded) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recorded) WithGroup(string) slog.Handler { return h }

// with is the records of message msg.
func (h *recorded) with(msg string) []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.DeleteFunc(slices.Clone(h.recs), func(r map[string]string) bool { return r["msg"] != msg })
}

// clean fails t for each field of each record holding one of markers, each marker checked apart.
func (h *recorded) clean(t *testing.T, markers ...string) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		for name, v := range r {
			for _, m := range markers {
				if strings.Contains(v, m) {
					t.Errorf("log %q: its %s holds %q: %q", r["msg"], name, m, v)
				}
			}
		}
	}
}

// The deletion pass logs a refused deletion and a catalog's failure by the input's pack and capture, never its key,
// the catalog's text with its URL cut to scheme and host.
func TestDeletionLogsNameNoKey(t *testing.T) {
	t.Parallel()
	for _, refused := range []bool{true, false} {
		name := map[bool]string{true: "refused", false: "catalog fails"}[refused]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				ctx := t.Context()
				l, ok, err := c.TakeLease(ctx, "compactor/2", "w", time.Hour)
				if err != nil || !ok {
					t.Fatalf("TakeLease = %v, %v", ok, err)
				}
				inputs, archive := catalog.MergedInputs(t, c, l, time.Hour)
				msg := "compact: a merged input's deletion refused"
				if refused {
					// The archive excluded between the listing and the deletion: the retry's guard refuses.
					catalog.FailFirstCommit(c, errors.New("commit failed retryably"))
					c.OnRetry(func(string) {
						if _, err := c.ExcludeInput(ctx, archive.ID, "operator", "act-1"); err != nil {
							t.Error(err)
						}
					})
				} else {
					catalog.FailCommits(c, errDSNFault)
					msg = "compact: delete a merged input"
				}
				h := &recorded{}
				r := &compact.Compactor{Catalog: c, Store: store.NewMem(), Log: slog.New(h), DeleteDelay: time.Hour,
					Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
				var rep compact.RetentionReport
				if err := r.Deletions(ctx, l, &rep); err != nil {
					t.Fatal(err)
				}
				recs := h.with(msg)
				if len(recs) != 2 || (refused && rep.Refused != 2) || (!refused && rep.Failed != 2) {
					t.Fatalf("%d records %q, report %+v; want two", len(recs), msg, rep)
				}
				for _, rec := range recs {
					if (rec["pack"] != inputs[0].ID.String() && rec["pack"] != inputs[1].ID.String()) || rec["capture"] != inputs[0].Capture.String() {
						t.Errorf("%q logged %v, want an input's pack and capture", msg, rec)
					}
					if !refused && !strings.Contains(rec["error"], "mysql://db.invalid: connection refused") {
						t.Errorf("%q logged %v, want the catalog's cause with its URL's scheme and host", msg, rec)
					}
				}
				h.clean(t, inputs[0].Key, inputs[1].Key, "pw-sentinel", "q-sentinel")
			})
		})
	}
}
