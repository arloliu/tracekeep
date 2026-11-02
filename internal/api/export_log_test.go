package api

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// errDSNFault is a catalog error naming a DSN with a password and a query, as a driver's text might.
var errDSNFault = errors.New("catalog: dial mysql://u:pw-sentinel@db.invalid/tk?token=q-sentinel: connection refused")

// faultOnce is a catalog whose first claim and first intent fail with errDSNFault.
type faultOnce struct {
	Catalog
	claims, intents atomic.Int32
}

func (f *faultOnce) ClaimExport(ctx context.Context, holder string, lease time.Duration, maxAttempts int) (catalog.Claim, bool, error) {
	if f.claims.Add(1) == 1 {
		return catalog.Claim{}, false, errDSNFault
	}

	return f.Catalog.ClaimExport(ctx, holder, lease, maxAttempts)
}

func (f *faultOnce) IntendExport(ctx context.Context, a catalog.Attempt, key string, putTimeout time.Duration) (int64, error) {
	if f.intents.Add(1) == 1 {
		return 0, errDSNFault
	}

	return f.Catalog.IntendExport(ctx, a, key, putTimeout)
}

// A catalog fault at a claim and inside an admitted attempt is logged by the export workers with its URL cut to scheme and host;
// the workers claim again, and the job completes at the next fence once the failed attempt's lease lapses.
func TestExportWorkerLogsNoCredential(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, captured(recordsOf(carriers(t)[:1], hour)))
		m.put(t, capture, hour, ms, entries, m.policy())
		f := &faultOnce{Catalog: c}
		m.srv.Catalog = f
		workers(t, m)
		job := startExport(t, c, capture)
		j := await(t, c, job)
		if j.State != catalog.ExportDone || j.Fence != 2 {
			t.Fatalf("job = %+v, want done by the second attempt", j)
		}
		logs := m.logs.String()
		if strings.Contains(logs, "sentinel") {
			t.Errorf("the log holds a credential or a query:\n%s", logs)
		}
		if !strings.Contains(logs, `msg="api: exports: claim" error="catalog: dial mysql://db.invalid: connection refused"`) ||
			!strings.Contains(logs, "api: exports: attempt ended") || strings.Count(logs, "mysql://db.invalid") != 2 {
			t.Errorf("the log = %s\nwant the claim's and the attempt's faults, each with its URL's scheme and host", logs)
		}
	})
}
