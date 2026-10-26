package metrics

import (
	"net/http"
	"strings"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/migrations"
)

// The catalog's metrics: both pools' usage, the retries counter and the schema version; its readiness pings it.
func TestCatalogMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := NewServer()
		if err := RegisterCatalog(s, c); err != nil {
			t.Fatal(err)
		}
		version, err := migrations.Latest(c.Engine())
		if err != nil {
			t.Fatal(err)
		}
		_, body := get(t, s.Handler(), "/metrics")
		for _, want := range []string{
			`go_sql_open_connections{db_name="catalog_write"}`, `go_sql_open_connections{db_name="catalog_read"}`,
			`tracekeep_catalog_migration_info{engine="` + c.Engine() + `",version="` + version + `"} 1`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("/metrics lacks %s", want)
			}
		}
		if code, _ := get(t, s.Handler(), "/readyz"); code != http.StatusOK {
			t.Errorf("/readyz = %d with the catalog answering, want 200", code)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if code, body := get(t, s.Handler(), "/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "catalog") {
			t.Errorf("/readyz = %d %q with the catalog closed, want 503 naming it", code, body)
		}
	})
}
