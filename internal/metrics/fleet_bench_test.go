package metrics

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// fleetHours is how many hours of segments each capture of the fleet holds: a day, the open capture's usual reach.
const fleetHours = 24

// BenchmarkReadCatalog is F8: the catalog part of ingest's metrics refresh at fleet size, every open capture's completeness read once
// (phase 2.7's plan).
// Its starting state is a fleet of one open capture a tool, each with fleetHours hours of segments; an iteration reads it whole,
// writes nothing, and asserts every tool's capture read.
// Its sizes are 10, 150 and 1,500 tools (TRACEKEEP_BENCH_SCALE), or TRACEKEEP_FLEET tools alone;
// MariaDB runs when TRACEKEEP_MARIADB_DSN names a server.
func BenchmarkReadCatalog(b *testing.B) {
	sizes := benchscale.Sizes(b, 10, 150, 1500)
	if v, err := strconv.Atoi(os.Getenv("TRACEKEEP_FLEET")); err == nil && v > 0 {
		sizes = []int{v}
	}
	for _, n := range sizes {
		b.Run(fmt.Sprintf("tools=%d", n), func(b *testing.B) {
			catalogtest.BenchEngines(b, func(b *testing.B, c *catalog.Catalog) {
				catalogtest.SeedFleet(b, c, n, 1, fleetHours)
				b.ReportAllocs()
				for b.Loop() {
					got, err := readCatalog(b.Context(), c)
					if err != nil || len(got.captures) != n {
						b.Fatalf("readCatalog = %d tools, %v", len(got.captures), err)
					}
				}
			})
		})
	}
}
