package metrics

import (
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// fleetHours is how many hours of segments each capture of the fleet holds: a day, the open capture's usual reach.
const fleetHours = 24

// seedFleet admits n tools' open captures, each with a segment in each of fleetHours hours, as ingest would register them.
func seedFleet(b *testing.B, c *catalog.Catalog, n int) {
	b.Helper()
	ctx := b.Context()
	cfg, err := c.ActivateConfig(ctx, []byte("config"))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := c.Register(ctx, "bench", []int64{cfg}, time.Hour); err != nil {
		b.Fatal(err)
	}
	baseNs := time.Now().Add(-fleetHours * time.Hour).Truncate(time.Hour).UnixNano()
	for i := range n {
		var id, instance tracepack.UUID
		_, _ = rand.Read(id[:])
		_, _ = rand.Read(instance[:])
		tool := fmt.Sprintf("TOOL-%04d", i)
		d := &catalog.Identity{Version: catalog.VersionKey{Capture: id}, Incarnation: 1, StreamSeq: int64(i) + 1, Hour: baseNs / int64(time.Hour)}
		desc := &catalog.Descriptor{Bytes: []byte("d"), ToolID: tool, RecorderInstanceID: instance, Device: "eqp", OriginNS: baseNs}
		if _, err := c.Admit(ctx, d, desc); err != nil {
			b.Fatal(err)
		}
		for h := range fleetHours {
			var pack tracepack.UUID
			_, _ = rand.Read(pack[:])
			from := uint64(h) * 1000 //nolint:gosec // a small hour index
			at := baseNs + int64(h)*int64(time.Hour)
			p := &catalog.Pack{
				ID: pack, Capture: id, ToolID: tool, Hour: at / int64(time.Hour), Role: catalog.RoleSegment, Key: "staging/" + pack.String(),
				Size: 1 << 20, SeqStart: from, FormatMinor: 2,
				Ranges: []catalog.Range{{SeqFrom: from, SeqTo: from + 999, TSFrom: at, TSTo: at + int64(time.Hour) - 1}},
			}
			if _, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: &catalog.Evidence{}}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkReadCatalog is F8: the catalog part of ingest's metrics refresh at fleet size, every open capture's completeness read once
// (phase 2.7's plan). TRACEKEEP_FLEET sets the number of tools, 1,500 by default; MariaDB runs when TRACEKEEP_MARIADB_DSN names a server.
func BenchmarkReadCatalog(b *testing.B) {
	n := 1500
	if v, err := strconv.Atoi(os.Getenv("TRACEKEEP_FLEET")); err == nil && v > 0 {
		n = v
	}
	for _, engine := range []string{catalog.EngineSQLite, catalog.EngineMariaDB} {
		b.Run(engine, func(b *testing.B) {
			if engine == catalog.EngineMariaDB && os.Getenv(catalogtest.EnvMariaDB) == "" {
				b.Skip("no MariaDB")
			}
			c := catalogtest.Open(b, engine)
			seedFleet(b, c, n)
			b.ResetTimer()
			for b.Loop() {
				got, err := readCatalog(b.Context(), c)
				if err != nil || len(got.captures) != n {
					b.Fatalf("readCatalog = %d tools, %v", len(got.captures), err)
				}
			}
		})
	}
}
