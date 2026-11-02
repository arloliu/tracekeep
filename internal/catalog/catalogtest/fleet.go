package catalogtest

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/classify"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/procconfig"
)

// FleetOrigin is the start of SeedFleet's first hour, 2026-01-01T00:00:00Z in Unix nanoseconds, fixed so a benchmark can hold a window constant.
const FleetOrigin = 1767225600 * int64(time.Second)

// FleetSeqs is the seqs SeedFleet registers a capture an hour, in one segment of one range.
const FleetSeqs = 1000

// FleetTool is the tool id of SeedFleet's i-th tool.
func FleetTool(i int) string { return fmt.Sprintf("TOOL-%04d", i) }

// BenchEngines runs f once per engine in a sub-benchmark named for it, each with a fresh catalog.
// MariaDB is skipped without TRACEKEEP_MARIADB_DSN, and fails instead when TRACEKEEP_REQUIRE_MARIADB is 1;
// an engine TRACEKEEP_BENCH_ENGINES leaves out runs no sub-benchmark.
func BenchEngines(b *testing.B, f func(b *testing.B, c *catalog.Catalog)) {
	b.Helper()
	for _, engine := range benchscale.Engines(b, catalog.EngineSQLite, catalog.EngineMariaDB) {
		b.Run(engine, func(b *testing.B) { f(b, Open(b, engine)) })
	}
}

// SeedFleet admits capturesPerTool open captures of each of tools tools (FleetTool), as ingest would:
// each with a decodable start descriptor of its own recorder instance on device eqp, bound to a decodable configuration,
// and a segment registered in each of hours hours from FleetOrigin, FleetSeqs seqs an hour, with empty evidence.
// A selector by one tool selects its capturesPerTool captures. It returns the captures, tool by tool, and asserts them open and gapless.
func SeedFleet(tb testing.TB, c *catalog.Catalog, tools, capturesPerTool, hours int) []tracepack.UUID {
	tb.Helper()
	ctx := tb.Context()
	content := procconfig.Content{Classifier: classify.Name(), MaxFrameLen: 1 << 20}
	cfg, err := c.ActivateConfig(ctx, content.Encode())
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := c.Register(ctx, "bench", []int64{cfg}, time.Hour); err != nil {
		tb.Fatal(err)
	}
	ids := make([]tracepack.UUID, 0, tools*capturesPerTool)
	for i := range tools {
		tool := FleetTool(i)
		for k := range capturesPerTool {
			origin := FleetOrigin + int64(k)
			id := admit(tb, c, tool, origin, int64(len(ids))+1)
			for h := range hours {
				var pack tracepack.UUID
				_, _ = rand.Read(pack[:])
				from := uint64(h) * FleetSeqs //nolint:gosec // a small hour index
				at := FleetOrigin + int64(h)*int64(time.Hour)
				p := &catalog.Pack{
					ID: pack, Capture: id, ToolID: tool, Hour: at / int64(time.Hour), Role: catalog.RoleSegment, Key: "staging/" + pack.String(),
					Size: 1 << 20, SeqStart: from, FormatMinor: 2,
					Ranges: []catalog.Range{{SeqFrom: from, SeqTo: from + FleetSeqs - 1, TSFrom: at, TSTo: at + int64(time.Hour) - 1}},
				}
				if _, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: &catalog.Evidence{}}); err != nil {
					tb.Fatal(err)
				}
			}
			ids = append(ids, id)
		}
	}
	checkFleet(tb, c, ids, hours)

	return ids
}

// admit admits a capture of tool from origin with a decodable descriptor of a fresh recorder instance.
func admit(tb testing.TB, c *catalog.Catalog, tool string, origin, streamSeq int64) tracepack.UUID {
	tb.Helper()
	var id, instance tracepack.UUID
	_, _ = rand.Read(id[:])
	_, _ = rand.Read(instance[:])
	device, mono, tolerance := "eqp", int64(0), uint64(time.Second)
	b, err := tracepack.EncodeDescriptor(&tracepack.PackMeta{
		ToolID: tool, Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
		Vantage: tracepack.VantageHost, Recorder: "bench", TimeSource: tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs: &origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
		LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		RecorderInstanceID: instance, RecorderInstanceName: &device,
	})
	if err != nil {
		tb.Fatal(err)
	}
	d := &catalog.Identity{Version: catalog.VersionKey{Capture: id}, Incarnation: 1, StreamSeq: streamSeq, Hour: origin / int64(time.Hour)}
	desc := &catalog.Descriptor{Bytes: b, ToolID: tool, RecorderInstanceID: instance, Device: device, OriginNS: origin}
	if res, err := c.Admit(tb.Context(), d, desc); err != nil || res.Outcome != catalog.Admitted {
		tb.Fatalf("Admit = %+v, %v", res, err)
	}

	return id
}

// checkFleet asserts every seeded capture open and, for the first, its hours retained without a gap.
func checkFleet(tb testing.TB, c *catalog.Catalog, ids []tracepack.UUID, hours int) {
	tb.Helper()
	open, err := c.OpenCaptures(tb.Context())
	if err != nil {
		tb.Fatal(err)
	}
	seeded := map[tracepack.UUID]bool{}
	for _, id := range ids {
		seeded[id] = true
	}
	n := 0
	for _, o := range open {
		if seeded[o.ID] {
			n++
		}
	}
	if n != len(ids) {
		tb.Fatalf("seeded %d open captures, %d open", len(ids), n)
	}
	if len(ids) == 0 || hours == 0 {
		return
	}
	cp, err := c.Completeness(tb.Context(), ids[0])
	if err != nil || cp.Frontier != uint64(hours)*FleetSeqs-1 || len(cp.Gaps) != 0 { //nolint:gosec // a small hour count
		tb.Fatalf("Completeness of a seeded capture = %+v, %v; want %d hours without a gap", cp, err, hours)
	}
}
