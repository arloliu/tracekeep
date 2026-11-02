// Package benchscale reads TRACEKEEP_BENCH_SCALE, which sizes a benchmark runs (650-perf-sec.md):
// its small size always, its medium and large sizes only when the variable names them;
// and TRACEKEEP_BENCH_ENGINES, which catalog engines it runs.
package benchscale

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// Env names the largest size the benchmarks run: small (the default when unset), medium or large.
const Env = "TRACEKEEP_BENCH_SCALE"

// Sizes returns the sizes to run, smallest first: small, then medium when Env is medium or large, then large when it is large.
// Any other value fails tb.
func Sizes[T any](tb testing.TB, small, medium, large T) []T {
	tb.Helper()
	switch v := os.Getenv(Env); v {
	case "", "small":
		return []T{small}
	case "medium":
		return []T{small, medium}
	case "large":
		return []T{small, medium, large}
	default:
		tb.Fatalf("%s=%q: want small, medium or large", Env, v)

		return nil
	}
}

// EnginesEnv names the catalog engines a benchmark runs, comma-separated (sqlite, mariadb); unset or empty runs every engine.
// A benchmark loop skips the other engines' sub-benchmarks entirely, so they emit no row,
// as bench.yaml's MariaDB runs need (650-perf-sec.md).
const EnginesEnv = "TRACEKEEP_BENCH_ENGINES"

// Engines returns the engines of all that EnginesEnv names, in all's order, or all when it names none.
// A name not in all fails tb.
func Engines(tb testing.TB, all ...string) []string {
	tb.Helper()
	v := os.Getenv(EnginesEnv)
	if strings.TrimSpace(v) == "" {
		return all
	}
	named := map[string]bool{}
	for name := range strings.SplitSeq(v, ",") {
		name = strings.TrimSpace(name)
		if !slices.Contains(all, name) {
			tb.Fatalf("%s=%q: %q is not one of %v", EnginesEnv, v, name, all)
		}
		named[name] = true
	}

	return slices.DeleteFunc(slices.Clone(all), func(e string) bool { return !named[e] })
}
