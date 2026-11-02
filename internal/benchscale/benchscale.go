// Package benchscale reads TRACEKEEP_BENCH_SCALE, which sizes a benchmark runs (650-perf-sec.md):
// its small size always, its medium and large sizes only when the variable names them.
package benchscale

import (
	"os"
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
