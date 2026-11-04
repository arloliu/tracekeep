package catalog

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/jmoiron/sqlx"

	"github.com/arloliu/tracekeep/internal/benchscale"
)

// standingKeys is how many keys of each standing BenchmarkKeyStandings' population holds, its largest ask.
const standingKeys = 1024

// Each capture of BenchmarkMergeCandidates' population holds scopes of hours 1 to mergeHours:
// those up to settledHour settled, the rest open, and those from readyHour on not yet quiet.
const (
	mergeHours  = 100
	settledHour = 90
	readyHour   = 96
)

// BenchmarkKeyStandings is the orphan pass's look at a chunk of listed keys, their standings asked together.
// Its starting state is one open capture with standingKeys registered packs, 100 an hour, and standingKeys tombstones;
// as many unknown keys are named nowhere.
// An iteration asks the standings of the first 1, 128, 129 or 1,024 keys of the three kinds interleaved, registered, tombstoned and unknown in turn,
// every key distinct, and asserts each standing; it writes nothing.
// keyStandings asks them, so the row does the same work whichever way the catalog answers a chunk.
func BenchmarkKeyStandings(b *testing.B) {
	benchEngines(b, func(b *testing.B, c *Catalog) {
		capture := seedCaptures(b, c, 1, 1)[0]
		packs, _ := seedPacks(b, c, capture, packsSeed{hour0: 1, hours: standingKeys/100 + 1, packs: 100, ranges: 1, span: 1})
		tombstoned := seedTombstones(b, c, standingKeys, "deleted", 1)
		keys := make([]string, 0, 3*standingKeys)
		want := make([]KeyStanding, 0, 3*standingKeys)
		for i := range standingKeys {
			keys = append(keys, "staging/"+packs[i].String(), tombstoned[i], fmt.Sprintf("staging/unknown-%d", i))
			want = append(want, KeyRegistered, KeyTombstoned, KeyUnknown)
		}
		for _, n := range []int{1, 128, 129, standingKeys} {
			b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if got, err := keyStandings(b, c, keys[:n]); err != nil || !slices.Equal(got, want[:n]) {
						b.Fatalf("standings of %d keys = %v, %v; want %v", n, got, err, want[:n])
					}
				}
			})
		}
	})
}

// BenchmarkMergeCandidates is the merge pass's list of the scopes it may merge, MergeCandidates.
// Its starting state is 1,000 or 100,000 scopes (TRACEKEEP_BENCH_SCALE), mergeHours of each of a hundredth as many open captures, each scope with one pack:
// nine in ten settled, their packs merged; of the open rest, every one staged, half before readyHour and half not yet quiet.
// An iteration lists the candidates before readyHour and asserts them all, by hour then capture; it writes nothing.
func BenchmarkMergeCandidates(b *testing.B) {
	for _, n := range slices.Compact(benchscale.Sizes(b, 1000, 100_000, 100_000)) {
		b.Run(fmt.Sprintf("scopes=%d", n), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				captures := seedCaptures(b, c, 1, n/mergeHours)
				for _, capture := range captures {
					seedPacks(b, c, capture, packsSeed{hour0: 1, hours: mergeHours, packs: 1, ranges: 1, span: 1})
				}
				seedTx(b, c, func(tx *sqlx.Tx) {
					for _, q := range []string{
						`UPDATE scopes SET state = 'settled', revision = 2 WHERE hour <= ?`,
						`UPDATE packs SET state = 'merged' WHERE hour <= ?`,
					} {
						if _, err := tx.ExecContext(b.Context(), q, settledHour); err != nil {
							b.Fatal(err)
						}
					}
				})
				byID := slices.Clone(captures)
				slices.SortFunc(byID, func(x, y tracepack.UUID) int { return bytes.Compare(x[:], y[:]) })
				var want []Scope
				for h := int64(settledHour + 1); h < readyHour; h++ {
					for _, capture := range byID {
						want = append(want, Scope{Capture: capture, Hour: h})
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					if got, err := mergeCandidates(b, c, readyHour); err != nil || !slices.Equal(got, want) {
						b.Fatalf("MergeCandidates = %d scopes, %v; want the %d seeded", len(got), err, len(want))
					}
				}
			})
		})
	}
}

// keyStandings reads keys' standings in their order, in asks of at most MaxKeyStandings keys.
func keyStandings(b *testing.B, c *Catalog, keys []string) ([]KeyStanding, error) {
	b.Helper()
	out := make([]KeyStanding, 0, len(keys))
	for chunk := range slices.Chunk(keys, MaxKeyStandings) {
		s, err := c.KeyStandings(b.Context(), chunk)
		if err != nil {
			return nil, err
		}
		out = append(out, s...)
	}

	return out, nil
}

// mergeCandidates lists the candidates before ready as scopes alone, so the row asserts the same answer whatever else MergeCandidates returns with them.
func mergeCandidates(b *testing.B, c *Catalog, ready int64) ([]Scope, error) {
	b.Helper()
	cands, err := c.MergeCandidates(b.Context(), ready)
	if err != nil {
		return nil, err
	}
	out := make([]Scope, len(cands))
	for i, cand := range cands {
		out[i] = cand.Scope
	}

	return out, nil
}
