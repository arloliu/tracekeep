package catalog

import (
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
)

// tinyListing is the objects of BenchmarkRecordListing's tiny listing.
const tinyListing = 10

// listingCase is one starting state of BenchmarkRecordListing: setup seeds it and returns the listing an iteration records,
// added is the claim rows an iteration adds and claims and unattributed the rows it leaves,
// and restore, nil for none, brings the starting state back outside the timer.
type listingCase struct {
	name                 string
	setup                func(tb testing.TB, c *Catalog) []Listed
	added                int
	claims, unattributed int
	restore              func(tb testing.TB, c *Catalog)
}

// BenchmarkRecordListing is the compactor's recording of a claim bucket listing, in bounded transactions.
// Its starting states hold 10,000, 100,000 or 1,000,000 objects (TRACEKEEP_BENCH_SCALE), attributed or unattributed apart:
// steady, every listed object already recorded; new, none recorded yet; gone, every recorded object missing from an empty listing;
// and attributed/tiny, a listing of tinyListing of the recorded objects against the whole recorded history, which writes nothing.
// One iteration records the listing and asserts the claim rows it added and the rows left, then restores the starting state outside the timer:
// a new listing's rows deleted again, a gone listing's unattributed rows inserted again.
func BenchmarkRecordListing(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 10000, 100000, 1000000) {
		b.Run(fmt.Sprintf("objects=%d", n), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				l := benchLease(b, c)
				for _, tc := range listingCases(n) {
					b.Run(tc.name, func(b *testing.B) {
						listing := tc.setup(b, c)
						b.ReportAllocs()
						for b.Loop() {
							added, err := c.RecordListing(b.Context(), l, listing, 1)
							b.StopTimer()
							if err != nil || added != tc.added {
								b.Fatalf("RecordListing = %d, %v; want %d added", added, err, tc.added)
							}
							claims, unattributed := count(b, c, `SELECT COUNT(*) FROM claim_objects`), count(b, c, `SELECT COUNT(*) FROM unattributed_objects`)
							if claims != tc.claims || unattributed != tc.unattributed {
								b.Fatalf("%d claim and %d unattributed rows, want %d and %d", claims, unattributed, tc.claims, tc.unattributed)
							}
							if tc.restore != nil {
								tc.restore(b, c)
							}
							b.StartTimer()
						}
						// The next case seeds its own rows from none.
						exec(b, c, `DELETE FROM claim_objects`)
						exec(b, c, `DELETE FROM unattributed_objects`)
					})
				}
			})
		})
	}
}

// listingCases are BenchmarkRecordListing's starting states of n objects.
func listingCases(n int) []listingCase {
	capture := tracepack.UUID{1}
	claims := func(tb testing.TB, c *Catalog) []Listed {
		return seedClaimObjects(tb, c, "claims", capture, 1, 0, n, "live")
	}
	deleteClaims := func(tb testing.TB, c *Catalog) { exec(tb, c, `DELETE FROM claim_objects`) }
	deleteUnattributed := func(tb testing.TB, c *Catalog) { exec(tb, c, `DELETE FROM unattributed_objects`) }
	var gone []Listed

	return []listingCase{
		{name: "attributed/steady", setup: claims, claims: n},
		{name: "attributed/new", setup: func(tb testing.TB, c *Catalog) []Listed {
			ls := claims(tb, c)
			deleteClaims(tb, c)

			return ls
		}, added: n, claims: n, restore: deleteClaims},
		{name: "attributed/gone", setup: func(tb testing.TB, c *Catalog) []Listed { claims(tb, c); return nil }, claims: n},
		{name: "attributed/tiny", setup: func(tb testing.TB, c *Catalog) []Listed { return claims(tb, c)[:tinyListing] }, claims: n},
		{name: "unattributed/steady", setup: func(tb testing.TB, c *Catalog) []Listed { return seedUnattributed(tb, c, n) }, unattributed: n},
		{name: "unattributed/new", setup: func(tb testing.TB, c *Catalog) []Listed {
			ls := seedUnattributed(tb, c, n)
			deleteUnattributed(tb, c)

			return ls
		}, unattributed: n, restore: deleteUnattributed},
		{name: "unattributed/gone", setup: func(tb testing.TB, c *Catalog) []Listed {
			gone = seedUnattributed(tb, c, n)

			return nil
		}, restore: func(tb testing.TB, c *Catalog) {
			rows := make([][]any, len(gone))
			for i, o := range gone {
				rows[i] = []any{o.Key, o.Size, 1}
			}
			seedTx(tb, c, func(tx *sqlx.Tx) {
				insertRows(tb, tx, "unattributed_objects", []string{"object_key", "size", "seen_at"}, rows)
			})
		}},
	}
}
