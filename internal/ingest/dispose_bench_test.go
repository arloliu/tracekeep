package ingest

import (
	"fmt"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
)

// BenchmarkDispose is one delivery's whole disposition, Executor.Dispose on the SQLite catalog:
// its catalog reads, a claim-check record's fetch from an in-memory bucket, and with masking on its masking and keys (P7, P19, P21).
//   - Starting state: the capture admitted by its start, once, outside the timer; the message of the baseline's first data carrier,
//     inline or claim-check, under a policy-less configuration (masking off) or the baseline's masking configuration (masking on).
//   - One iteration disposes of that message, which writes nothing, so each iteration does the same work; nothing is reset.
//   - Each iteration asserts the record batched with the redaction entries of the first disposition, none with masking off and some with it on.
func BenchmarkDispose(b *testing.B) {
	pol := policy.Baseline()
	carriers, err := maskingtest.Carriers(&pol)
	if err != nil {
		b.Fatal(err)
	}
	carrier := carriers[0]
	if carrier.Kind != tracepack.KindData {
		b.Fatalf("the first carrier is a %v record, want data", carrier.Kind)
	}
	for _, masking := range []string{"off", "on"} {
		for _, transport := range []string{"inline", "claim-check"} {
			b.Run("masking="+masking+"/"+transport, func(b *testing.B) {
				c := catalogtest.Open(b, catalog.EngineSQLite)
				cl := newClaims()
				x := executor(b, c, cl)
				if masking == "on" {
					for id := range x.Configs {
						x.Configs[id] = maskingConfig(b)
					}
					x.Keys = maskingKeys(b, c)
				}
				capture := tracepack.UUID{0xd1}
				if _, dec := dispose(b, x, inline(b, capture, startRecord(b, now), descriptorOf(b, "eqp"))); dec.Action != ActBatch {
					b.Fatalf("start disposed %+v, want batched", dec)
				}
				r := carrierRecord(&carrier, 1)
				m := inline(b, capture, r, nil)
				if transport == "claim-check" {
					m = claimCheck(b, cl, capture, r, nil)
				}
				d, dec := dispose(b, x, m)
				want := len(d.Redaction)
				if dec.Action != ActBatch || (want > 0) != (masking == "on") {
					b.Fatalf("disposed %+v with %d redaction entries, want batched and masked only with masking on", dec, want)
				}
				ctx := b.Context()
				b.SetBytes(int64(len(r.Payload)))
				b.ReportAllocs()
				for b.Loop() {
					d, dec, err := x.Dispose(ctx, m)
					if err != nil || dec.Action != ActBatch || len(d.Redaction) != want {
						b.Fatalf("disposed %+v with %d redaction entries, %v; want batched with %d", dec, len(d.Redaction), err, want)
					}
				}
			})
		}
	}
}

// fetchDeliveries is the deliveries of one fetch, ingest.fetch_batch's default.
const fetchDeliveries = 500

// BenchmarkDisposeFetch is one fetch's dispositions, Executor.Dispose of each of its deliveries in turn on the SQLite catalog (P7):
// a fetch of fetchDeliveries inline data records spread over k captures, ordinary or covered by a termination order.
//   - Starting state: k captures admitted by their starts, once, outside the timer;
//     fetchDeliveries messages built once, the captures taking turns as a fetch from many equipment interleaves them,
//     seqs from 1 within a capture and each message its own stream seq on incarnation 1.
//     Order-covered: each capture also holds an active termination order whose cut-off covers every message.
//   - Ordinary: each delivery is unresolved and admitted, three catalog reads a message on this path
//     (the version in parked, the capture's active orders, the capture's row), and is batched; it writes nothing, so nothing is reset.
//   - Order-covered: each delivery resolves covered by its capture's order (parked, the orders and the held-seq probe),
//     and the order's application terminates its version in a transaction of its own, after which it is acknowledged under the termination.
//     That writes the version's parked row, its copy row, an audit row and a scope event; they are deleted outside the timer after each iteration,
//     so every iteration resolves the same unresolved versions against a catalog of the same size.
//   - Each iteration asserts every delivery's final disposition: batched when ordinary, acknowledged as terminated when order-covered.
//   - The catalog reads a message are this path's, read from the code: the catalog has no statement count a benchmark can read.
func BenchmarkDisposeFetch(b *testing.B) {
	for _, k := range benchscale.Sizes(b, 1, 10, 100) {
		for _, covered := range []bool{false, true} {
			name := "ordinary"
			if covered {
				name = "order-covered"
			}
			b.Run(fmt.Sprintf("captures=%d/%s", k, name), func(b *testing.B) {
				c := catalogtest.Open(b, catalog.EngineSQLite)
				x := executor(b, c, newClaims())
				ctx := b.Context()
				captures := make([]tracepack.UUID, k)
				for i := range captures {
					captures[i] = tracepack.UUID{0xdf, byte(i >> 8), byte(i)}
					if _, dec := dispose(b, x, inline(b, captures[i], startRecord(b, now), descriptorOf(b, "eqp"))); dec.Action != ActBatch {
						b.Fatalf("start disposed %+v, want batched", dec)
					}
					if covered {
						if _, err := c.TerminateCapture(ctx, captures[i], 1, fetchDeliveries, "op", fmt.Sprintf("bench-%d", i)); err != nil {
							b.Fatal(err)
						}
					}
				}
				msgs := make([]Message, fetchDeliveries)
				for i := range msgs {
					m := inline(b, captures[i%k], dataRecord(uint64(i/k+1), now), nil) //nolint:gosec // a small index
					m.StreamSeq = int64(i + 1)
					msgs[i] = m
				}
				want := Decision{Action: ActBatch}
				if covered {
					want = final(ActAck, ReasonTerminated)
				}
				write, _ := c.Pools()
				b.ReportAllocs()
				for b.Loop() {
					for _, m := range msgs {
						_, dec, err := x.Dispose(ctx, m)
						if err != nil || dec.Action != want.Action || dec.Reason != want.Reason {
							b.Fatalf("disposed %+v, %v; want %+v", dec, err, want)
						}
					}
					if covered {
						b.StopTimer()
						for _, table := range []string{"parked", "parked_copies", "scope_events", "audit_log WHERE actor = 'ingest'"} {
							res, err := write.ExecContext(ctx, "DELETE FROM "+table)
							if err != nil {
								b.Fatal(err)
							}
							if n, err := res.RowsAffected(); err != nil || n != fetchDeliveries {
								b.Fatalf("the iteration wrote %d rows in %s, %v; want one a delivery", n, table, err)
							}
						}
						b.StartTimer()
					}
				}
			})
		}
	}
}
