package ingest

import (
	"testing"

	"github.com/arloliu/go-secs/tracepack"

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
