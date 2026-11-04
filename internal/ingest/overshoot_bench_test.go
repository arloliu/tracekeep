package ingest

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
)

// fetched is one delivery a benchConsumer hands out: its message as the bus delivers it,
// counting the reads of its body, once a disposition, and the naks a failed flush sends.
// Only the methods a step calls are implemented; the embedded Msg is nil.
type fetched struct {
	jetstream.Msg
	m           Message
	meta        jetstream.MsgMetadata
	reads, naks *atomic.Int64
}

func (f *fetched) Metadata() (*jetstream.MsgMetadata, error) { return &f.meta, nil }
func (f *fetched) Headers() nats.Header                      { return f.m.Header }
func (f *fetched) Subject() string                           { return f.m.Subject }
func (f *fetched) Data() []byte {
	f.reads.Add(1)
	return f.m.Body
}
func (f *fetched) NakWithDelay(time.Duration) error {
	f.naks.Add(1)
	return nil
}

// benchConsumer is a consumer whose every fetch delivers the same messages at once, then ends.
type benchConsumer struct {
	jetstream.Consumer
	msgs []jetstream.Msg
}

func (c benchConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	ch := make(chan jetstream.Msg, len(c.msgs))
	for _, m := range c.msgs {
		ch <- m
	}
	close(ch)

	return fetchBatch(ch), nil
}

// fetchBatch is a fetch's messages, ending without an error.
type fetchBatch <-chan jetstream.Msg

func (b fetchBatch) Messages() <-chan jetstream.Msg { return b }
func (b fetchBatch) Error() error                   { return nil }

// heldPayload sums the payload bytes of every delivery g holds, the set its heartbeat walks:
// open and closed batches, and those a flush took and has not finished; ok is false while another holds g's lock.
func heldPayload(g *Ingester) (bytes int, ok bool) {
	if !g.mu.TryLock() {
		return 0, false
	}
	defer g.mu.Unlock()
	g.Batcher.Held(func(e Entry) { bytes += len(e.D.Record.Record.Payload) })
	for _, e := range g.flushing {
		bytes += len(e.D.Record.Record.Payload)
	}

	return bytes, true
}

// BenchmarkFetchOvershoot is one step's fetch of near-limit inline payloads into one capture, Ingester.Step (P10):
// how far past the batch's byte bound a fetch larger than the bound takes the payload held.
// It reports the peak payload bytes held against the bound (held-B, bound-B, x-bound); its time is incidental.
//   - Starting state: a replica on the embedded bus and the SQLite catalog, its capture admitted by its start once, outside the timer;
//     n messages of one capture and hour built once, each an S6F11 whose body is the inline limit, the server's max_payload less 4096
//     (the producer's rule); a consumer that delivers all n at every fetch.
//     The batch's byte bound is ingest.batch_max_bytes' default scaled by n against ingest.fetch_batch's default (64 MiB at 500),
//     so every size has production's fetch-to-bound ratio; the flush interval is zero, so every batch is due once the fetch is done.
//   - The Flusher executes no configuration, so the due batch's flush fails on its first line and its messages are nak'd:
//     nothing is written, and every iteration disposes of the same unresolved deliveries again.
//   - The held set is read at every reading of the ingester's clock outside its lock: before each delivery's disposition,
//     and once the fetch is done, before the due batches are taken; the reading under the lock, a batch's Add, is skipped.
//     It is read too at the end of each timed stage: a delivery's disposition, its batching and a batch's flush, the flushing batch still held.
//   - Each iteration asserts n deliveries read and disposed of, some payload held, n nak'd by the failed flush, and nothing held after the step.
func BenchmarkFetchOvershoot(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 20, 100, 500) {
		b.Run(fmt.Sprintf("fetch=%d", n), func(b *testing.B) {
			r := newReplica(b, catalogtest.Open(b, catalog.EngineSQLite))
			g := r.g
			capture := tracepack.UUID{0x0f}
			if _, dec := dispose(b, g.Executor, inline(b, capture, startRecord(b, now), descriptorOf(b, "eqp"))); dec.Action != ActBatch {
				b.Fatalf("start disposed %+v, want batched", dec)
			}
			limit := int(r.nc.MaxPayload()) - 4096
			subject, err := tpbus.Subject(prefix, "T1", capture)
			if err != nil {
				b.Fatal(err)
			}
			reads, naks := &atomic.Int64{}, &atomic.Int64{}
			msgs := make([]jetstream.Msg, n)
			for i := range msgs {
				m := nearLimit(b, capture, uint64(i+1), limit) //nolint:gosec // a small index
				m.Subject, m.Header = subject, headers(&m.Env)
				meta := jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: uint64(i + 2)}, NumDelivered: 1} //nolint:gosec // a small index
				msgs[i] = &fetched{m: m, meta: meta, reads: reads, naks: naks}
			}
			bound := 64 << 20 * n / fetchDeliveries
			g.Consumer = benchConsumer{msgs: msgs}
			g.Batcher = NewBatcher(0, bound) // every batch is due at the step's end, so the step leaves nothing held
			g.Flusher = &Flusher{Configs: map[int64]Config{}}
			peak, stepPeak := 0, 0
			clk := g.Now
			g.Now = func() time.Time {
				if held, ok := heldPayload(g); ok {
					stepPeak = max(stepPeak, held)
				}

				return clk()
			}
			g.Timed = func(string, time.Duration) {
				if held, ok := heldPayload(g); ok {
					stepPeak = max(stepPeak, held)
				}
			}
			ctx := b.Context()
			for b.Loop() {
				read, naked := reads.Load(), naks.Load()
				stepPeak = 0
				if err := g.Step(ctx); err != nil {
					b.Fatal(err)
				}
				if got := reads.Load() - read; got != int64(n) {
					b.Fatalf("%d deliveries read, want the %d fetched", got, n)
				}
				if got := naks.Load() - naked; got != int64(n) || stepPeak == 0 {
					b.Fatalf("%d deliveries nak'd, peak %d bytes held; want the %d of the failed flush, batched first", got, stepPeak, n)
				}
				peak = max(peak, stepPeak)
				if held, _ := heldPayload(g); held != 0 {
					b.Fatalf("%d payload bytes held after the step, want none", held)
				}
			}
			b.ReportMetric(float64(peak), "held-B")
			b.ReportMetric(float64(bound), "bound-B")
			b.ReportMetric(float64(peak)/float64(bound), "x-bound")
		})
	}
}

// nearLimit is the inline message of an S6F11 of seq whose body is limit bytes.
func nearLimit(tb testing.TB, capture tracepack.UUID, seq uint64, limit int) Message {
	tb.Helper()
	record := func(text int) Message {
		r := tracepack.Record{
			Seq: seq, TSUTCNs: now + int64(seq), Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, //nolint:gosec // a small seq
			Fidelity: tracepack.FidelityWireExact, Payload: maskingtest.Frame(6, 11, maskingtest.ASCII(strings.Repeat("v", text))),
		}

		return inline(tb, capture, r, nil)
	}
	m := record(1 << 16)
	m = record(1<<16 + limit - len(m.Body))
	if len(m.Body) != limit {
		tb.Fatalf("a body of %d bytes, want %d", len(m.Body), limit)
	}

	return m
}

// headers are an envelope's headers as the bus delivers them.
func headers(env *tpbus.Envelope) nats.Header {
	h := nats.Header{}
	for k, v := range map[string]string{
		tpbus.HeaderLayout: env.Layout, tpbus.HeaderMsgID: env.MsgID, tpbus.HeaderDescriptor: env.Descriptor,
		tpbus.HeaderPayloadRef: env.PayloadRef, tpbus.HeaderPayloadDigest: env.PayloadDigest,
	} {
		if v != "" {
			h.Set(k, v)
		}
	}

	return h
}
