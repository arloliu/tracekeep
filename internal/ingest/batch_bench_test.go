package ingest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/metrics"
)

// counting is a Msg that only counts what was done to it, so a benchmark's acknowledgements hold and allocate nothing.
type counting struct {
	acked, progressed, other atomic.Int64
}

func (m *counting) Ack() error                      { m.acked.Add(1); return nil }
func (m *counting) DoubleAck(context.Context) error { m.acked.Add(1); return nil }
func (m *counting) NakWithDelay(time.Duration) error {
	m.other.Add(1)
	return nil
}
func (m *counting) InProgress() error { m.progressed.Add(1); return nil }
func (m *counting) Term() error       { m.other.Add(1); return nil }

// heldEntries are n entries of capture, seqs from 1, each a data record read from its message as a fetch reads it, all on h.
func heldEntries(tb testing.TB, capture tracepack.UUID, n int, h Msg) []Entry {
	tb.Helper()
	es := make([]Entry, n)
	for i := range es {
		r := dataRecord(uint64(i+1), now) //nolint:gosec // a small index
		d := delivered(inline(tb, capture, r, nil))
		d.Record.Record, d.ConfigID = r, 1
		es[i] = Entry{D: d, Msg: h}
	}

	return es
}

// benchIngester is an ingester without a bus, for the bookkeeping benchmarks: its batcher, its metrics and a fixed clock.
func benchIngester(tb testing.TB) *Ingester {
	tb.Helper()
	m, err := metrics.NewIngest(metrics.NewServer(), func() time.Time { return time.Unix(0, now) })
	if err != nil {
		tb.Fatal(err)
	}

	return &Ingester{Batcher: NewBatcher(time.Second, 1<<30), Metrics: m, Now: func() time.Time { return time.Unix(0, now) }}
}

// BenchmarkBatcher fills a fresh Batcher and empties it (P10).
//   - Starting state: n deliveries built once outside the timer, one record each of n captures in one hour, so n batches open.
//   - One iteration: NewBatcher, n Adds, one Due at the interval's end; the batcher is new each iteration, so nothing is reset.
//   - Each iteration asserts Due returns the n batches, one record each.
func BenchmarkBatcher(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1, 100, 1500) {
		b.Run(fmt.Sprintf("batches=%d", n), func(b *testing.B) {
			es := make([]Entry, n)
			for i := range es {
				es[i] = heldEntries(b, tracepack.UUID{0xba, byte(i >> 8), byte(i)}, 1, &counting{})[0]
			}
			due := time.Unix(0, now).Add(time.Second)
			b.ReportAllocs()
			for b.Loop() {
				bt := NewBatcher(time.Second, 1<<20)
				for _, e := range es {
					bt.Add(e, time.Unix(0, now))
				}
				got := bt.Due(due)
				if len(got) != n {
					b.Fatalf("Due returned %d batches, want %d", len(got), n)
				}
				for _, g := range got {
					if len(g.Entries) != 1 {
						b.Fatalf("a batch holds %d records, want 1", len(g.Entries))
					}
				}
			}
		})
	}
}

// BenchmarkAct is the ingester's bookkeeping of one delivery it batches, Ingester.act with ActBatch (P10):
// its alert check, its disposition counted, its deferral id formatted and held, and its batcher's Add under the lock.
//   - Starting state: an ingester with metrics whose batcher holds n deliveries of one capture, built once outside the timer.
//   - One iteration acts on one more delivery of the capture, then cuts its batch back to the n held:
//     the cut is a re-slice, inside the timer since stopping the timer costs far more, and keeps the population fixed.
//   - Each iteration asserts the batch held n+1 records before the cut.
func BenchmarkAct(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 100, 1000, 10000) {
		b.Run(fmt.Sprintf("held=%d", n), func(b *testing.B) {
			capture := tracepack.UUID{0xac}
			es := heldEntries(b, capture, n+1, &counting{})
			g := benchIngester(b)
			for _, e := range es[:n] {
				g.Batcher.Add(e, g.Now())
			}
			bt := g.Batcher.batches[Key{Capture: capture, Hour: HourOf(now)}]
			held, bytes := bt.Entries, bt.Bytes
			e, dec, ctx := es[n], Decision{Action: ActBatch}, b.Context()
			b.ReportAllocs()
			for b.Loop() {
				g.act(ctx, e, dec, 1)
				if len(bt.Entries) != n+1 {
					b.Fatalf("the batch holds %d records, want %d", len(bt.Entries), n+1)
				}
				bt.Entries, bt.Bytes = held[:n], bytes
			}
		})
	}
}

// BenchmarkHeartbeat is one heartbeat, Ingester.beat (P10): every held message kept in progress, its deferral id formatted, the ids held.
//   - Starting state: the batcher holding n deliveries of one capture, built once outside the timer, on a counting Msg.
//   - One iteration is one beat, which changes nothing held; nothing is reset.
//   - Each iteration asserts n more in-progress acknowledgements.
func BenchmarkHeartbeat(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 100, 1000, 10000) {
		b.Run(fmt.Sprintf("held=%d", n), func(b *testing.B) {
			h := &counting{}
			g := benchIngester(b)
			for _, e := range heldEntries(b, tracepack.UUID{0xbe}, n, h) {
				g.Batcher.Add(e, g.Now())
			}
			b.ReportAllocs()
			for b.Loop() {
				before := h.progressed.Load()
				g.beat()
				if got := h.progressed.Load() - before; got != int64(n) {
					b.Fatalf("a beat kept %d messages in progress, want %d", got, n)
				}
			}
		})
	}
}

// BenchmarkTakeDue is the ingester's flush of n batches due at once, Ingester.flush, for its bookkeeping alone (P10):
// before and after each batch's flush the messages of the batches not yet finished are copied out for the heartbeat, a suffix of the due batches each time.
//   - Starting state: an ingester with metrics whose Flusher executes no configuration, so each batch's flush fails on its first line,
//     before any catalog read, write or put; n deliveries built once outside the timer, one record each of n captures in one hour.
//   - One iteration flushes the n batches: added to the batcher and taken due outside the timer, as a step takes them;
//     each flush fails and its message is nak'd, the same work on every iteration, so the time left is the copies, the naks and the joined error.
//   - Each iteration asserts the joined error, n messages nak'd and none left held.
func BenchmarkTakeDue(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 10, 100, 1500) {
		b.Run(fmt.Sprintf("batches=%d", n), func(b *testing.B) {
			h := &counting{}
			es := make([]Entry, n)
			for i := range es {
				es[i] = heldEntries(b, tracepack.UUID{0x7d, byte(i >> 8), byte(i)}, 1, h)[0]
			}
			g := benchIngester(b)
			g.Flusher = &Flusher{Configs: map[int64]Config{}}
			g.AckWait, g.FlushTimeout, g.Heartbeat = 7*time.Minute, time.Minute, 27500*time.Millisecond // the defaults: no batch is kept
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				for _, e := range es {
					g.Batcher.Add(e, g.Now())
				}
				due := g.take(func() []*Batch { return g.Batcher.Due(g.Now().Add(time.Second)) })
				if len(due) != n {
					b.Fatalf("%d batches due, want %d", len(due), n)
				}
				naks := h.other.Load()
				b.StartTimer()
				if err := g.flush(ctx, due); err == nil {
					b.Fatal("flush succeeded, want every batch refused its configuration")
				}
				if got := h.other.Load() - naks; got != int64(n) {
					b.Fatalf("%d messages nak'd, want %d", got, n)
				}
				if len(g.flushing) != 0 || g.batched() != 0 {
					b.Fatalf("%d messages still flushing and %d batched, want none", len(g.flushing), g.batched())
				}
			}
		})
	}
}
