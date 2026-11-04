package ingest

import (
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// A fetch of near-limit payloads into one capture holds at most the batch's byte bound and one record:
// a batch that reaches its bound is flushed before the fetch's next delivery is batched (ingest.md §6.2),
// so no fetch is held in full and no closed batch waits for the fetch's end.
// Every record is still written once and every message acknowledged after its segment's registration.
func TestFullBatchFlushedWithinItsFetch(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		g := r.g
		capture := tracepack.UUID{0x0e}
		limit := int(r.nc.MaxPayload()) - 4096
		const n = 20
		g.FetchBatch, g.MaxWait = n+1, 5*time.Second
		bound := 64 << 20 * n / fetchDeliveries // production's fetch-to-bound ratio, as BenchmarkFetchOvershoot
		g.Batcher = NewBatcher(time.Hour, bound)
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		for seq := uint64(1); seq <= n; seq++ {
			r.publish(t, nearLimit(t, capture, seq, limit), nil)
		}
		peak := 0
		read := func() {
			if held, ok := heldPayload(g); ok {
				peak = max(peak, held)
			}
		}
		clk := g.Now
		g.Now = func() time.Time { read(); return clk() }
		g.Timed = func(string, time.Duration) { read() }
		registered, acked := 0, 0
		g.Flusher.hooks[hookAfterRegister] = func() error { registered++; return nil }
		g.Flusher.hooks[hookAfterAck] = func() error {
			if registered == 0 {
				t.Error("a message acknowledged before any registration")
			}
			acked++
			return nil
		}

		if err := g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		if most := bound + limit; peak > most || peak == 0 {
			t.Errorf("peak %d payload bytes held, want some and at most the bound and one record, %d", peak, most)
		}
		if registered < 2 {
			t.Errorf("%d flushes registered, want the fetch split by the batch's bound", registered)
		}
		g.Batcher.Interval = 0 // the rest is due
		if err := g.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		if acked != n+1 {
			t.Errorf("%d messages acknowledged, want %d", acked, n+1)
		}
		stored := 0
		for _, k := range storedKeys(t, r.st) {
			recs := records(t, r.st, k)
			stored += len(recs)
			payload := 0
			for _, rec := range recs {
				payload += len(rec.Payload)
			}
			if payload > bound+limit {
				t.Errorf("%s holds %d payload bytes, past the bound and one record", k, payload)
			}
		}
		if stored != n+1 {
			t.Errorf("%d records stored, want %d, each once", stored, n+1)
		}
		if left := r.settled(t, 0); left != 0 {
			t.Errorf("%d messages left on the stream, want every one acknowledged", left)
		}
	})
}

// A batch is due as soon as an Add takes it to its byte bound, without waiting for Due's next reading.
func TestBatcherClosesAtItsByteBound(t *testing.T) {
	t.Parallel()
	b := NewBatcher(time.Hour, 10)
	at := time.Unix(0, now)
	add := func(seq uint64, size int) {
		d := &Delivery{Raw: Raw{Version: Version{Capture: tracepack.UUID{1}, Seq: seq}, TS: now}}
		d.Record.Record.Payload = make([]byte, size)
		b.Add(Entry{D: d, Msg: &msg{}}, at)
	}
	add(1, 4)
	add(2, 4)
	if len(b.closed) != 0 {
		t.Fatalf("%d batches closed below the bound, want none", len(b.closed))
	}
	add(3, 4)
	if len(b.closed) != 1 || len(b.closed[0].Entries) != 3 {
		t.Fatalf("closed %+v, want the batch of seqs 1 to 3, closed at its bound", b.closed)
	}
	add(4, 4)
	due := b.Due(at)
	if len(due) != 1 || len(due[0].Entries) != 3 {
		t.Fatalf("due %+v, want only the closed batch", due)
	}
	if all := b.All(); len(all) != 1 || len(all[0].Entries) != 1 {
		t.Fatalf("left %+v, want seq 4's batch open", all)
	}
}
