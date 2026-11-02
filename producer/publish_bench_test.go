package producer_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// publishBurst is the records one iteration of BenchmarkPublish observes, well inside the ordinary lane's default bound.
const publishBurst = 256

// BenchmarkPublish measures the records the publisher publishes a second on the in-memory bus, reported as records/s.
// Its starting state is a producer with an open capture whose start is on the bus.
// An iteration observes publishBurst inbound data records of 14 B at the present instant
// and waits until the bus stored publishBurst more messages, which a stuck publisher fails after 10 s.
// It asserts each iteration that every record was stored and the recorder counted no drop;
// after the loop the bus holds the start and every record, each once.
// The in-memory bus keeps every message, so its heap grows with the iterations; that cost stays inside the timer.
func BenchmarkPublish(b *testing.B) {
	h := newHarness(b, producer.Tunables{})
	h.begin()
	h.wait(1)
	var stored, want atomic.Int64
	done := make(chan struct{}, 1)
	h.bus.FailPublish(func(*producer.Msg) error {
		if stored.Add(1) == want.Load() {
			done <- struct{}{}
		}

		return nil
	})
	payload := frame(14)
	timeout := time.NewTimer(time.Hour)
	b.ReportAllocs()
	for b.Loop() {
		want.Add(publishBurst)
		for range publishBurst {
			h.observe(payload)
		}
		timeout.Reset(10 * time.Second)
		select {
		case <-done:
		case <-timeout.C:
			b.Fatalf("bus stored %d of %d records", stored.Load(), want.Load())
		}
		if c := h.rec.Counters(); c.Dropped != [core.NumDropReasons]uint64{} {
			b.Fatalf("dropped %v, want none", c.Dropped)
		}
	}
	timeout.Stop()
	b.ReportMetric(float64(want.Load())/b.Elapsed().Seconds(), "records/s")
	if got := len(h.bus.Messages()); int64(got) != 1+want.Load() {
		b.Fatalf("bus holds %d messages, want the start and %d records", got, want.Load())
	}
}
