package ingest

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/store"
)

// simulated is a pass's clock, read and advanced by the one goroutine that flushes, which also runs the heartbeat's ticks
// at the times the production derivation sets, so a tick never reads a batch the flush is sorting.
type simulated struct {
	t, next time.Time
	g       *Ingester
	// ticking runs the ticks; off, the heartbeat is late for the whole pass.
	ticking bool
}

func (s *simulated) now() time.Time { return s.t }

// advance moves the clock by d, running each tick it passes.
func (s *simulated) advance(d time.Duration) {
	end := s.t.Add(d)
	for !s.next.After(end) {
		s.t = s.next
		if s.ticking {
			s.g.beat()
		}
		s.next = s.next.Add(s.g.Heartbeat)
	}
	s.t = end
}

// simulatedPut is a put that takes d on the simulated clock.
type simulatedPut struct {
	store.ObjectStore
	clock *simulated
	d     time.Duration
}

func (p simulatedPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	p.clock.advance(p.d)

	return p.ObjectStore.Put(ctx, key, r, size)
}

// ackTimed is a message whose acknowledgement records its age on the simulated clock since its ack wait last started,
// its fetch or its last in-progress ack.
type ackTimed struct {
	*msg
	clock       *simulated
	since       time.Time
	age         time.Duration
	acked, kept int
}

func (m *ackTimed) InProgress() error {
	m.since, m.kept = m.clock.now(), m.kept+1
	return m.msg.InProgress()
}

func (m *ackTimed) Ack() error {
	m.age, m.acked = m.clock.now().Sub(m.since), m.acked+1
	return m.msg.Ack()
}

// A pass of queued batches at the production derivation of the tick: each flush runs within its budget,
// and every delivery is acknowledged within its ack wait, whether the tick is floored at a second above what the timing leaves,
// or late for the whole pass: a batch whose ack wait would end within its budget and two ticks is kept as its flush starts.
func TestKeptAtTheFlushStart(t *testing.T) {
	t.Parallel()
	defaults := config.Ingest{FlushInterval: 5 * time.Minute, MaxWait: 5 * time.Second, FlushTimeout: time.Minute, AckWait: 7 * time.Minute}
	for name, tc := range map[string]struct {
		timing  config.Ingest
		batches int
		put     time.Duration
		ticking bool
	}{
		"the tick's floor": {
			timing:  config.Ingest{FlushInterval: 100 * time.Millisecond, MaxWait: 100 * time.Millisecond, FlushTimeout: 900 * time.Millisecond, AckWait: 1200 * time.Millisecond},
			batches: 2, put: 700 * time.Millisecond, ticking: true,
		},
		"the defaults":                {timing: defaults, batches: 4, put: 50 * time.Second, ticking: true},
		"the defaults, the tick late": {timing: defaults, batches: 4, put: 50 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				p := newPipeline(t, c)
				g := p.ingester(slog.New(slog.DiscardHandler))
				start := time.Unix(0, now)
				clock := &simulated{t: start, g: g, ticking: tc.ticking}
				g.AckWait, g.FlushTimeout, g.Heartbeat = tc.timing.AckWait, tc.timing.FlushTimeout, tc.timing.Heartbeat()
				clock.next = start.Add(g.Heartbeat)
				g.Now, p.f.Now = clock.now, clock.now
				p.f.Store = simulatedPut{ObjectStore: p.st, clock: clock, d: tc.put}
				var msgs []*ackTimed
				for i := range tc.batches {
					for _, m := range traffic(t, tracepack.UUID{0x4d, byte(i)}) {
						d, dec := dispose(t, p.x, m)
						if dec.Action != ActBatch {
							t.Fatalf("disposed %+v, want batched", dec)
						}
						h := &ackTimed{msg: &msg{}, clock: clock, since: start}
						p.b.Add(Entry{D: d, Msg: h}, start)
						msgs = append(msgs, h)
					}
				}
				clock.advance(tc.timing.FlushInterval + tc.timing.MaxWait) // due, and seen after a fetch's wait
				if err := g.flush(t.Context(), g.take(p.b.All)); err != nil {
					t.Fatal(err)
				}
				for i, h := range msgs {
					if h.acked != 1 || h.age >= g.AckWait {
						t.Errorf("message %d, the tick every %s: %d acknowledgements, the last %s after its ack wait started, %d in-progress; want one within %s",
							i, g.Heartbeat, h.acked, h.age, h.kept, g.AckWait)
					}
				}
			})
		})
	}
}
