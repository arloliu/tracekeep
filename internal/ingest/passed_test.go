package ingest

import (
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// passes records the PassSettled signals of an ingester, and the events of its flushes beside them, in order.
type passes struct {
	mu     sync.Mutex
	events []string
	errs   []error
	at     []time.Time
}

func (p *passes) settled(at time.Time, _ []*Batch, err error) {
	p.mu.Lock()
	p.events = append(p.events, "settled")
	p.errs, p.at = append(p.errs, err), append(p.at, at)
	p.mu.Unlock()
}

func (p *passes) event(e string) {
	p.mu.Lock()
	p.events = append(p.events, e)
	p.mu.Unlock()
}

func (p *passes) read() ([]string, []error, []time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.events), slices.Clone(p.errs), slices.Clone(p.at)
}

// passIngester is an ingester over p's executor, batcher and flusher, its PassSettled recorded in rec.
func passIngester(p *pipeline, rec *passes) *Ingester {
	return &Ingester{
		Executor: p.x, Batcher: p.b, Flusher: p.f, Log: slog.New(slog.DiscardHandler), Now: time.Now, PassSettled: rec.settled,
	}
}

// feedCapture batches capture's start and n data records, each with the message mk returns.
func feedCapture(t *testing.T, p *pipeline, capture tracepack.UUID, n uint64, mk func() Msg) {
	t.Helper()
	ms := []Message{inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp"))}
	for seq := uint64(1); seq <= n; seq++ {
		ms = append(ms, inline(t, capture, dataRecord(seq, now+int64(seq)), nil))
	}
	for _, m := range ms {
		d, dec := dispose(t, p.x, m)
		if dec.Action != ActBatch {
			t.Fatalf("seq %d disposed %+v, want batched", d.Raw.Version.Seq, dec)
		}
		p.b.Add(Entry{D: d, Msg: mk()}, time.Unix(0, now))
	}
}

// heldAck is a message whose first acknowledgement blocks until released.
type heldAck struct {
	msg
	entered, release chan struct{}
	once             *sync.Once
}

func (m *heldAck) Ack() error {
	m.once.Do(func() {
		close(m.entered)
		<-m.release
	})

	return m.msg.Ack()
}

// An acknowledgement held after the batch's registration gives no completion: the pass settles only once it is sent.
func TestPassSettledWaitsForTheAcknowledgements(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		rec := &passes{}
		g := passIngester(p, rec)
		entered, release, once := make(chan struct{}), make(chan struct{}), &sync.Once{}
		feedCapture(t, p, tracepack.UUID{1}, 3, func() Msg { return &heldAck{entered: entered, release: release, once: once} })
		done := make(chan error, 1)
		go func() { done <- g.flush(t.Context(), p.b.All()) }()
		<-entered // registered, its first acknowledgement held
		if ks := storedKeys(t, p.st); len(ks) != 1 {
			t.Fatalf("objects %v at the held acknowledgement, want the segment registered", ks)
		}
		if events, _, _ := rec.read(); len(events) != 0 {
			t.Fatalf("signals %v while an acknowledgement is held, want none", events)
		}
		released := time.Now()
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		events, errs, at := rec.read()
		if len(events) != 1 || errs[0] != nil || at[0].Before(released) {
			t.Errorf("signals %v %v at %v, want one without failure after the release at %v", events, errs, at, released)
		}
	})
}

// A pass of several batches gives one completion, after its last acknowledgement; a failed batch marks it, and an empty pass gives none.
func TestPassSettledOncePerPass(t *testing.T) {
	t.Parallel()
	refused := errors.New("refused after registration")
	for name, tc := range map[string]struct {
		fail    bool
		wantErr bool
	}{
		"settled": {false, false},
		"failed":  {true, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				p := newPipeline(t, c)
				rec := &passes{}
				g := passIngester(p, rec)
				registered := 0
				p.f.hooks[hookAfterRegister] = func() error {
					registered++
					if tc.fail && registered == 1 { // the first batch fails, the second is acknowledged
						return refused
					}

					return nil
				}
				p.f.hooks[hookAfterAck] = func() error { rec.event("ack"); return nil }
				for _, capture := range []tracepack.UUID{{1}, {2}, {3}} {
					feedCapture(t, p, capture, 2, func() Msg { return &msg{} })
				}
				batches := p.b.All()
				if len(batches) != 3 {
					t.Fatalf("%d batches, want 3", len(batches))
				}
				err := g.flush(t.Context(), batches)
				if (err != nil) != tc.wantErr {
					t.Fatalf("flush = %v, want failed %v", err, tc.wantErr)
				}
				events, errs, _ := rec.read()
				acks := 3 * 3
				if tc.fail {
					acks = 2 * 3
				}
				want := append(slices.Repeat([]string{"ack"}, acks), "settled")
				if !slices.Equal(events, want) {
					t.Errorf("events %v, want %d acknowledgements then one completion", events, acks)
				}
				if len(errs) == 1 && (errs[0] != nil) != tc.wantErr {
					t.Errorf("the completion's failure %v, want failed %v", errs[0], tc.wantErr)
				}
				if err := g.flush(t.Context(), p.b.All()); err != nil {
					t.Fatal(err)
				}
				if events, _, _ := rec.read(); len(events) != len(want) {
					t.Errorf("an empty pass signalled: %v", events[len(want):])
				}
			})
		})
	}
}
