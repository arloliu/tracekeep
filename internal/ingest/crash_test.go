package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// The crash points of ingest.md §6.3, each held at a flush hook placed between the two side effects it splits.

var errCrash = errors.New("crash")

// traffic is a capture's start and three records, as published; delivering it again is a redelivery.
func traffic(t *testing.T, capture tracepack.UUID) []Message {
	t.Helper()
	msgs := []Message{inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp"))}
	for seq := uint64(1); seq <= 3; seq++ {
		msgs = append(msgs, inline(t, capture, dataRecord(seq, now+int64(seq)), nil))
	}

	return msgs
}

// deliver disposes and batches msgs and returns their handles.
func (p *pipeline) deliver(t *testing.T, msgs []Message) []*msg {
	t.Helper()
	hs := make([]*msg, len(msgs))
	for i, m := range msgs {
		hs[i] = p.feed(t, m)
	}

	return hs
}

// flushOne flushes the one open batch and returns the flush's error.
func (p *pipeline) flushOne(t *testing.T) (Flushed, error) {
	t.Helper()
	bs := p.b.All()
	if len(bs) != 1 {
		t.Fatalf("%d batches open, want one", len(bs))
	}

	return p.f.Flush(t.Context(), bs[0])
}

func untouched(t *testing.T, hs []*msg) {
	t.Helper()
	for i, h := range hs {
		if got := h.done(); len(got) != 0 {
			t.Errorf("message %d: %v, want untouched (redelivered after the ack wait)", i, got)
		}
	}
}

// failingPut fails its first put, as a crash before the object exists would.
type failingPut struct {
	store.ObjectStore
	failed bool
}

func (f *failingPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if !f.failed {
		f.failed = true
		return errCrash
	}

	return f.ObjectStore.Put(ctx, key, r, size)
}

// A crash before the put leaves nothing: the redelivery writes the batch.
func TestCrashBeforeThePut(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.Store = &failingPut{ObjectStore: p.st}
		capture := tracepack.UUID{1}
		hs := p.deliver(t, traffic(t, capture))
		if _, err := p.flushOne(t); !errors.Is(err, errCrash) {
			t.Fatalf("Flush = %v, want the crash", err)
		}
		untouched(t, hs)
		if ks := keys(t, p.st); len(ks) != 0 {
			t.Fatalf("objects %v after a crash before the put", ks)
		}
		p.deliver(t, traffic(t, capture))
		if _, err := p.flushOne(t); err != nil {
			t.Fatal(err)
		}
		if ks := keys(t, p.st); len(ks) != 1 {
			t.Errorf("objects %v after the redelivery, want one segment", ks)
		}
	})
}

// A crash after the put, before the registration, leaves an orphan and the messages unacknowledged;
// the redelivery writes a second segment under its own pack id, whose records are byte-identical to the orphan's (G5-214).
func TestCrashAfterThePut(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.hooks[hookAfterPut] = func() error { return errCrash }
		capture := tracepack.UUID{2}
		hs := p.deliver(t, traffic(t, capture))
		if _, err := p.flushOne(t); !errors.Is(err, errCrash) {
			t.Fatalf("Flush = %v, want the crash", err)
		}
		untouched(t, hs)
		orphan := keys(t, p.st)
		if len(orphan) != 1 {
			t.Fatalf("objects %v, want the orphan", orphan)
		}
		if got, _ := c.Capture(t.Context(), capture); got.HasHighestSeq {
			t.Fatalf("the orphan was registered: %+v", got)
		}

		p.f.hooks[hookAfterPut] = nil
		p.deliver(t, traffic(t, capture))
		fl, err := p.flushOne(t)
		if err != nil {
			t.Fatal(err)
		}
		ks := keys(t, p.st)
		if len(ks) != 2 || !strings.Contains(strings.Join(ks, " "), fl.Packs[0].String()) {
			t.Fatalf("objects %v, want the orphan and the redelivery's segment %s", ks, fl.Packs[0])
		}
		second := slices.DeleteFunc(slices.Clone(ks), func(k string) bool { return k == orphan[0] })[0]
		a, b := records(t, p.st, orphan[0]), records(t, p.st, second)
		if len(a) != len(b) {
			t.Fatalf("%d records against %d", len(a), len(b))
		}
		for i := range a {
			if !sameRecord(&a[i], &b[i]) {
				t.Errorf("record %d differs between the orphan and the redelivery: %+v against %+v", i, a[i], b[i])
			}
		}
	})
}

// A crash during the acknowledgements leaves a registered segment and some messages unacknowledged;
// their redelivery writes them again into a segment the catalog marks a suspect against the first.
func TestCrashDuringTheAcks(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		acks := 0
		p.f.hooks[hookAfterAck] = func() error {
			if acks++; acks == 2 {
				return errCrash
			}
			return nil
		}
		capture := tracepack.UUID{3}
		msgs := traffic(t, capture)
		hs := p.deliver(t, msgs)
		if _, err := p.flushOne(t); !errors.Is(err, errCrash) {
			t.Fatalf("Flush = %v, want the crash", err)
		}
		untouched(t, hs[2:])

		p.f.hooks[hookAfterAck] = nil
		rest := p.deliver(t, msgs[2:])
		if _, err := p.flushOne(t); err != nil {
			t.Fatal(err)
		}
		for i, h := range rest {
			if got := h.done(); !slices.Equal(got, []string{"ack"}) {
				t.Errorf("redelivered message %d: %v, want acknowledged", i, got)
			}
		}
		cs, err := c.Conflicts(t.Context(), capture)
		if err != nil || len(cs) != 1 || cs[0].State != "suspect" || cs[0].SeqFrom != 2 || cs[0].SeqTo != 3 {
			t.Errorf("conflicts %+v, %v; want the redelivered seqs 2-3 a suspect", cs, err)
		}
	})
}

// A batch the Writer refuses three times is deterministic: the write-failure alert is raised and its messages stay deferred.
func TestWriteFailureThreeTimes(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.hooks[hookWrite] = func() error { return errors.New("refused") }
		var logged bytes.Buffer
		g := &Ingester{Flusher: p.f, Batcher: p.b, Log: slog.New(slog.NewTextHandler(&logged, nil))}
		capture := tracepack.UUID{4}
		var last []*msg
		for try := 1; try <= writeFailureLimit; try++ {
			last = p.deliver(t, traffic(t, capture))
			if err := g.flush(t.Context(), p.b.All()); !errors.Is(err, errWrite) {
				t.Fatalf("try %d: flush = %v, want the Writer's refusal", try, err)
			}
			alerted := strings.Contains(logged.String(), ReasonWriteFailure)
			if alerted != (try == writeFailureLimit) {
				t.Errorf("try %d: write-failure alert %v", try, alerted)
			}
		}
		for i, h := range last {
			if got := h.done(); !slices.Equal(got, []string{"nak " + lastNakDelay.String()}) {
				t.Errorf("message %d after the third refusal: %v, want deferred with the long delay", i, got)
			}
		}
	})
}

// sameRecord compares two records as stored, payloads included.
func sameRecord(a, b *tracepack.Record) bool { return reflect.DeepEqual(a, b) }
