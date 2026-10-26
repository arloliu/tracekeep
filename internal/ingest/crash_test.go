package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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

// failingPut fails its at-th put, counting from one, as a crash before the object exists would.
type failingPut struct {
	store.ObjectStore
	at, puts int
}

func (f *failingPut) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	if f.puts++; f.puts == f.at {
		return errCrash
	}

	return f.ObjectStore.Put(ctx, key, r, size)
}

// A crash before the put leaves nothing: the redelivery writes the batch.
func TestCrashBeforeThePut(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.Store = &failingPut{ObjectStore: p.st, at: 1}
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

// A failure during the acknowledgements leaves a registered segment and the rest of the messages nak'd, never the acknowledged ones;
// their redelivery writes them again into a segment the catalog marks a suspect against the first.
func TestCrashDuringTheAcks(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
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
		if err := g.flush(t.Context(), p.b.All()); !errors.Is(err, errCrash) {
			t.Fatalf("flush = %v, want the crash", err)
		}
		for i, h := range hs {
			want := []string{"ack"}
			if i >= 2 {
				want = []string{"nak " + firstNakDelay.String()}
			}
			if got := h.done(); !slices.Equal(got, want) {
				t.Errorf("message %d: %v, want %v", i, got, want)
			}
		}

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

// A batch the Writer refuses three times is deterministic: the write-failure alert is raised and its messages stay deferred,
// though records joined the batch between the tries.
func TestWriteFailureThreeTimes(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.hooks[hookWrite] = func() error { return fmt.Errorf("refused: %w", tracepack.ErrInvalidRecord) }
		var logged bytes.Buffer
		g := p.ingester(slog.New(slog.NewTextHandler(&logged, nil)))
		capture := tracepack.UUID{4}
		var last []*msg
		for try := 1; try <= writeFailureLimit; try++ {
			msgs := traffic(t, capture)
			for seq := uint64(4); seq < uint64(4+try); seq++ { // the capture is live
				msgs = append(msgs, inline(t, capture, dataRecord(seq, now+int64(seq)), nil))
			}
			last = p.deliver(t, msgs)
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

// A write that fails for any other reason than the Writer's validation, such as the working file's, is retried and never counted.
func TestWriteFailureOnlyCountsRefusals(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.hooks[hookWrite] = func() error { return errCrash }
		var logged bytes.Buffer
		g := p.ingester(slog.New(slog.NewTextHandler(&logged, nil)))
		capture := tracepack.UUID{5}
		for try := 1; try <= writeFailureLimit+1; try++ {
			hs := p.deliver(t, traffic(t, capture))
			if err := g.flush(t.Context(), p.b.All()); !errors.Is(err, errCrash) || errors.Is(err, errWrite) {
				t.Fatalf("try %d: flush = %v, want the failure, not a refusal", try, err)
			}
			if got := hs[0].done(); !slices.Equal(got, []string{"nak " + firstNakDelay.String()}) {
				t.Fatalf("try %d: %v, want retried with the short delay", try, got)
			}
		}
		if strings.Contains(logged.String(), ReasonWriteFailure) {
			t.Error("a transient failure raised the write-failure alert")
		}
	})
}

// The Writer refusing a conflict segment registers nothing: every segment is written before the first put.
func TestRefusedConflictSegmentRegistersNothing(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		writes := 0
		p.f.hooks[hookWrite] = func() error {
			if writes++; writes == 2 {
				return fmt.Errorf("refused: %w", tracepack.ErrInvalidRecord)
			}
			return nil
		}
		capture := tracepack.UUID{6}
		hs := p.deliver(t, append(traffic(t, capture), otherVersion(t, capture, 1)))
		if _, err := p.flushOne(t); !errors.Is(err, errWrite) {
			t.Fatalf("Flush = %v, want the refusal", err)
		}
		untouched(t, hs)
		if ks := keys(t, p.st); len(ks) != 0 {
			t.Errorf("objects %v after a refusal", ks)
		}
		if got, _ := c.Capture(t.Context(), capture); got.HasHighestSeq {
			t.Errorf("a segment was registered: %+v", got)
		}
	})
}

// A put failing for the conflict segment after the batch's segment registered leaves every message unacknowledged;
// the redelivery writes both again and every message is acknowledged.
func TestPutFailsAfterTheFirstSegment(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.Store = &failingPut{ObjectStore: p.st, at: 2}
		capture := tracepack.UUID{7}
		msgs := append(traffic(t, capture), otherVersion(t, capture, 1))
		hs := p.deliver(t, msgs)
		if _, err := p.flushOne(t); !errors.Is(err, errCrash) {
			t.Fatalf("Flush = %v, want the crash", err)
		}
		untouched(t, hs)
		if got, _ := c.Capture(t.Context(), capture); got.HighestSeq != 3 {
			t.Fatalf("highest seq %d, want the batch's segment registered", got.HighestSeq)
		}
		hs = p.deliver(t, msgs)
		fl, err := p.flushOne(t)
		if err != nil || len(fl.Packs) != 2 {
			t.Fatalf("Flush = %+v, %v; want a segment and a conflict segment", fl, err)
		}
		for i, h := range hs {
			if got := h.done(); !slices.Equal(got, []string{"ack"}) {
				t.Errorf("redelivered message %d: %v, want acknowledged", i, got)
			}
		}
	})
}

// A batch whose hour passed the retention horizon at its flush registers nothing, and each message is terminated with its audit row.
func TestFlushBeyondTheHorizon(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		capture := tracepack.UUID{8}
		hs := p.deliver(t, traffic(t, capture))
		p.f.HorizonHour = func() int64 { return HourOf(now) + 1 }
		if err := g.flush(t.Context(), p.b.All()); err != nil {
			t.Fatal(err)
		}
		for i, h := range hs {
			if got := h.done(); !slices.Equal(got, []string{"term"}) {
				t.Errorf("message %d: %v, want terminated", i, got)
			}
		}
		if ks := keys(t, p.st); len(ks) != 0 {
			t.Errorf("objects %v, want the segment deleted", ks)
		}
		if got, _ := c.Capture(t.Context(), capture); got.HasHighestSeq {
			t.Errorf("a segment was registered: %+v", got)
		}
	})
}

// ingester runs p's flushes on a clock that stands still.
func (p *pipeline) ingester(log *slog.Logger) *Ingester {
	at := time.Unix(0, now)

	return &Ingester{Executor: p.x, Flusher: p.f, Batcher: p.b, Log: log, Heartbeat: time.Hour, Now: func() time.Time { return at }}
}

// otherVersion is another version of a data record at seq.
func otherVersion(t *testing.T, capture tracepack.UUID, seq uint64) Message {
	t.Helper()
	r := dataRecord(seq, now+int64(seq))
	r.Payload = append(bytes.Clone(r.Payload), 0xff)

	return inline(t, capture, r, nil)
}

// sameRecord compares two records as stored, payloads included.
func sameRecord(a, b *tracepack.Record) bool { return reflect.DeepEqual(a, b) }
