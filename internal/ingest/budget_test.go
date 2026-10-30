package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/policy"
)

// baselineCarriers are the baseline's carriers, every one masked.
func baselineCarriers(t *testing.T) []maskingtest.Carrier {
	t.Helper()
	b := policy.Baseline()
	carriers, err := maskingtest.Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}

	return carriers
}

// A batch closes before the record whose redaction entries would take its metadata past the batcher's budget:
// the records are written as several segments, each holding its records' entries, and none is lost.
func TestMetadataBudgetClosesABatch(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := maskingPipeline(t, c, maskingKeys(t, c))
		p.b.MaxMetaBytes = 1024
		carriers := baselineCarriers(t)
		feedCarriers(t, p, tracepack.UUID{0x61}, carriers)
		n := len(carriers) + 1
		held := 0
		p.b.Held(func(Entry) { held++ })
		if held != n {
			t.Fatalf("the batcher holds %d records, want %d: a closed batch's records are held too", held, n)
		}
		p.flushAll(t)
		keys := storedKeys(t, p.st)
		if len(keys) < 2 {
			t.Fatalf("packs %q, want the batch split into several segments", keys)
		}
		stored := 0
		for _, k := range keys {
			recs := records(t, p.st, k)
			stored += len(recs)
			in := map[uint64]bool{}
			for _, r := range recs {
				in[r.Seq] = true
			}
			meta := header(t, p, k).Meta
			if len(recs) > 1 && meta.Redaction == nil {
				t.Errorf("%s: no redaction entries", k)
			}
			for _, e := range meta.Redaction {
				if !in[e.Seq] {
					t.Errorf("%s: an entry of seq %d, which the pack does not hold", k, e.Seq)
				}
			}
		}
		if stored != n {
			t.Errorf("%d records stored, want %d", stored, n)
		}
	})
}

// A closed batch is due at once, and its key opens a new batch for the records that follow.
func TestBatcherClosesAtItsBudget(t *testing.T) {
	t.Parallel()
	b := NewBatcher(time.Minute, 1<<20)
	b.MaxMetaBytes = 2 * redactionEntryBytes(&tracepack.RedactionEntry{Domain: "d"})
	at := time.Unix(0, now)
	add := func(seq uint64, masks int) {
		d := &Delivery{Raw: Raw{Version: Version{Capture: tracepack.UUID{1}, Seq: seq}, TS: now}}
		for range masks {
			d.Redaction = append(d.Redaction, tracepack.RedactionEntry{Seq: seq, Domain: "d"})
		}
		b.Add(Entry{D: d, Msg: &msg{}}, at)
	}
	add(1, 1)
	add(2, 1)
	if due := b.Due(at); len(due) != 0 {
		t.Fatalf("%d batches due at the budget, want none", len(due))
	}
	add(3, 1)
	add(4, 0)
	due := b.Due(at)
	if len(due) != 1 || len(due[0].Entries) != 2 {
		t.Fatalf("due %+v, want the batch of seqs 1 and 2", due)
	}
	// A record alone over the budget opens its own batch, which the Writer then refuses.
	add(5, 3)
	all := b.All()
	if len(all) != 2 {
		t.Fatalf("%d batches left, want seqs 3-4 and seq 5 apart", len(all))
	}
}

// A segment whose metadata exceeds the reader's budget is refused, deterministically, rather than retried as a transient failure.
func TestOverBudgetSegmentIsRefused(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := maskingPipeline(t, c, maskingKeys(t, c))
		p.f.Reader.MaxPackMetadataLen = 1024
		feedCarriers(t, p, tracepack.UUID{0x62}, baselineCarriers(t))
		for _, b := range p.b.All() {
			_, err := p.f.Flush(t.Context(), b)
			if !errors.Is(err, errWrite) || !errors.Is(err, tracepack.ErrReadLimit) {
				t.Fatalf("flush = %v, want the reader budget as a refusal", err)
			}
		}
		if keys := storedKeys(t, p.st); len(keys) != 0 {
			t.Errorf("packs %q stored, want none", keys)
		}
	})
}

// part parts b into two batches of its key, those of its entries in held and the rest, as the metadata budget's close leaves siblings.
func part(b *Batch, held func(seq uint64) bool) (in, out *Batch) {
	in, out = &Batch{Key: b.Key, ConfigID: b.ConfigID}, &Batch{Key: b.Key, ConfigID: b.ConfigID}
	for _, e := range b.Entries {
		if held(e.D.Raw.Version.Seq) {
			in.Entries = append(in.Entries, e)
		} else {
			out.Entries = append(out.Entries, e)
		}
	}

	return in, out
}

// refuseSeq makes the Writer refuse every segment holding seq.
func refuseSeq(f *Flusher, seq uint64) {
	f.check = func(s *segment) error {
		for _, e := range s.entries {
			if e.D.Raw.Version.Seq == seq {
				return fmt.Errorf("refused: %w", tracepack.ErrInvalidRecord)
			}
		}

		return nil
	}
}

// Refusals are counted by the records refused: a sibling batch of the same key that succeeds, flushed before or after,
// leaves the refused records' count alone, so they reach the write-failure alert at their third refusal though fresh records join them.
func TestRefusalsCountByRecord(t *testing.T) {
	t.Parallel()
	for _, refusedFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("refused first %v", refusedFirst), func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				p := newPipeline(t, c)
				refuseSeq(p.f, 1)
				var logged bytes.Buffer
				g := p.ingester(slog.New(slog.NewTextHandler(&logged, nil)))
				capture := tracepack.UUID{0x63}
				msgs := traffic(t, capture)
				for try := 1; try <= writeFailureLimit; try++ {
					hs := p.deliver(t, msgs)
					batches := p.b.All()
					if len(batches) != 1 {
						t.Fatalf("try %d: %d batches, want one", try, len(batches))
					}
					refused, fresh := part(batches[0], func(seq uint64) bool { return seq <= 1 })
					order := []*Batch{refused, fresh}
					if !refusedFirst {
						order = []*Batch{fresh, refused}
					}
					if err := g.flush(t.Context(), order); !errors.Is(err, errWrite) {
						t.Fatalf("try %d: flush = %v, want the refusal", try, err)
					}
					if alerted := strings.Contains(logged.String(), ReasonWriteFailure); alerted != (try == writeFailureLimit) {
						t.Fatalf("try %d: write-failure alert %v", try, alerted)
					}
					for i, h := range hs {
						got := h.done()
						switch {
						case i > 1 && !slices.Equal(got, []string{"ack"}):
							t.Errorf("try %d: fresh message %d: %v, want acknowledged", try, i, got)
						case i <= 1 && try == writeFailureLimit && !slices.Equal(got, []string{"nak " + lastNakDelay.String()}):
							t.Errorf("message %d after the third refusal: %v, want deferred", i, got)
						default:
						}
					}
					// The refused records come back with a fresh record of the live capture.
					seq := uint64(3 + try)
					msgs = append(msgs[:2:2], inline(t, capture, dataRecord(seq, now+int64(seq)), nil))
				}
			})
		})
	}
}

// Siblings refused once each in one flush are each refused once: no record of theirs reached the limit.
func TestSiblingRefusalsInOneFlush(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		p.f.check = func(*segment) error { return fmt.Errorf("refused: %w", tracepack.ErrInvalidRecord) }
		var logged bytes.Buffer
		g := p.ingester(slog.New(slog.NewTextHandler(&logged, nil)))
		p.deliver(t, traffic(t, tracepack.UUID{0x64}))
		b := p.b.All()[0]
		first, rest := part(b, func(seq uint64) bool { return seq == 0 })
		second, third := part(rest, func(seq uint64) bool { return seq == 1 })
		if err := g.flush(t.Context(), []*Batch{first, second, third}); !errors.Is(err, errWrite) {
			t.Fatalf("flush = %v, want the refusals", err)
		}
		if strings.Contains(logged.String(), ReasonWriteFailure) {
			t.Error("three siblings refused once each raised the write-failure alert")
		}
	})
}

// A flush that registered its segment and acknowledged part of its batch forgets the refusals of the records it acknowledged,
// and keeps those of the rest, whose messages return.
func TestAcknowledgedRecordsForgetTheirRefusals(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		p.deliver(t, traffic(t, tracepack.UUID{0x65}))
		b := p.b.All()[0]
		g.refused(b.Entries)
		acks := 0
		p.f.hooks[hookAfterAck] = func() error {
			if acks++; acks == 2 {
				return errCrash
			}

			return nil
		}
		if err := g.flush(t.Context(), []*Batch{b}); !errors.Is(err, errCrash) {
			t.Fatalf("flush = %v, want the crash after two acknowledgements", err)
		}
		for i, e := range b.Entries {
			_, kept := g.writeFailures[e.D.Raw.Version]
			if kept != (i >= 2) {
				t.Errorf("entry %d: refusals kept %v, want them forgotten once acknowledged", i, kept)
			}
		}
	})
}

// refusedDigest is the payload digest of m's record.
func refusedDigest(t *testing.T, m Message) [32]byte {
	t.Helper()
	raw, _ := ReadRaw(&m.Env, m.Body, 0)

	return raw.Version.Digest
}

// Two versions of one seq in sibling batches are deferred and settled each on its own:
// the one that registers leaves the other's write-failure deferral counted, flushed before or after it.
func TestSiblingVersionsKeepTheirDeferrals(t *testing.T) {
	t.Parallel()
	for _, refusedFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("refused first %v", refusedFirst), func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				p := newPipeline(t, c)
				g := p.ingester(slog.New(slog.DiscardHandler))
				s := metrics.NewServer()
				m, err := metrics.NewIngest(s, func() time.Time { return time.Unix(0, now) })
				if err != nil {
					t.Fatal(err)
				}
				g.Metrics = m
				capture := tracepack.UUID{0x66}
				other := otherVersion(t, capture, 2)
				var refusedVersion Version
				p.f.check = func(seg *segment) error {
					for _, e := range seg.entries {
						if e.D.Raw.Version == refusedVersion {
							return fmt.Errorf("refused: %w", tracepack.ErrInvalidRecord)
						}
					}

					return nil
				}
				start := traffic(t, capture)
				p.deliver(t, start[:1])
				if err := g.flush(t.Context(), p.b.All()); err != nil {
					t.Fatal(err)
				}
				for try := 1; try <= writeFailureLimit; try++ {
					p.deliver(t, []Message{other})
					if try == writeFailureLimit {
						// Seq 2's other version arrives too, in a sibling batch of the same key that registers.
						p.deliver(t, start[1:])
					}
					b := p.b.All()[0]
					refused := &Batch{Key: b.Key, ConfigID: b.ConfigID}
					fresh := &Batch{Key: b.Key, ConfigID: b.ConfigID}
					for _, e := range b.Entries {
						if e.D.Raw.Version.Digest == refusedDigest(t, other) {
							refused.Entries = append(refused.Entries, e)
						} else {
							fresh.Entries = append(fresh.Entries, e)
						}
					}
					refusedVersion = refused.Entries[0].D.Raw.Version
					order := []*Batch{refused}
					if len(fresh.Entries) > 0 {
						order = []*Batch{refused, fresh}
						if !refusedFirst {
							order = []*Batch{fresh, refused}
						}
					}
					if err := g.flush(t.Context(), order); !errors.Is(err, errWrite) {
						t.Fatalf("try %d: flush = %v, want the refusal", try, err)
					}
				}
				want := fmt.Sprintf(`tracekeep_ingest_deferred_messages{reason="%s"} 1`, ReasonWriteFailure)
				if b := scrape(t, s); !strings.Contains(b, want) {
					t.Errorf("/metrics lacks %s: the registered version settled the refused one's deferral", want)
				}
			})
		})
	}
}
