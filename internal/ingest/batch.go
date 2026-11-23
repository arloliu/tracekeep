package ingest

import (
	"bytes"
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// Msg is the acknowledgement handle of one delivery; a jetstream.Msg is one.
type Msg interface {
	// Ack acknowledges the message.
	Ack() error
	// DoubleAck acknowledges the message and waits for the server to confirm it.
	DoubleAck(ctx context.Context) error
	// NakWithDelay returns the message for redelivery after delay.
	NakWithDelay(delay time.Duration) error
	// InProgress resets the message's ack wait.
	InProgress() error
	// Term terminates the message: it is never redelivered.
	Term() error
}

// Entry is a delivery waiting in a batch, with its message.
type Entry struct {
	D   *Delivery
	Msg Msg
}

// Key names a batch: a capture and the UTC hour of its records (ingest.md §6.2), the hour of each record's own time.
type Key struct {
	Capture tracepack.UUID
	Hour    int64
}

// Batch holds the records of one (capture, hour) until it is flushed.
type Batch struct {
	Key      Key
	ConfigID int64
	Entries  []Entry
	// Bytes counts the payload bytes held.
	Bytes int
	// MetaBytes estimates the encoded size of the redaction entries held, which the segment's metadata carries.
	MetaBytes int
	opened    time.Time
}

// parked reports a batch of parked copies, the parked consumer's: a consumer batches only its own deliveries.
func (b *Batch) parked() bool { return len(b.Entries) > 0 && b.Entries[0].D.Msg.Parked }

// DefaultBatchMetaBytes is half the reader's metadata budget, which every reader of a pack opens with:
// the estimate's margin, and room for the rest of the metadata.
const DefaultBatchMetaBytes = tracepack.DefaultMaxPackMetadataLen / 2

// RedactionBytes bounds the encoded size of entries, as a batch counts its metadata against the reader's budget (ingest.md §6.2).
func RedactionBytes(entries []tracepack.RedactionEntry) int {
	n := 0
	for i := range entries {
		n += redactionEntryBytes(&entries[i])
	}

	return n
}

// redactionEntryBytes bounds the encoded size of one redaction entry:
// each of its six fields and the entry itself is an 8-byte TLV header, the seq 8 bytes and each masked range 16,
// with 8 bytes over for an array's own length.
func redactionEntryBytes(e *tracepack.RedactionEntry) int {
	n := 72 + len(e.Domain) + len(e.Digest) + len(e.SIV) + 16*len(e.MaskedRanges) + len(e.RawMaskedRanges)
	if e.ItemPath != nil {
		n += len(*e.ItemPath)
	}

	return n
}

// inOrder sorts batches by capture and hour, so the ingester, flushing them one at a time, registers a capture's earlier hour first;
// the sort is stable, keeping a closed batch before the batch its key opened after it.
func inOrder(batches []*Batch) []*Batch {
	slices.SortStableFunc(batches, func(x, y *Batch) int {
		return cmp.Or(bytes.Compare(x.Key.Capture[:], y.Key.Capture[:]), cmp.Compare(x.Key.Hour, y.Key.Hour))
	})

	return batches
}

// Batcher holds the open batches; it is driven by its caller and starts nothing of its own.
type Batcher struct {
	// Interval is F: a capture's batches are due together this long after the first record of its earliest open batch arrived,
	// so none waits longer than F and an hour's change moves no capture's flush time (ingest.md §6.2).
	Interval time.Duration
	// MaxBytes closes a batch, due at once, when a record takes it to this many payload bytes:
	// its caller flushes it before batching another (ingest.md §6.2), so no fetch is held whole.
	MaxBytes int
	// MaxMetaBytes closes a batch before the record whose redaction entries would take its MetaBytes past it,
	// so a segment's metadata stays within the reader's budget (ingest.md §6.2).
	MaxMetaBytes int
	batches      map[Key]*Batch
	// closed holds the batches MaxMetaBytes and MaxBytes closed, and their captures' batches of an earlier hour, due at once.
	closed []*Batch
}

// NewBatcher returns a batcher with no open batch.
func NewBatcher(interval time.Duration, maxBytes int) *Batcher {
	return &Batcher{Interval: interval, MaxBytes: maxBytes, MaxMetaBytes: DefaultBatchMetaBytes, batches: map[Key]*Batch{}}
}

// HourOf is the UTC hour, in hours since the Unix epoch, of a record at ts (UTC nanoseconds).
func HourOf(ts int64) int64 {
	return tracepack.HourOf(time.Unix(0, ts))
}

// Add puts e in its batch, opened at now if it is the first
// or if e's redaction entries would take the open one past MaxMetaBytes, which closes that one;
// a batch e takes to MaxBytes is closed too.
func (b *Batcher) Add(e Entry, now time.Time) {
	k := Key{Capture: e.D.Raw.Version.Capture, Hour: HourOf(e.D.Raw.TS)}
	meta := 0
	for i := range e.D.Redaction {
		meta += redactionEntryBytes(&e.D.Redaction[i])
	}
	bt := b.batches[k]
	if bt != nil && bt.MetaBytes+meta > b.MaxMetaBytes {
		b.close(bt)
		bt = nil
	}
	if bt == nil {
		bt = &Batch{Key: k, ConfigID: e.D.ConfigID, opened: now}
		b.batches[k] = bt
	}
	bt.Entries = append(bt.Entries, e)
	bt.Bytes += len(e.D.Record.Record.Payload)
	bt.MetaBytes += meta
	if bt.Bytes >= b.MaxBytes {
		b.close(bt)
		delete(b.batches, k)
	}
}

// Closed removes and returns the batches MaxMetaBytes and MaxBytes closed, with their captures' batches of an earlier hour,
// ordered by capture and hour.
func (b *Batcher) Closed() []*Batch {
	closed := b.closed
	b.closed = nil

	return inOrder(closed)
}

// Due removes and returns the batches to flush at now, ordered by capture and hour:
// those closed, and every batch of a capture whose earliest open batch has been open for the interval.
func (b *Batcher) Due(now time.Time) []*Batch {
	earliest := map[tracepack.UUID]time.Time{}
	for k, bt := range b.batches {
		if t, ok := earliest[k.Capture]; !ok || bt.opened.Before(t) {
			earliest[k.Capture] = bt.opened
		}
	}
	due := b.closed
	b.closed = nil
	for k, bt := range b.batches {
		if now.Sub(earliest[k.Capture]) >= b.Interval {
			due = append(due, bt)
			delete(b.batches, k)
		}
	}

	return inOrder(due)
}

// All removes and returns every open batch, for a flush at shutdown, ordered by capture and hour.
func (b *Batcher) All() []*Batch {
	all := append(make([]*Batch, 0, len(b.closed)+len(b.batches)), b.closed...)
	b.closed = nil
	for k, bt := range b.batches {
		all = append(all, bt)
		delete(b.batches, k)
	}

	return inOrder(all)
}

// Held calls f for every record a batch holds: in-progress acks keep their messages from redelivery
// while the batch waits (architecture.md §3.1).
func (b *Batcher) Held(f func(Entry)) {
	for _, bt := range b.closed {
		for _, e := range bt.Entries {
			f(e)
		}
	}
	for _, bt := range b.batches {
		for _, e := range bt.Entries {
			f(e)
		}
	}
}

// close makes bt due at once, and with it its capture's open batches of an earlier hour,
// so a later hour closed early is flushed after its capture's earlier hour, not F before it.
func (b *Batcher) close(bt *Batch) {
	b.closed = append(b.closed, bt)
	for k, o := range b.batches {
		if k.Capture == bt.Key.Capture && k.Hour < bt.Key.Hour {
			b.closed = append(b.closed, o)
			delete(b.batches, k)
		}
	}
}
