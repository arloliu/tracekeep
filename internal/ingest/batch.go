package ingest

import (
	"context"
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

// DefaultBatchMetaBytes is half the reader's metadata budget, which every reader of a pack opens with:
// the estimate's margin, and room for the rest of the metadata.
const DefaultBatchMetaBytes = tracepack.DefaultMaxPackMetadataLen / 2

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

// Batcher holds the open batches; it is driven by its caller and starts nothing of its own.
type Batcher struct {
	// Interval is F: a batch is due this long after its first record arrived.
	Interval time.Duration
	// MaxBytes makes a batch due once it holds this many payload bytes.
	MaxBytes int
	// MaxMetaBytes closes a batch before the record whose redaction entries would take its MetaBytes past it,
	// so a segment's metadata stays within the reader's budget (ingest.md §6.2).
	MaxMetaBytes int
	batches      map[Key]*Batch
	// closed holds the batches MaxMetaBytes closed, due at once.
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
// or if e's redaction entries would take the open one past MaxMetaBytes, which closes that one.
func (b *Batcher) Add(e Entry, now time.Time) {
	k := Key{Capture: e.D.Raw.Version.Capture, Hour: HourOf(e.D.Raw.TS)}
	meta := 0
	for i := range e.D.Redaction {
		meta += redactionEntryBytes(&e.D.Redaction[i])
	}
	bt := b.batches[k]
	if bt != nil && bt.MetaBytes+meta > b.MaxMetaBytes {
		b.closed = append(b.closed, bt)
		bt = nil
	}
	if bt == nil {
		bt = &Batch{Key: k, ConfigID: e.D.ConfigID, opened: now}
		b.batches[k] = bt
	}
	bt.Entries = append(bt.Entries, e)
	bt.Bytes += len(e.D.Record.Record.Payload)
	bt.MetaBytes += meta
}

// Due removes and returns the batches to flush at now:
// those MaxMetaBytes closed, those open for the interval, those holding MaxBytes, and those of a capture that has records of a later hour.
func (b *Batcher) Due(now time.Time) []*Batch {
	latest := map[tracepack.UUID]int64{}
	for k := range b.batches {
		latest[k.Capture] = max(latest[k.Capture], k.Hour)
	}
	due := b.closed
	b.closed = nil
	for k, bt := range b.batches {
		if now.Sub(bt.opened) >= b.Interval || bt.Bytes >= b.MaxBytes || k.Hour < latest[k.Capture] {
			due = append(due, bt)
			delete(b.batches, k)
		}
	}

	return due
}

// All removes and returns every open batch, for a flush at shutdown.
func (b *Batcher) All() []*Batch {
	all := append(make([]*Batch, 0, len(b.closed)+len(b.batches)), b.closed...)
	b.closed = nil
	for k, bt := range b.batches {
		all = append(all, bt)
		delete(b.batches, k)
	}

	return all
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
