package logfeed

import (
	"cmp"
	"slices"
	"time"
)

// Disposition is what a file's window does with an entry it is given (logfeed.md §15.5).
type Disposition int

const (
	// Held: the entry waits in the window.
	Held Disposition = iota + 1
	// Duplicate: an entry at its offset is held; the caller acknowledges both once the held one is registered.
	Duplicate
	// Stale: at or below the file's watermark, below an entry already released, or of a stopped file; acknowledged at once, never converted.
	Stale
)

// Window is one file's reorder window: it holds a file's entries until each has waited, then releases them in offset order (logfeed.md §15.5 *Order*),
// and keeps the released ones until a registered flush settles them, so a redelivery of one is a duplicate, not stale.
// It is not safe for concurrent use: one converter drives a file.
type Window[T any] struct {
	wait time.Duration
	// released is the highest offset released, the file's watermark at first, the stale bound; stopped refuses every entry.
	released int64
	stopped  bool
	held     []waiting[T]
	// unsettled are the released entries no registered flush holds yet, by offset.
	unsettled map[int64]T
}

type waiting[T any] struct {
	offset  int64
	arrived time.Time
	item    T
}

// NewWindow is a window of wait over a file whose watermark is watermark, catalog.NoWatermark for a new file.
func NewWindow[T any](wait time.Duration, watermark int64) *Window[T] {
	return &Window[T]{wait: wait, released: watermark, unsettled: map[int64]T{}}
}

// Add gives the window item at offset, arrived at; a duplicate's held or unsettled item is returned with it.
func (w *Window[T]) Add(offset int64, arrived time.Time, item T) (Disposition, T) {
	var zero T
	if held, ok := w.unsettled[offset]; ok {
		return Duplicate, held
	}
	if w.stopped || offset <= w.released {
		return Stale, zero
	}
	i, found := slices.BinarySearchFunc(w.held, offset, func(h waiting[T], o int64) int { return cmp.Compare(h.offset, o) })
	if found {
		return Duplicate, w.held[i].item
	}
	w.held = slices.Insert(w.held, i, waiting[T]{offset: offset, arrived: arrived, item: item})

	return Held, zero
}

// Release returns, in offset order, the held entries from the lowest that have each waited the window at now,
// stopping at the first that has not, which holds the ones above it.
func (w *Window[T]) Release(now time.Time) []T {
	n := 0
	for n < len(w.held) && now.Sub(w.held[n].arrived) >= w.wait {
		n++
	}

	return w.take(n)
}

// Drain releases every held entry, as a file's stop does once its successor's first entry has left its own window (§15.6 *Boundaries*).
func (w *Window[T]) Drain() []T { return w.take(len(w.held)) }

func (w *Window[T]) take(n int) []T {
	if n == 0 {
		return nil
	}
	out := make([]T, n)
	for i := range n {
		out[i] = w.held[i].item
		w.unsettled[w.held[i].offset] = w.held[i].item
	}
	w.released = w.held[n-1].offset
	w.held = slices.Delete(w.held, 0, n)

	return out
}

// Settle forgets the released entries a registered flush settled, at or below watermark; a redelivery of one is then stale.
func (w *Window[T]) Settle(watermark int64) {
	for o := range w.unsettled {
		if o <= watermark {
			delete(w.unsettled, o)
		}
	}
}

// Held is the number of entries in the window.
func (w *Window[T]) Held() int { return len(w.held) }

// Stop refuses every later first arrival of the file as stale; a redelivery of an unsettled entry is still its duplicate.
func (w *Window[T]) Stop() { w.stopped = true }
