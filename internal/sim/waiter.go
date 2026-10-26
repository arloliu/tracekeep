package sim

import (
	"sync"
	"time"

	"github.com/arloliu/tracekeep/producer"
)

// waiter is the simulator's response waiter, the role the gateway's waiter plays:
// it opens a wait for each forwarded request and ends it with the reply, its timer, or the device stopping.
type waiter struct {
	// after runs f once d has passed and returns a stop that reports whether it prevented f; time.AfterFunc by default.
	after func(d time.Duration, f func()) (stop func() bool)

	mu    sync.Mutex
	waits map[producer.Token]func() bool // each open wait's timer stop
}

var _ producer.TimeoutSource = (*waiter)(nil)

func newWaiter() *waiter {
	return &waiter{
		after: func(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop },
		waits: make(map[producer.Token]func() bool),
	}
}

// Await opens the wait for t: it reports Awaiting, then the reply, the timeout or the cancellation that ends it.
func (w *waiter) Await(t producer.Token, timeout time.Duration) {
	t.Awaiting(timeout)
	w.mu.Lock()
	defer w.mu.Unlock()
	// The timer's callback takes the lock, so it cannot end the wait before it is recorded.
	w.waits[t] = w.after(timeout, func() {
		if w.take(t) {
			t.TimedOut(timeout)
		}
	})
}

// reply ends t's wait with its reply; a t with no open wait is left alone.
func (w *waiter) reply(t producer.Token) {
	if w.take(t) {
		t.Replied()
	}
}

// cancel ends t's wait as the device stopping does; a t with no open wait is left alone.
func (w *waiter) cancel(t producer.Token) {
	if w.take(t) {
		t.Cancelled()
	}
}

// cancelAll ends every open wait as the device stopping does.
func (w *waiter) cancelAll() {
	w.mu.Lock()
	toks := make([]producer.Token, 0, len(w.waits))
	for t := range w.waits {
		toks = append(toks, t)
	}
	w.mu.Unlock()
	for _, t := range toks {
		w.cancel(t)
	}
}

// take removes t's open wait and stops its timer; it reports whether t had one.
func (w *waiter) take(t producer.Token) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	stop, ok := w.waits[t]
	if !ok {
		return false
	}
	delete(w.waits, t)
	stop()

	return true
}
