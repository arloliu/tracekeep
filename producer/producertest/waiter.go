package producertest

import (
	"sync"
	"time"

	"github.com/arloliu/tracekeep/producer"
)

// Clock is a clock a test moves by hand; a Waiter's timers run on it.
type Clock struct {
	mu   sync.Mutex
	now  time.Duration
	subs []func(now time.Duration)
}

// Waiter is a fake response waiter, the producer.TimeoutSource a gateway's waiter implements,
// with its timers on a Clock (docs/specs/tracekeep/producer-sdk.md §4.7 *Send path*).
// A test ends a wait with Reply or Cancel, or times it out by advancing the clock.
type Waiter struct {
	clk   *Clock
	mu    sync.Mutex
	waits map[producer.Token]wait
}

type wait struct {
	timeout  time.Duration
	deadline time.Duration
}

var _ producer.TimeoutSource = (*Waiter)(nil)

// NewClock returns a clock at zero.
func NewClock() *Clock { return &Clock{} }

// NewWaiter returns a waiter whose timers run on clk.
func NewWaiter(clk *Clock) *Waiter {
	w := &Waiter{clk: clk, waits: make(map[producer.Token]wait)}
	clk.mu.Lock()
	clk.subs = append(clk.subs, w.fire)
	clk.mu.Unlock()

	return w
}

// Compete runs every report on t at once, each from its own goroutine, and returns when all have returned:
// the competing reports of producer.md §4.6, such as a reply racing a timeout or a send failure racing a reply.
func Compete(t producer.Token, reports ...func(producer.Token)) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, r := range reports {
		wg.Go(func() {
			<-start
			r(t)
		})
	}
	close(start)
	wg.Wait()
}

// Now returns how far the clock has moved.
func (c *Clock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

// Advance moves the clock by d; every wait whose timeout has passed times out before Advance returns.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	now, subs := c.now, c.subs
	c.mu.Unlock()
	for _, f := range subs {
		f(now)
	}
}

// Await opens the wait for t: it reports Awaiting, then exactly one terminal outcome when the wait ends.
func (w *Waiter) Await(t producer.Token, timeout time.Duration) {
	t.Awaiting(timeout)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waits[t] = wait{timeout: timeout, deadline: w.clk.Now() + timeout}
}

// Reply ends t's wait with a reply; false when t has no open wait.
func (w *Waiter) Reply(t producer.Token) bool {
	if !w.end(t) {
		return false
	}
	t.Replied()

	return true
}

// Cancel ends t's wait as the device stopping would; false when t has no open wait.
func (w *Waiter) Cancel(t producer.Token) bool {
	if !w.end(t) {
		return false
	}
	t.Cancelled()

	return true
}

func (w *Waiter) end(t producer.Token) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.waits[t]; !ok {
		return false
	}
	delete(w.waits, t)

	return true
}

// fire is the timer branch: every wait past its deadline times out with the timeout it was opened with.
func (w *Waiter) fire(now time.Duration) {
	w.mu.Lock()
	var due []producer.Token
	var timeouts []time.Duration
	for t, wt := range w.waits {
		if now >= wt.deadline {
			due, timeouts = append(due, t), append(timeouts, wt.timeout)
			delete(w.waits, t)
		}
	}
	w.mu.Unlock()
	for i, t := range due {
		t.TimedOut(timeouts[i])
	}
}
