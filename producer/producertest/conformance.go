package producertest

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/arloliu/tracekeep/producer"
)

const (
	// awaitBound is how long Await may take before it counts as blocking its caller.
	awaitBound = time.Second
	// outcomeBound is how long the check waits for a terminal outcome.
	outcomeBound = 5 * time.Second
	// quiet is how long the check watches for a report a source must not make, when the harness has no Quiesce.
	quiet = 100 * time.Millisecond
)

// TimeoutSourceHarness drives an integrator's producer.TimeoutSource for CheckTimeoutSource.
type TimeoutSourceHarness struct {
	// Source is the waiter under check.
	Source producer.TimeoutSource
	// Reply ends the wait opened for t with a reply, as the device's reply would.
	Reply func(t producer.Token)
	// Cancel ends the wait opened for t any other way, as the device stopping would.
	Cancel func(t producer.Token)
	// Expire lets d pass on the waiter's clock, so every wait opened with a timeout of at most d times out.
	Expire func(d time.Duration)
	// Quiesce, if set, returns once the source has made every report the harness's calls so far cause.
	// Without it the check watches for 100 ms, so a source that reports later than that can pass a duplicate.
	Quiesce func()
}

// CheckTimeoutSource checks a TimeoutSource against the send-path rules (producer-sdk.md §4.7, the sixth guarantee):
// Await returns at once and reports Awaiting with the wait's timeout before anything else,
// each wait ends in exactly one terminal outcome,
// and TimedOut carries the timeout the wait was opened with, so a later, shorter timeout applies to new waits only.
// It returns every violation it found, nil when there is none.
func CheckTimeoutSource(h TimeoutSourceHarness) error {
	if h.Source == nil || h.Reply == nil || h.Cancel == nil || h.Expire == nil {
		return errors.New("producertest: the harness needs Source, Reply, Cancel and Expire")
	}
	quiesce := h.Quiesce
	if quiesce == nil {
		quiesce = func() { time.Sleep(quiet) }
	}
	var errs []error
	timeouts := []time.Duration{30 * time.Second, 10 * time.Second}
	// open opens n waits, the second with a shorter timeout, as after a HotReload.
	open := func(name string, n int) []*recorder {
		toks := make([]*recorder, 0, n)
		for i := range n {
			t := newRecorder()
			if err := awaitPromptly(h.Source, t, timeouts[i]); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				return nil
			}
			toks = append(toks, t)
		}

		return toks
	}
	// ended checks that t's wait ended in want and that nothing more is reported on it, whatever happens to it.
	ended := func(name string, t *recorder, timeout time.Duration, want string) {
		if err := t.settle(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			return
		}
		h.Reply(t)
		h.Cancel(t)
		h.Expire(time.Minute)
		quiesce()
		if got, exp := t.calls(), []string{fmt.Sprintf("awaiting %v", timeout), want}; !slices.Equal(got, exp) {
			errs = append(errs, fmt.Errorf("%s: reports %q, want %q", name, got, exp))
		}
	}

	for _, sc := range []struct {
		name, want string
		end        func(t *recorder)
	}{
		{"reply", "replied", func(t *recorder) { h.Reply(t) }},
		{"cancel", "cancelled", func(t *recorder) { h.Cancel(t) }},
		{"timeout", "timed-out 30s", func(*recorder) { h.Expire(30 * time.Second) }},
	} {
		if toks := open(sc.name, 1); toks != nil {
			sc.end(toks[0])
			ended(sc.name, toks[0], timeouts[0], sc.want)
		}
	}

	// Each of two waits times out with its own timeout, the shorter first: a new timeout never moves an open wait.
	const two = "timeouts of two waits"
	if toks := open(two, 2); toks != nil {
		h.Expire(10 * time.Second)
		if err := toks[1].settle(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", two, err))
		}
		quiesce()
		if got := toks[0].calls(); len(got) != 1 {
			errs = append(errs, fmt.Errorf("%s: the 30s wait reports %q at 10s, want only Awaiting", two, got))
		}
		h.Expire(20 * time.Second)
		ended(two, toks[0], timeouts[0], "timed-out 30s")
		ended(two, toks[1], timeouts[1], "timed-out 10s")
	}

	return errors.Join(errs...)
}

func awaitPromptly(src producer.TimeoutSource, t *recorder, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		src.Await(t, timeout)
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(awaitBound):
		return fmt.Errorf("Await blocked its caller for %v", awaitBound)
	}
}

// recorder is a Token that records every report made on it.
type recorder struct {
	mu   sync.Mutex
	log  []string
	end  chan struct{}
	once sync.Once
}

var _ producer.Token = (*recorder)(nil)

func newRecorder() *recorder { return &recorder{end: make(chan struct{})} }

func (r *recorder) Sent()                    { r.add("sent", false) }
func (r *recorder) SendFailed()              { r.add("send-failed", false) }
func (r *recorder) SubmissionFailed()        { r.add("submission-failed", false) }
func (r *recorder) Awaiting(d time.Duration) { r.add(fmt.Sprintf("awaiting %v", d), false) }
func (r *recorder) Replied()                 { r.add("replied", true) }
func (r *recorder) TimedOut(d time.Duration) { r.add(fmt.Sprintf("timed-out %v", d), true) }
func (r *recorder) Cancelled()               { r.add("cancelled", true) }

func (r *recorder) add(s string, terminal bool) {
	r.mu.Lock()
	r.log = append(r.log, s)
	r.mu.Unlock()
	if terminal {
		r.once.Do(func() { close(r.end) })
	}
}

func (r *recorder) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.log)
}

// settle waits for the first terminal outcome.
func (r *recorder) settle() error {
	select {
	case <-r.end:
		return nil
	case <-time.After(outcomeBound):
		return fmt.Errorf("no terminal outcome within %v; reports %q", outcomeBound, r.calls())
	}
}
