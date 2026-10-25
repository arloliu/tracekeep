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
	// quiet is how long the check watches an ended wait for a report it must not make.
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
	var errs []error
	check := func(name string, want []string, end func(toks []*recorder)) {
		toks := []*recorder{newRecorder()}
		if len(want) > 1 {
			toks = append(toks, newRecorder())
		}
		timeouts := []time.Duration{30 * time.Second, 10 * time.Second}
		for i, t := range toks {
			if err := awaitPromptly(h.Source, t, timeouts[i]); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				return
			}
		}
		end(toks)
		for i, t := range toks {
			if err := t.settle(); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				continue
			}
			// A conforming source reports nothing more on an ended wait, whatever happens to it.
			h.Reply(t)
			h.Cancel(t)
			h.Expire(time.Minute)
			time.Sleep(quiet)
			exp := []string{fmt.Sprintf("awaiting %v", timeouts[i]), want[i]}
			if got := t.calls(); !slices.Equal(got, exp) {
				errs = append(errs, fmt.Errorf("%s: reports %q, want %q", name, got, exp))
			}
		}
	}

	check("reply", []string{"replied"}, func(toks []*recorder) { h.Reply(toks[0]) })
	check("cancel", []string{"cancelled"}, func(toks []*recorder) { h.Cancel(toks[0]) })
	check("timeout", []string{"timed-out 30s"}, func([]*recorder) { h.Expire(30 * time.Second) })
	// The second wait opened with a shorter timeout, as after a HotReload: each times out with its own.
	check("timeouts of two waits", []string{"timed-out 30s", "timed-out 10s"}, func([]*recorder) { h.Expire(30 * time.Second) })

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
