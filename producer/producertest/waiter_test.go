package producertest_test

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// calls records the reports made on a token.
type calls struct {
	mu  sync.Mutex
	log []string
}

func (c *calls) add(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, s)
}

func (c *calls) get() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.log)
}

func (c *calls) Sent()                    { c.add("sent") }
func (c *calls) SendFailed()              { c.add("send-failed") }
func (c *calls) SubmissionFailed()        { c.add("submission-failed") }
func (c *calls) Awaiting(d time.Duration) { c.add("awaiting " + d.String()) }
func (c *calls) Replied()                 { c.add("replied") }
func (c *calls) TimedOut(d time.Duration) { c.add("timed-out " + d.String()) }
func (c *calls) Cancelled()               { c.add("cancelled") }

func waiterHarness(w *producertest.Waiter, clk *producertest.Clock) producertest.TimeoutSourceHarness {
	return producertest.TimeoutSourceHarness{
		Source:  w,
		Reply:   func(t producer.Token) { w.Reply(t) },
		Cancel:  func(t producer.Token) { w.Cancel(t) },
		Expire:  func(d time.Duration) { clk.Advance(d) },
		Quiesce: func() {}, // the waiter reports synchronously
	}
}

func TestWaiterConforms(t *testing.T) {
	t.Parallel()
	clk := producertest.NewClock()
	if err := producertest.CheckTimeoutSource(waiterHarness(producertest.NewWaiter(clk), clk)); err != nil {
		t.Fatal(err)
	}
}

func TestWaiterTimesOutOnItsClock(t *testing.T) {
	t.Parallel()
	clk := producertest.NewClock()
	w := producertest.NewWaiter(clk)
	tok := &calls{}

	w.Await(tok, 10*time.Second)
	clk.Advance(9 * time.Second)
	if got := tok.get(); !slices.Equal(got, []string{"awaiting 10s"}) {
		t.Fatalf("reports before the timeout = %v", got)
	}
	clk.Advance(time.Second)
	if got := tok.get(); !slices.Equal(got, []string{"awaiting 10s", "timed-out 10s"}) {
		t.Fatalf("reports = %v", got)
	}
	// A reply after the timeout is no outcome of a wait that has ended.
	if w.Reply(tok) {
		t.Fatalf("Reply ended a wait that had timed out")
	}
}

func TestWaiterKeepsTheTimeoutAWaitOpenedWith(t *testing.T) {
	t.Parallel()
	clk := producertest.NewClock()
	w := producertest.NewWaiter(clk)
	early, late := &calls{}, &calls{}

	w.Await(early, 10*time.Second)
	w.Await(late, 5*time.Second) // a HotReload shortened the timeout for new waits
	clk.Advance(10 * time.Second)
	if got := early.get(); got[len(got)-1] != "timed-out 10s" {
		t.Fatalf("first wait = %v", got)
	}
	if got := late.get(); got[len(got)-1] != "timed-out 5s" {
		t.Fatalf("second wait = %v", got)
	}
}

// badSource reports a timeout other than the one its wait was opened with.
type badSource struct{}

func (badSource) Await(t producer.Token, timeout time.Duration) {
	t.Awaiting(timeout)
	t.TimedOut(timeout + time.Second)
}

func TestCheckTimeoutSourceCatchesAViolation(t *testing.T) {
	t.Parallel()
	err := producertest.CheckTimeoutSource(producertest.TimeoutSourceHarness{
		Source: badSource{},
		Reply:  func(producer.Token) {},
		Cancel: func(producer.Token) {},
		Expire: func(time.Duration) {},
	})
	if err == nil || !strings.Contains(err.Error(), "timed-out") {
		t.Fatalf("CheckTimeoutSource = %v, want a timeout violation", err)
	}
}

// moving moves every open wait to the newest wait's deadline, though each reports the timeout it opened with.
type moving struct {
	mu            sync.Mutex
	now, deadline time.Duration
	waits         map[producer.Token]time.Duration
}

func (m *moving) Await(t producer.Token, timeout time.Duration) {
	t.Awaiting(timeout)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = m.now + timeout
	m.waits[t] = timeout
}

func (m *moving) end(t producer.Token, report func()) {
	m.mu.Lock()
	_, ok := m.waits[t]
	delete(m.waits, t)
	m.mu.Unlock()
	if ok {
		report()
	}
}

func (m *moving) advance(d time.Duration) {
	m.mu.Lock()
	m.now += d
	var due map[producer.Token]time.Duration
	if m.now >= m.deadline {
		due, m.waits = m.waits, map[producer.Token]time.Duration{}
	}
	m.mu.Unlock()
	for t, timeout := range due {
		t.TimedOut(timeout)
	}
}

func TestCheckTimeoutSourceCatchesAMovedWait(t *testing.T) {
	t.Parallel()
	m := &moving{waits: map[producer.Token]time.Duration{}}
	err := producertest.CheckTimeoutSource(producertest.TimeoutSourceHarness{
		Source:  m,
		Reply:   func(t producer.Token) { m.end(t, t.Replied) },
		Cancel:  func(t producer.Token) { m.end(t, t.Cancelled) },
		Expire:  m.advance,
		Quiesce: func() {},
	})
	if err == nil || !strings.Contains(err.Error(), "the 30s wait reports") {
		t.Fatalf("CheckTimeoutSource = %v, want the moved wait found", err)
	}
}

func TestCompeteReleasesEveryReportAtOnce(t *testing.T) {
	t.Parallel()
	tok := &calls{}
	producertest.Compete(tok,
		producer.Token.Replied,
		func(t producer.Token) { t.TimedOut(time.Second) },
		producer.Token.SendFailed,
	)
	got := tok.get()
	slices.Sort(got)
	if want := []string{"replied", "send-failed", "timed-out 1s"}; !slices.Equal(got, want) {
		t.Fatalf("reports = %v, want %v", got, want)
	}
}

// Competing reports reach the SDK's table, which keeps the first terminal outcome only.
func TestCompetingReportsOnTheProducer(t *testing.T) {
	t.Parallel()
	p, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects"}, producer.WithBus(producertest.NewBus()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	clk := producertest.NewClock()
	w := producertest.NewWaiter(clk)

	tok := p.Pending().Register(1, 7, "c")
	tok.Sent()
	w.Await(tok, time.Second)
	producertest.Compete(tok, func(t producer.Token) { w.Reply(t) }, func(producer.Token) { clk.Advance(time.Second) })
	if w.Reply(tok) || w.Cancel(tok) {
		t.Fatalf("the wait ended twice")
	}
}
