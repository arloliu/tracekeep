package catalog

import (
	"errors"
	"math"
	"testing"
	"time"
)

const (
	month = 720 * time.Hour
	delay = time.Hour
)

func hourNS(h int64) int64 { return h * int64(time.Hour) }

func recordRetention(t *testing.T, c *Catalog, l Lease, period time.Duration, now int64) Retention {
	t.Helper()
	r, err := c.RecordRetention(t.Context(), l, period, delay, now)
	if err != nil {
		t.Fatal(err)
	}

	return r
}

func latestRetention(t *testing.T, c *Catalog) Retention {
	t.Helper()
	r, err := c.Retention(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return r
}

// The deadline and the eviction horizon are one function of the recorded row:
// an hour may go at the first time its end plus the period in force has passed.
func TestRetentionDeadline(t *testing.T) {
	t.Parallel()
	plain := Retention{Known: true, Period: 10 * time.Hour}
	if got, want := plain.Deadline(100), hourNS(111); got != want {
		t.Errorf("Deadline = %d, want %d", got, want)
	}
	// After shortening from 10h to 2h at hour 200, 10h stays in force until hour 201.
	held := Retention{Known: true, Period: 2 * time.Hour, Held: 10 * time.Hour, HeldUntil: hourNS(201)}
	cases := []struct {
		hour, want int64
	}{
		{hour: 100, want: hourNS(111)}, // past both: its end plus the held period
		{hour: 190, want: hourNS(201)}, // the held period reaches past the window: the window's end
		{hour: 199, want: hourNS(202)}, // the new period alone, after the window
		{hour: 300, want: hourNS(303)},
	}
	for _, tc := range cases {
		if got := held.Deadline(tc.hour); got != tc.want {
			t.Errorf("hour %d: Deadline = %d, want %d", tc.hour, got, tc.want)
		}
		// The horizon agrees with the deadline at, just before and after it.
		for _, now := range []int64{tc.want - 1, tc.want, tc.want + 1} {
			evictable := tc.hour < held.HorizonHour(now)
			if evictable != (now >= tc.want) {
				t.Errorf("hour %d at %d: evictable %v, deadline %d", tc.hour, now, evictable, tc.want)
			}
		}
	}
	if held.InForce(hourNS(201)-1) != 10*time.Hour || held.InForce(hourNS(201)) != 2*time.Hour {
		t.Error("the held period is not in force exactly until its end")
	}
}

// The compactor records its period under the lease; a shortening holds the period in force for D,
// two shortenings inside one window hold the longest, and every process reads the same row.
func TestRecordRetention(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		if r := latestRetention(t, c); r.Known {
			t.Fatalf("Retention before any record = %+v, want unknown", r)
		}
		l := compactorLease(t, c, "w")
		t0 := hourNS(1000)
		recordRetention(t, c, l, month, t0)
		if r := latestRetention(t, c); !r.Known || r.Period != month || r.HeldUntil != 0 {
			t.Fatalf("first record = %+v", r)
		}
		recordRetention(t, c, l, month, t0+1)
		if n := count(t, c, `SELECT COUNT(*) FROM retention_periods`); n != 1 {
			t.Errorf("an unchanged period inserted a row: %d rows", n)
		}
		// Shortened: the month stays in force for D.
		t1 := t0 + hourNS(1)
		r := recordRetention(t, c, l, 10*time.Hour, t1)
		if r.Period != 10*time.Hour || r.Held != month || r.HeldUntil != t1+int64(delay) {
			t.Fatalf("shortened = %+v", r)
		}
		// Shortened again inside the window: the month is still what is held.
		t2 := t1 + int64(30*time.Minute)
		r = recordRetention(t, c, l, 5*time.Hour, t2)
		if r.Held != month || r.HeldUntil != t2+int64(delay) {
			t.Fatalf("shortened twice = %+v, want the month held", r)
		}
		// Lengthened: the hold stays as it was.
		t3 := t2 + 1
		r = recordRetention(t, c, l, 8*time.Hour, t3)
		if r.Period != 8*time.Hour || r.Held != month || r.HeldUntil != t2+int64(delay) {
			t.Fatalf("lengthened = %+v", r)
		}
		// A restart reads the same row.
		if got := latestRetention(t, c); got != r {
			t.Errorf("Retention = %+v, want %+v", got, r)
		}
		// Shortened after the window: what was in force then, 8h, is held.
		t4 := t2 + int64(delay) + 1
		r = recordRetention(t, c, l, 4*time.Hour, t4)
		if r.Held != 8*time.Hour || r.HeldUntil != t4+int64(delay) {
			t.Fatalf("shortened after the window = %+v, want 8h held", r)
		}
	})
}

func TestRecordRetentionUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		compactorLease(t, c, "new")
		if _, err := c.RecordRetention(t.Context(), stale, month, delay, hourNS(1)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("RecordRetention = %v, want ErrLeaseLost", err)
		}
		if latestRetention(t, c).Known {
			t.Error("a refused record wrote")
		}
	})
}

// A URL issued just before a shortening or a merge expires before the object goes only if D leaves twice the clock skew beyond its lifetime:
// the issuer a minute fast and the validator a minute slow stretch the URL by two minutes,
// the recorder a minute slow and the evictor a minute fast shorten the wait by two.
func TestDeletionTimeAcrossSkewedClocks(t *testing.T) {
	t.Parallel()
	const lifetime, skew = 15 * time.Minute, time.Minute
	real0 := hourNS(1000) // the true time of the issue, and of the shortening or the merge
	// The validator, a minute slow, accepts the URL the fast issuer signed while its own clock is before the signed expiry.
	accepted := func(trueNs int64) bool { return trueNs-int64(skew) < real0+int64(skew)+int64(lifetime) }
	for _, tc := range []struct {
		d    time.Duration
		safe bool
	}{{lifetime + 4*skew - time.Second, false}, {lifetime + 4*skew, true}, {lifetime + 4*skew + time.Second, true}} {
		recordedAt := real0 - int64(skew) // the slow recorder's now
		// The shortening: the month held until the recorder's now + D; the fast evictor's first eviction of an hour the month keeps.
		held := Retention{Known: true, Period: time.Hour, Held: month, HeldUntil: recordedAt + int64(tc.d)}
		hour := int64(990)
		evicted := real0
		for hour >= held.HorizonHour(evicted+int64(skew)) {
			evicted += int64(time.Second)
		}
		// The merge: delete_at the recorder's now + D; the fast evictor deletes once its clock reaches it.
		deleted := recordedAt + int64(tc.d) - int64(skew)
		for name, at := range map[string]int64{"eviction": evicted, "deletion": deleted} {
			if safe := !accepted(at); safe != tc.safe {
				t.Errorf("D %s: %s %s after the issue while the URL is accepted: safe %v, want %v", tc.d, name, time.Duration(at-real0), safe, tc.safe)
			}
		}
	}
}

// At the end of int64 nanoseconds a deadline saturates rather than wrapping into the past, so no future pack reads as due.
func TestRetentionDeadlineAtTheLimit(t *testing.T) {
	t.Parallel()
	last := int64(math.MaxInt64) / nsPerHour
	for _, r := range []Retention{
		{Known: true, Period: 720 * time.Hour},
		{Known: true, Period: time.Hour, Held: 720 * time.Hour, HeldUntil: math.MaxInt64},
	} {
		for _, h := range []int64{last - 1, last} {
			if got := r.Deadline(h); got != math.MaxInt64 {
				t.Errorf("%+v: Deadline(%d) = %d, want the last representable time", r, h, got)
			}
		}
		if got, want := r.Deadline(0), nsPerHour+int64(max(r.Period, r.Held)); r.HeldUntil == 0 && got != want {
			t.Errorf("Deadline(0) = %d, want %d", got, want)
		}
	}
}
