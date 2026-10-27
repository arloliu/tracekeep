package catalog

import (
	"errors"
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
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		if _, err := c.RecordRetention(t.Context(), stale, month, delay, hourNS(1)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("RecordRetention = %v, want ErrLeaseLost", err)
		}
		if latestRetention(t, c).Known {
			t.Error("a refused record wrote")
		}
	})
}
