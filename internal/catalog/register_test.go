package catalog

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// admitted returns a capture admitted under a fresh gate.
func admitted(t *testing.T, c *Catalog) tracepack.UUID {
	t.Helper()
	gate(t, c)
	id := newID(t)
	if _, err := c.Admit(t.Context(), start(id), descriptor(t, "d")); err != nil {
		t.Fatal(err)
	}

	return id
}

func segment(t *testing.T, capture tracepack.UUID, hour int64, from, to uint64) *Pack {
	t.Helper()
	id := newID(t)

	return &Pack{
		ID: id, Capture: capture, ToolID: "T1", Hour: hour, Role: RoleSegment, Key: "staging/" + id.String(), Size: 100,
		SeqStart: from, FormatMinor: 1, Ranges: []Range{{SeqFrom: from, SeqTo: to, TSFrom: int64(from), TSTo: int64(to)}},
	}
}

func register(t *testing.T, c *Catalog, p *Pack, r Registration) PackOutcome {
	t.Helper()
	out, err := c.RegisterPack(t.Context(), p, r)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

func events(t *testing.T, c *Catalog) []string {
	t.Helper()
	var evs []struct {
		Hour  int64  `db:"hour"`
		Event string `db:"event"`
	}
	if err := c.read.SelectContext(t.Context(), &evs, `SELECT hour, event FROM scope_events ORDER BY seq`); err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = fmt.Sprintf("%s@%d", e.Event, e.Hour)
	}

	return out
}

func count(t *testing.T, c *Catalog, query string, args ...any) int {
	t.Helper()
	var n int
	if err := c.read.GetContext(t.Context(), &n, query, args...); err != nil {
		t.Fatal(err)
	}

	return n
}

func TestRegisterPack(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		p := segment(t, capture, 1, 0, 9)
		if out := register(t, c, p, Registration{Evidence: &Evidence{}}); out != Registered {
			t.Fatalf("RegisterPack = %v, want registered", out)
		}
		if out := register(t, c, p, Registration{Evidence: &Evidence{}}); out != AlreadyRegistered {
			t.Errorf("a second registration = %v, want already registered", out)
		}
		got, err := c.Capture(t.Context(), capture)
		if err != nil || !got.HasHighestSeq || got.HighestSeq != 9 {
			t.Errorf("capture = %+v, %v; want highest seq 9", got, err)
		}
		// Only the first registration emits created.
		if evs := events(t, c); !slices.Equal(evs, []string{"created@1"}) {
			t.Errorf("events = %v, want one created", evs)
		}
		// A later pack with a lower highest seq leaves the highest alone.
		register(t, c, segment(t, capture, 1, 20, 29), Registration{Evidence: &Evidence{}})
		register(t, c, segment(t, capture, 1, 10, 19), Registration{Evidence: &Evidence{}})
		if got, _ := c.Capture(t.Context(), capture); got.HighestSeq != 29 {
			t.Errorf("highest seq %d, want 29", got.HighestSeq)
		}
	})
}

// A registration with evidence that finds its pack adopted by the reconciler leaves its batch unacknowledged (T-32).
func TestRegisterFindsAnAdoptedPack(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		lease, _, err := c.TakeLease(t.Context(), "compactor", "w", ttl)
		if err != nil {
			t.Fatal(err)
		}
		p := segment(t, capture, 1, 0, 9)
		if out := register(t, c, p, Registration{Lease: &lease, Adopted: true}); out != Registered {
			t.Fatalf("adoption = %v, want registered", out)
		}
		if out := register(t, c, p, Registration{Evidence: &Evidence{}}); out != FoundAdopted {
			t.Errorf("ingest's registration of the adopted pack = %v, want found adopted", out)
		}
	})
}

func TestRegisterGuards(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		tomb := segment(t, capture, 5, 0, 9)
		if _, err := c.write.ExecContext(ctx, `INSERT INTO tombstones (object_key, reason, at) VALUES (?, 'deleted', 0)`, tomb.Key); err != nil {
			t.Fatal(err)
		}
		if out := register(t, c, tomb, Registration{Evidence: &Evidence{}}); out != Tombstoned {
			t.Errorf("a tombstoned key = %v, want tombstoned", out)
		}
		old := segment(t, capture, 3, 0, 9)
		if out := register(t, c, old, Registration{Evidence: &Evidence{}, HorizonHour: 4}); out != BeyondHorizon {
			t.Errorf("an hour past the horizon = %v, want beyond the horizon", out)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, old.Key); n != 1 {
			t.Errorf("the key past the horizon has %d tombstones, want one", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM packs`); n != 0 {
			t.Errorf("%d packs registered past the guards", n)
		}

		// The reconciler's orphan of a capture no start admitted, pending or rejected, stays unregistered.
		lease, _, _ := c.TakeLease(ctx, "compactor", "w", ttl)
		if out := register(t, c, segment(t, newID(t), 5, 0, 9), Registration{Lease: &lease, Adopted: true}); out != UnadmittedCapture {
			t.Errorf("an orphan of an unknown capture = %v, want unadmitted", out)
		}
		rejected := newID(t)
		if _, err := c.Reject(ctx, start(rejected), []byte("bad")); err != nil {
			t.Fatal(err)
		}
		if out := register(t, c, segment(t, rejected, 5, 0, 9), Registration{Lease: &lease, Adopted: true}); out != UnadmittedCapture {
			t.Errorf("an orphan of a rejected capture = %v, want unadmitted", out)
		}
		// An outcome that registers nothing writes nothing: no placeholder row for an unknown capture, its tombstone apart.
		stray := segment(t, newID(t), 1, 0, 9)
		if out := register(t, c, stray, Registration{Evidence: &Evidence{}, HorizonHour: 4}); out != BeyondHorizon {
			t.Fatalf("a stray past the horizon = %v", out)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM captures WHERE capture_id = ?`, stray.Capture[:]); n != 0 {
			t.Errorf("a refused registration left %d capture rows", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM packs`); n != 0 {
			t.Errorf("%d packs registered past the guards", n)
		}
		lapse(t, c, `UPDATE leases SET until_ns = 0`)
		if _, err := c.RegisterPack(ctx, segment(t, capture, 5, 0, 9), Registration{Lease: &lease, Adopted: true}); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("a registration under a lapsed lease = %v, want ErrLeaseLost", err)
		}
	})
}

// A pack overlapping another pack of the capture, in another hour, is a suspect;
// both scopes advance and emit a conflict event (ingest.md §6.2).
func TestRegisterOverlapIsSuspect(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		first := segment(t, capture, 1, 0, 9)
		register(t, c, first, Registration{Evidence: &Evidence{}})
		second := segment(t, capture, 2, 5, 14)
		register(t, c, second, Registration{Evidence: &Evidence{}})

		var rows []struct {
			From  int64  `db:"seq_from"`
			To    int64  `db:"seq_to"`
			Other []byte `db:"other_pack_id"`
			State string `db:"state"`
		}
		if err := c.read.SelectContext(t.Context(), &rows, `SELECT seq_from, seq_to, other_pack_id, state FROM conflicts`); err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].From != 5 || rows[0].To != 9 || tracepack.UUID(rows[0].Other) != first.ID || rows[0].State != "suspect" {
			t.Fatalf("conflicts = %+v, want the suspect run 5-9 against the first pack", rows)
		}
		if evs := events(t, c); !slices.Equal(evs, []string{"created@1", "created@2", "conflict@2", "conflict@1"}) {
			t.Errorf("events = %v", evs)
		}
		if n := count(t, c, `SELECT revision FROM scopes WHERE hour = 1`); n != 2 {
			t.Errorf("the first scope's revision is %d, want 2", n)
		}
	})
}

// A registration into a settled scope reopens it; a stop moves an open capture to stopped.
func TestRegisterReopensAndStops(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		capture := admitted(t, c)
		register(t, c, segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		if _, err := c.write.ExecContext(ctx, `UPDATE scopes SET state = 'settled'`); err != nil {
			t.Fatal(err)
		}
		stop := segment(t, capture, 1, 10, 10)
		stop.Stop = true
		register(t, c, stop, Registration{Evidence: &Evidence{}})
		if evs := events(t, c); !slices.Equal(evs, []string{"created@1", "reopened@1"}) {
			t.Errorf("events = %v, want created then reopened", evs)
		}
		if got, _ := c.Capture(ctx, capture); got.State != StateStopped || !got.StopSeen {
			t.Errorf("capture after a stop = %+v, want stopped", got)
		}
	})
}

// Ingest's evidence writes a receipt and a claim object row for each claim-check record; the reconciler's registration writes none.
func TestRegisterReceipts(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		ev := &Evidence{Receipts: []Receipt{{Seq: 3, Header: [44]byte{1}, PayloadSHA256: [32]byte{2}, ObjectKey: "claim/3", ObjectSize: 5000}}}
		register(t, c, segment(t, capture, 1, 0, 9), Registration{Evidence: ev})
		if n := count(t, c, `SELECT COUNT(*) FROM claim_records WHERE seq = 3`); n != 1 {
			t.Errorf("%d receipts, want one", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM claim_objects WHERE object_key = 'claim/3' AND state = 'live'`); n != 1 {
			t.Errorf("%d claim objects, want one", n)
		}
	})
}

// A conflict the batch split is recorded confirmed against the pack holding the other version,
// whichever of the flush's packs registers first.
func TestRegisterBatchConflict(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		first := segment(t, capture, 1, 0, 9)
		other := segment(t, capture, 1, 4, 4)
		other.Role = RoleConflict
		other.Conflicts = []Version{{Seq: 4, Pack: first.ID}}
		if out := register(t, c, other, Registration{Evidence: &Evidence{}}); out != Registered {
			t.Fatalf("the conflict segment before the pack it names = %v", out)
		}
		register(t, c, first, Registration{Evidence: &Evidence{}})
		if n := count(t, c, `SELECT COUNT(*) FROM conflicts WHERE state = 'confirmed' AND seq_from = 4`); n != 1 {
			t.Errorf("%d confirmed conflicts at seq 4, want one", n)
		}
	})
}

// A pack whose seqs do not fit a signed BIGINT, and a registration mode no caller has, are refused before anything is written.
func TestRegisterRefusesInvalidInput(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		lease, _, _ := c.TakeLease(t.Context(), "compactor", "w", ttl)
		ingest := Registration{Evidence: &Evidence{}}
		big := uint64(closeSentinel)
		cases := map[string]struct {
			mutate func(*Pack)
			r      Registration
		}{
			"range at the sentinel":      {func(p *Pack) { p.Ranges[0].SeqTo = big }, ingest},
			"seq start overflow":         {func(p *Pack) { p.SeqStart = big + 1 }, ingest},
			"loss overflow":              {func(p *Pack) { p.Losses = []Loss{{SeqFrom: ptr(uint64(1)), SeqTo: ptr(big + 5)}} }, ingest},
			"conflict overflow":          {func(p *Pack) { p.Conflicts = []Version{{Seq: big}} }, ingest},
			"receipt overflow":           {func(*Pack) {}, Registration{Evidence: &Evidence{Receipts: []Receipt{{Seq: big + 1}}}}},
			"no range":                   {func(p *Pack) { p.Ranges = nil }, ingest},
			"adoption without the lease": {func(*Pack) {}, Registration{Adopted: true}},
			"adoption with evidence":     {func(*Pack) {}, Registration{Lease: &lease, Adopted: true, Evidence: &Evidence{}}},
			"evidence under the lease":   {func(*Pack) {}, Registration{Lease: &lease, Evidence: &Evidence{}}},
			"neither evidence nor lease": {func(*Pack) {}, Registration{}},
		}
		for name, tc := range cases {
			p := segment(t, capture, 1, 0, 9)
			tc.mutate(p)
			if _, err := c.RegisterPack(t.Context(), p, tc.r); !errors.Is(err, ErrInvalidRegistration) {
				t.Errorf("%s: RegisterPack = %v, want ErrInvalidRegistration", name, err)
			}
		}
		if n := count(t, c, `SELECT COUNT(*) FROM packs`) + count(t, c, `SELECT COUNT(*) FROM scopes`) + count(t, c, `SELECT COUNT(*) FROM claim_records`); n != 0 {
			t.Errorf("refused registrations wrote %d rows", n)
		}
	})
}

// A close holds only the sentinel: it registers without a range and moves no seq of the capture.
func TestRegisterClose(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		register(t, c, segment(t, capture, 1, 0, 9), Registration{Evidence: &Evidence{}})
		lease, _, _ := c.TakeLease(t.Context(), "compactor", "w", ttl)
		cl := segment(t, capture, 1, 0, 0)
		cl.Role, cl.Ranges, cl.SeqStart = RoleClose, nil, closeSentinel
		if out := register(t, c, cl, Registration{Lease: &lease}); out != Registered {
			t.Fatalf("a close = %v, want registered", out)
		}
		if got, _ := c.Capture(t.Context(), capture); got.HighestSeq != 9 {
			t.Errorf("highest seq %d after the close, want 9", got.HighestSeq)
		}
	})
}

func ptr[T any](v T) *T { return &v }

// A loss keeps the bounds its coverage entry has: an unknown upper bound, or none at all for a carried entry.
func TestRegisterOpenEndedLoss(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		p := segment(t, capture, 1, 0, 9)
		p.Losses = []Loss{{SeqFrom: ptr(uint64(10)), TSFrom: ptr(int64(5))}, {}}
		register(t, c, p, Registration{Evidence: &Evidence{}})
		if n := count(t, c, `SELECT COUNT(*) FROM pack_losses WHERE seq_from = 10 AND seq_to IS NULL`); n != 1 {
			t.Errorf("%d open-ended losses, want one", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM pack_losses WHERE seq_from IS NULL AND ts_from IS NULL`); n != 1 {
			t.Errorf("%d losses without bounds, want one", n)
		}
	})
}

// A confirmed conflict at a seq keeps the suspect run that starts there against the same pack.
func TestRegisterConfirmedAndSuspectAtOneSeq(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		capture := admitted(t, c)
		first := segment(t, capture, 1, 0, 9)
		register(t, c, first, Registration{Evidence: &Evidence{}})
		p := segment(t, capture, 1, 5, 9)
		p.Role = RoleConflict
		p.Conflicts = []Version{{Seq: 5, Pack: first.ID}}
		register(t, c, p, Registration{Evidence: &Evidence{}})
		if n := count(t, c, `SELECT COUNT(*) FROM conflicts WHERE state = 'confirmed' AND seq_from = 5 AND seq_to = 5`); n != 1 {
			t.Errorf("%d confirmed conflicts at 5, want one", n)
		}
		if n := count(t, c, `SELECT COUNT(*) FROM conflicts WHERE state = 'suspect' AND seq_from = 5 AND seq_to = 9`); n != 1 {
			t.Errorf("%d suspects over 5-9, want one", n)
		}
	})
}
