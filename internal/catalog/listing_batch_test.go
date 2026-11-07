package catalog

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// holdAtNth holds the transaction that reaches hook id for the n-th time, counted from 1, until release is closed;
// held is closed once it is held. Set it before the transactions it concerns start, and clear the hook after them.
func holdAtNth(c *Catalog, id int, n int64) (held, release chan struct{}) {
	held, release = make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	c.hooks[id] = func() {
		if calls.Add(1) == n {
			close(held)
			<-release
		}
	}

	return held, release
}

// completenessReads counts the completeness input reads f takes.
func completenessReads(c *Catalog, f func()) int64 {
	var calls atomic.Int64
	c.hooks[hookCompletenessRead] = func() { calls.Add(1) }
	defer func() { c.hooks[hookCompletenessRead] = nil }()
	f()

	return calls.Load()
}

// varyListing gives each capture of pop inputs of its own beside seedListing's, so a row read for one capture and handed to another shows:
// the i-th capture has a parked version above its highest seq, 129, at 130 + i mod 3 (a gap below it unless i mod 3 is 0),
// its record behind the records before it for an odd i, a clock step; an evicted seq 140+i, a conflict over 200+i and a declared loss of 300+i.
// The last capture also takes an endpoint, the previous capture's recorder instance, which overlaps it, and the latest record.
func varyListing(t *testing.T, c *Catalog, pop *listingPop) {
	t.Helper()
	n := len(pop.captures)
	parked, evicted, conflicts, losses := make([][]any, 0, n), make([][]any, 0, n), make([][]any, 0, n), make([][]any, 0, n)
	for i, cl := range pop.captures {
		id, seq := cl.ID, int64(130+i%3)
		ts := hourNS(pop.hours) + 20
		if i%2 == 1 {
			ts = hourNS(1) + 5
		}
		var header [44]byte
		binary.LittleEndian.PutUint64(header[:8], uint64(seq))
		binary.LittleEndian.PutUint64(header[8:16], uint64(ts))
		parked = append(parked, []any{id[:], seq, header[:], make([]byte, 32), "parking", "x", floorHour(ts), sql.NullString{}, 0})
		evicted = append(evicted, []any{id[:], 0, int64(140 + i), int64(140 + i), int64(140 + i), int64(140 + i)})
		a, b := pop.packs[i][0].ID, pop.packs[i][1].ID // neither is deleted: seedListing deletes every third
		conflicts = append(conflicts, []any{id[:], int64(200 + i), int64(200 + i), a[:], b[:], "suspect"})
		losses = append(losses, []any{b[:], 0, int64(300 + i), int64(300 + i), nil, nil})
	}
	seedRows(t, c, "parked", []string{"capture_id", "seq", "header", "payload_digest", "state", "reason", "hour", "terminated_by", "parked_at"}, parked)
	seedRows(t, c, "evicted_ranges", []string{"capture_id", "hour", "seq_from", "seq_to", "ts_from", "ts_to"}, evicted)
	seedRows(t, c, "conflicts", []string{"capture_id", "seq_from", "seq_to", "pack_id", "other_pack_id", "state"}, conflicts)
	seedRows(t, c, "pack_losses", []string{"pack_id", "ord", "seq_from", "seq_to", "ts_from", "ts_to"}, losses)
	// The last capture joins the series of the latest earlier capture of its device and source, so it overlaps that one.
	last := pop.captures[len(pop.captures)-1]
	prev := pop.captures[len(pop.captures)-2]
	for i := len(pop.captures) - 2; i >= 0; i-- {
		if c := pop.captures[i]; c.Device == last.Device && c.Source == last.Source {
			prev = c
			break
		}
	}
	s := spec{instance: prev.RecorderInstanceID, device: last.Device, source: last.Source, origin: last.OriginNS, endpoint: "192.0.2.1:5000", deviceID: 7, equipmentID: "T1"}
	exec(t, c, `UPDATE captures SET descriptor = ?, recorder_instance_id = ?, highest_seq_ts = ? WHERE capture_id = ?`,
		descriptorBytes(t, s), prev.RecorderInstanceID[:], hourNS(pop.hours)+50, last.ID[:])
}

// perCapture is the listings' answer for sel built from the unbatched reads: ListCaptures, then each capture's Completeness.
func perCapture(t *testing.T, c *Catalog, sel Selector) ([]CaptureListing, []DeviceInfo) {
	t.Helper()
	caps, err := c.ListCaptures(t.Context(), sel, readQ)
	if err != nil {
		t.Fatal(err)
	}
	listed := make([]CaptureListing, 0, len(caps))
	var devices []DeviceInfo
	for _, ci := range caps {
		cp := completeness(t, c, ci.ID)
		listed = append(listed, CaptureListing{CaptureInfo: ci, Completeness: cp})
		if len(devices) == 0 || devices[len(devices)-1].Name != ci.Device || devices[len(devices)-1].Source != ci.Source {
			devices = append(devices, DeviceInfo{Name: ci.Device, Source: ci.Source})
		}
		d := &devices[len(devices)-1]
		d.EquipmentEndpoint, d.DeviceID, d.CaptureMethod = ci.EquipmentEndpoint, ci.DeviceID, ci.CaptureMethod // ordered by origin: the last is the latest
		if ci.State == StateOpen {
			d.OpenCaptures++
		}
		d.Overlap = d.Overlap || ci.Overlap
		if ci.LastRecordNS != nil && (d.LastRecordNS == nil || *ci.LastRecordNS > *d.LastRecordNS) {
			d.LastRecordNS = ci.LastRecordNS
		}
		for _, g := range cp.Gaps {
			d.Gap = d.Gap || g.meets(sel.From, sel.To)
		}
	}

	return listed, devices
}

// The listings read completeness in chunks of 128 captures, each chunk taking the reads one capture's Completeness takes,
// and answer as the unbatched reads do at 127, 128 and 129 captures, on one device or four, over every input kind:
// whole-capture completeness whatever the window, including windows ending at or starting from a backward gap's endpoints;
// and a device whose first capture has a gap keeps its later captures' endpoint, open count, overlap and last record.
func TestListingsBatchedAsPerCapture(t *testing.T) {
	t.Parallel()
	for _, n := range []int{127, 128, 129} {
		for _, devices := range []int{1, 4} {
			t.Run(fmt.Sprintf("captures=%d/devices=%d", n, devices), func(t *testing.T) {
				t.Parallel()
				engines(t, func(t *testing.T, c *Catalog) {
					pop := seedListing(t, c, n, devices, 3, true)
					varyListing(t, c, pop)
					h := hourNS
					windows := []Selector{
						pop.whole(), pop.hour(),
						window(0, h(1)+5), window(0, h(1)+6), // the odd captures' backward gaps run from h(1)+5 to h(3)+9
						window(h(3)+9, h(4)), window(h(3)+8, h(4)),
					}
					var gap, noGap bool
					for _, sel := range windows {
						wantCaps, wantDevices := perCapture(t, c, sel)
						var got Listing
						chunks := int64((len(wantCaps) + 127) / 128)
						one := completenessReads(c, func() { completeness(t, c, wantCaps[0].ID) })
						if calls := completenessReads(c, func() { got = listPacks(t, c, sel) }); calls != chunks*one {
							t.Errorf("window %d–%d: ListPacks over %d captures took %d completeness reads, want %d chunks of %d",
								sel.From, sel.To, len(wantCaps), calls, chunks, one)
						}
						if !sameAnswer(got.Captures, wantCaps) {
							t.Errorf("window %d–%d: ListPacks' captures differ from the per-capture reads", sel.From, sel.To)
						}
						var gotDevices []DeviceInfo
						var err error
						// Each device's first capture first, then the rest: a chunk more at most.
						calls := completenessReads(c, func() { gotDevices, err = c.ListDevices(t.Context(), sel, readQ) })
						if calls > (chunks+1)*one {
							t.Errorf("window %d–%d: ListDevices took %d completeness reads, want at most %d", sel.From, sel.To, calls, (chunks+1)*one)
						}
						if err != nil || !sameAnswer(gotDevices, wantDevices) {
							t.Errorf("window %d–%d: ListDevices = %+v, %v; want %+v", sel.From, sel.To, gotDevices, err, wantDevices)
						}
						for _, d := range wantDevices {
							gap, noGap = gap || d.Gap, noGap || !d.Gap
						}
					}
					if !gap || !noGap {
						t.Fatalf("the windows found a gap %t and a device without one %t: the fixture misses a case", gap, noGap)
					}
					// The early answer: every device has a gap in the whole window before its last capture,
					// whose metadata is still reported.
					_, all := perCapture(t, c, pop.whole())
					last := pop.captures[n-1]
					for _, d := range all {
						if !d.Gap {
							t.Errorf("device %s has no gap in the whole window; the fixture gives each device one", d.Name)
						}
						if d.Name == last.Device && d.Source == last.Source && (d.EquipmentEndpoint == nil || !d.Overlap || d.LastRecordNS == nil || *d.LastRecordNS != hourNS(pop.hours)+50) {
							t.Errorf("series (%s, %s) = %+v, want the last capture's endpoint, overlap and record", d.Name, d.Source, d)
						}
					}
				})
			})
		}
	}
}

// A listing reads every chunk from the snapshot its watermark opened: registrations committing at the watermark,
// after each of a chunk's input reads and at the boundary between chunks, into a capture of the first chunk and one of the second,
// change neither ListPacks' answer nor ListDevices' (barrier-controlled, both engines).
// Each round registers a record in an hour of its own, past the window of the round before, leaving a gap only that round's window meets.
func TestListingsReadOneSnapshotAcrossChunks(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		pop := seedListing(t, c, 129, 2, 2, false)
		exec(t, c, `UPDATE captures SET state = 'open'`) // every capture selected by a window past its records, and registering
		listed := pop.listed()
		targets := []tracepack.UUID{listed[0].ID, listed[128].ID}
		one := completenessReads(c, func() { completeness(t, c, targets[0]) })
		next := map[tracepack.UUID]uint64{targets[0]: 121, targets[1]: 121} // each round leaves the seq before it a gap
		round := int64(0)
		registerRound := func() {
			round++
			hour := 100 + round
			for _, id := range targets {
				p := segment(t, id, hour, next[id], next[id])
				p.Ranges[0].TSFrom, p.Ranges[0].TSTo = hourNS(hour), hourNS(hour)
				if out, err := c.RegisterPack(t.Context(), p, Registration{Evidence: &Evidence{}}); err != nil || out != Registered {
					t.Fatalf("RegisterPack = %v, %v", out, err)
				}
				next[id] += 2
			}
		}
		// sel is the round's window: the half hour before its record, which only its own gap meets.
		sel := func() Selector { return window(hourNS(101+round)-hourNS(1)/2, hourNS(101+round)) }
		// hold lists s while a round registers at the n-th call of hook id, named at, and asserts the listing as before the round.
		hold := func(at string, id int, n int64, list func(Selector) (any, error)) {
			t.Helper()
			s := sel()
			before, err := list(s)
			if err != nil {
				t.Fatal(err)
			}
			held, release := holdAtNth(c, id, n)
			var got any
			done := run(func() { got, err = list(s) })
			<-held
			registerRound()
			close(release)
			<-done
			c.hooks[id] = nil
			if err != nil || !sameAnswer(got, before) {
				t.Errorf("%s %d: a registration committed during the listing changed it (%v)", at, n, err)
			}
			if after, err := list(s); err != nil || sameAnswer(after, before) {
				t.Errorf("%s %d: the registration changed nothing a later listing reads (%v)", at, n, err)
			}
		}
		packs := func(s Selector) (any, error) { return c.ListPacks(t.Context(), s, readQ) }
		devices := func(s Selector) (any, error) { return c.ListDevices(t.Context(), s, readQ) }
		hold("ListPacks at its watermark", hookWatermarkRead, 1, packs)
		for n := int64(1); n <= 2*one; n++ {
			hold("ListPacks after completeness read", hookCompletenessRead, n, packs)
			hold("ListDevices after completeness read", hookCompletenessRead, n, devices)
		}
	})
}
