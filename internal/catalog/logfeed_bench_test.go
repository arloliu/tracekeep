package catalog

import (
	"fmt"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// benchLogGroup ensures the lease groups and takes group 1's lease for a day, so a long benchmark never outlives it.
func benchLogGroup(b *testing.B, c *Catalog) Lease {
	b.Helper()
	gate(b, c)
	if err := c.EnsureLogGroups(b.Context(), 4); err != nil {
		b.Fatal(err)
	}
	l, ok, err := c.TakeLease(b.Context(), LogGroupLease(1), "bench", 24*time.Hour)
	if err != nil || !ok {
		b.Fatalf("take group 1's lease = %v, %v", ok, err)
	}

	return l
}

// benchAdmitLog admits device's capture of file under l.
func benchAdmitLog(b *testing.B, c *Catalog, l Lease, file LogFile, device string) tracepack.UUID {
	b.Helper()
	id := newID(b)
	if _, err := c.AdmitLog(b.Context(), l, 1, LogKey{LogFile: file, Device: device}, id, logDescriptor(b, device), nil); err != nil {
		b.Fatal(err)
	}

	return id
}

// BenchmarkRegisterLogFlush registers one equipment's flush of entries entries over devices devices, each iteration a fresh flush:
// a segment a device of its share of the entries' records, two entries a record, in one hour, and the file's watermark raised (logfeed.md §15.10).
func BenchmarkRegisterLogFlush(b *testing.B) {
	for _, entries := range []int{16, 96, 480} {
		for _, devices := range []int{1, 3} {
			b.Run(fmt.Sprintf("entries=%d/devices=%d", entries, devices), func(b *testing.B) {
				benchEngines(b, func(b *testing.B, c *Catalog) {
					l := benchLogGroup(b, c)
					captures := make([]tracepack.UUID, devices)
					for d := range captures {
						captures[d] = benchAdmitLog(b, c, l, logFile, fmt.Sprintf("dev%d", d))
					}
					records := uint64(max(1, entries/2/devices)) //nolint:gosec // small positive counts
					var seq uint64
					var offset int64
					for b.Loop() {
						f := &LogFlush{Group: 1}
						for _, id := range captures {
							p := segment(b, id, 5, seq, seq+records-1)
							p.EquipmentID = logFile.EquipmentID
							f.Packs = append(f.Packs, p)
						}
						seq += records
						offset += int64(entries) * 600
						f.Files = []LogFileMark{{LogFile: logFile, Watermark: offset}}
						if out, err := c.RegisterLogFlush(b.Context(), l, f); err != nil || out != Registered {
							b.Fatalf("RegisterLogFlush = %v, %v", out, err)
						}
					}
				})
			})
		}
	}
}

// BenchmarkAdmitLog admits a log capture of a new key each iteration, a device of a new file.
func BenchmarkAdmitLog(b *testing.B) {
	benchEngines(b, func(b *testing.B, c *Catalog) {
		l := benchLogGroup(b, c)
		n := 0
		for b.Loop() {
			n++
			benchAdmitLog(b, c, l, LogFile{Source: "feed-a", EquipmentID: "T1", File: fmt.Sprintf("host-%d.log", n)}, "eqp")
		}
	})
}

// BenchmarkStopLogCapture requests the stop of an open log capture each iteration, its group's lease held.
func BenchmarkStopLogCapture(b *testing.B) {
	benchEngines(b, func(b *testing.B, c *Catalog) {
		l := benchLogGroup(b, c)
		ids := make([]tracepack.UUID, 0, 2000)
		for i := range 2000 {
			ids = append(ids, benchAdmitLog(b, c, l, logFile, fmt.Sprintf("dev%d", i)))
		}
		i := 0
		for b.Loop() {
			// Each capture stays open after a request, so the ids are reused in turn.
			if out, err := c.StopLogCapture(b.Context(), ids[i%len(ids)]); err != nil || out != StopRequested {
				b.Fatalf("StopLogCapture = %v, %v", out, err)
			}
			i++
		}
	})
}
