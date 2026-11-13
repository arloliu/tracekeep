package admin_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// logCaptures admits two log captures of equipment T1 under group 0's lease, which it returns, and a gateway's capture, in that order.
func logCaptures(t *testing.T, c *catalog.Catalog) (catalog.Lease, []tracepack.UUID) {
	t.Helper()
	ctx := t.Context()
	cfg, err := c.ActivateConfig(ctx, []byte("config"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Register(ctx, "r", []int64{cfg}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureLogGroups(ctx, 2); err != nil {
		t.Fatal(err)
	}
	l, ok, err := c.TakeLease(ctx, catalog.LogGroupLease(0), "converter", time.Minute)
	if err != nil || !ok {
		t.Fatalf("take group 0's lease = %v, %v", ok, err)
	}
	ids := []tracepack.UUID{{1}, {2}, {9}}
	for i, device := range []string{"eqp", "lp1"} {
		key := catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: device}
		d := &catalog.Descriptor{
			EquipmentID: "T1", RecorderInstanceID: tracepack.UUID{byte(i + 10)}, Device: device, Source: "feed-a", OriginNS: 1,
			CaptureMethod: "log", Vantage: "host", SourceDialect: "host-sml-feed/1",
		}
		if _, err := c.AdmitLog(ctx, l, key, ids[i], d, nil); err != nil {
			t.Fatal(err)
		}
	}
	gw := ids[2]
	d := &catalog.Descriptor{Bytes: []byte("d"), EquipmentID: "T1", RecorderInstanceID: tracepack.UUID{4}, Device: "eqp", OriginNS: 1}
	if _, err := c.Admit(ctx, identity(gw, 0, 1, 1), d); err != nil {
		t.Fatal(err)
	}

	return l, ids
}

// capture stop requests a log capture's stop while its group's lease is held, writes it once the lease is gone,
// answers already for a stopped one, and refuses a gateway's capture (logfeed.md §15.11).
func TestCaptureStop(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		l, ids := logCaptures(t, c)
		a, b, gw := ids[0], ids[1], ids[2]
		ad := &admin.Admin{Catalog: c, Actor: "op"}
		stop := func(id tracepack.UUID) (admin.Stop, error) {
			out, err := admin.Run(t.Context(), ad, []string{"capture", "stop", id.String()})
			s, _ := out.(admin.Stop)

			return s, err
		}
		if s, err := stop(a); err != nil || s.Outcome != catalog.StopRequested || s.ActionID == "" {
			t.Errorf("stop with the lease held = %+v, %v; want requested", s, err)
		}
		if err := c.ReleaseLease(t.Context(), l); err != nil {
			t.Fatal(err)
		}
		if s, err := stop(b); err != nil || s.Outcome != catalog.StopWritten {
			t.Errorf("stop with the lease released = %+v, %v; want written", s, err)
		}
		if s, err := stop(b); err != nil || s.Outcome != catalog.StopAlready {
			t.Errorf("stop of a stopped capture = %+v, %v; want already", s, err)
		}
		if _, err := stop(gw); !errors.Is(err, catalog.ErrNotLogCapture) {
			t.Errorf("stop of a gateway's capture = %v, want ErrNotLogCapture", err)
		}
		if _, err := admin.Run(t.Context(), ad, []string{"capture", "stop"}); !errors.Is(err, admin.ErrUsage) {
			t.Errorf("capture stop without a capture = %v, want a usage error", err)
		}
	})
}

// checkFeedStreams asserts status's line for each of f's group streams, each holding one message.
func checkFeedStreams(t *testing.T, got []admin.FeedStream, f bus.Feed) {
	t.Helper()
	if len(got) != f.Groups {
		t.Fatalf("status's feed streams %+v, want %d", got, f.Groups)
	}
	for n, fs := range got {
		if fs.Group != n || fs.Stream != bus.FeedStreamName(f, n) || fs.BacklogMessages != 1 || fs.Error != "" {
			t.Errorf("status's group %d stream = %+v, want its one message", n, fs)
		}
	}
}

// status shows each group stream's backlog and their sum with the feed enabled, a stream it cannot read marked so, and nothing of it with the feed disabled.
func TestStatusFeed(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		a, js, traffic := setup(t, c)
		parked, err := js.Stream(ctx, bus.StreamName(cfg, catalog.RoleParking))
		if err != nil {
			t.Fatal(err)
		}
		f := bus.Feed{
			Stream: "tklog", Prefix: "tklog", Groups: 2, MaxBytes: 64 << 20, Duplicates: time.Minute, Replicas: 1,
			AckWait: time.Minute, MaxAckPending: 100,
		}
		if err := bus.EnsureFeed(ctx, js, f, 0, traffic.CachedInfo().Config, parked.CachedInfo().Config); err != nil {
			t.Fatal(err)
		}
		for n := range 2 {
			if _, err := js.Publish(ctx, fmt.Sprintf("tklog.%d.T1", n), []byte("{}")); err != nil {
				t.Fatal(err)
			}
		}
		st, err := a.Status(ctx)
		if err != nil || st.Feed != nil {
			t.Fatalf("status with the feed disabled = %+v, %v; want no feed", st.Feed, err)
		}
		a.Feed = &f
		st, err = a.Status(ctx)
		if err != nil || st.Feed == nil || st.Feed.BacklogMessages != 2 || st.Feed.BacklogBytes == 0 || st.Feed.SilentEquipment != 0 {
			t.Fatalf("status's feed = %+v, %v; want two messages of backlog", st.Feed, err)
		}
		checkFeedStreams(t, st.Feed.Streams, f)

		// A stream that cannot answer is reported as such; the others are still shown.
		if err := js.DeleteStream(ctx, bus.FeedStreamName(f, 1)); err != nil {
			t.Fatal(err)
		}
		st, err = a.Status(ctx)
		if err != nil || st.Feed.BacklogMessages != 1 || st.Feed.Streams[0].BacklogMessages != 1 || st.Feed.Streams[1].Error == "" {
			t.Errorf("status's feed with a stream gone = %+v, %v; want group 0's message and group 1's error", st.Feed, err)
		}
	})
}
