package bus_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/testbus"
)

// Preflight's probe passes a mapping into G partitions, its probes stored nowhere,
// and refuses no mapping, a mapping into more partitions than G, even one more, and one into fewer.
func TestProbeFeedMapping(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		groups, partitions int
		ok                 bool
	}{
		"G partitions":  {4, 4, true},
		"no mapping":    {4, 0, false},
		"more than G":   {4, 8, false},
		"fewer than G":  {4, 2, false},
		"one partition": {4, 1, false},
		"twice as many": {4, 16, false},
		// The first probes of these reach every group before one falls outside them.
		"one group, two partitions":    {1, 2, false},
		"two groups, three partitions": {2, 3, false},
		"one group":                    {1, 1, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			srv := testbus.StartServer(t, testbus.Options{})
			if tc.partitions > 0 {
				srv.MapFeed("tklog", tc.partitions)
			}
			nc, err := nats.Connect(srv.URL())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			f := feed(tc.groups, 1)
			if err := bus.EnsureFeed(ctx, js, f, 0, trafficStream, parkedStream); err != nil {
				t.Fatal(err)
			}
			err = bus.ProbeFeedMapping(ctx, nc, js, f)
			if tc.ok && err != nil || !tc.ok && !errors.Is(err, bus.ErrFeedMapping) {
				t.Fatalf("the probe over %d partitions = %v, want ok %v", tc.partitions, err, tc.ok)
			}
			if msgs, _, err := bus.FeedBacklog(ctx, js, f); err != nil || msgs != 0 {
				t.Errorf("the feed after the probe holds %d, %v; want nothing", msgs, err)
			}
		})
	}
}

// A probe whose connection drops while it waits for the mapped copy fails with ErrFeedUnavailable, not as a missing mapping:
// the copy may have been lost with the connection. The server maps nothing, so the copy never comes; the drop is made once the probe published.
func TestProbeFeedMappingConnectionDropped(t *testing.T) {
	t.Parallel()
	srv := testbus.StartServer(t, testbus.Options{})
	nc, err := nats.Connect(srv.URL(), nats.ReconnectWait(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	f := feed(1, 1)
	// Another connection sees the probe's publish on its unmapped subject, so the drop follows the publish.
	watch, err := nats.Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	published, err := watch.SubscribeSync(f.Prefix + "._tracekeep_probe_0")
	if err != nil {
		t.Fatal(err)
	}
	if err := watch.Flush(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- bus.ProbeFeedMapping(t.Context(), nc, js, f) }()
	if _, err := published.NextMsg(10 * time.Second); err != nil {
		t.Fatalf("the probe's publish: %v", err)
	}
	srv.Stop()
	srv.Restart()
	if err := <-done; !errors.Is(err, bus.ErrFeedUnavailable) || errors.Is(err, bus.ErrFeedMapping) {
		t.Errorf("the probe over a dropped connection = %v, want ErrFeedUnavailable alone", err)
	}
}
