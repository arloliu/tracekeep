package bustest_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/link"
)

// stallMarkers are the start of a payload of repeated "ABC", raw and as base64, however the server delivers it.
var stallMarkers = [][]byte{[]byte("ABCABCABCABCABCABC"), []byte("QUJDQUJDQUJDQUJDQUJD")}

// stallConn passes the server's bytes through until a stall marker arrives, then withholds it and all that follows
// until the connection closes, as a link that stops delivering mid-object does.
type stallConn struct {
	net.Conn
	stalled chan struct{} // closed once the marker is withheld
	closed  chan struct{}
	once    sync.Once
	tail    []byte
	held    bool
}

func (c *stallConn) Dial(network, address string) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	c.Conn = conn

	return c, err
}

func (c *stallConn) Read(p []byte) (int, error) {
	if c.held {
		<-c.closed
		return 0, net.ErrClosed
	}
	n, err := c.Conn.Read(p)
	seen := slices.Concat(c.tail, p[:n])
	for _, m := range stallMarkers {
		if i := bytes.Index(seen, m); i >= 0 {
			c.held = true
			close(c.stalled)
			if cut := i - len(c.tail); cut > 0 {
				return cut, nil
			}

			return c.Read(p)
		}
	}
	c.tail = append(c.tail[:0], seen[max(len(seen)-len(stallMarkers[1]), 0):]...)

	return n, err
}

func (c *stallConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// A claim read whose body stops arriving on the wire returns once its context is cancelled,
// not at its deadline, so Close never waits out a stalled read-back.
func TestClaimReadStallReturnsOnCancel(t *testing.T) {
	t.Parallel()
	b := newBus(t)
	sc := &stallConn{stalled: make(chan struct{}), closed: make(chan struct{})}
	nc, err := nats.Connect(b.url, nats.ReconnectBufSize(-1), nats.NoReconnect(), nats.SetCustomDialer(sc))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	def, err := link.Bus(nc)
	if err != nil {
		t.Fatalf("default bus: %v", err)
	}
	pb, ok := def.(producer.Bus)
	if !ok {
		t.Fatalf("default bus %T is no producer.Bus", def)
	}
	ctx := t.Context()
	objs, err := pb.Objects(ctx, bucket)
	if err != nil {
		t.Fatalf("objects: %v", err)
	}
	payload := bytes.Repeat([]byte("ABC"), 200<<10) // five chunks
	if _, err := objs.Put(ctx, producer.ObjectMeta{Name: "stalled", ChunkSize: 128 << 10}, payload); err != nil {
		t.Fatalf("put: %v", err)
	}

	const deadline = 10 * time.Second
	getCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := objs.Get(getCtx, "stalled")
		done <- err
	}()
	select {
	case <-sc.stalled:
	case err := <-done:
		t.Fatalf("Get returned %v before its body stalled", err)
	}
	start := time.Now()
	cancel()
	err = <-done
	if took := time.Since(start); took > deadline/5 {
		t.Errorf("Get returned %v after cancellation; want it promptly, well before its %v deadline", took, deadline)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Get: %v; want context.Canceled", err)
	}
}
