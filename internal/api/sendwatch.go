package api

import (
	"cmp"
	"context"
	"net/http"
	"time"
)

// sendChunk is the most one write hands the connection at a time, under a write deadline of its own,
// so a client that keeps reading completes each chunk within the send timeout however large the message;
// one taking less than a chunk per send timeout is ended, as api.md §10.1 *Bounds* states.
const sendChunk = 16 << 10

// sendWatch is a QueryRecords stream's response writer (api.md §10.1 *Records* *Bounds*):
// a write or flush that makes no progress for the send timeout fails, ending the stream, and is remembered as stalled.
// Between writes no deadline is left on the connection, which a keep-alive's next request reuses.
type sendWatch struct {
	http.ResponseWriter
	rc       *http.ResponseController
	timeout  time.Duration
	deadline time.Time
	stalled  bool
}

// sendWatchKey is the context key of a stream's sendWatch.
type sendWatchKey struct{}

// watchSends wraps a QueryRecords request's writer in a sendWatch the handler finds in its context.
func (s *Server) watchSends(w http.ResponseWriter, r *http.Request) (http.ResponseWriter, *http.Request) {
	sw := &sendWatch{ResponseWriter: w, rc: http.NewResponseController(w), timeout: cmp.Or(s.Records.SendTimeout, time.Minute)}

	return sw, r.WithContext(context.WithValue(r.Context(), sendWatchKey{}, sw))
}

// sendStalled tells whether ctx's stream ended because a send made no progress.
func sendStalled(ctx context.Context) bool {
	sw, ok := ctx.Value(sendWatchKey{}).(*sendWatch)

	return ok && sw.stalled
}

func (sw *sendWatch) Write(p []byte) (int, error) {
	n := 0
	defer sw.disarm()
	for n < len(p) {
		sw.arm()
		m, err := sw.ResponseWriter.Write(p[n:min(len(p), n+sendChunk)])
		n += m
		if err != nil {
			sw.check()
			return n, err
		}
	}

	return n, nil
}

// Flush implements http.Flusher, which connect requires of a stream's writer.
func (sw *sendWatch) Flush() {
	sw.arm()
	if err := sw.rc.Flush(); err != nil {
		sw.check()
	}
	sw.disarm()
}

// Unwrap lets an http.ResponseController reach the connection.
func (sw *sendWatch) Unwrap() http.ResponseWriter { return sw.ResponseWriter }

func (sw *sendWatch) arm() {
	sw.deadline = time.Now().Add(sw.timeout)
	_ = sw.rc.SetWriteDeadline(sw.deadline) // a writer without deadlines, as a test's recorder, is never stalled
}

func (sw *sendWatch) disarm() { _ = sw.rc.SetWriteDeadline(time.Time{}) }

// check marks a failed write stalled when its deadline had passed.
func (sw *sendWatch) check() { sw.stalled = sw.stalled || !time.Now().Before(sw.deadline) }
