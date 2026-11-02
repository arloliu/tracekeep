package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
)

// opDownload labels the download endpoint's requests in the request metrics.
const opDownload = "Download"

// statusGone counts a download whose client went (api.md §10.1 *Downloads*); it is never sent.
const statusGone = 499

// download serves the object a download token in the path names (api.md §10.1 *Downloads*):
// HEAD and ranged GET as S3 answers them, through the store's ReaderAt; a refused token is forbidden and an absent object not found.
// Neither the path nor the token is logged, and no response is cached or sent as a referrer.
// A client gone is answered nothing more, counted under statusGone and never logged.
func (s *Server) download(w *downloadWriter, r *http.Request) {
	start := time.Now()
	s.serveDownload(w, r)
	status := w.status
	if w.gone {
		status = statusGone
	}
	s.Metrics.Request(opDownload, strconv.Itoa(status), time.Since(start))
}

func (s *Server) serveDownload(w *downloadWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}
	key, expiry, err := s.Downloads.Tokens.ObjectUntil(r.Context(), strings.TrimPrefix(r.URL.Path, "/download/"))
	if err != nil {
		w.gone = r.Context().Err() != nil
		if !w.gone {
			http.Error(w, "download refused", http.StatusForbidden)
		}

		return
	}
	// The download ends at the token's expiry plus the margin, at most its object's deadline, so no read it serves outlives the object.
	// Measured by the downloads' clock, the one the token's expiry is judged by.
	ctx, cancel := context.WithTimeout(r.Context(), expiry.Add(s.Downloads.Margin).Sub(s.Downloads.now()))
	defer cancel()
	obj, err := records.Open(ctx, s.Store, key)
	if err == nil {
		defer obj.Close()
	}
	switch {
	case r.Context().Err() != nil:
		w.gone = true

		return
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)

		return
	case err != nil:
		// The budget's end or the store's failure, before any header: keyless, as records.Open makes it.
		s.downloadFailed(http.StatusBadGateway, err)
		http.Error(w, "the store failed", http.StatusBadGateway)

		return
	}
	// The transfer ends with ctx: each read checks it, whatever the store does with it, and the writes are bounded by its deadline.
	if dl, ok := ctx.Deadline(); ok {
		_ = http.NewResponseController(w).SetWriteDeadline(dl)
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": fileName(key)}))
	// A key is never reused (storage.md §5.2), so an opaque digest of it is a strong validator, which ServeContent answers If-None-Match and If-Range by.
	sum := sha256.Sum256([]byte(key))
	h.Set("ETag", `"`+hex.EncodeToString(sum[:16])+`"`)
	rd := &ctxReaderAt{ctx: ctx, r: obj, size: obj.Size()}
	// ServeContent answers HEAD, Range (206) and its refusals (416); no modification time is sent.
	// Its status is sent before its first read, so a failure it meets ends the body short of its length, which the client sees cut.
	http.ServeContent(w, r.WithContext(ctx), "", time.Time{}, io.NewSectionReader(rd, 0, obj.Size()))
	// A failed write ends the request's context too, so a write's failure is judged by its own error:
	// past the download's deadline, the budget ended; anything else, the client went.
	switch {
	case w.err != nil && errors.Is(w.err, os.ErrDeadlineExceeded):
		s.downloadFailed(w.status, errors.New("the response's write passed the download's deadline"))
	case w.err != nil, rd.err != nil && r.Context().Err() != nil:
		w.gone = true
	case rd.err != nil:
		s.downloadFailed(w.status, rd.err)
	default:
	}
}

// fileName is the name a download is saved under, the only part of its key a header carries:
// a bundle's <job_id>-<fence>.tar from exports/<job_id>/<fence>.tar, else the key's last element.
func fileName(key string) string {
	if rest, ok := strings.CutPrefix(key, "exports/"); ok {
		if job, fence, ok := strings.Cut(rest, "/"); ok && !strings.Contains(fence, "/") {
			return job + "-" + fence
		}
	}

	return path.Base(key)
}

// downloadFailed logs a download's failure, its error keyless, as one line.
func (s *Server) downloadFailed(status int, err error) {
	if s.Log != nil {
		s.Log.Error("api: download failed", "status", status, "error", err)
	}
}

// ctxReaderAt is r whose reads fail once ctx is done; err is the first read's failure.
// An end of file before size, the object's, is a failure too, never the transfer's end.
type ctxReaderAt struct {
	ctx  context.Context //nolint:containedctx // the download's context, which bounds every read of its transfer
	r    io.ReaderAt
	size int64
	err  error
}

func (c *ctxReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := 0, c.ctx.Err()
	if err == nil {
		n, err = c.r.ReadAt(p, off)
	}
	if err != nil && c.err == nil && (!errors.Is(err, io.EOF) || off+int64(n) < c.size) {
		c.err = err
	}

	return n, err
}

// downloadWriter stamps every response it commits as uncached and sent as no referrer, whatever the handler removed before,
// and records the status, the first write's failure, and whether the client went.
type downloadWriter struct {
	http.ResponseWriter
	status int
	err    error
	gone   bool
}

func (d *downloadWriter) WriteHeader(status int) {
	if d.status == 0 {
		d.status = status
		h := d.Header()
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
	}
	d.ResponseWriter.WriteHeader(status)
}

// Unwrap lets an http.ResponseController reach the connection, to bound a transfer's writes.
func (d *downloadWriter) Unwrap() http.ResponseWriter { return d.ResponseWriter }

func (d *downloadWriter) Write(b []byte) (int, error) {
	if d.status == 0 {
		d.WriteHeader(http.StatusOK)
	}
	n, err := d.ResponseWriter.Write(b)
	if err != nil && d.err == nil {
		d.err = err
	}

	return n, err
}
