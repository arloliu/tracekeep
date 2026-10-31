package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
)

// opDownload labels the download endpoint's requests in the request metrics.
const opDownload = "Download"

// download serves the object a download token in the path names (api.md §10.1 *Downloads*):
// HEAD and ranged GET as S3 answers them, through the store's ReaderAt; a refused token is forbidden and an absent object not found.
// Neither the path nor the token is logged, and no response is cached or sent as a referrer.
func (s *Server) download(w *downloadWriter, r *http.Request) {
	start := time.Now()
	s.serveDownload(w, r)
	s.Metrics.Request(opDownload, strconv.Itoa(w.status), time.Since(start))
}

func (s *Server) serveDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}
	key, expiry, err := s.Downloads.Tokens.ObjectUntil(r.Context(), strings.TrimPrefix(r.URL.Path, "/download/"))
	if err != nil {
		http.Error(w, "download refused", http.StatusForbidden)
		return
	}
	// The download ends at the token's expiry plus the margin, at most its object's deadline, so no read it serves outlives the object.
	// Measured by the downloads' clock, the one the token's expiry is judged by.
	ctx, cancel := context.WithTimeout(r.Context(), expiry.Add(s.Downloads.Margin).Sub(s.Downloads.now()))
	defer cancel()
	r = r.WithContext(ctx)
	obj, err := records.Open(ctx, s.Store, key)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		if s.Log != nil {
			s.Log.Error("api: open a download", "error", err) // keyless, as records.Open makes it
		}
		http.Error(w, "the store failed", http.StatusBadGateway)

		return
	}
	defer obj.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	// ServeContent answers HEAD, Range (206) and its refusals (416); no modification time is sent.
	http.ServeContent(w, r, "", time.Time{}, io.NewSectionReader(obj, 0, obj.Size()))
}

// downloadWriter stamps every response it commits as uncached and sent as no referrer, whatever the handler removed before,
// and records the status.
type downloadWriter struct {
	http.ResponseWriter
	status int
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

func (d *downloadWriter) Write(b []byte) (int, error) {
	if d.status == 0 {
		d.WriteHeader(http.StatusOK)
	}

	return d.ResponseWriter.Write(b)
}
