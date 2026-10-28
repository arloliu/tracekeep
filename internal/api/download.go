package api

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/arloliu/tracekeep/internal/store"
)

// opDownload labels the download endpoint's requests in the request metrics.
const opDownload = "Download"

// download serves the object a download token in the path names (api.md §10.1 *Downloads*):
// HEAD and ranged GET as S3 answers them, through the store's ReaderAt; a refused token is forbidden and an absent object not found.
// Neither the path nor the token is logged, and no response is cached or sent as a referrer.
func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	status := s.serveDownload(w, r)
	s.Metrics.Request(opDownload, strconv.Itoa(status), time.Since(start))
}

func (s *Server) serveDownload(w http.ResponseWriter, r *http.Request) int {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return http.StatusMethodNotAllowed
	}
	key, err := s.Downloads.Tokens.Object(r.Context(), strings.TrimPrefix(r.URL.Path, "/download/"))
	if err != nil {
		http.Error(w, "download refused", http.StatusForbidden)
		return http.StatusForbidden
	}
	obj, err := s.Store.Open(r.Context(), key)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return http.StatusNotFound
	}
	if err != nil {
		if s.Log != nil {
			s.Log.Error("api: open a download", "error", err)
		}
		http.Error(w, "the store failed", http.StatusBadGateway)

		return http.StatusBadGateway
	}
	defer obj.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	// ServeContent answers HEAD, Range (206) and its refusals (416); no modification time is sent.
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	http.ServeContent(sw, r, "", time.Time{}, io.NewSectionReader(obj, 0, obj.Size()))

	return sw.status
}

// statusWriter records the status a handler wrote.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}
