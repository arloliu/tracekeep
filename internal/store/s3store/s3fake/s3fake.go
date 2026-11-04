// Package s3fake is an S3-compatible endpoint for tests of what reads through the s3 store, offline:
// one bucket's objects, path-style, answered as MinIO and Ceph RGW answer (storage.md §5.3).
// Only tests import it.
package s3fake

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arloliu/tracekeep/internal/store/s3store"
)

// Bucket is the one bucket a Server holds.
const Bucket = "packs"

// Server answers a HEAD that misses with a bare 404, a GET that misses naming NoSuchKey or NoSuchBucket,
// and a range from past an object's end with a 416.
type Server struct {
	srv  *httptest.Server
	mu   sync.Mutex
	objs map[string][]byte
	held chan struct{}
	// Heads counts the HEADs it answered, of objects and of the bucket.
	Heads atomic.Int64
	// IgnoreRange, when set, answers a GET with the whole object and no Content-Range, as a proxy ignoring Range does.
	IgnoreRange atomic.Bool
}

// New starts a Server, closed when tb ends.
func New(tb testing.TB) *Server {
	tb.Helper()
	s := &Server{objs: map[string][]byte{}}
	s.srv = httptest.NewServer(s)
	tb.Cleanup(s.srv.Close)

	return s
}

// Store is an s3 store on bucket of the Server; one other than Bucket is missing.
func (s *Server) Store(tb testing.TB, bucket string) *s3store.Store {
	tb.Helper()
	st, err := s3store.New(s3store.Config{Endpoint: s.srv.URL, Bucket: bucket, AccessKeyID: "ak", SecretAccessKey: "sk"})
	if err != nil {
		tb.Fatal(err)
	}

	return st
}

// Edit runs fn on the objects by key, which it may change, under the Server's lock.
func (s *Server) Edit(fn func(objs map[string][]byte)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.objs)
}

// HoldNextGet makes the next GET wait for its request's end unanswered; the channel closes when that GET arrives.
func (s *Server) HoldNextGet() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.held = make(chan struct{})

	return s.held
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if r.Method == http.MethodHead {
		s.Heads.Add(1)
	}
	switch {
	case bucket != Bucket && r.Method == http.MethodHead:
		w.WriteHeader(http.StatusNotFound)
	case bucket != Bucket:
		s3Error(w, http.StatusNotFound, "NoSuchBucket")
	case key == "": // the bucket's HEAD
	case r.Method == http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.Edit(func(objs map[string][]byte) { objs[key] = body })
	case r.Method == http.MethodHead:
		s.head(w, key)
	default:
		s.get(w, r, key)
	}
}

func (s *Server) head(w http.ResponseWriter, key string) {
	s.mu.Lock()
	obj, ok := s.objs[key]
	s.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(obj)))
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, key string) {
	s.mu.Lock()
	obj, ok := s.objs[key]
	held := s.held
	s.held = nil
	s.mu.Unlock()
	switch {
	case held != nil:
		close(held)
		<-r.Context().Done()

		return
	case !ok:
		s3Error(w, http.StatusNotFound, "NoSuchKey")
		return
	case s.IgnoreRange.Load():
		_, _ = w.Write(obj)
		return
	default:
	}
	from, to, _ := strings.Cut(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-")
	lo, err1 := strconv.Atoi(from)
	hi, err2 := strconv.Atoi(to)
	switch {
	case err1 != nil || err2 != nil:
		w.WriteHeader(http.StatusBadRequest)
		return
	case lo >= len(obj):
		s3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
		return
	default:
	}
	hi = min(hi, len(obj)-1)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi, len(obj)))
	w.Header().Set("Content-Length", strconv.Itoa(hi-lo+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(obj[lo : hi+1])
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}
