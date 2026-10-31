package api

import (
	"bytes"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

// frontendCSP is every frontend answer's Content-Security-Policy (api.md §10.3 *Serving*):
// inline styles admitted for the components' positioning, never an inline script.
const frontendCSP = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; " +
	"object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

// frontend serves the frontend's build at a path no other route claims (api.md §10.3 *Serving*):
// GET and HEAD only; a file of the build with its type, cached for good under /assets/ and revalidated otherwise;
// index.html for a path naming no file, so a client route reloads, but not found under /assets/.
// The files are public and the request metrics do not count them.
func (s *Server) frontend(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", frontendCSP)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "same-origin")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}
	if s.Web == nil {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/")
	if fi, err := fs.Stat(s.Web, name); name == "" || err != nil || fi.IsDir() {
		if strings.HasPrefix(r.URL.Path, "/assets/") {
			http.NotFound(w, r)
			return
		}
		name = "index.html"
	}
	b, err := fs.ReadFile(s.Web, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if strings.HasPrefix(name, "assets/") {
		h.Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		h.Set("Cache-Control", "no-cache")
	}
	// ServeContent types the file by its name, answers HEAD and ranges, and never redirects.
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(b))
}
