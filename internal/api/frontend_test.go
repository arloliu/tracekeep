package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

// build is a frontend build: its page, a hashed script and a file at its root.
var frontendBuild = fstest.MapFS{
	"index.html":           {Data: []byte("<!DOCTYPE html><title>app</title>")},
	"assets/index-abc1.js": {Data: []byte("console.log(1)")},
	"favicon.svg":          {Data: []byte("<svg/>")},
}

func frontendServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := &Server{Auth: testAuthenticator(&failures{}), Web: frontendBuild}
	srv := httptest.NewServer(s.routes(&recorder{}))
	t.Cleanup(srv.Close)

	return srv
}

// The frontend answers every path no other route claims (api.md §10.3 *Serving*):
// its files with their types, index.html for a client route, not found for a missing asset, GET and HEAD only,
// each answer with the fixed security headers; the services, /auth/, /download/ and unclean paths keep their answers.
func TestFrontendServing(t *testing.T) {
	t.Parallel()
	srv := frontendServer(t)
	for _, tc := range []struct {
		method, path string
		status       int
		body, ctype  string
		cache        string
		frontend     bool
	}{
		{http.MethodGet, "/", http.StatusOK, "<title>app</title>", "text/html", "no-cache", true},
		{http.MethodGet, "/records?tool=T-1", http.StatusOK, "<title>app</title>", "text/html", "no-cache", true},
		{http.MethodGet, "/exports/0f3a", http.StatusOK, "<title>app</title>", "text/html", "no-cache", true},
		{http.MethodGet, "/assets/index-abc1.js", http.StatusOK, "console.log(1)", "text/javascript", "public, max-age=31536000, immutable", true},
		{http.MethodGet, "/favicon.svg", http.StatusOK, "<svg/>", "image/svg+xml", "no-cache", true},
		{http.MethodGet, "/assets/gone-0000.js", http.StatusNotFound, "", "", "", true},
		{http.MethodGet, "/assets", http.StatusOK, "<title>app</title>", "text/html", "no-cache", true},
		{http.MethodHead, "/", http.StatusOK, "", "text/html", "no-cache", true},
		{http.MethodPost, "/", http.StatusMethodNotAllowed, "", "", "", true},
		{http.MethodDelete, "/records", http.StatusMethodNotAllowed, "", "", "", true},
		// Not the frontend's.
		{http.MethodPost, "/tracekeep.v1.Unknown/Call", http.StatusNotFound, "", "", "", false},
		{http.MethodGet, "/tracekeep.v1.Unknown/", http.StatusNotFound, "", "", "", false},
		{http.MethodGet, "/auth/me", http.StatusNotFound, "", "", "", false},
		{http.MethodGet, "/download/x", http.StatusNotFound, "", "", "", false},
		{http.MethodGet, "/a/../records", http.StatusNotFound, "", "", "", false},
		{http.MethodGet, "/records/", http.StatusNotFound, "", "", "", false},
	} {
		req, err := http.NewRequestWithContext(t.Context(), tc.method, srv.URL+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.URL.Opaque = tc.path[:strings.IndexByte(tc.path+"?", '?')] // send the path as written, unclean included
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		name := tc.method + " " + tc.path
		if resp.StatusCode != tc.status {
			t.Errorf("%s = %d, want %d", name, resp.StatusCode, tc.status)
			continue
		}
		if tc.body != "" && !strings.Contains(string(b), tc.body) {
			t.Errorf("%s answered %q, want %q", name, b, tc.body)
		}
		if tc.ctype != "" && !strings.HasPrefix(resp.Header.Get("Content-Type"), tc.ctype) {
			t.Errorf("%s typed %q, want %q", name, resp.Header.Get("Content-Type"), tc.ctype)
		}
		if tc.cache != "" && resp.Header.Get("Cache-Control") != tc.cache {
			t.Errorf("%s cached %q, want %q", name, resp.Header.Get("Cache-Control"), tc.cache)
		}
		if tc.status == http.StatusMethodNotAllowed && resp.Header.Get("Allow") != "GET, HEAD" {
			t.Errorf("%s allows %q, want GET, HEAD", name, resp.Header.Get("Allow"))
		}
		csp := resp.Header.Get("Content-Security-Policy")
		if tc.frontend != (csp == frontendCSP) {
			t.Errorf("%s CSP %q, want the frontend's: %t", name, csp, tc.frontend)
		}
		if tc.frontend && (resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("Referrer-Policy") != "same-origin") {
			t.Errorf("%s headers = %v, want nosniff and same-origin", name, resp.Header)
		}
	}
}

// Without a build nothing is served, the headers still set.
func TestFrontendWithoutABuild(t *testing.T) {
	t.Parallel()
	s := &Server{Auth: testAuthenticator(&failures{})}
	srv := httptest.NewServer(s.routes(&recorder{}))
	t.Cleanup(srv.Close)
	resp, err := srv.Client().Get(srv.URL + "/") //nolint:noctx // a test's probe
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || resp.Header.Get("Content-Security-Policy") != frontendCSP {
		t.Errorf("/ without a build = %d %v, want not found with the headers", resp.StatusCode, resp.Header)
	}
}
