package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

const objectKey = "staging/t1/pack.tpk"

// downloads is a server holding one object, its download tokens at the clock now, and a log written to logs.
func downloads(t *testing.T, now *time.Time, logs *bytes.Buffer) (*httptest.Server, *auth.Downloads, []byte) {
	t.Helper()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	mem := store.NewMem()
	body := []byte("0123456789abcdefghij")
	if err := mem.Put(t.Context(), objectKey, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	tokens := &auth.Downloads{Keys: downloadKeys(t, c), Now: func() time.Time { return *now }}
	s := &Server{Catalog: c, Auth: testAuthenticator(nil), Store: mem, Downloads: Downloads{Tokens: tokens},
		Log: slog.New(slog.NewTextHandler(logs, nil))}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	return srv, tokens, body
}

func get(t *testing.T, srv *httptest.Server, method, path string, header map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return resp, b
}

// The API answers HEAD and ranged GET of the object a token names, as S3 does, uncached and with no referrer.
func TestDownload(t *testing.T) {
	t.Parallel()
	now := time.Now()
	var logs bytes.Buffer
	srv, tokens, body := downloads(t, &now, &logs)
	tok, err := tokens.Sign(t.Context(), objectKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	path := "/download/" + tok
	resp, got := get(t, srv, http.MethodGet, path, nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) || resp.Header.Get("Cache-Control") != "no-store" ||
		resp.Header.Get("Referrer-Policy") != "no-referrer" || resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("GET = %d %q, headers %v", resp.StatusCode, got, resp.Header)
	}
	resp, got = get(t, srv, http.MethodGet, path, map[string]string{"Range": "bytes=2-5"})
	if resp.StatusCode != http.StatusPartialContent || string(got) != "2345" || resp.Header.Get("Content-Range") != "bytes 2-5/20" {
		t.Errorf("ranged GET = %d %q %s, want 206 of bytes 2-5", resp.StatusCode, got, resp.Header.Get("Content-Range"))
	}
	resp, got = get(t, srv, http.MethodGet, path, map[string]string{"Range": "bytes=15-"})
	if resp.StatusCode != http.StatusPartialContent || string(got) != "fghij" {
		t.Errorf("suffix GET = %d %q", resp.StatusCode, got)
	}
	resp, got = get(t, srv, http.MethodHead, path, nil)
	if resp.StatusCode != http.StatusOK || len(got) != 0 || resp.ContentLength != int64(len(body)) {
		t.Errorf("HEAD = %d, %d bytes, length %d", resp.StatusCode, len(got), resp.ContentLength)
	}
	if resp, _ := get(t, srv, http.MethodPost, path, nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d, want 405", resp.StatusCode)
	}
	now = now.Add(time.Minute)
	if resp, got := get(t, srv, http.MethodGet, path, nil); resp.StatusCode != http.StatusForbidden || bytes.Contains(got, body) {
		t.Errorf("GET at the expiry = %d, want refused", resp.StatusCode)
	}
	if strings.Contains(logs.String(), tok) {
		t.Errorf("the log holds the token:\n%s", logs.String())
	}
}

func TestDownloadRefusals(t *testing.T) {
	t.Parallel()
	now := time.Now()
	var logs bytes.Buffer
	srv, tokens, _ := downloads(t, &now, &logs)
	absent, err := tokens.Sign(t.Context(), "staging/t1/absent.tpk", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := tokens.Sign(t.Context(), objectKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		path string
		want int
	}{
		"an absent object": {"/download/" + absent, http.StatusNotFound},
		"a changed token":  {"/download/" + valid[:len(valid)-2] + "AA", http.StatusForbidden},
		"no token":         {"/download/", http.StatusForbidden},
		"garbage":          {"/download/x.y", http.StatusForbidden},
	} {
		if resp, _ := get(t, srv, http.MethodGet, tc.path, nil); resp.StatusCode != tc.want {
			t.Errorf("%s: GET = %d, want %d", name, resp.StatusCode, tc.want)
		}
	}
	if strings.Contains(logs.String(), valid[:20]) || strings.Contains(logs.String(), absent[:20]) {
		t.Errorf("the log holds a token:\n%s", logs.String())
	}
}
