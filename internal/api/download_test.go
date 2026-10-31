package api

import (
	"bytes"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
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
	// A download checks its token alone: a session cookie, valid or not, neither admits nor refuses it.
	cookie := map[string]string{"Cookie": auth.SessionCookie + "=not-a-session"}
	if resp, got := get(t, srv, http.MethodGet, path, cookie); resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Errorf("GET with a session cookie beside the token = %d", resp.StatusCode)
	}
	if resp, _ := get(t, srv, http.MethodGet, "/download/", cookie); resp.StatusCode != http.StatusForbidden {
		t.Errorf("GET with a session cookie and no token = %d, want refused", resp.StatusCode)
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

// Every download response is uncached and sent with no referrer, a refused range included,
// and an unclean download path is refused or not found, never redirected with its token.
func TestDownloadEdges(t *testing.T) {
	t.Parallel()
	now := time.Now()
	var logs bytes.Buffer
	srv, tokens, _ := downloads(t, &now, &logs)
	tok, err := tokens.Sign(t.Context(), objectKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	noRedirect := &http.Client{Transport: srv.Client().Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for name, tc := range map[string]struct {
		path, rng string
		want      int
	}{
		"a range past the end": {"/download/" + tok, "bytes=100-", http.StatusRequestedRangeNotSatisfiable},
		"a doubled slash":      {"/download//" + tok, "", http.StatusForbidden},
		"a dot segment":        {"/download/./" + tok, "", http.StatusForbidden},
		"a leading double":     {"//download/" + tok, "", http.StatusNotFound},
		"a nested path":        {"/download/" + tok + "/x", "", http.StatusForbidden},
	} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+tc.path, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		if tc.rng != "" {
			req.Header.Set("Range", tc.rng)
		}
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want || resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Referrer-Policy") != "no-referrer" ||
			resp.Header.Get("Location") != "" {
			t.Errorf("%s: %d, headers %v; want %d, uncached, no referrer, no redirect", name, resp.StatusCode, resp.Header, tc.want)
		}
	}
}

// Without download tokens (presigned downloads) the API serves no download path, refusing it without a panic.
func TestNoDownloadsWithoutTokens(t *testing.T) {
	t.Parallel()
	s := &Server{Catalog: catalogtest.Open(t, catalog.EngineSQLite), Auth: testAuthenticator(nil), Store: store.NewMem()}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	if resp, _ := get(t, srv, http.MethodGet, "/download/x.y", nil); resp.StatusCode != http.StatusNotFound || resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("a download without tokens = %d, %v; want not found", resp.StatusCode, resp.Header)
	}
}

// With people's sign-in enabled, a download still checks its token alone:
// a valid session of every role neither admits a request without a token nor stands in for one.
func TestDownloadIgnoresASession(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	dir := t.TempDir()
	write := func(name string, b byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	k, err := keys.New(keys.Config{
		Declared: map[string]keys.Declaration{
			"dl-1": {Provider: keys.ProviderFile, Ref: write("dl-1", 9)},
			"s-1":  {Provider: keys.ProviderFile, Ref: write("s-1", 8), Kind: catalog.KindSession},
		},
		Download: keys.Purpose{Issuing: "dl-1", Accepted: []string{"dl-1"}},
		Session:  keys.Purpose{Issuing: "s-1", Accepted: []string{"s-1"}},
	}, c)
	if err != nil {
		t.Fatal(err)
	}
	mem := store.NewMem()
	body := []byte("0123456789")
	if err := mem.Put(t.Context(), objectKey, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	const public = "https://tk.example.invalid"
	cookies := &auth.Cookies{Keys: k}
	people := auth.NewPeople(auth.PeopleConfig{ClientID: "tk-web", PublicURL: public, SessionLifetime: time.Hour},
		auth.NewVerifier(auth.OIDCConfig{Issuer: "https://127.0.0.1:1", Audience: "tk-services"}), cookies)
	tokens := &auth.Downloads{Keys: k}
	s := &Server{Catalog: c, Auth: auth.NewAuthenticator(auth.AuthenticatorConfig{People: people}), People: people, Store: mem,
		Downloads: Downloads{Tokens: tokens}, Log: slog.New(slog.DiscardHandler)}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	session, err := cookies.SealSession(t.Context(), auth.Session{Subject: "person-1", Roles: []string{auth.RoleAdmin, auth.RolePackReader, auth.RoleExporter}, Expiry: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	h := map[string]string{"Cookie": auth.SessionCookie + "=" + session, "Origin": public}
	if resp, got := get(t, srv, http.MethodGet, "/download/", h); resp.StatusCode != http.StatusForbidden || bytes.Contains(got, body) {
		t.Errorf("a session and no token = %d, want refused", resp.StatusCode)
	}
	tok, err := tokens.Sign(t.Context(), objectKey, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if resp, got := get(t, srv, http.MethodGet, "/download/"+tok, h); resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Errorf("a session beside a valid token = %d", resp.StatusCode)
	}
}

// Without people's sign-in, /auth/ is not found, still with no-store; an unclean /auth/ path is never redirected.
func TestAuthNotConfigured(t *testing.T) {
	t.Parallel()
	now := time.Now()
	var logs bytes.Buffer
	srv, _, _ := downloads(t, &now, &logs)
	for _, path := range []string{"/auth/login", "/auth/callback?code=c", "/auth//callback?code=c"} {
		resp, _ := get(t, srv, http.MethodGet, path, nil)
		if resp.StatusCode != http.StatusNotFound || resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Location") != "" {
			t.Errorf("GET %s = %d, Cache-Control %q, Location %q; want 404, no-store, none", path, resp.StatusCode, resp.Header.Get("Cache-Control"), resp.Header.Get("Location"))
		}
	}
}
