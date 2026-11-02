package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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

// A download names its file as an attachment, a pack's the last element of its key and a bundle's <job_id>-<fence>.tar,
// and carries a strong ETag, the quoted hex of the first 16 bytes of the key's SHA-256, on GET, HEAD and a 206 alike;
// one object's tokens give one ETag, and objects whose keys share a last element give two, so If-None-Match and If-Range are answered as S3 does.
// No Last-Modified is sent, and no header carries the token, the key or any prefix of it.
func TestDownloadHeaders(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	mem := store.NewMem()
	body := []byte("0123456789abcdefghij")
	const job = "0192f7a4-f5c1-7d3a-9b2e-4f6a8c0d1e2f"
	keys := map[string]string{
		"staging/t1/a/pack.tpk": "pack.tpk", "staging/t1/b/pack.tpk": "pack.tpk", "exports/" + job + "/2.tar": job + "-2.tar",
	}
	for key := range keys {
		if err := mem.Put(t.Context(), key, bytes.NewReader(body), int64(len(body))); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	tokens := &auth.Downloads{Keys: downloadKeys(t, c), Now: func() time.Time { return now }}
	var logs bytes.Buffer
	s := &Server{Catalog: c, Auth: testAuthenticator(nil), Store: mem, Downloads: Downloads{Tokens: tokens}, Log: slog.New(slog.NewTextHandler(&logs, nil))}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	tags := map[string]string{}
	for key, name := range keys {
		sum := sha256.Sum256([]byte(key))
		etag := `"` + hex.EncodeToString(sum[:16]) + `"`
		tags[key] = etag
		for i := range 2 { // two tokens for one object
			tok, err := tokens.Sign(t.Context(), key, now.Add(time.Duration(i+1)*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			path := "/download/" + tok
			for _, tc := range []struct {
				method string
				header map[string]string
				status int
			}{
				{http.MethodGet, nil, http.StatusOK},
				{http.MethodHead, nil, http.StatusOK},
				{http.MethodGet, map[string]string{"Range": "bytes=2-5"}, http.StatusPartialContent},
				{http.MethodGet, map[string]string{"If-None-Match": etag}, http.StatusNotModified},
				{http.MethodGet, map[string]string{"Range": "bytes=2-5", "If-Range": etag}, http.StatusPartialContent},
				{http.MethodGet, map[string]string{"Range": "bytes=2-5", "If-Range": `"stale"`}, http.StatusOK},
			} {
				resp, got := get(t, srv, tc.method, path, tc.header)
				h := resp.Header
				if resp.StatusCode != tc.status || h.Get("ETag") != etag || h.Get("Last-Modified") != "" ||
					tc.status != http.StatusNotModified && h.Get("Content-Disposition") != "attachment; filename="+name {
					t.Errorf("%s %s %v = %d, ETag %q, Content-Disposition %q, Last-Modified %q; want %d, %s, attachment %s, none",
						tc.method, key, tc.header, resp.StatusCode, h.Get("ETag"), h.Get("Content-Disposition"), h.Get("Last-Modified"), tc.status, etag, name)
				}
				if tc.status == http.StatusOK && tc.method == http.MethodGet && !bytes.Equal(got, body) {
					t.Errorf("%s %v: body %q, want the whole object", key, tc.header, got)
				}
				for hk, vs := range h {
					v := strings.Join(vs, " ")
					if strings.Contains(v, tok) || strings.Contains(v, "staging") || strings.Contains(v, "exports") || strings.Contains(v, "t1") {
						t.Errorf("%s %s: header %s %q carries the token or part of the key", tc.method, key, hk, v)
					}
				}
			}
		}
	}
	if tags["staging/t1/a/pack.tpk"] == tags["staging/t1/b/pack.tpk"] {
		t.Error("two objects whose keys share a last element share an ETag")
	}
	if logs.Len() != 0 {
		t.Errorf("a download logged %s", logs.String())
	}
}
