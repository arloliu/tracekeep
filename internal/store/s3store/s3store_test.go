package s3store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store"
	"github.com/arloliu/tracekeep/internal/store/storetest"
)

// The suite needs a store: `make test-s3` starts each in a container and sets these;
// without them the tests skip, so `make test` stays offline.
const (
	envEndpoint = "TRACEKEEP_S3_ENDPOINT"
	envKey      = "TRACEKEEP_S3_ACCESS_KEY"
	envSecret   = "TRACEKEEP_S3_SECRET_KEY"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) store.ObjectStore { return newStore(t) })
}

// A presigned URL reads the object, and one of an absent key is refused as not found, not for its signature:
// the check the api role makes at start (storage.md §5.2 *Downloads*).
func TestPresignGet(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	data := []byte("pack bytes")
	if err := s.Put(t.Context(), "archive/c1/p1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if got, status := fetch(t, s, "archive/c1/p1"); status != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("presigned GET = %d %q, want 200 %q", status, got, data)
	}
	if _, status := fetch(t, s, "archive/c1/none"); status != http.StatusNotFound {
		t.Errorf("presigned GET of an absent key = %d, want 404", status)
	}
}

// newStore returns a store on a bucket of its own, emptied and removed when the test ends.
func newStore(t *testing.T) *s3store.Store {
	t.Helper()
	s, _ := newBucket(t, true)

	return s
}

// newBucket returns a store on a bucket of its own and the bucket's name; the bucket is emptied when the test ends,
// and removed then when remove is set, as a test that removes it itself does not.
func newBucket(t *testing.T, remove bool) (*s3store.Store, string) {
	t.Helper()
	if os.Getenv(envEndpoint) == "" {
		t.Skipf("%s not set: no S3-compatible store to test against", envEndpoint)
	}
	bucket := "tk-" + strings.ToLower(rand.Text())
	s := storeOn(t, bucket, os.Getenv(envSecret))
	if _, err := s.Client().CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = s.List(ctx, "", func(info store.ObjectInfo) error { return s.Delete(ctx, info.Key) })
		if !remove {
			return
		}
		if _, err := s.Client().DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
			t.Errorf("delete bucket: %v", err)
		}
	})

	return s, bucket
}

func fetch(t *testing.T, s *s3store.Store, key string) ([]byte, int) {
	t.Helper()
	url, _, err := s.PresignGet(t.Context(), key, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return body, resp.StatusCode
}

// A missing bucket is a misconfiguration, never a missing key: Stat, Open, a ranged read and a delete in it fail as store errors,
// where a missing key in a bucket that exists is store.ErrNotFound. Denied access is a store error too.
// A HEAD's 404 carries no body naming the bucket, so Stat and Open are where a store could blur the two.
func TestMissingBucketIsNotAMissingKey(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, bucket := newBucket(t, true) // skips without a store to test against
	data := []byte("pack bytes")
	if err := s.Put(ctx, "archive/c1/p1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	notFound := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s = %v, want store.ErrNotFound", what, err)
		}
	}
	storeError := func(what string, err error) {
		t.Helper()
		if err == nil || errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s = %v, want a store error, not store.ErrNotFound", what, err)
		}
	}

	_, err := s.Stat(ctx, "archive/c1/none")
	notFound("Stat of a missing key", err)
	_, err = s.Open(ctx, "archive/c1/none")
	notFound("Open of a missing key", err)
	notFound("a ranged read of a removed key", readRemoved(t, s, "archive/c1/p1", ""))

	missing := storeOn(t, "tk-missing-"+strings.ToLower(rand.Text()), os.Getenv(envSecret))
	_, err = missing.Stat(ctx, "archive/c1/p1")
	storeError("Stat in a missing bucket", err)
	_, err = missing.Open(ctx, "archive/c1/p1")
	storeError("Open in a missing bucket", err)
	storeError("Delete in a missing bucket", missing.Delete(ctx, "staging/x"))
	gone, goneBucket := newBucket(t, false)
	if err := gone.Put(ctx, "archive/c1/p1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	storeError("a ranged read in a removed bucket", readRemoved(t, gone, "archive/c1/p1", goneBucket))

	if err := s.Put(ctx, "archive/c1/p1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	denied := storeOn(t, bucket, "not-the-secret")
	_, err = denied.Stat(ctx, "archive/c1/p1")
	storeError("Stat denied", err)
	_, err = denied.Open(ctx, "archive/c1/p1")
	storeError("Open denied", err)
	storeError("Delete denied", denied.Delete(ctx, "archive/c1/p1"))
}

// A HEAD's 404 names neither the key nor the bucket, so Stat and Open ask whether the bucket is there:
// a missing bucket is a store error, and only a missing key in a bucket that exists is store.ErrNotFound.
// The endpoint is a fake that answers as MinIO and Ceph RGW do, every HEAD that misses with a bare 404.
func TestStatAsksTheBucketAfterA404(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/packs" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	for _, tc := range []struct {
		bucket string
		found  bool
	}{{"packs", true}, {"missing", false}} {
		s, err := s3store.New(s3store.Config{Endpoint: srv.URL, Bucket: tc.bucket, AccessKeyID: "ak", SecretAccessKey: "sk"})
		if err != nil {
			t.Fatal(err)
		}
		_, statErr := s.Stat(t.Context(), "archive/c1/p1")
		_, openErr := s.Open(t.Context(), "archive/c1/p1")
		for _, err := range []error{statErr, openErr} {
			if errors.Is(err, store.ErrNotFound) != tc.found || err == nil {
				t.Errorf("bucket %s: %v, want store.ErrNotFound %t", tc.bucket, err, tc.found)
			}
		}
	}
}

// readRemoved opens key, removes it, and its bucket too when bucket names it, then reads the object it opened by range.
func readRemoved(t *testing.T, s *s3store.Store, key, bucket string) error {
	t.Helper()
	ctx := t.Context()
	o, err := s.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if err := s.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if bucket != "" {
		if _, err := s.Client().DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
			t.Fatal(err)
		}
	}
	_, err = o.ReadAt(make([]byte, 4), 0)

	return err
}

// storeOn returns a store on bucket, which need not exist, signing with secret.
func storeOn(t *testing.T, bucket, secret string) *s3store.Store {
	t.Helper()
	s, err := s3store.New(s3store.Config{
		Endpoint: os.Getenv(envEndpoint), Bucket: bucket, AccessKeyID: os.Getenv(envKey), SecretAccessKey: secret,
	})
	if err != nil {
		t.Fatal(err)
	}

	return s
}

// A presigned URL never outlives until, and the expiry PresignGet returns is when it stops reading: its signed date plus its lifetime.
// Presigning needs no store, so this runs without one.
func TestPresignStaysInsideUntil(t *testing.T) {
	t.Parallel()
	s, err := s3store.New(s3store.Config{Endpoint: "https://s3.example.invalid", Bucket: "packs", Region: "us-east-1", AccessKeyID: "ak", SecretAccessKey: "sk"})
	if err != nil {
		t.Fatal(err)
	}
	for _, life := range []time.Duration{15 * time.Minute, 15*time.Minute + 700*time.Millisecond, 3 * time.Second} {
		until := time.Now().Add(life)
		u, expires, err := s.PresignGet(t.Context(), "archive/c1/p1", until)
		if err != nil {
			t.Fatal(err)
		}
		q, err := neturl.ParseQuery(u[strings.Index(u, "?")+1:])
		if err != nil {
			t.Fatal(err)
		}
		signed, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
		if err != nil {
			t.Fatal(err)
		}
		secs, err := strconv.Atoi(q.Get("X-Amz-Expires"))
		if err != nil {
			t.Fatal(err)
		}
		end := signed.Add(time.Duration(secs) * time.Second)
		if end.After(until) || !end.Equal(expires) {
			t.Errorf("lifetime %s: the URL ends %s, reported %s; want it inside %s, reported as signed", life, end, expires, until)
		}
	}
	if _, _, err := s.PresignGet(t.Context(), "archive/c1/p1", time.Now().Add(-time.Second)); !errors.Is(err, store.ErrNoLifetime) {
		t.Errorf("PresignGet until a past time = %v, want ErrNoLifetime", err)
	}
}

// A service that takes a put's body and never answers holds no call past its bound:
// the response-header timeout ends a call whose context has no deadline, and a context's end ends any other.
// A local server stands in for the service, so this runs without one.
func TestPutEndsWhenTheServiceNeverAnswers(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		guard := time.NewTimer(10 * time.Second) // a call no bound ends is answered after it, and the test fails
		defer guard.Stop()
		select {
		case <-release:
		case <-guard.C:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	data := []byte("pack bytes")
	s, err := s3store.New(s3store.Config{
		Endpoint: srv.URL, Bucket: "packs", AccessKeyID: "ak", SecretAccessKey: "sk", ResponseHeaderTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	//nolint:usetesting // a call without a deadline is the case
	if err := s.Put(context.Background(), "staging/a", bytes.NewReader(data), int64(len(data))); err == nil || !strings.Contains(err.Error(), "timeout awaiting response headers") {
		t.Errorf("Put without a deadline = %v, want the response-header timeout", err)
	}

	s, err = s3store.New(s3store.Config{Endpoint: srv.URL, Bucket: "packs", AccessKeyID: "ak", SecretAccessKey: "sk", ResponseHeaderTimeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := s.Put(ctx, "staging/b", bytes.NewReader(data), int64(len(data))); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Put under a deadline = %v, want the deadline", err)
	}
}
