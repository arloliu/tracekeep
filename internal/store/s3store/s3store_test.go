package s3store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
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
	endpoint := os.Getenv(envEndpoint)
	if endpoint == "" {
		t.Skipf("%s not set: no S3-compatible store to test against", envEndpoint)
	}
	bucket := "tk-" + strings.ToLower(rand.Text())
	s, err := s3store.New(s3store.Config{
		Endpoint: endpoint, Bucket: bucket,
		AccessKeyID: os.Getenv(envKey), SecretAccessKey: os.Getenv(envSecret),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client().CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = s.List(ctx, "", func(info store.ObjectInfo) error { return s.Delete(ctx, info.Key) })
		if _, err := s.Client().DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
			t.Errorf("delete bucket: %v", err)
		}
	})

	return s
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

// A missing bucket is a misconfiguration, never a missing key: a delete in it fails instead of succeeding quietly.
func TestMissingBucketIsNotAMissingKey(t *testing.T) {
	t.Parallel()
	newStore(t) // skips without a store to test against
	missing, err := s3store.New(s3store.Config{
		Endpoint: os.Getenv(envEndpoint), Bucket: "tk-missing-" + strings.ToLower(rand.Text()),
		AccessKeyID: os.Getenv(envKey), SecretAccessKey: os.Getenv(envSecret),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := missing.Delete(t.Context(), "staging/x"); err == nil {
		t.Error("Delete in a missing bucket succeeded")
	}
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
