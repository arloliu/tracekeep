// Package s3store is the ObjectStore on an S3-compatible service (storage.md §5.2, T-34),
// built on aws-sdk-go-v2; it is also a store.Presigner.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/arloliu/tracekeep/internal/store"
)

// readAhead is the least a ranged read fetches, so the small reads that walk a pack's structure share a request.
const readAhead = 64 << 10

// defaultRegion is signed into requests when Config names none; S3-compatible stores accept any.
const defaultRegion = "us-east-1"

var (
	errNegativeOffset = errors.New("s3store: negative offset")
	errBodySize       = errors.New("s3store: body length differs from size")
)

// Config names the bucket and how to reach it.
type Config struct {
	// Endpoint is the URL of an S3-compatible service; empty means AWS itself.
	Endpoint string
	// Region defaults to us-east-1.
	Region string
	// Bucket holds every object of the store; it must exist.
	Bucket string
	// AccessKeyID and SecretAccessKey are static credentials.
	// shortcut: static credentials only; add the SDK's default chain when a deployment authenticates by role.
	AccessKeyID     string
	SecretAccessKey string
}

// Store is an ObjectStore on one bucket.
type Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	// sign presigns a GET of key for lifetime; the SDK's, or a test's that signs late.
	sign func(ctx context.Context, key string, lifetime time.Duration) (string, error)
}

var (
	_ store.ObjectStore = (*Store)(nil)
	_ store.Presigner   = (*Store)(nil)
)

// New returns a store on cfg's bucket; it does not contact the service.
func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3store: no bucket")
	}
	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}
	client := s3.New(s3.Options{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
	}, func(o *s3.Options) {
		if cfg.Endpoint == "" {
			return
		}
		// The SDK's default checksums and virtual-host addressing break some S3-compatible stores (T-34).
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	st := &Store{client: client, presign: s3.NewPresignClient(client), bucket: cfg.Bucket}
	st.sign = func(ctx context.Context, key string, lifetime time.Duration) (string, error) {
		req, err := st.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &st.bucket, Key: &key}, s3.WithPresignExpires(lifetime))
		if err != nil {
			return "", err
		}

		return req.URL, nil
	}

	return st, nil
}

// Client is the store's S3 client, for setup the ObjectStore interface does not cover, such as creating the bucket in tests.
func (s *Store) Client() *s3.Client {
	return s.client
}

// Put implements store.ObjectStore with one PutObject, which is atomic and durable when it succeeds.
func (s *Store) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	// A body of another length would go out as a malformed request, which the client retries before failing.
	if n, err := remaining(r); err != nil || n != size {
		return fmt.Errorf("s3store: put %s: body of %d bytes, size %d: %w", key, n, size, errors.Join(err, errBodySize))
	}
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket, Key: &key, Body: r, ContentLength: &size,
	})
	if err != nil {
		return fmt.Errorf("s3store: put %s: %w", key, err)
	}

	return nil
}

// Open implements store.ObjectStore; the object's reads use ctx.
func (s *Store) Open(ctx context.Context, key string) (store.Object, error) {
	info, err := s.Stat(ctx, key)
	if err != nil {
		return nil, err
	}

	return &object{ctx: ctx, s: s, key: key, size: info.Size}, nil
}

// Stat implements store.ObjectStore.
func (s *Store) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return store.ObjectInfo{}, wrap("stat", key, err)
	}

	return store.ObjectInfo{Key: key, Size: aws.ToInt64(out.ContentLength)}, nil
}

// List implements store.ObjectStore.
func (s *Store) List(ctx context.Context, prefix string, fn func(store.ObjectInfo) error) error {
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("s3store: list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(store.ObjectInfo{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size)}); err != nil {
				return err
			}
		}
	}

	return ctx.Err()
}

// Delete implements store.ObjectStore.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	if err = wrap("delete", key, err); err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	return nil
}

// PresignGet implements store.Presigner.
// A signature carries the SDK's signing time and its lifetime, each in whole seconds, and the URL stops reading at their sum:
// the lifetime is computed from the clock before signing, and the sum is read back from the signed URL itself,
// a URL signed late re-signed for what until leaves then; the sum is the expiry returned.
func (s *Store) PresignGet(ctx context.Context, key string, until time.Time) (string, time.Time, error) {
	lifetime := until.Sub(time.Now().Truncate(time.Second)).Truncate(time.Second)
	for range 3 {
		if lifetime < time.Second {
			return "", time.Time{}, fmt.Errorf("s3store: presign %s: %w", key, store.ErrNoLifetime)
		}
		u, err := s.sign(ctx, key, lifetime)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("s3store: presign %s: %w", key, err)
		}
		end, err := SignedEnd(u)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("s3store: presign %s: %w", key, err)
		}
		if !end.After(until) {
			return u, end, nil
		}
		lifetime -= end.Sub(until).Truncate(time.Second) + time.Second
	}

	return "", time.Time{}, fmt.Errorf("s3store: presign %s: the signatures keep outliving %s", key, until)
}

// SignedEnd is the time a presigned URL stops reading: its X-Amz-Date plus its X-Amz-Expires.
func SignedEnd(u string) (time.Time, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return time.Time{}, errors.New("a presigned URL that does not parse")
	}
	q := parsed.Query()
	signed, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	if err != nil {
		return time.Time{}, errors.New("a presigned URL without its X-Amz-Date")
	}
	secs, err := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
	if err != nil || secs <= 0 {
		return time.Time{}, errors.New("a presigned URL without its X-Amz-Expires")
	}

	return signed.Add(time.Duration(secs) * time.Second), nil
}

// get fills buf from key's bytes at from with one ranged GET.
func (s *Store) get(ctx context.Context, key string, from int64, buf []byte) error {
	rng := fmt.Sprintf("bytes=%d-%d", from, from+int64(len(buf))-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key, Range: &rng})
	if err != nil {
		return wrap("read", key, err)
	}
	defer out.Body.Close()
	// A proxy that ignores Range answers with the whole object, whose bytes would land at the wrong offset.
	if out.ContentRange == nil {
		return fmt.Errorf("s3store: read %s: the store ignored the range", key)
	}
	if _, err := io.ReadFull(out.Body, buf); err != nil {
		return fmt.Errorf("s3store: read %s: %w", key, err)
	}

	return nil
}

// remaining returns the bytes r holds from its current offset, leaving the offset where it was.
func remaining(r io.Seeker) (int64, error) {
	cur, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := r.Seek(cur, io.SeekStart); err != nil {
		return 0, err
	}

	return end - cur, nil
}

// wrap names the operation, and maps a 404 to store.ErrNotFound unless the bucket is what is missing.
func wrap(op, key string, err error) error {
	if err == nil {
		return nil
	}
	// Every API error of the SDK has a code (smithy.APIError); a local interface keeps smithy-go out of our imports.
	var api interface{ ErrorCode() string }
	if errors.As(err, &api) && api.ErrorCode() == "NoSuchBucket" {
		return fmt.Errorf("s3store: %s %s: %w", op, key, err)
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
		return fmt.Errorf("%w: %s", store.ErrNotFound, key)
	}

	return fmt.Errorf("s3store: %s %s: %w", op, key, err)
}

// object reads by range and keeps the last read-ahead window.
type object struct {
	ctx  context.Context
	s    *Store
	key  string
	size int64

	mu     sync.Mutex
	win    []byte
	winOff int64
}

func (o *object) Size() int64 { return o.size }

func (o *object) Close() error { return nil }

func (o *object) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errNegativeOffset
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= o.size {
		return 0, io.EOF
	}
	want := min(int64(len(p)), o.size-off)
	switch {
	case o.cached(p[:want], off):
	case want >= readAhead || off+want == o.size: // nothing to read ahead
		if err := o.s.get(o.ctx, o.key, off, p[:want]); err != nil {
			return 0, err
		}
	default:
		buf := make([]byte, min(readAhead, o.size-off))
		if err := o.s.get(o.ctx, o.key, off, buf); err != nil {
			return 0, err
		}
		copy(p, buf)
		o.mu.Lock()
		o.win, o.winOff = buf, off
		o.mu.Unlock()
	}
	if want < int64(len(p)) {
		return int(want), io.EOF
	}

	return int(want), nil
}

// cached fills p from the window when the window holds all of it.
func (o *object) cached(p []byte, off int64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if off < o.winOff || off+int64(len(p)) > o.winOff+int64(len(o.win)) {
		return false
	}
	copy(p, o.win[off-o.winOff:])

	return true
}
