// Package s3store is the ObjectStore on an S3-compatible service (storage.md §5.2, T-34),
// built on aws-sdk-go-v2; it is also a store.Presigner.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/arloliu/tracekeep/internal/store"
)

// readAhead is the least a ranged read fetches, so the small reads that walk a pack's structure share a request.
const readAhead = 64 << 10

// defaultRegion is signed into requests when Config names none; S3-compatible stores accept any.
const defaultRegion = "us-east-1"

var errNegativeOffset = errors.New("s3store: negative offset")

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

	return &Store{client: client, presign: s3.NewPresignClient(client), bucket: cfg.Bucket}, nil
}

// Client is the store's S3 client, for setup the ObjectStore interface does not cover, such as creating the bucket in tests.
func (s *Store) Client() *s3.Client {
	return s.client
}

// Put implements store.ObjectStore with one PutObject, which is atomic and durable when it succeeds.
func (s *Store) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
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

	return nil
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
func (s *Store) PresignGet(ctx context.Context, key string, lifetime time.Duration) (string, error) {
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}, s3.WithPresignExpires(lifetime))
	if err != nil {
		return "", fmt.Errorf("s3store: presign %s: %w", key, err)
	}

	return req.URL, nil
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

// wrap names the operation, and maps a 404 to store.ErrNotFound unless the bucket is what is missing.
func wrap(op, key string, err error) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
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
