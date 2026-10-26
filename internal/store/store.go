// Package store holds the object stores packs live in (docs/specs/tracekeep/storage.md §5.2):
// the ObjectStore interface, its mem and fs backends, and the optional Presigner.
// The s3 backend is the package s3store; every backend passes the conformance suite in storetest.
package store

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound reports a key that holds no object.
var ErrNotFound = errors.New("store: object not found")

// ObjectStore keeps whole objects under keys.
// Pack keys are exactly the tracepack storage profile's, and no key is overwritten (storage.md §5.2).
type ObjectStore interface {
	// Put stores size bytes of r under key.
	// The object is visible only once complete, and a nil error means it is durable;
	// an error means nothing is visible, or the caller treats the key as unknown and the reconciler decides.
	// r is seekable, so a backend can sign or resend the body (T-34).
	Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error
	// Open opens the object under key for reading; an absent key returns an error wrapping ErrNotFound.
	Open(ctx context.Context, key string) (Object, error)
	// Stat describes the object under key; an absent key returns an error wrapping ErrNotFound.
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	// List calls fn for every object whose key starts with prefix, in no promised order,
	// and stops at fn's first error, which it returns.
	List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error
	// Delete removes the object under key; an absent key is not an error.
	Delete(ctx context.Context, key string) error
}

// Object is an opened object; ReadAt may be called concurrently.
type Object interface {
	io.ReaderAt
	// Size is the object's length in bytes.
	Size() int64
	// Close releases the object.
	Close() error
}

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Key  string
	Size int64
}

// Presigner is the optional capability of a store that hands readers a URL (storage.md §5.2 *Downloads*).
type Presigner interface {
	// PresignGet returns a URL that reads the object under key until lifetime has passed.
	PresignGet(ctx context.Context, key string, lifetime time.Duration) (string, error)
}
