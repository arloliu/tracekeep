package s3store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/store"
)

// lateSigner signs as the SDK would, its signing time late by lag: a stall between PresignGet's clock and the SDK's.
func lateSigner(lag time.Duration, calls *int) func(context.Context, string, time.Duration) (string, error) {
	return func(_ context.Context, key string, lifetime time.Duration) (string, error) {
		*calls++
		signed := time.Now().Add(lag).UTC().Format("20060102T150405Z")

		return fmt.Sprintf("https://s3.example.invalid/packs/%s?X-Amz-Date=%s&X-Amz-Expires=%d&X-Amz-Signature=synthetic", key, signed, int(lifetime/time.Second)), nil
	}
}

// A URL signed late is read back from its own signature and re-signed for what until leaves, never outliving until.
func TestPresignSignedLate(t *testing.T) {
	t.Parallel()
	var calls int
	s := &Store{sign: lateSigner(3*time.Minute, &calls)}
	until := time.Now().Add(15 * time.Minute)
	u, expires, err := s.PresignGet(t.Context(), "archive/c1/p1", until)
	if err != nil {
		t.Fatal(err)
	}
	end, err := SignedEnd(u)
	if err != nil || end.After(until) || !end.Equal(expires) || calls != 2 {
		t.Errorf("signed late: the URL ends %s (%v), reported %s, after %d signings; want it inside %s, reported as signed, re-signed once", end, err, expires, calls, until)
	}
	calls = 0
	s.sign = lateSigner(20*time.Minute, &calls)
	if _, _, err := s.PresignGet(t.Context(), "archive/c1/p1", until); !errors.Is(err, store.ErrNoLifetime) {
		t.Errorf("signed later than until = %v, want ErrNoLifetime", err)
	}
}
