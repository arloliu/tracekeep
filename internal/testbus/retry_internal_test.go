package testbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// script returns an f for attempt that answers each attempt with the next of errs, and counts the attempts.
func script(errs ...error) (f func(context.Context) error, calls *int) {
	calls = new(int)

	return func(context.Context) error {
		*calls++
		if *calls > len(errs) {
			return errors.New("an attempt past the script")
		}

		return errs[*calls-1]
	}, calls
}

func TestRetry(t *testing.T) {
	t.Parallel()
	peerOffline := &jetstream.APIError{Code: 400, ErrorCode: errCodeNoPeers, Description: "no suitable peers for placement, peer offline"}
	permanent := errors.New("permanent")
	exhausted := make([]error, retryAttempts)
	for i := range exhausted {
		exhausted[i] = peerOffline
	}
	for _, tc := range []struct {
		name  string
		errs  []error
		want  error
		calls int
	}{
		{"peer offline then success", []error{peerOffline, nil}, nil, 2},
		{"deadline then success", []error{context.DeadlineExceeded, nil}, nil, 2},
		{"permanent error", []error{permanent, nil}, permanent, 1},
		{"exhaustion", exhausted, peerOffline, retryAttempts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, calls := script(tc.errs...)
			err := attempt(t.Context(), 0, f)
			if !errors.Is(err, tc.want) || (tc.want == nil) != (err == nil) || *calls != tc.calls {
				t.Errorf("retry = %v after %d attempts, want %v after %d", err, *calls, tc.want, tc.calls)
			}
		})
	}
}

// A test that ends while Retry waits to try again ends the retry at once, its wait never served.
func TestRetryCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	err := attempt(ctx, time.Hour, func(context.Context) error {
		calls++
		cancel()

		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Errorf("retry = %v after %d attempts, want canceled after 1", err, calls)
	}
}
