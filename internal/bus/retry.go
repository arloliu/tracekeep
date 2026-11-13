package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const retryAttempts = 5

// errCodeNoPeers is nats-server's JSClusterNoPeersErrF, a placement with too few peers online; nats.go names no constant for it.
const errCodeNoPeers jetstream.ErrorCode = 10005

// Retry runs f, an idempotent request, until it succeeds, giving each of five attempts its own 10 s deadline and waiting wait before the next:
// a request to a cluster just formed may get no reply in time, a new replicated stream's election running or the request lost,
// and a placement may find a peer the meta leader has not yet heard from, which it reports as offline.
// Any other error ends it at once, as does ctx's end.
// testbus.Retry keeps a copy of its own: the SDK's test module imports testbus, never this package's dependencies.
func Retry(ctx context.Context, wait time.Duration, f func(ctx context.Context) error) error {
	for n := 1; ; n++ {
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := f(actx)
		cancel()
		var api *jetstream.APIError
		passing := errors.Is(err, context.DeadlineExceeded) || errors.As(err, &api) && api.ErrorCode == errCodeNoPeers
		if err == nil || !passing || n == retryAttempts {
			return err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w after %w", ctx.Err(), err)
		case <-timer.C:
		}
	}
}
