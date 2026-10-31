# 300 — Testing

## Organization

- **Unit**: `*_test.go` beside the code (same package or `_test` suffix).
- **Benchmarks**: `*_bench_test.go`.
- **Fuzz**: `Fuzz*` targets for every parser of external input, usually in `fuzz_test.go`.

## Rules

- Test data is synthetic.
  The only recorded input a test may use is a masked or synthetic tracepack pack;
  never copy real traffic, tool IDs, variable IDs or vendor values from `data/` into a test, fixture or golden file.
- No emojis in test output.
- `t.Context()` for contexts, `t.Setenv()` for env.
- `for b.Loop()` for benchmarks.
- Assertions: the standard `testing` package; no mocking framework.
- Cleanup via `t.Cleanup` or `defer`.
- `t.Parallel()` where safe.

## Async

- Never use `time.Sleep` to wait for state; wait on the event or channel that signals it.
- `time.Sleep` is acceptable only to *inject* a delay into the scenario itself.
- A concurrent schedule is **barrier-controlled**, never timed:
  the test holds a goroutine at a named point (between unlock and enqueue, between commit and acknowledgement),
  runs the competing step, then releases it.
  Every schedule the specs list as a gate is such a test.

## Catalog

Every catalog test runs on SQLite and on MariaDB:
the two engines serialize differently, and the design's proofs cover both.

## Bus

Anything that touches JetStream runs against `internal/testbus`:
an embedded nats-server with JetStream on a random port and file storage under `t.TempDir()`.
No test reaches a network or a shared server.
The SDK's tests use `producertest`'s in-memory bus; the SDK module never imports `internal/testbus`.
What needs a real server (the SDK's default connection, `WithConn`, `Preflight` on a live stream) is tested in `producer/internal/bustest`,
a test-only module that imports both and runs, like the SDK, with `GOWORK=off` (T-35).
The packaging jobs are the exception (T-48): the Helm chart on kind and the systemd unit on the runner,
each a CI job of its own that drives the built image or binary against real services and a browser, never part of `make test`.

## Frontend

`web/`'s tests are Vitest with Testing Library on jsdom (`make web-check`).
The API is faked by connect's router transport implementing `MachineService`, and `/auth/` by a stubbed `fetch`;
no test reaches the Go server or a network.
The browser end to end (Playwright) runs only in the packaging jobs above.

## Patterns

Table-driven only when there are multiple cases; a single case is a plain function.

## Running

```bash
make test    # every module, race, -short
make bench
```
