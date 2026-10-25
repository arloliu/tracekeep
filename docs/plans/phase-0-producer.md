# Phase 0 — producer

Status: active (2026-10-25) — phase 0.2 in progress on `feat/producer/secsobs`

Milestone 0 of the [roadmap](../ROADMAP.md):
the producer SDK, in this repository only, with the simulator and the embedded bus that prove it.
Its scope and gate are the roadmap's phase 0 row;
the design is `docs/specs/tracekeep/producer.md`, `producer-sdk.md` and `architecture.md` §3.4.
This plan only orders the work.

## Before it starts

- `producer-sdk.md` §4.7 amended for nats.go v1.55.0 and the nats.go and nats-server facts the specs cite re-verified (T-19; done, nothing the specs rely on changed).
- Item 4 of P1 (module interfaces, T-30) applied to the specs (done: `Begin`, `Rotate` and `End`, `secsobs` observer options, the narrow `Bus`);
  item 5's token protocol fixed far enough for the `TimeoutSource` fixture of phase 0.4 (done: `producer.md` §4.6 *Send-path token protocol*, `producer-sdk.md` §4.7 *Send path*; reviewed with eqp-hub's maintainers before phase G).

## Phases

| Phase | Scope | State |
|---|---|---|
| 0.1 — core | `producer`: capture object, seq and clock, lanes, rotation and the lifecycle lock, the dependency barrier, `start` publication, claim-check puts, the retry horizon, `Metrics` and `Logger` | done (#1, 6d651da) |
| 0.2 — observers | `producer/secsobs`: wire, socket, lifecycle and transaction events to records, the frame copy, the socket→epoch map; HSMS-SS only | in-progress |
| 0.3 — bus | `producer/busconfig`, `Preflight`, `internal/testbus`, and the default connection the SDK dials (until then `New` needs `WithBus`) | pending |
| 0.4 — proving | `producer/producertest`, `cmd/tracekeep-sim` (loopback and replay), the `TimeoutSource` fixture; thinning disabled | pending |
| 0.5 — gate | the roadmap's phase 0 gate on both resolved module graphs | pending |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 0.2 — observers

Work on the branch `feat/producer/secsobs`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The observers are exercised with synthetic go-secs events called directly;
the go-secs loopback pair that drives them over a real connection is `cmd/tracekeep-sim`'s, in phase 0.4.

| Step | Scope | State |
|---|---|---|
| 1 | transport events through the lanes: an observation carries its event as plain fields and the publisher encodes it with tracepack, so an event callback allocates nothing | done (6740387) |
| 2 | `secsobs` observers: wire frames (kind from the SType byte, direction from the host vantage, the borrowed frame copied only once admitted), socket events, go-secs's own T3 from the transaction observer; `ConnOptions` | done (2e629e6) |
| 3 | `AttachLifecycle`: lifecycle events to state transitions with the cause mapping of tracepack-go.md §5.1 | in-progress |
