# Phase 0 — producer

Status: active (2026-10-26) — phase 0.3 in progress on `feat/producer/bus`

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
| 0.2 — observers | `producer/secsobs`: wire, socket, lifecycle and transaction events to records, the frame copy, the socket→epoch map; HSMS-SS only | done (#3) |
| 0.3 — bus | `producer/busconfig`, `Preflight`, `internal/testbus`, and the default connection the SDK dials (until then `New` needs `WithBus`) | in-progress |
| 0.4 — proving | `producer/producertest`, `cmd/tracekeep-sim` (loopback and replay), the `TimeoutSource` fixture; thinning disabled | pending |
| 0.5 — gate | the roadmap's phase 0 gate on both resolved module graphs | pending |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 0.3 — bus

Work on the branch `feat/producer/bus`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
`internal/testbus` exists already (c528a10).
What needs a real server is tested in `producer/internal/bustest` (T-35), which step 3 creates;
`busconfig.ConsumerConfig` waits for ingest in phase 1, which owns the flush interval its `ack_wait` depends on.

| Step | Scope | State |
|---|---|---|
| 1 | `busconfig`: the traffic stream's and the claim bucket's settings of [STO §4] *Stream settings* and T-28, the bucket's stream name, the duplicate window | pending |
| 2 | `Preflight` over `Bus` (`StreamInfo`, `CheckConn`), against `producertest`'s bus: stream and bucket settings, `max_payload`, TLS, the reconnect buffer, the current capture's `start` and descriptor | pending |
| 3 | the default connection: `New` dials in the background with the correctness-bearing options, the two publish option sets, chunked claim objects; `WithConn`; a bus not yet connected counts as down; the `bustest` module; removes the `shortcut:` in `producer.go` | pending |
| 4 | against `internal/testbus`: the synchronous `start` with the production option set, a publish while disconnected fails at once, a multi-chunk claim object at `max_payload` 4140, `Preflight` naming each misconfiguration, and its background run once connected | pending |
