# Phase 0 — producer

Status: active (2026-10-25) — phase 0.1 in progress on `feat/producer/core`

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
| 0.1 — core | `producer`: capture object, seq and clock, lanes, rotation and the lifecycle lock, the dependency barrier, `start` publication, claim-check puts, the retry horizon, `Metrics` and `Logger` | in-progress |
| 0.2 — observers | `producer/secsobs`: wire, socket, lifecycle and transaction events to records, the frame copy, the socket→epoch map; HSMS-SS only | pending |
| 0.3 — bus | `producer/busconfig`, `Preflight`, `internal/testbus` | pending |
| 0.4 — proving | `producer/producertest`, `cmd/tracekeep-sim` (loopback and replay), the `TimeoutSource` fixture; thinning disabled | pending |
| 0.5 — gate | the roadmap's phase 0 gate on both resolved module graphs | pending |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 0.1 — core

Work on the branch `feat/producer/core`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Concurrent schedules are barrier-controlled (`.agents/rules/300-testing.md`):
the core holds named test hooks, nil outside tests, that a test sets to hold a goroutine at a point.

| Step | Scope | State |
|---|---|---|
| 1 | `Metrics` and `Logger` interfaces with no-op defaults; `Config`, `Tunables` and their defaults; the descriptor type and `recorder_instance_id` (UUIDv5, length-prefixed) | pending |
| 2 | capture object: the lock and the fields it guards, seq allocation, the clock check with ε, the reservation set, the socket→epoch map, the fault state; a benchmark showing the critical section allocates nothing | pending |
| 3 | lanes: the reserved lane (count bound, never drops) and the ordinary lane (count and byte budgets, drops the newest); the observer callback's steps 1–8 with its recover | pending |
| 4 | `Begin`, `Rotate` and `End` on the lifecycle goroutine: the lifecycle lock and generation, rotation with its commit point, abandonment, fault rotation with backoff | pending |
| 5 | `Bus` and its message types, a minimal in-memory `Bus` in `producertest` (brought forward from 0.4, which completes it), the publisher goroutine: the dependency barrier, `start` publication, claim-check puts with read-back, the retry horizon H, `End`'s drain | pending |
