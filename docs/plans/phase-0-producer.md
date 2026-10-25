# Phase 0 — producer

Status: active (2026-10-26) — phase 0.5 in progress on `feat/producer/gate`

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
| 0.3 — bus | `producer/busconfig`, `Preflight`, `internal/testbus`, and the default connection the SDK dials (until then `New` needs `WithBus`) | done (#4) |
| 0.4 — proving | `producer/producertest`, `cmd/tracekeep-sim` (loopback and replay), the `TimeoutSource` fixture; thinning disabled | done (#7) |
| 0.5 — gate | the roadmap's phase 0 gate on both resolved module graphs | in-progress |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 0.5 — gate

Work on the branch `feat/producer/gate`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Most gate items already have a test from phases 0.1–0.4:
a duplicate `Start`, `Stop` with a stalled publisher, frame reuse after the callback, a publish during a disconnect, a multi-chunk claim object at `max_payload` 4140, `Preflight` on each misconfiguration,
and the pause between the unlock and the enqueue of a clock step.
This phase adds the schedules no test holds yet and tightens the items a test covers only in part.

Every barrier schedule is a test that holds a goroutine at a named hook and runs the competing step (`300-testing.md`),
and is checked against a mutation of the ordering it guards: the test must fail when that ordering is broken.
A hook is a nil function check, added with the first test that needs it.
One sits inside the capture's critical section:
§4.2 lets a callback that holds the old lock finish against an abandoned capture, and only a callback held there can show that;
the check allocates nothing and touches nothing the section does not, and the allocation tests run again with it.
The package `producer` reaches the core's hooks through `internal/link`, as it reaches the recorder.

| Step | Scope | State |
|---|---|---|
| 1 | core schedules on the existing hooks: opposing clock steps from two callbacks, a callback held across A→B→C rotations, `Stop` after a callback loaded the pointer, an abandoned rotation's callback resumed after a later `Start` | done (a53471e) |
| 2 | `Stop` and the capture lock: a callback held inside the critical section while `End` passes its deadline (the capture is abandoned, the record dropped as `abandoned`, the `Report` says so), and a HotReload held in its `TryLock` loop when `Stop` arrives (it returns `ErrRotationAbandoned`, its `start` is never published, `End` returns by its deadline) | done (8832b07) |
| 3 | faults and the lifecycle: `End` between `handleFault`'s two lifecycle-lock sections, a fault rotation racing a HotReload, and the reserved lane filled while the bus is down, then the successor after it comes back | done (dd8cfbc) |
| 4 | the publisher: the `start` goes by the synchronous publish (a call count on `producertest.Bus`) with the production option set (one attempt against `internal/testbus`); an uncertain upload is read back, deleted and put again before its reference is published (an operation log on `producertest.Bus`) | done (0864a65) |
| 5 | drops visible as gaps: the chaos harness checks for seq holes in the bus-down scenario, and the simulator's bus-down test runs with a small lane and retry horizon so its outage drops records | pending |
| 6 | both resolved module graphs: the pinned `go.mod` gives the minimum nats.go; a Makefile target and a CI job run the SDK's and `bustest`'s tests on a copy with the latest nats.go | pending |

Step 6's job fails when a nats.go release breaks the SDK, which is its purpose; a red `main` from it is fixed in the SDK or recorded, not silenced.
Absolute bounds on each device verb stand for "no tool-side latency change": a ratio against a run with the bus up would flake under the race detector.
Once the phase merges, the plan closes and the roadmap's phase 0 row is marked done.
