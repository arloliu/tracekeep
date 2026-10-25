# Phase 0 — producer

Status: active (2026-10-26) — phase 0.4 in progress on `feat/producer/proving`

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
| 0.4 — proving | `producer/producertest`, `cmd/tracekeep-sim` (loopback and replay), the `TimeoutSource` fixture; thinning disabled | in-progress |
| 0.5 — gate | the roadmap's phase 0 gate on both resolved module graphs | pending |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 0.4 — proving

Work on the branch `feat/producer/proving`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
`producertest.Bus`, `internal/testbus` with its stop and restart, and the `bustest` module exist already;
`Pending`, `Token` and `TimeoutSource` do not, and a transport event carries no `detail` yet.

Thinning stays disabled, so the sampling rule, `pollChannels`, a token attached to a record and the zero-fill wait until thinning is enabled.
The send path is still built in full where T3 depends on it:
registration is unconditional, and a `TimedOut` on an unbound, ambiguous or unmapped entry emits nothing
(`producer.md` §4.6 *Send-path token protocol*),
so the ambiguity set and the socket binding come with the pending-request table.
The token protocol's signatures are reviewed with eqp-hub's maintainers before phase G and may change;
the fixture and the simulator follow the current sketch in `producer-sdk.md` §4.7 *Send path*.

The simulator is a module of its own until the SDK's first tag (T-38).
It cannot import the SDK's internal packages, so replay drives the loopback pair with a pack's messages and the observers record them;
the host side plays both eqp-hub roles, the forwarder that registers before `ForwardDataMessage` and the waiter that opens the wait,
and the equipment side can withhold a reply, so loopback traffic yields real T3 events.
The harness covers the gate's chaos scenarios against `producertest.Bus`, and the simulator against `internal/testbus` covers the bus going down and up while a tool runs;
the rest of the gate is phase 0.5's.

| Step | Scope | State |
|---|---|---|
| 1 | the simulator module: `cmd/tracekeep-sim` with its own `go.mod` (the SDK and the service through `replace`), the Makefile's `MODULES`, and `100-overview.md`'s module table and workspace sentence | pending |
| 2 | the pending-request table: `Pending().Register`, `Token`'s state machine (first-wins progress, a failure removes only a live entry, the first terminal outcome seals it), expiry after `Awaiting` plus the grace and removal of an entry never awaited, the ambiguity set with its cap, on an injectable clock, with counters for every no-op | pending |
| 3 | binding and T3: the outbound wire observation binds an entry to its socket by session id and System Bytes, a reconnect before it marks the entry ambiguous; a `TimedOut` emits `timer-expiry` to the current capture with that capture's epoch for the socket, the effective timeout carried as a duration and formatted into `detail` by the publisher, so the call allocates nothing | pending |
| 4 | `producertest`: the `TimeoutSource` fixture, a fake waiter on a controllable clock that produces a reply, a timeout, a cancellation and the competing reports, and the conformance check an integrator's `TimeoutSource` runs | pending |
| 5 | `producertest`: the chaos harness that drives disconnects, HotReload, `Stop` under a stalled publisher and fault injection against an integrator's wiring | pending |
| 6 | the simulator's loopback mode: a go-secs HSMS-SS pair on localhost with `secsobs` installed, forwarded primaries through the send path, withheld replies for T3; run against `internal/testbus`, including the bus going down and up while traffic runs | pending |
| 7 | the simulator's replay mode: a synthetic tracepack pack's messages sent through the loopback pair at the pack's pace or faster | pending |
