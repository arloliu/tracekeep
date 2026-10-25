# Phase 0 — producer

Status: active (2026-10-25) — the repository bootstrap closed; phase 0.1 next

Milestone 0 of the [roadmap](../ROADMAP.md):
the producer SDK, in this repository only, with the simulator and the embedded bus that prove it.
Its scope and gate are the roadmap's phase 0 row;
the design is `docs/specs/tracekeep/producer.md`, `producer-sdk.md` and `architecture.md` §3.4.
This plan only orders the work.

## Before it starts

- `producer-sdk.md` §4.7 amended for nats.go v1.55.0 and the nats.go and nats-server facts the specs cite re-verified (T-19).
- Item 4 of [P1](../specs/tracekeep/proposals/p1-interfaces.md) applied to the specs (accepted with `Begin`, `Rotate` and `End`, T-26);
  item 5's token protocol fixed far enough for the `TimeoutSource` fixture of phase 0.4.

## Phases

| Phase | Scope | State |
|---|---|---|
| 0.1 — core | `producer`: capture object, seq and clock, lanes, rotation and the lifecycle lock, the dependency barrier, `start` publication, claim-check puts, the retry horizon, `Metrics` and `Logger` | pending |
| 0.2 — observers | `producer/secsobs`: wire, socket, lifecycle and transaction events to records, the frame copy, the socket→epoch map; HSMS-SS only | pending |
| 0.3 — bus | `producer/busconfig`, `Preflight`, `internal/testbus` | pending |
| 0.4 — proving | `producer/producertest`, `cmd/tracekeep-sim` (loopback and replay), the `TimeoutSource` fixture; thinning disabled | pending |
| 0.5 — gate | the roadmap's phase 0 gate on both resolved module graphs | pending |

Only the phase under way is detailed; the others are detailed when they start.
