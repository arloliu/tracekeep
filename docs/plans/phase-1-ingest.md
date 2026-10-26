# Phase 1 — ingest

Status: active (2026-10-26) — phase 1.3 in progress

Milestone 1 of the [roadmap](../ROADMAP.md):
the `ingest` role with its stores, catalog and reconciler, and `tracekeep dev` as the single-process loop.
Its scope and gate are the roadmap's phase 1 row and the implementation checklist under it;
the design is `docs/specs/tracekeep/storage.md`, `catalog.md`, `ingest.md`, `parking.md`, `rollouts.md` and `architecture.md` §3.1.
This plan only orders the work.

## Before it starts

- The SDK's first tag, which the service needs to require the SDK by version (T-17, T-27): `producer/v0.1.0` (done, 8db8928).
  Phase 1.3 adds the requirement, since the service reads the bus through `busconfig`;
  phase 1.6 folds the simulator back into the service module (T-38).
- The eqp-hub API review (T-26) runs beside this plan; nothing here waits for it.

## Phases

| Phase | Scope | State |
|---|---|---|
| 1.1 — stores | `internal/store`: `ObjectStore`, `mem`, `fs`, `s3` with `Presigner` and ranged reads, the conformance suite against MinIO and Ceph RGW in CI (`storage.md` §5.1–§5.3, T-34) | done (#9) |
| 1.2 — catalog | `schema.hcl`, Atlas migrations per engine, `internal/catalog` on SQLite and MariaDB: the prologue, the lease, membership, `Admit`, `Reject`, `RegisterPack` and the reads ingest needs, barrier schedules on both engines (`catalog.md` §7.1–§7.4, `rollouts.md` §6.4) | done (#11) |
| 1.3 — ingest | `internal/bus` and `internal/ingest`: the disposition classifier, claim-check fetch, batching, masking with the bound configuration, the write seam, registration, per-message acks, the activation gate (`ingest.md` §6.1–§6.3, `architecture.md` §3.1) | in-progress |
| 1.4 — parking | parking, resolution, incarnations, termination orders and their re-check in `Admit`, `Reject` and `RegisterPack`, the parking transactions, `bus adopt-stream` and the termination commands (`parking.md`, `operations.md` §10.4) | pending |
| 1.5 — reconciler | full verification, quarantine and adoption under the compactor lease (`storage.md` §5.4) | pending |
| 1.6 — dev and gate | `tracekeep dev` and the role subcommands with their configuration (T-22), the simulator in the service module, the roadmap's phase 1 gate and checklist items 1–7 | pending |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 1.3 — ingest

Work on the branch `feat/ingest/core`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The service now requires the SDK at `producer/v0.1.0` for `busconfig`;
the simulator stays a module of its own until phase 1.6 (T-38).
Bus tests run against `internal/testbus`; tests that touch the catalog run on both engines through a harness the catalog exports, and `make test-catalog` runs ingest's too.
Time is injected: no test sleeps for the flush interval F, and every concurrent schedule is barrier-controlled,
its hook placed between the two side effects it splits.
Parking is phase 1.4: until then a deferral parking would move is a nak with delay,
and resolution reads `parked` and `termination_orders`, which hold nothing yet.
Phase 1 binds a processing configuration without a policy, so a batch is classified but not masked (`rollouts.md` §6.4).

| Step | Scope | State |
|---|---|---|
| 1 | the bus: the service requires the SDK; `tracekeep migrate`'s bus part ensures the traffic stream, the claim bucket with `AllowDirect` off (T-28) and the stream's `bus_streams` incarnation row; `internal/bus` opens the pull consumer `tk-ingest-<incarnation>` with §3.1's settings and checks `Created` and `LastSeq` against the row; the catalog's two-engine test harness exported | done (8f32c67) |
| 2 | the disposition classifier: a pure function from the facts gathered for one delivery to a disposition or the next step, a table-driven test with a case per §6.1 row; the raw reads (layout and length, `ts_utc_ns`, the version) and the resolution lookup come before framing | done (e4182cc) |
| 3 | admission: start recognition, inline or claim-check fetched and checked first, descriptor validation, `Admit` and `Reject`, age applied only after admission, beyond-retention terminations with their audit row | done (fd9d8ab) |
| 4 | batching and the flush: per (capture, hour), sorted, equal copies deduplicated, conflicting copies in conflict segments; classified with the bound configuration; written with `tracepack.NewWriter` to a working file, put, registered with delivery evidence, then each message acknowledged, `DoubleAck` for a claim-check one; in-progress acks while a batch is held | done (498bbfb) |
| 5 | the activation gate in the process: the configurations the binary executes activated, the membership registered and renewed before every fetch, `MaxWait` within the lease, every delivery checked against the lease and the generation, a record bound to a configuration the replica lacks deferred | done (b83af85) |
| 6 | the crash points of §6.3 as barrier tests on both engines: before the put, after the put before registration, after registration during the acks; redelivery byte-identical; a batch failing the `Writer` three times stays deferred with `write-failure` | pending |
