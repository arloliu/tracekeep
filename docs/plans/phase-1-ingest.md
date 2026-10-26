# Phase 1 — ingest

Status: active (2026-10-26) — phase 1.1 in progress

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
| 1.1 — stores | `internal/store`: `ObjectStore`, `mem`, `fs`, `s3` with `Presigner` and ranged reads, the conformance suite against MinIO and Ceph RGW in CI (`storage.md` §5.1–§5.3, T-34) | in-progress |
| 1.2 — catalog | `schema.hcl`, Atlas migrations per engine, `internal/catalog` on SQLite and MariaDB: the prologue, the lease, membership, `Admit`, `Reject`, `RegisterPack` and the reads ingest needs, barrier schedules on both engines (`catalog.md` §7.1–§7.4, `rollouts.md` §6.4) | pending |
| 1.3 — ingest | `internal/bus` and `internal/ingest`: the disposition classifier, claim-check fetch, batching, masking with the bound configuration, the write seam, registration, per-message acks, the activation gate (`ingest.md` §6.1–§6.3, `architecture.md` §3.1) | pending |
| 1.4 — parking | parking, resolution, incarnations, termination orders, the parking tables, `bus adopt-stream` and the termination commands (`parking.md`, `operations.md` §10.4) | pending |
| 1.5 — reconciler | full verification, quarantine and adoption under the compactor lease (`storage.md` §5.4) | pending |
| 1.6 — dev and gate | `tracekeep dev` and the role subcommands with their configuration (T-22), the simulator in the service module, the roadmap's phase 1 gate and checklist items 1–7 | pending |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 1.1 — stores

Work on the branch `feat/store/backends`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The conformance suite is one exported function every backend's test calls, so `mem`, `fs` and `s3` are held to the same postconditions:
a `Put` visible only when complete, an error that leaves nothing visible (a body that fails mid-stream), `Open`'s `ReadAt` at every offset and in parallel, `Stat`, `List` by prefix, `Delete`, and not-found from each.
The `s3` runs need a store, so they run when an endpoint is configured and skip otherwise;
`make test` stays offline, and a separate target and CI job start the containers.

| Step | Scope | State |
|---|---|---|
| 1 | the interface, `ObjectInfo`, not-found, the conformance suite and `mem` | done (1c3d840) |
| 2 | `fs`: `.partial/` write, fsync, rename, directory fsync; a failed put leaves nothing under the key | done (2f4dc21) |
| 3 | `s3` on aws-sdk-go-v2: path-style with a custom endpoint, both checksum options `WhenRequired`, a seekable body, ranged reads with a small read-ahead, `Presigner` | done (779c77c) |
| 4 | the suite against MinIO and Ceph RGW: pinned container images, a make target that runs them locally, and a CI job | done (b6fab06) |
