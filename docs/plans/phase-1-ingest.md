# Phase 1 — ingest

Status: active (2026-10-26) — phase 1.2 in progress

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
| 1.2 — catalog | `schema.hcl`, Atlas migrations per engine, `internal/catalog` on SQLite and MariaDB: the prologue, the lease, membership, `Admit`, `Reject`, `RegisterPack` and the reads ingest needs, barrier schedules on both engines (`catalog.md` §7.1–§7.4, `rollouts.md` §6.4) | in-progress |
| 1.3 — ingest | `internal/bus` and `internal/ingest`: the disposition classifier, claim-check fetch, batching, masking with the bound configuration, the write seam, registration, per-message acks, the activation gate (`ingest.md` §6.1–§6.3, `architecture.md` §3.1) | pending |
| 1.4 — parking | parking, resolution, incarnations, termination orders and their re-check in `Admit`, `Reject` and `RegisterPack`, the parking transactions, `bus adopt-stream` and the termination commands (`parking.md`, `operations.md` §10.4) | pending |
| 1.5 — reconciler | full verification, quarantine and adoption under the compactor lease (`storage.md` §5.4) | pending |
| 1.6 — dev and gate | `tracekeep dev` and the role subcommands with their configuration (T-22), the simulator in the service module, the roadmap's phase 1 gate and checklist items 1–7 | pending |

Only the phase under way is detailed; the others are detailed when they start.

## Phase 1.2 — catalog

Work on the branch `feat/catalog/core`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Every catalog test runs on SQLite and on MariaDB (`300-testing.md`):
MariaDB runs when a server is configured and is skipped otherwise, so `make test` stays offline,
and a make target and a CI job start a MariaDB container for it, as `make test-s3` does for the stores.
The schema holds every table of §7.1, the parking rows included, so later phases add transactions, not tables.
The termination re-checks of `Admit`, `Reject` and `RegisterPack`, and the parked versions a registration stages, land with phase 1.4, which writes the rows they read;
nothing before it can create a termination order or a parked version.
`atlas migrate lint` is available only to Atlas Pro users since Atlas v0.38,
so CI runs `atlas migrate validate` and checks that `atlas migrate diff` finds nothing to add, on both engines (`catalog.md` §7.4 amended).

| Step | Scope | State |
|---|---|---|
| 1 | `schema.hcl` with one variable per engine-specific type, `atlas.hcl`, `migrations/sqlite` and `migrations/mariadb` with their `atlas.sum`, embedded for tests; the MariaDB container target and CI job, with validate and the drift check | done (4dce2d8) |
| 2 | `internal/catalog`: opening each engine (SQLite's one-connection write pool, `BEGIN IMMEDIATE`, `busy_timeout`; MariaDB at `READ COMMITTED`), whole-transaction retries, the dialect value, the capture-row prologue, the event-seq allocation, the two-engine test harness | pending |
| 3 | leases, membership and processing configurations: take and renew a lease with its fence token; register, renew and leave a membership, refused unless it executes every unretired configuration; activate and retire a configuration; each on the `membership` sentinel row | pending |
| 4 | `Admit` and `Reject`, with the binding of the newest configuration every leased member supports, and the capture lookup | pending |
| 5 | `RegisterPack`: idempotent on `pack_id`, `adopted` for a caller with evidence, the tombstone and horizon guards, ranges, losses, suspects with the touched scopes' revisions and events, receipts, the capture's highest seq and stop, the scope upsert and its event | pending |
| 6 | barrier schedules on both engines: two first sightings of one capture, two overlapping registrations of one capture (the suspect is never missed), event allocation with an uncommitted earlier transaction, a membership renewal racing an admission | pending |
