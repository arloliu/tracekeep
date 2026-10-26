# Phase 1 — ingest

Status: active (2026-10-27) — phase 1.8 in progress

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
| 1.3 — ingest | `internal/bus` and `internal/ingest`: the disposition classifier, claim-check fetch, batching, masking with the bound configuration, the write seam, registration, per-message acks, the activation gate (`ingest.md` §6.1–§6.3, `architecture.md` §3.1) | done (#13) |
| 1.4 — parking | parking, resolution, incarnations, termination orders and their re-check in `Admit`, `Reject` and `RegisterPack`, the parking transactions, `bus adopt-stream` and the termination commands (`parking.md`, `operations.md` §10.4) | done (#14) |
| 1.5 — reconciler | full verification, quarantine and adoption under the compactor lease (`storage.md` §5.4); a receipt's claim object key is the bucket-qualified `Tpk-Payload-Ref`, while the claim bucket is read by the bare message id | done (#16) |
| 1.6 — dev and roles | `tracekeep dev` and the role subcommands with their configuration (T-22), each processing configuration named for its classifier's implementation and version, the simulator in the service module, the gate's loopback capture | done (#17) |
| 1.7 — completeness and gate | catalog completeness (`catalog.md` §7.3) as a read, and the gate and checklist cases the phases before left open | done (#18) |
| 1.8 — metrics | every role's metrics of `operations.md` §10.4 with their endpoint; the gate names none, so they follow it | in-progress |

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
| 6 | the crash points of §6.3 as barrier tests on both engines: before the put, after the put before registration, after registration during the acks; redelivery byte-identical; a batch failing the `Writer` three times stays deferred with `write-failure` | done (6f0a154) |

## Phase 1.4 — parking

Work on the branch `feat/ingest/parking`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The service requires the SDK at `producer/v0.2.0`, whose `busconfig` defines the parking stream and the park prefix.
Every catalog step runs on both engines, every transaction starts with the capture-row helper (T-32),
and every concurrent schedule is barrier-controlled.
The reconciler's per-sequence `acked` check is phase 1.5;
the command-line wiring of the operator commands is phase 1.6, with the role subcommands.

A deferral is **indefinite**, and parked rather than nak'd, when it cannot clear by itself within the nak backoff:
a `rejected` capture's records, an expired record of a `rejected` or unknown capture (G5-251),
a batch at the `write-failure` limit, a record of an unknown capture whose catalog row is already `pending` (an earlier message was parked),
and a claim-check message of an unknown capture whose object is missing once its nak delay reached the last step.
Every other deferral stays a nak with delay, as in phase 1.3.
The claim-missing clause is the owner's decision T-40 (`ingest.md` §6.1),
since an object a producer is still putting may yet appear.

Two shapes the steps share:
**every acknowledgement of a resolved version goes through the coordinator's *acknowledge***
(`DoubleAck`, then the message's copy row marked `acked` when it has one), never the plain `Ack` of a batched message;
**the parked consumer is a second `Ingester`** over the parking stream, sharing the executor, the flusher and the catalog,
with its own consumer, batcher, nak delay, heartbeat and membership (named for the replica's, `<replica>/parked`),
renewed before each of its own fetches.

| Step | Scope | State |
|---|---|---|
| 1 | the termination re-check (a registration aborted by it naks its batch for reclassification, never a `write-failure` count): `Admit` and `Reject` take the delivery's identity (version key, incarnation, stream seq) and re-read its resolution and the capture's active orders under the row, returning `terminated` and writing nothing else; an order's application (the version inserted `terminated` under the order's action id with its copy row, the audit row and a `terminated` scope event, T-33) inline and as a transaction of its own; `RegisterPack`'s evidence carries every record's identity, its guard aborts and tombstones on a terminated or order-covered version, and it moves the batch's `parking`/`parked` versions to `staged` by compare-and-set; barrier tests of a termination committed before and after an admission and a registration (checklist 7) | done (d5db4fc) |
| 2 | the parking transactions: *begin* (the `pending` row when none, the state re-read, the horizon guard for an admitted capture, the `parked` and `parked_copies` rows insert-or-find), *advance*, a copy marked `acked`, a copy row inserted by the parked consumer, `parked → evicted-hour`; operator termination of parked versions and of a capture with its order; an order `spent` by `FirstSeq` or by retirement; `bus adopt-stream`'s catalog part (a new incarnation, copies `lost`, orders `spent`, refused while a lease is live); the reads `parked list` and `capture show` need | done (7631b7e) |
| 3 | the bus: the service on `producer/v0.2.0`; `tracekeep migrate`'s bus part ensures the parking stream with the traffic stream's replicas, refuses prefixes sharing a first token and overlapping subject filters, and records the parking incarnation; the parked consumer `tk-parked-<incarnation>`; `bus adopt-stream`'s bus part deletes every `tk-*` consumer of the stream and verifies none remains | done (28fc93a) |
| 4 | the parking coordinator: the version token and subject, the copy (body, `Tpk-*` headers and `Nats-Msg-Id` unchanged, `Nats-Expected-Stream` removed), the publish expecting last subject sequence 0, confirmation by a `PubAck` with `Duplicate` false or error 10071 with a strictly positive parsed sequence and nothing else, *advance* and *acknowledge*; the three-outcome test against `internal/testbus`, standalone and clustered, asserted on storage; confirmation failures converging (checklist 6) | done (d77a8b5) |
| 5 | disposition: indefinite deferrals park through the coordinator when `parking.enabled`; a resolved version is acknowledged (as already staged, or under the termination with its audit row and its copy row `acked`); `StepApplyOrder` executed | done (7516a95) |
| 6 | the parked consumer: its copy row inserted or found first, the same disposition with `parking.redelivery_delay`, never terminated for age or delivery count; a `parking` row advanced; staging through the flush with evidence; a horizon refusal moving the version to `evicted-hour`; every acknowledgement through the coordinator | done (0a64356) |
| 7 | the operator operations as functions the command line calls in phase 1.6: `parked list`, `parked terminate`, `capture terminate` (the cut-off from a fresh `Info` read before the transaction), `capture show`, `bus adopt-stream` | done (7e0fb81) |
| 8 | the gate's parking schedules: checklist items 1, 2, 4, 5 and the roadmap row's parking, termination and incarnation cases | done (19523df, 0b2637e, 5ee63ae) |

Left to later phases from the roadmap row's parking cases:
the reconciler's per-sequence `acked` marking, its `GetMsg` cases and the sole copy acknowledged at exactly `LastSeq` go to phase 1.5;
the parked consumer's wiring into the roles, `parking.enabled` toggled on a running service, and `rejected → open` with a parked copy
(phase 1 accepts one descriptor shape, so no fixed consumer exists to accept a rejected one) go to phase 1.6.
The owner's decisions on the points this phase raised are T-40:
an order never terminates a seq a registered pack holds (0fc0dcc), a restore changes `Created`,
and `parked replay` is withdrawn to the deferred proposal P1.

## Phase 1.5 — reconciler

Work on the branch `feat/compact/reconciler`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The reconciler lives in `internal/compact` and runs under the compactor lease;
its wiring into the `compact` role and the `object adopt` and `object list` commands are phase 1.6.
Every catalog step runs on both engines, every transaction starts with the capture-row helper (T-32),
and every concurrent schedule is barrier-controlled.
The claim-key note of the 1.5 row belongs to survivor deletion (`compactor.md` §8.4), which goes with retention to phase 2.

An orphan is a key under `staging/` or `archive/` that the catalog neither registers nor tombstones.
The specs give the reconciler no grace period, and adoption may win against `ingest`'s own registration of a healthy flush (`ingest.md` §6.3),
so a pass adopts a key only when the previous pass also found it unknown:
this plan's default, which the owner may change, and which costs one pass interval of delay.
`compact` does not import `ingest`; the reconciler derives a pack's ranges and stop from the records it reads.
Quarantined and unadmitted orphans are reported (logged, and returned by the pass), never deleted;
the specs keep no table for them, and the metrics come with every role's in phase 1.8.

| Step | Scope | State |
|---|---|---|
| 1 | the reconciler's registration guard: an adoption whose pack holds a seq with a `terminated` `parked` row, or whose capture has a termination order in any state, is lost to the termination and its key tombstoned (`storage.md` §5.4); checked before the unadmitted-capture guard; and a read of a key's standing: registered, tombstoned or unknown | done (5d93e3d) |
| 2 | copy-row reconciliation in the catalog: the `waiting` copy rows of resolved versions on an incarnation, and the reconciler's compare-and-set `waiting → acked` under the capture row, which re-reads that the version is resolved and the incarnation not retired (`parking.md` *resolution*) | done (b74e169, a2b7a03) |
| 3 | the orphan pass: unknown keys listed twice, tombstoned keys deleted again, `tracepack.Verify` in full with quarantine on any outcome but finalized-consistent or any writer defect, the exact seq set, endpoint times, stop and coverage read from the records, the key re-derived from the pack and compared, then `RegisterPack` adopted under the lease; a key beyond the horizon or lost to a termination deleted after the commit | done (a18ccf9) |
| 4 | the copy pass: per role, the current incarnation checked on a refreshed handle (`Created`, `LastSeq` not rewound) and `AllowDirect` off, then `GetMsg` on that handle for each waiting copy of a resolved version at or below `LastSeq`; only not-found marks the row `acked`, anything else advances nothing | done (a2b7a03) |
| 5 | the lease loop: the compactor lease taken and renewed, both passes run under it, the incarnations checked at every renewal; a lost lease or a changed incarnation stops the reconciler | done (7e9a695) |
| 6 | the gate's reconciliation schedules: checklist item 3, the roadmap row's reconciler cases (the sole parked copy acknowledged at exactly `LastSeq` and reconciled after a restart, a handle created while `AllowDirect` was on reading through the API, a lagging replica on the clustered testbus), and the orphan cases (a crash after the put adopted while `ingest`'s registration is in flight, a pending capture's orphan left unregistered, a terminated orphan tombstoned and deleted, a tombstoned key deleted again, a corrupt, writer-defective or misnamed object quarantined) | done (2fe7bf9) |

Left to later phases:
the `compact` role's wiring and the `object adopt` and `object list` commands go to phase 1.6, its metrics (`orphans_adopted_total`, `orphans_quarantined_total`) to phase 1.8;
a close's orphan, whose `seq_start` is the close sentinel 2^63−1, is reported and kept, and waits for the compactor that writes closes, phase 2.
The follower schedule reads through a follower but does not induce a replica's lag; the protection it relies on is that `AllowDirect` is refused and the handle refreshed.

## Phase 1.6 — dev and roles

Work on the branch `feat/cmd/roles`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Phase 1.7 closes phase 1: catalog completeness and the gate and checklist cases this phase leaves open,
listed by an audit of every gate case against the tests, with a schedule of the traffic and parked consumers both processing at once; this phase owns the gate's loopback capture only.
Of those, `rejected → open` with a parked copy is tested in 1.7 against a consumer whose accepted descriptors the test widens, standing in for a fixed release;
checklist item 7's close left `abandoned` goes to phase 2 with the closes, and the lagging replica stays the follower schedule of phase 1.5.

The configuration is one struct loaded with fuda v1.7.1 (T-22), its tunables and defaults in `docs/guides/configuration.md`, which `operations.md` points at.
Two defaults the specs do not fix are this plan's, for the owner to change:
the retention period, 30 days (`compactor.md` §8.4 sizes the claim bucket for "a month"),
and `ack_wait`, 7 minutes, the middle of what T-25 leaves between F plus an upload and Q minus F.
The built-in processing configuration is named `classify.Name`, the go-secs version the binary was built with;
a role refuses to start when the build reports no version (a workspace build), and only `dev` accepts it.
`internal/testbus` serves `dev` as well as the tests, so it gains a constructor that takes no `testing.TB`.
The `object list --unattributed` and `--survivors` commands report data that retention writes in phase 2, and `object adopt` has no defined semantics yet:
`object list` reports nothing until phase 2, and `object adopt` waits for its specification.
Two things wait for phase 2: admin through the process that owns a SQLite catalog, which is the `api` role's (`architecture.md` §3.1),
so with SQLite an admin command runs while the service is stopped;
and applying the catalog's migrations from the binary, which needs an applier that records the revision it reached.
A replica executes every unretired configuration of its own classifier, so a changed `ingest.max_frame_len` never locks a start out;
new captures keep binding the newest of them until an operator retires it (`rollouts.md` §6.4).

| Step | Scope | State |
|---|---|---|
| 1 | `internal/config`: the struct, defaults (T-12, T-25 and the two above), environment overrides, secrets by `ref`, validation; `docs/guides/configuration.md` | done (bc52061) |
| 2 | the built-in processing configuration named for its classifier, and the refusal of an unversioned build outside `dev` | done (bcb2d07) |
| 3 | the simulator back in the service module (T-38): its `go.mod` removed, its tests run with the service's | done (f7b67a5) |
| 4 | the roles: `migrate` (the bus's streams and bucket and both incarnations, the catalog's schema being applied by the Atlas CLI, T-12), `ingest` (the traffic and parked consumers sharing the executor, flusher and coordinator, `parking.enabled` read at start), `compact` (the reconciler under the lease, released on stop), `all`, and `admin` over `internal/admin`; each refuses to start against a stream that is not its incarnation or that allows direct gets | done (3375194) |
| 5 | `parking.enabled` turned off on a running service: waiting and evicted-hour copies stay listable and terminable (the gate's case the parking phase left here) | done (0a1758f) |
| 6 | `tracekeep dev`: an embedded bus, an `fs` store, SQLite, `ingest` and `compact`, and the loopback simulator, in one process | done (c3eb1c0, f2cb656) |
| 7 | the gate's loopback capture: `dev` run by a test, the simulator's capture registered and read back with the tracepack reader, and once by hand with the tracepack CLI | done (927c9fd) |

## Phase 1.7 — completeness and gate

Work on the branch `feat/catalog/completeness`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review, and closes phase 1's gate.
Completeness is a catalog read of one capture (`catalog.md` §7.3): its producer coverage, frontier and the classification of each seq up to it;
`ListPacks`, `ListCaptures` and the change cursor, which carry it to readers with no-data hours, unregistered intervals and overlaps, are phase 2's machine API.
Crash gaps come from closes, which phase 2 writes, so the read reports them once `capture_closes` holds them.
The remaining cases are those an audit of every gate case against the tests found missing or partial after phase 1.6;
every catalog schedule runs on both engines and every concurrent one is barrier-controlled.

| Step | Scope | State |
|---|---|---|
| 1 | `Completeness(capture)`: producer coverage from exact `pack_ranges` and `evicted_ranges`, the frontier, each seq classified once by retained > evicted > deferred > declared loss > gap up to it, gaps with their time intervals (an open-ended missing prefix from the capture's origin, an evicted neighbour's endpoint), the unknown tail, declared losses from `pack_losses` and terminated versions, and conflicts; the gate's two-pack counterexamples: two packs whose spans cover a seq neither holds, and two packs each filling the other's holes | done (4c5ba76) |
| 2 | ingest cases: a duplicate `start` after a crash before the put staged with its seq in the segment; an initial claim-check `start` at `max_payload` 4140 through ingest; a second claim digest at a staged seq deferred as a claim conflict, the object being named for the seq, while the first keeps its receipt; two inline versions of one seq parked inside one duplicate window; a later copy of a terminated version acknowledged with its own audit row; a terminated `pending` `start` followed by a different-version `start` that admits | done (7146720, 37d0d7c, d0a1569) |
| 3 | stream cases: a stale handler's late nak racing the acknowledgement of the same original; a stream deleted and recreated while a handler holds an old delivery, its late acknowledgement and nak removing nothing and a reused sequence inheriting nothing at its disposition; the parking stream's stale handler after a restore | done (b43cf0b) |
| 4 | the rest: a `start` with no descriptor header; a termination committed immediately after parking's step (1), held at a barrier; `rejected → open` with a parked copy, then staged, against a consumer whose accepted descriptors the test widens; the traffic and parked consumers processing at once | done (391910e) |

## Phase 1.8 — metrics

Work on the branch `feat/metrics/roles`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review, and ends phase 1.
`operations.md` §10.4's metrics are implemented where a phase 1 role already produces their data;
every other metric lands with the code that emits it (merge, close, deletion and retention in phase 2, the api's with the api, masking's in phase 3),
and `tracekeep status`, an admin command over the reads the machine API brings, is phase 2's.
`internal/metrics` owns the registry, the listener (`/metrics`, `/healthz`, `/readyz`) and one collector struct per role;
a component records through a pointer to its struct, nil recording nothing, so the tests and `dev` need no metrics;
`tool_id` is the only bounded label beside the reasons and outcomes, and `capture_id` is never one.
The service requires `github.com/prometheus/client_golang` v1.25.0; the SDK keeps its own `Metrics` interface and no Prometheus dependency.

| Step | Scope | State |
|---|---|---|
| 1 | the facility: the registry, the listener on `metrics.listen` (default `:9090`, empty for none), `/healthz`, and `/readyz` answering every readiness check a role registers; the configuration key in `docs/guides/configuration.md` | pending |
| 2 | catalog: both pools' usage, transaction retries by `busy` and `deadlock`, the embedded migrations' latest version; readiness by a ping | pending |
| 3 | ingest: `messages_total{disposition}`, `deferred_messages{reason}`, `deferred_oldest_age_seconds{reason}`, `parked_messages{reason}`, `segments_total{tool_id}`, `segment_bytes_total{tool_id}`, `put_seconds`, `registration_seconds`, `in_progress_acks_total`, `redelivered_total`, `conflicts_total{tool_id}`, `declared_losses_total{tool_id}`, `consumer_ack_pending`, `stream_backlog_messages`, `stream_backlog_bytes`; and on a periodic pass over open captures, `captures_open{tool_id}`, `open_gaps{tool_id}`, `captures_with_unknown_tail{tool_id}`, `gap_observations_total{tool_id}`; readiness once both consumers joined | pending |
| 4 | compact: `cycle_seconds{task=reconcile}`, `last_success_timestamp{task=reconcile}`, `lease_held`, `orphans_adopted_total`, `orphans_quarantined_total`, `tombstones_total`; readiness while the lease is held or a standby waits for it | pending |
| 5 | the roles wired: each serves the listener and registers its collectors and readiness; `dev` and `all` serve one listener for both roles | pending |
