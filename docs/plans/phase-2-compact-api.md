# Phase 2 — compact and machine API

Status: active (2026-10-27) — phase 2.2 in progress

Milestone 2 of the [roadmap](../ROADMAP.md):
the compactor's state machines, the catalog reads they make answerable, and the machine API the VE service reads them through.
The owner's answers to the points it raised before starting are T-41.
Its scope and gate are the roadmap's phase 2 row and the implementation checklist under it;
the design is `docs/specs/tracekeep/compactor.md`, `catalog.md` §7.2–§7.3, `api.md` §10.1–§10.2, `operations.md` §10.4 and `storage.md` §5.2 *Downloads*.
This plan only orders the work.

## Before it starts

- T-7's gate: connect-go's overhead against plain gRPC, measured for `ListPacks` and a streamed `QueryRecords` shape before phase 2.6 starts,
  the result recorded in the decision log.
  It runs beside phases 2.1–2.5, which serve no API.
- tracepack v0.9.0 is already the service's (T-36), so every close is written uncompressed (G5-272):
  phase 2.2 drops `capture_closes.codec` and the codec from `compactor.md` §8.2 and `catalog.md` §7.1,
  which closes the roadmap's open item on it.
- Not in this phase:
  `QueryRecords`, `FindTransaction`, `Export` and an export bundle's retention, people's OIDC, the frontend and holds are phase 4's;
  `Restore`, `Digest` and the `keys` module's other purposes are phase 3's;
  configuration retirement waits for the first release that needs it.

## Phases

| Phase | Scope | State |
|---|---|---|
| 2.1 — merge and conflicts | the merge of §8.1 with `CommitMerge`, `RecordConflict` confirming or clearing suspects, `scope merge`, `conflict acknowledge`, `scope exclude-input`, and their metrics | done (#22) |
| 2.2 — close | `CloseIntent` and `PublishClose` of §8.2 with both triggers, `abandoned` intents (checklist 7's close), `capture close`, crash gaps in completeness, `closes_total{trigger}`; the reconciler adopting a close's orphan, which phase 1 reports and keeps (`internal/compact/orphans.go`), as a pack holding no producer range that moves neither coverage nor `highest_seq`, racing the publication in both orders | in-progress |
| 2.3 — deletion and retention | each object's deletion deadline as one function (hour end + retention, `merged_at` + D for a merged input), D above the download URL's lifetime, and a shortened retention kept from evicting an hour before an issued URL's promise across restarts and processes (specified in `compactor.md` §8.3 first, then built); `DeletePack` after D; `EvictHour` with `evicted_ranges` and the `parked → evicted-hour` move; the claim bucket listed and its receipt-less objects attributed by the SDK's metadata (hour, capture, seq, the bare object name against the bucket-qualified `Tpk-Payload-Ref`), with their `claim_objects` rows, and hours that hold claim objects but no scope, before eviction reads them; §8.4's predicate as one function; survivors deleted by the reconciler; `object list --survivors` and `--unattributed`; and their metrics | pending |
| 2.4 — reads and cursor | `ListDevices`, `ListCaptures`, `ListPacks` and `ListScopeChanges` as catalog reads (§7.3): `no-data` hours, unregistered intervals, overlap, the watermark read first, `low_water` and `epoch`, pruning by seq prefix | pending |
| 2.5 — service auth | client credentials against the IdP's JWKS with lazy discovery and one refresh on an unknown `kid`, mTLS, the static-token flag never enabled by a failure, the role each operation checks, `auth_failures_total`; and the `download` key purpose of `masking-keys.md` §9 brought forward whole (T-41): the binding's insert-or-compare, the `file` and `env` providers, issuing and accepted key ids, purpose separation | pending |
| 2.6 — api | `proto/`, connect-go behind 2.5's checks from its first served request, the `api` role in `all` and `dev` with its readiness and metrics, the four reads, `PackURL` with 2.3's deadline, a 15-minute lifetime, its margin and floor and `pack-superseded`, presigned where the store presigns and `storage.presign` is on, otherwise served by the API with a download token (`HEAD` and ranged `GET`), presigning checked at start, `audit_log` for API actions | pending |
| 2.7 — admin and status | `tracekeep status`, `deferred list`, admin through the `api` process on SQLite, `tracekeep migrate` applying the catalog's migrations through Atlas's Go library with its own revision table (T-41), tested on a fresh catalog, an upgrade and a failed application, `gap_observations_total` and `captures_with_unknown_tail`, the cost of ingest's metrics read measured at fleet size | pending |
| 2.8 — gate | the roadmap row's cases and the checklist the phases before left open, audited against the tests as 1.7 did | pending |

Only the phase under way is detailed; the others are detailed when they start.
Every compactor transaction starts with the capture-row helper and checks the lease after it (T-32, `.agents/rules/600-invariants.md`);
every catalog step runs on both engines, and every concurrent schedule is barrier-controlled.
Each phase lands the metrics of `operations.md` §10.4 for the code it adds.
Each phase's detail names the gate and checklist cases it owns:
the survivor, shared-object and unknown-capture races in 2.3, the cursor's commit order and pruning in 2.4, a promised download's lifetime in 2.6;
2.8 audits them all, the membership and old-version cases phase 1 built included.
The roadmap's gate case "a parked message replays in order after its blocker clears" is the parked consumer's own retry after `parking.redelivery_delay`,
staged through a flush sorted by seq (T-40 withdrew an early replay to proposal P1).

## Phase 2.2 — close

Work on the branch `feat/compact/close`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The close runs in the compactor's cycle under the one lease, after the merge pass, so a close's segment is merged with its hour's like any segment.
The inactivity period I (default 2 hours, T-12) becomes `compact.inactivity` in the configuration and its guide.
A changed check constraint is renamed with its change, since Atlas v1.3.0 writes MySQL's `DROP CHECK` for a modified check, which MariaDB refuses,
and `DROP CONSTRAINT` for a dropped one.

| Step | Scope | State |
|---|---|---|
| 1 | schema: `capture_closes.codec` dropped with the codec from `compactor.md` §8.2 and `catalog.md` §7.1 (T-36, closing the roadmap's open item), and a third trigger kind, `operator`, for `capture close` | done (e16ffd9) |
| 2 | catalog: the candidate scan, captures `open` without `stop_seen`, by inactivity (no registration for I, the capture's first sighting standing in for a capture never registered) or by successor (a later capture of its `tool_id` and `recorder_instance_id`, by origin, with a registration at least Q old, and none of its own for Q); `CloseIntent` under the capture row and the lease, eligibility re-read for its trigger inside, the intent row with its fixed `pack_id`, `writer_time`, serialized `PackMeta`, highest seq and its time and the gap bounds, the capture `closed`; an operator's request ended in the same commit; the pending intents read | done (5b2fc80) |
| 3 | catalog: `PublishClose` under the capture row and the lease, on the intent's `pack_id`: registered as a pack holding no range, or found registered (adopted first), and the intent `published`, the scope's event last; the horizon guard refusing it tombstones the key and abandons the intent, and a key already tombstoned abandons it too; neither coverage nor `highest_seq` moves | done (27eff21) |
| 4 | the close pass: candidates to intents, then every pending intent written by `tracepack.WriteClose` from exactly its stored options, put under its `staging/` key (a put whose result was lost checked by `Stat` against the written size), and published; `capture close` as a request decided under the capture row (not open or `stop_seen`: failed), its intent ending it | done (8a7cd29) |
| 5 | the reconciler adopting a close's orphan as a pack holding no range, before and after the publication | done (b953cf3) |
| 6 | metrics: `closes_total{trigger}`, `cycle_seconds{task=close}`, `last_success_timestamp{task=close}`; `compact.inactivity` | done (32942a7) |
| 7 | schedules, barrier-controlled on both engines, asserting durable state: a registration between the candidate scan and the intent (no close), and one after it (staged, the capture stays `closed`, its close's bytes unchanged); a late `stop` before the intent (no close) and after it (staged; `stop_seen` set, the capture stays `closed`); the successor trigger refusing to close an overlapping capture still registering, and closing it once silent for Q; a crash after the intent, after the put and after the publication, each retried to the same bytes; the close's object adopted before its publication; a horizon refusal leaving the intent `abandoned` and never retried (checklist 7); a capture closed after seq 100 keeping `highest_seq` 100, its crash gap read by completeness; a lease lost before each commit | done (10f7d1c) |

## Phase 2.1 — merge and conflicts

Work on the branch `feat/compact/merge`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The merge runs in the compactor's cycle beside the reconciler, under the one lease;
merged inputs stay `merged` until phase 2.3 deletes them.
The quiet period Q (default 15 minutes, T-12) becomes `compact.quiet_period` in the configuration and its guide.

| Step | Scope | State |
|---|---|---|
| 1 | catalog: the candidate scan (`open` or `reopened`, hour end + Q passed, `staged` packs, no `conflicts` row in any state), the snapshot (revision r and the `staged` inputs, segments and archives alike, read together with the eligibility), `CommitMerge` re-reading under the capture row that the scope has no `conflicts` row and every input is still `staged`, then the revision and lease compare-and-sets, zero affected rows or a failed re-read rolling back the whole transaction, the `settled` event last | done (258e353) |
| 2 | the merge pass: `tracepack.Merge` over `ReaderAt`s of the inputs, the archive written to the working directory and put under its `archive/` key, then `CommitMerge`; a rolled-back merge or a crash after the put leaves the archive to the reconciler, which adopts it as a `staged` input of the next merge ([FMT I-12]); a merge failure leaves the scope as it was and is counted | done (706a9e5, af8af1f) |
| 3 | `RecordConflict`: a suspect's overlapping records compared by bytes and redaction entries, cleared for equal copies and confirmed for differing ones only while the row names exactly the compared packs (otherwise `changed`); a merge failing on a conflict writes the row; every touched scope's revision advanced and a `conflict` event each | done (f895ce1) |
| 4 | admin: `conflict acknowledge` (a state `acknowledged` the `conflicts` check and `catalog.md` §7.1 gain; the scope stays unmerged), `scope exclude-input` (the pack `excluded` under the capture row, the revision advanced, an `excluded` event emitted, the schema's event check widened, T-41), each audited; `scope merge` as a `compact_requests` row the compactor holding the lease executes at its next cycle (the table added to the schema, T-41), tested with no compactor running, a lease lapsing or taken over while the request executes, and a crash during it, the request's acceptance told from its execution and a retry after a lost response executing it once | done (4a543a8) |
| 5 | metrics: `cycle_seconds{task=merge}`, `last_success_timestamp{task=merge}`, `merges_total{outcome}`, `scopes{state}`, `conflicts_open`, `oldest_unmerged_scope_age_seconds`; `compact.quiet_period` validated strictly above `flush_interval` + `ack_wait` + `compact.redelivery_slack` (default 2 minutes, `catalog.md` §7.3, T-25, T-41), tested at, below and above that bound with the defaults and `dev`'s | done (1d324db) |
| 6 | the merge's schedules, barrier-controlled on both engines, each asserting the durable rows and objects, not only the result: a registration between snapshot and commit, in `open` and in `reopened`, failing the stale commit and merged by the next cycle; a lease takeover before the lease row's lock and one blocked behind it, and a lease lapsed without takeover, the stale worker writing nothing and never deleting its archive; a losing commit leaving no write of its own (archive row, range, loss, merge relation, input change or event); a commit durable whose result was lost, then no second merge relation or `settled` event; the archive adopted by the reconciler before `CommitMerge` (the adopted rows stay, the stale commit adds nothing) and after it (the reconciler finds it registered and changes nothing); a crash after the put, a new lease holder adopting the archive two passes later, its suspects cleared and a fresh merge settling each record once, and with a termination order on the capture the archive tombstoned instead and the inputs intact; a staged archive merged again with a late segment, ranges, endpoint times and losses kept; a merge that writes no receipt, no suspect against its own inputs and no `last_registered_at`; a damaged input, a failed read, a failed put and a put visible despite its error, the scope and inputs unchanged and no conflict fabricated | done (f7427ce) |
| 7 | the conflict and exclusion schedules, likewise: a conflict raised, confirmed or cleared on the scope, from another hour included, between the snapshot and the commit, failing the stale commit; one cleared before the snapshot, after which the merge proceeds; one raised before the snapshot, or acknowledged, keeping the scope unmerged; a suspect acknowledged while its comparison runs, which then neither clears it nor makes the scope mergeable; the compared pair's packs changed before `RecordConflict` commits, returning `changed` with no state, revision or event written; equal records with equal and with differing redaction entries; a third pack registered while a comparison runs, its pair rows kept; a comparison that fails to read, or whose lease is lost before its transaction, clearing and confirming nothing, and a later comparison converging; a pack excluded before the snapshot, never an input, and after it, failing the stale commit; an excluded pack still readable, its exclusion's pack state, revision, audit row and event committed together or not at all | done (29ce22e) |
