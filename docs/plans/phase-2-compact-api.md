# Phase 2 — compact and machine API

Status: active (2026-10-27) — phase 2.4 in progress

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
| 2.2 — close | `CloseIntent` and `PublishClose` of §8.2 with both triggers, `abandoned` intents (checklist 7's close), `capture close`, crash gaps in completeness, `closes_total{trigger}`; the reconciler adopting a close's orphan, which phase 1 reports and keeps (`internal/compact/orphans.go`), as a pack holding no producer range that moves neither coverage nor `highest_seq`, racing the publication in both orders | done (#23) |
| 2.3 — deletion and retention | each object's deletion deadline as one function (hour end + retention, `merged_at` + D for a merged input), D above the download URL's lifetime, and a shortened retention kept from evicting an hour before an issued URL's promise across restarts and processes (specified in `compactor.md` §8.3 first, then built); `DeletePack` after D; `EvictHour` with `evicted_ranges` and the `parked → evicted-hour` move; the claim bucket listed and its receipt-less objects attributed by the SDK's metadata (hour, capture, seq, the bare object name against the bucket-qualified `Tpk-Payload-Ref`), with their `claim_objects` rows, and hours that hold claim objects but no scope, before eviction reads them; §8.4's predicate as one function; survivors deleted by the reconciler; `object list --survivors` and `--unattributed`; and their metrics | done (#24) |
| 2.4 — reads and cursor | `ListDevices`, `ListCaptures`, `ListPacks` and `ListScopeChanges` as catalog reads (§7.3): `no-data` hours, unregistered intervals, overlap, the watermark read first, `low_water` and `epoch`, pruning by seq prefix | in-progress |
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

## Phase 2.4 — reads and cursor

Work on the branch `feat/catalog/reads`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The reads are the catalog's domain results the API of phase 2.6 serves unchanged:
they take no capture row and no lease, and each reads one snapshot, as `Completeness` does.
Pruning is the one write: it runs in the retention pass under the lease, with the configured retention period, and allocates no seq.
No proto, no transport and no operator truncation of the cursor come with it; a test sets `counters.epoch` by SQL.
The gate and checklist cases this phase owns are the cursor's commit order and pruning:
the VE service reads a settled hour and learns of a reopen, and an event cursor with an uncommitted earlier transaction and with pruning.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `catalog.md` §7.3 made exact where it is silent: the captures a window selects (admitted, of the tool and devices or the one capture, origin before the window's end and span reaching its start, an `open` capture's span unbounded), a capture's span (origin to `highest_seq_ts`, origin alone before any record) for overlap and as a predecessor, the first capture of a recorder (no interval unless `previous_capture_id` names no admitted capture), a `no-data` hour's flag from the spans, an hour without a scope whose ranges were evicted reported evicted, a scope-less hour's parked seqs read from the capture's completeness, `ListDevices`' fields (the latest capture's endpoint and device id, open captures, overlap, last record time, a gap whose interval meets the window, an unknown bound counting as meeting it), the cursor as (epoch, seq), stale when the epoch differs or a pruned seq lies above it (`after < low_water − 1`), a page's next cursor past the events it scanned and at the snapshot's watermark when the page is short, the tool and device filter, pruning with the configured retention period in the retention pass under the lease, `low_water` never lowered; §7.2's pruning, which checks the lease | done (a210c4a, 0de0d2d, 1cc6af5) |
| 2 | catalog: the window's captures and `ListCaptures`: state, device, recorder, predecessor, the descriptor's thinning entries, processing configuration, overlap from the spans of one (`tool_id`, `recorder_instance_id`), unregistered intervals against Q, tested at Q and past it, with overlapping predecessors, a missing predecessor and a capture with no record | done (576333b) |
| 3 | catalog: `ListDevices`: per device its name, the latest capture's `equipment_endpoint` and `device_id`, open captures, overlap, last record time and whether the window holds a gap | done (a3bb346) |
| 4 | catalog: `ListPacks`: one read snapshot whose first statement reads the watermark and epoch, then per requested hour the scopes of the window's captures with state, revision and packs with their ranges, a scope-less hour `no-data` with its flag, evicted, or holding deferred seqs and declared losses, and each capture's completeness and unregistered intervals | done (6550341, 2a8dbd2) |
| 5 | catalog: `ListScopeChanges`: events after a cursor for the tool and devices, paginated, the next cursor, stale by epoch and by `low_water`, tested at `after = low_water − 1` and `− 2` | done (4ee868a) |
| 6 | catalog and the retention pass: `PruneEvents` under the lease, `low = MIN(seq)` over events younger than the period, nothing written without one, the prefix deleted and `low_water` raised monotonically in one transaction | done (ed0788c) |
| 7 | schedules, barrier-controlled on both engines: an event allocated and uncommitted while a listing reads its watermark, then read after it; an event committed between the watermark and the listing's rows, replayed above the watermark; a settled hour listed, reopened by a registration, the `reopened` event read with its revision; pruning racing a reader; a stale cursor re-listing and continuing from the watermark; a lease lost before the prune commits | done (a4c04cd) |

## Phase 2.3 — deletion and retention

Work on the branch `feat/compact/retention`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Deletion, the claim bucket's listing, retention and survivor cleanup run in the compactor's cycle under the one lease, after the close pass, in that order,
so the listing's rows exist before eviction reads them.
D becomes `compact.delete_delay` (default 1 hour) and the download URL's lifetime `store.download_lifetime` (default 15 minutes, T-41),
D validated above it by more than twice two minutes of clock skew;
the margin and floor are phase 2.6's.
A changed check constraint is renamed with its change (phase 2.2).
The gate and checklist cases this phase owns are the survivor race, the shared-object race and the unknown-capture race.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `compactor.md` §8.3 gains the deadline as one function and the retention record: the compactor records its configured period in an append-only catalog row under the lease before any eviction, a shortened period keeping the longest period in force within the last D (D above every URL's lifetime) for D, so the api and compact roles compute one deadline from the catalog alone, across restarts and processes, and before the first record no deadline exists; a merged input's deadline from its own merge's `merges.delete_at`, `merges.at` + D fixed at the commit (so a changed D applies to later merges), not the scope's `merged_at`; the registration's horizon guard on the configured period at once (refusing early is safe; only eviction waits); eviction's rows spelled out (an evicted pack's `pack_ranges`, `pack_losses`, `merge_inputs`, `merges` and the `conflicts` rows naming it go with it; `evicted_ranges` coalesced within its hour, keyed by capture, hour and first seq); `unattributed_objects` for the bucket's objects without the SDK's metadata; the listing's transaction taking no capture row; the clock assumption the horizon guard and eviction share; a refused eviction or deletion counted every cycle; `catalog.md` §7.1–§7.2 and the configuration guide to match | done (13198b8, ddda182) |
| 2 | schema: `retention_periods` (period, the period held and until when, one row per change), `unattributed_objects` (key, size, first seen), `merges.delete_at`, `evicted_ranges` keyed by capture, hour and first seq | done (1a40f89) |
| 3 | catalog: the retention record under the lease, and the deadline and eviction horizon as one function of its row, tested at, inside and past a shortening's window, a lengthening, two shortenings inside one window and a restart reading the row | done (88645c5) |
| 4 | catalog: the deletion candidates (a `merged` input of a `committed` merge whose output is `staged`, `merged` or, merged in turn, `deleted`, its merge's `delete_at` passed, so a chain of merges deletes in any order) and `DeletePack` under the capture row and the lease, no other `packs` row naming the key, the pack `deleted` and its tombstone, the object removed after the commit and, after a crash, by the orphan pass that deletes tombstoned keys; a pack's deadline for phase 2.6 | done (8002f60) |
| 5 | catalog: §8.4's predicate as one function of a (capture, seq)'s `parked` versions, their copies and each role's current incarnation; the eviction candidates (hours of admitted captures past the eviction horizon holding a scope, a pack, a `live` claim object or a `parked` version) and `EvictHour` under the capture row and the lease: the hour's ranges into `evicted_ranges`, its rows deleted, its `claim_objects` rows deleted except survivors, marked with the hour, its `parked` rows moved `parked → evicted-hour`, the tombstones, the objects removed after the commit; a conflict's other scope advanced with a `conflict` event | done (3535afb) |
| 6 | catalog and the claim pass: the bucket listed, an object without a `claim_objects` row attributed by its name (`<capture>.<seq>`, against the bucket-qualified key) and the SDK's metadata (the 44-byte header, its seq and time matching, the hour from the time) as a `live` row whatever its capture's state, and one without that metadata recorded in `unattributed_objects`, removed once gone; survivor cleanup: `MarkSurvivorDeletable` under the capture row and the lease on the predicate, the object deleted (an absent one counts), the row removed under the lease, a `deletable` row resumed | done (995fc03, f0a7bed) |
| 7 | the compactor's passes in the cycle: deletion, the listing, retention (the record first), survivors; the claim objects of an evicted hour deleted from the bucket after its commit, a crash between converging through the next listing; `compact.delete_delay` and `store.download_lifetime`, D validated above the lifetime by more than twice two minutes of clock skew | done (e1e91a4) |
| 8 | admin: `object list --survivors` (key, capture, seq, evicted hour, size) and `--unattributed` | done (520e884) |
| 9 | metrics: `cycle_seconds{task=delete,retention}`, `last_success_timestamp{task}`, `deletions_total`, `tombstones_total` for deletion and retention, `claim_objects`, `claim_object_bytes`, `claim_objects_survivors` | done (10eabdd) |
| 10 | schedules, barrier-controlled on both engines, asserting durable state: a registration and a deletion of one key in both orders; an unresolved `parked` row committed before the eviction (a survivor) and after it (the horizon guard terminates or moves it to `evicted-hour`); two versions of one (capture, seq), the object kept until the last satisfies the predicate (staged needs no acknowledgement, terminated needs every current-incarnation copy `acked`, a retired incarnation's `lost` copy not blocking); a claim object of a `pending`, `rejected` or unknown capture kept, then evicted once admitted; a survivor released after its version resolves, and an interrupted release resumed; a crash after each commit; a retention shortened with a restart inside its window evicting nothing early; a lease lost before each commit; a refused eviction counted | done (ddda182) |

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
| 4 | the close pass: candidates to intents, then every pending intent written by `tracepack.WriteClose` from exactly its stored options, put under its `staging/` key (a put whose result was lost checked by its size and its bytes' SHA-256 against the written close), and published; `capture close` as a request decided under the capture row (not open or `stop_seen`: failed), its intent ending it | done (8a7cd29) |
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
