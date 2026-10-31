# tracekeep — catalog

Status: draft (2026-10-25) — split from seed v21

Covers the catalog's tables (parking tables in `parking.md`), its operations, completeness and the change cursor, engines, transactions and migrations.
From seed v21 §7, except the parking rows of §7.1;
section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/catalog`, the schema or migrations, or anything that reads completeness or the cursor.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Role**: the catalog is the service's database; tracepack defines no view of it (`positioning.md` §2).
- **Operations**: domain transactions, never statements:
  admit or reject a capture, register a pack, commit a merge, record a conflict, insert and publish a close, delete a pack, evict an hour, release a survivor, begin parking and its later steps, terminate;
  besides them, take or renew the compactor lease, register, renew or leave a membership, activate or retire a configuration, prune events, append the audit log, and the reads (§7.2, §7.3).
- **Contracts**: each transaction states its preconditions, guard reads, affected-row checks, durable outcome and `counters` use (§7.2 table);
  about forty statements, the multi-statement transactions guarded by compare-and-swap predicates with affected-row checks and unique constraints;
  no `SELECT … FOR UPDATE` (§7.2).
- **Lock order**: capture → `leases` → `counters('membership')` → `counters('scope_events')`, each step optional (T-32):
  one unexported helper runs the prologue, so the capture row is first by construction;
  a transaction committed under the compactor lease updates the `leases` row next;
  admission and every membership change update the `membership` sentinel row;
  the `scope_events` row is last, only in a transaction that allocates an event seq, and parking touches no counter except when it terminates (T-33);
  nothing else is locked (§7.2; `ingest.md` §6.3; `compactor.md` §8.3).
- **Aborts**: a failed scope-revision or lease check rolls back the whole transaction, which returns the outcome unretried;
  a tombstone commits before its object is deleted (§7.2; `compactor.md` §8.1, §8.3).
- **Reads**: domain results;
  `ListPacks` returns completeness and the watermark read before the listing, and the module owns cursor recovery (§7.3).
- **Read set**: a human read reads an hour's `staged` and `excluded` packs from one snapshot, never a `merged` input, and a capture whose configuration has no policy is withheld outside `tracekeep dev` (§7.3).
- **Completeness**: producer coverage is the union of `pack_ranges` and `evicted_ranges`;
  a seq of an unresolved parked version is deferred, one of a terminated version a declared loss;
  a hole is a gap, evicted, or the unknown tail;
  an hour with no scope and no parked seq is `no-data`;
  unregistered intervals are reported against every earlier capture of the device,
  and overlapping captures of one device are flagged, never merged into one series (§7.3).
- **Device**: `captures.device` is the descriptor's `recorder_instance_name`, indexed with `tool_id` for selection (§7.1).
- **Cursor**: an event's seq is allocated from the `counters` row as the last statement, so allocation order is commit order;
  the watermark is read before the listing;
  pruning removes a seq prefix only;
  a stale cursor re-lists from `ListPacks` (§7.3).
- **Engines**: MariaDB at `READ COMMITTED` with whole-transaction retries;
  SQLite with one writer, `BEGIN IMMEDIATE`, and only the `all` process writing;
  all of it inside the module, never a caller's (§7.2, §7.4).
- **Tests**: the barrier schedules run against the transactions on both engines (§7.4).
- **Migrations**: Atlas, versioned per engine, expand-then-contract across two releases (§7.4).
- **Open**: signatures are a sketch.

## 7. Catalog (requirements 3 and 4)

### 7.1 Tables

| Table | Holds | Key facts |
|---|---|---|
| `captures` | descriptor bytes and decoded fields; the processing configuration ids; `state` `pending`/`open`/`stopped`/`closed`/`rejected` (`pending`: a placeholder without descriptor, inserted by parking before any `start`, §6.1) (a rejected `start`'s descriptor bytes kept for the operator); `data_revision`; `highest_seq`, `highest_seq_ts` (producer seqs only); `stop_seen`; `last_registered_at`; thinning parameters; `device`, decoded from the descriptor's `recorder_instance_name` at admission (none on a `pending` row) | one row per capture; indexed by (`tool_id`, `recorder_instance_id`, start time) for successor lookup and by (`tool_id`, `device`, start time) for device selection |
| `capture_closes` | the close intent: trigger time, `highest_seq`, `highest_seq_ts`, bounds, fixed `pack_id`, `writer_time`, the serialized `PackMeta`, the trigger (`inactivity`, `successor`, or `operator` for `capture close`), `state` `pending`/`published`/`abandoned` | one row per capture; inserted before the close is written (§8.2); `abandoned` when the horizon guard refuses its publication (T-32) |
| `packs` | `pack_id`, capture, `hour`, role, object key, size, `seq_start`, `format_minor`, `state` `staged`/`merged`/`excluded`/`deleted`, `adopted` (registered by the reconciler, without delivery evidence) | index `(tool_id, hour)`, `(capture_id, hour, state)`; `excluded` packs are read, never merged, never deleted by the merge relation |
| `pack_ranges` | `(pack_id, seq_from, seq_to, ts_from, ts_to)`: the exact seq runs a pack holds with their endpoint times | the completeness evidence of §7.3; the close sentinel is never a range |
| `evicted_ranges` | `(capture_id, hour, seq_from, seq_to, ts_from, ts_to)`, keyed by capture, hour and `seq_from` and coalesced within the hour: seqs whose packs retention deleted, with their hour and endpoint times | §7.3 reports them as evicted, never as gaps, keeps the times a neighbouring gap needs, and tells an evicted hour from a `no-data` hour; a prefix scalar cannot express an hour (§8.3) |
| `claim_records` | `(capture_id, seq, header44, payload_sha256)` → first `pack_id`: the **receipts** of records that travelled by claim-check, one row per version | version-sensitive staged proof for §6.1 and §8.4; outlives merges and deletions; removed only by retention |
| `pack_losses` | a pack's `coverage` entries: declared losses, each bound as the entry has it (an unknown upper bound absent) | carried into the archive's rows on merge |
| `conflicts` | `(capture_id, seq_from, seq_to)` and the two packs holding versions of that seq run, `suspect`, `confirmed` or `acknowledged` (the operator closed the incident, `operations.md` §10.4) | surfaced by `ListPacks`; blocks the scope's merge, in every state, unless one of its packs is `excluded`; a run, not a row per seq, since a redelivered batch overlaps its first copy over every seq it holds |
| `claim_objects` | claim object key, capture, seq, **hour** of the record (from the receipt's header), size; `survivor` with the evicted hour once retention skipped it (§8.4) | deleted when §8.4's predicate holds: with the hour by retention, or later as a survivor by the reconciler |
| `members`, `member_configs` | replica id, lease expiry, generation; the configurations it executes | the activation gate and membership leases of §6.4; named so since the cutover's fence (T-43), which an old binary's `consumers` does not reach |
| `parked`, `parked_copies`, `bus_streams`, `termination_orders` | the parking tables | in [parking.md](parking.md) |
| `scopes` | `(capture_id, hour)`: `state` `open`/`settled`/`reopened`/`unmergeable` (`compactor.md` §8.1, T-45), `revision`, `archive_pack_id`, `merged_at` | CAS on `revision` (§8.1) |
| `scope_events` | `(seq, capture_id, hour, event, revision, at)`, `seq` from the `counters` row; a `low_water` and an `epoch` in `counters` | the LS-11 cursor (§7.3); pruned by seq prefix only |
| `counters` | named counters: `scope_events`, and the `membership` sentinel, whose value counts registrations and numbers each membership's generation, never reused | a row lock serializes allocation with commit order (`scope_events`) and admission with membership changes (`membership`, §6.4, T-32) |
| `merges`, `merge_inputs` | every merge with its inputs, output, outcome and `delete_at`, the time its inputs are deleted (`merges.at` + D, fixed at the commit) | the deletion relation and a merged input's deadline (`compactor.md` §8.3) |
| `retention_periods` | the retention period recorded by the compactor, with the period held in force after a shortening and until when; one row per change, the latest in force | the deadline and eviction horizon of `compactor.md` §8.3, the same in every process |
| `unattributed_objects` | claim bucket objects without the SDK's metadata: key, size, first seen | `object list --unattributed`; never deleted automatically (`compactor.md` §8.4) |
| `tombstones` | object keys deleted by retention or deletion, with the time | the reconciler never re-registers them |
| `processing_configs`, `policies` | versioned configurations captures bind to | never updated in place |
| `key_bindings` | `key_id`, kind, provider reference and version, material fingerprint | insert-or-compare, fail closed (§9) |
| `leases` | name, holder, `fence_token`, `until` | compactor singleton, the row `compactor/2` since the cutover's fence (T-43) |
| `compact_requests` | an operator's `scope merge` or `capture close`: target, requester, time, `state` `pending`/`done`/`failed` with the outcome | executed by the compactor holding the lease at its next cycle, under that lease, and retried until it ends (T-41) |
| `audit_log` | restores, digests, exports, admin changes, by whom | append-only |

`seq` is at most 2^63 − 1, so it fits a signed `BIGINT`; times are `BIGINT` UTC nanoseconds; UUIDs are `BINARY(16)`.
`packs` and `pack_ranges` are the large tables:
about 1,500 tools × 12 segments an hour is some 13 million pack rows a month before deletion,
so their indexes are the design.

### 7.2 Operations

Admit a capture (the prologue of §6.3 inserts-or-finds the row; `pending → open`, or descriptor bytes compared on an admitted one), look a capture up by id (cached in `ingest`), register a pack (§6.3, idempotent on `pack_id`),
list packs and ranges by tool and hour range, pick scopes to merge, commit a merge (§8.1), insert and publish a close (§8.2), pick deletable packs (§8.3),
take or renew a lease, allocate an event seq (the last statement of the emitting transaction, below), append `audit_log`.
About forty statements;
point lookups, range scans on one index, inserts, and the multi-statement domain transactions below, guarded by **compare-and-swap predicates with affected-row checks** and unique constraints;
no `SELECT … FOR UPDATE`, so the same statements mean the same on both engines;
the intentional row locks are the capture row, first in every pack-changing transaction (§6.3),
the `leases` row, taken by a transaction the compactor commits under its lease after the capture row,
the `counters('membership')` row, taken by admission and every membership change (§6.4),
and the `counters('scope_events')` row, last (§7.3).

**Domain transactions.**
The catalog's callable interface is the transactions below and the domain reads of §7.3;
no caller issues a statement, opens a transaction or chooses a lock.
One unexported helper runs the prologue of §6.3:
one upsert that inserts a `pending` capture row when the catalog has none for the capture and otherwise updates the capture's row (`data_revision + 1`),
so it takes the row's exclusive lock at once (an insert-ignore followed by the update would hold a shared lock between them on MariaDB, and two transactions doing so on one row deadlock);
it returns the capture row as read under that lock, which is where the transaction's guard reads begin.
Every transaction of the table starts with that helper,
so the capture row is first by construction and no caller restates the lock order.
A second unexported step allocates an event seq from the `counters` row as the transaction's last statement (§7.3);
only a transaction that emits one of the events of §7.3 calls it, and the last column shows which.
Signatures are a sketch.

| Transaction (caller) | Preconditions | Guards after the prologue; affected-row checks | Durable outcome | `counters` |
|---|---|---|---|---|
| `Admit` (`ingest`, §6.1) | an accepted descriptor; the delivery's identity (version key, bus incarnation, stream seq) | the delivery's version resolution and the capture's active termination orders re-read under the row: terminated, or covered by an order (the version then inserted `terminated` as `parking.md` states), returns `terminated` and writes nothing else (T-32); the capture's state re-read under the row; a `rejected` capture's retained bytes re-validated against the delivery's; on every binding path the `membership` sentinel updated before `members` and configurations are read | no row or `pending` → `open`, the processing configuration bound; `rejected` → `open`; equal bytes on an admitted capture: nothing; different bytes: `descriptor-conflict`, nothing written | `membership`, when it binds; last: `terminated`, when an order's application terminates the version |
| `Reject` (`ingest`, §6.1) | a descriptor that is not accepted; the delivery's identity | the termination re-check of `Admit`; the capture's state re-read under the row; an admitted capture is never overwritten | `captures.state = rejected` with its descriptor bytes, durably, and the `start` stays deferred whatever its age; on an admitted capture nothing is written: different bytes return `descriptor-conflict`, equal bytes `already-admitted` with the bound processing configuration, and the classifier decides again (T-32) | last: `terminated`, when an order's application terminates the version |
| `RegisterPack` (`ingest`; the reconciler under the compactor lease, §5.4; a close's publication, §8.2) | the object is put (put → register → ack); the reconciler's is verified and its exact seq set read (§5.4); **delivery evidence**, each record's version key, bus incarnation and stream seq, passed by `ingest` only (T-32) | first, `pack_id` already registered: nothing more (idempotent; the registration that committed it won every race), except that a caller with evidence finding the pack `adopted` gets `adopted` and leaves its batch unacknowledged, so the redelivery writes another segment with the receipts (T-32); a `tombstones` row for the key: abort; the hour beyond the retention horizon: abort, and the key is tombstoned before the object is deleted; with evidence, a batch version found `terminated` (at the `parked → staged` CAS included) or a message covered by an active termination order whose seq no registered pack holds: abort, and the key is tombstoned in the same transaction (T-32, T-40); for the reconciler, the lease, a `pending` capture leaves the pack unregistered (§5.4), and a seq with a `terminated` `parked` row or a capture with a termination order: abort, and the key is tombstoned (`storage.md` §5.4) | the rows of §6.3: `packs`, `pack_ranges`, `pack_losses`, `conflicts` with `revision + 1` on every other scope they touch, the capture's highest seq and stop, the scope upserted with `revision + 1`; with evidence only, receipts and a parked record's version `staged` (`parking.md`); without evidence, coverage only | last, when it emits `created`, `reopened` or `conflict` |
| `CommitMerge` (the compactor under its lease, §8.1) | the archive put; the snapshot's revision r and `staged` inputs | `scopes … WHERE … AND revision = r` and `leases … WHERE holder = ? AND fence_token = ?`: zero affected rows on either rolls back the whole transaction, and the archive is never deleted directly (the reconciler adopts it) | the archive's `packs` row with its ranges and losses, `merges`, `merge_inputs`, the inputs `merged`, the scope `settled` with `revision + 1` | last: `settled` |
| `RefuseMerge` (the compactor under its lease, a merge over the reader's metadata budget, `compactor.md` §8.1, T-45) | the merge refused by tracepack over one of the reader's budgets; the snapshot's revision r | `scopes … WHERE … AND revision = r` and the lease: zero affected rows rolls back the whole transaction | the scope `unmergeable` with `revision + 1`; an operator's request the merge executed ended `failed` with the reason | last: `unmergeable` |
| `RecordConflict` (`compact`: a suspect confirmed or cleared, a merge that fails on a conflict, §6.2) | the overlapping records compared, with the pack ids compared, or the merge failed | the lease; to clear, the row still names exactly the compared packs, otherwise `changed` returns and nothing is written, and `compact` compares again next cycle (T-32) | the `conflicts` row written or cleared; a confirmation merges the compared packs in as confirmed and never drops a pack it did not compare; `revision + 1` on every scope it touches | last: `conflict` for each touched scope |
| `CloseIntent` (the compactor, §8.2) | a candidate of the trigger scan | the lease; re-read inside: `state = 'open'`, not `stop_seen`, `last_registered_at` older than I or Q by trigger; `captures … WHERE state = 'open' AND data_revision = d`: zero rows aborts, retried next cycle | the `capture_closes` row fixing every byte-affecting input; the capture `closed` | none |
| `PublishClose` (the compactor, §8.2) | a `pending` intent; the close written from the stored options and put, a lost put checked by `Stat` | those of `RegisterPack` without evidence, on the intent's fixed `pack_id`, and the lease | one transaction (T-32): the close's pack registered, or found already registered (the reconciler may adopt the close's object first, `storage.md` §5.4), then `capture_closes … SET state = 'published' WHERE state = 'pending'`, the event seq allocated last; a horizon-guard abort tombstones the key, and it or a tombstone already on the key (retention evicted an adopted close) sets the intent `abandoned`; a `pending` intent is retried every cycle | as `RegisterPack` |
| `DeletePack` (the compactor after delay D, §8.3) | an input of a successful merge whose output is `staged`, `merged` or, merged in turn, `deleted` | the lease; no `packs` row other than this one names the key; either fails: abort | the pack `deleted` and its `tombstones` row; the object removed only after that commit | none |
| `EvictHour` (retention, one hour of one capture, §8.3) | the hour older than the retention period | the lease and the key check of §8.3; survivors chosen by the predicate of §8.4 under the row | the hour's `pack_ranges` union in `evicted_ranges` (coalesced); its packs, ranges, receipts, scope row and `claim_objects` rows deleted, survivors kept and marked with the hour; the hour's `parked` rows moved `parked → evicted-hour` by compare-and-set (`parking.md`); tombstones written; the objects, survivors excepted, removed only after that commit | none |
| `MarkSurvivorDeletable` (the compactor's reconciler, §8.4) | a `survivor` row | the lease; the predicate of §8.4 | the row `deletable`; the object is then deleted and the row removed under the lease, a step that resumes and never skips | none |
| `BeginParking` (`ingest`, step (1) of `parking.md`) | a deferral indefinite by its nature; the delivery's identity | the termination re-check of `Admit`; the capture's state re-read: for an admitted capture, a new version whose hour is beyond the horizon is terminated as `beyond-retention` instead; a `pending` or `rejected` capture has no guard | the version's `parked` row inserted or found (`parking`), and the original's `parked_copies` row; returns the version's resolution, and a resolved version publishes nothing; a covered seq a registered pack holds resolves as staged and writes nothing (T-40) | last: `terminated`, when an order's application terminates the version |

The parking protocol's later steps (`parking → parked`, a copy marked `acked`, a copy row inserted by the parked consumer, a version moved to `evicted-hour`), operator termination and a termination order's application are specified in `parking.md`;
each starts with the helper like every transaction of the table (T-32),
and they change rows forward only by compare-and-set.
Parking touches no counter except a termination's, by the operator or by an order's application (T-33):
for each hour holding a seq it terminated, it upserts that scope with `revision + 1`, creating it when missing, and allocates a `terminated` event as its last statement;
an hour retention has evicted gets neither, since its scope is gone and its seqs read from `evicted_ranges` and `parked`.
Every compactor transaction that changes domain state checks the lease (`leases … WHERE holder = ? AND fence_token = ?`, affected rows checked) after the helper, with no exception (T-32).
Outside the table, without the helper:
taking or renewing the compactor lease (`leases`; a worker that lost it does nothing further, §8.1);
registering, renewing or leaving a membership and activating or retiring a configuration (`rollouts.md` §6.4), each of which updates `counters('membership')` first, judging a lease's lapse by the database's clock as `Admit` does (T-32);
pruning `scope_events` by seq prefix, which checks the compactor lease, sets `counters.low_water` in the same transaction and allocates no seq (§7.3);
appending `audit_log` for the API's actions (other audit rows are written inside their own transaction);
and the reads (look a capture up, list packs and ranges, pick scopes to merge, pick deletable packs),
whose answers each transaction above re-validates instead of trusting.

**Retries stay inside the module.**
A MariaDB deadlock or an `SQLITE_BUSY` re-runs the whole transaction from the helper (§7.4), so its guards are read again;
the write pool's one connection, `BEGIN IMMEDIATE` and the second writer's refusal to start are the module's, never a caller's.
A failed scope-revision or lease check rolls back the whole transaction (§8.1), as does every other guard that aborts,
except the durable write a guard's own outcome names (a tombstone, an `abandoned` intent, a `terminated` version), which commits alone in its place;
the transaction returns that outcome and is not retried,
and the caller acts on it: the merge and the close at the next cycle (§8.1, §8.2).

### 7.3 Completeness (LS-10) and the change cursor (LS-11)

- **Producer coverage**: for a capture, the union of `pack_ranges` of its non-deleted packs over all hours, plus `evicted_ranges`;
  the close sentinel 2^63 − 1 is never in either.
  The **frontier** is the higher of `highest_seq` (the highest producer seq, §6.3) and the capture's highest parked seq (`parking.md` §7.1, T-33).
  A **gap** is a hole in that union between seq 0 ([FMT I-12]) and the frontier, other than a deferred or terminated seq (below),
  reported with the time interval between the records on either side (from the endpoint times),
  which may span hours or run backwards after a clock step;
  a missing prefix has no record before it,
  so its interval starts at the capture's `capture_origin_utc_ns` and is marked open-ended on that side.
  A hole inside `evicted_ranges` is reported as **evicted**, a third class beside gap and unknown;
  a seq both retained and evicted counts as retained;
  a gap next to an evicted run takes its time bound from the evicted run's endpoint time.
  The seqs above the frontier are the **unknown tail**, whatever the capture's state, closed included ([STO §4]).
  A segment of hour h that arrives after the merge still extends the union;
  nothing about completeness depends on scope state.
- **Deferred and terminated seqs** (T-33) read from the `parked` table, kept for the capture's lifetime (`parking.md` §7.1):
  a seq whose version is `parking`, `parked` or `evicted-hour` is **deferred**, with its time from the carried header;
  a seq whose version is `terminated` is a **declared loss** with the action id in `terminated_by`.
  Each seq is classified once, by the precedence retained > evicted > deferred > declared loss > gap,
  since an unresolved version of the seq can still be staged;
  the precedence classifies the seq only, and a declared loss of one version is still reported beside another version that is retained.
  Classification runs up to the frontier,
  so a parked seq above `highest_seq` is deferred or lost, never the unknown tail,
  and a parked seq never reads as a gap (`parking.md` §6.1).
- **Declared losses** come from `pack_losses` and from `terminated` `parked` rows;
  **crash gaps** from `capture_closes` (`gap_start`, `gap_end`);
  **conflicts** from `conflicts`.
- **Selection**: a read names a tool, optionally its devices (at most 1,000) or one capture, and a nonempty window [from, to).
  It selects the admitted captures (`open`, `stopped` or `closed`) of that tool and those devices whose span meets the window:
  a capture's **span** runs from `capture_origin_utc_ns` to `highest_seq_ts`, or is the origin alone before any record,
  and for the window only, a capture still `open` has a span without end, since it may yet record.
- **Read set** (T-48): the packs a human read reads for a capture and an hour, which `QueryRecords`, `FindTransaction` and `Export` share (`api.md` §10.1):
  the hour's `staged` packs (its last merge's archive and the segments registered after it, or every segment before a merge) and its `excluded` packs, which are read and marked;
  never a `merged` input, which its archive holds, nor a `deleted` pack.
  It is read from one snapshot, each pack with its object key, size, role, state and ranges,
  together with whether the hour has a scope and whether retention evicted ranges of it.
  The read set names the capture's configuration (`captures.config_id`), and outside `tracekeep dev` a capture whose configuration has no policy is **withheld**:
  the read checks every read set it reads and reads none of a withheld capture's packs, naming it withheld instead,
  so a capture admitted before the cutover never shows a clear body (`rollouts.md`, T-42).
- `ListPacks(tool, from, to)` iterates the requested hours, at most a leap year's 8,784:
  for each, the scopes of every selected capture overlapping it, with packs, ranges, state and revision;
  an hour with no scope is reported as `no-data` together with whether a selected capture's span, unbounded while `open`, meets that hour,
  so a missing whole hour is a result, not an absence;
  an hour with no scope whose ranges retention evicted is reported evicted instead, naming the captures,
  and an hour with no scope that holds parked seqs reports them as deferred or declared losses (T-33),
  read from the capture's completeness, so each seq keeps the one class the precedence gives it.
  An **unregistered interval** is reported
  when a capture starts more than Q after the latest span end of every earlier capture of its (`tool_id`, `recorder_instance_id`),
  earlier meaning an earlier origin,
  not after the end of one predecessor, so two overlapping captures are never reported as missing traffic;
  or when its `previous_capture_id` names no admitted capture, an interval whose start is unknown.
  The first capture of a recorder, with no earlier capture and no `previous_capture_id`, has none.
- **Domain results**: the read side returns results, never rows for a caller to classify.
  `ListPacks` returns the watermark it read before the listing (*Cursor* below) with each capture's completeness:
  gaps with their time intervals, evicted, deferred and unknown-tail seqs, `no-data` hours, unregistered intervals, declared losses, crash gaps and conflicts, an unresolved suspect reported as a conflict (§6.2);
  `ListCaptures` and `ListDevices` return the `overlap` flag;
  `ListDevices` reports per device the latest selected capture's `equipment_endpoint` and `device_id` from its descriptor,
  how many selected captures are `open`, the overlap flag, the last record time (the highest `highest_seq_ts`),
  and whether the window holds a gap: a gap whose time interval meets the window, an unknown bound counting as meeting it;
  `ListScopeChanges` (`api.md` §10.1) returns the events after a cursor, or reports the cursor stale when it is below `low_water` or carries another epoch,
  so the module detects a stale cursor and `ListPacks` returns the watermark to continue from:
  the client re-lists the window and continues from that watermark.
- **Overlap**: two leaders running at once write two captures of one (`tool_id`, `recorder_instance_id`), which [STO §4] allows.
  When the spans of two such captures intersect (*Selection* above, never unbounded here),
  both are flagged `overlap` in `ListCaptures` and `ListDevices`;
  the flag is computed from the spans, not from the `open` state, so it survives the close of either capture.
  A read by device returns both, each with its own completeness, and never interleaves them into one series.
- **Settled** means "merged, and nothing more is expected on the normal path":
  a scope becomes a merge candidate at hour end + quiet period Q (default 15 minutes, above F + `ack_wait` + the redelivery slack `compact.redelivery_slack`, default 2 minutes, T-41),
  and `settled` when the merge commits; a registration afterwards sets `reopened`; a re-merge sets `settled` again.
  A scope whose merge exceeds the reader's metadata budget is `unmergeable` and never settles, a registration leaving it so, until an operator's `scope merge` settles it (`compactor.md` §8.1, T-45).
  Settlement is a scheduling fact; completeness is the ranges and gaps, reported beside it.
- **Cursor**: `scope_events.seq` is allocated from the `counters` row as the last statement of the emitting transaction;
  the row lock makes allocation order commit order on MariaDB, and SQLite has one writer,
  so a reader that has seen seq s never misses a lower seq committed later.
  Events are emitted for `created`, `settled`, `reopened`, `conflict` (a suspect raised, confirmed or cleared on that scope, whichever hour the other version is in, §6.2)
  `terminated` (a seq of that hour terminated, deferred becoming a declared loss, §7.2, T-33)
  `excluded` (a pack of that scope excluded from its merges by the operator, `operations.md` §10.4, T-41)
  and `unmergeable` (its merge refused over the reader's metadata budget, `compactor.md` §8.1, T-45);
  every conflict transition, in registration or in `compact` (confirmation, clearing, a merge that fails on a conflict), increments the revision of **every** scope it touches in the same transaction, capture row first and `counters` last,
  so a replayed event is never ignored for a changed state.
  Pruning deletes a **seq prefix only**:
  `low = min(seq) over events younger than the retention period`, delete `seq < low`, set `counters.low_water = low` in the same transaction, never by time alone,
  since event time and seq are not ordered together;
  the period is the configured retention period of §8.3, and the compactor prunes in its retention pass under the lease;
  `low_water` is only ever raised, so a retry or an older snapshot never lets a pruned seq read as present.
  `epoch` changes only when an operator truncates otherwise.
  A **cursor** is the pair (epoch, seq): every event up to seq has been read.
  It is stale when it carries another epoch or when a pruned seq lies above it, `seq < low_water − 1`,
  and a client with a stale cursor re-lists the window from `ListPacks` and continues from the watermark and epoch that listing returns.
  `ListScopeChanges` returns the events above the cursor whose capture is of the selected tool and devices, in seq order, a page at a time,
  with the cursor to continue from:
  the last seq the page scanned, so events of other tools never hold it back,
  or, when the page is short, the watermark of its snapshot, read before the events as `ListPacks` reads it.
  The **watermark is read before the listing**:
  `ListPacks` reads `counters.scope_events` = w first, then the scope and pack rows, and returns w;
  an event committed during the listing has seq > w and is replayed to the client,
  which treats events as idempotent (each carries the scope's revision, and a replayed revision is ignored).
  Pruning with no event younger than the retention period deletes nothing.

### 7.4 Engines, transactions and migrations

- **Recommended stack**: `database/sql` + `sqlx`, hand-written SQL, `go-sql-driver/mysql` and `modernc.org/sqlite` (no cgo).
  A `dialect` value isolates insert-ignore and the upsert clause; both engines use `CHECK` constraints, not `ENUM`;
  the transactions are CAS-based and engine-neutral (§7.2).
- **MariaDB** 11.1 or later: `READ COMMITTED`, the session in UTC, strings compared byte for byte, trailing spaces included (`utf8mb4_nopad_bin`), as on SQLite; every multi-statement transaction retries whole on deadlock;
  the capture row, the `leases` row and the two `counters` rows are the intended lock waits, always in the order of §7.2.
- **SQLite**: one writer connection (`MaxOpenConns = 1` on the write pool), `BEGIN IMMEDIATE` for every write transaction, `busy_timeout` set, retry whole on `SQLITE_BUSY`;
  only the `all` process writes, enforced by deployment and checked at start (a second writer refuses to start on SQLite).
- **Tests**: the repository tests run the same adversarial schedules on both engines with barriers, not timing,
  against the transactions of §7.2:
  registration during a merge snapshot, lease takeover between statements, close intent versus a late registration, event allocation with an uncommitted earlier transaction, two first uses of one key id,
  a termination racing an admission, a parking and a registration, and a membership renewal racing an admission.
- **ent** rejected for now (code generation and a graph model for forty flat statements; its migration opinions beside Atlas);
  **sqlc** rejected (per-engine query sets);
  **bun** is the fallback if the dialect value grows.
- **Migrations**: Atlas versioned.
  One `schema.hcl`;
  `atlas migrate diff` produces `migrations/mariadb/` and `migrations/sqlite/` with their `atlas.sum`;
  CI runs `atlas migrate validate` and checks that `atlas migrate diff` finds nothing to add, on both engines
  (`atlas migrate lint` is available only to Atlas Pro users since Atlas v0.38).
  `tracekeep migrate` applies them with Atlas's Go library (`ariga.io/atlas/sql/migrate` and its `mysql` and `sqlite` drivers, which build without cgo, T-41),
  checking each directory's `atlas.sum` and recording every applied revision in a table of its own, `tracekeep_revisions`,
  so a deployment needs no Atlas binary; the Atlas CLI stays the development tool for `diff` and `validate`,
  and a catalog is migrated by one of the two, never both:
  a catalog that holds tables but no revision in `tracekeep_revisions` was migrated by another tool and is refused.
  A migration runs statement by statement, as Atlas runs it; one that fails is recorded with the statements it applied,
  and the next `tracekeep migrate`, once the failure's cause is gone, resumes from the statement that failed.
  Migrators take turns: on MariaDB under a named lock (`GET_LOCK`, named for the database) held from before the revisions are read to the last statement,
  on SQLite under the catalog's writer lock, so a SQLite catalog is never migrated beside a running service.
  A `tracekeep dev` directory made before this path existed holds tables and no revisions, and is refused like any other tool's: dev starts on a fresh one.
  `tracekeep migrate` applies the catalog first, then ensures the bus, whose incarnations it records in the catalog.
  Schema changes are expand-then-contract across two releases,
  so an old and a new consumer run against one schema during a rollout.
