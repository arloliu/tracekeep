# tracekeep — compactor

Status: draft (2026-10-25) — split from seed v21

Covers the compactor's state machines: merge, closing a capture left open, deletion and retention, and claim-check objects.
From seed v21 §8; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/compact` (with `.agents/rules/600-invariants.md`).

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Singleton**: one active compactor under a catalog lease with a fence token;
  every commit that changes domain state checks the lease, with no exception, and a worker that lost it does nothing further (§8.1, T-32).
- **Merge**: snapshot a scope's revision r and its `staged` inputs, merge, put the archive, then commit in one transaction guarded by r and the lease;
  zero affected rows rolls back the whole transaction, and a rolled-back merge never deletes its archive directly (§8.1).
- **Close**: an intent transaction fixes every byte-affecting input, then `tracepack.WriteClose` writes the same bytes on every retry;
  pending intents are retried each cycle;
  a capture is closed by a later capture of its device only once it has itself been silent for Q,
  so an overlapping live capture is never closed (§8.2).
- **Deletion**: a pack that is an input of a successful merge whose output is retained is deleted after delay D,
  which exceeds any download URL's lifetime;
  the tombstone commits, in a transaction that starts with the capture row, before the object is removed (§8.3).
- **Retention**: whole hours, from each record's own `ts_utc_ns`; ranges move to `evicted_ranges`;
  claim objects go with their hour, survivors excepted (§8.3, §8.4).
- **Claim objects**: no early sweep;
  one deletion predicate, for an hour's eviction and for survivor cleanup alike, evaluated under the capture row:
  one function called inside both deletion transactions, with the current incarnation an explicit input,
  never sufficient alone (an admitted capture, an evicted hour and an attributable hour remain prerequisites);
  an object without the SDK's metadata is never deleted automatically (§8.4).

## 8. Compactor state machines

### 8.1 Merge

1. Take the compactor lease (`leases`, `fence_token` = a monotonic counter) and renew it.
2. Candidate scopes: `open` or `reopened` (never `unmergeable`, below), hour end + Q passed, having packs in state `staged`, and no `conflicts` row
   (a row touches a scope when either of its packs is a pack of that capture and hour, in any state,
   unless either pack is `excluded`, which is never an input).
3. Snapshot: read `scopes.revision` = r first, then the **inputs**:
   every pack of the scope in state `staged`, segments and archives alike (an archive stays `staged` until it is an input of a later merge);
   whatever changes after r was read advances the revision, so the commit's compare-and-set catches it on either engine.
4. `tracepack.Merge` over `ReaderAt`s of the inputs; write the archive to a temp file; put under its `archive/` key.
   The archive's `scope_generation` is the revision its commit gives the scope (r + 1), its `publisher_epoch` the lease's fence token,
   and its writer tag the service's, as a segment's; the merge generates its `pack_id` and `replacement_set_id`.
5. Commit, **one transaction**, all or nothing, starting with the capture row update of §6.3:
   insert the archive `packs` row with its `pack_ranges` (the union) and `pack_losses`;
   insert `merges` and `merge_inputs`; mark inputs `merged`;
   `UPDATE scopes SET state='settled', archive_pack_id=?, revision=revision+1 WHERE … AND revision=r`;
   `UPDATE leases … WHERE holder=? AND fence_token=?`;
   re-read under the capture row that no `conflicts` row touches the scope and every input is still `staged`, aborting otherwise;
   the `counters` update that allocates the `settled` event's seq is the last statement (§7.3);
   zero affected rows on either update **rolls back the whole transaction**.
   A rolled-back merge **never deletes its uploaded archive directly**:
   deletion is owned by the tombstone protocol of §8.3, under the lease, and only for a key no `packs` row names;
   a worker that lost its lease does nothing further,
   and the orphan is adopted by the reconciler as a `staged` pack, an input of the next merge,
   where [FMT I-12] deduplicates its records.
6. A crash after the put and before the commit leaves the same orphan, handled the same way.

Because every registration bumps `revision` (§6.3),
a segment registered between the snapshot and the commit, in any scope state, fails the CAS,
and the next cycle merges it with the registered archive.
A merge failure (conflict, damaged input) leaves the scope as it was and raises an alert;
the operator acts ([STO §4] *Merge*).

**A merge over the reader's budget** (T-45):
tracepack refuses, before it writes anything, a merge whose archive's metadata would exceed the reader's metadata budget (`ErrMergeLimit`)
or one of whose inputs exceeds it (`ErrReadLimit`),
which many masked segments of one hour can reach though each is within it (`ingest.md` §6.2);
the scope's inputs only grow, so the refusal is deterministic.
The merge then commits the refusal instead, one transaction starting with the capture row (`catalog.md` §7.2 `RefuseMerge`):
`UPDATE scopes SET state='unmergeable', revision=revision+1 WHERE … AND revision=r`, the lease check,
an operator's request it executes ended `failed` with the reason, and the `unmergeable` event last;
a stale revision writes nothing, and the next cycle tries once more.
An `unmergeable` scope is never a candidate, and a registration into it advances its revision and leaves it `unmergeable`;
only an operator's `scope merge` merges it again (`operations.md` §10.4), which settles it or refuses it again.
Its packs stay `staged` and readable, and its completeness is their ranges and gaps;
none of them becomes deletable, since no merge took them (§8.3), and retention evicts the hour on time.
The refusal is counted apart from a failure, so it neither withholds the merge task's success nor counts as an unmerged scope's age (`operations.md` §10.4).

### 8.2 Close a capture left open

Trigger: a later capture of the same (`tool_id`, `recorder_instance_id`) has been registered for at least Q (a handover's orderly `stop` has had time to be flushed)
**and** the capture itself has had no registration for Q;
or no registration for an inactivity period I (default 2 hours).
`previous_capture_id` is absent after a gateway restart (§4.2), so adjacency, not that field, is the trigger.
The successor trigger looks at every later capture of the key and never chooses between candidates.
Its second condition keeps two overlapping leaders (§7.3 *Overlap*) from closing each other's live captures:
a capture that is still receiving records registers a segment at least every F, and Q exceeds F plus `ack_wait`,
so only a capture whose leader stopped writing is closed this way.
A capture with no records for Q is closed although its leader lives;
records that arrive later are staged as below, and the inactivity trigger would close it at I anyway.

1. Intent, one transaction starting with the capture row update of §6.3:
   read `captures` (`state`, `data_revision` = d, `highest_seq`, `highest_seq_ts`, `stop_seen`, `last_registered_at`);
   abort unless `state = 'open'` and not `stop_seen`;
   for the inactivity trigger abort unless `last_registered_at < now − I`, and for the successor trigger unless `last_registered_at < now − Q`,
   **inside this transaction** (eligibility is re-validated, not carried from the candidate scan);
   `INSERT INTO capture_closes` with trigger time, those values, the bounds, a fixed `pack_id` and `writer_time`, and the **serialized `PackMeta`** the close will be written with (a close is always written uncompressed, G5-272, T-36),
   its `classifier`, `max_frame_len` and, for a masking configuration, `redaction_policy` taken from the capture's bound configuration (`rollouts.md` §6.4) before the intent is persisted;
   an intent persisted by an earlier release replays the bytes it holds;
   `UPDATE captures SET state='closed' WHERE capture_id=? AND state='open' AND data_revision=d`;
   zero rows aborts (a registration raced the snapshot; retry next cycle).
2. Write: `tracepack.WriteClose` with exactly the stored options and `Now` returning `writer_time`,
   so a retry under a new compactor version writes the same bytes;
   put under its `staging/` key;
   after a put whose result was lost, `Stat` the key and compare size and digest with the intent before proceeding;
   in **one transaction** (T-32), register the pack, or find it already registered (the reconciler may adopt the close's `staging/` object first, §5.4), and set `state='published'` on the `pending` intent, the event seq allocated last;
   when the horizon guard aborts the registration, or the key is already tombstoned (retention evicted a close the reconciler had adopted), the intent is set `abandoned` instead, so it is not retried.
3. Every compactor cycle retries `pending` intents; an `abandoned` one is final.

A record that arrives after the intent is staged like any record;
only a genuine `stop` boundary sets `stop_seen`;
the capture stays `closed`, the close's bytes and gap are never recomputed,
and a read whose range meets the gap reports `incomplete` ([STO §4]).

### 8.3 Deletion and retention

A pack is deletable when it is an input of a successful merge
whose output pack is in state `staged` or `merged`, or `deleted`, which only a merged pack becomes, its records living on in a later merge's output (the relation in `merges`/`merge_inputs`, never a flag or lineage tag),
so a chain of merges is deleted in any order and a refused input stays a candidate;
it is deleted a delay D (default 1 hour) after its own merge, at the `merges.delete_at` its commit fixed (`merges.at` + D), D exceeding the lifetime of any download URL, presigned or served by the API (§5.2 *Downloads*, §10.1), by more than twice two minutes of clock skew (below), and any read in flight;
a URL issued after the merge is clamped to that time instead (§10.1 *Downloads*, T-33),
and a `tombstones` row is written in the same transaction as the `deleted` state.
Every deletion, by this rule or by retention, runs a transaction
whose **first statement updates the capture's row** (the same row every registration updates first, §6.3),
then checks the lease and that no `packs` row other than the one being deleted names the key,
then inserts the `tombstones` row;
the object is removed only after that commit.
Registration and deletion of one key therefore serialize on the capture row before either reads its predicates, on both engines, and have mutually exclusive durable outcomes:
either the tombstone commits first and the registration's predicate aborts it,
or the registration commits first and the deletion finds a `packs` row and aborts.
The lock order is capture row → `leases` row → `counters('membership')` row → `counters('scope_events')` row, each step optional, nothing else is locked (T-32).
A download URL's expiry is clamped to its object's **deadline**, the earliest time it may be deleted (§10.1 *Downloads*),
and retention never evicts an hour before a time an issued URL was promised (T-33).
The deadline is **one function**, the same in the api role that clamps a URL and in the compactor that deletes,
of catalog rows alone, the latest `retention_periods` row and a merged input's `merges.delete_at`, never of a process's own configuration,
so every process and every restart computes the same deadline:
- a row holds the retention period P and, after a shortening, the period H held in force until a time U;
  the period in force at t is P, or the larger of P and H while t < U;
- the compactor records its configured period under the lease before every retention pass, by inserting a row when it differs from the latest's P,
  so the record locks no existing row (the lock order is unchanged):
  a longer period is inserted with the latest row's H and U;
  a shorter one with H the period in force at that moment and U now + D,
  so two shortenings inside one window hold the longest;
  D exceeds every URL's lifetime by more than twice the two minutes two hosts' clocks may disagree (the download margin's skew, T-33),
  once between the hosts that issue and validate the URL and once between those that record U and evict,
  so a URL issued before the change has expired by U however their clocks differ;
- an hour may be evicted at the first t at which its end plus the period in force at t has passed,
  which is its deadline, and the eviction horizon is the hour that rule reaches now;
  a merged input's deadline is the earlier of its hour's and its own merge's `delete_at` (through `merge_inputs`), never the scope's `merged_at`, which a later merge moves,
  so a changed D applies to later merges only;
  a merge a version before `delete_at` committed, during a rollout too, leaves it 0, the column's default, read as `merges.at` + the deciding process's D;
- before the first record no deadline exists and no URL is issued.
The registration's horizon guard (§6.3) follows the configured period at once:
refusing a registration early loses nothing an issued URL was promised; only eviction waits.
The guard and eviction read different processes' clocks,
so their mutual exclusion (`parking.md` *Retention, admission and parking*) assumes, as Q, I and the download margin do,
one configured period in every role and host clocks that agree well within an hour boundary.
An hour whose eviction a guard refuses (another hour's `packs` row naming one of its keys), like a merged input whose deletion one refuses,
is left in place, logged and counted every cycle, and the task records no success until an operator decides.
A completed export bundle under `exports/` is deleted at the end of its retention (default 24 h), its URLs clamped to that time likewise.
Retention deletes whole hours older than the eviction horizon:
in one transaction per hour of one admitted capture it copies the hour's `pack_ranges` union into `evicted_ranges`, deletes the packs, ranges, receipts, the hour's `claim_objects` rows **except survivors** (§8.4) and the scope row, and writes the tombstones;
then it deletes the hour's claim objects, survivors excepted, from the bucket.
The rows of an evicted pack go with it: its `pack_ranges` and `pack_losses`, the `merges` and `merge_inputs` naming it and the `conflicts` rows naming it,
so a declared loss of the hour reads afterwards as any hole outside `evicted_ranges` does;
`evicted_ranges` are coalesced within their hour, keyed by capture, hour and first seq, so an evicted hour stays distinct from a `no-data` one,
and they end the hour for the capture: a registration (`ingest`'s, the reconciler's adoption, a close's publication) or a parking into an hour they hold is refused as beyond the horizon whatever horizon its caller read (`ingest.md` §6.3),
so a late pack never makes an evicted hour a candidate again;
an hour evicted while it held no pack keeps none, and what a caller's earlier horizon lets into it is evicted again (`parking.md` *Retention, admission and parking*).
An hour holding claim objects but no scope is a candidate like any other:
the claim bucket is listed before every retention pass, and an object without a `claim_objects` row is attributed by its name, `<capture_id>.<seq>`, and the SDK's metadata (§8.4) and given a `live` row,
so a crash between an eviction's commit and its bucket deletions leaves objects the next listing attributes to the now scope-less hour, which is evicted again.
The listing's transaction checks the lease and takes no capture row:
it only inserts a missing row, as `ingest`'s receipt does, both ignoring an existing one,
and taking the row of a capture the catalog does not know would create a `pending` capture for a stray object.
The one retention decision therefore governs packs, receipts and claim objects alike, from the record's own `ts_utc_ns`, never from an object store clock.
Seq order and hour order differ after a clock step, so evicted seqs are a set of ranges, not a prefix (§7.3).
The same pass prunes the change cursor's events by seq prefix with the configured retention period (`catalog.md` §7.3), under the lease.
Holds for LS-13 (a), if the owner chooses them, exempt an hour from retention.

### 8.4 Claim-check objects

There is **no early sweep**.
A claim object lives until retention evicts the hour of its record (§8.3), under the same decision and the same clock as the record itself.
With `max_payload` near 8 MiB almost no record travels by claim-check,
so the bucket holds few objects, encrypted at rest and behind TLS like the stream ([STO §4] *Clear-text protection*, G5-213),
and the cost is a month of rare large payloads.
What this buys: every late or conflicting reference inside retention finds its payload;
the service never deletes an incarnation it did not stage;
no proof about acceptance times, drained subjects or object incarnations is needed.

- The **already-staged** test for a delivery whose object is missing is "a receipt for (capture, seq) exists with the same 44-byte header and payload digest" (§6.1), or a `parked` row of the same version in state `staged` or `terminated`,
  which outlives the hour (§6.1 *resolution*: acknowledged as already staged or under the termination);
  a differing receipt is a conflict and the delivery is deferred as `claim-conflict`.
  A missing object inside retention is therefore always an incident (a bucket fault or a non-SDK producer that never completed its put), never a consequence of this design's own deletion.
- **Objects without a receipt** are governed by the same decision from the same clock:
  the SDK writes the record's `ts_utc_ns` into the object's `Metadata`,
  so retention deletes a receipt-less object
  when the **hour of that timestamp** is evicted, exactly as it would a record, holds included,
  **and only if its capture is known** (admitted);
  an object of a `rejected` capture, or of a capture the catalog does not know (a `pending` row or none: admission unresolved), is kept
  until the capture is resolved,
  since G5-251 and G5-257 may keep its reference deferred indefinitely
  and a fixed consumer must still be able to fetch and check it;
  an object referenced by an **unresolved** `parked` row (state neither `staged` nor `terminated`, §6.1: the copy is an accepted, unacknowledged message that may still need it) of an **admitted** capture (a `write-failure`, a `claim-conflict`, an object missing at delivery and restored later) is likewise kept:
  the retention transaction of §8.3, serialized with the parking protocol on the capture row, skips within the evicted hour every object whose (capture, seq) has such a row, keeps its `claim_objects` row and marks it **`survivor`** with the evicted hour, counts the skip (`claim_objects_survivors`) and logs it per hour,
  because the parked copy is an accepted, unacknowledged bus message under the *Large records* lifetime invariant (G5-267);
  **one predicate, for the hour's eviction and for survivor cleanup alike, evaluated under the capture row**:
  the object of (capture, seq) is deletable only when **every** `parked` version row of that (capture, seq) is `staged`, or `terminated` with every known copy row of the **current** incarnation `acked`
  (a `lost` copy of a retired incarnation does not block a terminated version:
  its message cannot return,
  and a same-version message that ever appears again is acknowledged under the termination without the object, G5-269;
  a `lost` copy of an **unresolved** version resolves nothing, that version waits for the operator,
  and `parked list` shows versions whose every copy is `lost`);
  after `staged` every later message of that version is acknowledged as already staged,
  after `terminated` every later message of that version is acknowledged under the termination without the object
  ([STO §4] *Large records*: an object may go once every message still naming it will be acknowledged as already staged or under a termination, G5-269),
  and an object named by several versions (claim-check objects are named `<capture_id>.<seq>`) is needed until the last of them satisfies this;
  the known-copies clause for a terminated version is the service's own caution (G5-269 does not require it),
  so that the common case, every message of the version already delivered, releases the object only after the server confirmed their acknowledgements;
  the predicate is **one function**, called inside both deletion transactions
  (the retention transaction of §8.3, where false makes the object a survivor, and the reconciler's `deletable` mark below, where false leaves the survivor in place),
  whose inputs are the `parked` version rows of (capture, seq) with their copy rows and, **explicitly**, the current incarnation of each stream (the unretired `bus_streams` row of each role, `parking.md`);
  it keeps the asymmetry above (`staged` needs no copy-acknowledgement gate, `terminated` does)
  and **does not by itself authorize deletion**:
  it decides only whether a parked version still needs the object,
  and an admitted capture, an evicted hour and an attributable hour (the `claim_objects` row, or for an object without a receipt the SDK's `Metadata`, below) remain prerequisites the caller checks;
  the resolution of a parked message changes nothing about its object **inside** retention:
  staged or terminated while its hour is still retained, the object stays until §8.3 evicts the hour, like any other (no early sweep);
  a survivor is deleted only once that predicate holds, by the compactor's reconciler in a durable, retried step:
  mark the `claim_objects` row `deletable` under the capture row and the lease (the `parked` and `parked_copies` rows stay), delete the object from the bucket, remove the `claim_objects` row under the lease;
  an interrupted step resumes, never skips;
  `tracekeep admin object list --survivors` shows survivors with their evicted hour and bytes;
  an object without a receipt whose `Metadata` lacks the SDK's header and time, or whose name, header seq and time disagree (a non-SDK producer), is **never deleted automatically**
  (one a receipt names is attributed by the receipt and goes with its hour):
  the listing records it in `unattributed_objects` until it is gone,
  `tracekeep admin object list --unattributed` shows it and the operator decides,
  which is the conservative disposition for a producer the design does not support.
- A **conflicting version** of a claim-check record (a producer fault, G5-200, G5-214) is kept beside the staged one for as long as the hour is retained:
  the deferred `claim-conflict` delivery can be resolved by the operator (§10.4) while both payloads exist.
  The only payload the service cannot keep is one a non-SDK producer overwrote under the same name before any consumer read it, a stated residual.
- The bucket has no age, size or count limit and the same TLS and at-rest protection as the stream ([STO §4] *Large records*).
