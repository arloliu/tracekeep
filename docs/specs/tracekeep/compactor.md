# tracekeep — compactor

Status: draft (2026-10-25) — split from seed v21

Covers the compactor's state machines: merge, closing a capture left open, deletion and retention, and claim-check objects.
From seed v21 §8; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/compact` (with `.agents/rules/600-invariants.md`).

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes under discussion are in [P1](proposals/p1-interfaces.md).

- **Singleton**: one active compactor under a catalog lease with a fence token;
  every commit checks the lease, and a worker that lost it does nothing further (§8.1).
- **Merge**: snapshot a scope's revision r and its `staged` inputs, merge, put the archive, then commit in one transaction guarded by r and the lease;
  zero affected rows rolls back the whole transaction, and a rolled-back merge never deletes its archive directly (§8.1).
- **Close**: an intent transaction fixes every byte-affecting input, then `tracepack.WriteClose` writes the same bytes on every retry;
  pending intents are retried each cycle (§8.2).
- **Deletion**: a pack that is an input of a successful merge whose output is retained is deleted after delay D,
  which exceeds any presigned URL's lifetime;
  the tombstone commits, in a transaction that starts with the capture row, before the object is removed (§8.3).
- **Retention**: whole hours, from each record's own `ts_utc_ns`; ranges move to `evicted_ranges`;
  claim objects go with their hour, survivors excepted (§8.3, §8.4).
- **Claim objects**: no early sweep;
  one deletion predicate, for an hour's eviction and for survivor cleanup alike, evaluated under the capture row;
  an object without the SDK's metadata is never deleted automatically (§8.4).

## 8. Compactor state machines

### 8.1 Merge

1. Take the compactor lease (`leases`, `fence_token` = a monotonic counter) and renew it.
2. Candidate scopes: `open` or `reopened`, hour end + Q passed, having packs in state `staged`, and no `conflicts` row.
3. Snapshot: read `scopes.revision` = r and the **inputs**:
   every pack of the scope in state `staged`, segments and archives alike (an archive stays `staged` until it is an input of a later merge).
4. `tracepack.Merge` over `ReaderAt`s of the inputs; write the archive to a temp file; put under its `archive/` key.
5. Commit, **one transaction**, all or nothing, starting with the capture row update of §6.3:
   insert the archive `packs` row with its `pack_ranges` (the union) and `pack_losses`;
   insert `merges` and `merge_inputs`; mark inputs `merged`;
   `UPDATE scopes SET state='settled', archive_pack_id=?, revision=revision+1 WHERE … AND revision=r`;
   `UPDATE leases … WHERE holder=? AND fence_token=?`;
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

### 8.2 Close a capture left open

Trigger: the next capture of the same (`tool_id`, `recorder_instance_id`) has been registered for at least Q (a handover's orderly `stop` has had time to be flushed), or no registration for an inactivity period I (default 2 hours).
`previous_capture_id` is absent after a gateway restart (§4.2), so adjacency, not that field, is the trigger.

1. Intent, one transaction starting with the capture row update of §6.3:
   read `captures` (`state`, `data_revision` = d, `highest_seq`, `highest_seq_ts`, `stop_seen`, `last_registered_at`);
   abort unless `state = 'open'` and not `stop_seen`;
   for the inactivity trigger abort unless `last_registered_at < now − I` **inside this transaction** (eligibility is re-validated, not carried from the candidate scan);
   `INSERT INTO capture_closes` with trigger time, those values, the bounds, a fixed `pack_id` and `writer_time`, and the **serialized `PackMeta` and codec** the close will be written with;
   `UPDATE captures SET state='closed' WHERE capture_id=? AND state='open' AND data_revision=d`;
   zero rows aborts (a registration raced the snapshot; retry next cycle).
2. Write: `tracepack.WriteClose` with exactly the stored options and `Now` returning `writer_time`,
   so a retry under a new compactor version writes the same bytes;
   put under its `staging/` key;
   after a put whose result was lost, `Stat` the key and compare size and digest with the intent before proceeding;
   register the pack; set `state='published'`.
3. Every compactor cycle retries `pending` intents.

A record that arrives after the intent is staged like any record;
only a genuine `stop` boundary sets `stop_seen`;
the capture stays `closed`, the close's bytes and gap are never recomputed,
and a read whose range meets the gap reports `incomplete` ([STO §4]).

### 8.3 Deletion and retention

A pack is deletable when it is an input of a successful merge
whose output pack is in state `staged` or `merged` (the relation in `merges`/`merge_inputs`, never a flag or lineage tag);
it is deleted after a delay D (default 1 hour) that exceeds the lifetime of any presigned URL (§10.1) and any read in flight,
and a `tombstones` row is written in the same transaction as the `deleted` state.
Every deletion, by this rule or by retention, runs a transaction
whose **first statement updates the capture's row** (the same row every registration updates first, §6.3),
then checks the lease and that no `packs` row other than the one being deleted names the key,
then inserts the `tombstones` row;
the object is removed only after that commit.
Registration and deletion of one key therefore serialize on the capture row before either reads its predicates, on both engines, and have mutually exclusive durable outcomes:
either the tombstone commits first and the registration's predicate aborts it,
or the registration commits first and the deletion finds a `packs` row and aborts.
The lock order is capture row → `counters` row, nothing else is locked.
Retention deletes whole hours older than the retention period:
in one transaction per hour it copies the hour's `pack_ranges` union into `evicted_ranges`, deletes the packs, ranges, receipts, the hour's `claim_objects` rows **except survivors** (§8.4) and the scope row, and writes the tombstones;
then it deletes the hour's claim objects, survivors excepted, from the bucket;
`evicted_ranges` of a capture are coalesced.
The one retention decision therefore governs packs, receipts and claim objects alike, from the record's own `ts_utc_ns`, never from an object store clock.
Seq order and hour order differ after a clock step, so evicted seqs are a set of ranges, not a prefix (§7.3).
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
  the resolution of a parked message changes nothing about its object **inside** retention:
  staged or terminated while its hour is still retained, the object stays until §8.3 evicts the hour, like any other (no early sweep);
  a survivor is deleted only once that predicate holds, by the compactor's reconciler in a durable, retried step:
  mark the `claim_objects` row `deletable` under the capture row (the `parked` and `parked_copies` rows stay), delete the object from the bucket, remove the `claim_objects` row;
  an interrupted step resumes, never skips;
  `tracekeep admin object list --survivors` shows survivors with their evicted hour and bytes;
  an object whose `Metadata` lacks the SDK's header and time (a non-SDK producer) is **never deleted automatically**:
  `tracekeep admin object list --unattributed` shows it and the operator decides,
  which is the conservative disposition for a producer the design does not support.
- A **conflicting version** of a claim-check record (a producer fault, G5-200, G5-214) is kept beside the staged one for as long as the hour is retained:
  the deferred `claim-conflict` delivery can be resolved by the operator (§10.4) while both payloads exist.
  The only payload the service cannot keep is one a non-SDK producer overwrote under the same name before any consumer read it, a stated residual.
- The bucket has no age, size or count limit and the same TLS and at-rest protection as the stream ([STO §4] *Large records*).
