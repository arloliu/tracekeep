# tracekeep — ingest

Status: draft (2026-10-25) — split from seed v21

Covers message disposition (without parking), batching and placement, crash points and the registration transaction.
From seed v21 §6.1–§6.3; §6.1 *Parking* is in `parking.md`;
section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/bus` or `internal/ingest` (with `.agents/rules/600-invariants.md`).

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Order of checks** for every delivery:
  layout and length, retention candidate, resolution of the version against `parked` rows and termination orders, framing (`CheckBusMessage`), admission before any age decision, then waits on catalog state or a claim object (§6.1).
- **Resolution first**: a version already `staged` or `terminated`, or covered by an active termination order, is acknowledged with no admission side effect,
  before framing or admission can see it (§6.1, G5-269; the version is defined in `parking.md`).
- **Dispositions are per message** and are the rows of the §6.1 table or an operator's action;
  nothing is terminated after N deliveries, and a deferral holds only its own capture's messages (§6.1).
- **Batching**: per (capture, UTC hour of the record), sorted by seq at flush;
  equal copies deduplicated, conflicting copies kept in conflict segments;
  one new segment per flush, never reopened (§6.2).
- **Masking** happens per delivery, after classification and before batching, with the capture's classifier and policy (§6.2).
- **Acknowledgement**: each message individually, after the transaction that registered a segment holding its record commits;
  `DoubleAck` for a claim-check message (§6.2; `architecture.md` §3.1).
- **Registration transaction**: idempotent on `pack_id`, the same for `ingest` and the reconciler;
  its prologue updates the capture row, its guards abort it, and the `counters` update is its last statement (§6.3).
- **Crash points**: every crash leaves a state the §6.3 table recovers by redelivery or the reconciler.
- **Decision shape**: a pure disposition classifier decides from the facts gathered so far and returns a final disposition or the next step:
  evidence (framing, a claim object's fetch, descriptor validation) or a transactional operation (admission, rejection, parking)
  that re-reads its facts under the capture row and returns the actual outcome;
  a snapshot decision never authorizes an effect on its own,
  and the table is also a table-driven test beside the barrier schedules, never instead of them (§6.1).

## 6. Ingest protocol

### 6.1 Message disposition

Checks run in this order for every delivery:
**layout and length** (`Tpk-Layout` = 1 and a body of at least 44 bytes, else malformed) → **retention candidate**, from `ts_utc_ns` read directly at body offset 8 (signed little-endian, bytes 8–15) → **resolution before framing and admission**:
the delivery's **version** is read from the raw message,
before `CheckBusMessage` sees it (body bytes 0–43 are the carried header, the payload digest is `Tpk-Payload-Digest` for a claim-check message, present with `Tpk-Payload-Ref`, and SHA-256 over body bytes 44 onward otherwise;
`capture_id` and `seq` come from `Nats-Msg-Id`), and is looked up in `parked`,
and an active **termination order** of its capture and incarnation covering its stream sequence is looked up too;
a version `staged` is acknowledged as already staged, a version `terminated`, or one an order covers, is acknowledged under the termination (G5-269; [STO §4] allows it for a `start` and for a record without descriptor) with **no admission side effect**,
whatever else the delivery is (a `start`, a claim-check message whose object is missing, a descriptor the service rejects):
the capture's row is untouched, a later `start` of another version admits normally, a later `start` of the same version is acknowledged again under the termination;
`CheckBusMessage` and its `ErrBusNotAccepted` (a `start` whose descriptor does not decode) are reached only by an **unresolved** message,
so a terminated `start` with a missing or invalid descriptor is acknowledged, as [STO §4] allows, instead of deferred → framing (`CheckBusMessage`) → **admission before any age decision**:
a delivery that may be a `start` (an inline `start`, or any claim-check delivery of a capture that is unknown or `rejected`, whose object is fetched and checked first, G5-257) completes start recognition and descriptor validation now;
an accepted descriptor runs the **admission transaction**:
no row or `pending` → `open` (the prologue of §6.3 inserts the `pending` row if none exists and locks it; then `pending → open`, bind the processing configuration), `rejected` → `open` when a fixed consumer now accepts the retained descriptor bytes (re-validated against the delivery's), equal bytes on an already admitted capture → nothing, different bytes → `descriptor-conflict`;
a descriptor that is not accepted writes `captures.state = rejected` with its bytes **durably**, raises the alert,
and the `start` is deferred whatever its age, as G5-251 requires.
A capture with no row, or with a **`pending`** row (a placeholder without descriptor that the first consumer to park any of its messages inserts, *Parking* below), is **unknown** throughout this document;
admission and rejection update a `pending` row in place,
so from its first sighting every transaction of a capture serializes on one row.
A `pending` row has no descriptor, `tool_id` or `recorder_instance_id`:
it is **not a registered capture** for `ListCaptures`, for §7.3's unregistered-interval test or for its "`previous_capture_id` names no registered capture" rule,
and it appears only in `deferred list`, `parked list` and `tracekeep status`.
Only then is age applied:
an expired record of an **admitted** capture, the just-admitted `start` included, terminates as beyond-retention;
an expired record of a `rejected` capture is deferred whatever its age;
an expired non-start record of a capture the catalog does **not** know is **never terminated by age** (its `start` may be lost, pending, parked or rejected):
it is deferred and, being indefinite, parked (below), until its capture becomes known or an operator decides.
Only after this does any wait on catalog state or on a claim object begin → **admission** (a `start` of an unknown capture registers it;
a claim-check message of an unknown capture is fetched first,
because `CheckBusMessage` recognizes a claim-check `start` only in `CheckBusPayload`) → capture known → claim payload → processing configuration executable → classify → mask → batch.

**The disposition classifier.**
The order of checks and the table below are one decision procedure, kept apart from its effects.
A **disposition classifier**, a pure function that performs no I/O and holds no lock
(not the record classifier of the capture's processing configuration),
takes the facts gathered so far for one delivery and returns either a **final disposition**, a row of the table,
or the **next step** it needs:
more **evidence**, gathered without a durable effect (framing by `CheckBusMessage`, the fetch and check of a claim object, start recognition and descriptor validation),
or a **transactional operation**: the admission transaction, the durable write of `rejected`, step (1) of the parking protocol, or the termination of an unresolved version under an active order (`parking.md`).
The step runs, what it returns joins the facts, and the classifier decides again, until it returns a final disposition.
Before resolution the facts are only those read from the raw message (layout and length, `ts_utc_ns`, the version)
and the lookups of the version in `parked` and of an active termination order;
nothing calls `CheckBusMessage`, fetches an object or validates a descriptor before the classifier has found the version unresolved,
so a resolved or order-covered delivery reaches its disposition from the raw message alone.
A transactional operation starts with the prologue of §6.3, **re-reads under the capture row** the mutable facts its outcome depends on, and returns the **actual outcome**,
which may differ from the one the classifier expected from its snapshot.
Among those facts are always the delivery's version resolution and the capture's active termination orders:
admission, rejection and parking's step (1) take the delivery's identity (version key, bus incarnation, stream seq),
and a delivery terminated, or covered by an order, meanwhile returns `terminated` with nothing else written,
since termination and every ingest effect of a capture serialize on its row and whichever commits first wins (T-32).
The admission transaction returns `open`, nothing changed (equal bytes), `descriptor-conflict` or `terminated`;
the durable write of `rejected` returns `rejected`, `descriptor-conflict` on an admitted capture with other bytes, `already-admitted` with the bound processing configuration on one with equal bytes, after which the classifier decides again (T-32), or `terminated`;
parking's step (1) returns a version already resolved (acknowledged, nothing published),
a new row of an admitted capture whose hour is past the retention horizon (terminated as `beyond-retention`),
or a row in `parking` or `parked` to publish.
A **snapshot decision never authorizes an effect on its own**:
admission, rejection and parking take effect only as the outcome a transactional operation returns;
a final disposition is acted on directly only when what its facts prove cannot be undone
(a resolved version, a receipt, an admitted capture, a record past the horizon),
or when it is a nak, which changes nothing durable and leaves the redelivery to be decided again.
Admission is an **intermediate step**, never a final disposition:
its outcome changes the facts the age decision reads,
so an expired `start` that admission has just admitted terminates as beyond-retention, and one it rejected is deferred.

| Condition | Disposition | Visible as |
|---|---|---|
| `ErrBusMalformed` (unknown layout, body length, bad `Nats-Msg-Id`, bad claim headers) | terminate on the bus, alert, count | seq gap |
| `ErrBusNotAccepted` (a `start` whose descriptor does not decode or names an unserved method) | nak with delay, alert `capture-rejected`; later records of the capture stay deferred until an operator terminates or a fixed consumer accepts (G5-251) | open incident |
| `start` of an unknown or `rejected` capture, inline or claim-check (fetched first) | the admission transaction above, then normal ingestion; acked only when a segment holding the `start` is registered; an expired `start` of a now-admitted capture terminates as beyond-retention after admission | — |
| claim-check message of an unknown capture whose object is missing | nak with delay, alert; parked once its nak delay has reached the last step (T-40) | pending / parked count |
| other record of a capture the catalog has not registered | nak with delay (5 s, backing off to 60 s); never terminate | pending count |
| `start` of a registered capture (a redelivery or a duplicate) | descriptor bytes equal → **normal ingestion**: the record is batched and acked only when a segment holding it is registered; bytes differ → alert `descriptor-conflict`, nak; never overwrite | — / open incident |
| claim-check object missing or `ErrBusPayloadMismatch` | a **receipt** for (capture, seq) exists (§6.3) whose 44-byte header and payload digest equal this delivery's → ack as already staged; so does a `parked` row of this version (capture, seq, header, digest) in state `staged`, and one in state `terminated` acknowledges it under the termination (G5-269, *Parking*, resolution); a receipt with other bytes → the delivery is a conflicting version: nak with delay, alert `claim-conflict`, operator; no receipt and no resolved row → nak with delay, alert | pending count / open incident |
| expired record (`ts_utc_ns` older than the retention horizon) of an admitted capture | terminate, alert `beyond-retention`, audit row; intentional and visible | audit |
| expired record of a `rejected` or unknown capture | deferred (G5-251), never terminated by age; parked | parked count |
| `DeriveBusRecord` → `ErrInvalidRecord` for a kind outside the four | terminate, alert (a producer fault; the record cannot be stored) | seq gap |
| `DeriveBusRecord` → `ErrInvalidRecord` for a classifier result out of range; `CheckBusPayload` called for a non-claim record | internal invariant failure: nak with delay, alert, never terminate | open incident |
| processing configuration this consumer cannot execute (§6.4) | nak with delay, alert `incompatible-configuration` | pending count |
| `ErrCannotScreen`, or masking fails for the record | nak with delay, alert `cannot-screen`; counted per capture; other captures unaffected | open incident |
| a key the record's masking needs is refused (`keys.Refused`: a binding mismatch, a key id undeclared or of another kind, material malformed, short or missing at its pinned version, `masking-keys.md` §9) | deferred indefinitely, so parked; alert `key-binding-mismatch` or `key-refused`; the key's other records follow it | parked count, `deferred_messages{reason=key-refused}` |
| a key the record's masking needs is unavailable (the provider unreachable, its deadline passed, its credential refused) | nak with delay; the key resolves at a later delivery | `deferred_messages{reason=key-unavailable}` |
| write or put fails | no ack; the batch is retried by redelivery; the same batch failing the `Writer`'s validation three times is deterministic: alert `write-failure`, the batch's messages stay deferred, operator | open incident |

Nothing is auto-terminated after N deliveries; every terminal action is one of the rows above or an operator's.
A deferred delivery is therefore **bounded per capture, never per service**:
it holds its own capture's messages on the stream and nothing else.
`ingest` exports the age and count of deferred messages per reason (aggregate),
and `tracekeep status` and `deferred list` show them per capture;
the admin command of §10.4 lets an operator terminate a capture's deferred messages,
which is the operator action G5-251 names.

The table stays the normative list of dispositions.
It is also the fixture of a table-driven test of the disposition classifier, each row a case,
which **supplements, never replaces**, the barrier-controlled bus and database schedules (§7.4 *Tests*, `.agents/rules/300-testing.md`):
the cases prove the decision, the schedules prove the re-reads under the capture row.

### 6.2 Batching and placement

- A batch is per (capture, UTC hour of `ts_utc_ns`);
  the hour is the record's, never the delivery's ([STO §4] *Scope and order*).
- At flush (interval F, size threshold, or hour change) the batch is **sorted by seq**;
  equal seq with equal bytes keeps one.
- **Conflicts**: equal seq with different bytes are both kept,
  as [FMT I-12] and [STO §4] *Merge* require of every component:
  the first version goes into the batch's segment, each further version into a **conflict segment** of its own for the same scope,
  and a `conflicts` row names them;
  every message of a conflicting record is acked only when a segment holding its version is registered.
  Readers report the conflict; a merge of the scope fails until an operator resolves it ([STO §4] *Merge*).
  Conflicts that batching cannot see (two consumers, two batches, any hours) are found at registration:
  a new `pack_ranges` row that overlaps an existing range of the **same capture, in any hour**, marks the pair `suspect` in `conflicts`
  (redelivery after a crash produces such overlaps legitimately, so suspects are common after an incident and rare otherwise);
  `compact` compares the overlapping records' bytes **and redaction entries**, clearing the row for equal copies and confirming it for differing ones,
  each only while the row still names exactly the packs it compared (`catalog.md` §7.2 `RecordConflict`, T-32);
  a merge that fails on a conflict writes the row too.
  Two concurrent registrations of one capture cannot both miss the overlap:
  the registration transaction's first statement updates the capture's row (§6.3),
  which serializes registrations of one capture on MariaDB,
  and SQLite has one writer.
  `ListPacks` reports an unresolved suspect as a conflict until `compact` clears it.
  `ListPacks` and `Export` report a conflict for the window whenever any version of the seq lies in it,
  whichever hour the other version is in.
- Each flush writes **one new segment** per batch (plus conflict segments) with `Meta.SeqStart` = its first record's seq, `AssignSeq` and `DetectClockSteps` off;
  the producer did both ([SEM §4]).
  `Writer` accepts strictly increasing seqs with gaps, which a sorted batch is;
  a record delivered later with a lower seq starts the next segment, which [STO §4] allows and the merge normalizes.
  No segment is reopened.
- **Masking, per delivery** (§6.1): each record is classified from its payload as received (`DeriveBusRecord` with the capture's classifier),
  then masked with its capture's configuration (`redact.Compiled.Mask`, the keys of `masking-keys.md` §9 given for that delivery),
  `unobserved` being the bus record's quality bit 7, before it joins a batch;
  inline and claim-check records alike, so a batch, a working file and a pack hold masked records only.
  The delivery keeps the record as received beside the masked one:
  its version key, its receipt (header and payload digest) and the termination evidence of §6.1 are the original's,
  and a capture's configuration, and so its policy and keys, never changes, so equal originals mask to equal records and entries.
- The segment's metadata names the capture's `classifier` and `max_frame_len` (tracepack-go.md §3)
  and, for a masking configuration, its `redaction_policy` (policy id and version, digest key id and algorithm, encryption algorithm and key id)
  and its records' redaction entries in `PackMeta.Redaction`, ordered by seq and then by first masked offset, conflict segments' their own;
  a masking configuration writes `format_minor` 1 at least.
- Ack ownership: each message is acked individually after the transaction that registered a segment holding its record commits;
  a claim-check message with `DoubleAck`.

### 6.3 Crash points and the registration transaction

| Crash | State left | Recovery |
|---|---|---|
| before the put | nothing | redelivery rewrites the batch, byte-identical (G5-214) |
| after the put, before registration | orphan in `staging/` | reconciler registers it, coverage only, and marks it `adopted`, so an `ingest` registration still in flight for it leaves its batch unacknowledged; redelivery writes a second segment with the same records under its own pack id, which writes the receipts and `parked` states the adopted pack never gains (T-32); [FMT I-12] deduplicates |
| after registration, during the per-message acks | registered segment, some messages acked | the unacked remainder is redelivered and written again; duplicates are harmless |
| after a confirmed ack | nothing left to recover: the receipt, the `claim_objects` row and, for a parked record, the `staged` state of its `parked` row were written at registration; a crash between that commit and the acknowledgement redelivers a message whose row is resolved (§6.1 *resolution*) | — |

The registration transaction is idempotent on `pack_id` and runs the same way for `ingest` and for the reconciler.
Its **prologue** is one upsert that inserts a `pending` capture row when the catalog has none for the capture and otherwise updates the capture's row (`data_revision + 1`);
every transaction that admits (§6.1), parks, registers, deletes, tombstones or evicts a pack of a capture, merges a scope of it or publishes its close starts with that same update (§8),
so on MariaDB READ COMMITTED they serialize on that row before reading any predicate,
and on SQLite the single writer serializes them;
it then checks its **guards** and aborts on any of them:
a `tombstones` row for the key;
the pack's hour older than the retention horizon, or holding the capture's `evicted_ranges`, which a caller whose horizon was read before an eviction committed meets under the capture row
(the key is then tombstoned and the object deleted, so a put delayed across eviction never registers;
an hour evicted while it held no pack of the capture keeps no range, and what lands in it later is evicted again);
for `ingest`, which passes **delivery evidence** (each record's version key, bus incarnation and stream seq), a version of the batch found `terminated` or a message covered by an active termination order
(the key is then tombstoned in the same transaction and the object deleted after the commit; the batch is not acknowledged, and each message is classified again at its redelivery, T-32);
for the reconciler, the lease, and a seq of the pack with a `terminated` `parked` row or a capture with a termination order (tombstoned likewise, `storage.md` §5.4).
The reconciler and a close's publication pass no evidence, so their registration writes coverage only: no receipt and no `parked` state.
It then inserts the `packs` row, its `pack_ranges` (exact seq runs with the `ts_utc_ns` of each run's first and last record;
**the close sentinel 2^63 − 1 is never a range**), its `pack_losses` (coverage entries), any `conflicts` rows
(and, for every other scope a new `suspect` or confirmed conflict touches, `revision + 1` and a `conflict` event,
so a reader of that scope learns of it, §7.3),
and, only when the caller passed delivery evidence (T-32), a **receipt** row (`claim_records`, keyed by capture, seq, the 44-byte record header **and** the payload's SHA-256,
so two versions of one seq are two rows) for every record that travelled by claim-check;
update `captures`: `last_registered_at = now`, and `highest_seq`/`highest_seq_ts`
when the pack's highest **producer** seq (sentinel excluded) exceeds the stored one (the time of the highest seq, not the latest time);
a `stop` boundary in the pack sets `stop_seen` and moves `state` from `open` to `stopped` (a `closed` capture stays `closed`);
upsert the scope: **every registration increments `scopes.revision`** whatever the state;
a new scope is `open` and emits `created`; a `settled` scope becomes `reopened` and emits `reopened` (§7.3);
the `counters` update that allocates an event seq is the **last** statement,
so its row lock is held only across the commit.

"Byte-identical" is promised for records and redaction entries;
two segments of the same records differ in pack id, batching and writer time,
which is why [FMT I-12] deduplicates at record level.
