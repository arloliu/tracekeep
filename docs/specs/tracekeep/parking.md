# tracekeep — parking

Status: draft (2026-10-25) — split from seed v21

Covers parking: why indefinite deferrals move to a parking stream, the copy, the parking protocol and its confirmation, resolution, the two tables, stream incarnations, operator termination and termination orders, the parked consumer, and the parking tables of the catalog.
From seed v21 §6.1 *Parking* and the parking rows of §7.1;
section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching parking, resolution, termination or stream replacement, in `internal/bus`, `internal/ingest` or the admin commands.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Why**: a nak-delayed message stays in the consumer's pending set,
  so indefinite deferrals would fill `max_ack_pending` and stop delivery for every capture;
  parking moves them to their own stream (§6.1 *Parking*).
- **The copy**: on the parking stream of the same cluster, body, `Tpk-*` headers and `Nats-Msg-Id` unchanged, on the version's own subject, with `Nats-Expected-Stream` removed.
- **Version**: `capture_id`, `seq`, the 44 record-header bytes as the body carries them, and one payload digest ([STO §4] *Termination*, G5-269).
- **Protocol order**: (1) under the capture row, re-read the capture's state and insert or find the `parked` and `parked_copies` rows;
  (2) publish the copy with expected last subject sequence 0;
  (3) CAS `parking → parked`;
  (4) `DoubleAck` the original and mark its copy row `acked`.
  A crash re-runs from (1).
- **Coordinator**: one parking coordinator owns those steps, so no caller holds half of them:
  begin under the capture row and return the version's state, publish only for a version in `parking`, `parked` or `evicted-hour`, confirm by the two outcomes below only, advance by CAS, `DoubleAck` then mark the copy row `acked`;
  no catalog transaction is open across the publish or the acknowledgement (§6.1 *The parking coordinator*).
- **Confirmation**: only a `PubAck` with `Duplicate` false, or error 10071 with a strictly positive stored sequence;
  anything else leaves the original unacknowledged and retried.
- **Resolution**: durable before any acknowledgement;
  a message of a `staged` version is acknowledged as already staged, one of a `terminated` version under the termination, neither needing the object or a receipt (G5-269).
- **Two tables**: `parked` is the version's resolution, immutable once resolved and kept for the capture's lifetime;
  `parked_copies` has one row per message, keyed by stream incarnation and stream sequence.
- **Forward-only states**, by CAS: `parking → parked → staged | terminated`, with `evicted-hour` between `parked` and `terminated`;
  copies `waiting → acked`, or `lost`.
- **Incarnations**: every durable consumer is named for its stream's incarnation;
  replacing, restoring or rewinding a stream is the stop-the-world procedure
  that ends in `tracekeep admin bus adopt-stream`, never a live operation.
- **Operator termination**: `parked terminate` and `capture terminate` write the catalog and hold no message;
  `capture terminate` also writes a termination order whose cut-off is the traffic stream's `LastSeq` on that incarnation;
  a termination emits a `terminated` scope event for each hour it touches (`catalog.md` §7.3).
- **Parked consumer**: never terminates a parked message for its age or delivery count;
  `parking.enabled = false` stops new moves only.
- **Locks**: every parking transaction starts with the catalog's capture-row helper, and so does termination, which every ingest effect of the capture re-checks under that row (T-32);
  parking touches no counter except a termination's scope event (T-33).

## 6.1 *Parking*

**Parking.**
A deferral that is indefinite by its nature (a `rejected` capture, a claim object still missing once its nak delay reached the last step, a `write-failure`, an unknown capture with an earlier message pending) would otherwise occupy the consumer's acknowledgement budget:
the server keeps a nak-delayed message in the consumer's pending set,
and when `max_ack_pending` is reached delivery stops for **every** capture,
so nak-with-delay alone cannot keep one bad capture from stalling the rest.
Such a message is therefore **parked**:
`ingest` publishes a copy to the **parking stream** of the same traffic cluster (`TRACEKEEP_PARKED`, work-queue, file storage, no age, size or count limit, the same TLS and at-rest protection),
body unchanged, the tracepack headers (`Tpk-*`) unchanged, **`Nats-Msg-Id` unchanged**
(`<capture_id>.<seq>` is the logical identity `CheckBusMessage` parses and the name a claim reference must equal;
the parking stream has its own duplicate window,
so the same id is legal there), on the **version's own subject** `<park prefix>.<tool token>.<capture_id>.<seq>.<version token>`
(a **version** is what [STO §4] *Termination* and G5-269 define:
`capture_id`, `seq`, the 44 record-header bytes **as the message's body carries them, before `DeriveBusRecord`**, and one payload digest, the `Tpk-Payload-Digest` value of a claim-check record or the SHA-256 of an inline payload;
the version token is the full lowercase hex, 64 characters, of SHA-256 over those 44 bytes followed by the 32 digest bytes,
so two versions of one seq never share a subject under the digest assumption the design already makes,
and a subject stays under 160 bytes),
and with exactly one transport header removed: `Nats-Expected-Stream`,
which the SDK's publish options stamp into the message and which the parking stream would reject;
`busconfig` defines the traffic prefix and the park prefix as two distinct first tokens,
and `Preflight` and `tracekeep migrate` refuse a configuration where either stream's subject filter could match the other's subjects,
since a different stream name alone separates nothing, and refuse a parking stream with fewer replicas than the traffic stream or with any age, size or count limit
(`tracekeep migrate` creates it with the traffic stream's replica count);
The **parking protocol** puts the catalog row before the copy, so the row, not the copy, is the protection point,
and **every branch serializes on the capture row**:
(1) a transaction whose first statement is the prologue of §6.3, which inserts a `pending` capture row when the catalog has none for the capture and otherwise updates the capture's row
(the discipline of §6.3 and §8.3, so admission, retention and parking of one capture serialize on both engines,
and two first sightings serialize on the primary key),
which then **re-reads the capture's state under that lock** and chooses its branch:
for an admitted capture (`open`, `stopped`, `closed`) a **new** row passes one guard, a record whose hour is older than the retention horizon is not parked but terminated as `beyond-retention` (the age row above),
since retention may already have evicted its hour;
for a `pending` or `rejected` capture no guard applies (G5-251: never terminated by age);
it then inserts-or-finds the `parked` row, keyed by the version (capture, seq, the carried header bytes, payload digest)
so a conflicting version is its own row, in state `parking`;
a row already **resolved** (`staged` or `terminated`) publishes nothing:
the original is acknowledged as already staged or under the termination (*resolution* below);
in every branch that parks or resolves a `parked` row the transaction also inserts-or-finds the original's row in **`parked_copies`** (the traffic stream's incarnation, its stream sequence from the delivery's metadata, the version key),
so every message of a version is accounted for;
a message an order covers whose seq a registered pack holds resolves as staged with no `parked` row, and the transaction writes nothing (T-40);
(2) for a row in `parking`, `parked` or `evicted-hour` state `ingest` publishes the copy on the version's subject with `WithExpectLastSequencePerSubject(0)`
(nats-server v2.15.1: standalone in `server/stream.go` `processJetStreamMsgWithBatch`, lines 6774–6800,
clustered in `server/jetstream_batching.go`, lines 766–822:
the expected value is compared with the last message stored under the subject,
and `0` is accepted exactly when the store finds none,
so after the sole copy is acknowledged and removed from the work-queue stream the subject is empty again).
**Confirmation** is one of exactly two outcomes:
a `PubAck` with `Duplicate` false (the copy is stored), or a wrong-last-sequence error that reports a stored last sequence
(nats-server error 10071 with a non-zero sequence:
the version's subject holds a message, which can only be a copy of this version;
nats.go v1.55.0 surfaces it as a wrapped `*jetstream.APIError`
whose `ErrorCode` is 10071 and whose `Description` carries the sequence as text, there is no structured field,
so the client parses a strictly positive integer from the pinned description format and treats zero, a malformed description or any other code as non-confirmation).
Nothing else confirms, and the original stays unacknowledged:
a `PubAck` with `Duplicate` true means the stream's duplicate window (server default two minutes, which `Preflight` reads from the stream and `busconfig` records) suppressed the publish
because a message with the same `Nats-Msg-Id`, another version of the seq included, was stored inside the window,
and it **stored nothing** (the duplicate check is by message id alone and returns before the store:
standalone in `server/stream.go` lines 7017–7036, clustered in `server/jetstream_batching.go` lines 607–629, whose `Duplicate` reply `server/jetstream_cluster.go` builds at lines 13190–13198).
The two checks run in opposite orders:
a standalone stream checks the expected last sequence before the duplicate window, a clustered stream the duplicate window first
(nats-server v2.14.5 behaves the same);
either order leads to one of the outcomes above and the protocol converges on both;
the original is then nak'd with a delay longer than the window and the publish is repeated
(the server purges duplicate entries on a timer, so a repeat may meet one more `Duplicate` and wait again;
with a working bus the repeats converge,
and a `Duplicate` that refers to this version's own earlier stored copy, a lost `PubAck` for instance, merely repeats
until the stored copy is confirmed by the 10071 path or resolved);
the clustered in-flight rejection (error 10164, which carries no sequence) and a timeout are retried the same way.
nats.go v1.55.0 exports 10164 as `JSErrCodeStreamWrongLastSequenceConstant`, documented as the replicated-stream equivalent of 10071;
nats-server v2.15.1 sends it only for in-flight conditions and still sends 10071 with the sequence for a real mismatch,
so publish errors are classified by `ErrorCode` and the parsed description as above, never through the key-value package's sentinels.
(3) CAS the row `parking → parked`.
(4) acknowledge the original with `DoubleAck` and mark its copy row `acked`.
A crash at any point redelivers the never-acknowledged original, which re-runs the protocol from (1).
A **second copy** of a version can exist only after the first was acknowledged
(the per-subject expectation refuses it while the first exists;
a handler stalled past the ack wait publishes late only into an empty subject),
so it is always a copy of a **resolved** version.
**Resolution is durable before any acknowledgement**:
the registration transaction that stages a parked record sets its `parked` row to `staged` in the same transaction as the receipt (§6.3)
and the copy is acknowledged only after that commit;
`parked terminate` and `capture terminate` write the row `terminated`, the declared loss and the audit row first, then acknowledge the copies;
a crash between either commit and the acknowledgement redelivers a copy whose row is already resolved.

**The parking coordinator.**
One coordinator owns the steps of the parking protocol, so no caller holds half of them.
It adds no step and changes no outcome:
each of its operations is a step above, whose text and proofs stay where they are.
`ingest` enters at *begin* with an original;
the parked consumer enters at *advance* with the copy it holds (a crash between (2) and (3), below) and at *acknowledge* for every acknowledgement of a copy.

- **Begin** is the whole of (1), under the capture row:
  the `pending` row when the catalog has none, the re-read of the capture's state, the branch it chooses (the horizon guard included),
  and the insert-or-find of the version's `parked` row and the delivery's `parked_copies` row;
  it returns the version's current state.
  A resolved state publishes nothing, and the message is acknowledged under *resolution* below, through *acknowledge*.
- **Publish** is (2), for a version in `parking`, `parked` or `evicted-hour` only (T-32).
  An `evicted-hour` version is unresolved, so its original is acknowledged only after a confirmed copy:
  the publish meets 10071 when the earlier copy is still on the parking stream, or stores a fresh one when that copy was `lost` to a replaced stream,
  and *advance* is then a no-op.
- **Confirm** is *Confirmation* above:
  a `PubAck` with `Duplicate` false, or error 10071 whose description parses to a strictly positive sequence, and nothing else;
  a `PubAck` with `Duplicate` true, error 10164, a timeout, a zero or malformed description and any other code leave the original unacknowledged and retried as (2) says.
- **Advance** is (3), a compare-and-set that moves states forward only (below),
  so a handler that wakes after the version moved past `parking` changes nothing.
- **Acknowledge** is (4) for whichever delivery the caller holds:
  `DoubleAck`, then that delivery's copy row marked `acked` once the server confirmed the acknowledgement.

No catalog transaction is open across the publish or the acknowledgement:
*begin* commits before the publish, since the committed row, not the copy, is the protection point,
and *advance*, the `acked` update and the parked consumer's insert-or-find of a copy row are each a transaction of their own,
each starting with the catalog's capture-row helper (`catalog.md` §7.2, T-32), as every parking transaction does.
A crash before an acknowledgement redelivers the message to its consumer, which runs its steps again (an original from *begin*, as above);
an acknowledgement the server did not confirm leaves the message to be redelivered and acknowledged again (*resolution* below);
an acknowledgement the server confirmed whose catalog update was lost leaves the copy row `waiting` for a message the work-queue stream removed,
which the reconciler's per-sequence check marks `acked` once the version is resolved (below), never before.

**Two tables, two lifetimes.**
The `parked` row is the **version's resolution**, immutable once `staged` or `terminated` (a termination records the operator action's id in `terminated_by`) and kept for the capture's lifetime (§7.1).
Every **message** of a version, the original on the traffic stream and each copy on the parking stream, has its own row in `parked_copies`, keyed by (**stream incarnation**, stream sequence) and carrying the version key and a state `waiting → acked`,
where `acked` means the server confirmed the acknowledgement (`DoubleAck`, [STO §4] *Durability*);
the parked consumer inserts-or-finds the row of the copy it holds (`msg.Metadata().Sequence.Stream`) before disposing of it,
so a redelivery finds its own row.
A stream **incarnation** is a catalog row of `bus_streams` (role, stream name, `StreamInfo.Created`, an incarnation id the catalog allocates, the highest `LastSeq` observed), created by `tracekeep migrate`
when it creates the stream.
**Every durable consumer of the service is named for the incarnation** (`tk-ingest-<incarnation id>`, `tk-parked-<incarnation id>`),
so an acknowledgement, a nak or a pull from a handler of a retired incarnation names a consumer the replacement stream does not have (adoption deletes any it inherited, below) and is dropped by the server (acknowledgements address `$JS.ACK.<stream>.<consumer>…`, nats-server `consumer.go`),
whatever sequence it carries;
this, not detection, is the fence against a stale acknowledgement removing new traffic.
`ingest`, the parked consumer and the reconciler verify at start and with every lease renewal (§6.4)
that `StreamInfo.Created` equals the row's and that `State.LastSeq` is not below the highest observed
(a snapshot restore comes back under a new `Created`, nats-server v2.15.1, and a restore of an older snapshot also rewinds sequences; either mismatch is a changed incarnation, T-40);
on either mismatch they stop fetching and publishing with the alert `bus-incarnation-changed`,
and the deliveries they hold are abandoned unacknowledged.
**Replacing, restoring or rewinding a traffic or parking stream is a maintenance procedure, never a live operation**:
stop every tracekeep process, replace the stream, run `tracekeep admin bus adopt-stream <role>`,
which allocates a new incarnation id, **deletes every `tk-*` consumer the stream still has**
(a snapshot restore brings the retired incarnation's durable consumers back with it, nats-server `stream.go` restore) and verifies none remains, marks the retired incarnation's copy rows `lost` (their messages cannot return) and its termination orders `spent`, then start the processes,
which validate the catalog's incarnation against the stream **before** creating their consumers under the new name;
`Preflight` refuses to run against a stream whose `Created` or `LastSeq` contradicts `bus_streams` until that command has run,
and `Preflight` and `tracekeep migrate` refuse a stream with a mirror or sources (its sequences are not the bus's own) or with `AllowDirect` (so every `GetMsg` is served by the stream leader, below).
**Resolution** (G5-269, spec v2.36): a message whose version is `staged` is acknowledged as already staged ([STO §4] *Already staged*);
a message whose version is `terminated` is acknowledged **under the termination** ([STO §4] *Termination*):
before or after the operator's act, original or copy, it is never written, an `audit_log` row records the acknowledgement with the action id, and its copy row becomes `acked`;
neither needs the object or a receipt.
A redelivered message whose copy row already says `acked` (an acknowledgement the server confirmed but whose catalog update was lost, or one that was not confirmed) is acknowledged again under the same rule,
since its version is resolved.
**Operator termination** writes the catalog and holds no message:
`parked terminate <capture> [seq]` and `capture terminate <id>` run one transaction, starting with the capture-row helper,
that writes every named unresolved version `terminated` with the declared loss and the audit row of the action, and, for `capture terminate`, a **termination order** (capture, the traffic stream's incarnation, cut-off = that stream's `LastSeq` read by a fresh `Info` inside the action, action id, state `active`/`spent`)
that covers the capture's deferred originals that were never parked:
a consumer that later receives a message of that capture **on that incarnation** with a stream sequence at or below the cut-off and an unresolved version inserts the version `terminated` under the order's action id, with its copy row, the declared loss and the audit row, and acknowledges it
(a message whose version is already `staged`, or whose seq a registered pack holds, is acknowledged as already staged, the order never undoes a resolution or a registration, T-40);
that insertion is a transaction that starts with the prologue of §6.3 and re-reads the version under the capture row,
so a version staged meanwhile is acknowledged as already staged;
a message above the cut-off, or on another incarnation, is new traffic (a partitioned producer, [STO §4], or a replaced stream) and is handled normally,
since G5-269 covers versions the operator decided about, never a version the operator has not seen;
a publish accepted after the `Info` carries a sequence above the cut-off by construction, so the order's edge is exact;
the `Info` is read before the transaction opens, so no network call happens under the lock.
Termination and every ingest effect of the capture serialize on the capture row, and whichever commits first wins (T-32):
`Admit`, `Reject`, *begin* and ingest's registration re-check under the row the delivery's version resolution and the capture's active orders (`catalog.md` §7.2),
so a delivery that loses to a termination makes no admission, parking or staging effect,
and a batch registered first is staged and its messages are acknowledged as already staged.
An order is `spent` when the stream's `FirstSeq` has passed its cut-off (no covered message can exist) or when its incarnation is retired;
`capture show <id>` lists a capture's orders with their state.
The acknowledgements themselves happen at the next delivery of each message, by whichever consumer holds it:
a parked copy returns within `parking.redelivery_delay` (default 60 s, the parked consumer's nak delay)
when that consumer is up;
an original on the traffic stream returns when its own nak delay elapses,
which after a `Duplicate` may be the duplicate window;
`parked list` shows the versions' states and which of their messages are still `waiting`,
which is the completion signal, not a time bound.
`parking.enabled = false` changes none of this.
`acked_at` on a version row is **metadata, never a protection predicate**:
set when the last known copy row became `acked`.
The reconciler marks a copy row `acked` only by its **own** identity:
for a resolved version it first refreshes **the very handle it will read with** (`Stream.Info`, which updates the handle's cached configuration, nats.go v1.55.0 `jetstream/stream.go`), checks on that answer the incarnation (`Created`, `LastSeq` not rewound) and `Config.AllowDirect == false`, and only then asks that handle for the copy's stream sequence with `Stream.GetMsg(seq)`,
which is then the leader-served `$JS.API.STREAM.MSG.GET`
(`GetMsg` picks the direct route from the handle's cached `AllowDirect`,
so a stale handle could read a replica that has not yet received the sequence);
only the API's not-found answer for a sequence **at or below** that `LastSeq` means the message was acknowledged on a work-queue stream
(the last message of a stream is removed like any other, so the acknowledged tail copy sits exactly at `LastSeq`);
a timeout, any other error, an incarnation mismatch or a sequence of a retired incarnation advances nothing;
it never infers anything from a subject count and never touches a copy of an unresolved version.
Version states move **forward only**, by compare-and-set:
`parking → parked → staged | terminated`, with `evicted-hour` between `parked` and `terminated` for a version
whose hour retention evicted (it can no longer be staged),
so a handler that wakes after the version was resolved cannot regress the row;
copy states likewise `waiting → acked` (`lost` for a copy of a retired incarnation).
The parked consumer advances a row it holds a copy of from `parking` to `parked` (a crash between (2) and (3)).
Two transactions move a version `parked → evicted-hour`, each by compare-and-set under the capture-row helper (T-32):
`EvictHour`, for the `parked` rows of the hour it evicts, which it already reads under the row to choose survivors (§8.4);
and the parked consumer, when the horizon guard refuses the registration of a version it holds a copy of, after advancing a `parking` row.
Retention, admission and parking therefore have mutually exclusive durable outcomes on the capture row:
an **unresolved** row (state neither `staged` nor `terminated`) committed first is seen by retention,
which keeps the object (§8.4 *survivors*);
an eviction committed first makes the horizon guard terminate the delivery instead;
an admission committed first is seen by the re-read, so the branch is chosen against the admitted state.
Parking takes the capture row and no other lock, and touches no counter except a termination's scope event (`catalog.md` §7.2, which holds the full lock order).
The **parked consumer** is a second pull consumer of `ingest` on the parking stream, with its own acknowledgement budget,
that runs the same disposition and staging protocol with long nak delays;
nothing is ever republished to the traffic stream, so duplicate windows and replay ordering never arise:
a parked message is staged from the parking stream
when its blocker clears (a rejected capture accepted by a fixed consumer in a `rejected → open` transaction that re-validates the descriptor, a claim object restored, a start finally admitted), or terminated by the operator,
and its seq never reads as a gap while it is parked: it reads as deferred, and a termination makes it a declared loss (`catalog.md` §7.3, T-33).
The parked consumer applies the rows above with the one exception G5-267 makes:
a parked message is **never terminated for its age or its delivery count**.
A parked message whose record hour retention evicted meanwhile is therefore neither staged (a registration into an evicted hour is refused by §6.3's horizon guard) nor age-terminated:
its row moves to `evicted-hour`, `parked list` shows it, its object is a survivor (§8.4), and only `parked terminate` or `capture terminate` ends it, as a declared loss with an audit row.
Parking is what makes deferral per capture and never per service.
G5-267 (2026-10-25, [STO §4] *Deferral*, spec v2.35) rules
that a message so moved is deferred on the bus in the sense of G5-251 and G5-257, under conditions the parking stream meets by construction and `Preflight` checks:
the same cluster, removal only by acknowledgement, no age, size or count limit, file storage with no fewer replicas than the traffic stream, *Clear-text protection*;
body, `Nats-Msg-Id` and every header [STO §4] defines unchanged (subject and `Nats-Expected-Stream` are the deployment's);
the copy confirmed before the original is acknowledged;
never republished to the traffic stream;
handled by the traffic stream's rules;
acknowledged only once its record is durable in `staging/` or an operator terminates it.
Parking is therefore the **default**; `parking.enabled = false` stops new moves only:
the parked consumer keeps consuming, and `parked list`/`parked terminate` keep working,
so messages already on the parking stream are never orphaned by the flag;
the cost of the flag is the documented service-wide stall when the main consumer's budget fills.

## 7.1 Tables: the parking rows

The other tables of §7.1 are in [catalog.md](catalog.md).

| Table | Holds | Key facts |
|---|---|---|
| `parked` | key = the version (capture, seq, the carried 44 header bytes, payload digest), one row per version; `terminated_by` action id; reason, `state` `parking`/`parked`/`evicted-hour`/`staged`/`terminated`, record hour, object key, parked at, resolved at, `acked_at` (metadata: when the last known copy was acknowledged; never a protection predicate) | the parking protocol of §6.1, inserted under the capture row; the protection point of §8.4; the durable resolution a later copy is acknowledged from, so **never removed by retention**: kept for the capture's lifetime in the catalog |
| `parked_copies` | key (incarnation id, stream sequence) → version key; `state` `waiting`/`acked`/`lost` | one row per message of a version (§6.1 *resolution*); `acked` only after a server-confirmed acknowledgement or the reconciler's leader-served per-sequence check; `lost` set by `bus adopt-stream`; kept with the version |
| `bus_streams` | incarnation id, role `traffic`/`parking`, stream name, `StreamInfo.Created`, highest `LastSeq` observed, `retired_at` | consumers are named for the incarnation id and verify `Created` and `LastSeq` with every lease renewal; a new row only by `bus adopt-stream` after a stop-the-world replacement (§6.1) |
| `termination_orders` | capture, incarnation id, cut-off, action id, `state` `active`/`spent` | §6.1 *operator termination*; `spent` when `FirstSeq` passes the cut-off or the incarnation retires |
