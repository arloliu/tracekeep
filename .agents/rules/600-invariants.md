# 600 — Invariants

Read before touching `producer/`, `internal/ingest/` or `internal/compact/`.
Each rule names the section that proves it; this file never argues, it points.
A change that would break a rule is a design change: amend the spec first, then the code.
Section numbers are those of the specs in `docs/specs/tracekeep/`, whose `README.md` maps each number to its file.

## The producer SDK

- **The producer's six guarantees** are the whole of what an integrator owes; everything else is the SDK's:
  the bus address, credentials and TLS in `Config` (or a connection through `WithConn`);
  every descriptor carrying the configured `toolID` and the configured device name;
  `secsobs`'s connection options and lifecycle attachment installed on the device's connection before it opens;
  `End` called on an orderly stop and never allowed to fail the device;
  `Rotate` called on every descriptor change;
  the send path: every forwarded W-bit primary registered before it is sent, its progress reported, and a `TimeoutSource` reporting the waiter's outcomes.
  The device's `Start`, `HotReload` and `Stop` map onto `Begin`, `Rotate` and `End` (`producer.md` §4.2 *Device verbs and SDK calls*).
  `producer-sdk.md` §4.7 *What the producer must guarantee*.
- **A callback never blocks on anything external**: not the bus, the claim bucket, a log or go-secs.
  Its one wait is for another callback to leave the capture's critical section,
  which holds no I/O, no allocation, no logging and no call into go-secs.
  `producer.md` §4.2 *Capture object*, §4.3; the residual wait is accepted in `tracekeep-decisions.md` T-9.
- **The commit point of a rotation** is the install of the new capture pointer, made while the old capture's lock is held.
  Every lock the rotation needs is taken before it, by a `TryLock` loop bounded by the operation's deadline;
  nothing is acquired after it, and a rotation abandoned before it installs nothing.
  `producer.md` §4.2 *Rotation* and *Lifecycle serialization*.
- **A forwarded request's token takes one terminal outcome**: the first of reply, timeout and cancellation seals the entry, and every later report of any kind is a counted no-op;
  expiry follows the wait the waiter reports opening and is no outcome,
  so T3 of a forwarded request comes only from the waiter's typed timeout on an unambiguous entry its outbound observation bound to a socket,
  and carries the epoch the capture current at the timeout holds for that socket.
  `producer.md` §4.6 *Send-path token protocol*, §4.4.

## Ingest and parking

- **Resolution before framing and admission**: a delivery's version is read from the raw message and looked up first;
  a `staged` version is acknowledged as already staged,
  a `terminated` one, or one an active termination order covers, is acknowledged under the termination,
  with no admission side effect, before `CheckBusMessage` or the admission transaction can see it.
  `ingest.md` §6.1 (the order of checks, G5-269).
- **States move forward only**, by compare-and-set:
  a parked version `parking → parked → staged | terminated`, with `evicted-hour` between `parked` and `terminated`;
  a copy row `waiting → acked`, or `lost` for a copy of a retired stream incarnation.
  `parking.md` (§6.1 *Parking*).
- **One parking coordinator owns the parking protocol's steps**, so no caller holds half of them:
  begin under the capture row, publish only for a version in `parking`, `parked` or `evicted-hour`,
  confirm only by a `PubAck` with `Duplicate` false or error 10071 with a strictly positive parsed sequence,
  advance by compare-and-set, then `DoubleAck` and mark that delivery's copy row `acked`;
  no catalog transaction is open across the publish or the acknowledgement.
  `parking.md` (§6.1 *The parking coordinator*).
- **Termination and every ingest effect of a capture serialize on the capture row; whichever commits first wins**:
  `Admit`, `Reject`, parking's step (1) and ingest's registration re-check under the row the delivery's version resolution and the capture's active termination orders,
  and a registration that finds one aborts and tombstones its key in the same transaction.
  `ingest.md` §6.1, `catalog.md` §7.2, `parking.md` (*Operator termination*), T-32.

## Catalog transactions

- **Lock order is capture row → `leases` row → `counters('membership')` row → `counters('scope_events')` row**, each step optional, and nothing else is locked;
  the `leases` row is taken only by a transaction the compactor commits under its lease, which every compactor transaction that changes domain state is;
  the `membership` row by admission and every membership change, renewals included.
  One unexported helper of the catalog runs the prologue, so every transaction that admits, parks, registers, deletes, tombstones or evicts a pack of a capture,
  merges a scope of it, publishes its close, or terminates, advances or acknowledges a parked version of it starts with the update of the capture's row by construction;
  where the transaction allocates an event seq, the `scope_events` update is its last statement, and parking touches no counter except a termination's event.
  `catalog.md` §7.2 *Domain transactions*, `ingest.md` §6.3 (the prologue), `compactor.md` §8.3, `rollouts.md` §6.4.

## Deletion

- **One deletion predicate**, for an hour's eviction and for survivor cleanup alike, evaluated under the capture row:
  the claim object of (capture, seq) is deletable only when every `parked` version row of that (capture, seq) is `staged`,
  or `terminated` with every known copy row of the current incarnation `acked`.
  `compactor.md` §8.4 *Objects without a receipt*.

## Keys

- **A key is used only after its binding check**: resolved at its binding's pinned version, never "latest",
  fingerprinted and compared before any derivation or use, and cached only after the check succeeds;
  each key id has one kind, and a request under another kind fails closed.
  `masking-keys.md` §9 *Order of a resolution* and *Key purposes stay distinct*.

## The bus

- **A traffic or parking stream is replaced, restored or rewound only by the stop-the-world procedure**:
  stop every tracekeep process, replace the stream, run `tracekeep admin bus adopt-stream <role>`, then start the processes.
  Never as a live operation;
  durable consumers are named per stream incarnation so that a stale handler's acknowledgement removes nothing.
  `parking.md` (incarnations), `tracekeep-decisions.md` T-12.
