# 600 — Invariants

Read before touching `producer/`, `internal/ingest/` or `internal/compact/`.
Each rule names the section that proves it; this file never argues, it points.
A change that would break a rule is a design change: amend the spec first, then the code.
Section numbers are those of the specs in `docs/specs/tracekeep/`, whose `README.md` maps each number to its file.

## The producer SDK

- **The producer's five guarantees** are the whole of what an integrator owes; everything else is the SDK's:
  the bus address, credentials and TLS in `Config` (or a connection through `WithConn`);
  every descriptor carrying the configured `toolID` and the configured device name;
  `secsobs`'s connection options and lifecycle attachment installed on the device's connection before it opens;
  `End` called on an orderly stop and never allowed to fail the device;
  `Rotate` called on every descriptor change.
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

## Catalog transactions

- **Lock order is capture row → `counters` row**, and nothing else is locked.
  Every transaction that admits, parks, registers, deletes, tombstones or evicts a pack of a capture,
  merges a scope of it or publishes its close starts with the update of the capture's row;
  the `counters` update is the last statement.
  `ingest.md` §6.3 (the prologue), `compactor.md` §8.3.

## Deletion

- **One deletion predicate**, for an hour's eviction and for survivor cleanup alike, evaluated under the capture row:
  the claim object of (capture, seq) is deletable only when every `parked` version row of that (capture, seq) is `staged`,
  or `terminated` with every known copy row of the current incarnation `acked`.
  `compactor.md` §8.4 *Objects without a receipt*.

## The bus

- **A traffic or parking stream is replaced, restored or rewound only by the stop-the-world procedure**:
  stop every tracekeep process, replace the stream, run `tracekeep admin bus adopt-stream <role>`, then start the processes.
  Never as a live operation;
  durable consumers are named per stream incarnation so that a stale handler's acknowledgement removes nothing.
  `parking.md` (incarnations), `tracekeep-decisions.md` T-12.
