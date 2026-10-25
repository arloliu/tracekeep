# P1 — module interfaces

Status: active (2026-10-25) — accepted (T-26); each item applied as its own change

Covers six changes to how tracekeep's modules present their interfaces, from a deep-module review of seed v21 and its external review (both kept outside the repository).
Read before designing the catalog, ingest's disposition, the producer SDK's API, the eqp-hub send hook, `ObjectStore` or `internal/keys`.
Nothing here reopens a decision in [tracekeep-decisions.md](../tracekeep-decisions.md) or a tracepack ruling;
accepted items amend the spec files they name.

## Principle

A module is deep when a lot of behaviour sits behind a small interface,
where the interface is everything a caller must know: operations, invariants, ordering, error modes.
The specs prove their invariants well but leave several interfaces unstated,
so an invariant such as the capture-row lock order is restated wherever a caller might break it.
Each change below moves such knowledge behind one interface, and keeps every mechanism and proof of the specs unchanged.

## 1. Catalog: domain transactions with their contracts

The specs name the operations (`catalog.md` §7.2) and the lock order (`ingest.md` §6.3, `compactor.md` §8.3) but fix no callable contract,
so a statement-level catalog remains possible.

Proposal: the catalog's interface is a set of domain transactions;
one unexported helper runs the prologue (insert-ignore a `pending` capture row, update the capture row),
so the lock order holds by construction.
Each transaction's contract states its preconditions, guard reads, affected-row checks, durable outcome and retry behaviour:

- the `counters` update is the last statement **only** of transactions that allocate an event seq;
  parking touches no counter (`parking.md`);
- a failed scope-revision or lease check rolls back the whole transaction (`compactor.md` §8.1);
- registration names its execution context: `ingest`, or the reconciler under the compactor lease (`storage.md` §5.4);
- a tombstone commits before its object is deleted (`compactor.md` §8.3);
- whole-transaction retries, SQLite's one write connection with `BEGIN IMMEDIATE`, and second-writer refusal stay inside the module (`catalog.md` §7.4).

The read side returns domain results:
`ListPacks` returns completeness and the cursor watermark it read before the listing, and owns gap, evicted and unknown-tail classes and cursor recovery (`catalog.md` §7.3).
The barrier schedules run against this interface on both engines.

## 2. Disposition: a staged classifier with transactional steps

The order of checks and the disposition table (`ingest.md` §6.1) are a decision procedure interleaved with effects.

Proposal: a pure classifier decides from the facts gathered so far and returns either a final disposition or the next step it needs:
more evidence (an object fetch, a descriptor check) or a transactional operation (admit, park).
Transactional operations re-read mutable facts under the capture row and return the actual outcome;
a snapshot decision never authorizes an effect on its own.
Gathering facts never calls `CheckBusMessage`, fetches an object or validates a descriptor before resolution;
admission is an intermediate step that changes the facts for the age decision.
The table becomes a table-driven test that supplements, never replaces, the bus and database schedules.

The deletion protection predicate of `compactor.md` §8.4 becomes one function, called inside both deletion transactions, with the current incarnation as an explicit input;
it keeps the asymmetry (`staged` needs no copy-acknowledgement gate, `terminated` does) and does not by itself authorize deletion:
an admitted capture, an evicted hour and attributable metadata remain prerequisites.

## 3. Parking: a coordinator over catalog transactions and the bus

The protocol (`parking.md`) already separates database and bus effects.
Proposal: a parking coordinator owns the steps, so no caller holds half of them:

1. begin parking under the capture row:
   insert or find the version row and the original's copy row, and return the current resolution;
2. publish only when the version is unresolved;
3. accept exactly the two confirmations (a `PubAck` with `Duplicate` false, or error 10071 with a strictly positive parsed sequence);
   `Duplicate` true, 10164, a timeout or a malformed description do not confirm;
4. advance by CAS without regressing a resolved version;
5. `DoubleAck` that delivery, then durably mark that copy row `acked`.

No database transaction spans the network acknowledgement;
a crash between the acknowledgement and the catalog update stays recoverable by redelivery or the per-sequence reconciliation.

## 4. Producer SDK: one vocabulary, observer options, a narrow injection contract

- **Lifecycle**: `producer.md` describes the SDK under the device's verbs (`Start`, `Stop`, `HotReload`)
  and `producer-sdk.md` §4.7 sketches `Begin`, `Rotate`, `End`.
  Choose one SDK vocabulary,
  or document the mapping once, keeping an idempotent start, a deadline-bounded stop that cannot fail the device, and rotation on every descriptor change;
  update the five guarantees and `.agents/rules/600-invariants.md` with it.
  The owner chose `Begin`, `Rotate` and `End` for the SDK (T-26);
  `producer.md` maps the device's `Start`, `Stop` and `HotReload` onto them once.
- **Observers**: the integrator receives the SDK's observer options instead of calling `Observe`.
  With go-secs v2.6+ the wire, socket and transaction observers are `hsms.ConnOption`s, passed for HSMS-SS through `hsmsss.WithConnectionOption`,
  and lifecycle events need a separate `SubscribeLifecycle`.
  Specify the return type, a valid construction, and an explicit lifecycle attach and cancel, installed before traffic starts.
  The adapter feeds the capture algorithm directly:
  no queue before seq allocation, no retained frame, no lifecycle or network work in a callback.
- **Injection**: keep `WithBus`, an accepted escape hatch, with an exported **narrow** contract
  whose methods a connection-free fake can implement (maximum payload, reconnect-buffer and TLS checks, publish, object store, stream information);
  NATS details stay private.
  `WithConn` keeps its verification and the SDK's publication policy applies on every route.
  Hiding `Bus` would not remove nats.go from the public interface
  while `WithConn(*nats.Conn)` exists, nor change the module's version floor.

## 5. One send-path protocol into eqp-hub

`producer.md` §4.6 registers every forwarded W-bit primary through `Pending()`,
and §4.4 adds `TimeoutSource` for the waiter's T3.
Proposal: one token protocol serves both sites (the forwarding call and the waiter's timer branch are different places in eqp-hub).
It distinguishes send progress (sent, send failed, submission failed) from terminal waiter outcomes (reply, timeout with the effective timeout of that wait, cancellation), with idempotency and ordering stated for competing notifications.
It keeps every rule of the association protocol:
registration before forwarding, the process-lifetime ambiguity set and its cap, socket binding from the outbound observation, provisional reply attachment, expiry, the record's own capture declaration, and T3 only from the waiter's typed timeout.

## 6. Storage downloads and key resolution

- **Downloads**: `PackURL` and export bundles need presigned URLs (`api.md` §10.1),
  which `ObjectStore` (`storage.md` §5.2) cannot produce and `fs` and `mem` cannot offer, standalone pilots included.
  Define an optional presigning capability with an API-owned download fallback
  that keeps authorization, expiry, range reads and compatibility with the deletion delay D.
- **Keys**: one `keys` module owns HKDF, fingerprints, the atomic insert-or-compare and the cache (`masking-keys.md` §9);
  providers resolve material by an immutable key-id-to-provider-reference-and-version mapping, or by a binding-aware request,
  so a bound key is retrieved at its pinned version, never as "latest".
  The binding check completes before derivation or use, and a cache entry is published only after it.
  Distinct key purposes, the session-cookie key of `api.md` §10.2 included, stay distinct.

## Also noted

- §6.4's activation gate (`rollouts.md`) deserves the same treatment:
  one membership-and-fetch module
  that owns lease renewal, pull limits, generation checks and configuration selection, keeping retirement's traffic-first observation.
- `busconfig` could own validation and reporting beside the desired configuration;
  the compactor's resumable operations need stated ownership and post-lease-loss behaviour;
  the API's use-case operations could centralize role checks and auditing.

## Applying it

Applied so far: item 4.

Each accepted item is applied as its own change to the spec files it names, with their interface sections and `.agents/rules/600-invariants.md` updated in the same commit.
A spec's mechanism and proofs win over this proposal wherever the two disagree.
