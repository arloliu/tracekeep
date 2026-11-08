# Roadmap

Status: current (2026-11-06) — milestones from seed v21 §12

Where tracekeep's work stands, and the single index of active plans (`.agents/rules/450-doc-lifecycle.md`).
The milestones and their gates are seed v21 §12, moved unchanged;
section numbers are the seed's, and `docs/specs/tracekeep/README.md` maps each to its file.

## Milestones

| Phase | Scope | Gate |
|---|---|---|
| 0 — producer, **in the tracekeep repository only** | the SDK (`producer`, `secsobs`, `busconfig`, `Preflight`, `producertest`) with §4.2's algorithm and its lifecycle lock; `cmd/tracekeep-sim`, which drives the SDK exactly as eqp-hub will, from a go-secs loopback pair with the real observers (true `raw-stream` frames, real epochs and T3) and from replayed tracepack packs (volume and shape; synthetic or masked data only); `internal/testbus` (embedded nats-server with JetStream); HSMS-SS only; claim-check puts; the `TimeoutSource` interface eqp-hub's waiter will implement later; **thinning disabled**; metrics; no cross-repository dependency | chaos test: traffic bus down and up while a tool runs, no tool-side latency change, drops visible as gaps; barrier tests for the lock algorithm (pause between unlock and enqueue of a step, opposing clock steps, A→B→C rotations between callback steps, `Stop` after pointer load, reserved lane full while disconnected, paused lock holder during `Stop`, a HotReload blocked on a capture lock when `Stop` arrives, a fault signal after `Stop`, a fault rotation racing a HotReload, resumption of an abandoned rotation after a later `Start`); duplicate `Start`; `Stop` with a stalled publisher; frame reuse after the callback; synchronous `start` with the production option set; a publish during a disconnect fails immediately; lane-full drops carry seqs (the next record's seq shows the hole); a multi-chunk claim object at `max_payload` 4140; an uncertain upload with purged chunks is read back and re-put, never referenced; `Preflight` names every misconfiguration of the chaos environment; SDK against the minimum and the latest resolved module graph |
| 1 — ingest | `ingest` with §6 (disposition, admission, parking, batching, conflicts, registration, per-message acks, claim-check fetch), `s3` and `fs` stores with the conformance suite run against MinIO and Ceph RGW containers (T-34), catalog on SQLite and MariaDB, Atlas CLI migrations, reconciler with full verification; processing configuration bound even without a policy; `tracekeep dev` as the single-process loop (embedded bus, `gofakes3` or `fs`, SQLite, sim producer); synthetic or non-production traffic only | a capture from `cmd/tracekeep-sim`'s go-secs loopback mode readable with the tracepack CLI (the pilot gateway's capture is phase G's gate); the crash-point matrix of §6.3 including partial acks; redelivery yields byte-identical records; conflict kept in two segments; the two-pack completeness counterexamples; a duplicate `start` after a crash before the put is staged, not lost; an initial claim-check `start` at `max_payload` 4140; two claim digests at one seq; a beyond-retention delivery terminates with an audit row; parking of an unknown capture creates a `pending` row that its later `start` admits in place; a handler stalled past the ack wait publishes a second copy only after the first was acknowledged, and that copy is acknowledged from the row without the object; against `internal/testbus`, three outcomes told apart: a publish with expected last subject sequence 0 is refused with error 10071 and a non-zero sequence while the subject holds a message, returns `Duplicate` true and stores nothing when the same `Nats-Msg-Id` was stored inside the window (another version's subject included), and stores a copy otherwise; two versions of one seq parked inside one duplicate window both end stored or unacknowledged, inline and claim-check; a crash after the staging commit and before the copy's acknowledgement, between the `PubAck` and the CAS, and between the CAS and the original's acknowledgement; two consumers' first sightings of one capture; `rejected → open` with a parked copy, then staged; a termination committed, each of its messages acknowledged under it at its next delivery, a crash before an acknowledgement and the redelivery acknowledged again, a copy of the version published a day later acknowledged under the same termination with its own audit row; a never-parked deferred original ended by a termination order at or below the cut-off, and one above the cut-off handled as new traffic; an inline acknowledgement confirmed by `DoubleAck` before the copy row says `acked`; a stale handler's late nak racing the acknowledgement of the same original; the reconciler marking a copy `acked` only when its own sequence is gone, never from a subject count; a traffic or parking stream deleted and recreated while a handler holds an old delivery: the handler's late acknowledgement names the retired incarnation's consumer and removes nothing on the new stream, fetching stops with `bus-incarnation-changed`, `bus adopt-stream` refuses while a lease is live and then marks the old copies `lost` and the orders `spent`, a reused sequence inherits nothing and an old order covers nothing on the new incarnation; a snapshot restore with equal `Created` and a lower `LastSeq` is detected as a rewind; a restore that brings `tk-ingest-<old id>` back is cleaned by `bus adopt-stream` and the new consumer is created without a work-queue overlap error; the sole parked copy acknowledged at exactly `LastSeq`, crash before the catalog update, no further publish, reconciled to `acked` after a restart; a reconciler handle created while `AllowDirect` was on refreshes itself and reads through the API (asserted on the request subject); a terminated inline `start` with an invalid descriptor acknowledged before `bus.CheckMessage` would have rejected it; the reconciler's `GetMsg` against a replica lagging behind the leader does not advance a row (clustered testbus, `AllowDirect` refused by `Preflight`); a terminated `pending` `start`, inline and claim-check with its object missing, acknowledged under the termination with no admission side effect, then a different-version `start` admitting and a same-version `start` acknowledged again; `parking.enabled` turned off with waiting and evicted-hour messages on the parking stream, which stay listable and terminable; the three-outcome testbus test asserts storage, not only the response, on the standalone check order and on the clustered one |
| 2 — compact and machine API | §8 merge, close, deletion, retention with `evicted_ranges` and claim objects; `ListCaptures`, `ListPacks`, `ListScopeChanges`, `PackURL`; service auth | the VE service reads a settled hour and learns of a reopen; registration between snapshot and commit in `open` and `reopened` states; lease takeover between statements; close versus late stop both orders; a late claim reference inside retention always finds its object; an hour's eviction removes packs, receipts and claim objects together; a conflicting claim version stays available until eviction; tombstone before adoption and adoption before deletion, both orders; a put crossing hour eviction never registers; a parked message replays in order after its blocker clears; a rejected capture's claim object survives eviction of its hour; a lapsed member cannot fetch; an old-version replica is refused after activation; event cursor with an uncommitted earlier transaction and with pruning; retention across a backward clock step reports evicted, not gap; close after seq 100 keeps `highest_seq` at 100; two copies of one parked version, the first acknowledged, the hour evicted, the second acknowledged from the `parked` row; unknown-capture parking racing admission and retention, both orders, on both engines; a crash after a parked record's staging commit, reconciler run, hour and receipts evicted, then a late copy acknowledged from the `staged` row; a version terminated, its hour evicted, its known messages acknowledged, the survivor released, then a later copy acknowledged under the termination without the object; a shared object, two gates: (i) versions A and B both `staged` before eviction, the hour deleted normally with a retried bucket delete; (ii) A `staged`, B unresolved at eviction, the survivor kept through repeated cleanup cycles and restarts, B then `terminated` and its known messages acknowledged, the survivor released, both orders, with cleanup crashes before and after the bucket delete; a terminated survivor with one copy marked `lost` by adoption released, and an unresolved version with every copy `lost` kept until the operator terminates it; the full `rejected → open → staged → acknowledged → horizon` sequence |
| 3 — masking and keys | policy binding with the baseline rules, Vault and file providers, bindings and fail-closed, `Restore`, `Digest`, audit; thinning of requests `fdc_grpc` marks interval-only by their origin, never a parallel-mode poll (T-42; eqp-hub's side is phase G's) | no clear recipe body (every baseline carrier) anywhere in storage or temp files; a restore round-trips; two first uses of one key id with different material; rotation between two deliveries of one record |
| G — eqp-hub integration (after phase 1 has proven the SDK; in parallel with phases 2–3) | one thin eqp-hub change: wire the SDK's observers into `eqp-hsms` behind a flag, pass `toolID` as a device property, expose the waiter's typed timeout through `TimeoutSource`, update the waiter's timeout on HotReload, carry `fdc_grpc`'s interval-only origin from `buildRequest` to the send path's registration (T-42); validated with eqp-hub's existing e2e harness against `tracekeep dev` | the pilot gateway's capture readable with the tracepack CLI; eqp-hub's chaos test with the traffic bus down and up shows no tool-side change; the SDK pin in eqp-hub is the only cross-repository artefact |
| 4 — people | OIDC BFF with the `session` key purpose, first pages, `QueryRecords` and `FindTransaction`, canonical `Export` bundles, Helm and systemd packaging (no holds, T-4) | a troubleshooting session end to end on a standalone host and on a cluster; IdP down before and after start; export of a capture spanning two hours with a conflict; no clear recipe body in an export |

**Implementation checklist** (from external review rounds 18–20, kept outside the repository;
schedules the gates above imply but do not enumerate;
each is a barrier-controlled test on both catalog engines where the catalog is involved):
1. restore both stream roles from snapshots that contain the old `tk-ingest`/`tk-parked` durables with pending messages;
   `bus adopt-stream` deletes and verifies; after restart the pending bytes reach the new consumers;
   a retained-consumer rewind; stale buffered acknowledgements, naks and pulls from a process that missed the stop;
2. resolution-input failures: terminated, staged and order-covered `start`s with missing or invalid descriptors;
   a missing or malformed claim digest; a digest without a reference (G5-258); a malformed `Nats-Msg-Id`;
   an inline body of exactly 44 bytes; the malformed row never fabricates a version or admits a capture;
3. reconciliation boundaries:
   the acknowledged copy at N with nothing published after, with N+1 published, and with N still present;
   no advancement on a failed refresh, `AllowDirect` on, an incarnation mismatch, an unrelated error or a retired identity;
4. termination-order boundaries: a publish sent before and accepted after the `Info` snapshot;
   `staged` precedence below the cut-off; a terminated-version message above the cut-off;
   `spent` by `FirstSeq` and by retirement;
5. the combined lifecycle: V1 parked, V2 never parked below the cut-off, V3 an evicted-hour survivor, V4 above the cut-off;
   a copy row inserted immediately before and immediately after the termination transaction;
   a late same-version message after the object's deletion, acknowledged under the termination;
6. confirmation failures:
   zero and malformed 10071 descriptions, 10164, a timeout and a lost `PubAck`, converging, asserted on the stored messages and not only on the responses;
7. termination against ingest effects (T-32): a termination committed immediately before and immediately after an admission, a parking step (1) and a registration of the same capture,
   the registration that loses aborting with its key tombstoned and its orphan never adopted;
   a membership renewal uncommitted while an admission reads the member set;
   a close whose publication the horizon guard refuses, left `abandoned`.

Phase 1 may run without a policy only on data that holds nothing sensitive; production traffic waits for phase 3.
Phases 0–3 run in one repository with one CI matrix (embedded bus, `fs` or `gofakes3`, MinIO and Ceph RGW for the `s3` conformance suite, SQLite and MariaDB in containers),
so every chaos and race test is a barrier-controlled in-process test;
eqp-hub reviews the SDK's API (`Bus`, `Preflight`, the six guarantees, the `secsobs` observer options) during phase 0 without changing any code.

## Beyond the seed

Milestones the owner set after seed v21 §12's (T-52).

| Phase | Scope | Gate |
|---|---|---|
| 5 — correctness, error handling and performance | no new feature: the service and the producer SDK correct under faults, their failures reported with their own codes and without keys or internal text, and their hot paths benchmarked and made faster where the numbers show it | the gate cases of its plan: a stalled remote call and a stalled client bounded, clean stops, the clustered testbus steady, keyless errors, a baseline and a before-and-after for every hot path, and an API write on SQLite within 2 s at the 99th percentile during sustained ingest |

## Active plans

None.

Phase 0 is done (#1, #3, #4, #7, #8).
Phase 1 is done (#9, #11, #13, #14, #16, #17, #18, #19).
Phase 2 is done (#22, #23, #24, #25, #26, #27, #28, #29).
Its closed plan's step tables (last text at e396be1) name commits of the branches before their rebase merge,
which are not on `main`; each is among the commits of the pull request its phase row names.
Phase 3 is done (#33, #34, #35, #36, #39, #41, #43); its closed plan's last text is at 6238649.
Phase 4 is done (#46, #48, #49, #52, #53, #55, #58); its closed plan's last text is at e95dee5.
Phase 5 is done (#64, #67, #70, #74, #78, #82, #83); its closed plan's last text, with its measurements and the gate's evidence, is at 89a01d8.
Like phase 2's, the commits its step tables and measurements name are the branches' before their rebase merge, each among the commits of the pull request its sub-phase names.
The capture identity (T-59: equipment, source and device) is done (#91, #92, #93; `producer/v0.6.0`); its closed plan's last text is at ab976c3.
The test efficiency (T-61) is done (#96, #97, #98 and the pull request that closed its plan); its closed plan's last text, with its measurements, is at 10803c8.
CI's `ci` job went from 9–14 min to about 8.5 min and its `catalog` job from 10–14 min to about 4 min, its packages in four legs.
The host log feed (P3; T-60, T-62 to T-66) is done (#100, #101, #102, #103, #104, #105, #106, #107, #108, #109, #110, #111, #112), `specs/tracekeep/logfeed.md` current;
its closed plan's last text, with its measurements and the gate's runs, is at 3c11e5e.

## Open items

- `TestRecordListingLeaseLostBetweenTransactions`'s `sweep pages` failed once on MariaDB on a loaded disk, its second page deleted after the lease lapsed;
  its barrier sees the lapse's statement in the process list, not that it waits for the lease row.
  The cause is not proven (the test efficiency plan's last text, 10803c8).
- The SDK sometimes marks a capture's first S1F3 ordering-uncertain, observed before its predecessor (`producer.md` §4.2);
  tracepack's lookup then does not match it, and the packaging browser test accepts that only with the gap shown.
  Why the first request is observed before the records allocated ahead of it is still open.
- An hour whose masked segments merge past the reader's metadata budget stays unmergeable (T-45);
  the lasting fix, redaction entries kept outside the pack metadata, is a format change asked of go-secs.
- Masking's cost at ingest, measured on 2026-10-30 (the measurements are kept outside the repository), leaves these improvements:
  - go-secs: a format-minor accessor, so a flush need not decode a working file's metadata twice (26–53 % of a masked segment's write);
    the AES-SIV cipher built once per key, not once per masked field (35–45 % of `Mask` on many-field records);
    no plaintext copy for a single contiguous range, and an HMAC reused across records;
    fewer allocations in the item walker.
  - tracekeep: a capture key derived once per fetch batch, or a derived-key cache, was closed by phase 5 on its numbers
    (masking adds about 4 µs a message, `Dispose` 31 against 27 µs; T-56), `masking-keys.md` §9 unchanged.
- The feed stream's cost is its acknowledgements (`logfeed.md` §15.10):
  each loads and removes its message under the stream's lock, and a flush acknowledges up to a flush interval's worth of held entries at once.
  Measured on 2026-11-08 on one host with the owner's fleet, 24,752 entries a second over 32 groups (the measurements are kept outside the repository):
  one stream of one replica carries it with a dip at every flush;
  a stream a group of one replica, the servers' subject mapping partitioning the publishes, removes the dip
  (its lowest ten seconds 22,305 entries a second and its publish p99 134 ms, against 5,623 and 712 ms on one stream).
  Three replicas carry about a third of the fleet on one stream and 85 % on a stream a group,
  the latter at about three times the CPU, network and writes of a stream a group of one replica.
  The owner chose on 2026-11-08 to take this next: a stream a group of one replica, acknowledged as today,
  the servers' subject mapping keeping the publishers unaware of the streams;
  the mapping is so far tried on one server only, not in a cluster or under JWT accounts.
- A log-style use was measured on the same host and day and is not taken up for now (the owner's choice):
  a stream a group of one replica kept by limits and consumed with no acknowledgement,
  each flush registering in its transaction the group's checkpoint, the stream sequence below which every entry is settled
  (registered, or acknowledged as stale or unreadable), so an earlier entry another equipment still holds stays above it,
  the stream purged below it, and a restart resuming from it, the files' watermarks dropping what it reads again (`logfeed.md` §15.5).
  - It smooths the publishes (the lowest ten seconds about 24,000 entries a second, publish p99 14 to 89 ms) and halves the bus disk's writes.
  - The stream holds everything above the oldest held entry, about F of the fleet: up to 45 GB against up to about 13 GB acknowledged,
    so `logfeed.max_bytes` must grow by as much to keep its outage margin; at 40 GiB the stream filled and the publishes stalled.
  - A purge loads the per-subject index of every block it drops, under the stream's lock:
    with the flushes aligned its p99 was 5 s and 21 purges timed out, with them spread 0.48 s.
  - The publishes were retried 1.4 to 12.8 million times without one failing, which is not explained.
  - A purge filtered by subject on one shared stream scans the whole stream under its lock (p99 3 s), so only a stream a group can purge.
  - A restart from the checkpoints read 7.9 million entries again, about F of the fleet, and every group caught up within 41 s;
    a converter's decoding of them is not measured.
  - A full stream refuses the publishes and a purge frees it, as discard-new does with acknowledgements.
- A fleet's flushes align, and their load with them, measured on the same host and day with the log stage:
  - Spreading them over the flush interval F, each equipment flushing at its own phase, removes the cold acknowledgements' wave at every F
    (the bus disk's reads over eight minutes 0.5 against 73 GB, the store's puts and the catalog's commits a second at p99 three to four times lower, their totals unchanged);
    it does not help after a burst, whose own volume brings the cold reads back.
    On a stream a group with acknowledgements, spreading measured worse than aligned flushes
    (publish p99 461 against 134 ms, the bus disk's reads 417 against 144 MB/s), contrary to the one-stream run;
    the owner chose to explain that before spreading is taken up.
  - The hour's change aligns them again, since it flushes every equipment's earlier hour at once (`ingest.md` §6.2, `logfeed.md` §15.7).
    Leaving an earlier hour's batches to the equipment's next flush took the hour's busiest second from 997 to 32 puts,
    the catalog's commits a second at p99 from 394 to 72 and its row lock waits from 1,318 to 108, with 30 % fewer packs,
    since the hour's flush no longer flushes the new hour's batch early as well.
  - No reader assumes an earlier hour sooner: every bound already allows a batch to wait F
    (`compact.quiet_period` must exceed `ingest.flush_interval` + `ingest.ack_wait` + `compact.redelivery_slack`, and the log stage's F must be below `ingest.ack_wait`).
  - To change: the log stage's trigger;
    ingest's `Batcher.Due`, due F after a batch's opening, where the batches of every capture active across the hour open together at its start,
    so a capture needs a flush time of its own;
    and ingest registering a capture's earlier hour first, or the gap alert a `for:`, since a later hour registered first shows the earlier one's tail as a gap for up to F.
- go-secs is asked to export a filtered JSONL writer; until it does, `StartExport` refuses `kind=filtered` (T-48).
- Configuration retirement waits for the first release that needs it.
- The veq branch `docs/ve/log-service-decisions` (commit 7b8a019) carries the LS-row amendments and V-35;
  it is not merged yet.
- The service's `go.mod` requires the SDK from its first tag (`producer/vX.Y.Z`) on;
  `producer/doc.go` blank-imports tracepack and nats.go only to keep the SDK's requirements until code imports them.
