# tracekeep — catalog

Status: draft (2026-10-25) — split from seed v21

Covers the catalog's tables (parking tables in `parking.md`), its operations, completeness and the change cursor, engines, transactions and migrations.
From seed v21 §7, except the parking rows of §7.1;
section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/catalog`, the schema or migrations, or anything that reads completeness or the cursor.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes under discussion are in [P1](proposals/p1-interfaces.md).

- **Role**: the catalog is the service's database; tracepack defines no view of it (`positioning.md` §2).
- **Operations**: admit a capture, look one up, register a pack, list packs and ranges, pick and commit merges, insert and publish a close, pick deletable packs, take or renew a lease, allocate an event seq, append the audit log (§7.2).
- **Transactions**: about forty statements;
  four multi-statement transactions guarded by compare-and-swap predicates with affected-row checks and unique constraints;
  no `SELECT … FOR UPDATE` (§7.2).
- **Lock order**: the capture row first in every pack-changing transaction, the `counters` row last, nothing else locked (§7.2; `ingest.md` §6.3; `compactor.md` §8.3).
- **Completeness**: producer coverage is the union of `pack_ranges` and `evicted_ranges`;
  a hole is a gap, evicted, or the unknown tail;
  an hour with no scope is `no-data`;
  unregistered intervals are reported against every earlier capture of the device,
  and overlapping captures of one device are flagged, never merged into one series (§7.3).
- **Device**: `captures.device` is the descriptor's `recorder_instance_name`, indexed with `tool_id` for selection (§7.1).
- **Cursor**: an event's seq is allocated from the `counters` row as the last statement, so allocation order is commit order;
  the watermark is read before the listing;
  pruning removes a seq prefix only;
  a stale cursor re-lists from `ListPacks` (§7.3).
- **Engines**: MariaDB at `READ COMMITTED` with whole-transaction retries;
  SQLite with one writer, `BEGIN IMMEDIATE`, and only the `all` process writing (§7.4).
- **Migrations**: Atlas, versioned per engine, expand-then-contract across two releases (§7.4).
- **Open**: the seed names the operations and the lock order but fixes no signatures.

## 7. Catalog (requirements 3 and 4)

### 7.1 Tables

| Table | Holds | Key facts |
|---|---|---|
| `captures` | descriptor bytes and decoded fields; the processing configuration ids; `state` `pending`/`open`/`stopped`/`closed`/`rejected` (`pending`: a placeholder without descriptor, inserted by parking before any `start`, §6.1) (a rejected `start`'s descriptor bytes kept for the operator); `data_revision`; `highest_seq`, `highest_seq_ts` (producer seqs only); `stop_seen`; `last_registered_at`; thinning parameters; `device`, decoded from the descriptor's `recorder_instance_name` at admission (none on a `pending` row) | one row per capture; indexed by (`tool_id`, `recorder_instance_id`, start time) for successor lookup and by (`tool_id`, `device`, start time) for device selection |
| `capture_closes` | the close intent: trigger time, `highest_seq`, `highest_seq_ts`, bounds, fixed `pack_id`, `writer_time`, the serialized `PackMeta` and codec, `state` `pending`/`published` | one row per capture; inserted before the close is written (§8.2) |
| `packs` | `pack_id`, capture, `hour`, role, object key, size, `seq_start`, `format_minor`, `state` `staged`/`merged`/`excluded`/`deleted` | index `(tool_id, hour)`, `(capture_id, hour, state)`; `excluded` packs are read, never merged, never deleted by the merge relation |
| `pack_ranges` | `(pack_id, seq_from, seq_to, ts_from, ts_to)`: the exact seq runs a pack holds with their endpoint times | the completeness evidence of §7.3; the close sentinel is never a range |
| `evicted_ranges` | `(capture_id, hour, seq_from, seq_to, ts_from, ts_to)`: seqs whose packs retention deleted, with their hour and endpoint times | §7.3 reports them as evicted, never as gaps, keeps the times a neighbouring gap needs, and tells an evicted hour from a `no-data` hour; a prefix scalar cannot express an hour (§8.3) |
| `claim_records` | `(capture_id, seq, header44, payload_sha256)` → first `pack_id`: the **receipts** of records that travelled by claim-check, one row per version | version-sensitive staged proof for §6.1 and §8.4; outlives merges and deletions; removed only by retention |
| `pack_losses` | a pack's `coverage` entries: declared losses | carried into the archive's rows on merge |
| `conflicts` | `(capture_id, seq)` → the packs holding each version | surfaced by `ListPacks`; blocks the scope's merge |
| `claim_objects` | claim object key, capture, seq, **hour** of the record (from the receipt's header), size; `survivor` with the evicted hour once retention skipped it (§8.4) | deleted when §8.4's predicate holds: with the hour by retention, or later as a survivor by the reconciler |
| `consumers` | replica id, lease expiry, the configurations it executes | the activation gate and membership leases of §6.4 |
| `parked`, `parked_copies`, `bus_streams`, `termination_orders` | the parking tables | in [parking.md](parking.md) |
| `scopes` | `(capture_id, hour)`: `state` `open`/`settled`/`reopened`, `revision`, `archive_pack_id`, `merged_at` | CAS on `revision` (§8.1) |
| `scope_events` | `(seq, capture_id, hour, event, revision, at)`, `seq` from the `counters` row; a `low_water` and an `epoch` in `counters` | the LS-11 cursor (§7.3); pruned by seq prefix only |
| `counters` | named counters (`scope_events`) | a row lock serializes allocation with commit order |
| `merges`, `merge_inputs` | every merge with its inputs, output and outcome | the deletion relation (§8.3) |
| `tombstones` | object keys deleted by retention or deletion, with the time | the reconciler never re-registers them |
| `processing_configs`, `policies` | versioned configurations captures bind to | never updated in place |
| `key_bindings` | `key_id`, kind, provider reference and version, material fingerprint | insert-or-compare, fail closed (§9) |
| `leases` | name, holder, `fence_token`, `until` | compactor singleton |
| `audit_log` | restores, digests, exports, admin changes, by whom | append-only |

`seq` is at most 2^63 − 1, so it fits a signed `BIGINT`; times are `BIGINT` UTC nanoseconds; UUIDs are `BINARY(16)`.
`packs` and `pack_ranges` are the large tables:
about 1,500 tools × 12 segments an hour is some 13 million pack rows a month before deletion,
so their indexes are the design.

### 7.2 Operations

Admit a capture (the prologue of §6.3 inserts-or-finds the row; `pending → open`, or descriptor bytes compared on an admitted one), look a capture up by id (cached in `ingest`), register a pack (§6.3, idempotent on `pack_id`),
list packs and ranges by tool and hour range, pick scopes to merge, commit a merge (§8.1), insert and publish a close (§8.2), pick deletable packs (§8.3),
take or renew a lease, allocate an event seq, append `audit_log`.
About forty statements;
point lookups, range scans on one index, inserts, and four multi-statement transactions guarded by **compare-and-swap predicates with affected-row checks** and unique constraints;
no `SELECT … FOR UPDATE`, so the same statements mean the same on both engines;
the two intentional row locks are the capture row, first in every pack-changing transaction (§6.3), and the `counters` row, last (§7.3).

### 7.3 Completeness (LS-10) and the change cursor (LS-11)

- **Producer coverage**: for a capture, the union of `pack_ranges` of its non-deleted packs over all hours, plus `evicted_ranges`;
  the close sentinel 2^63 − 1 is never in either.
  A **gap** is a hole in that union between seq 0 ([FMT I-12]) and `highest_seq` (the highest producer seq, §6.3),
  reported with the time interval between the records on either side (from the endpoint times),
  which may span hours or run backwards after a clock step;
  a missing prefix has no record before it,
  so its interval starts at the capture's `capture_origin_utc_ns` and is marked open-ended on that side.
  A hole inside `evicted_ranges` is reported as **evicted**, a third class beside gap and unknown;
  a seq both retained and evicted counts as retained;
  a gap next to an evicted run takes its time bound from the evicted run's endpoint time.
  The seqs above `highest_seq` are the **unknown tail**, whatever the capture's state, closed included ([STO §4]).
  A segment of hour h that arrives after the merge still extends the union;
  nothing about completeness depends on scope state.
- **Declared losses** come from `pack_losses`;
  **crash gaps** from `capture_closes` (`gap_start`, `gap_end`);
  **conflicts** from `conflicts`.
- `ListPacks(tool, from, to)` iterates the requested hours:
  for each, the scopes of every capture of the tool overlapping it, with packs, ranges, state and revision;
  an hour with no scope is reported as `no-data` together with whether a capture of the tool was open then,
  so a missing whole hour is a result, not an absence.
  An **unregistered interval** is reported
  when a capture starts more than Q after the latest `highest_seq_ts` of every earlier capture of its (`tool_id`, `recorder_instance_id`),
  not after the end of one predecessor, so two overlapping captures are never reported as missing traffic;
  or when its `previous_capture_id` names no registered capture.
- **Overlap**: two leaders running at once write two captures of one (`tool_id`, `recorder_instance_id`), which [STO §4] allows.
  When the spans of two such captures intersect, each from `capture_origin_utc_ns` to `highest_seq_ts`,
  both are flagged `overlap` in `ListCaptures` and `ListDevices`;
  the flag is computed from the spans, not from the `open` state, so it survives the close of either capture.
  A read by device returns both, each with its own completeness, and never interleaves them into one series.
- **Settled** means "merged, and nothing more is expected on the normal path":
  a scope becomes a merge candidate at hour end + quiet period Q (default 15 minutes, above F + `ack_wait` + redelivery slack),
  and `settled` when the merge commits; a registration afterwards sets `reopened`; a re-merge sets `settled` again.
  Settlement is a scheduling fact; completeness is the ranges and gaps, reported beside it.
- **Cursor**: `scope_events.seq` is allocated from the `counters` row as the last statement of the emitting transaction;
  the row lock makes allocation order commit order on MariaDB, and SQLite has one writer,
  so a reader that has seen seq s never misses a lower seq committed later.
  Events are emitted for `created`, `settled`, `reopened` and `conflict` (a suspect raised, confirmed or cleared on that scope, whichever hour the other version is in, §6.2);
  every conflict transition, in registration or in `compact` (confirmation, clearing, a merge that fails on a conflict), increments the revision of **every** scope it touches in the same transaction, capture row first and `counters` last,
  so a replayed event is never ignored for a changed state.
  Pruning deletes a **seq prefix only**:
  `low = min(seq) over events younger than the retention period`, delete `seq < low`, set `counters.low_water = low` in the same transaction, never by time alone,
  since event time and seq are not ordered together;
  `epoch` changes only when an operator truncates otherwise,
  and a client whose cursor is below `low_water` or carries another epoch re-lists the window from `ListPacks` and continues from the watermark that listing returns.
  The **watermark is read before the listing**:
  `ListPacks` reads `counters.scope_events` = w first, then the scope and pack rows, and returns w;
  an event committed during the listing has seq > w and is replayed to the client,
  which treats events as idempotent (each carries the scope's revision, and a replayed revision is ignored).
  Pruning with no event younger than the retention period deletes nothing.

### 7.4 Engines, transactions and migrations

- **Recommended stack**: `database/sql` + `sqlx`, hand-written SQL, `go-sql-driver/mysql` and `modernc.org/sqlite` (no cgo).
  A `dialect` value isolates insert-ignore, the upsert clause and `ENUM` versus `CHECK`;
  the transactions are CAS-based and engine-neutral (§7.2).
- **MariaDB**: `READ COMMITTED`; every multi-statement transaction retries whole on deadlock;
  the capture row and the `counters` row are the intended lock waits, always in that order.
- **SQLite**: one writer connection (`MaxOpenConns = 1` on the write pool), `BEGIN IMMEDIATE` for every write transaction, `busy_timeout` set, retry whole on `SQLITE_BUSY`;
  only the `all` process writes, enforced by deployment and checked at start (a second writer refuses to start on SQLite).
- **Tests**: the repository tests run the same adversarial schedules on both engines with barriers, not timing:
  registration during a merge snapshot, lease takeover between statements, close intent versus a late registration, event allocation with an uncommitted earlier transaction, two first uses of one key id.
- **ent** rejected for now (code generation and a graph model for forty flat statements; its migration opinions beside Atlas);
  **sqlc** rejected (per-engine query sets);
  **bun** is the fallback if the dialect value grows.
- **Migrations**: Atlas versioned.
  One `schema.hcl`;
  `atlas migrate diff` produces `migrations/mariadb/` and `migrations/sqlite/` with their `atlas.sum`;
  `atlas migrate lint` in CI.
  The **first release applies them with the Atlas CLI** (`atlas migrate apply`: a Job or init container on Kubernetes, the binary shipped in the standalone package),
  because the embedded Go library path is unverified;
  `tracekeep migrate` wraps that CLI and is replaced by the library once verified.
  Schema changes are expand-then-contract across two releases,
  so an old and a new consumer run against one schema during a rollout.
