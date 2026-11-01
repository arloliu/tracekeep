# Phase 5 — correctness, error handling and performance

Status: draft (2026-11-01) — awaiting the owner

The owner's next phase after the roadmap's milestones:
no new feature, but the service and the producer SDK made correct under faults, their failures reported, and their hot paths measured and made faster.
Its work comes from an audit of `main` at e3b9722, recorded below as *The audit*;
each step cites its findings by id.
The design stays in `docs/specs/tracekeep/`:
a finding marked *spec* changes a spec, and its spec step comes first.
This plan only orders the work.

## Before it starts

The owner's answers to the points below become one decision-log entry when the plan is activated (T-52).
Each point gives the recommendation the plan assumes until the owner answers.

1. **Where phase 5 sits in the roadmap**:
   the milestone table is seed v21 §12, "moved unchanged", so a new row would break that sentence.
   Recommended: a short section after the table, *Beyond the seed*, with a phase 5 row of the same columns.
2. **Spec-neutral first**: a finding that changes a spec waits for the owner's answer on it here.
   Recommended answers:
   - a store failure during an export fails the attempt, so the lease lapses and another attempt retries, instead of leaving the pack out of a `done` bundle (C6, amends `api.md` §10.1);
   - `FindTransaction` answers `not_found` for an unknown capture and a non-retryable code for a corrupt pack (E2, amends `api.md` §10.1's codes);
   - a restore takes a read slot and a bounded merge budget (C17, amends `api.md` §10.1's `max_reads`);
   - a scope whose merge keeps failing backs off, and a cycle merges at most a bounded number of scopes (P17, amends `compactor.md` §8.1 and §8.3);
   - not now: batched `PrepareAck` and `AckCopy` (P8's second half, `parking.md`), a derived-key cache or a capture key per fetch batch (P19, P21, `masking-keys.md` §9), and verifying with stale JWKS after a failed fetch (E14); each is decided again from 5.3's numbers;
   - not at all: a cap on `QueryRecords`' window; C2 skips the empty hours instead, which needs no spec change.
3. **TLS for the bus and the store**:
   `650-perf-sec.md` asks for TLS to NATS and S3, but the configuration accepts `nats://` without a CA and an `http://` endpoint (E12).
   Recommended: refuse both outside `tracekeep dev`, named in the configuration guide and the upgrade notes, since an installation that relies on them stops starting.
4. **go-secs' share stays out**:
   T-45, the format-minor accessor that would end the flush's reopen (P22), the filtered writer (T-48), tracepack's epoch-hour error text,
   and the first S1F3's `ordering-uncertain` (K1), which the SDK reports correctly from go-secs' timestamps.
   Recommended: ask go-secs for each; here K1 is documented as expected in `producer.md`, and the browser test keeps accepting the gap.
5. **SQLite's one write connection**:
   an API write waits in `database/sql`'s pool behind ingest and the compactor, unbounded and unmeasured (P6).
   Recommended: shorten the long write transactions (P1–P4), give an API request's audit write a deadline and a wait histogram,
   and add a priority gate only if 5.3 then still shows a starved short write.
   `synchronous` stays `FULL`, and the audit log stays in the catalog, since its rows commit with domain rows.
6. **Measure first**: `650-perf-sec.md` asks for benchmarks before and after, and only two exist.
   Recommended: 5.3 adds benchmarks for every hot path, an opt-in `pprof` on the metrics listener, and baseline numbers recorded in this plan, before any performance step;
   a performance finding that 5.3 shows is not worth its change is closed with its number.
7. **The gate**: proposed below in *Gate cases and their owners*; the owner sets the bound of the write-connection case.
8. **The split below** into six sub-phases, correctness before error handling before performance.

Not in this phase: new features, go-secs' changes (point 4), the eqp-hub side of phase G, and a `producer/` tag, which stays the owner's call.

## Phases

| Phase | Scope | State |
|---|---|---|
| 5.1 — correctness and liveness | detailed below | pending |
| 5.2 — error handling and signals | E1–E18: context errors mapped to their own codes, the SDK's panics, drops and sink events counted from zero, the service's bus events logged, dropped errors logged, readiness without internal text, retries with backoff, TLS and configuration bounds (points 3 and 2), JWKS fetched ahead of its TTL, downloads' headers; the spec and guide rows each names | pending |
| 5.3 — measurement | benchmarks for the SDK's observe path, ingest's dispose, batch, flush and acknowledgement, the catalog's registration, listing and reads on both engines, `QueryRecords` over an hour of N records in K packs (binary and JSON), merge and the compactor's passes; `pprof` on the metrics listener, opt-in; the baselines recorded here | pending |
| 5.4 — performance: the catalog and its writer | P1–P6, each measured before and after: set-based registration, the overlap probe's index, the claim listing in bounded transactions, the missing indexes, listings bounded to their window, the API audit write's deadline and wait histogram (point 5) | pending |
| 5.5 — performance: ingest, reads, compactor, API | P7–P20 that 5.3 shows worth their change, each measured before and after | pending |
| 5.6 — gate | audit every gate case against the test bodies, then one step per gap, as 4.6 did | pending |

## Phase 5.1 — correctness and liveness

Work on the branch `fix/phase-5/correctness`, one commit per step, test first, each catalog test on both engines;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Every step's test fails before its fix: a barrier or an injected fault where timing decides.

| Step | Scope | State |
|---|---|---|
| 1 | spec for C6 and C17 if the owner takes point 2's answers, and `operations.md` §10.4 for the bounds below | pending |
| 2 | C1: a deadline on every flush step, sized from the ack wait; the S3 client's response-header timeout; the final flush bounded; a put that never answers releases the batch's messages and lets the role stop | pending |
| 3 | C2: `QueryRecords` reads only the hours with a scope or an eviction, read when the stream reaches them; a window of years over one capture answers in a bounded number of reads | pending |
| 4 | C3 and C17: a stream's send bounded, so a client that stops reading frees its slot and memory; the servers' idle timeouts; a restore's merge budget and slot | pending |
| 5 | C4 and C5: `api` and `all` exit 0 on a stop signal; readiness fails before the listener closes; both packaging legs assert the unit's clean stop | pending |
| 6 | C6, C7, C8, C9: an export attempt failed by a store error; a claim object's size checked before it is read; a missing bucket told from a missing object by `Stat` and `Open`, on MinIO and Ceph RGW; due batches left to the bounded final flush on a stop | pending |
| 7 | C10, C11: retry closures that reset their results; `Incarnate` retried on a duplicate id | pending |
| 8 | C12–C15, the SDK: a discarded rotation start skipped, not counted dropped; sink events drained on `Close`; `Close` cancelling the publisher's waits; a transient claim read retried, not deleted | pending |
| 9 | C16, C18: the clustered testbus retried on "peer offline" and its consumer creation retried, then passing 20 consecutive runs; the simulator's rate bounded and a timed replay's end a success | pending |

## The audit

Read 2026-11-01 at e3b9722 by five read-only passes (the SDK; ingest, bus and store; the catalog; compactor, records and API; auth, keys, service and commands),
with `go vet` and the SDK's `-race` tests clean, and the catalog's costs measured on SQLite on an idle machine.
No invariant of `600-invariants.md` was found broken, and no message is acknowledged before its data is durable.
Severity is high (H), medium (M) or low (L); *spec* marks a finding whose fix changes a spec.

### Correctness and liveness

| Id | Finding | Where | Sev |
|---|---|---|---|
| C1 | a stalled S3 put or catalog call blocks an ingester and its stop without bound: no deadline on a flush step, the final flush unbounded, the S3 client without a response timeout | `internal/ingest/flush.go:249`, `ingester.go:148`, `internal/store/s3store/s3store.go:71` | H |
| C2 | `QueryRecords` reads every hour of its window for each capture, a read set per hour, so one request over a wide window runs millions of catalog transactions holding a read slot | `internal/records/records.go:129`, `internal/catalog/reads.go:142` | H |
| C3 | a client that stops reading a stream holds its read slot and up to its held bytes without bound; the servers set no idle timeout | `internal/api/query.go:553`, `internal/service/api.go:79`, `internal/metrics/server.go:108` | M |
| C4 | `api` and `all` exit 1 on a normal stop signal, so the systemd unit ends `failed` | `internal/service/api.go:181`, `cmd/tracekeep/main.go:143` | M |
| C5 | readiness stays green while the API drains, so traffic still reaches a closed listener | `internal/service/api.go:127`, `internal/service/service.go:286` | L |
| C6 | a transient store failure during an export leaves the pack out of a `done` bundle (*spec*) | `internal/api/export_bundle.go:300` | M |
| C7 | a claim object is read whole before its declared length is checked, so an oversized one can exhaust each replica's memory in turn | `internal/ingest/ingester.go:561`, `deliver.go:251` | M |
| C8 | a HEAD on a missing bucket likely answers like a missing object, so packs read as gone instead of the store failing (to confirm on MinIO and Ceph RGW) | `internal/store/s3store/s3store.go:131` | M |
| C9 | on a stop, due batches flush under the cancelled context: naked, or registered and left unacknowledged, then written again | `internal/ingest/ingester.go:182` | L |
| C10 | two retried transaction closures keep the previous attempt's results | `internal/catalog/exports.go:150`, `exports.go:349` | L |
| C11 | `Incarnate` allocates `MAX+1` and does not retry a duplicate on MariaDB, so two roles starting together can fail one | `internal/catalog/incarnation.go:41` | L |
| C12 | the SDK counts a discarded rotation start as an abandoned drop and can fire `TracepubDropping` with nothing lost | `producer/publisher.go:313` | M |
| C13 | the SDK's sink loop exits on `Close` without draining, so a stopped device can keep `current_capture` set | `producer/producer.go:322` | M |
| C14 | the SDK's `Close` waits out a publish, claim or rotation timeout it could cancel | `producer/publisher.go:369`, `producer/lifecycle.go:382` | L |
| C15 | the SDK deletes and re-puts a claim object after a transient read error | `producer/publisher.go:513` | L |
| C16 | the clustered testbus retries only a deadline, not a placement's "peer offline", and one test creates its R3 consumer without a retry | `internal/testbus/testbus.go:279`, `internal/compact/gate_test.go:161` | M |
| C17 | a restore merges without a read slot under tracepack's default 512 MiB budget (*spec* for the slot) | `internal/api/restore.go:202` | M |
| C18 | the simulator panics on an extreme rate, and a timed replay exits 1 | `internal/sim/replay.go:83`, `cmd/tracekeep-sim/main.go:112` | L |

### Error handling

| Id | Finding | Where | Sev |
|---|---|---|---|
| E1 | a client's cancellation or deadline answers `unavailable` and logs an error, polluting the alert | `internal/api/query.go:317`, `convert.go:23`, `transaction.go:60` | M |
| E2 | `FindTransaction` answers `unavailable` for an unknown capture and, forever, for a corrupt pack (*spec*) | `internal/api/transaction.go:39`, `transaction.go:111` | M |
| E3 | the service's bus connection sets no handlers: async errors go to stderr outside the log, disconnects unlogged | `internal/service/service.go:168` | M |
| E4 | the SDK counts observer panics but never reports them | `producer/internal/core/observe.go:201` | M |
| E5 | the SDK's labelled counters start at their first event, so `increase()` misses a first drop after a restart | `producer/publisher.go:734` | M |
| E6 | the SDK's lifecycle events are dropped without a count when the sink lags | `producer/lifecycle.go:314` | L |
| E7 | a parking publish's error is dropped; a lasting failure shows only as deferrals | `internal/ingest/park.go:104` | L |
| E8 | the export cleanup logs a failed delete without its cause | `internal/compact/exports.go:41` | L |
| E9 | the dev IdP's serve error and the environment's close error are dropped | `internal/service/devsignin.go:115`, `cmd/tracekeep/main.go:133` | L |
| E10 | `/readyz` returns the checks' internal error text on the unauthenticated metrics port | `internal/metrics/server.go:86` | L |
| E11 | a retryable transaction re-runs 16 times at once, up to about 80 s on SQLite, without jitter | `internal/catalog/tx.go:94` | L |
| E12 | the bus URL and the S3 endpoint are accepted without TLS (point 3) | `internal/config/config.go:232` | M |
| E13 | the OIDC leeway and the session and login lifetimes have no upper bound | `internal/config/auth.go:54` | L |
| E14 | JWKS expire hard and refetch synchronously, so every request waits and an IdP down at that moment refuses all | `internal/auth/oidc.go:278` | M |
| E15 | `/download/` sends no `Content-Disposition`, ETag or `Last-Modified` | `internal/api/download.go:65` | L |
| E16 | `/auth/me` without a cookie logs a warning and counts a failure on every poll | `internal/auth/bff.go:158` | L |
| E17 | a refusal tracekeep writes itself carries a raw epoch hour | `internal/api/transaction.go:102` | L |
| E18 | `operations.md` §10.4 lacks the SDK's `abandoned` drop reason and the counters' start | `docs/specs/tracekeep/operations.md` | L |

### Performance

| Id | Finding | Where | Sev |
|---|---|---|---|
| P1 | a pack's registration runs about four statements per record under the write connection: 27 µs a delivery, 1.3 s for 50k | `internal/catalog/register.go:410`, `register.go:569` | H |
| P2 | the conflict probe scans the capture's earlier ranges: 14 ms a range at 200k | `internal/catalog/register.go:519`, `resolve.go:92` | H |
| P3 | the claim listing is recorded in one write transaction, about 6 s at a million objects every retention cycle | `internal/catalog/claims.go:47`, `internal/compact/retention.go:137` | M |
| P4 | `audit_log`, `scope_events`, `merges` and `conflicts` lack the indexes their frequent predicates need | `migrations/` | M |
| P5 | listings run 6–8 queries a capture over its whole history, and long snapshots hold SQLite's WAL back | `internal/catalog/reads.go:166`, `completeness.go:182` | M |
| P6 | an API write waits behind ingest in the pool without bound or measurement (point 5) | `internal/catalog/catalog.go:181`, `resolve.go:141` | M |
| P7 | ingest makes three catalog reads a message | `internal/ingest/deliver.go:187`, `deliver.go:231` | M |
| P8 | acknowledgements run one at a time; a parked copy costs three round trips (*spec* to batch) | `internal/ingest/flush.go:161`, `park.go:120` | M |
| P9 | batches due together flush one after another | `internal/ingest/ingester.go:453` | L |
| P10 | per-tick and per-message formatting, a quadratic copy of due batches, a batch overshooting its bytes by a fetch | `internal/ingest/ingester.go:231`, `ingester.go:454` | L |
| P11 | `fs` syncs every parent directory on a put; `s3` opens with a HEAD before its ranged reads | `internal/store/fs.go:225`, `internal/store/s3store/s3store.go:120` | L |
| P12 | a read decodes the capture's configuration again for every hour | `internal/api/query.go:72`, `export_bundle.go:278` | M |
| P13 | a seq range is applied after merging and cloning every record of the hour | `internal/records/records.go:223`, `records.go:360` | L |
| P14 | each record item is encoded twice, once to measure it | `internal/api/query.go:530` | L |
| P15 | the conflict check merges both packs whole under a 512 MiB budget | `internal/compact/conflicts.go:88` | L |
| P16 | the orphan pass reads the catalog twice for every stored object every cycle | `internal/compact/orphans.go:121` | M |
| P17 | a scope with a damaged input is merged again every cycle, with no cap per cycle (*spec*) | `internal/compact/merge.go:68` | L |
| P18 | every TLS handshake reads the certificate, key and CA files under one mutex | `internal/service/tlsfiles.go:51` | M |
| P19 | every masked record takes the keys' process-wide mutex and a label lookup, and derives with HKDF (*spec* to cache) | `internal/keys/keys.go:291` | L |
| P20 | the SDK's publisher takes each capture's lock twice a pass, so a callback can wait on it, not only on another callback (T-9's wording) | `producer/publisher.go:273` | L |
| P21 | the capture key is derived once a record, not once a fetch batch (*spec*; the roadmap's open item) | `internal/ingest/` | L |
| P22 | the flush's reopen: the budget check can move before the writer, the format minor still needs go-secs (point 4) | `internal/ingest/flush.go:325` | L |

### Known before the audit

K1, the first S1F3's `ordering-uncertain`: go-secs stamps a state change after `Open` returns, and a reply can be read before its request's write returns,
so the SDK reports an order go-secs gave it (point 4).
Only two benchmarks exist, the SDK's allocation and the fleet metrics', and nothing serves `pprof` (point 6).

## Gate cases and their owners

| Gate case | Owner |
|---|---|
| a store or catalog call that never answers bounds an ingester's flush and its stop | 5.1 |
| a client that stops reading frees its read slot, and a wide window reads only the hours that hold data | 5.1 |
| `tracekeep all` under its unit stops with exit 0 on both packaging legs | 5.1 |
| the clustered testbus family passes 20 consecutive runs | 5.1 |
| every refusal answers its own code, and no error a client or log sees carries a key, URL or internal text | 5.2 |
| every hot path has a benchmark and a recorded baseline | 5.3 |
| every performance change has its before and after, and no benchmark regresses beyond noise | 5.4, 5.5 |
| an API write during sustained ingest on SQLite answers within the owner's bound | 5.4 |
