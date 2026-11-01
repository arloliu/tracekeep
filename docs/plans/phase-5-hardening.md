# Phase 5 — correctness, error handling and performance

Status: draft (2026-11-01) — answered by the owner; under external plan review

The owner's next phase after the roadmap's milestones:
no new feature, but the service and the producer SDK made correct under faults, their failures reported, and their hot paths measured and made faster.
Its work comes from an audit of `main` at e3b9722 and its external review, recorded below as *The audit*;
each step cites its findings by id.
The design stays in `docs/specs/tracekeep/`:
a finding marked *spec* changes a spec, and its spec step comes first.
This plan only orders the work.

## Before it starts

The owner answered on 2026-11-01: every recommendation below is taken, except point 3's refusal.
Points 9 and 10 came from the external review after that answer and carry the recommendation the plan assumes until the owner answers them.
The answers become one decision-log entry when the plan is activated (T-52).

1. **Where phase 5 sits in the roadmap**:
   the milestone table is seed v21 §12, "moved unchanged", so a new row would break that sentence.
   Answered: a short section after the table, *Beyond the seed*, with a phase 5 row of the same columns.
2. **Which findings change a spec**:
   - taken: a store failure during an export fails the attempt, so the lease lapses and another attempt retries, instead of leaving the pack out of a `done` bundle (C6, `api.md` §10.1);
     `FindTransaction` answers `not_found` for an unknown capture and a non-retryable code for a corrupt pack (E2, `api.md` §10.1);
     a restore takes a read slot and bounds the readers it opens, refusing what it cannot compare whole rather than comparing part (C17, `api.md` §10.1, `masking-keys.md` §9);
     a scope whose merge keeps failing backs off, and a cycle merges at most a bounded number of scopes (P17, `compactor.md` §8.1 and §8.3);
     a transient claim read is retried, and only content proven incomplete or wrong is deleted and put again (C15, `producer.md` §4.3 *Uncertain uploads*);
     a client's cancellation and deadline answer `canceled` and `deadline_exceeded`, while a dependency's deadline stays `unavailable` and an audit failure `internal` (E1, `api.md` §10.1);
     discovery and JWKS fetched ahead of their TTL in the background, rate-limited, never verifying past the TTL (E14, `api.md` §10.2);
     the SDK's counters reported from zero (E5, `operations.md` §10.4);
     a stream whose send makes no progress for a configured time ends (C3, `api.md` §10.1 and the configuration guide);
   - not now: batched `PrepareAck` and `AckCopy` (P8's second half, `parking.md`), a derived-key cache or a capture key per fetch batch (P19, P21, `masking-keys.md` §9), and verifying with stale JWKS after a failed fetch; each is decided again from 5.3's numbers;
   - not at all: a cap on `QueryRecords`' window, since C2 skips the hours with no scope and no eviction instead;
     and E16's quiet `/auth/me`, since `api.md` §10.2 *Failures* logs every refused request, which stays.
3. **TLS for the bus and the store**:
   `650-perf-sec.md` asks for TLS to NATS and S3, but the configuration accepts `nats://` without a CA and an `http://` endpoint (E12).
   Answered: neither is refused, so no installation stops starting;
   outside `tracekeep dev` the service logs a warning at start naming each link without TLS, and `650-perf-sec.md` and the configuration guide say so.
4. **go-secs' share stays out**:
   T-45, the format-minor accessor that would end the flush's reopen (P22), the filtered writer (T-48) and tracepack's epoch-hour error text.
   K1 stays the roadmap's open item: the audit's two hypotheses go to go-secs with their source references, and no cause is recorded until a trace shows one.
5. **SQLite's one write connection**:
   an API write waits in `database/sql`'s pool behind ingest and the compactor, unbounded and unmeasured (P6).
   Answered: shorten the long write transactions (P1–P4), give an API request's audit write a deadline and a wait histogram,
   and add a priority gate only if 5.3 then still shows a starved short write.
   `synchronous` stays `FULL`, and the audit log stays in the catalog, since its rows commit with domain rows.
6. **Measure first**: 5.3 adds benchmarks for every hot path, an opt-in `pprof` on the metrics listener, and baselines recorded in this plan, before any performance step;
   the audit's timings are its own, on an idle machine, and 5.3 replaces them;
   a performance finding that 5.3 shows is not worth its change is closed with its number.
7. **The gate**: below in *Gate cases and their owners*, with the bound of the write-connection case defined there.
8. **The split below** into six sub-phases, correctness before error handling before performance.
9. **The publisher's reads under a capture's lock (C19)**:
   `600-invariants.md` and T-9 let a callback wait only for another callback, but the publisher reads the reservation state under the same lock.
   Recommended: amend T-9 and the invariant to name the publisher's reads, which hold no I/O, allocation, logging or call into go-secs,
   with a barrier test that proves it, and merge the two reads of a pass into one;
   a lock-free mirror of the reservation state would put the dependency barrier at risk for a wait the callbacks already accept from each other.
10. **A claim object's largest size (C7)**:
    no spec bounds a claim payload, and HSMS allows up to 4 GiB.
    Recommended: `ingest.max_record_bytes`, default 64 MiB, the batch's default byte bound;
    a declared length above it is refused before any read, as a malformed record (`ingest.md` §6.1), and a read stops at the declared length.

Not in this phase: new features, go-secs' changes (point 4), the eqp-hub side of phase G, and a `producer/` tag, which stays the owner's call.

## Phases

| Phase | Scope | State |
|---|---|---|
| 5.1 — correctness and liveness | detailed below | pending |
| 5.2 — error handling and signals | spec first (`api.md` §10.1–§10.2, `operations.md` §10.4, `650-perf-sec.md`, the configuration guide), then E1–E15, E17–E19: each error with its own code, the SDK's panics, drops and sink events counted from zero, the service's bus events logged, dropped errors logged with keyless causes, readiness without internal text, retries with jittered backoff and a total bound, the warning for a link without TLS (point 3), configuration bounds, JWKS fetched ahead of its TTL, downloads' headers, the restore's store errors keyless; each error test injects sentinel keys, URLs, tokens and backend text and asserts their absence from the response and the log | pending |
| 5.3 — measurement | benchmarks for the SDK's observe path, ingest's dispose, batch, flush and acknowledgement, the catalog's registration, listing and reads on both engines, `QueryRecords` over an hour of N records in K packs (binary and JSON measured apart), merge and the compactor's passes; P4's predicates explained on both engines; `pprof` on the metrics listener, opt-in; the baselines and the offered rate of the write-connection case recorded here | pending |
| 5.4 — performance: the catalog and its writer | P1–P6, each measured before and after, each test on both engines: set-based registration, the overlap probe's index, the claim listing in bounded transactions that never forget a row from a chunk not yet listed, the confirmed indexes, listings bounded to their window with completeness unchanged (gaps beyond the window, a backward clock step, eviction, parked records), the API audit write's deadline and wait histogram (point 5) | pending |
| 5.5 — performance: ingest, reads, compactor, API | P7–P18 and P20 that 5.3 shows worth their change, each measured before and after; concurrent acknowledgements only with completion tracked by identity, not as a prefix; any key-path change keeps the binding-check and cancellation tests | pending |
| 5.6 — gate | audit every gate case against the test bodies, then one step per gap, as 4.6 did | pending |

## Phase 5.1 — correctness and liveness

Work on the branch `fix/phase-5/correctness`, one commit per step, test first, each catalog test on both engines;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Every step after the first adds the failing case its row names before its fix: a barrier, a test double or an injected fault where timing decides.

| Step | Scope | State |
|---|---|---|
| 1 | spec: C3's send timeout, C6, C7's limit (point 10), C11's one live incarnation per role, C15, C17, C19 (point 9) in T-9, `600-invariants.md` and `producer.md` §4.2, and `operations.md` §10.4 and the configuration guide for every new bound and key | pending |
| 2 | C1, one contract for the flush: every dependency on its path honours its context (the S3 client's response-header timeout, the catalog's queries, the working file's writes and its reopen); each step's deadline lies inside one total budget for a stop, shared by every batch still due; a flush returns only after its operations returned, so no detached worker keeps a batch's memory or files; any error, a deadline included, is an unknown outcome: the batch's unacknowledged messages are naked or left to redelivery, an object it may have put is left to reconciliation and the orphan pass, never deleted by the flush, and registration still precedes acknowledgement. Failing cases: each boundary blocked in turn; a put made durable whose answer is lost; a catalog commit whose answer is lost; a stop after part of the acknowledgement prefix; several batches due at a stop sharing the budget; each asserting acknowledgement eligibility, the objects kept, every message's disposition and every worker ended | pending |
| 3 | C2: `QueryRecords` steps only through the hours with a scope or an eviction, each read set read when the stream reaches it. Failing cases: a window of years over one capture in a bounded number of reads; a stream paused in an earlier hour while a later hour registers, then resumed; an eviction-only hour; a cursor resumed past an empty stretch; an object gone and its read set read again | pending |
| 4 | C3 and C17: a send that makes no progress for the configured time ends the stream and frees its slot and memory, while a slow client that keeps reading is untouched; the servers' idle timeouts; a restore holds a read slot and opens a bounded number of readers, refusing with `resource_exhausted`, no payload and no success audit when its copies exceed them. Failing cases: a stalled stream over HTTP/1.1 and HTTP/2 beside an active one, the client resuming from its last cursor; many equivalent copies with heavy metadata, a differing copy past the bound, copies in different hours, cleanup after every failure | pending |
| 5 | C4 and C5: `api` and `all` exit 0 on a stop signal, with an error still non-zero; readiness fails before the listener closes. Failing cases: the command's exit code under a cancelled context, and `/readyz` during a drain. The standalone leg asserts the unit inactive, not failed, with exit status 0; the cluster leg asserts exit code 0 of every terminated container after a rollout restart; both proven by a Packaging run on `main` at a recorded commit, as 4.6 did | pending |
| 6 | C6, C7, C9: an export attempt failed by a store error, so a later attempt under a new fence redoes it, and the failed one can never complete; a claim object read through a bounded retrieval: the declared length checked against the limit before any read, the read capped at the declared length, excess and truncation detected, the digest checked; due batches handed to the bounded final flush on a stop. Failing cases: a store read failing after earlier packs succeeded; an excessive declared size, acceptable metadata over oversized content, truncated content, a digest mismatch and a stalled object, with the double counting bytes read; a stop between a fetch and the due batches | pending |
| 7 | C8, verification first: `Stat`, `Open` and a ranged read against a missing object, a missing bucket and a denied access on MinIO and Ceph RGW; a store answering a missing bucket as a missing object fails as a store error, and if both answer `NoSuchBucket` the finding closes with that evidence | pending |
| 8 | C10, C11: retry closures reset their results, tested by an attempt that fails after filling them and a retry that returns another result; one live incarnation per role, enforced by the catalog (a unique key on the live role, partial on SQLite and on a generated column on MariaDB), a losing insert retried to read the winner, a different stream or creation time still refused, never adopted. Failing cases: two replicas of one role starting together, including the interleaving where one commits between the other's check and its id; two roles colliding on an id; a differing stream; each asserting one live row and one consumer name for every same-stream caller | pending |
| 9 | C12–C15 and C19, the SDK: a discarded rotation start released and skipped, not counted dropped; the `current_capture` gauge kept as state the sink applies, so an event lost to a full queue is repaired once the event producers have stopped and before `Close` returns; `Close` cancelling the publisher's and the lifecycle's waits, no capture installed after cancellation, the rotation's commit-point rule kept; a transient claim read retried, content proven wrong deleted and put again; the publisher's reads under a capture's lock bounded and merged into one a pass. Failing cases: a discarded start and the drop counter; a blocked sink with a full queue, a capture ended, then the sink released and the producer closed, the gauge absent; publish, claim and rotation waits cancelled one at a time; a read error versus a short object; a callback's progress while the publisher's read is paused at a barrier | pending |
| 10 | C16, C18: the testbus retry over test doubles: "peer offline" then success, a deadline then success, a permanent error, exhaustion and cancellation; the R3 consumer created through the retry; then 20 consecutive clustered runs as supplementary evidence; the simulator's rate bounded and a timed replay's end a success | pending |

## The audit

Read 2026-11-01 at e3b9722 by five read-only passes (the SDK; ingest, bus and store; the catalog; compactor, records and API; auth, keys, service and commands),
with `go vet` and the SDK's `-race` tests clean, and the catalog's costs measured on SQLite on an idle machine;
then reviewed against the code by an external reviewer, which moved P20 to C19, rewrote C11, added E19 and corrected the references below.
No message is acknowledged before its data is durable;
one invariant of `600-invariants.md` is exceeded, the callback's wait (C19).
Severity is high (H), medium (M) or low (L); *spec* marks a finding whose fix changes a spec.

### Correctness and liveness

| Id | Finding | Where | Sev |
|---|---|---|---|
| C1 | a stalled S3 put or catalog call blocks an ingester and its stop without bound: no deadline on a flush step, the final flush unbounded, the S3 client without a response timeout | `internal/ingest/flush.go:249`, `ingester.go:148`, `internal/store/s3store/s3store.go:71` | H |
| C2 | `QueryRecords` reads every hour of its window for each capture, a read set per hour, so one request over a wide window runs millions of catalog transactions holding a read slot | `internal/records/records.go:129`, `internal/catalog/readset.go:42` | H |
| C3 | a client that stops reading a stream holds its read slot and up to its held bytes without bound; the servers set no idle timeout (*spec* for the send timeout) | `internal/api/query.go:553`, `internal/service/api.go:79`, `internal/metrics/server.go:108` | M |
| C4 | `api` and `all` exit 1 on a normal stop signal, so the systemd unit ends `failed` | `internal/service/api.go:181`, `cmd/tracekeep/main.go:143` | M |
| C5 | readiness stays green while the API drains, so traffic still reaches a closed listener | `internal/service/api.go:127`, `internal/service/service.go:286` | L |
| C6 | a transient store failure during an export leaves the pack out of a `done` bundle (*spec*) | `internal/api/export_bundle.go:300` | M |
| C7 | a claim object is read whole before its declared length is checked, so one oversized object can exhaust each replica's memory in turn (*spec* for the limit) | `internal/ingest/ingester.go:561`, `deliver.go:250` | H |
| C8 | an unclassified 404 reads as a missing object, and a HEAD carries no `NoSuchBucket` body, so a missing bucket may read as missing packs; to be shown on MinIO and Ceph RGW before any fix | `internal/store/s3store/s3store.go:131`, `s3store.go:259` | M |
| C9 | on a stop, due batches flush under the cancelled context: naked, or registered and left unacknowledged, then written again | `internal/ingest/ingester.go:180` | L |
| C10 | two retried transaction closures keep the previous attempt's results | `internal/catalog/exports.go:180`, `exports.go:384` | L |
| C11 | nothing keeps one live incarnation per role: `Incarnate` checks for a live row, then allocates `MAX+1` and inserts, with no unique key on the live role, so two replicas of one role starting together on MariaDB can each record one and use different consumer names | `internal/catalog/incarnation.go:42`, `migrations/mariadb/20261026092450_init.sql:14` | M |
| C12 | the SDK counts a discarded rotation start as an abandoned drop and can fire `TracepubDropping` with nothing lost | `producer/publisher.go:313` | M |
| C13 | the SDK's lifecycle events go through a lossy queue, and its sink loop exits on `Close` without draining, so a stopped device can keep `current_capture` set | `producer/lifecycle.go:314`, `lifecycle.go:322` | M |
| C14 | the SDK's `Close` waits out a publish, claim or rotation timeout it could cancel | `producer/publisher.go:369`, `producer/lifecycle.go:382` | L |
| C15 | the SDK deletes and re-puts a claim object after a transient read error (*spec*) | `producer/publisher.go:513`, `docs/specs/tracekeep/producer.md:261` | L |
| C16 | the clustered testbus retries only a deadline, not a placement's "peer offline", and one test creates its R3 consumer without a retry | `internal/testbus/testbus.go:279`, `internal/compact/gate_test.go:161` | M |
| C17 | a restore opens every copy's reader and merges without a read slot, under tracepack's default 512 MiB budget (*spec*) | `internal/api/restore.go:168`, `restore.go:202` | M |
| C18 | the simulator panics on an extreme rate, and a timed replay exits 1 | `internal/sim/replay.go:83`, `cmd/tracekeep-sim/main.go:112` | L |
| C19 | the publisher reads each capture's reservation state under its lock twice a pass, so a callback can wait on the publisher, which `600-invariants.md` and T-9 do not allow (*spec*) | `producer/publisher.go:273`, `producer/internal/core/capture.go:261`, `capture.go:277` | M |

### Error handling

| Id | Finding | Where | Sev |
|---|---|---|---|
| E1 | a client's cancellation or deadline answers `unavailable` and logs an error, polluting the alert (*spec*) | `internal/api/query.go:317`, `convert.go:23`, `transaction.go:60` | M |
| E2 | `FindTransaction` answers `unavailable` for an unknown capture and, forever, for a corrupt pack (*spec*) | `internal/api/transaction.go:39`, `transaction.go:111` | M |
| E3 | the service's bus connection sets no handlers: async errors go to stderr outside the log, disconnects unlogged | `internal/service/service.go:168` | M |
| E4 | the SDK counts observer panics but never reports them | `producer/internal/core/observe.go:201` | M |
| E5 | the SDK's labelled counters start at their first event, as `operations.md` §10.4 says, so `increase()` misses a first drop after a restart (*spec*) | `producer/publisher.go:734`, `docs/specs/tracekeep/operations.md:98` | M |
| E6 | the SDK's lifecycle events are dropped without a count when the sink lags | `producer/lifecycle.go:314` | L |
| E7 | a parking publish's error is dropped; a lasting failure shows only as deferrals | `internal/ingest/park.go:104` | L |
| E8 | the export cleanup logs a failed delete without its cause; the cause it adds must be keyless | `internal/compact/exports.go:41` | L |
| E9 | the dev IdP's serve error and the environment's close error are dropped | `internal/service/devsignin.go:115`, `cmd/tracekeep/main.go:133` | L |
| E10 | `/readyz` returns the checks' internal error text on the unauthenticated metrics port | `internal/metrics/server.go:86` | L |
| E11 | a retryable transaction re-runs 16 times at once, up to about 80 s on SQLite, without jitter | `internal/catalog/tx.go:94` | L |
| E12 | the bus URL and the S3 endpoint are accepted without TLS and without a word (point 3: warned, not refused) | `internal/config/config.go:232` | M |
| E13 | the OIDC leeway and the session and login lifetimes have no upper bound | `internal/config/auth.go:54` | L |
| E14 | JWKS expire hard and refetch synchronously, so every request waits and an IdP down at that moment refuses all (*spec*) | `internal/auth/oidc.go:278`, `docs/specs/tracekeep/api.md:364` | M |
| E15 | `/download/` sends no `Content-Disposition`, ETag or `Last-Modified` | `internal/api/download.go:65` | L |
| E16 | `/auth/me` without a cookie logs a warning and counts a failure on every poll; closed, as `api.md` §10.2 asks for it (point 2) | `internal/auth/bff.go:158` | — |
| E17 | a refusal tracekeep writes itself carries a raw epoch hour | `internal/api/transaction.go:102` | L |
| E18 | `operations.md` §10.4's drop reasons lack the SDK's `abandoned` | `docs/specs/tracekeep/operations.md`, `producer/publisher.go:50` | L |
| E19 | a restore opens copies outside the records' keyless wrapper, so a store error quoting an object key reaches the response and the log | `internal/api/restore.go:229`, `restore.go:87`, `internal/records/records.go:528` | M |

### Performance

| Id | Finding | Where | Sev |
|---|---|---|---|
| P1 | a pack's registration runs about four statements per record under the write connection: 27 µs a delivery, 1.3 s for 50k | `internal/catalog/register.go:410`, `register.go:569` | H |
| P2 | the conflict probe scans the capture's earlier ranges: 14 ms a range at 200k | `internal/catalog/register.go:519`, `resolve.go:92` | H |
| P3 | the claim listing is recorded in one write transaction, about 6 s at a million objects every retention cycle | `internal/catalog/claims.go:47`, `internal/compact/retention.go:137` | M |
| P4 | predicates that may lack an index, to be explained on both engines in 5.3: `audit_log` by capture and by actor, `scope_events` by time in the prune, `merges` by deletion time, `conflicts` by either pack | `internal/catalog/resolve.go:170`, `resolve.go:191`, `prune.go:25`, `delete.go:29`, `evict.go:229` | M |
| P5 | listings run 6–8 queries a capture over its whole history, and long snapshots hold SQLite's WAL back | `internal/catalog/reads.go:166`, `completeness.go:182` | M |
| P6 | an API write waits behind ingest in the pool without bound or measurement (point 5) | `internal/catalog/catalog.go:181`, `resolve.go:141` | M |
| P7 | ingest makes three catalog reads a message | `internal/ingest/deliver.go:187`, `deliver.go:231` | M |
| P8 | acknowledgements run one at a time, the caller reading them as a prefix; a parked copy costs three round trips (*spec* to batch) | `internal/ingest/flush.go:161`, `ingester.go:463`, `park.go:120` | M |
| P9 | batches due together flush one after another | `internal/ingest/ingester.go:453` | L |
| P10 | per-tick and per-message formatting, a quadratic copy of due batches, a batch overshooting its bytes by a fetch | `internal/ingest/ingester.go:231`, `ingester.go:454` | L |
| P11 | `fs` syncs every parent directory on a put; `s3` opens with a HEAD before its ranged reads | `internal/store/fs.go:225`, `internal/store/s3store/s3store.go:120` | L |
| P12 | a read decodes the capture's configuration again for every hour | `internal/api/query.go:72`, `export_bundle.go:278` | M |
| P13 | a seq range is applied after merging and cloning every record of the hour | `internal/records/records.go:223`, `records.go:360` | L |
| P14 | under JSON each record item is marshalled once to measure it and again to send; under binary its size is computed twice | `internal/api/query.go:530` | L |
| P15 | the conflict check merges both packs whole under a 512 MiB budget | `internal/compact/conflicts.go:88` | L |
| P16 | the orphan pass reads the catalog twice for every stored object every cycle | `internal/compact/orphans.go:121` | M |
| P17 | a scope with a damaged input is merged again every cycle, with no cap per cycle (*spec*) | `internal/compact/merge.go:68` | L |
| P18 | every TLS handshake reads the certificate, key and CA files under one mutex | `internal/service/tlsfiles.go:51` | M |
| P19 | every masked record takes the keys' process-wide mutex and a label lookup, and derives with HKDF (*spec* to cache; point 2: not now) | `internal/keys/keys.go:291` | L |
| P20 | moved to C19 | | — |
| P21 | the capture key is derived once a record, not once a fetch batch (*spec*; point 2: not now) | `internal/ingest/` | L |
| P22 | the flush's reopen stays while go-secs exports no format minor; moving only the budget check before the writer saves nothing meanwhile, so it waits whole (point 4) | `internal/ingest/flush.go:325` | — |

### Known before the audit

K1, the first S1F3's `ordering-uncertain`, stays open (point 4).
The audit read two possible causes in go-secs, neither reproduced:
a Selected state stamped after `Open` returns, and a reply read before its request's write returns.
Only two benchmarks exist, the SDK's allocation and the fleet metrics', and nothing serves `pprof` (point 6).

## Gate cases and their owners

| Gate case | Owner |
|---|---|
| every C finding has a test that failed before its fix, named in its step | 5.1 |
| a store or catalog call that never answers ends an ingester's flush and its stop within the stop budget, with every message's disposition and every object kept as step 2 states | 5.1 |
| a client that stops reading frees its read slot within the send timeout, and a window of years reads only the hours with a scope or an eviction | 5.1 |
| `tracekeep all` stops with exit status 0, its unit inactive on the standalone leg and its containers' exit code 0 on the cluster leg, from a Packaging run on `main` at a recorded commit | 5.1 |
| the testbus retry passes its test-double cases, and the clustered family then passes 20 consecutive runs | 5.1 |
| every refusal answers its own code, and no response or log carries an object key, a store URL, a download token, a cookie, an authorization code, a secret, a clear payload or a store's error text, shown by sentinels injected into every error path 5.2 touches | 5.2 |
| every hot path has a benchmark and a recorded baseline | 5.3 |
| every performance change has its before and after, and no benchmark regresses beyond its recorded noise | 5.4, 5.5 |
| on SQLite under `tracekeep dev` ingesting at the offered rate 5.3 records, with the ingest lag not growing, at least 1,000 successful, audited `PackURL` calls after a one-minute warm-up answer within 2 s at the 99th percentile | 5.4 |
