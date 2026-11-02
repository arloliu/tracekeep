# Phase 5 — correctness, error handling and performance

Status: active (2026-11-02) — 5.3 in progress

The owner's next phase after the roadmap's milestones:
no new feature, but the service and the producer SDK made correct under faults, their failures reported, and their hot paths measured and made faster.
Its work comes from an audit of `main` at e3b9722 and its external review, recorded below as *The audit*;
each step cites its findings by id.
The design stays in `docs/specs/tracekeep/`:
a finding marked *spec* changes a spec, and its spec step comes first.
This plan only orders the work.

## Before it starts

The owner answered on 2026-11-01: every recommendation below is taken, except point 3's refusal;
points 9 and 10 came from the external review and were taken the same day.
The answers are T-52.

1. **Where phase 5 sits in the roadmap**:
   the milestone table is seed v21 §12, "moved unchanged", so a new row would break that sentence.
   Answered: a short section after the table, *Beyond the seed*, with a phase 5 row of the same columns.
2. **Which findings change a spec**:
   - taken: a store failure during an export fails the attempt, so the lease lapses and another attempt retries, instead of leaving the pack out of a `done` bundle (C6, `api.md` §10.1);
     `FindTransaction` answers `not_found` for an unknown capture and a non-retryable code for a corrupt pack (E2, `api.md` §10.1);
     a restore takes a read slot and bounds the readers it opens, refusing what it cannot compare whole rather than comparing part (C17, `api.md` §10.1, `masking-keys.md` §9);
     a scope whose merge keeps failing backs off, and a cycle merges at most a bounded number of scopes (P17, `compactor.md` §8.1 and §8.3);
     a transient claim read is retried, and only content proven incomplete or wrong is deleted and put again (C15, `producer.md` §4.2 *Uncertain uploads*);
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
   Answered: T-52 admits the publisher's reads, which hold no I/O, allocation, logging or call into go-secs, and T-9 gains "(superseded in part by T-52)" with its text kept;
   the invariant names those reads, the two reads of a pass merge into one, and the publisher's external waits stay outside the lock;
   a lock-free mirror of the reservation state would put the dependency barrier at risk for a wait the callbacks already accept from each other.
10. **A claim object's largest size (C7)**:
    no spec bounds a claim payload, and HSMS allows up to 4 GiB.
    Answered: `ingest.max_record_bytes`, default 64 MiB, the batch's default byte bound.
    The bound is a resource refusal, never a malformed record:
    after resolution, so a staged or terminated version still settles as today, an unresolved claim declared above the bound is deferred unfetched,
    its reason `too-large` logged and counted, until the operator raises the bound or terminates the version (`ingest.md` §6.1, `parking.md`);
    a read takes at most the declared length plus one byte, so content beyond the declaration is found and refused as a mismatch, as today.

Points 11 to 20 are 5.2's, read from the code at c4e73bb on 2026-11-02 and settled on the recommendations under the owner's standing instruction;
the owner may override any before 5.2's spec step lands, which records them as T-53.

11. **What a response may say about a dependency**:
    the gate keeps a store's error text out of every response and log, but most catalog failures answer `unavailable` with the catalog's whole error chain, a driver's host and port included,
    and `Restore`, `Digest`, `PackURL` and the export RPCs return catalog, key and audit failures with their text.
    Answered: an `unavailable` or `internal` answer of a `MachineService` RPC carries a fixed message naming what failed (the catalog, the store, a key provider, the audit), never an error's text;
    tracekeep's own refusals (a bound, an hour, a pack id, the details `PackSuperseded` and `RecordConflict`) keep their text.
    `AdminService` answers its operator, the reader of the operator's log, so its answer may carry what that log may (a catalog's cause included), never what it may not:
    a usage error stays `invalid_argument` and any other failure `failed_precondition` with its text, as today,
    except an audit failure, which the catalog's audit write tags `catalog.ErrAudit` wherever it runs (the pool or a transaction) and which answers `internal`, and a cancellation, which passes through as point 12 states.
    **What a log may carry**: the operator's log keeps a catalog's cause (a driver's text, a host), a bus error's text and a key provider's error with its reference, as `masking-keys.md` §9 allows;
    it never carries an object key, a store's error text (made keyless by `Keyless`), a URL's query or user information, a token, a password, a secret, a cookie, an authorization code or a clear payload,
    whichever error's text would carry it: a boundary that logs an error whose text may hold such material (a store's, an IdP fetch's) logs fields it builds itself, never the error's text.
    The tests assert the first list's sentinels absent from a `MachineService` response, the second's from every response, `AdminService`'s included, and from the whole log.
12. **A client's cancellation (E1)**, decided once per request, where its handler returns, in this order:
    a refusal tracekeep decides keeps its code, `QueryRecords`' send that made no progress included (`deadline_exceeded`);
    an audit failure stays `internal`;
    otherwise an `unavailable` or `unknown` answer given once the request's own context has ended becomes `canceled` or `deadline_exceeded` by that context, its outcome `cancelled`, counted, never logged as a failure;
    a dependency's own deadline leaves the request's context live and stays `unavailable`.
    `Restore` and `Digest`, which count their outcomes, apply the rule to their own error before counting and mark it decided; the interceptor applies it to every other handler's error and leaves a decided one as it is, so the code, the outcome and the log agree.
    `PackURL`'s and an export's URL failure (presigning or a download token) is a store's failure: an ended request context answers as above, a live one `unavailable` with point 11's message, never `internal`.
    A request cut by its replica's stop is counted and logged as `canceled`; its client, whose connection closes, receives nothing.
    **Downloads**, plain HTTP: a request whose own context ended answers nothing more and logs no failure, counted under the status 499;
    the download's own budget (its token's expiry plus the margin) or the store failing while the client is there answers 502 and logs one keyless line, as today, if no header has been sent;
    once the status is sent, it ends the transfer short of its length, which the client sees as a cut body, logs one keyless line and counts under the status already sent;
    the object is closed in every case.
13. **A pack that does not open (E2)**: `FindTransaction` answers `not_found` for a capture the catalog does not know,
    and `data_loss` for a pack that does not open as one of the capture (corrupt, not a pack, an unsupported format, another capture's), which a retry cannot cure, logged as an error;
    a pack the store cannot read, or missing, stays `unavailable`.
14. **Restore's read limit (carried from 5.1)**: a comparison past `api.records.held_bytes`, or a copy whose read meets tracepack's read limit (an error or an incompleteness of reason `read-limit`), answers `resource_exhausted`, never a comparison of part;
    a corrupt copy stays as today.
15. **An export's work file (carried from 5.1)**: a failure of the attempt's own work file (its creation, a write, its close, a full disk) fails the attempt as a store failure does,
    so no pack is left out for it and no reason names a local path;
    only the pack's own bytes (corrupt, not a pack, an unsupported format, a read limit) leave a pack out, and a bundle past its size bound is still refused for good.
16. **The SDK's counters (E4–E6, E18)**: every counter and every label value the SDK can emit is reported at zero when the publisher starts;
    an observer's panic is `observer_panics_total`, and the publisher logs one error per report in which it rose, never the panic's value;
    rotations and the log lines the sink has no room for (`log_lines_dropped_total`, a line still queued when the sink stops included) are counted by atomic adds where they happen, the lifecycle's lock held or not, never by a call into the integrator there;
    the publisher reports them with the rest, outside every lock.
    `Close` cuts off emission in this order: it stops the publisher, the lifecycle's workers and the fault worker, closes the bus connection it owns (an injected one's publisher only cleaned up, as today), and marks the producer closed;
    an emission adds itself to an in-flight count, checks that mark, enqueues or counts its drop, none of which blocks, and leaves the count; `Close` sets the mark, then waits for the count to reach zero,
    so every emission that saw no mark has settled before what follows, and one that sees it (a late callback, the unjoined first dial) is discarded uncounted, which `Close`'s documentation says;
    the sink then stops, counting every line still queued as dropped, and `Close` reports once more on its own goroutine, before the sink's last apply, so the counts it reports are final; a second or concurrent `Close` does none of it again.
    The SDK's own lines keep point 11's log rule: a bus callback's, a publish's and a claim object's failure is logged as fields the SDK builds (a NATS or JetStream error code, a context error, an HTTP status, else a class), never the error's text, which can hold a server URL with credentials, an object name or a backend's text;
    the fields are built outside every lifecycle and capture lock, and the SDK imports nothing of the service.
    No alert is added for panics; the runbook reads the counter.
    The chart's gateway rules keep their fallback for a gateway on an SDK before this change (no `producer/` tag is cut in this phase).
17. **The service's bus events (E3)**: logged (lost, restored, an async error, closed with an error), each server named redacted; no service metric is added.
18. **A link without TLS (point 3, E12)**: judged once at a role's start, `all` once for its roles: the bus by its connection's TLS state, so a `nats://` URL upgraded by the server is not warned,
    and the S3 store by an `http://` endpoint, an empty one (the provider's default, over TLS) not; not the catalog's DSN, not `admin` or `status`, not a reconnect.
19. **Bounds (E13)**: `auth.oidc.leeway` at most 5 min; session lifetime at most 24 h, login lifetime at most 1 h, the token exchange deadline at most 1 min;
    `jwks_ttl` at most 24 h, its minimum fetch interval at most 1 h and below the TTL; `keys.vault.timeout` at most 1 min;
    and 5.1's `ingest.stop_timeout` at most 10 min and `ingest.max_record_bytes` at most 4 GiB, HSMS's largest message.
    The older durations without a maximum stay as they are.
20. **Retries and fetches (E11, E14)**: a retried catalog transaction waits a random time up to a cap doubling from 10 ms to 1 s, under the caller's context;
    it makes at most 16 runs, with no wait after the sixteenth, and after each wait it stops once 30 s have passed since the first run began, returning the last error;
    a run in flight is never cut by that bound, only by the caller's context
    (on SQLite `busy_timeout`, 5 s, bounds each statement's wait for the lock inside a run, so under contention that holds the lock most of each wait the time bound comes before the count; neither bounds the whole call, which the caller's context does).
    Discovery and the JWKS: a request that finds them cached past half their TTL is answered from the cache and starts one fetch in the background, under the existing minimum interval;
    that fetch runs under the verifier's own context, never the request's, and is the one in flight a cold or unknown-key request shares;
    the verifier's `Close`, called by the API on every exit after it is built, cancels it and waits for it, and starts nothing after;
    a failure is logged with fields the verifier builds (the endpoint's scheme, host and path, an HTTP status by its number, a class: unreachable, timeout, status, malformed), never the error's text, and cached keys never verify past their TTL.

Points 21 to 29 are 5.3's, read from the code at 8a6203f on 2026-11-02 and settled on the recommendations under the owner's standing instruction;
the owner may override any before 5.3's spec step lands, which records them as T-54.

21. **Where a baseline comes from, and its noise**: the SDK, ingest, the API, the records reader, the compactor and the catalog on SQLite run on one local machine;
    the catalog on MariaDB runs in CI, by a workflow started by hand that takes a `base` and a `head` commit and benchmarks both in one job, so a later step's before and after share a runner.
    Every run is `-count 10`; the recorded noise of a benchmark is benchstat's interval at that count, before and after taken on the same machine in one sitting.
    The audit's timings stay as the record of 2026-11-01; the Baselines section below supersedes them.
22. **What a baseline records**: the commit, the Go version, the engine and its version, the core count and class (never a vendor's model), the filesystem the test's temporary directory is on,
    `-count`, `-benchtime` and `-cpu`, and benchstat's table; no host or runner name.
23. **Benchmarks stay alive**: every benchmark runs once at its small size in ordinary CI (`-benchtime 1x`, the catalog's on both engines),
    and its medium and large sizes only when `TRACEKEEP_BENCH_SCALE` names them, so no seed of a million rows runs in the test budget.
24. **Paths that change what they measure**: a registration benchmark seeds the capture's ranges to its size by bulk SQL and runs a fixed `-benchtime Nx`, each iteration a fresh pack above the seeded ones,
    so the overlap probe sees the size its row names; a merge reseeds its scope while the timer is stopped, or measures the writer over a fixed snapshot;
    the claim listing, the orphan pass and every read are idempotent and loop as they are.
25. **Registration with its evidence**: every registration fixture today passes empty evidence, which hides P1;
    the registration benchmark builds one identity per record and the receipts of claim-check records, and settles whether the audit's 27 µs a delivery was registration's (the scratch dispose benchmark measured the same figure).
26. **What 5.4 inherits for P4** (read from the code, to be confirmed by step 9's plans):
    `AuditEntries` by capture and by actor has no production caller, so P4's first two predicates may close without an index;
    the prune's floor (`MIN(seq) … WHERE at > ?`) and the deletable merges (a `CASE` over `delete_at`) need their queries rewritten before an index can serve them;
    the conflicts of an evicted hour need only the capture added to their predicate, which the primary key then serves;
    and the eviction's `merges` by capture and hour, outside P4, has no index.
    An EXPLAIN shows an index chosen, not rows read, so P2 and P5 are judged by benchmarks and, on MariaDB, by `ANALYZE`'s rows read; step 9 records plans and asserts none until 5.4 adds an index.
27. **The write-connection case** (point 7's gate, 5.4): one simulator is one capture, and the catalog's writes grow with captures times flushes, not with the message rate, so a one-tool run may never load SQLite's writer.
    A harness in `internal/service`, run only when `TRACEKEEP_LOAD` is set, starts `tracekeep dev` without its simulator and its own simulated tools on dev's bus, then sweeps the tool count and the rate at the default 10 s flush.
    A rate is sustained when the stream's backlog, read from the bus's stream state at each flush window, keeps a flat minimum and the achieved publish rate meets the offered one; the consumer's pending acknowledgements are recorded only as a saturation signal.
    The offered rate 5.4's gate uses is half the highest sustained rate at the recorded tool count.
    For the gate's `PackURL` calls the harness lists packs again as it goes and asks for the newest, since the compactor merges and supersedes them during the run.
28. **pprof**: `metrics.pprof`, default off, refused when `metrics.listen` is empty; its handlers registered on the metrics listener's own mux; `tracekeep dev -pprof`.
    The listener is unauthenticated and a profile shows the heap and the command line, so `650-perf-sec.md` and the configuration guide say to keep the listener reachable only by its operators while it is on.
29. **Shared fixtures**: the test helpers the benchmarks need take `testing.TB`; `catalogtest` gains a fleet seeder, a bulk-insert seeder, an `Explain` and an engine loop for benchmarks;
    the catalog's own benchmarks stay in package `catalog` on its `newCatalog`, since `catalogtest` imports `catalog`;
    and the queries step 9 explains move into unexported constants, so the harness explains the text that runs.

Not in this phase: new features, go-secs' changes (point 4), the eqp-hub side of phase G, and a `producer/` tag, which stays the owner's call.

## Phases

| Phase | Scope | State |
|---|---|---|
| 5.1 — correctness and liveness | C1–C19, each with its failing case, the spec first | done (#64) |
| 5.2 — error handling and signals | spec first (`api.md` §10.1–§10.2, `operations.md` §10.4, `650-perf-sec.md`, the configuration guide), then E1–E15, E17–E19: each error with its own code, the SDK's panics, drops and sink events counted from zero, the service's bus events logged, dropped errors logged with keyless causes, readiness without internal text, retries with jittered backoff and a bound on further runs, the warning for a link without TLS (point 3), configuration bounds, JWKS fetched ahead of its TTL, downloads' headers, the restore's store errors keyless; each error test injects sentinels and asserts their absence where point 11 forbids them; detailed below | done (#67) |
| 5.3 — measurement | benchmarks for the SDK's observe path, ingest's dispose, batch, flush and acknowledgement, the catalog's registration, listing and reads on both engines, `QueryRecords` over an hour of N records in K packs (binary and JSON measured apart), merge and the compactor's passes; P4's predicates explained on both engines; `pprof` on the metrics listener, opt-in; the baselines and the offered rate of the write-connection case recorded here | in-progress |
| 5.4 — performance: the catalog and its writer | P1–P6, each measured before and after, each test on both engines: set-based registration, the overlap probe's index, the claim listing in bounded transactions that never forget a row from a chunk not yet listed, the confirmed indexes, listings bounded to their window with completeness unchanged (gaps beyond the window, a backward clock step, eviction, parked records), the API audit write's deadline and wait histogram (point 5) | pending |
| 5.5 — performance: ingest, reads, compactor, API | P7–P18 that 5.3 shows worth their change, each measured before and after; concurrent acknowledgements only with completion tracked by identity, not as a prefix; any key-path change keeps the binding-check and cancellation tests | pending |
| 5.6 — gate | audit every gate case against the test bodies, then one step per gap, as 4.6 did | pending |

## Phase 5.3 — measurement

Work on the branch `test/phase-5/measurement`, one commit per step, the spec first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
A benchmark has no failing case: each uses `b.Loop` and `b.ReportAllocs`, takes the small, medium and large sizes its row names, runs once at its small size in CI (point 23), and has a row in *Baselines*.
Step 2 lands alone, since it changes every package's test helpers; steps 3 to 10 then touch disjoint packages.
A plan or a default this section states from reading the code, not from a run, is marked *to confirm*.

| Step | Scope | State |
|---|---|---|
| 1 | spec, with T-54 recording points 21 to 29: `operations.md` §10.4 (`/debug/pprof/` on the metrics listener when `metrics.pprof` is on), the configuration guide (`metrics.pprof`), `650-perf-sec.md` (pprof's exposure, point 28; the benchmarks' smoke run and scale knob, point 23) | pending |
| 2 | fixtures and tooling: the helpers point 29 names widened to `testing.TB` in `internal/ingest`, `internal/api`, `internal/compact`, `internal/catalog`, `internal/records` and the SDK's tests; `catalogtest`'s seeders, `Explain` and benchmark engine loop; the explained queries moved to constants; `internal/metrics/fleet_test.go` renamed `fleet_bench_test.go`; `scripts/catalog.sh bench` without `-race`, `make bench-smoke` and `make bench-catalog`; a `bench.yaml` workflow started by hand with `base` and `head` inputs, its benchstat table printed to the log and uploaded; CI's smoke run. Checked by the existing tests passing unchanged and the smoke run passing | pending |
| 3 | pprof (point 28) in `internal/config`, `internal/metrics` and `cmd/tracekeep`. Failing cases first: `/debug/pprof/` not found when off and served when on; `metrics.pprof` with an empty `metrics.listen` refused naming the key | pending |
| 4 | the SDK (`producer/`): the observe path (`core.Recorder.Observe`, data and transport event, payload 14 B, 1 KiB and 64 KiB, the ordinary lane drained each iteration), a callback through `secsobs` with 1 and 4 concurrent callbacks, and records published a second through the publisher on the in-memory bus; the existing `BenchmarkAllocate` kept. Measures the callback's budget of `650-perf-sec.md` | pending |
| 5 | ingest (`internal/ingest`): `Executor.Dispose` inline and claim-check, masking off and on (P7, P19, P21); `Batcher.Add` and `Due` over 1, 100 and 1,500 open batches (P10); `Flusher.Flush` over 100, 1,000 and 10,000 records, 14 B and 1 KiB, on the memory and the `fs` store (P11's `fs` puts); the acknowledgements on the embedded bus, inline, claim-check and a parked copy, 100, 1,000 and 10,000 messages (P8); the ingester's flush of several due batches (P9). The untracked scratch dispose and write-file benchmarks may seed these | pending |
| 6 | the catalog (`internal/catalog`), both engines: registration with populated evidence (point 25), 100, 1,000 and 50,000 deliveries a pack over 1,000, 20,000 and 200,000 prior ranges (P1, P2); `Resolve` under an active termination order (P2's second probe); the claim listing over 10,000, 100,000 and 1,000,000 objects (P3); `ListCaptures`, `ListDevices`, `ListPacks` and `Completeness` over 1, 100 and 1,500 captures with 24, 720 and 8,760 hours of history, a one-hour and a whole-history window (P5); an audit write while registrations run (P6); `ReadSet`, `NextReadHour`, `RecordCopies` and `KeyStanding` (P16's lookups) | pending |
| 7 | reads (`internal/records`, `internal/api`, `internal/store`, `internal/service`): the records reader over an hour of 1,000, 72,000 and 360,000 records in 1, 12 and 16 packs (16 is `max_readers`), disjoint and overlapping; `QueryRecords` over HTTP with a binary and a JSON client (P14), a 24-hour window (P12) and a narrow seq range in a large hour (P13); `fs.Put` by key depth (P11); the TLS files' load per handshake (P18) | pending |
| 8 | the compactor (`internal/compact`): the merge writer over 2, 12 and 64 inputs of 1,000 and 10,000 records, masked inputs at a fixed count below the merge's limit; the merge pass with failing scopes (P17); the conflict check over two overlapping packs of 1,000, 10,000 and 100,000 records (P15); the orphan pass over 1,000, 10,000 and 100,000 objects (P16); a retention cycle's claim listing (P3) | pending |
| 9 | the EXPLAIN harness (point 26), in package `catalog`: P4's five predicates, P2's two probes, P5's completeness inputs and the eviction's `merges` scan, on both engines; a small seed always, logging plans, and a large seed under `TRACEKEEP_EXPLAIN` that runs `ANALYZE` first and records SQLite's plans with and without statistics and MariaDB's rows read; no assertion until 5.4 | pending |
| 10 | the load harness (point 27) in `internal/service`: the sweep of tools (1, 10, 100) and rate, the sustained rate and the offered one; and the `PackURL` measurement 5.4's gate runs, at least 1,000 audited calls after a one-minute warm-up, each with its own 10 s deadline, recording p50, p99, failures, the backlog's minima and the achieved rate | pending |
| 11 | baselines: every benchmark at its three sizes locally and the catalog's on MariaDB through `bench.yaml`, step 9's plans and step 10's sweep, all recorded in *Baselines* below; a performance row 5.5 then shows not worth its change is closed there with its number (point 6) | pending |

## Baselines

Filled by step 11, one table per package, each row a benchmark and size with its time, bytes and allocations an operation and benchstat's interval,
under a line naming the run's commit and point 22's machine facts; then step 9's plans per query and engine, and step 10's sweep and offered rate.

Carried from 5.2 to the gate (5.6): a few catalog and compactor errors on paths 5.2 did not touch name an object key in the operator's log
(a claim survivor's release and removal, a registration's key lookups, an abandoned close's delete), which point 11 keeps out of every log.

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
| C5 | readiness stays green while the API drains, so traffic still reaches a closed listener | `internal/service/api.go:41`, `api.go:127`, `api.go:158` | L |
| C6 | a transient store failure during an export leaves the pack out of a `done` bundle (*spec*) | `internal/api/export_bundle.go:300` | M |
| C7 | a claim object is read whole before its declared length is checked, so one oversized object can exhaust each replica's memory in turn (*spec* for the bound and its deferral) | `internal/ingest/ingester.go:561`, `deliver.go:250` | H |
| C8 | an unclassified 404 reads as a missing object, and a HEAD carries no `NoSuchBucket` body, so a missing bucket may read as missing packs; to be shown on MinIO and Ceph RGW before any fix | `internal/store/s3store/s3store.go:131`, `s3store.go:259` | M |
| C9 | on a stop, due batches flush under the cancelled context: naked, or registered and left unacknowledged, then written again | `internal/ingest/ingester.go:180` | L |
| C10 | retried transaction closures keep a rolled-back attempt's results: `DeletePack` can return `true` from an attempt whose commit failed while its retry's guard refuses, so the compactor deletes an object no committed deletion authorized; the two export closures likewise | `internal/catalog/delete.go:55`, `delete.go:80`, `tx.go:107`, `exports.go:180`, `exports.go:384` | H |
| C11 | nothing keeps one live incarnation per role (*spec* for the migration and `catalog.md`): `Incarnate` checks for a live row, then allocates `MAX+1` and inserts, with no unique key on the live role, so two replicas of one role starting together on MariaDB can each record one and use different consumer names | `internal/catalog/incarnation.go:42`, `migrations/mariadb/20261026092450_init.sql:14` | M |
| C12 | the SDK counts a discarded rotation start as an abandoned drop and can fire `TracepubDropping` with nothing lost | `producer/publisher.go:313` | M |
| C13 | the SDK's lifecycle events go through a lossy queue, and its sink loop exits on `Close` without draining, so a stopped device can keep `current_capture` set | `producer/lifecycle.go:314`, `lifecycle.go:322` | M |
| C14 | the SDK's `Close` waits out a publish, claim or rotation timeout it could cancel | `producer/publisher.go:369`, `producer/lifecycle.go:382` | L |
| C15 | the SDK deletes and re-puts a claim object after a transient read error (*spec*) | `producer/publisher.go:513`, `docs/specs/tracekeep/producer.md:261` | L |
| C16 | the clustered testbus retries only a deadline, not a placement's "peer offline", and one test creates its R3 consumer without a retry | `internal/testbus/testbus.go:279`, `internal/compact/gate_test.go:161` | M |
| C17 | a restore lists every copy, then opens every copy's reader and merges without a read slot, under tracepack's default 512 MiB budget (*spec*) | `internal/catalog/copies.go:28`, `internal/api/restore.go:168`, `restore.go:202` | M |
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
| E12 | the bus URL and the S3 endpoint are accepted without TLS and without a word (point 3: warned, not refused; *spec* for `650-perf-sec.md`) | `internal/config/config.go:232` | M |
| E13 | the OIDC leeway and the session and login lifetimes have no upper bound | `internal/config/auth.go:54` | L |
| E14 | JWKS expire hard and refetch synchronously, so every request waits and an IdP down at that moment refuses all (*spec*) | `internal/auth/oidc.go:278`, `docs/specs/tracekeep/api.md:364` | M |
| E15 | `/download/` sends no `Content-Disposition`, ETag or `Last-Modified` | `internal/api/download.go:65` | L |
| E16 | `/auth/me` without a cookie logs a warning and counts a failure on every poll; closed, as `api.md` §10.2 asks for it (point 2) | `internal/auth/bff.go:158` | — |
| E17 | a refusal tracekeep writes itself carries a raw epoch hour | `internal/api/transaction.go:102` | L |
| E18 | `operations.md` §10.4's drop reasons lack the SDK's `abandoned` | `docs/specs/tracekeep/operations.md:72`, `producer/publisher.go:50` | L |
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
| P16 | the orphan pass runs up to two standing queries for every listed object every cycle | `internal/compact/orphans.go:148`, `internal/catalog/register.go:390`, `register.go:396` | M |
| P17 | a scope with a damaged input is merged again every cycle, with no cap per cycle (*spec*) | `internal/compact/merge.go:68` | L |
| P18 | every TLS handshake reads the certificate, key and CA files under one mutex | `internal/service/tlsfiles.go:51` | M |
| P19 | every masked record takes the keys' process-wide mutex and a label lookup, and derives with HKDF (*spec* to cache; point 2: not now) | `internal/keys/keys.go:291` | L |
| P20 | moved to C19 | | — |
| P21 | the capture key is derived once a record, not once a fetch batch (*spec*; point 2: not now) | `internal/ingest/deliver.go:374` | L |
| P22 | the flush's reopen stays while go-secs exports no format minor; moving only the budget check before the writer saves nothing meanwhile, so it waits whole (point 4) | `internal/ingest/flush.go:325` | — |

### Known before the audit

K1, the first S1F3's `ordering-uncertain`, stays open (point 4).
The audit read two possible causes in go-secs, neither reproduced:
a Selected state stamped after `Open` returns, and a reply read before its request's write returns.
Only two benchmarks exist, the SDK's allocation and the fleet metrics', and nothing serves `pprof` (point 6).

## Gate cases and their owners

| Gate case | Owner |
|---|---|
| every C finding has a test that failed before its fix, except C8, closed on the stores' evidence, and C19, proven by the SDK's conformance cases (#64) | 5.1 |
| a remote store or catalog call that never answers ends an ingester's flush and its stop within the stop budget, local file work that stalls is reported, the acknowledged prefix kept, every message not acknowledged left to redelivery, and every object it may have put kept for reconciliation (`ingest.md` §6.2) | 5.1 |
| a client that stops reading frees its read slot within the send timeout, and a window of years reads only the hours with a scope or an eviction | 5.1 |
| `tracekeep all` stops with exit status 0, its unit inactive on the standalone leg and its containers' exit code 0 on the cluster leg, from a Packaging run on `main` at a recorded commit | 5.1 |
| the testbus retry passes its test-double cases, and the clustered family then passes 20 consecutive runs | 5.1 |
| every refusal answers its own code, and no response or log carries what point 11 keeps out of it (an object key, a URL's query or credentials, a download token, a cookie, an authorization code, a secret, a clear payload, a store's error text; a `MachineService` response also no catalog or provider text), shown by sentinels injected into every error path 5.2 touches | 5.2 |
| every hot path has a benchmark and a recorded baseline: those of `650-perf-sec.md` and of steps 4 to 8, each with its row in *Baselines*; the offered rate and the plans of point 26 recorded there | 5.3 |
| every performance change has its before and after, and no benchmark regresses beyond its recorded noise | 5.4, 5.5 |
| on SQLite under `tracekeep dev` ingesting at the offered rate 5.3 records, with the ingest lag not growing, at least 1,000 successful, audited `PackURL` calls after a one-minute warm-up answer within 2 s at the 99th percentile | 5.4 |
