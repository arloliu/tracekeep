# Phase 5 — correctness, error handling and performance

Status: active (2026-11-03) — 5.5 next

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

Points 21 to 29 are 5.3's, read from the code at 8a6203f on 2026-11-02 and settled on the recommendations under the owner's standing instruction and amended by the plan's external review;
the owner may override any before 5.3's spec step lands, which records them as T-54.

21. **Where a baseline comes from, and when a change regressed**: the SDK, ingest, the API, the records reader, the compactor and the catalog on SQLite run on one local machine;
    the catalog on MariaDB runs in CI, by a workflow started by hand that takes a `base` and a `head` commit and benchmarks both in one job.
    Every run is `-count 10`; benchstat at a pinned version reports each comparison, and the decision is the change's own interval,
    a 95% percentile bootstrap (10,000 resamples) of the ratio of head's median to base's, computed by a small tool step 2 adds beside the benchmarks, for each metric on its own:
    - **repeatability** r is `abs(median(base₂) / median(base₁) - 1)` over two runs of the base, after a rate is turned into a time; the **allowance** T is 5% plus r;
    - time and bytes an operation **pass** when the interval's upper bound is at most 1 + T, **regress** when its lower bound exceeds 1 + T,
      and are **inconclusive** otherwise, measured again with a longer `-benchtime` or more runs, never passed;
    - allocations an operation are exact when every sample of both base runs has the same count, so any increase regresses, a zero base included;
      when the base's own samples differ (a driver's allocations vary from run to run), they are judged as time and bytes are; bytes from a zero base are exact;
    - a rate (records a second, step 4) is judged as its inverse, a time;
    - a row missing on either side, or run with other parameters or on another engine, **fails**.
    Each sitting measures its own repeatability (base against base), so a MariaDB job and a local sitting judge only their own before and after;
    the raw samples stay with the run (the workflow's artifact, the local sitting's own files), and the plan records benchstat's tables.
    The audit's timings stay as the record of 2026-11-01; the Baselines section below supersedes them.
22. **What a baseline records**: the commit; the Go and benchstat versions; the engine and its version;
    SQLite's journal mode and `synchronous` level, and MariaDB's `innodb_flush_log_at_trx_commit` and the storage class of its data directory;
    the core count and class (never a vendor's model); the filesystem classes of the test's temporary directory, the flush's working file and the `fs` store's root;
    `-count`, `-benchtime` and `-cpu`; and benchstat's table, each row saying whether its time includes a synchronization to disk. No host or runner name.
23. **Benchmarks stay alive**: every benchmark runs once at its small size in ordinary CI (`-run '^$' -bench . -benchtime 1x`, without the race detector),
    the catalog's on both engines, MariaDB's run failing rather than skipping when the engine is missing (`make bench-smoke`, and `scripts/catalog.sh bench-smoke` in the catalog job);
    its medium and large sizes run only when `TRACEKEEP_BENCH_SCALE` names them, and the fleet benchmark that exists today takes its small size from that knob too.
24. **Each benchmark states its iteration**: its starting state, what one iteration does, what is reset outside the timer, and the work it asserts each iteration, so every iteration does the same work:
    - registration: a fresh pack whose ranges lie above the seeded ones, at a fixed `-benchtime Nx` where N times the pack's ranges is at most a tenth of the seeded ranges, so the probe's population grows by at most a tenth;
      the same N on both sides of a comparison, the ranges before and after recorded with the result;
      a probe that hits does so against a distinct seeded overlap each iteration, asserting the conflicts each adds;
      the overlap probe and the evidence's processing are measured apart (a pack of one range and many deliveries; many ranges and one delivery), and a probe that finds an overlap apart from one that misses near the end of a large history;
    - the claim listing: a steady listing of objects already recorded, apart from a listing whose objects are all new and one whose recorded objects have all disappeared, each reseeded outside the timer,
      attributed and unattributed objects apart, asserting the rows added and removed;
    - the orphan pass: a steady pass over registered objects, apart from a pass that adopts its objects, whose new unknown objects are seeded and sighted once by an untimed pass, asserting each timed pass adopted them all;
    - `Batcher`: a fresh batcher each iteration, filled by `Add` and emptied by `Due`, asserting the batches `Due` returns;
    - a flush: fresh entries with fresh seqs each iteration, the entries copied outside the timer, since the splitter sorts them in place, under registration's bound on the ranges it adds, the catalog's ranges and packs before and after recorded;
    - a merge: its scope reseeded outside the timer, or the merge writer over a fixed snapshot;
    - P4's pruning, deletion selection and eviction: their population restored outside the timer, asserting the prefix pruned, the candidates selected and the scope evicted;
    - every read and lookup: idempotent, asserting the count it returns.
25. **Registration with its evidence**: the fleet fixture the metrics benchmark uses today registers with empty evidence, which hides P1;
    the registration benchmark builds one identity per record and the receipts of claim-check records, and settles whether the audit's 27 µs a delivery was registration's (the scratch dispose benchmark measured the same figure).
26. **What 5.4 inherits for P4** (read from the code, to be confirmed by step 9's plans and step 6's timings), as candidates for the measurements to choose among, not settled rewrites:
    `AuditEntries` by capture and by actor has no production caller, so P4's first two predicates may close without an index;
    the prune's floor (`MIN(seq) … WHERE at > ?`) may want an index on `at`, a walk of the seq prefix, or both, under `catalog.md`'s prune by seq prefix (a backward clock step, no younger event);
    the deletable merges wrap their deadline in a `CASE`, which no index serves as written;
    the conflicts of an evicted hour may want the capture in their predicate, which the primary key's prefix serves, measured with many conflicts in one capture;
    and the eviction's `merges` by capture and hour, outside P4, has no index.
    An EXPLAIN shows an index chosen, not rows read, so P2 and P5 are judged by benchmarks and, on MariaDB, by `ANALYZE`'s rows read; step 9 records plans and asserts none until 5.4 adds an index.
27. **The write-connection case** (point 7's gate, 5.4): the writer's occupancy grows with the deliveries a flush registers (each resolved, each claim-check one adding two inserts) and with captures times flushes,
    so one simulated tool may not load SQLite's writer the way a fleet does.
    A harness in `internal/service`, run only when `TRACEKEEP_LOAD` is set, starts `tracekeep dev` without its simulator and K simulated tools on dev's bus,
    each its own producer and loopback with its own tool id, dev's subject prefix and claim bucket and an explicit T3, the harness counting the forwards it makes.
    Each tool's loopback answers every request and forwards plain data (no withholding, process program or thinning), so a forward makes two records, its primary and its reply;
    the offered rate is the forwards a second of all K tools together, split evenly, and a window's records are the stream's records published in it.
    Its units: forwards a second offered and made; records a second published (the stream's last sequence) and dropped (the producers' counters);
    and the producers' queues, read from their own gauges through a `Metrics` the harness gives each producer: the ordinary ring's depth (records held by the publisher until released included) and the reserved lane's.
    A sweep point runs a one-minute warm-up and then twelve 10-second windows, sampling the stream's backlog and the producers' queues every second, its counters taken as deltas from its own start;
    it is **sustained** when the made rate is within 2% of the offered one, nothing is dropped, and the least-squares slope of the windows' backlog minima,
    and of the windows' maxima of the producers' queued records, each stays below 0.5% of a window's mean published records a window (so a rise of 1% a window fails), and **not sustained** otherwise.
    Between points the tools stop, the backlog drains to zero and every producer's queues are empty.
    The sweep runs K of 1, 10 and 100 at doubling rates; the offered rate is half the highest sustained rate at K = 100, run again for sixty windows and shown sustained there under the same rule, with K, the flush and the rate recorded.
    The gate's `PackURL` measurement holds the same rule over its own windows, so ingest stays sustained while it runs, and spans at least ten flushes the ingester completed, counted, not inferred from the pacing.
    Its calls run one at a time, paced at ten a second, each attempt with its own 10 s deadline, on the newest pack listed at most a second earlier; a call's latency is its RPC attempt's, the listing and the audit check outside it.
    A call counts when it succeeds and its audit row exists. A `pack-superseded` refusal, the audit written or not, lists again and retries at most three times, reported with the attempts as its denominator;
    more than 5% of attempts superseded fails the run, and any other error, a deadline included, fails the run, its duration kept.

28. **pprof**: `metrics.pprof`, default off, refused when `metrics.listen` is empty; its handlers registered on the metrics listener's own mux, which `Env.Observe` builds; `tracekeep dev -pprof`, set where dev sets its metrics address.
    The listener is unauthenticated and a profile shows the heap and the command line, so `650-perf-sec.md` and the configuration guide say to keep the listener reachable only by its operators while it is on.
29. **Shared fixtures**: the test helpers the benchmarks need take `testing.TB`, among them the service's certificate and client helpers and the `fs` store's `newFS`;
    the catalog's benchmarks and step 9's harness stay in package `catalog`, on `newCatalog` and in-package seeders (bulk inserts in one write transaction), since `catalogtest` imports `catalog`;
    other packages seed through `catalogtest`, which gains a fleet seeder and an engine loop for benchmarks.
    A seeded population is explicit: descriptors and configurations that decode; for listings, one tool selecting the size's captures; for completeness, parked, evicted, lost and conflicting inputs in stated shares;
    each seeder asserts what it seeded. The queries step 9 explains move into unexported constants, so the harness explains the text that runs.


Points 30 to 38 are 5.4's, read from the code at 1c0e5f2 on 2026-11-02 against the *Baselines* and settled on the recommendations under the owner's standing instruction (a merged proposal of two independent reads, Codex's and Claude's);
the owner may override any before 5.4's spec step lands, which records them as T-55.

30. **The range probes' bound (P2)**: a capture's `pack_ranges` probes (`overlapQuery` `register.go:501`, `heldSeqQuery` `resolve.go:72`, `RecordCopies` `copies.go:28`) bound `seq_from` from above only,
    so on `pack_ranges_capture_seq` they read every earlier range of the capture (MariaDB reads 200,000 for one probe; 13.9 ms a range, `Resolve` 10.4 ms and `RecordCopies` 27 ms at 200k).
    `pack_ranges` gains a virtual generated column `span = seq_to - seq_from` and an index `pack_ranges_capture_span (capture_id, span)`;
    each probe adds `r.seq_from >= first - m`, where `m` is the capture's largest stored span (`MAX(span)` for the capture, one seek of that index), which loses no range:
    a range overlapping `[first, last]` has `seq_to >= first` and `seq_to - seq_from <= m`.
    The engine maintains the column on every insert, so an older writer during a rollout (`catalog.md` §7.4, expand-then-contract) keeps it true, no backfill runs, and no seeder maintains it;
    an eviction lowering `m` is correct, since `m` is the largest span still stored. `seq_from` and `seq_to` are signed `bigint`, so `first - m` below zero only widens the bound when SQL computes it; Go's seqs are `uint64`, so a bound computed in Go floors at zero before it subtracts, and a capture with no range probes nothing.
    `m` and the probe are read in one statement or one snapshot, so a range inserted between them is never missed: `Resolve`'s probe, which runs on the read pool without a snapshot (`resolve.go:61`), takes a snapshot or folds `m` into its statement.
    The bound makes a bounded index range possible, not chosen: step 9 accepts it by measurement, the probes' plans on both engines before and after `ANALYZE` and MariaDB's rows read, beside their times.
    `heldSeqQuery` becomes `EXISTS`. If step 3 finds the generated column unsupported by Atlas's `schema.hcl` or by either engine's online `ALTER`, the fallback is an index on `(capture_id, seq_to, seq_from)`, which bounds only probes near the end, and the step says so.
31. **Registration's evidence in sets (P1)**: one transaction a pack, as now, with every outcome, its precedence and its tombstone reason unchanged (`register.go:410–437`);
    the per-delivery reads become: the capture's `parked` rows over the evidence's seq span in one read on the primary key's prefix (`capture_id = ? AND seq BETWEEN ? AND ?`), matched to the deliveries by version in Go,
    which serves both the resolution's and `readVersionState`'s read; the capture's active termination orders read once, a delivery covered when its incarnation has an order whose cut-off is at or above its stream seq;
    a covered delivery's held probe through point 30's bound in the registration's own transaction; and `stageParked` updating only the versions that read found `parking` or `parked`, which the capture row keeps unchanged meanwhile.
    The inserts of ranges, losses, receipts and claim objects are multi-row, each statement within 900 parameters; nothing commits a chunk on its own.
32. **The claim listing in bounded transactions (P3)**: `RecordListing` keeps its signature; it reads the recorded claim and unattributed keys on the read pool, and writes only what the listing adds,
    in transactions of at most 1,024 objects, each checking the lease, multi-row; a steady listing writes nothing
    (on MariaDB the listing now costs about 240 µs an attributed object and 480 µs an unattributed one, a statement a round trip, about 4 and 8 minutes at a million).
    The unattributed sweep runs only after every chunk committed, paged by key, each page deleting rows absent from the whole listing or attributed since, under the lease;
    a failure anywhere before it skips the sweep, so a key in a later chunk is never forgotten, and the rows added so far stay (each is idempotent).
    Attributed rows absent from a listing stay, as now.
    A listing that writes nothing still verifies the lease (`VerifyLease`, `lease.go:83`), so a stale holder gets `ErrLeaseLost` as it does now (`claims.go:47`);
    the count returned is the claim rows the committed chunks inserted, and zero with the error on any failure, as now; a retry inserts only what is still missing.
33. **Deletion and eviction (P4)**, the measured choice among point 26's candidates, which it supersedes:
    the deletable merges' `CASE` deadline splits into two sargable branches (`delete_at <> 0 AND delete_at <= now`, `delete_at = 0 AND at <= now - D`) over a new `merges (outcome, delete_at, at)` index, legacy zero deadlines keeping their process-D fallback;
    eviction selects an hour's conflicts by two indexed joins over new `conflicts (capture_id, pack_id)` and `conflicts (capture_id, other_pack_id)`, and its merges by a new `merges (capture_id, hour)`, deleting by key in batches, `EvictHour` staying one transaction;
    both accepted as point 30's is, by their plans before and after `ANALYZE`, MariaDB's rows read and their times.
    Closed here with their numbers: the audit listings by capture and by actor (no production caller; MariaDB's scans 114 and 37 ms; an index would tax every audit append),
    and the prune (237 ms at a million events is mostly its 500,000 deletes, not the floor's 16 ms scan), measured again after P1–P3.
34. **Listings batched, not windowed (P5)**: `Completeness` answers a whole capture (`completeness.go:86`, `catalog.md` §7.3), so no time window clips its inputs without a contract change.
    `ListPacks` and `ListDevices` read the seven inputs for chunks of 128 captures at a time (`capture_id IN`), in one snapshot, classifying outside it where the buffered input allows,
    `ListDevices` keeping its early answer for a device with a gap (on MariaDB a listing over 100 captures now costs about 2.4 ms a capture). Maintained coverage summaries wait until the batched listing still costs.
    The "windowed" `Completeness` benchmark passes no window (`reads_bench_test.go:136–147`): it is fixed or dropped, and the listing benchmarks assert the complete answer, not counts.
    A chunk of one capture reads its inputs with `= ?` and without the key column; `classify` orders the losses, crash gaps and conflicts itself, so no input query sorts them for it.
35. **The API's audit write (P6)**: `Catalog.Audit` takes its connection explicitly and records the wait for it in `catalog_audit_wait_seconds`, a histogram labelled `engine` and `outcome` (`acquired`, `deadline`, `canceled`, `error`), timed from the request for the connection to its acquisition or failure, not the insert; its failure wraps `ErrAudit`.
    Every standalone audit an API request makes runs under a 1 s deadline within the caller's own, through one catalog helper, `Catalog.AuditWithin`:
    the four handlers' (`PackURL`, `Digest`, `Restore`, and `GetExport`'s URL, `export.go:124`) and an operator read's, which `admin.Run` writes after its operation (`run.go:43`) for `AdminService` and the CLI alike;
    the deadline bounds the audit stage only, never the operation before it, and an audit written inside a domain transaction is untouched, as is ingest's (`deliver.go:475`).
    Each failure is answered as now (`ErrAudit`, `internal`, T-53, also when the parent's deadline expires first), `Restore` still clearing its recovered bytes, and no URL, payload or command result returned;
    no priority over ingest, since the gate's `PackURL` already holds at 5.78 ms p99.
36. **Schema in one step**: point 30's generated column, then its index and point 33's four indexes, are two migrations per engine, each generated from `schema.hcl` by `scripts/catalog.sh diff` with `schema.hcl` committed at each stage, landed before any code step, with no backfill and no hand-edited statement.
    Two, because Atlas 1.3.0 groups a table's added column and its index into one `ALTER TABLE`, which MariaDB refuses to run online for a virtual column, and on SQLite rebuilds the table for a new column in an index;
    apart, MariaDB adds the virtual column in place and the indexes by the server's default, which takes no table lock for an index-only `ALTER` (*to confirm* by step 3, which runs the second migration while inserts naming the old columns go on and inspects the generated SQL),
    and SQLite adds the column by `ALTER TABLE ADD COLUMN` and the indexes by `CREATE INDEX` (a SQLite catalog is never migrated beside a running service, `catalog.md` §7.4, so a rebuild there would cost time, not correctness);
    old writers name their columns (`register.go:450`), so the column breaks no insert; no new index holds a wide object key; `utf8mb4_nopad_bin` unchanged.
37. **Before and after**: 5.4's base is the branch after step 2, which adds the benchmark rows the changes need (a parked share in the evidence, many captures with few probes, few due merges among many, a held probe far from the end), so both sides run them; a local sitting runs base, head and base again for every row 5.4 touches (P1–P6 and the rows the inconclusive list names among them),
    and `bench.yaml` runs the catalog's on MariaDB with the same base and head; a regression under point 21 fails the step.
    `bench.yaml` as 5.3 left it cannot finish one pass in its 180 minutes (*Baselines*), so step 2 narrows it before any comparison:
    only the packages whose benchmarks reach MariaDB (`internal/catalog`, `internal/metrics`), only their `mariadb` rows (`TRACEKEEP_BENCH_ENGINES=mariadb`, read by `benchEngines` and `catalogtest.BenchEngines`, which skip any other engine's row),
    and one job per group of benchmarks, each running base, head and base: registration first and alone (`benchjudge -require`s its MariaDB shapes, and its fixed-N pass has never completed on MariaDB),
    then the claim listing, the listings, the lookups and `Resolve`, the audit, prune, deletion and eviction, and the metrics fleet;
    at `medium`, each job within the hosted runner's 360-minute limit (*to confirm*: step 2 records each job's time, and a group that does not fit is split again, never run with fewer samples).
    `scripts/catalog.sh bench` takes the group's packages and `-bench` expression, and appends registration's fixed-N passes only in the registration group; `benchjudge -require`s registration's shapes only there;
    each matrix job uploads its samples under its own name and records its rows; the test and smoke commands keep their package list.
    Step 2 also adds an eviction benchmark with many unaffected merges and a small chain of merges in the evicted hour, restoring the same population outside the timer and asserting the merges and inputs kept and deleted by identity.
38. **The gate's write-connection case** (point 27) runs in step 9, after every production change; any earlier run is preliminary: 1,000 audited `PackURL` calls at the offered rate, p99 within 2 s, enforced.

Not in this phase: new features, go-secs' changes (point 4), the eqp-hub side of phase G, and a `producer/` tag, which stays the owner's call.

## Phases

| Phase | Scope | State |
|---|---|---|
| 5.1 — correctness and liveness | C1–C19, each with its failing case, the spec first | done (#64) |
| 5.2 — error handling and signals | spec first (`api.md` §10.1–§10.2, `operations.md` §10.4, `650-perf-sec.md`, the configuration guide), then E1–E15, E17–E19: each error with its own code, the SDK's panics, drops and sink events counted from zero, the service's bus events logged, dropped errors logged with keyless causes, readiness without internal text, retries with jittered backoff and a bound on further runs, the warning for a link without TLS (point 3), configuration bounds, JWKS fetched ahead of its TTL, downloads' headers, the restore's store errors keyless; each error test injects sentinels and asserts their absence where point 11 forbids them | done (#67) |
| 5.3 — measurement | benchmarks for the SDK's observe path, ingest's dispose, batch, flush and acknowledgement, the catalog's registration, listing and reads on both engines, `QueryRecords` over an hour of N records in K packs (binary and JSON measured apart), merge and the compactor's passes; P4's predicates explained on both engines; `pprof` on the metrics listener, opt-in; the baselines and the offered rate of the write-connection case recorded here | done (#70) |
| 5.4 — performance: the catalog and its writer | P1–P6 under points 30 to 38, each measured before and after, each test on both engines: registration's evidence read in sets, the range probes bounded by the capture's widest range, the claim listing in bounded transactions that never forget a row from a chunk not yet listed, deletion and eviction on new indexes, listings batched across captures with completeness unchanged, the API audit write's deadline and wait histogram (point 5); `bench.yaml` narrowed so it finishes | done (#74) |
| 5.5 — performance: ingest, reads, compactor, API | P7–P18 that 5.3 shows worth their change, each measured before and after; concurrent acknowledgements only with completion tracked by identity, not as a prefix; any key-path change keeps the binding-check and cancellation tests | pending |
| 5.6 — gate | audit every gate case against the test bodies, then one step per gap, as 4.6 did | pending |

## Phase 5.4 — performance: the catalog and its writer

Work on the branch `perf/phase-5/catalog`, one commit per step, the spec first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Every change runs its tests on both engines and has its before and after under point 37; a change that is not measured faster where the *Baselines* row named it is dropped, not merged.
Steps 1–3 land in order; steps 4–8 then go to separate implementers, steps 4 and 5 together since both are registration's;
steps 4 and 8 both edit `resolve.go` (`heldSeqQuery`, `Catalog.Audit`), so step 8 moves `Audit` and its helper to `audit.go` in its first commit and rebases on step 4.

| Step | Scope | State |
|---|---|---|
| 1 | spec, with T-55 recording points 30 to 38: `catalog.md` §7.1 (`pack_ranges.span` and its index; the `merges` and `conflicts` indexes), §7.2 (`RecordListing` in bounded transactions, the lease verified when nothing is written, the sweep last), `compactor.md` §8.3 (the listing no longer one transaction), `operations.md` §10.4 (`catalog_audit_wait_seconds` and its outcomes), `api.md` (the audit write's deadline, `GetExport` and an operator read's audit included) | done (#74) |
| 2 | benchmarks (point 37): the evidence with a parked share; a held probe far from the end; few due merges among many; eviction with a merge chain among unaffected merges; the listings asserting complete answers and the "windowed" `Completeness` row fixed; `bench.yaml` and `scripts/catalog.sh bench` narrowed and split, its engine filter checked to skip SQLite's rows and to run every `mariadb` row, each group's rows listed and their union checked against every `mariadb` row, and a run of it on this step recorded with each job's time | done (#74) |
| 3 | schema (point 36): `schema.hcl`, two migrations per engine (the column, then the indexes), their generated SQL inspected for that shape, a failure between them resuming, `migrate` tests on both engines that a migrated catalog with stored ranges answers each range's `span`, that an insert naming the old columns gets it, and that the migration applies online on MariaDB; the fallback of point 30 taken here if the column is not supported | done (#74) |
| 4 | P2 (point 30): the three probes bounded by the stored span, held as `EXISTS`. Failing cases first: a range overlapping from far below found, nested, equal and singleton ranges, seq 0 and the largest seq, a merged hour's wide range and the bound after it is evicted, cross-hour conflicts, a held seq at the start of a long history, a wide range inserted by a barrier between `Resolve`'s span and its probe, rows inserted directly by the seeders | done (#74) |
| 5 | P1 (point 31): the evidence read in sets. Failing cases first: every outcome and tombstone reason against the per-delivery path, a parked version in each state, an order's cut-off at, above and below a stream seq, two incarnations, a refusal in the last delivery, statements at the 900-parameter boundary | done (#74) |
| 6 | P3 (point 32): the claim listing bounded. Failing cases first: a key only in the last chunk never forgotten, a key repeated across chunks, a failure between chunks skipping the sweep and its retry, a key attributed after the read and before its sweep page kept, a lease lost before, between chunks and between sweep pages, an expired lease with an empty and an unchanged listing refused, a steady listing writing nothing, survivors and deletable rows kept | done (#74) |
| 7 | P4 (point 33): deletion and eviction. Failing cases first: legacy and current deadlines at their boundary, both sides of a conflict, cross-hour conflicts' revisions and events, a merge chain, a lost lease rolling back | done (#74) |
| 8 | P5 and P6 (points 34, 35): listings batched at 127, 128 and 129 captures with complete answers equal to the unbatched ones and the registration barriers across chunk boundaries; the audit's deadline and wait histogram for the four handlers and an `AdminService` read (its operation done, then its audit expiring with no result returned), a held writer expiring an audit with no row written later, an earlier parent deadline, one observation an attempt with its outcome | done (#74) |
| 9 | measurement (points 37, 38): the local sitting and `bench.yaml` on MariaDB, base against head, recorded in *Baselines* under a 5.4 heading; the write-connection gate run and recorded; a P row its numbers do not support is reverted and closed with them | done (#74) |

Carried to 5.5 (P10): the traffic consumer's heartbeat sends an in-progress acknowledgement for every held message each beat (`ingester.go:276`, `Heartbeat/held=10000` 3.01 ms),
so at a large fleet with a long flush the acknowledgements a second can exceed the messages a second; an `ack_wait` above the flush, without in-progress acknowledgements, is the alternative P3 takes for its feed.

## Baselines

Filled by step 11, one table per package, each row a benchmark and size with its time, bytes and allocations an operation and benchstat's interval,
under a line naming the run's commit and point 22's machine facts; then step 9's plans per query and engine, and step 10's sweep and offered rate.

Carried from 5.2 to the gate (5.6): a few catalog and compactor errors on paths 5.2 did not touch name an object key in the operator's log
(a claim survivor's release and removal, a registration's key lookups, an abandoned close's delete), which point 11 keeps out of every log.

**The local sitting** (2026-11-02), at 77930eb, the tip of `test/phase-5/measurement`:
Go 1.26.10 linux/amd64, benchstat `v0.0.0-20261009192801-be2c69fb417e` (pinned in `bench.yaml`);
SQLite 3.54.1, journal mode WAL, `synchronous` FULL;
32 logical cores (16 cores, two threads each), x86-64 desktop class, 62 GiB of memory, not idle (other work held a load average of about 2 to 3);
ext4 on an NVMe SSD for the test's temporary directory, which also holds the flush's working file and the `fs` store's root;
`-cpu 32 -count 10 -benchtime 1s`, no race detector, except where a table's notes say otherwise.
The small and medium rows ran in one pass at `TRACEKEEP_BENCH_SCALE=medium` and the large rows alone in a second at `large`, so each row is one run of ten samples:
the sitting measured no repeatability of its own (point 21's r), which a comparison measures in its own sitting.
A `sync` row's time includes a synchronization to disk, as `bench.yaml`'s summary classifies it.
Every catalog benchmark's `mariadb` row skipped locally;
an interval of ±5% or more is inconclusive under point 21 and is listed under *What the numbers say*.

### catalog (`internal/catalog`, SQLite)

`RegisterPack` ran at a fixed `-benchtime`, as its godoc requires: 100x for `evidence`, `miss` and `hit`, but 10x for `deliveries=50000` (1.4 s an iteration), and 1x for `ranges`;
`ranges=1000/prior=1000` and `ranges=50000` at every prior skip by that bound and have no row.
`RecordListing` at 1,000,000 objects ran at 1x, `PruneEvents` at 1,000,000 events at 5x and `EvictHour` at 100,000 conflicts at 10x, each reseeded every iteration.

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| RegisterPack/evidence/deliveries=100/prior=1000 | 4.47 ms ±34% | 482 KiB | 12.4k | yes |
| RegisterPack/evidence/deliveries=1000/prior=1000 | 29.5 ms ±1% | 4.6 MiB | 121k | yes |
| RegisterPack/miss/prior=1000 | 589 µs ±2% | 18.9 KiB | 488 | yes |
| RegisterPack/hit/prior=1000 | 950 µs ±73% | 22 KiB | 570 | yes |
| RegisterPack/evidence/deliveries=100/prior=20000 | 6.19 ms ±32% | 482 KiB | 12.4k | yes |
| RegisterPack/evidence/deliveries=1000/prior=20000 | 30.7 ms ±2% | 4.6 MiB | 121k | yes |
| RegisterPack/miss/prior=20000 | 1.84 ms ±1% | 18.9 KiB | 488 | yes |
| RegisterPack/hit/prior=20000 | 1.9 ms ±1% | 22 KiB | 570 | yes |
| RegisterPack/ranges/ranges=100/prior=1000 | 10.2 ms ±13% | 257 KiB | 6444 | yes |
| RegisterPack/ranges/ranges=100/prior=20000 | 131 ms ±1% | 257 KiB | 6438 | yes |
| RegisterPack/ranges/ranges=1000/prior=20000 | 1.31 s ±0% | 2.4 MiB | 60.5k | yes |
| RegisterPack/evidence/deliveries=100/prior=200000 | 17 ms ±8% | 482 KiB | 12.4k | yes |
| RegisterPack/evidence/deliveries=1000/prior=200000 | 42.6 ms ±1% | 4.6 MiB | 121k | yes |
| RegisterPack/miss/prior=200000 | 13.8 ms ±1% | 19 KiB | 488 | yes |
| RegisterPack/hit/prior=200000 | 13.9 ms ±0% | 22.1 KiB | 570 | yes |
| RegisterPack/evidence/deliveries=50000/prior=1000 | 1.39 s ±0% | 230 MiB | 6.04M | yes |
| RegisterPack/evidence/deliveries=50000/prior=20000 | 1.39 s ±0% | 230 MiB | 6.04M | yes |
| RegisterPack/evidence/deliveries=50000/prior=200000 | 1.39 s ±1% | 230 MiB | 6.04M | yes |
| RegisterPack/ranges/ranges=100/prior=200000 | 1.39 s ±1% | 271 KiB | 6473 | yes |
| RegisterPack/ranges/ranges=1000/prior=200000 | 13.9 s ±0% | 2.4 MiB | 60.5k | yes |
| RecordListing/objects=10000/attributed/steady | 53.5 ms ±1% | 7.25 MiB | 170k |  |
| RecordListing/objects=10000/attributed/new | 62.8 ms ±1% | 7.25 MiB | 170k | yes |
| RecordListing/objects=10000/attributed/gone | 14.5 µs ±1% | 2.36 KiB | 55 |  |
| RecordListing/objects=10000/unattributed/steady | 64.2 ms ±2% | 18 MiB | 460k |  |
| RecordListing/objects=10000/unattributed/new | 68.6 ms ±1% | 18 MiB | 460k | yes |
| RecordListing/objects=10000/unattributed/gone | 30 ms ±12% | 5.75 MiB | 170k | yes |
| RecordListing/objects=100000/attributed/steady | 537 ms ±2% | 72.5 MiB | 1.7M |  |
| RecordListing/objects=100000/attributed/new | 657 ms ±1% | 72.5 MiB | 1.7M | yes |
| RecordListing/objects=100000/attributed/gone | 18 µs ±0% | 2.34 KiB | 53 |  |
| RecordListing/objects=100000/unattributed/steady | 681 ms ±2% | 181 MiB | 4.6M |  |
| RecordListing/objects=100000/unattributed/new | 724 ms ±1% | 181 MiB | 4.6M | yes |
| RecordListing/objects=100000/unattributed/gone | 340 ms ±4% | 59.6 MiB | 1.7M | yes |
| RecordListing/objects=1000000/attributed/steady | 5.45 s ±1% | 725 MiB | 17M |  |
| RecordListing/objects=1000000/attributed/new | 6.64 s ±1% | 725 MiB | 17M | yes |
| RecordListing/objects=1000000/attributed/gone | 116 µs ±19% | 2.34 KiB | 54 |  |
| RecordListing/objects=1000000/unattributed/steady | 7.65 s ±1% | 1.8 GiB | 46M |  |
| RecordListing/objects=1000000/unattributed/new | 9.22 s ±2% | 1.8 GiB | 46M | yes |
| RecordListing/objects=1000000/unattributed/gone | 4.58 s ±1% | 595 MiB | 17M | yes |
| Listings/captures=1/devices=1/gaps=false/ListCaptures | 52.4 µs ±0% | 10.5 KiB | 195 |  |
| Listings/captures=1/devices=1/gaps=false/ListDevices | 133 µs ±3% | 31.2 KiB | 689 |  |
| Listings/captures=1/devices=1/gaps=false/ListPacks | 314 µs ±1% | 83.5 KiB | 1653 |  |
| Listings/captures=1/devices=1/gaps=false/Completeness | 76.4 µs ±1% | 21 KiB | 508 |  |
| Listings/captures=1/devices=1/gaps=true/ListCaptures | 53.1 µs ±2% | 10.5 KiB | 195 |  |
| Listings/captures=1/devices=1/gaps=true/ListDevices | 131 µs ±1% | 32 KiB | 673 |  |
| Listings/captures=1/devices=1/gaps=true/ListPacks | 292 µs ±3% | 73.6 KiB | 1453 |  |
| Listings/captures=1/devices=1/gaps=true/Completeness | 73.1 µs ±2% | 21.9 KiB | 491 |  |
| Listings/captures=100/devices=1/gaps=false/ListCaptures | 665 µs ±3% | 374 KiB | 4872 |  |
| Listings/captures=100/devices=1/gaps=false/ListDevices | 9.14 ms ±3% | 2.34 MiB | 53.3k |  |
| Listings/captures=100/devices=1/gaps=false/ListPacks | 18.6 ms ±2% | 7.25 MiB | 126k |  |
| Listings/captures=100/devices=1/gaps=false/Completeness | 8.36 ms ±0% | 2.04 MiB | 50.1k |  |
| Listings/captures=100/devices=1/gaps=true/ListCaptures | 664 µs ±2% | 374 KiB | 4872 |  |
| Listings/captures=100/devices=1/gaps=true/ListDevices | 809 µs ±5% | 396 KiB | 5351 |  |
| Listings/captures=100/devices=1/gaps=true/ListPacks | 19.4 ms ±6% | 7.04 MiB | 124k |  |
| Listings/captures=100/devices=1/gaps=true/Completeness | 8.37 ms ±6% | 2.05 MiB | 49.9k |  |
| Listings/captures=100/devices=100/gaps=false/ListCaptures | 708 µs ±2% | 374 KiB | 4872 |  |
| Listings/captures=100/devices=100/gaps=false/ListDevices | 8.96 ms ±0% | 2.39 MiB | 53.5k |  |
| Listings/captures=100/devices=100/gaps=false/ListPacks | 18.8 ms ±4% | 7.25 MiB | 126k |  |
| Listings/captures=100/devices=100/gaps=false/Completeness | 8.58 ms ±3% | 2.04 MiB | 50.1k |  |
| Listings/captures=100/devices=100/gaps=true/ListCaptures | 702 µs ±5% | 374 KiB | 4872 |  |
| Listings/captures=100/devices=100/gaps=true/ListDevices | 8.93 ms ±1% | 2.4 MiB | 53.3k |  |
| Listings/captures=100/devices=100/gaps=true/ListPacks | 18.3 ms ±1% | 7.04 MiB | 124k |  |
| Listings/captures=100/devices=100/gaps=true/Completeness | 8.36 ms ±1% | 2.05 MiB | 49.9k |  |
| Listings/captures=1500/devices=1/gaps=false/ListCaptures | 9.51 ms ±4% | 5.55 MiB | 73.2k |  |
| Listings/captures=1500/devices=1/gaps=false/ListDevices | 146 ms ±1% | 35.1 MiB | 800k |  |
| Listings/captures=1500/devices=1/gaps=false/ListPacks | 309 ms ±0% | 120 MiB | 1.88M |  |
| Listings/captures=1500/devices=1/gaps=false/Completeness | 145 ms ±2% | 30.6 MiB | 752k |  |
| Listings/captures=1500/devices=1/gaps=true/ListCaptures | 9.46 ms ±2% | 5.55 MiB | 73.2k |  |
| Listings/captures=1500/devices=1/gaps=true/ListDevices | 9.95 ms ±3% | 5.57 MiB | 73.7k |  |
| Listings/captures=1500/devices=1/gaps=true/ListPacks | 309 ms ±2% | 119 MiB | 1.85M |  |
| Listings/captures=1500/devices=1/gaps=true/Completeness | 142 ms ±0% | 30.8 MiB | 750k |  |
| Listings/captures=1500/devices=100/gaps=false/ListCaptures | 10.4 ms ±4% | 5.55 MiB | 73.2k |  |
| Listings/captures=1500/devices=100/gaps=false/ListDevices | 150 ms ±3% | 35.2 MiB | 800k |  |
| Listings/captures=1500/devices=100/gaps=false/ListPacks | 320 ms ±2% | 120 MiB | 1.88M |  |
| Listings/captures=1500/devices=100/gaps=false/Completeness | 142 ms ±1% | 30.6 MiB | 752k |  |
| Listings/captures=1500/devices=100/gaps=true/ListCaptures | 10 ms ±1% | 5.55 MiB | 73.2k |  |
| Listings/captures=1500/devices=100/gaps=true/ListDevices | 136 ms ±1% | 32.4 MiB | 731k |  |
| Listings/captures=1500/devices=100/gaps=true/ListPacks | 316 ms ±3% | 119 MiB | 1.85M |  |
| Listings/captures=1500/devices=100/gaps=true/Completeness | 141 ms ±1% | 30.8 MiB | 750k |  |
| ListingHistory/hours=24/window=hour/ListCaptures | 110 µs ±1% | 45.1 KiB | 632 |  |
| ListingHistory/hours=24/window=hour/ListDevices | 195 µs ±1% | 66.5 KiB | 1110 |  |
| ListingHistory/hours=24/window=hour/ListPacks | 1.08 ms ±1% | 287 KiB | 5962 |  |
| ListingHistory/hours=24/window=hour/Completeness | 802 µs ±2% | 210 KiB | 4991 |  |
| ListingHistory/hours=24/window=whole/ListCaptures | 113 µs ±3% | 45 KiB | 629 |  |
| ListingHistory/hours=24/window=whole/ListDevices | 195 µs ±5% | 66.5 KiB | 1107 |  |
| ListingHistory/hours=24/window=whole/ListPacks | 1.97 ms ±5% | 710 KiB | 12.8k |  |
| ListingHistory/hours=24/window=whole/Completeness | 843 µs ±3% | 210 KiB | 4991 |  |
| ListingHistory/hours=720/window=hour/ListCaptures | 110 µs ±2% | 45.1 KiB | 632 |  |
| ListingHistory/hours=720/window=hour/ListDevices | 978 µs ±1% | 266 KiB | 4379 |  |
| ListingHistory/hours=720/window=hour/ListPacks | 9.21 ms ±2% | 1.91 MiB | 46.9k |  |
| ListingHistory/hours=720/window=hour/Completeness | 8.79 ms ±0% | 1.83 MiB | 45.9k |  |
| ListingHistory/hours=720/window=whole/ListCaptures | 110 µs ±1% | 45 KiB | 629 |  |
| ListingHistory/hours=720/window=whole/ListDevices | 942 µs ±1% | 266 KiB | 4376 |  |
| ListingHistory/hours=720/window=whole/ListPacks | 37.9 ms ±4% | 17.1 MiB | 277k |  |
| ListingHistory/hours=720/window=whole/Completeness | 8.79 ms ±1% | 1.83 MiB | 45.9k |  |
| ListingHistory/hours=8760/window=hour/ListCaptures | 111 µs ±2% | 45.1 KiB | 632 |  |
| ListingHistory/hours=8760/window=hour/ListDevices | 13.8 ms ±1% | 3.07 MiB | 41.9k |  |
| ListingHistory/hours=8760/window=hour/ListPacks | 120 ms ±2% | 26 MiB | 519k |  |
| ListingHistory/hours=8760/window=hour/Completeness | 117 ms ±0% | 26 MiB | 518k |  |
| ListingHistory/hours=8760/window=whole/ListCaptures | 111 µs ±1% | 45 KiB | 629 |  |
| ListingHistory/hours=8760/window=whole/ListDevices | 10.8 ms ±2% | 3.07 MiB | 41.9k |  |
| ListingHistory/hours=8760/window=whole/ListPacks | 522 ms ±2% | 221 MiB | 3.38M |  |
| ListingHistory/hours=8760/window=whole/Completeness | 119 ms ±1% | 26 MiB | 518k |  |
| Lookups/prior=1000/ReadSet | 1.22 ms ±3% | 483 KiB | 10.9k |  |
| Lookups/prior=1000/NextReadHour | 11.2 µs ±1% | 1.35 KiB | 32 |  |
| Lookups/prior=1000/RecordCopies | 172 µs ±1% | 3.84 KiB | 96 |  |
| Lookups/prior=1000/KeyStanding/registered | 9.02 µs ±5% | 1.98 KiB | 53 |  |
| Lookups/prior=1000/KeyStanding/tombstoned | 4.31 µs ±2% | 1016 B | 27 |  |
| Lookups/prior=1000/KeyStanding/unknown | 8.93 µs ±7% | 1.98 KiB | 53 |  |
| Lookups/prior=20000/ReadSet | 1.23 ms ±4% | 483 KiB | 10.9k |  |
| Lookups/prior=20000/NextReadHour | 11.4 µs ±2% | 1.35 KiB | 32 |  |
| Lookups/prior=20000/RecordCopies | 2.57 ms ±0% | 3.84 KiB | 96 |  |
| Lookups/prior=20000/KeyStanding/registered | 9.05 µs ±1% | 1.98 KiB | 53 |  |
| Lookups/prior=20000/KeyStanding/tombstoned | 4.3 µs ±2% | 1016 B | 27 |  |
| Lookups/prior=20000/KeyStanding/unknown | 9.17 µs ±4% | 1.98 KiB | 53 |  |
| Lookups/prior=200000/ReadSet | 1.22 ms ±3% | 483 KiB | 10.9k |  |
| Lookups/prior=200000/NextReadHour | 11.3 µs ±1% | 1.35 KiB | 32 |  |
| Lookups/prior=200000/RecordCopies | 27 ms ±0% | 3.85 KiB | 96 |  |
| Lookups/prior=200000/KeyStanding/registered | 9.16 µs ±2% | 1.98 KiB | 53 |  |
| Lookups/prior=200000/KeyStanding/tombstoned | 4.54 µs ±4% | 1016 B | 27 |  |
| Lookups/prior=200000/KeyStanding/unknown | 9.09 µs ±1% | 1.98 KiB | 53 |  |
| Resolve/prior=1000/held | 72.2 µs ±1% | 3.56 KiB | 95 |  |
| Resolve/prior=1000/unheld | 72.1 µs ±0% | 3.56 KiB | 95 |  |
| Resolve/prior=20000/held | 970 µs ±0% | 3.56 KiB | 95 |  |
| Resolve/prior=20000/unheld | 968 µs ±1% | 3.56 KiB | 95 |  |
| Resolve/prior=200000/held | 10.4 ms ±0% | 3.57 KiB | 95 |  |
| Resolve/prior=200000/unheld | 10.4 ms ±1% | 3.57 KiB | 95 |  |
| AuditWhileRegistering/registering=0 | 226 µs ±977% | 1016 B | 23 | yes |
| AuditWhileRegistering/registering=1 | 5.47 ms ±60% | 480 KiB | 12.4k | yes |
| AuditWhileRegistering/registering=4 | 8.32 ms ±10% | 1.04 MiB | 26.5k | yes |
| AuditWhileRegistering/registering=16 | 30.6 ms ±24% | 3.89 MiB | 99k | yes |
| PruneEvents/events=10000 | 2.14 ms ±4% | 3.23 KiB | 80 | yes |
| PruneEvents/events=100000 | 21.1 ms ±1% | 3.23 KiB | 80 | yes |
| PruneEvents/events=1000000 | 237 ms ±1% | 3.2 KiB | 80 | yes |
| DeleteCandidates/merges=1000 | 1.95 ms ±2% | 233 KiB | 5046 |  |
| DeleteCandidates/merges=100000 | 513 ms ±0% | 31.2 MiB | 500k |  |
| DeleteCandidates/merges=1000000 | 6.46 s ±0% | 311 MiB | 5M |  |
| EvictHour/conflicts=10 | 660 µs ±59% | 28 KiB | 738 | yes |
| EvictHour/conflicts=1000 | 795 µs ±6% | 28 KiB | 738 | yes |
| EvictHour/conflicts=100000 | 14.4 ms ±2% | 28.4 KiB | 735 | yes |

### ingest (`internal/ingest`)

`Acknowledge` ran at 50x, its large rows at 20x (`inline`, `claim-check`) and 5x (`parked`), since each iteration publishes its messages untimed and `-benchtime 1s` ran it for hours;
`Flush` and `FlushDue` at 100x, their large rows at 20x, within their bound of 100.

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| Flush/store=mem/payload=14B/records=100 | 4.25 ms ±1% | 593 KiB | 12.6k | yes |
| Flush/store=mem/payload=14B/records=1000 | 29.6 ms ±17% | 5.87 MiB | 120k | yes |
| Flush/store=mem/payload=1KiB/records=100 | 4.29 ms ±1% | 974 KiB | 12.6k | yes |
| Flush/store=mem/payload=1KiB/records=1000 | 29.9 ms ±2% | 11.9 MiB | 120k | yes |
| Flush/store=fs/payload=14B/records=100 | 4.94 ms ±2% | 569 KiB | 12.7k | yes |
| Flush/store=fs/payload=14B/records=1000 | 29.7 ms ±1% | 5.61 MiB | 120k | yes |
| Flush/store=fs/payload=1KiB/records=100 | 4.91 ms ±83% | 741 KiB | 12.7k | yes |
| Flush/store=fs/payload=1KiB/records=1000 | 30.8 ms ±10% | 11.7 MiB | 120k | yes |
| Flush/store=mem/payload=14B/records=10000 | 285 ms ±1% | 60.1 MiB | 1.19M | yes |
| Flush/store=mem/payload=1KiB/records=10000 | 292 ms ±1% | 88.4 MiB | 1.19M | yes |
| Flush/store=fs/payload=14B/records=10000 | 284 ms ±2% | 60.1 MiB | 1.19M | yes |
| Flush/store=fs/payload=1KiB/records=10000 | 295 ms ±1% | 88.1 MiB | 1.19M | yes |
| FlushDue/batches=2 | 8.54 ms ±1% | 1.18 MiB | 26.2k | yes |
| FlushDue/batches=10 | 42.7 ms ±2% | 6.44 MiB | 131k | yes |
| FlushDue/batches=50 | 221 ms ±2% | 49.9 MiB | 657k | yes |
| Batcher/batches=1 | 232 ns ±5% | 464 B | 5 |  |
| Batcher/batches=100 | 18.3 µs ±9% | 29.3 KiB | 228 |  |
| Batcher/batches=1500 | 257 µs ±5% | 467 KiB | 3054 |  |
| Act/held=100 | 334 ns ±2% | 296 B | 5 |  |
| Act/held=1000 | 333 ns ±4% | 296 B | 5 |  |
| Act/held=10000 | 312 ns ±8% | 296 B | 5 |  |
| Heartbeat/held=100 | 27.9 µs ±1% | 38 KiB | 509 |  |
| Heartbeat/held=1000 | 271 µs ±1% | 364 KiB | 5012 |  |
| Heartbeat/held=10000 | 3.01 ms ±1% | 3.91 MiB | 50k |  |
| Dispose/masking=off/inline | 27.2 µs ±1% | 6.05 KiB | 135 |  |
| Dispose/masking=off/claim-check | 27.5 µs ±1% | 6.14 KiB | 137 |  |
| Dispose/masking=on/inline | 31 µs ±6% | 10.4 KiB | 187 |  |
| Dispose/masking=on/claim-check | 32 µs ±4% | 10.5 KiB | 189 |  |
| Acknowledge/inline/messages=100 | 6.44 µs ±7% | 665 B | 0 |  |
| Acknowledge/inline/messages=1000 | 72.4 µs ±8% | 41.9 KiB | 374 |  |
| Acknowledge/claim-check/messages=100 | 1.58 ms ±13% | 1.21 MiB | 3170 |  |
| Acknowledge/claim-check/messages=1000 | 12.3 ms ±2% | 3.8 MiB | 31.1k |  |
| Acknowledge/parked/messages=100 | 61.9 ms ±2% | 2.85 MiB | 29.5k | yes |
| Acknowledge/parked/messages=1000 | 626 ms ±4% | 15.6 MiB | 295k | yes |
| Acknowledge/inline/messages=10000 | 942 µs ±12% | 823 KiB | 9431 |  |
| Acknowledge/claim-check/messages=10000 | 120 ms ±1% | 28.4 MiB | 310k |  |
| Acknowledge/parked/messages=10000 | 6.04 s ±3% | 143 MiB | 2.95M | yes |

### records (`internal/records`)

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| Read/records=1000/packs=1/disjoint | 237 µs ±5% | 196 KiB | 3173 |  |
| Read/records=1000/packs=12/disjoint | 479 µs ±2% | 340 KiB | 4996 |  |
| Read/records=1000/packs=12/overlapping | 667 µs ±1% | 626 KiB | 5027 |  |
| Read/records=1000/packs=16/disjoint | 564 µs ±1% | 393 KiB | 5658 |  |
| Read/records=1000/packs=16/overlapping | 774 µs ±1% | 622 KiB | 5631 |  |
| Read/records=72000/packs=1/disjoint | 11 ms ±1% | 13.6 MiB | 216k |  |
| Read/records=72000/packs=12/disjoint | 13.3 ms ±1% | 12.3 MiB | 218k |  |
| Read/records=72000/packs=12/overlapping | 22.1 ms ±1% | 35.4 MiB | 218k |  |
| Read/records=72000/packs=16/disjoint | 14.5 ms ±1% | 12.3 MiB | 219k |  |
| Read/records=72000/packs=16/overlapping | 22.8 ms ±2% | 34.2 MiB | 219k |  |
| Read/records=360000/packs=1/disjoint | 57.4 ms ±2% | 84 MiB | 1.08M |  |
| Read/records=360000/packs=12/disjoint | 59.7 ms ±1% | 67.5 MiB | 1.08M |  |
| Read/records=360000/packs=12/overlapping | 116 ms ±1% | 242 MiB | 1.08M |  |
| Read/records=360000/packs=16/disjoint | 63.6 ms ±2% | 65.4 MiB | 1.08M |  |
| Read/records=360000/packs=16/overlapping | 118 ms ±2% | 219 MiB | 1.08M |  |

### api (`internal/api`)

`QueryRecordsDay` reports its configuration reads: 25 an operation at 10 and 100 records an hour, 37 at 1,000.

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| QueryRecords/records=1000/binary/first-page | 3.29 ms ±6% | 3.02 MiB | 48.9k |  |
| QueryRecords/records=1000/binary/whole | 3.25 ms ±3% | 3.02 MiB | 48.9k |  |
| QueryRecords/records=1000/json/first-page | 8.55 ms ±7% | 7.84 MiB | 152k |  |
| QueryRecords/records=1000/json/whole | 8.92 ms ±6% | 7.84 MiB | 152k |  |
| QueryRecords/records=1000/binary/seq-range | 917 µs ±1% | 368 KiB | 5192 |  |
| QueryRecords/records=72000/binary/first-page | 11.4 ms ±1% | 14.4 MiB | 227k |  |
| QueryRecords/records=72000/binary/whole | 172 ms ±1% | 210 MiB | 3.27M |  |
| QueryRecords/records=72000/json/first-page | 32.3 ms ±2% | 38.1 MiB | 747k |  |
| QueryRecords/records=72000/json/whole | 475 ms ±1% | 557 MiB | 10.8M |  |
| QueryRecords/records=72000/binary/seq-range | 11.3 ms ±0% | 4.36 MiB | 76.2k |  |
| QueryRecords/records=360000/binary/first-page | 12.7 ms ±1% | 17.1 MiB | 227k |  |
| QueryRecords/records=360000/binary/whole | 972 ms ±2% | 1.22 GiB | 16.3M |  |
| QueryRecords/records=360000/json/first-page | 34 ms ±1% | 42.1 MiB | 747k |  |
| QueryRecords/records=360000/json/whole | 2.54 s ±1% | 2.97 GiB | 53.9M |  |
| QueryRecords/records=360000/binary/seq-range | 48.2 ms ±1% | 22.2 MiB | 364k |  |
| QueryRecordsDay/records-an-hour=10 | 3.4 ms ±1% | 1.34 MiB | 23.6k |  |
| QueryRecordsDay/records-an-hour=100 | 8.53 ms ±1% | 7.06 MiB | 121k |  |
| QueryRecordsDay/records-an-hour=1000 | 67.4 ms ±3% | 69.3 MiB | 1.1M |  |

### store (`internal/store`)

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| FSPut/bytes=1024/depth=1 | 683 µs ±1% | 2.55 KiB | 36 | yes |
| FSPut/bytes=1024/depth=4 | 1.08 ms ±96% | 3.46 KiB | 67 | yes |
| FSPut/bytes=1024/depth=8 | 728 µs ±1% | 5.32 KiB | 126 | yes |
| FSPut/bytes=1048576/depth=1 | 1.03 ms ±0% | 33.6 KiB | 36 | yes |
| FSPut/bytes=1048576/depth=4 | 1.04 ms ±1% | 34.6 KiB | 67 | yes |
| FSPut/bytes=1048576/depth=8 | 1.07 ms ±1% | 36.4 KiB | 126 | yes |
| FSPut/bytes=16777216/depth=1 | 4.11 ms ±0% | 33.6 KiB | 36 | yes |
| FSPut/bytes=16777216/depth=4 | 4.11 ms ±1% | 34.5 KiB | 67 | yes |
| FSPut/bytes=16777216/depth=8 | 4.13 ms ±1% | 36.4 KiB | 126 | yes |

### service (`internal/service`)

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| TLSFiles/unchanged/handshakes=1 | 5.46 µs ±2% | 2.66 KiB | 15 |  |
| TLSFiles/unchanged/handshakes=8 | 5.64 µs ±1% | 2.9 KiB | 16 |  |
| TLSFiles/reload | 27.2 µs ±1% | 14.3 KiB | 166 |  |

### compact (`internal/compact`)

The `objects=100000` orphan rows ran at 1x with fewer samples:
`steady` at `-count 4`, since its seed registers 100,000 packs one at a time, about six minutes a run;
`adopting` at `-count 5`, since one iteration takes about four minutes;
benchstat gives no interval under six samples.
`MergePass` `cycles=10` reports 10, 1 or 0 scopes failed a cycle as its name says, and nothing merged.

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| Compare/records=1000 | 296 µs ±0% | 29 KiB | 293 |  |
| Compare/records=10000 | 2.01 ms ±0% | 43.2 KiB | 293 |  |
| Compare/records=100000 | 17.6 ms ±1% | 11.6 MiB | 376 |  |
| MergeWrite/inputs=2/records=1000/masked=false | 1.26 ms ±1% | 50.6 KiB | 479 | yes |
| MergeWrite/inputs=2/records=10000/masked=false | 4.51 ms ±1% | 239 KiB | 480 | yes |
| MergeWrite/inputs=12/records=1000/masked=false | 4.17 ms ±4% | 6.76 MiB | 1939 | yes |
| MergeWrite/inputs=12/records=10000/masked=false | 27.8 ms ±2% | 55.4 MiB | 2204 | yes |
| MergeWrite/inputs=12/records=1000/masked=true | 7.94 ms ±1% | 18.7 MiB | 79.5k | yes |
| MergeWrite/inputs=12/records=10000/masked=true | 38.3 ms ±3% | 64.5 MiB | 79.7k | yes |
| MergeWrite/inputs=64/records=1000/masked=false | 18.4 ms ±2% | 52.3 MiB | 9156 | yes |
| MergeWrite/inputs=64/records=10000/masked=false | 129 ms ±16% | 60 MiB | 9561 | yes |
| MergePass/healthy=10/failing=0/fresh | 17.7 ms ±1% | 785 KiB | 12.2k | yes |
| MergePass/healthy=10/failing=0/cycles=10 | 213 µs ±1% | 11.6 KiB | 310 |  |
| MergePass/healthy=10/failing=1/fresh | 17.7 ms ±1% | 804 KiB | 12.6k | yes |
| MergePass/healthy=10/failing=1/cycles=10 | 959 µs ±1% | 198 KiB | 3401 |  |
| MergePass/healthy=10/failing=10/fresh | 18.5 ms ±5% | 974 KiB | 15.3k | yes |
| MergePass/healthy=10/failing=10/cycles=10 | 7.22 ms ±4% | 1.84 MiB | 31k |  |
| MergePass/healthy=100/failing=0/fresh | 164 ms ±2% | 7.61 MiB | 121k | yes |
| MergePass/healthy=100/failing=0/cycles=10 | 285 µs ±7% | 11.6 KiB | 310 |  |
| MergePass/healthy=100/failing=1/fresh | 163 ms ±2% | 7.65 MiB | 122k | yes |
| MergePass/healthy=100/failing=1/cycles=10 | 1.03 ms ±4% | 198 KiB | 3401 |  |
| MergePass/healthy=100/failing=10/fresh | 167 ms ±4% | 7.85 MiB | 125k | yes |
| MergePass/healthy=100/failing=10/cycles=10 | 7.31 ms ±1% | 1.84 MiB | 31k |  |
| MergePass/healthy=1000/failing=0/fresh | 1.67 s ±4% | 76.3 MiB | 1.22M | yes |
| MergePass/healthy=1000/failing=0/cycles=10 | 655 µs ±1% | 11.6 KiB | 310 |  |
| MergePass/healthy=1000/failing=1/fresh | 1.65 s ±2% | 76.3 MiB | 1.22M | yes |
| MergePass/healthy=1000/failing=1/cycles=10 | 1.45 ms ±2% | 199 KiB | 3404 |  |
| MergePass/healthy=1000/failing=10/fresh | 1.64 s ±3% | 76.6 MiB | 1.22M | yes |
| MergePass/healthy=1000/failing=10/cycles=10 | 7.4 ms ±1% | 1.85 MiB | 31.1k |  |
| Orphans/objects=1000/steady | 8.96 ms ±1% | 1.99 MiB | 53k |  |
| Orphans/objects=1000/adopting | 499 ms ±3% | 28.7 MiB | 619k | yes |
| Orphans/objects=10000/steady | 93.1 ms ±5% | 20.2 MiB | 530k |  |
| Orphans/objects=10000/adopting | 6.61 s ±1% | 286 MiB | 6.21M | yes |
| Orphans/objects=100000/steady | 1.04 s | 205 MiB | 5.3M |  |
| Orphans/objects=100000/adopting | 232 s | 2.79 GiB | 62.1M | yes |
| RetainClaimListing/objects=1000 | 5.82 ms ±1% | 1.05 MiB | 21.1k |  |
| RetainClaimListing/objects=10000 | 60.8 ms ±1% | 11.1 MiB | 210k |  |
| RetainClaimListing/objects=100000 | 717 ms ±1% | 115 MiB | 2.1M |  |

### metrics (`internal/metrics`)

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| ReadCatalog/tools=10 | 725 µs ±1% | 166 KiB | 4521 |  |
| ReadCatalog/tools=150 | 11.4 ms ±4% | 2.4 MiB | 66.7k |  |
| ReadCatalog/tools=1500 | 116 ms ±1% | 24 MiB | 667k |  |

### the SDK (`producer/`, with `GOWORK=off`)

`producer/internal/core`:

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| Allocate | 20.7 ns ±0% | 0 B | 0 |  |
| Observe/kind=data/payload=14 | 95.6 ns ±0% | 16 B | 1 |  |
| Observe/kind=data/payload=1024 | 172 ns ±1% | 1 KiB | 1 |  |
| Observe/kind=event | 87.4 ns ±0% | 0 B | 0 |  |
| Observe/kind=data/payload=65536 | 4.07 µs ±20% | 64 KiB | 1 |  |

`producer/secsobs`:

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| Callback/callbacks=1 | 115 ns ±1% | 16 B | 1 |  |
| Callback/callbacks=4 | 178 ns ±6% | 16 B | 1 |  |

`producer`, whose `BenchmarkPublish` also reports 639,000 records a second (±2%):

| Benchmark | time/op | B/op | allocs/op | sync |
|---|---|---|---|---|
| Publish | 401 µs ±2% | 670 KiB | 5634 |  |

### catalog on MariaDB (`bench.yaml`, one partial pass)

Two runs of `bench.yaml` on `main` at 1c0e5f2 (base and head the same commit), at `small` and at `medium`:
a hosted runner, a virtual machine of 4 logical cores (2 cores, two threads each), the work, the temporary directory and MariaDB's data on one ext-family filesystem (`stat -f`: ext2/ext3);
MariaDB 11.8.9 in the image `scripts/catalog.sh` runs, `innodb_flush_log_at_trx_commit` 1;
`-cpu 4 -count 10 -benchtime 1s`.
Neither run finished its first pass within the job's 180 minutes:
at `small` the catalog's benchmarks took 21 minutes and the ingest package the rest, and at `medium` the catalog's alone took the whole job.
So the job measured no repeatability, `RegisterPack` (its fixed-N pass, which runs last) has no MariaDB row, nor has any large size,
and each row below is one run of ten samples from the `medium` run, its SQLite rows left out.
These numbers judge nothing against the local sitting (another machine, another engine); 5.4 measures its before and after in its own sittings.

| Benchmark (MariaDB) | time/op |
|---|---|
| RecordListing/objects=10000/attributed/steady, new | 2.37 s ±2%, 2.38 s ±4% |
| RecordListing/objects=10000/attributed/gone | 626 µs ±2% |
| RecordListing/objects=10000/unattributed/steady, new, gone | 4.87 s ±2%, 4.81 s ±1%, 2.53 s ±3% |
| RecordListing/objects=100000/attributed/steady, new | 23.4 s ±1%, 23.8 s ±3% |
| RecordListing/objects=100000/unattributed/steady, new, gone | 48.5 s ±1%, 48.1 s ±1%, 25.0 s ±1% |
| RecordListing/objects=100000/attributed/gone | 672 µs ±2% |
| Listings/captures=1 (ListCaptures, ListDevices, ListPacks, Completeness) | 1.40, 3.63, 5.57, 2.35 ms |
| Listings/captures=100/devices=1/gaps=false (the same four) | 2.40, 226, 241, 249 ms |
| Listings/captures=100/devices=1/gaps=true (the same four) | 2.41, 4.82, 238, 249 ms |
| ListingHistory/hours=24/window=hour (ListPacks, Completeness) | 25.0, 24.2 ms |
| ListingHistory/hours=720/window=hour (ListPacks, Completeness) | 40.6, 40.2 ms |
| ListingHistory/hours=720/window=whole (ListPacks, Completeness) | 80.2, 41.7 ms |
| Lookups/prior=1000 (ReadSet, NextReadHour, RecordCopies) | 3.23 ms, 337 µs, 1.39 ms |
| Lookups/prior=20000 (ReadSet, NextReadHour, RecordCopies) | 3.24 ms, 338 µs, 5.90 ms |
| Resolve/prior=1000, 20000 (held) | 1.06, 3.79 ms |
| AuditWhileRegistering/registering=0, 1, 4 | 456 µs ±19%, 516 µs ±63%, 886 µs ±32% |
| PruneEvents/events=10000, 100000 | 12.8 ms ±11%, 108 ms ±29% |
| DeleteCandidates/merges=1000, 100000 | 2.77 ms ±2%, 1.45 s ±39% |
| EvictHour/conflicts=10, 1000 | 10.1 ms ±25%, 11.7 ms ±27% |

What they add to the local sitting:
the claim listing costs about 240 µs an object attributed and 480 µs unattributed on MariaDB, linear from 10,000 to 100,000, so a million objects would take about 4 and 8 minutes, a statement a round trip (P3);
a listing over 100 captures costs about 2.4 ms a capture, a few statements a capture, where `ListDevices` answers early for a device with a gap (P5);
`RecordCopies` and `Resolve` grow with the prior ranges as on SQLite (P2);
and the `bench.yaml` job as written cannot finish: 5.4 narrows it to MariaDB's rows and splits it so each job fits.

### Plans (step 9, the large seed)

SQLite, before → after `ANALYZE`, one line a query; the large seed holds one capture, so after `ANALYZE` a capture's predicate selects every row and SQLite prefers a scan:

| Query | Access path | After `ANALYZE` |
|---|---|---|
| `auditByCaptureQuery` | scan `audit_log` by `audit_log_at`, a temporary B-tree for the order | the same |
| `auditByActorQuery` | scan `audit_log` by `audit_log_at`, a temporary B-tree for the order | the same |
| `pruneFloorQuery` | search `scope_events` by its primary key | the same |
| `deleteCandidatesQuery` | scan `packs` by its key; `merge_inputs` by `merge_inputs_pack`, `merges` and `packs` by key | scan `merges`; `packs` by key, `merge_inputs` by its key (covering), `packs` by key; a temporary B-tree for `DISTINCT` |
| `evictConflicts` (select and delete) | scan `conflicts`, each `IN` list a search of `packs` by `packs_capture_hour_state` with a Bloom filter | the same |
| `evictMergeInputsQuery` | search `merge_inputs` by its key (covering), its `IN` list a scan of `merges` | the same |
| `evictMergesQuery` | scan `merges` | the same |
| `overlapQuery` | search `pack_ranges` by `pack_ranges_capture_seq` (capture, `seq_from <`), `packs` by key | scan `pack_ranges`, `packs` by key |
| `heldSeqQuery` | search `pack_ranges_capture_seq` (covering) | the same |
| `frontierQuery` | search `captures` by key | the same |
| `retainedQuery` | search `pack_ranges` by `pack_ranges_capture_seq` (capture), `packs` by key | scan `pack_ranges`, `packs` by key |
| `evictedQuery` | search `evicted_ranges` by key (capture) | scan `evicted_ranges` |
| `parkedQuery` | search `parked` by key (capture) | the same |
| `lossesQuery` | search `packs` by `packs_capture_hour_state` (capture), `pack_losses` by key | scan `pack_losses`, `packs` by key |
| `crashGapsQuery` | search `capture_closes` by key (capture) | the same |
| `conflictsQuery` | search `conflicts` by key (capture) | the same |

MariaDB 11.8 (local, in the image `scripts/catalog.sh` runs), after `ANALYZE TABLE`: the access path and the rows each table read (`ANALYZE FORMAT=JSON`'s `r_rows` times `r_loops`), a statement that writes inside a rolled-back transaction:

| Query | Access path, rows read | Time |
|---|---|---|
| `auditByCaptureQuery` | `audit_log` full scan, 100,000 | 114 ms |
| `auditByActorQuery` | `audit_log` full scan, 100,000 | 37 ms |
| `pruneFloorQuery` | `scope_events` full scan, 100,000 | 16 ms |
| `deleteCandidatesQuery` | `merge_inputs` by `merge_inputs_pack` (covering), 100,000; `packs`, `merges`, `packs` by key, 100,000 each | 882 ms |
| `evictConflicts` select | `conflicts` full scan, 100,000; each `IN` list `packs` by `packs_capture_hour_state`, 0 and 100 | 185 ms |
| `evictConflicts` delete | `conflicts` full scan, 100,000; each `IN` list `packs` by `packs_capture_hour_state`, 0 and 100 | 163 ms |
| `evictMergeInputsQuery` | `merge_inputs` full scan, 100,000; `merges` by key, 100,000 | 249 ms |
| `evictMergesQuery` | `merges` full scan, 100,000 | 167 ms |
| `overlapQuery` | `pack_ranges` range of `pack_ranges_capture_seq`, 200,000; `packs` by key, 1 | 99 ms |
| `heldSeqQuery` | `pack_ranges` range of `pack_ranges_capture_seq`, 200,000 | 17 ms |
| `frontierQuery` | `captures`, a constant by key | 0 ms |
| `retainedQuery` | `pack_ranges` full scan, 200,000; `packs` by key, 200,000 | 502 ms |
| `evictedQuery` | `evicted_ranges` full scan, 1,000 | 0.1 ms |
| `parkedQuery` | `parked` range of its key, 10,000 | 7 ms |
| `lossesQuery` | `pack_losses` full scan, 1,000; `packs` by key, 1,000 | 0.9 ms |
| `crashGapsQuery` | `capture_closes`, a constant by key | 0 ms |
| `conflictsQuery` | `conflicts` range of its key, 100,000 | 23 ms |

The seed holds one capture, so a range over a capture's prefix reads every row as a scan would;
the audit, prune and eviction predicates read their whole table with no index to choose (P4),
and the overlap probe reads all 200,000 prior ranges for one new range (P2).

### The write-connection case (step 10)

`TRACEKEEP_LOAD=baseline`, dev's flush of 10 s, a one-minute warm-up and twelve 10-second windows a point (the slopes and the bound in records a window):

| K | offered fwd/s | made fwd/s | published rec/s | dropped | backlog slope | ring slope | reserved slope | bound | sustained |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 100.0 | 200.0 | 0 | 3.35 | 0.00 | 0.00 | 10.00 | yes |
| 1 | 200 | 200.0 | 400.0 | 0 | 9.53 | 0.00 | 0.00 | 20.00 | yes |
| 1 | 400 | 400.0 | 800.0 | 0 | 17.47 | 0.00 | 0.00 | 40.00 | yes |
| 1 | 800 | 799.8 | 1599.6 | 0 | 72.81 | 0.00 | 0.00 | 79.98 | yes |
| 1 | 1600 | 1236.0 | 2472.0 | 0 | 85.65 | 0.00 | 0.00 | 123.60 | no: made 1236/s against 1600/s offered |
| 10 | 100 | 100.0 | 200.0 | 0 | 0.29 | -0.28 | 0.00 | 10.00 | yes |
| 10 | 200 | 200.0 | 400.0 | 0 | 2.20 | 0.22 | 0.00 | 20.00 | yes |
| 10 | 400 | 400.0 | 800.1 | 0 | 24.37 | -0.30 | 0.00 | 40.00 | yes |
| 10 | 800 | 800.0 | 1600.0 | 0 | 50.53 | 0.10 | 0.00 | 80.00 | yes |
| 10 | 1600 | 1600.0 | 3200.0 | 0 | 91.15 | -0.66 | 0.00 | 160.00 | yes |
| 10 | 3200 | 3198.6 | 6397.2 | 0 | 22956.21 | -0.01 | 0.00 | 319.86 | no: the backlog minima rise |
| 100 | 100 | 100.0 | 200.1 | 0 | -11.82 | -1.16 | 0.00 | 10.00 | yes |
| 100 | 200 | 200.0 | 400.1 | 0 | 3.36 | -2.82 | 0.00 | 20.00 | yes |
| 100 | 400 | 400.0 | 800.0 | 0 | 8.47 | -0.06 | 0.00 | 40.00 | yes |
| 100 | 800 | 800.0 | 1600.0 | 0 | 72.69 | 0.25 | 0.00 | 80.00 | yes |
| 100 | 1600 | 1600.0 | 3200.0 | 0 | -54.06 | -3.36 | 0.00 | 160.00 | yes |
| 100 | 3200 | 3200.0 | 6400.0 | 0 | 23668.86 | -2.35 | 0.00 | 320.00 | no: the backlog minima rise |

The highest sustained rate is 800 fwd/s at K = 1, where one tool's loopback made only 1,236 of 1,600 a second, and 1,600 fwd/s at K = 10 and K = 100:
at 3,200 fwd/s (6,400 records a second) ingest falls behind.
**The offered rate is 800 fwd/s at K = 100, with dev's flush of 10 s**, shown sustained over sixty windows:
800.0 fwd/s made, 1,600.0 records a second published, nothing dropped, the backlog slope −11.72 against a bound of 80.

`PackURL` at that rate, 1,200 calls (more than the gate's 1,000, the windows run whole):

| p50 | p99 | made | succeeded | superseded | failed | flushes | made fwd/s | published rec/s | sustained |
|---|---|---|---|---|---|---|---|---|---|
| 1.06 ms | 5.78 ms | 1,200 | 1,200 | 0 | 0 | 11 | 800.0 | 1,600.0 | yes |

The gate (1,000 audited successes, p99 within 2 s) passes here; it is reported, not enforced, and 5.4 runs it.

### What the numbers say

Read for 5.4 and 5.5; no P row closes here.

- **P1** (registration's evidence): 30 µs a delivery at 1,000 deliveries and 28 µs at 50,000 (1.39 s a pack, at every prior), so the audit's 1.3 s for 50k holds.
- **Point 25**: the audit's 27 µs a delivery was registration's, 28 µs at 50,000 deliveries whatever the history;
  `Dispose` costs its own 27 µs a message (masking off), a separate cost of the same size, which is why the scratch dispose benchmark showed the same figure.
- **P2** (the overlap probe): 0.10, 1.3 and 13.9 ms a range at 1,000, 20,000 and 200,000 prior ranges, linear in the history, the audit's 14 ms at 200k confirmed;
  one range costs 0.6, 1.8 and 13.8 ms, a hit no more than a miss, and `Resolve` 72 µs, 0.97 ms and 10.4 ms, held or not.
- **P3** (the claim listing): 5.45 s steady and 6.64 s all new at a million attributed objects, 7.65 s and 9.22 s unattributed, 4.58 s all gone, linear from 100,000; the audit's 6 s holds.
- **P4**: the prune 2.1, 21 and 237 ms at 10k, 100k and 1M events, linear, on the primary key;
  `DeleteCandidates` 1.95 ms, 513 ms and 6.46 s at 1k, 100k and 1M merges, a scan of `packs`, then of `merges` after `ANALYZE`;
  `EvictHour` 0.66, 0.80 and 14.4 ms at 10, 1,000 and 100,000 conflicts, a scan of `conflicts`;
  the audit listings scan `audit_log` by time, as point 26 read.
- **P5** (listings over history): with the one-hour window held, `ListPacks` takes 1.08, 9.2 and 120 ms over 24, 720 and 8,760 hours of history, `Completeness` 0.80, 8.8 and 117 ms, `ListDevices` 0.20, 0.98 and 13.8 ms, `ListCaptures` a flat 0.11 ms;
  at 1,500 captures `ListPacks` takes 0.31 s and `Completeness` 0.14 s.
- **P6** (an audit write beside registrations): 0.23 ms alone, 5.5 ms beside one registering writer, 8.3 ms beside 4 and 31 ms beside 16;
  every interval is wide (±977%, ±60%, ±10%, ±24%), so the rows are inconclusive, but the waiting is plain.
- **P7, P19, P21**: `Dispose` 27 µs a message inline and claim-check, 31 to 32 µs masked, so masking adds about 4 µs.
- **P8**: an acknowledgement costs 64 to 94 ns a message inline, 12 µs claim-check and 0.6 ms a parked copy (6.0 s for 10,000).
- **P9**: `FlushDue` 8.5, 42.7 and 221 ms for 2, 10 and 50 due batches, about 4.3 ms a batch, one `Flush` of 100 records each: the batches flush one after another.
- **P10**: the heartbeat 0.27 to 0.30 µs a held message, `Act` a flat 0.3 µs, the batcher 0.17 µs a batch at 1,500.
- **P11** (`fs` puts): 0.68 ms for 1 KiB at depth 1 and 0.73 ms at depth 8, 4.1 ms for 16 MiB at every depth; a flush on `fs` costs what one on memory does within noise.
- **P12**: a 24-hour `QueryRecords` reads the capture's configuration 25 times at 10 and 100 records an hour, 37 times at 1,000.
- **P13**: a narrow seq range costs 0.92 ms, 11.3 ms and 48 ms in an hour of 1,000, 72,000 and 360,000 records, against a first page's 3.3, 11.4 and 12.7 ms.
- **P14**: JSON costs 2.6 to 2.8 times binary: the whole hour of 360,000 records 2.54 s and 2.97 GiB against 0.97 s and 1.22 GiB.
- **P15**: the conflict check 0.30, 2.0 and 17.6 ms over two packs of 1k, 10k and 100k records.
- **P16** (the orphan pass): steady 9.0 ms, 93 ms and 1.04 s over 1k, 10k and 100k objects, about 9 to 11 µs an object;
  adopting 0.50 s, 6.6 s and 232 s, superlinear.
- **P17**: ten cycles over 10 failing scopes take 7.2 to 7.4 ms whatever the healthy count, 74 µs a scope a cycle; a fresh cycle 17.7 ms, 164 ms and 1.65 s at 10, 100 and 1,000 healthy scopes, failures or not.
- **P18**: the TLS files cost 5.5 µs a handshake, 5.6 µs with 8 at once, 27 µs a reload.
- **The SDK** (`650-perf-sec.md`'s callback budget): an observe 96 ns at 14 B, 172 ns at 1 KiB and 4.07 µs at 64 KiB (±20%), the `secsobs` callback 115 ns, 178 ns with 4 at once; the publisher 639,000 records a second.

Inconclusive under point 21 (an interval of ±5% or more), measured again before any comparison relies on them:
- catalog: `RegisterPack/evidence/deliveries=100/prior=1000` (±34%), `RegisterPack/hit/prior=1000` (±73%), `RegisterPack/evidence/deliveries=100/prior=20000` (±32%), `RegisterPack/ranges/ranges=100/prior=1000` (±13%), `RegisterPack/evidence/deliveries=100/prior=200000` (±8%), `RecordListing/objects=10000/unattributed/gone` (±12%), `RecordListing/objects=1000000/attributed/gone` (±19%), `Listings/captures=100/devices=1/gaps=true/ListDevices` (±5%), `Listings/captures=100/devices=1/gaps=true/ListPacks` (±6%), `Listings/captures=100/devices=1/gaps=true/Completeness` (±6%), `Listings/captures=100/devices=100/gaps=true/ListCaptures` (±5%), `ListingHistory/hours=24/window=whole/ListDevices` (±5%), `ListingHistory/hours=24/window=whole/ListPacks` (±5%), `Lookups/prior=1000/KeyStanding/registered` (±5%), `Lookups/prior=1000/KeyStanding/unknown` (±7%), `AuditWhileRegistering/registering=0` (±977%), `AuditWhileRegistering/registering=1` (±60%), `AuditWhileRegistering/registering=4` (±10%), `AuditWhileRegistering/registering=16` (±24%), `EvictHour/conflicts=10` (±59%), `EvictHour/conflicts=1000` (±6%);
- ingest: `Flush/store=mem/payload=14B/records=1000` (±17%), `Flush/store=fs/payload=1KiB/records=100` (±83%), `Flush/store=fs/payload=1KiB/records=1000` (±10%), `Batcher/batches=1` (±5%), `Batcher/batches=100` (±9%), `Batcher/batches=1500` (±5%), `Act/held=10000` (±8%), `Dispose/masking=on/inline` (±6%), `Acknowledge/inline/messages=100` (±7%), `Acknowledge/inline/messages=1000` (±8%), `Acknowledge/claim-check/messages=100` (±13%), `Acknowledge/inline/messages=10000` (±12%);
- records: `Read/records=1000/packs=1/disjoint` (±5%);
- api: `QueryRecords/records=1000/binary/first-page` (±6%), `QueryRecords/records=1000/json/first-page` (±7%), `QueryRecords/records=1000/json/whole` (±6%);
- store: `FSPut/bytes=1024/depth=4` (±96%);
- compact: `MergeWrite/inputs=64/records=10000/masked=false` (±16%), `MergePass/healthy=10/failing=10/fresh` (±5%), `MergePass/healthy=100/failing=0/cycles=10` (±7%), `Orphans/objects=10000/steady` (±5%), `Orphans/objects=100000/steady` (no interval, under six samples), `Orphans/objects=100000/adopting` (no interval, under six samples);
- producer/internal/core: `Observe/kind=data/payload=65536` (±20%);
- producer/secsobs: `Callback/callbacks=4` (±6%).

### 5.4: before and after (step 9)

Base 9c8ca3c (the branch after step 2), head db2a5b1 (the branch with steps 3–8), each judged by `benchjudge` under point 21.

**The local sitting** (2026-11-03), on the machine above, `-count 10 -benchtime 1s` at `medium`, `RegisterPack`'s fixed-N passes included (`-fixed 'BenchmarkRegisterPack/'`), base, head and base again:
312 metrics, 286 pass, 26 regress, none inconclusive.
**`bench.yaml` on MariaDB**, run 41545303322 at `medium`, each job base, head and base on its own runner:
the metrics and listings jobs pass, the lookups, retention and claim-listing jobs regress only on the rows below, and the registration job only on the bytes of `RegisterPack/ranges`.
Its one inconclusive row, `DeleteCandidates/merges=100000` (1.16 by the medians), was measured again locally on MariaDB 11.8 at `-benchtime 3s`: 0.95 [0.937, 0.974], a pass.
Each job fits the hosted runner's 360 minutes: registration took 269 minutes, the claim listing 121, the others 8 to 27.

Time an operation, head over base (the medians):

| Row | SQLite | MariaDB |
|---|---|---|
| `RegisterPack/evidence/deliveries=1000/prior=20000` (P1) | 0.04 | 0.005 |
| `RegisterPack/ranges/ranges=1000/prior=20000` (P2) | 0.02 | 0.08 |
| `Resolve/prior=20000/held`, `unheld`, `wide-evicted` (P2) | 0.03 | 0.24 |
| `Lookups/prior=20000/RecordCopies` (P2) | 0.02 | 0.19 |
| `RecordListing/objects=100000/attributed/steady`, `new` (P3) | 0.18, 0.55 | 0.02, 0.05 |
| `RecordListing/objects=100000/unattributed/steady`, `new` (P3) | 0.34, 0.84 | 0.02, 0.02 |
| `DeleteCandidates/merges=100000/due=10` (P4) | 0.0001 | 0.001 |
| `EvictMerges/merges=100000` (P4) | 0.07 | 0.05 |
| `Listings/captures=100/devices=100/ListDevices`, `ListPacks` (P5) | 0.55, 0.79 | 0.05, 0.11 |
| `Listings/captures=100/Completeness` (P5) | 0.90 | 0.98 to 1.01 |
| `AuditWhileRegistering/registering=1`, `4` (P6) | 0.21, 0.22 | 1.03, 1.01 |

The regressions, each a cost of the change named:

| P row | Rows | SQLite | MariaDB |
|---|---|---|---|
| P2, the span bound | `Resolve/*/held-start` | time 1.21 to 1.25 | time 1.07 to 1.11 |
| | `Resolve/*/wide-stored` at prior=1000 | time 1.07 | time 1.10 |
| | `RegisterPack/ranges/*` | passes | bytes 1.08 to 1.12 (time 0.08 to 0.49) |
| | `Resolve/*` (every case), `Lookups/*/RecordCopies` | bytes 1.06; RecordCopies 1.05 at prior=1000 | bytes 1.09; RecordCopies 1.11 |
| P3, bounded transactions | `RecordListing/*/attributed/gone` | time 1.17 to 1.22, bytes 1.08, allocations 55 → 60 and 53 → 59 | bytes 1.30, allocations 47 → 58 (time 0.74) |
| | `RecordListing/objects=100000/unattributed/gone` | time 1.26 (98 commits) | passes (0.03) |
| | `RecordListing/*/attributed/new` | passes | bytes 1.38 (time 0.05, allocations 0.35) |
| P4, the merges deleted with `RETURNING` | `EvictMerges/*` | bytes 1.06, allocations 765 → 786 at 1,000 | bytes 1.08, allocations 645 → 659 at 1,000 |
| P6, the explicit connection | `AuditWhileRegistering/registering=0` | bytes 1.08, allocations 23 → 25 | bytes 1.06, allocations 23 → 25, and bytes 1.6 to 1.9 at `registering=1`, `4` |

Why each costs what it does:
P2 binds the capture and the seq twice more for the span (`heldSeqQuery`, the overlap probe), once a range in a registration, and on SQLite a held seq at a long history's start reads `MAX(span)` from the index of a virtual column, not a covering one;
P3 verifies the lease before the first chunk and reads the sweep a page at a time, so a listing that writes little pays both, and a large unattributed sweep commits once a chunk;
on MariaDB the bytes of `attributed/new` are the driver's packet buffer for the multi-row inserts;
P4's `RETURNING` reads the deleted ids as rows;
P6 takes a connection from the pool before the insert, and at `registering=1` and `4` the benchmark's bytes count the concurrent registrations' allocations.

**The write-connection gate** (point 38) at db2a5b1, `TRACEKEEP_LOAD=gate TRACEKEEP_LOAD_OFFERED=800` (K = 100, dev's flush of 10 s):

| p50 | p99 | made | succeeded | superseded | failed | flushes | made fwd/s | published rec/s | sustained |
|---|---|---|---|---|---|---|---|---|---|
| 1.17 ms | 1.98 ms | 1,199 | 1,199 | 0 | 0 | 12 | 800.1 | 1,600.1 | yes |

The gate (1,000 audited successes, p99 within 2 s) passes, enforced.

Point 37 fails the step on any regression; the owner kept each P row's cost above for its gains (2026-11-03), so no P row is reverted.

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
