# tracekeep — decisions

Status: draft (2026-10-25) — split from seed v21

Covers the decisions tracekeep's design rests on:
the rulings it applies, the owner's decisions (T-1 onward) and the service defaults.
From seed v21 §13 (T-N is §13 item N,
so a "§13 item N" or "(§13)" in another file resolves here) and the decisions of the repository bootstrap.
Read before reopening anything a spec file says is decided, and append a `T-<N>` entry for every new owner decision.

This log is append-only (`.agents/rules/450-doc-lifecycle.md`):
a superseded entry keeps its text and gains "(superseded by T-<N>)"; IDs are never reused.
Rulings of other repositories keep their own IDs and are cited, never copied.

## Rulings relied on

**Already decided by a ruling** (applied here, not open):
claim-check above the inline limit (G5-217); only positively identified interval polls are thinned (G5-225);
the log service holds the keys (G5-207); the digest key spans captures and rotates by declaration (G5-220);
export schema by `format_minor` (G5-233); conflicts are kept, never resolved by a writer ([FMT I-12]); seq starts at 0.

The tracepack rulings the specs cite, by number (go-secs `docs/specs/tracepack/tracepack-decisions.md`):
G5-198, G5-200, G5-201, G5-207, G5-208, G5-211, G5-213, G5-214, G5-216, G5-217, G5-218, G5-220, G5-222, G5-224, G5-225, G5-227, G5-231, G5-233, G5-236, G5-250, G5-251, G5-252, G5-257, G5-258, G5-262, G5-267, G5-268, G5-269, G5-270.
The VE decisions they cite are V-8, V-12, V-26, V-27, V-29 and V-30 (veq `docs/specs/ve/`).

## Owner decisions

Each entry gives the question as the design put it, then the owner's decision.

### T-1 — wire observation

Adopt wire observation in the gateway, which makes the captures `raw-stream` / `wire-exact`, and amend LS-1 accordingly.

**Decided (2026-10-25):** wire observation, LS-1 to be amended.

### T-2 — loss visibility for durably admitted captures

Accept that loss visibility is guaranteed for durably admitted captures, with an unadmitted capture visible as an unregistered interval,
and amend LS-15 accordingly; or ask for another mechanism.

**Decided (2026-10-25):** the durably-admitted qualification accepted, LS-15 to be amended.

### T-3 — sampling and the poll channel

Sampling parameters `N` and `T`; the `fdc_grpc` change (interval-only requests on a dedicated channel);
whether timer-triggered collection contexts ride the poll channel;
whether to keep the rule in `thinning.source` as the key=value convention of §4.6 (the tracepack maintainers' advice) or still ask for a structured tag.
Parallel mode thins nothing (§4.6); that option is closed by fact, not by choice.

**Decided (2026-10-25):** N = 60 and T = 60 s as defaults
that **must be configurable** per poll channel in the producer's configuration (the channel superseded by T-42), the `fdc_grpc` interval-only channel to be built (superseded by T-42),
whether timer-triggered collection contexts count as interval polling **left to the FDC maintainers** (the one item still open; superseded by T-42), parallel mode closed by fact, the rule kept in `thinning.source` as the key=value convention.

### T-4 — retention against lineage

Retention against lineage (LS-13): (b), the VE copies what it promotes, is recommended; then tracekeep needs no holds.

**Decided (2026-10-25):** LS-13 (b), the VE copies what it promotes, no holds.

### T-5 — the VE service as a privileged reader

The VE service as a privileged reader through `PackURL` (whole packs with ciphertext in place;
[STO §3] reserves them for privileged components, V-26 allows it): a trust-boundary decision.

**Decided (2026-10-25):** the VE service is a privileged reader through `PackURL` with the `pack-reader` role.

### T-6 — key operation and production admission

Who operates the digest key and the master keys (Vault paths, who may declare a rotation);
the production-admission check of each tool type's recipe carriers against the policy.
LS-8's fields beyond recipe bodies:
which classes join the policy, and that this gates production admission of a tool type.

**Decided (2026-10-25):**
the digest key and the master keys are operated by the tracekeep maintainers (Vault paths and rotation declarations theirs),
and LS-8 stays at the baseline (recipe bodies), so production admission of a tool type checks its recipe carriers only.

### T-7 — stack

Stack confirmation: sqlx + Atlas, connect-go, Vite + React;
the SDK as a separate module in the tracekeep repository
that dials the bus itself by default (`WithConn`/`WithBus` as the escape hatch).

**Decided (2026-10-25):** the stack as designed, with one condition:
**before the API is implemented (phase 2), measure connect-go's overhead against plain gRPC** for `ListPacks` and streamed `QueryRecords`
and record the result as a phase 2 gate.

**Measured (2026-10-28):** connect-go v1.21.1 against grpc-go v1.85.0 on Go 1.26,
one process over a loopback TCP listener (h2c for connect, insecure credentials for gRPC), no TLS, both stacks untuned,
`wrapperspb.BytesValue` messages of random bytes with hand-written handlers, medians of five `testing.B` runs on a 32-thread x86-64 host.

| shape | gRPC | connect (Connect protocol) | connect (gRPC protocol) |
|---|---|---|---|
| unary, 64 KiB response (a `ListPacks` answer) | 65 µs | 82 µs (+26%) | 90 µs (+38%) |
| unary, 4 KiB response | 21 µs | 42 µs (+104%) | 53 µs (+154%) |
| unary, 64 KiB, 32 callers in parallel | 17 µs | 42 µs (+156%) | 49 µs (+195%) |
| server stream, 1,000 × 1 KiB (a `QueryRecords` shape) | 0.96 ms | 4.05 ms (+322%) | 3.82 ms (+298%) |

These rows have compression off.
With connect's default gzip on, which compresses every response that the client accepts, incompressible bytes cost 8.7 times gRPC's time for the 64 KiB call and 36 times for the stream.
So the api role turns response compression off, or raises its threshold above a pack's ranges.
Without gzip the added cost is 17 to 36 µs per unary call and about 3 µs per streamed message.
The Connect, gRPC and gRPC-Web protocols land within 6 to 16 percent of one another, except at 4 KiB, where the two gRPC-framed protocols run 25 to 34 percent slower.
The cost likely sits in `net/http`'s HTTP/2 layer, but no profile was taken.
Behind store reads and network round trips the difference is small for `ListPacks`.
For a long `QueryRecords` stream it is about 3 ms per thousand records, which phase 4 should measure again with real records before it settles the stream's batching.
Runs varied by up to ±16 percent per row.

### T-8 — repository name

Repository name (V-29 is provisional).

**Decided (2026-10-25):** `github.com/arloliu/tracekeep`.

### T-9 — the producer's residual wait

Accept, under LS-14, the producer's one residual wait:
a callback may wait for another callback to leave the capture's critical section (§4.2), a dependency on scheduling and on nothing external;
or require a different producer algorithm.

**Decided (2026-10-25):** the residual callback wait accepted under LS-14.

### T-10 — the requests to the tracepack maintainers

**Closed.**
The two requests to the tracepack maintainers were answered and approved on 2026-10-25 as G5-267 (parking is deferred on the bus, [STO §4] *Deferral*) and G5-268 (the bytes of a masked record are fixed by the format, [FMT §14], [STO §4] *Masking*), tracepack spec v2.35;
§6.1, §6.4 and §8.4 are aligned with the approved text, and parking is the service default.
A follow-up question of the same day was answered and approved as **G5-269** (tracepack spec v2.36, go-secs 8cee7eb, `tracepack-decisions.md` line 1775):
an operator terminates a record **version** (capture, seq, the carried 44 header bytes, payload digest),
every message of that version is acknowledged under the termination without another act,
the *Already staged* exception gains an "under a termination" sibling,
and *Deferral* condition 5 keeps an object only for a copy
that will be acknowledged neither as already staged nor under a termination; §6.1 *resolution* and §8.4 follow it.

**Decided (2026-10-25):** G5-267, G5-268 and G5-269 approved (tracepack spec v2.36, implemented by tracepack **v0.7.1**,
which the SDK's `go.mod` pins).

### T-11 — producers that bypass the SDK

The fault boundary for producers that bypass the SDK:
the design guarantees loss visibility and payload retention for SDK producers;
a non-SDK producer that overwrites a claim object, publishes two versions of one seq, or puts objects without the SDK's metadata is outside that guarantee
(its conflicting version in another hour may be evicted before delivery;
its unattributed objects are never deleted automatically),
and the owner decides whether such producers are supported at all.

**Decided (2026-10-25):** SDK producers only, the guarantee boundary documented.

### T-12 — service defaults

**Decided here, for the service** (tunable defaults; none of them stands in for a proof above;
the residuals stated in §4.2, §6.4 and §8.4 are listed as such, not as closed;
the object of a terminated version is released only after the server confirmed the acknowledgement of its known messages of the current incarnation, a caution G5-269 does not require, §8.4):
quiet period Q = 15 min; inactivity close I = 2 h; retirement inactivity bound L = 7 days; deletion delay D = 1 h;
producer retry horizon H = 5 min; publish deadline τ = 10 s;
the process-wide registration key set capped at 4 million, reset by a producer restart;
the reserved lane 4096 records and the unqueued-reservation bound 1 s;
retiring a processing configuration may require rotating captures;
receipt-less claim objects follow their record's hour once their capture is known;
compactor singleton under a catalog lease; parking enabled by default (G5-267);
durable consumers named per stream incarnation, `Preflight` refusing a traffic or parking stream with a mirror, sources or `AllowDirect`, and **replacing, restoring or rewinding either stream only as the stop-the-world maintenance procedure of §6.1 that ends in `bus adopt-stream`**
(an operational constraint on every deployment, cluster or host, made part of the design's approval in external review rounds 18–20, kept outside the repository);
the traffic stream and claim bucket are created by `tracekeep migrate`; Atlas CLI for the first release (superseded by T-41); HSMS-SS only in phase 0;
thinning disabled until the poll channel exists (superseded by T-42).

## Repository decisions

Taken by the owner on 2026-10-25 for the repository bootstrap.

### T-13 — toolchain

`mise` manages Go, golangci-lint, Atlas and Node; `mise.toml` is the single pin; `.tool-versions` is not kept.

### T-14 — Go version

`go 1.26.0` in every `go.mod`, the line the gateway is on;
the development toolchain is the latest Go 1.26 patch, 1.26.10 (replacing Go 1.27).

### T-15 — lint

golangci-lint 2.15.0, the latest release, built for Go 1.26.

### T-16 — modules

Two modules: the service (`github.com/arloliu/tracekeep`) and the SDK (`github.com/arloliu/tracekeep/producer`, its own `go.mod`, depending only on tracepack and nats.go).

### T-17 — workspace (superseded by T-27)

`go.work` at the root names both modules and is committed, with `go.work.sum`;
the service's `go.mod` requires the SDK by version and carries no `replace`;
CI also builds the service with `GOWORK=off` so its pinned requirement on the SDK stays buildable;
the SDK is never built through the workspace in CI.

### T-18 — tracepack (superseded by T-23)

`v0.7.1` in both modules.

### T-19 — nats.go

`v1.55.0` in both modules, nats-server `v2.15.1` for `internal/testbus`.
This raises the SDK's floor above the gateway's v1.40.1 and so revises `producer-sdk.md` §4.7 *Versions*;
the nats.go v1.40.1 and nats-server v2.14.5 facts the specs cite are re-verified, and §4.7 amended, before the SDK's first code.

### T-20 — document lifecycle

The veq document lifecycle (living and finite documents, the Status line, subject READMEs, `docs-check`, closing by deletion), with tracekeep's additions in `.agents/rules/450-doc-lifecycle.md`.

### T-21 — interface sections

Each subject spec file opens with an *Interface* section:
a non-normative summary of what a caller must know, traceable to the text below it, which stays authoritative.
Contract changes go through proposals, starting with P1 (module interfaces, applied by T-30).

### T-22 — configuration

The service's configuration is loaded with `github.com/arloliu/fuda`:
one struct whose tags carry each key, default, environment override and validation rule,
human-readable durations and sizes for the tunables,
and fuda's `ref` resolution for configuration secrets such as bus credentials and the catalog password.
Key material for masking is not configuration:
it is resolved by the `KeyProvider` at the version a binding pins (`masking-keys.md` §9), never through a `ref`.
The configuration is read once at start; live reload is not used,
since a capture's processing configuration is immutable once bound (`rollouts.md` §6.4).
The version is pinned when the service's `go.mod` first requires it.

### T-23 — tracepack v0.8.0 (superseded by T-36)

`v0.8.0` (spec v2.37, format 1.2, requiring go-secs v2.8.0) in both modules, replacing T-18's `v0.7.1`.
It carries G5-270's `recorder_instance_name`, which P2 (device selection, applied by T-24) uses for the device name,
and still reads and writes everything v0.7.1 does: a descriptor without the tag decodes as before.
A pack holding the tag is format minor 2 and exports as `tracepack-jsonl/3`.
Consumers and mergers must run v0.8.0 before any producer writes the tag,
so the SDK release that writes it follows the service's upgrade.
Taken before any `go.mod` exists, so nothing is built against v0.7.1.

## Later owner decisions

Taken by the owner after the repository bootstrap began, each dated.

### T-24 — device selection (2026-10-25)

P2 (device selection) is accepted and applied; its last text is at commit 024516d.
A reader selects traffic by (`tool_id`, device), the device being the gateway's configured `eqp-hsms` device name,
carried in the descriptor as `recorder_instance_name` (G5-270).
`recorder_instance_id` is a UUIDv5 of `tool_id` and the device name, derived by the SDK from each capture's descriptor,
so the SDK's `Config` holds no identity and the gateway's `AppID` is not part of it.
Overlapping captures of one device are flagged and never closed while live.
Applied to `producer.md` §4.2 and §4.5, `producer-sdk.md` §4.7, `catalog.md` §7.1 and §7.3, `compactor.md` §8.2, `api.md` §10.1,
`rollouts.md` §6.4, `positioning.md` §2, `glossary.md` and `.agents/rules/600-invariants.md`.
Not verified: whether two deployments with different `AppID`s serve one tool in practice;
if they do, they are reported as overlapping captures.

### T-25 — flush interval (2026-10-25)

`ingest`'s flush interval F defaults to 5 minutes,
the value the catalog's sizing implies, until measurements on real traffic tune it.
It joins the service defaults of T-12.
The bounds it touches:
a consumer's `ack_wait` must exceed F plus the upload ([STO §4] *Stream settings*), so more than 5 minutes,
and Q = 15 min must exceed F plus `ack_wait` plus redelivery slack (§7.3),
which leaves under 10 minutes for `ack_wait` and the slack together;
raising F later means raising Q with it.

### T-26 — module interfaces (2026-10-25)

P1 (module interfaces) is accepted as a whole;
each of its six items is applied as its own change to the spec files it names, item 4 before the SDK's first code.
For item 4 the SDK's lifecycle vocabulary is `Begin`, `Rotate` and `End`:
the SDK does not take the gateway's verbs, and `producer.md` maps the device's `Start`, `Stop` and `HotReload` onto them once.
Item 5's token protocol is reviewed with the eqp-hub maintainers before phase G.

### T-27 — local workspace (2026-10-25)

`go.work` and `go.work.sum` are not committed; they are gitignored, and `make work` creates `go.work` with both modules, as in go-secs.
A committed workspace can make a build pass against local modules that a consumer's `go get` would not see,
and it overrides a workspace a developer sets up for wider work, such as one that adds go-secs.
The rest of T-17 stands:
the service's `go.mod` requires the SDK by version and carries no `replace`,
CI builds the service with `GOWORK=off`, and the SDK is never built through a workspace.
When the service starts importing the SDK, a CI leg that creates the workspace can verify a change spanning both modules.

### T-28 — nats.go upgrades and the claim bucket's reads (2026-10-25)

Two decisions after the T-19 re-verification, which found no nats behavior the specs rely on changed in nats.go v1.55.0 or nats-server v2.15.1.
The SDK raises its nats.go requirement beyond v1.55.0 only for stability and upstream fixes, never for a feature,
since every raise also raises each producer that imports the SDK.
The claim bucket's stream has `AllowDirect` off:
nats.go creates object stores with it on, so `tracekeep migrate` turns it off after creating the bucket,
`busconfig` records the setting and `Preflight` refuses a bucket that has it on,
so the SDK's never-overwrite check reads the stream's leader and never a lagging replica (`producer.md` §4.2 *Large records*).

### T-29 — clock-step tolerance (2026-10-25)

The tunable ε that the operations table lists is the producer's clock-step tolerance of `producer.md` §4.2,
written into each descriptor as `clock_step_tolerance_ns` ([SEM §4]).
Its default is tracepack's `DefaultClockStepTolerance`, one second (tracepack v0.8.0):
it keeps the operating system's slewed corrections from being recorded as steps,
and a larger value would widen the gap by which a consumer closes a capture its producer left open ([STO §4]).

### T-30 — module interfaces applied (2026-10-25)

P1 (module interfaces) is applied in full and closed; its last text is at commit a9ae497.
Its six items now live in the specs:
the catalog's domain transactions (`catalog.md` §7.2–§7.3),
ingest's disposition classifier and the one deletion predicate (`ingest.md` §6.1, `compactor.md` §8.4),
the parking coordinator (`parking.md`),
the SDK's lifecycle, observer options and narrow `Bus` (`producer.md` §4.2, `producer-sdk.md` §4.7),
the send-path token protocol and `TimeoutSource` (`producer.md` §4.4, §4.6, `producer-sdk.md` §4.7),
and optional presigning with an API download fallback and the `keys` module (`storage.md` §5.2, `api.md` §10.1, `masking-keys.md` §9).
The token protocol is still reviewed with the eqp-hub maintainers before phase G (T-26).

### T-31 — producer defaults, overlapping lifecycle calls and the send path (2026-10-25)

Defaults the SDK's first phase fixed in code without an entry, now decided:
the ordinary lane holds 16384 records and 64 MiB of frame bytes;
a fresh capture that faults again within a minute waits 30 s (`FaultBackoff`) before the next fault rotation;
the traffic stream's default name is `TRACEKEEP_TRAFFIC` (`producer/busconfig`);
every `recorder_instance_id` is derived under the namespace UUID `e4bb8a4e-8a26-46c0-8b44-79994bc40a3e`, which never changes, since changing it changes every device's identity;
and the encodings `producer.md` §4.2 and §4.5 state stand as written (the 4-byte big-endian length prefixes inside `recorder_instance_id`, the claim metadata keys `tpk-record-header` and `tpk-ts-utc-ns`).
ε is a field of `Tunables`, one value per producer, which the SDK writes into every descriptor it starts or rotates to;
that is what T-29 means by "written into each descriptor".

Overlapping lifecycle calls, on which the specs were silent:
the first `End` to take the lifecycle lock owns the capture and its `Report`;
a later `End` finds no capture and returns an empty `Report`, or `Contended` when it cannot take the lock by its deadline,
and an empty `Report` says nothing about whether the first `End`'s drain has finished.
A `Report`'s drain and drop figures are those at its return.
A `Close` during an `End` stops the publisher; the `End` still returns by its deadline and counts what is left as dropped.

The send path's rules (`producer.md` §4.6), fixed before the phase that builds its fixture;
the signatures are still reviewed with the eqp-hub maintainers (T-26):
- an entry is `live`, `completed`, `removed` or `expired`:
  progress is first-wins, a failure removes only a `live` entry, the first terminal outcome seals the entry so every later report of either kind is a counted no-op (amended by T-39), and expiry, which ends `live` and `completed` entries, is no outcome;
- expiry follows the wait, not the registration:
  the waiter reports `Awaiting(timeout)` on the token when it opens the wait, and the entry expires at that time plus the timeout plus a reporting grace (default 5 s);
  an entry never awaited is removed 30 s after its registration; both are tunables;
- an ambiguous entry counts its `TimedOut` and emits nothing;
  an entry records its socket, never an epoch;
  the T3 event goes to the capture current when the timeout is reported, with the epoch that capture's socket map holds for the socket,
  and a socket absent from that map is counted as unattributed and emits nothing;
- the integrator's obligations gain a sixth, the send path:
  register every forwarded W-bit primary before sending it, report its progress, and implement `TimeoutSource`, which the conformance check covers.

### T-32 — parking, termination and ingest serialize on the capture row (2026-10-25)

The roadmap's open parking and catalog items and the findings of an external review of the applied module interfaces (kept outside the repository), decided:
- every parking transaction runs the catalog's capture-row helper:
  *advance*, the `acked` update, a copy row's insert, operator termination, an order's application,
  and the move `parked → evicted-hour`, which `EvictHour` and the parked consumer make, each by compare-and-set;
- a redelivered original whose version is `evicted-hour` is published like one whose version is `parked`;
- termination and every ingest effect of a capture serialize on the capture row, and whichever commits first wins:
  `Admit`, `Reject`, `BeginParking` and ingest's `RegisterPack` take the delivery's identity
  and re-check under the row its version's resolution and the capture's active termination orders;
  a terminated or covered delivery returns `terminated` and writes nothing else,
  and a `RegisterPack` that finds one aborts and tombstones the key in the same transaction,
  so `capture terminate` either precedes or follows each batch in flight;
- `RegisterPack` takes delivery evidence (each record's version key, bus incarnation and stream seq) as an input;
  the reconciler and a close's publication pass none and write coverage only;
  the reconciler refuses, tombstones and deletes an orphan that holds a terminated seq or belongs to a capture with a termination order;
  an adopted pack never gains evidence later, because a redelivery writes another segment under its own pack id,
  and an `ingest` registration that finds its own key already adopted leaves its batch unacknowledged so that redelivery happens;
- `Reject` on an admitted capture writes nothing:
  different bytes return `descriptor-conflict`;
  equal bytes return the admitted state and its processing configuration, and the classifier decides again;
- admission serializes with membership on a sentinel row, `counters('membership')`,
  which every binding path of `Admit`, every member registration, renewal and leave, and every configuration activation and retirement updates;
  the lock order is capture → `leases` → `counters('membership')` → `counters('scope_events')`, each step optional;
- `RecordConflict` takes the pack ids it compared and clears only while the row names exactly those, otherwise it returns `changed`;
- a close's registration and its publication are one transaction, the event seq allocated last;
  a close the horizon guard refuses, or whose key retention already tombstoned, leaves its intent `abandoned` and its key tombstoned;
- every compactor transaction that changes domain state checks the lease.

### T-33 — deferred and terminated seqs, change events and download lifetimes (2026-10-25)

- Completeness reads the `parked` table:
  a seq of an unresolved version is **deferred**, a seq of a terminated version a **declared loss** with its action id;
  per seq the precedence is retained > evicted > deferred > declared loss > gap, and it never hides another version's loss;
  an hour holding only parked seqs reports them instead of `no-data`,
  and classification runs up to the higher of `highest_seq` and the highest parked seq.
  `ListPacks` and its proto gain the class.
- A termination emits a scope event of a new kind, `terminated`, with `revision + 1` and the counter last:
  the event goes on the scope of each terminated seq's hour, which is upserted when missing, except in an hour retention has evicted, whose scope is gone;
  entering parking emits nothing, and parking touches no counter except there.
- A download URL, presigned or served by the API, expires at the earlier of now plus its lifetime and its object's deadline minus a margin.
  The deadline is the earliest time the object may be deleted:
  the hour's earliest eviction for every pack, staged and excluded packs included, or `merged_at + D` for a merged input when that is earlier,
  and for a completed export bundle the end of its own retention.
  Issuance whose clamped lifetime falls below a floor is refused with `pack-superseded`, and the VE client re-lists.
  The margin is 2 min, the floor 1 min and an export bundle's retention 24 h, all tunable;
  a later retention change never shortens what an issued URL was promised.
- Session and download keys: configuration names one issuing key id and a list of accepted ids per purpose,
  every credential carries its key id in its authenticated part,
  rotation adds the new id everywhere, then switches issuance, then drops the old id after the longest credential lifetime plus clock tolerance,
  and an unknown or wrong-purpose id fails closed.
- `storage.presign`, on by default, lets an operator turn presigning off for an `s3` store whose presigned URLs readers cannot use
  (a proxy, a different host name, a policy refusing query-string authentication);
  downloads then go through the API.
  The api role checks presigning at start and reports a store that refuses its signature.

### T-34 — the S3 client and the stores it is tested against (2026-10-25)

The `s3` backend uses aws-sdk-go-v2 (`service/s3` and its presign client), not minio-go:
MinIO's open-source server is archived and no longer maintained,
and aws-sdk-go-v2 compiles to modules of one publisher where minio-go pulls in about ten.
With a custom endpoint it uses path-style addressing and sets both checksum options to `WhenRequired`,
since the SDK's default checksums break some S3-compatible stores;
`Put` passes a seekable body.
Deployments run MinIO and Ceph RGW,
so the shared `ObjectStore` conformance suite runs against both, in containers, from the phase that builds the store;
the specs recommend no particular store.

### T-35 — where the SDK's bus is tested, and TLS as a finding (2026-10-26)

The SDK's default connection must be tested against a real JetStream,
but the SDK cannot import `internal/testbus` or nats-server without pulling the server into every producer's graph,
and the service module does not require the SDK before its first tag.
Those tests therefore live in `producer/internal/bustest`, a module of its own that only tests:
it requires the SDK and the service through `replace` directives to this repository, is never tagged and is required by nothing,
so its `replace` directives and its nats-server requirement reach no producer and no build of the service.
It runs, like the SDK, with `GOWORK=off`, so CI needs no workspace.
`New` refuses a connection given through `WithConn` only when its reconnect buffer is enabled, which correctness depends on (`producer.md` §4.2 *Large records*);
a connection without TLS is reported by `Preflight` and never refused,
so the embedded test bus and a development bus run on plain `nats://`.

### T-36 — tracepack v0.9.0 (2026-10-26)

`v0.9.0` (spec v2.38, format 1.2, still requiring go-secs v2.8.0) replaces T-23's `v0.8.0`.
Its one breaking change, G5-272, is that `WriteClose` always writes the close segment uncompressed and `CloseOptions.Uncompressed` is gone;
a close begun by v0.8.0 and retried by v0.9.0 would therefore write other bytes.
tracekeep has written no close yet, so the compactor is built on v0.9.0 from its first close and never meets that case.
It also adds `FindTransactions`, which pairs every primary of a time range in one pass (G5-271, G5-273); the API may adopt it, and nothing requires it.
Only the SDK requires tracepack today; the service takes the same version when its first package needs it.

### T-37 — the vantage of a `pdf_*` device (2026-10-26)

`producer.md` §4.5 fixes vantage host for every capture, yet lets an operator capture a device whose peer is a host-side system,
which would invert vantage and every `dir` if the gateway were the equipment endpoint there.
It is not: the legacy host system's `pdf` endpoint runs in passive mode, and eqp-hub's `eqp-hsms` device holds the host role on that connection.
A `pdf_*` capture therefore keeps vantage host with no per-device option, and capturing it needs no restriction.

### T-38 — the simulator builds as a module of its own until the SDK's first tag (2026-10-26)

`cmd/tracekeep-sim` drives the SDK, but the service module does not require the SDK before its first tag (T-17, T-27),
so in the service module the simulator cannot build with `GOWORK=off`, and CI has no workspace.
Until that tag the simulator is the module `github.com/arloliu/tracekeep/cmd/tracekeep-sim`, built like the bus-test module (T-35):
it requires the SDK, and the service when it needs `internal/testbus`, through `replace` directives to this repository,
is never tagged and is required by nothing, and runs with `GOWORK=off`.
An SDK change and the simulator code that uses it therefore land in one pull request.
A pseudo-version of `producer/` was rejected because rebase merges rewrite the commit it names;
an early SDK tag was rejected because phase 0.4 itself still changes the SDK's API.
The cost is that `go install …@latest` cannot build the simulator while it carries `replace`.
When the service first requires the SDK by tag, at the latest for phase 1's `tracekeep dev`, the simulator's `go.mod` is removed and it returns to the service module.

### T-39 — a Sent that follows the reply (2026-10-26)

The eqp-hub maintainers' review of the send path (T-26) found that a reply can arrive before the forwarding call returns,
so a gateway's `Sent` often follows the waiter's terminal outcome.
T-31 counted it as a report on a sealed entry, so every fast reply moved `ignored{reason="sealed"}`.
A `Sent` that is the token's first progress report is therefore taken on a `completed` entry too, uncounted, and changes nothing (`producer.md` §4.6);
every other report after the outcome stays a counted no-op.

### T-40 — what phase 1.4 settled about parking (2026-10-27)

The owner's answers to four points phase 1.4 raised.
**An order never undoes a registration**:
an inline record staged with no parked row, whose acknowledgement was lost, used to be terminated by an order covering its redelivery while a pack held it;
a covered message whose seq a registered pack holds now resolves as staged on every path that re-checks the orders (`parking.md` *Operator termination*, `catalog.md` §7.2 `RegisterPack`).
The check reads `pack_ranges`, which records seqs, not versions, so another version of a held seq also reads staged;
a per-version receipt for every record was rejected as a write on every registration for a capture the operator has already ended.
An evicted hour's seqs are no longer held, so an order still covers them.
**A restore changes `Created`**: on nats-server v2.15.1 a restored stream comes back under a new `StreamInfo.Created`,
so the earlier text that a restore keeps it was wrong; a changed `Created` or a rewound `LastSeq` is a changed incarnation (`parking.md`).
**A missing claim object is indefinite once its nak delay reached the last step**,
since an object a producer is still putting may yet appear (`ingest.md` §6.1); parking at the first miss and never parking were rejected.
**`parked replay` is withdrawn** from the admin commands:
the parked consumer retries on its own after `parking.redelivery_delay`, and an early retry needs a design of its own, kept as proposal P1.

### T-41 — what the phase 2 plan settled (2026-10-27)

The owner's answers to nine points the phase 2 plan raised before it started.
**Downloads from a store that does not presign** (`fs`, which `dev` uses, or `storage.presign` off) are served by the API in phase 2:
the `download` key purpose of `masking-keys.md` §9 comes forward whole, its binding check, `file` and `env` providers and issuing and accepted key ids included,
and phase 3 adds the other purposes.
**`ListDevices`** is built in phase 2 with the other reads.
**The catalog's migrations are applied by the binary through Atlas's Go library**, which replaces T-12's "Atlas CLI for the first release":
`ariga.io/atlas` v1.3.0 (Apache-2.0) builds its `sql/migrate`, `sql/mysql` and `sql/sqlite` packages without cgo and imports no cgo SQLite driver;
its executor needs a revision store of the service's own, since the CLI's lives in a separate module (`catalog.md` §7.4).
**`object adopt` is withdrawn**: the reconciler adopts what it can verify, and a manual adoption needs a proposal that defines it.
**`ListDevices`, `ListCaptures` and `ListScopeChanges`** are granted to `pack-reader` and to `viewer` (`api.md` §10.2).
**A download URL lives 15 minutes** by default, below D.
**`scope merge` and `capture close` are requests** in `compact_requests` that the compactor holding the lease executes at its next cycle,
so neither needs the service stopped nor gives an admin process the lease.
**An excluded input emits `excluded`**, a scope event of its own.
**The redelivery slack** of `catalog.md` §7.3 is `compact.redelivery_slack`, default 2 minutes,
and Q is validated strictly above F + `ack_wait` + slack (5 + 7 + 2 < 15 minutes).

### T-42 — what the phase 3 plan settled (2026-10-29)

The owner's answers to nine points the phase 3 plan raised before it started.
**Thinning takes its provenance from a per-request origin, not a channel**:
a dedicated interval-only channel changes the production gateway too much,
so `fdc_grpc`'s `buildRequest` marks a request interval-only when every context it consolidates is a kernel DC task,
the mark travels on the hub's message to the send path, and the SDK's registration takes it in place of the channel;
a request holding a proxy context, and everything `buildRequest` does not build (drain resends, post-event and MES commands), is unmarked and never thinned,
so a lost mark costs a thin, never a wrong one.
**Every kernel DC task counts as interval polling**, event-started and timer-started alike, which answers T-3's open item.
**Parallel mode never thins**: the legacy host's polls carry no provenance.
The eqp-hub side is phase G's; until it ships nothing is marked and nothing thins.
**Exports are checked in phase 4**, whose gate gains "no clear recipe body in an export".
**`Restore` addresses a record by `(capture, seq, hour)`**, and restores it only when every non-excluded copy of `(capture, seq)` is equal in any hour;
differing versions are refused as a conflict, an acknowledged one included, and no version selector is offered.
**The `session` key purpose waits for phase 4**, with people's OIDC; phase 3 adds `digest` and `master`.
**Vault is tested** against a dev-mode container in CI and an `httptest` fake.
**A tool type's production admission** (T-6) is an operator procedure in `docs/guides/`;
the service refuses to start outside `tracekeep dev` with a policy that lacks any baseline row.
**The cutover to masking**: outside `tracekeep dev` a capture is admitted only under a configuration whose policy holds every baseline row,
a capture admitted before keeps its binding, a consumer that cannot mask cannot hold a membership and a compactor that cannot write a masked close cannot hold the lease,
and every producer whose capture predates the cutover is stopped or rotated before sensitive traffic starts, old leaders included.
The plan splits the phase into six sub-phases.

### T-43 — old consumers fenced at the masking migration (2026-10-30)

A phase 1 or 2 ingest replica decodes a configuration's content leniently and would run a masking configuration as a policy-less one,
and a replica of the release before masking rebuilds it but writes clear text,
so neither can be trusted to report that it cannot execute one.
**The migration of the release that masks fences them out at the catalog**, as it fences old compactors:
the membership tables move to names an earlier release does not use and the compactor lease to a row it does not name,
so an earlier ingest replica can neither register nor renew and an earlier compactor can neither take nor renew the lease.
Every ingest replica and every compactor is upgraded at that migration; the earlier ones fail closed and stop fetching or compacting.
A content shape earlier parsers reject was considered and not taken: it fences only once a masking configuration exists, and changes the stored content's form.


### T-44 — `Digest` takes a list of inputs (2026-10-30)

A scorer comparing one recipe digests many items, and one call and one audit row an item would multiply both by the recipe's size.
**`Digest` takes one digest key id and a list of inputs**, each a domain, a kind and an original,
at most 1,000 inputs and 8 MiB of originals a request, answered in order;
one audit row a request names the count and the distinct domains, and the rate limit counts inputs.
One input a call, as the earlier signature had it, was considered and not taken.

### T-45 — an hour merged past the metadata budget stays unmergeable (2026-10-30)

Masking puts one redaction entry a masked field in a pack's metadata, which every reader opens within tracepack's budget, 64 MiB, about 470,000 entries;
ingest keeps each segment within it, but the segments of one heavily masked hour can merge past it, and tracepack then refuses the merge.
The refusal is deterministic, since a scope's inputs only grow, and retrying it every cycle re-reads every input and withheld the merge task's success for good.
**Such a scope is marked `unmergeable` once, under its capture row and the lease, with an `unmergeable` event**:
it is no longer a candidate, a registration leaves it so, and only an operator's `scope merge` tries it again;
its segments stay readable and are evicted on time, and the refusal is counted apart from a failure, raising its own alert.
The budget stays tracepack's default, since every reader, the VE's included, must agree on it and it bounds their memory.
Considered and not taken: raising the budget, one merge writing several archives (the format allows one archive a merge), and partitioned merges of an hour's packs (kept in reserve until production rates are measured).
The lasting fix, redaction entries outside the pack metadata, is a format change asked of go-secs.

### T-46 — an SDK change the service uses lands in two pull requests (2026-10-30)

The service requires the SDK at a tagged version and builds and tests with `GOWORK=off`,
so it can use an SDK change only once that change is tagged;
a rebase merge rewrites a branch's commits, so a tag can only follow the merge, on `main`.
**An SDK change lands in its own pull request, `producer/vX.Y.Z` is tagged on `main` once it merges,
and a second pull request raises the service's requirement to that tag and makes the service use it.**
Thinning is the first: `producer/v0.3.0` follows the SDK's pull request, and the second drives thinning from `cmd/tracekeep-sim`.
Considered and not taken: one pull request whose service side requires a pseudo-version of the branch or a `replace` of the local module,
since the first names a commit the rebase merge discards and the second does not build from a tagged release.
