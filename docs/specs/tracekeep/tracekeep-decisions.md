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
G5-198, G5-200, G5-201, G5-207, G5-208, G5-211, G5-213, G5-214, G5-216, G5-217, G5-218, G5-220, G5-222, G5-224, G5-225, G5-227, G5-231, G5-233, G5-236, G5-249, G5-250, G5-251, G5-252, G5-257, G5-258, G5-262, G5-267, G5-268, G5-269, G5-270, G5-302, G5-303, G5-304, G5-305.
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

**Measured again (2026-10-31)** for `QueryRecords`' batching, as the line above asked:
connect-go v1.21.1 on Go 1.26.10, one process over loopback h2c, the Connect protocol, compression off,
a server stream of 4,096 records of 1.5 KiB each (a masked record of about 512 bytes with about 1 KiB of SML), random bytes,
medians of five `testing.B` runs on the same host.

| records per message | stream time | per record |
|---|---|---|
| 1 | 17.5 ms | 4.3 µs |
| 16 | 5.1 ms | 1.2 µs |
| 64 (96 KiB) | 3.1 ms | 0.75 µs |
| 256 (384 KiB) | 2.4 ms | 0.59 µs |

One record a message costs about 3.5 µs more per record, as the first measurement found;
batches of about 100 KiB recover most of it, and quadrupling them gains little more while a browser waits longer for its first message.
**So a `QueryRecords` message holds items up to 128 KiB encoded** (`api.md` §10.1 *Records*).

### T-8 — repository name

Repository name (V-29 is provisional).

**Decided (2026-10-25):** `github.com/arloliu/tracekeep`.

### T-9 — the producer's residual wait

Accept, under LS-14, the producer's one residual wait:
a callback may wait for another callback to leave the capture's critical section (§4.2), a dependency on scheduling and on nothing external;
or require a different producer algorithm.

**Decided (2026-10-25):** the residual callback wait accepted under LS-14 (superseded in part by T-52, which admits the publisher's reads).

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

### T-24 — device selection (2026-10-25) (identity superseded by T-59)

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

### T-47 — the thinning parameters are read from the stored descriptor (2026-10-31)

A `thinning` entry's `source` carries the sampling rule as key=value text tracekeep owns (`producer.md` §4.6),
and `ListCaptures` names a capture's thinning parameters (`api.md`).
**The catalog parses them from the descriptor bytes it already stores, when a capture is read;
a `source` that does not parse leaves the parameters absent and its text kept, and never refuses the capture.**
A producer of another version, or another tool's text, then costs only the parameters,
and the catalog needs no columns or migration for values the descriptor already holds.
Considered and not taken: parsing at admission into columns of `captures`, which adds a migration for no reader that needs them indexed,
and refusing a capture whose `source` does not parse, which would lose traffic over a descriptive field.

### T-48 — what the phase 4 plan settled (2026-10-31)

The owner's answers to twelve points the phase 4 plan raised before it started; three were corrected after the plan's first outside review, and the owner was told.
**No holds**: T-4 stands, and the roadmap row's holds, left from seed v21, are dropped.
**`FindTransaction` joins `QueryRecords`**, over a pack source that answers every hour the lookup asks for; `FindTransactions` stays unadopted.
**`Export` ships `kind=canonical` only**:
tracepack's per-record JSONL writer is not exported, and the payload format lives in go-secs,
so `kind=filtered` is refused until go-secs exports a filtered writer.
**The first pages** are the tool and time browser with scope completeness, the record viewer and export;
SML is rendered by the server from go-secs; the restore and admin pages, and the typed admin operations, come later.
**People need an IdP**: the static-token provider stays a service credential.
**Every accepted session key id is resolved at start**, a failure failing the start,
so a started `api` replica validates every cookie without the key provider;
a replica started while the provider is down does not start, and those already running keep serving.
**The frontend build is optional for the Go build**: a tracked fallback page is embedded beside `web/dist`,
and a release binary and the image are built after `make web` and never ship the fallback.
**The gate runs the real packaging**: the Helm chart on kind and the systemd unit on the CI runner, both driven by a browser,
as CI jobs of their own, the testing rule's one exception to "no test reaches a network".
**Human reads see masked packs only, from one read set**:
outside `tracekeep dev`, a pack whose capture's configuration has no policy is withheld from `QueryRecords`, `FindTransaction` and `Export` and named as withheld;
the three read an hour's `staged` packs (its archive and the segments registered after a reopen) and its `excluded` packs, never a `merged` input.
**The image** is a static, distroless build for amd64 and arm64, published as `ghcr.io/arloliu/tracekeep` (its publication deferred by T-50).
The plan splits the phase into six sub-phases.

### T-49 — the frontend's stack and `tracekeep dev`'s sign-in (2026-10-31)

Phase 4.4 asked how a developer's browser signs in to `tracekeep dev`, which served plain HTTP with a static token,
when people's sign-in needs TLS and an IdP and the static token is a service credential (T-48),
and which components and styling the frontend uses.
**`tracekeep dev` serves the API over TLS with a self-signed certificate and runs a test IdP of its own**, `internal/devidp`,
so people sign in under it by the same authorization code flow as in production, and the gate's standalone leg can reuse it (superseded by T-50: both legs sign in at Dex);
the service trusts it through a new `auth.oidc.ca_file`, which a private IdP in production needs as well.
**The frontend is Vite, React and TypeScript with shadcn/ui on Tailwind CSS v4, TanStack Router, Query and Virtual**,
and no framework that needs a server, since the build ships embedded in the one binary (`api.md` §10.3).
Considered and not taken: a loopback principal that `dev` admits without a credential, which leaves the sign-in flow untested locally and loosens dev;
an external IdP each developer runs, which every developer must set up;
plain CSS without a component library, and Mantine, for the owner's preference of a current React stack whose components the repository owns.

### T-50 — the packaging's IdP, dependencies, publication and test triggers (2026-10-31)

Phase 4.5 asked which IdP the packaging tests sign in at, what the Helm chart installs beside tracekeep, whether the image is published, and when the browser-driven packaging tests run.
**Both packaging tests sign in at Dex**, its mock connector signing a fixed person in with a `groups` claim,
a real OIDC provider that needs no password form; this replaces T-49's remark that the standalone leg could reuse `tracekeep dev`'s test IdP, which stays dev's and tests'.
**The chart deploys tracekeep alone**: the bus, MariaDB, the object store, Vault and the IdP are the cluster's own, named by values, never subcharts.
**The image is built, not published, in phase 4.5**: its shape and name stay T-48 point 11's (`ghcr.io/arloliu/tracekeep`, static, distroless, amd64 and arm64),
and a release workflow that pushes it waits for a later phase; a deployment builds it meanwhile.
**The packaging tests run on `main`, daily and when dispatched**, not on pull requests, which build the image for both platforms instead;
a packaging fault a pull request introduces is found on `main` and fixed forward.
Considered and not taken: tracekeep's own test IdP packaged as a binary, which is no real IdP and would widen what may start it; Keycloak, whose size and start time cost every run;
optional subcharts, which pin versions the clusters already own; an `edge` image on every merge; the packaging tests on every pull request.

### T-51 — tracepack v0.13.0: an unknown layout waits, a quarantine needs evidence (2026-11-01)

tracepack v0.13.0 gives its errors a public taxonomy and reports a bus message of a `Tpk-Layout` it does not know as `bus.ErrUnsupportedLayout`, a newer producer's message, instead of `bus.ErrMalformed`.
**Ingest defers such a message**: nak with delay, the `unsupported-layout` alert, never terminated, never a seq gap, never parked, since it has no version to park,
and its claim object kept until its hour's retention as any other (`ingest.md` §6.1);
consumers that know a layout are deployed before any producer emits it (`rollouts.md`), so the message waits for the upgrade and is lost only to the stream's own retention.
This replaces the malformed disposition of an unknown layout, which counted a message no consumer could yet read as a seq gap.
An absent `Tpk-Layout` is deferred too, as tracepack's check reads it.
**The reconciler quarantines on evidence of the object's bytes only**: `Verify` failing with `ErrNotTracepack`, `ErrCorrupt`, `ErrNotFinalized` or `ErrReadLimit` quarantines the object;
any other error, `ErrUnsupportedFormat` and the store's own read failing included, leaves the key for the next pass (`storage.md` §5.4),
where every error but the store's quarantined before, a newer module's pack included; since v0.13.0 marks every fault of a pack's bytes with a sentinel, the store's read errors need no tag of tracekeep's own.
Considered and not taken: parking the message, which needs the version the unknown layout hides; terminating it after a bound, which turns a deployment's order into a loss.

### T-52 — what the phase 5 plan settled (2026-11-01)

The owner's answers to ten points the phase 5 plan (correctness, error handling and performance) raised before it started; points 9 and 10 came from the plan's outside review.
**Phase 5 sits after the seed's milestones**, in a roadmap section of its own, so the milestone table stays seed v21 §12 unchanged.
**Spec changes the phase makes**: a store failure fails an export attempt rather than leaving a pack out of a `done` bundle;
`FindTransaction` answers `not_found` for an unknown capture and a non-retryable code for a corrupt pack;
a restore takes a read slot, bounds the copies it lists and the readers it opens, and refuses what it cannot compare whole;
a scope whose merge keeps failing backs off, and a cycle merges a bounded number of scopes;
the producer retries a transient claim read and re-puts only content proven wrong;
a client's cancellation and deadline answer `canceled` and `deadline_exceeded`;
discovery and JWKS are fetched ahead of their TTL, never used past it;
the SDK reports its counters from zero;
a stream whose send makes no progress for a configured time ends.
Not now, decided again from measurements: batched parking acknowledgements, a derived-key cache or a capture key per fetch batch, stale JWKS after a failed fetch.
Not at all: a cap on `QueryRecords`' window, and a quiet `/auth/me`.
**A link to the bus or the store without TLS is warned of at start, never refused**, so no installation stops starting.
**go-secs' share stays out of the phase**: T-45, the format-minor accessor, the filtered writer, tracepack's epoch-hour text;
the first S1F3's `ordering-uncertain` stays open, its two read hypotheses sent to go-secs.
**SQLite's one write connection** is relieved by shorter write transactions, and an API request's audit write gets a deadline and a wait histogram;
a priority gate only if measurement still shows a starved write; `synchronous` stays `FULL`, and the audit log stays in the catalog.
**Performance work is measured first**, before and after, and a finding not worth its change closes with its number.
**The publisher's reads of a capture's reservation state under its lock are admitted**, beside T-9's wait for another callback:
they hold no I/O, allocation, logging or call into go-secs, the two reads of a pass merge into one, and the publisher's external waits stay outside the lock.
**A claim payload is bounded by `ingest.max_record_bytes`**, default 64 MiB, as a resource refusal, never a malformed record:
an unresolved claim declared above it is deferred unfetched, `too-large`, until the bound is raised or the version terminated;
a read takes at most the declared length plus one byte.
The plan splits the phase into six sub-phases, correctness before error handling before performance, and sets the gate, including a `PackURL` answering within 2 s at the 99th percentile on SQLite during sustained ingest.
Considered and not taken: refusing a link without TLS, which stops installations that rely on one; a lock-free mirror of the reservation state, which puts the dependency barrier at risk for a wait the callbacks already accept from each other;
a separate audit database file, whose rows could not commit with the domain rows they record.

### T-53 — what the phase 5 plan's error handling settled (2026-11-02)

Ten points the phase 5 plan raised before its error-handling sub-phase, read from the code and settled on the plan's recommendations under the owner's standing instruction.
**What an answer says about a dependency**: an `unavailable` or `internal` answer of `MachineService` carries a fixed message naming what failed (the catalog, the store, a key provider, the audit), never an error's text;
tracekeep's own refusals keep their text, an hour in one written in RFC 3339.
`AdminService` answers its operator, so it may carry what the operator's log may, a catalog's cause included; a failed audit write answers `internal` there too.
**What a log may carry**: a catalog's cause, a bus error's text and a key provider's error with its reference;
never an object key, a store's error text, a URL's query or user information, a token, a password, a secret, a cookie, an authorization code or a clear payload,
so a boundary whose error may hold such material logs fields it builds itself (`operations.md` §10.4 *Logs*).
**A client gone** is decided once, where the handler returns: tracekeep's refusal keeps its code and an audit failure stays `internal`;
otherwise an `unavailable` or `unknown` answer after the request's own context ended becomes `canceled` or `deadline_exceeded`, outcome `cancelled`, never logged as a failure;
a dependency's deadline with the request live stays `unavailable`, as does a download URL that could not be made.
A download whose client is gone counts under 499; one cut after its status is sent ends short and counts under that status (`api.md` §10.1).
**A pack that does not open as one of its capture** answers `FindTransaction` with `data_loss`, an unknown capture `not_found`, a missing or unreadable pack `unavailable`.
**A restore past its held bytes or tracepack's read limit** answers `resource_exhausted`, never a comparison of part (`masking-keys.md` §9).
**An export's own work file failing** fails the attempt as a store failure does, so no pack is left out for it.
**The SDK reports every counter and label value from zero**, counts observer panics and the log lines its sink drops, logs its own failures as fields it builds,
and `Close` cuts emission off before one final report (`producer.md` §4.3 *Diagnostics*).
**The service's bus connection logs** its loss, restoration, asynchronous errors and a close with an error, each server redacted.
**A link without TLS** is warned of once at a role's start, the bus by its connection's TLS state and the store by an `http://` endpoint (`650-perf-sec.md`).
**New maxima**: `auth.oidc.leeway` 5 min, session lifetime 24 h, login lifetime 1 h, token exchange 1 min, `jwks_ttl` 24 h, its fetch interval 1 h and below the TTL, `keys.vault.timeout` 1 min, `ingest.stop_timeout` 10 min, `ingest.max_record_bytes` 4 GiB.
**A retried catalog transaction** waits a random time under a cap doubling from 10 ms to 1 s and stops before a further run once 16 runs are made or 30 s have passed, never cutting a run in flight (`catalog.md` §7.2).
**Discovery and the JWKS** are refreshed in the background by a request past half their TTL, under the verifier's own context, which its `Close` cancels and joins;
a failed fetch is logged by fields, never its text (`api.md` §10.2).
Considered and not taken: fixed messages on `AdminService`, whose reader is the operator; a panic alert, the runbook reading the counter instead; a service metric for bus events;
warning of the catalog's DSN, of `admin` and `status`, or at a reconnect; a retry bound that cuts a run in flight; `Last-Modified` on downloads, which the store's interface cannot give; maxima on the older durations.

### T-54 — what the phase 5 plan's measurement settled (2026-11-02) (count superseded by T-61)

Nine points the phase 5 plan raised before its measurement sub-phase, read from the code and settled on the plan's recommendations under the owner's standing instruction, amended by the plan's external review.
**Where a baseline comes from**: one local machine for every package and the catalog on SQLite; the catalog on MariaDB in CI, by a workflow started by hand that measures a base twice and a head once in one job.
**When a change regresses**: each metric of a benchmark at `-count 10` is judged by the 95% bootstrap interval of the ratio of head's median to base's;
the allowance is 5% plus the base's repeatability, `abs(median(base₂) / median(base₁) - 1)`; above it regresses, within it passes, across it is measured again;
allocations are exact when the base's samples all agree, and judged as time is when the base's own samples vary; bytes from a zero base are exact; a missing row, other parameters or other sample counts fail (`650-perf-sec.md`).
**What a baseline records**: the commit, the Go and benchstat versions, the engine and its durability settings, the core count and class, the filesystems of the work, never a host or a vendor's model.
**Benchmarks stay alive**: each runs once at its small size in CI, the catalog's on both engines, its larger sizes only under `TRACEKEEP_BENCH_SCALE`.
**Each benchmark does the same work in each iteration**, its starting state, reset and asserted work stated, a growing table bounded and recorded.
**Registration is measured with its evidence**, since the fixtures before it registered none.
**P4's predicates** are candidates for the measurements to choose among: two have no production caller, two need their queries rewritten before an index can serve them.
**The write-connection case** loads `tracekeep dev` with many simulated tools, judges a rate sustained by the slope of the backlog's and the producers' queues' minima and maxima,
offers half the highest sustained rate, and fails a `PackURL` run on any failure but a bounded supersession.
**pprof** is opt-in on the metrics listener (`operations.md` §10.4).
Considered and not taken: passing a comparison only because no increase is significant; a single simulated tool for the write-connection case; asserting query plans before an index exists.


### T-55 — what the phase 5 plan's catalog performance settled (2026-11-03)

Nine points the phase 5 plan raised before its catalog performance sub-phase, read from the code against the measured baselines and settled on the plan's recommendations under the owner's standing instruction, amended by the plan's external review.
**The range probes' bound**: `pack_ranges` gains `span`, a virtual column the engine computes, indexed with the capture;
a probe for the ranges meeting `[first, last]` reads only `seq_from >= first - m`, `m` the capture's largest stored span, read in the probe's statement or snapshot (`catalog.md` §7.1).
The engine maintains it, so an older writer during a rollout keeps it true, no backfill runs, and an eviction lowering it is correct.
**Registration's evidence in sets**: one transaction a pack, with every outcome, its precedence and its tombstone reason unchanged; the parked rows and the termination orders read once a pack; multi-row inserts.
**The claim listing in bounded transactions**: written in chunks under the lease, only what it adds, the unattributed sweep last (`compactor.md` §8.3).
**Deletion and eviction**: the deletable merges' deadline sargable, and indexes for the deletable merges and an eviction's conflicts and merges; the audit listings and the prune take none, measured too small to pay for one.
**Listings batched, not windowed**: completeness answers a whole capture, so the listings read their inputs for chunks of captures in one snapshot.
**The standalone audit write**: under a deadline of 1 s within the request's own, its wait for a connection measured (`api.md`, `operations.md` §10.4); no priority over ingest.
**Schema**: two generated migrations per engine, the column then the indexes, so each runs online on MariaDB.
**Before and after**: the base is the branch once its benchmarks are corrected; the benchmark workflow runs only MariaDB's rows, one job per group.
**The write-connection gate** runs after every production change.
Considered and not taken: a span kept by the application (an older writer would not keep it); an index on the range's end (it bounds only probes near the end); windowing completeness; priority for the audit over ingest; indexes for the audit listings.

### T-56 — what the phase 5 plan's remaining performance work settled (2026-11-04)

Nine points the phase 5 plan raised before its sub-phase on ingest, reads, the compactor and the API, read from the code against the measured baselines and settled on the plan's recommendations under the owner's standing instruction, amended by the plan's external review.
**Measure first**: why ingest stops keeping up is found before any ingest change, by profiles at its ceiling, the same run with the consumer's pending budget raised past the held records, a claim-check load and a run at the production flush and `ack_wait`, judged by its completed drain cycles; a ceiling that moves with the budget alone is a configuration's.
**Closed with their numbers**: the filesystem store's put concurrency, a merge's peak memory, the key provider's contention, the per-message masking keys and the inline acknowledgement; a parked copy's batched acknowledgement is deferred to the gate's recovery measurement, a scope decision, not a measured one.
**A read's configurations**: decoded once an RPC or an export attempt, every read set's id still checked (`api.md` §10.1).
**A narrow read filters before it copies**: the records reader clones only the records it keeps, its cursors, resume position and reported defects unchanged.
**The orphan pass's look in sets**: one read of up to 128 keys' standings, advisory as before, registration and deletion keeping their own guards.
**A failing merge backs off and a cycle's merges are capped** (`compactor.md` §8.1, §8.3): keyed by the snapshot's revision, so new work makes a scope due at once; an operator's request bypasses the wait once; the state is the compactor process's memory, forgotten on a restart or another holder's take; a cooling scope withholds the merge task's success as its failure did;
the cap, `compact.merge_attempts`, counts requests and scheduled scopes, requests first, and never closes, deletion or retention, so a `scope merge` waits past its next cycle only behind more than a cycle's worth of earlier requests.
**Admitted only if measurement shows it**: each remaining ingest, store and API change is written only past an admission threshold taken from measurement, at the base or at a checkpoint after the change it depends on, and merged only past a benefit threshold against the commit before it;
concurrent acknowledgement tracks completion by delivery, never as a prefix; a store read opened by its catalog size checks that size against the first ranged read, falling back to the store's size for one below a pack's header.
**Before and after**: the base is the branch once its benchmarks are added, judged as the catalog's sub-phase was; the benchmark workflow runs only if the catalog changes.
**The write-connection gate** runs again on the final head.
Considered and not taken: a backoff kept in the catalog (a schema for a best-effort schedule); a derived-key cache; batching a parked copy's acknowledgements now; a flush budget that grows with `ack_wait`.

### T-57 — what the phase 5 plan's gate settled (2026-11-05)

Five points the phase 5 plan raised before its gate, read from every gate case against the test bodies, the packaging runs and the measurement record, amended by the plan's external review;
the owner set the recovery drain's thresholds and the performance case's wording, the rest follow the specs under the owner's standing instruction.
**No object key in a log** (`operations.md` §10.4): a field holding a key is a key, while a claim object's name, its version's capture and seq, is what a log already names; each boundary is cleaned where it is crossed and by the error's origin, never by trimming text at the log call:
a store's own error through `Keyless`, a callback's error through a store call unchanged, a catalog's or the bus's through the API's text sanitizer, a start's URL error by its structure; every outcome the code reads from an error (a lease lost, a missing claim, an object's end) kept.
**A recovery backlog's drain**: measured beside the offered rate from a harness event that releases a rejected capture's parked records to every copy acknowledged by identity, the traffic judged by completed flush cycles within one episode;
a parked copy's batched acknowledgement is admitted if the traffic falls behind, a paced `PackURL` p99 passes 2 s, or the acknowledgement takes a fifth of a drain longer than a compactor cycle, each condition true, false or unknown, an unknown never read as false;
admitted, its protocol goes into `parking.md` and is reviewed before its code.
**The performance case**: every change has its before and after, and no benchmark regresses beyond its noise but by the costs the owner kept; a row still inconclusive is measured once more and, if still so, recorded and not passed.
**A test's budget starts on cue**, once the stalled call waits, never covering the test's own setup.
**Evidence on `main`**: the measurements of the catalog's and the paths' sub-phases stand on `main`, whose code outside tests matches their final commit; the clustered runs, the packaging run and the write-connection gate run again on the gate's merge, and the gate closes once they are recorded.
Considered and not taken: cleaning every error with `Keyless` (it would make a catalog failure the store's and lose a lease lost); judging a short drain over windows shorter than a flush; pooling windows across episodes.

### T-58 — a parked copy's acknowledgement stays one at a time (2026-11-06)

The phase 5 gate's recovery drain, read at 100,000 parked records under T-57's thresholds, closed batching a parked copy's `PrepareAck` and `AckCopy`.
**Measured**: one episode beside 800 fwd/s over 100 tools, dev's flush of 10 s, `max_ack_pending` 600,000; the drain took 1 m 53 s from the release to every copy `acked`, the acknowledgement 58% of it at 0.65 ms a copy;
the traffic sustained over 50 flush cycles, `PackURL`'s p99 4.75 ms over 1,121 calls, nothing dropped.
**Why closed**: every condition T-57 set is false: the traffic kept up, the p99 stayed below 2 s, and the acknowledgement's share counts only in a drain longer than a compactor cycle (5 min), which this one was not.
**What reopens it**: a drain at a larger backlog or on a slower bus that passes a compactor cycle with the acknowledgement a fifth of it, or traffic falling behind during one; the measurement is `TRACEKEEP_LOAD=recovery`.
Considered and not taken: batching for the acknowledgement's share alone (a new protocol for confirmations stopped halfway and lost bookkeeping, which the thresholds admit only when a condition is true).

### T-59 — equipment, source and device (2026-11-06)

A capture's identity is rebuilt on three layers, superseding T-24's identity; T-24's selection by device stays.
**The term**: `tool_id` becomes `equipment_id`, and "tool" becomes "equipment", in tracekeep's documents, code, wire, catalog and SDK, as in tracepack (G5-303);
the gateway and the VE keep their own term and map it to `equipment_id` at the SDK's boundary.
**Three layers**:
`equipment_id` names the equipment and is the key of every read.
A source is the deployment of a program or service that records traffic, a gateway's or a log feed's,
named by the deployment and stable across restarts and leader changes, never a host or pod name.
A device is a unit attached to the equipment: the main machine, a sensor, a load port.
A link that is no unit, a host-facing shadow link or a peer such as an FDC system, belongs to the unit it serves.
A source records one link per device, the traffic actually in and out of that device's HSMS link;
the traffic of a parallel or shadow run is not recorded.
**A series** is one source's recording of one device of one equipment (G5-305):
`recorder_instance_id` is a UUIDv5 of (`equipment_id`, device, source) under the SDK's namespace,
`recorder_instance_name` the source's name, and `device` the descriptor's own field (G5-304), which tracekeep requires on every capture.
Chaining, unregistered intervals and overlap are per series, as before;
two sources recording one device are two series, never an overlap.
**Reads**: a selector names `equipment_id` and, optionally, devices and sources, with its window; every result names its source.
**The gateway**: the SDK's `Config` requires `Source`, reversing T-24's `Config` that holds no identity;
the equipment id and the device come from configuration as before.
**No compatibility**: with no deployed producer and no data to keep, the change is breaking throughout:
one catalog migration, producer v0.6.0, and tracepack v0.15.0 required, which rewrites its formats 1.0–1.2 in place (G5-303); nothing reads the old descriptor.
It is delivered by its own plan, before the host log feed's phase.
Considered and not taken: the device kept in `recorder_instance_name`, a field tracepack gives the deployment (G5-270);
a `recorder_instance_id` naming the source alone, since chaining, unregistered intervals and overlap are per device;
a connection layer below the device, since one link per device is recorded;
sources of fixed kinds, which would not tell two deployments of one kind apart.

### T-60 — the host log feed accepted (2026-11-06)

P3 (host log feed) is accepted on T-59's identity, with the open points the owner settled on 2026-11-06.
Its design goes into a new living spec, `logfeed.md`, and the specs it amends, in a later change; the proposal stays active until they carry it.
The proposal's decisions 2, 3, 5–8 and 10 stand as it states them;
1, 4 and 9 are superseded below, and so are its per-source configuration, its consumer settings and its capture key.
The proposal's source, the host a file was tailed on, reads here as the equipment, and its connection as the device;
a source is T-59's recording deployment, so decision 7's lease groups hash the equipment and decision 10's registration is per equipment's flush.
**Ownership**: tracekeep owns the feed's message (stream, subjects, message id and the versioned field schema), the pairing, `sml.Parse` and the building of frames;
go-secs owns the tracepack text for a live converter, G5-302 (tracepack spec v2.42, documentation only).
**Positioning**: the host program's log is the legacy route, converted best effort; the gateway's capture stays the preferred record of an equipment.
A log capture is read by T-59's selector like any other, its source telling it apart, so the reads leave out no method by default (supersedes decision 1).
Its vantage is `host`, its directions the host program's own.
**No per-equipment configuration** (supersedes decisions 4 and 9):
the pipeline gives every entry its `equipment_id`, its source (the feed deployment's name) and an IANA `tz`; tracekeep learns the equipment from the feed.
One global rule table maps the header's device-number token where the host logs one, else its session ID, to a device's name, with per-equipment overrides for exceptions;
an entry no rule matches belongs to the main device, `eqp` by default, so there is no host capture and no unmapped-connection alert,
and a body without a header or an unclassified entry is an annotation in that capture.
A reply takes its primary's device through pairing.
Where the host logs no device number, the main machine and a peer on stream 99 share a session and are not separated.
**Keys**: subjects `tklog.<equipment_id>`; the capture key (source, `equipment_id`, device, file); the file offset per (source, `equipment_id`, file);
the lease groups hashed by `equipment_id`.
A file is the host program's dated log file: its offsets deduplicate, it bounds a capture, and it is the capture's `source_ref`.
**Time**: tracekeep converts each entry's logged local `ts` in its `tz` to UTC and stores `ts_utc_ns` as traffic does;
a local time that occurs twice or never takes the earlier reading and is `ordering-uncertain`.
An entry without a valid `tz` is one tracekeep cannot read; tracekeep has no time-zone setting.
**Consumption**: the feed consumer takes ingest's `ack_wait` and its in-progress-ack heartbeat (`architecture.md` §3.1), not an `ack_wait` of 2 × F without in-progress acks:
phase 5 measured the heartbeat at 1.02 in-progress acknowledgements a message and its removal at 1.5% of the bus server's CPU,
and reverted the protocol that removed it (`git show 89a01d8:docs/plans/phase-5-hardening.md`, P10).
F defaults to ingest's (T-25).
The feed shares the traffic cluster only.
Its stream has no incarnation row: `migrate` ensures it, and preflight checks its subjects against both existing streams,
the log prefix checked in the service, never in the SDK's `busconfig`.
**Catalog**: the flush registration of a lease group's converter checks the group's fence in its transaction.
The reconciler deletes a log capture's orphan segment and never adopts it:
an adoption would raise `highest_seq` without the file offsets, and the redelivered entries would be converted again under new seqs.
The capture-key and file-offset rows are deleted with their captures, a file's offset row with its last capture.
Admission waits for a masking configuration as traffic's does: until one holding the baseline is active, nothing is admitted or acknowledged,
and the entries wait in the stream within its `max_bytes`.
**Operations**: `capture stop` on a log capture records a stop request, which the converter holding the equipment's lease group writes at its next flush, a later entry of the file being late;
with no converter running, the stop is written to the catalog row directly.
`capture close` and `capture terminate` refuse a log capture, and a retirement's quiescence check for one asks only for its stop.
`tracekeep status` shows the feed's backlog and the number of equipment silent for 15 minutes.
The web UI shows a capture's source and method and marks a log capture as reconstructed, claiming no coverage.
**Delivery**: one phase of five steps: the catalog's operations on both engines; the stream, configuration and preflight;
the converter, in a new package `internal/logfeed` sharing with `internal/ingest` only what holds no delivery;
the reads, the web UI, alerts, metrics and runbook; and a gate.
A pipeline guide ships with placeholder patterns only.
Test entries are generated from go-secs messages, beside hand-written synthetic edge cases.
The gate runs 2,000 simulated equipment beside traffic at K = 100, on SQLite and then MariaDB, from measured per-operation costs and one short real run.
Considered and not taken: per-source configuration of equipment, time zone and connections (2,000 entries kept in step by hand);
a host capture for an unmapped connection; a `leg` field, which nothing in the logs fills today;
the time converted in the pipeline, which would resolve an ambiguous hour silently;
an `ack_wait` of 2 × F without in-progress acks; adopting a log capture's orphan; a separate cluster for the feed.
Not verified: whether the device-number assignment is the same on every equipment, from one equipment's log; the rule table's overrides cover one where it differs.

### T-61 — test efficiency (2026-11-06)

The owner chose, on 2026-11-06, to cut the tests' and CI's time before P3, by three means, without dropping an engine or the race detector from any test.
**Catalog setup**: a SQLite test catalog is a copy of a template each test process migrates once, since SQLite's migration stays slow with its syncs off (229 ms, 620 ms or more under `-race`, 403 ms with `synchronous(OFF)`) and a copy takes 76 µs;
the test MariaDB server keeps its data on tmpfs, where a database's creation, migration and drop take 63 ms against 1.5 s on disk, and each test keeps a database migrated for it alone;
the benchmarks' server keeps the disk, whose durability a baseline records.
**The catalog job**: CI's MariaDB job skips the engine loops' SQLite runs, which the `ci` job runs,
and its packages run on parallel runners.
**Harness time**: the chaos harness's waits are parameters its own failure tests shorten.
The load harness's production mode runs at ratio-scaled time, F : `ack_wait` : heartbeat = 10 s : 14 s : 4.7 s, keeping ingest's defaults' proportions,
its pending budgets scaled by the same factor so its control stays constrained;
the real 5 m and 7 m, and what depends on a flush's absolute size such as a large batch's drain, run only for a phase gate.
The load harness runs on either engine, as T-60's gate needs: 2,000 equipment beside traffic at K = 100 on SQLite and then MariaDB, from per-operation benchmarks and one short real run.
**Benchmarks** run at `-count 5` at their small size by default, superseding T-54's `-count 10`:
the bootstrap interval may widen, so more rows may land in "measure again", which T-54's rule already handles.
Considered and not taken: running `-race` only on some packages or jobs, though it is most of the tests' CPU (68 s wall and 15 min 44 s CPU against 14 s and 67 s for the service module);
a pool of MariaDB databases reset between tests, whose isolation a trigger or a connection a test leaves behind breaks;
cloning a MariaDB database for every test (0.8 s); keeping `-count 10`.

### T-62 — the host log feed's performance and payload (2026-11-07) (compression superseded by T-64)

The owner settled, on 2026-11-07, how the host log feed bears its load, from measured payload costs and the forwarder's sink as its documentation and source state it (`logfeed.md` §15.10).
**Measured**: entries generated from go-secs messages, sized to the sample's 570 B, encode in JSON at 1.31 times the log text, about 750 B an entry, and 880 B in the stream's accounting with its subject, message-id header and record;
decoding takes 3 µs an entry and `sml.Parse` with the frame's build 4.6 µs a body, about 2% of one core at 2,000 pieces of equipment;
a built frame is 282 B against 893 B of SML; S2 compresses the stream's blocks four times and one message 1.7 times.
So the feed's limits are the catalog writer, the bus's message count and the held entries, not tracekeep's decoding.
**The forwarder's sink** publishes one event a message, cannot batch or compress, retries every failed publish and holds one request in flight unless configured;
its concurrency must be raised to reach the fleet's 3,200 entries a second, which reorders a file's entries by its retries.
**Decided**:
the reorder window defaults to 30 s, and the pipeline's in-flight time, retries included, is bounded below it, an entry arriving later being counted late (`TracekeepLogEntriesLate`);
the lease groups are the stream's partitions, a subject transform `{{partition(G,1)}}` mapping `tklog.<equipment token>` to `tklog.<n>.<equipment token>` and group n's consumer filtering `tklog.<n>.>`, since tracekeep keeps no list of equipment, G changed only by `migrate` on a drained stream;
the stream is compressed with S2, its `max_bytes` still counting logical bytes;
each message carries a message id, `<source>/<equipment token>/<file>/<offset>`, under a duplicate window longer than the pipeline's retry cap;
the payload stays one JSON entry a message.
Also carried into `logfeed.md` from the owner's 2026-11-06 answers:
a host program's log and the gateway's capture of one device are two series, never chained or overlapping;
the host program's leg to an FDC system is filed under the main device, sharing its capture where the log does not separate them, and its records claim no HSMS header;
and the gateway's `Source` is an operator-set, required key of its own configuration, never its application id.
A converted entry keeps only its acknowledgement handle, since its payload held for F would take about 800 MB more across the replicas.
Considered and not taken: a 5 s window, which loses every entry a retry delays past it;
one publish in flight per forwarder, which caps an instance near 1,000 entries a second;
a consumer per replica dropping the groups it does not hold; a binary codec, about 20% fewer bytes for a descriptor file shipped with the pipeline.
**P3 closed**: P3 (host log feed) applied as `logfeed.md` (§15) and the amendments to `ingest.md`, `catalog.md`, `compactor.md`, `storage.md`, `rollouts.md`, `api.md`, `operations.md`, `architecture.md` and `glossary.md`; last text at 4e385a1.

### T-63 — the host log feed's load measured first (2026-11-07)

The owner chose, on 2026-11-07, to measure the host log feed's two largest unmeasured costs before its converter is built, ahead of T-60's gate:
the feed stream's load on a three-node cluster at the fleet's rate, held and acknowledged as the converter will, with a consumer's re-creation;
and the catalog writer's cost of the feed's registration on both engines.
`logfeed.md` §15.10 rests both on estimates: the bus's from message counts and phase 5's in-progress acknowledgements, the writer's from `RegisterPack`'s baselines, with no MariaDB figure.
A number past its budget stops the plan for the owner's decision, before the converter's cost is spent.

### T-64 — the feed stream uncompressed, its consumers' state in memory (2026-11-07)

The owner chose, on 2026-11-07, from step 1 of the host log feed's plan, which T-63 stopped for this decision,
to store the feed stream without compression and to keep its group consumers' state in memory, superseding T-62's S2.
**Measured** at ingest's own times (F 300 s, `ack_wait` 7 min, reorder window 30 s) on a three-node cluster in one process, the feed at 3,200 entries a second beside the traffic at K = 100:
as T-62 specified it, the feed was judged sustained with its publish latency at 620 ms at the 99th percentile and its publishers' queue rising each cycle,
the traffic beside it fell behind in one run of two, and the process took 4.1 cores and 16 GiB.
Its profiles showed two costs of the bus servers:
a work queue's acknowledgement into an S2 block decompresses the whole block, 3.4 GB of the heap across the three servers;
and a file consumer encodes its whole pending state up to ten times a second on each replica, 36% to 46% of the process's CPU with about a million entries pending, the feed's and the traffic's.
Without compression, the feed's latency fell to 51 ms and the decompression's memory went, but the traffic still fell behind;
with its consumers' state in memory as well, the feed and the traffic both sustained, the latency 4.3 ms, the process two cores and its disk writes from 147 to 43 MiB/s.
**The cost**: each replica holds the stream's logical bytes, a 40 GiB backlog taking 40 GiB of its disk;
and a consumer lost with the whole cluster is re-created, delivering every unacknowledged message again from the oldest, as a takeover does (`logfeed.md` §15.5).
The traffic consumer's own state, about 476,000 pending entries at its production budget, is the remaining encoding cost; it is not the feed's to change.
Considered and not taken: keeping S2 for its disk, about a quarter, at a gigabyte a server and the latency; consumers of one replica, which a work queue refuses; measuring on separate hosts first.

### T-65 — the log converter's group bound (2026-11-07)

The owner chose, on 2026-11-07, from step 10b of the host log feed's plan, to bound each lease group's converter by bytes and to keep no payload in a held delivery.
The step's fleet is the owner's sizing: 1,980 pieces of equipment logging about 1.2 messages a second of about 6 KB, 20 logging about 500, about 20 GB a day zipped each,
and bursts of 200 comparing their recipes for three minutes at 10 messages of 50 KB a second; about 25,000 entries a second in all.
**Measured** at ingest's own times on a three-node cluster in one process, the converter held everything its group received until its equipment's flush:
an equipment's own threshold, 64 MiB of batched payload, was never reached by the slow ones, so a group held F's worth, bounded only by `max_ack_pending`, which counts messages.
Eight of the 32 group consumers stopped at 100,000 held entries, stalling their equipment until F; the process grew past 50 GB in five minutes while 93% of the host's CPU stood idle.
The heap held the deliveries' wire messages, 5.5 GB, beside the built records, 1.4 GB, though `logfeed.md` §15.7 already held a converted entry to its acknowledgement handle.
**The rule** (`logfeed.md` §15.7 *The group's bound*): a group past `logfeed.group_max_bytes` (128 MiB) of batched payload, or holding more than half its consumer's `max_ack_pending`,
flushes its equipment with the most batched bytes first; a held delivery keeps only its reply subject.
**The cost**: the fast equipment's segments are smaller and more frequent than F's, more registrations and more merge inputs; F stays the slow equipment's interval.
Considered and not taken: a shorter F for the whole feed, which multiplies every equipment's registrations; a replica-wide budget, which shares state across the groups' goroutines.

### T-66 — the gate's load judged on a bus of its own, the burst on what it sent (2026-11-07)

The owner chose, on 2026-11-07, from step 10b of the host log feed's plan, how the gate judges the fleet's load.
**The traffic**: in one process holding the three bus servers, dev and the harness, the traffic's cycle verdict beside the fleet failed in three runs of four,
its minima sloping −56, +41, +377 and +98 records a second against a bound of 8, while every traffic record was acknowledged within F and 7 seconds;
a control with the fleet idle sustained. With the servers as processes of their own, pinned to the host's other CCD and their stores on another disk,
the traffic sustained beside the fleet on both catalog engines, its minima flat. The gate's environment is that one (`scripts/load-feed.sh`), its criterion unchanged.
**The burst**: one stream of one replica lives on one server, which took the fleet's 95 MB/s with every publish stored,
but the burst's 117 MB/s more waited: its publishers made 65% to 89% of its rate, and raising their publishes in flight only queued them past their timeout.
The gate judges what the burst sent: digested within two flushes of its end, every entry accounted for once; its rate is reported, not judged.
A peak is the forwarders' buffers' to hold, as the owner accepted that a peak need not be converted in real time (`docs/guides/log-pipeline.md` *Peaks*).
Considered and not taken: splitting the feed into several streams, so their leaders spread over the servers, judged no-go on 2026-11-07:
the requirement asks no faster intake of a peak than the forwarders' buffers give, the ceiling's cause is unprofiled (the stream's single inbound loop, its disk, or the traffic stream on the same server),
and the split's choices (how many streams, which groups each, changing their number, the leaders' placement) were undecided.
