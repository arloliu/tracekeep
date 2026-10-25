# tracekeep — decisions

Status: draft (2026-10-25) — split from seed v21

Covers the decisions tracekeep's design rests on: the rulings it applies, the owner's decisions (T-1 onward) and the service defaults.
From seed v21 §13 (T-N is §13 item N, so a "§13 item N" or "(§13)" in another file resolves here) and the decisions of the repository bootstrap.
Read before reopening anything a spec file says is decided, and append a `T-<N>` entry for every new owner decision.

This log is append-only (`.agents/rules/450-doc-lifecycle.md`):
a superseded entry keeps its text and gains "(superseded by T-<N>)"; IDs are never reused.
Rulings of other repositories keep their own IDs and are cited, never copied.

## Rulings relied on

**Already decided by a ruling** (applied here, not open): claim-check above the inline limit (G5-217); only positively identified interval polls are thinned (G5-225);
the log service holds the keys (G5-207); the digest key spans captures and rotates by declaration (G5-220); export schema by `format_minor` (G5-233); conflicts are kept, never resolved by a writer ([FMT I-12]); seq starts at 0.

The tracepack rulings the specs cite, by number (go-secs `docs/specs/tracepack/tracepack-decisions.md`):
G5-198, G5-200, G5-201, G5-207, G5-208, G5-211, G5-213, G5-214, G5-216, G5-217, G5-218, G5-220, G5-222, G5-224, G5-225, G5-227, G5-231, G5-233, G5-236, G5-250, G5-251, G5-252, G5-257, G5-258, G5-262, G5-267, G5-268, G5-269.
The VE decisions they cite are V-8, V-12, V-26, V-27, V-29 and V-30 (veq `docs/specs/ve/`).

## Owner decisions

Each entry gives the question as the design put it, then the owner's decision.

### T-1 — wire observation

Adopt wire observation in the gateway, which makes the captures `raw-stream` / `wire-exact`, and amend LS-1 accordingly.

**Decided (2026-10-25):** wire observation, LS-1 to be amended.

### T-2 — loss visibility for durably admitted captures

Accept that loss visibility is guaranteed for durably admitted captures, with an unadmitted capture visible as an unregistered interval, and amend LS-15 accordingly; or ask for another mechanism.

**Decided (2026-10-25):** the durably-admitted qualification accepted, LS-15 to be amended.

### T-3 — sampling and the poll channel

Sampling parameters `N` and `T`; the `fdc_grpc` change (interval-only requests on a dedicated channel);
whether timer-triggered collection contexts ride the poll channel; whether to keep the rule in `thinning.source` as the key=value convention of §4.6 (the tracepack maintainers' advice) or still ask for a structured tag. Parallel mode thins nothing (§4.6); that option is closed by fact, not by choice.

**Decided (2026-10-25):** N = 60 and T = 60 s as defaults that **must be configurable** per poll channel in the producer's configuration, the `fdc_grpc` interval-only channel to be built, whether timer-triggered collection contexts count as interval polling **left to the FDC maintainers** (the one item still open), parallel mode closed by fact, the rule kept in `thinning.source` as the key=value convention.

### T-4 — retention against lineage

Retention against lineage (LS-13): (b), the VE copies what it promotes, is recommended; then tracekeep needs no holds.

**Decided (2026-10-25):** LS-13 (b), the VE copies what it promotes, no holds.

### T-5 — the VE service as a privileged reader

The VE service as a privileged reader through `PackURL` (whole packs with ciphertext in place; [STO §3] reserves them for privileged components, V-26 allows it): a trust-boundary decision.

**Decided (2026-10-25):** the VE service is a privileged reader through `PackURL` with the `pack-reader` role.

### T-6 — key operation and production admission

Who operates the digest key and the master keys (Vault paths, who may declare a rotation); the production-admission check of each tool type's recipe carriers against the policy.
LS-8's fields beyond recipe bodies: which classes join the policy, and that this gates production admission of a tool type.

**Decided (2026-10-25):** the digest key and the master keys are operated by the tracekeep maintainers (Vault paths and rotation declarations theirs), and LS-8 stays at the baseline (recipe bodies), so production admission of a tool type checks its recipe carriers only.

### T-7 — stack

Stack confirmation: sqlx + Atlas, connect-go, Vite + React; the SDK as a separate module in the tracekeep repository that dials the bus itself by default (`WithConn`/`WithBus` as the escape hatch).

**Decided (2026-10-25):** the stack as designed, with one condition: **before the API is implemented (phase 2), measure connect-go's overhead against plain gRPC** for `ListPacks` and streamed `QueryRecords` and record the result as a phase 2 gate.

### T-8 — repository name

Repository name (V-29 is provisional).

**Decided (2026-10-25):** `github.com/arloliu/tracekeep`.

### T-9 — the producer's residual wait

Accept, under LS-14, the producer's one residual wait: a callback may wait for another callback to leave the capture's critical section (§4.2), a dependency on scheduling and on nothing external; or require a different producer algorithm.

**Decided (2026-10-25):** the residual callback wait accepted under LS-14.

### T-10 — the requests to the tracepack maintainers

**Closed.** The two requests to the tracepack maintainers were answered and approved on 2026-10-25 as G5-267 (parking is deferred on the bus, [STO §4] *Deferral*) and G5-268 (the bytes of a masked record are fixed by the format, [FMT §14], [STO §4] *Masking*), tracepack spec v2.35; §6.1, §6.4 and §8.4 are aligned with the approved text, and parking is the service default. A follow-up question of the same day was answered and approved as **G5-269** (tracepack spec v2.36, go-secs 8cee7eb, `tracepack-decisions.md` line 1775): an operator terminates a record **version** (capture, seq, the carried 44 header bytes, payload digest), every message of that version is acknowledged under the termination without another act, the *Already staged* exception gains an "under a termination" sibling, and *Deferral* condition 5 keeps an object only for a copy that will be acknowledged neither as already staged nor under a termination; §6.1 *resolution* and §8.4 follow it.

**Decided (2026-10-25):** G5-267, G5-268 and G5-269 approved (tracepack spec v2.36, implemented by tracepack **v0.7.1**, which the SDK's `go.mod` pins).

### T-11 — producers that bypass the SDK

The fault boundary for producers that bypass the SDK: the design guarantees loss visibility and payload retention for SDK producers; a non-SDK producer that overwrites a claim object, publishes two versions of one seq, or puts objects without the SDK's metadata is outside that guarantee (its conflicting version in another hour may be evicted before delivery; its unattributed objects are never deleted automatically), and the owner decides whether such producers are supported at all.

**Decided (2026-10-25):** SDK producers only, the guarantee boundary documented.

### T-12 — service defaults

**Decided here, for the service** (tunable defaults; none of them stands in for a proof above; the residuals stated in §4.2, §6.4 and §8.4 are listed as such, not as closed; the object of a terminated version is released only after the server confirmed the acknowledgement of its known messages of the current incarnation, a caution G5-269 does not require, §8.4): quiet period Q = 15 min; inactivity close I = 2 h; retirement inactivity bound L = 7 days; deletion delay D = 1 h; producer retry horizon H = 5 min; publish deadline τ = 10 s; the process-wide registration key set capped at 4 million, reset by a producer restart;
the reserved lane 4096 records and the unqueued-reservation bound 1 s; retiring a processing configuration may require rotating captures; receipt-less claim objects follow their record's hour once their capture is known;
compactor singleton under a catalog lease; parking enabled by default (G5-267); durable consumers named per stream incarnation, `Preflight` refusing a traffic or parking stream with a mirror, sources or `AllowDirect`, and **replacing, restoring or rewinding either stream only as the stop-the-world maintenance procedure of §6.1 that ends in `bus adopt-stream`** (an operational constraint on every deployment, cluster or host, made part of the design's approval in external review rounds 18–20, kept outside the repository); the traffic stream and claim bucket are created by `tracekeep migrate`; Atlas CLI for the first release; HSMS-SS only in phase 0; thinning disabled until the poll channel exists.

## Repository decisions

Taken by the owner on 2026-10-25 for the repository bootstrap.

### T-13 — toolchain

`mise` manages Go, golangci-lint, Atlas and Node; `mise.toml` is the single pin; `.tool-versions` is not kept.

### T-14 — Go version

`go 1.26.0` in every `go.mod`, the line the gateway is on; the development toolchain is the latest Go 1.26 patch, 1.26.10 (replacing Go 1.27).

### T-15 — lint

golangci-lint 2.15.0, the latest release, built for Go 1.26.

### T-16 — modules

Two modules: the service (`github.com/arloliu/tracekeep`) and the SDK (`github.com/arloliu/tracekeep/producer`, its own `go.mod`, depending only on tracepack and nats.go).

### T-17 — workspace

`go.work` at the root names both modules and is committed, with `go.work.sum`; the service's `go.mod` requires the SDK by version and carries no `replace`;
CI also builds the service with `GOWORK=off` so its pinned requirement on the SDK stays buildable; the SDK is never built through the workspace in CI.

### T-18 — tracepack

`v0.7.1` in both modules.

### T-19 — nats.go

`v1.55.0` in both modules, nats-server `v2.15.1` for `internal/testbus`.
This raises the SDK's floor above the gateway's v1.40.1 and so revises `producer-sdk.md` §4.7 *Versions*;
the nats.go v1.40.1 and nats-server v2.14.5 facts the specs cite are re-verified, and §4.7 amended, before the SDK's first code.

### T-20 — document lifecycle

The veq document lifecycle (living and finite documents, the Status line, subject READMEs, `docs-check`, closing by deletion), with tracekeep's additions in `.agents/rules/450-doc-lifecycle.md`.

### T-21 — interface sections

Each subject spec file opens with an *Interface* section: a non-normative summary of what a caller must know, traceable to the text below it, which stays authoritative.
Contract changes go through proposals, starting with [P1](proposals/p1-interfaces.md).
