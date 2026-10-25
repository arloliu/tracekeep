# tracekeep — rollouts

Status: draft (2026-10-25) — split from seed v21

Covers the processing configuration a capture binds, breaking releases, retirement and producer quiescence, linking old classifiers and maskers, and the activation gate with membership leases.
From seed v21 §6.4; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before changing a classifier, the masker, the policy or key binding, or the release process of `ingest`.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes under discussion are in [P1](proposals/p1-interfaces.md).

- **Binding**: a capture binds one immutable processing configuration at registration: classifier implementation, version and settings, policy id, version and hash, key ids with fingerprints, the format features it needs; the masker is recorded beside them (§6.4).
- **Execution**: a consumer never processes a record bound to a configuration it cannot execute; the record is deferred, never processed with a fallback.
- **Breaking release**: a classifier change or a masking-output change; rolled out beside the old consumers.
- **Retirement**: only when every bound capture is producer-quiescent (`stop_seen`, or no registration for L with both streams drained, observed traffic first, then parking); a failed observation is not emptiness.
- **Linking**: a release links every classifier, and every masker a masking-output change left in use, that a retained or open capture references.
- **Activation gate**: a replica registers in `consumers` with the configurations it executes and renews a lease before every fetch; registration is refused unless it executes every unretired configuration; a new capture binds the newest configuration every leased member supports; capture binding and membership changes serialize on one `counters` row; every delivered message is checked against the member's lease and membership generation before processing.
- **Determinism**: across service upgrades, masking determinism is tracepack's guarantee (G5-268), not tracekeep's.

### 6.4 Processing configuration and rolling upgrades

A capture binds, at registration, one immutable **processing configuration**:
`classifier` = implementation name and version (the go-secs classifier the binary links) and every setting that changes its result, `max_frame_len` included (G5-250);
`policy` = id, version and content hash; `digest_key_id` and `enc_key_id` with their material fingerprints (§9); and the **format features** the capture needs (`format_minor` 1 for masking or thinning).
A consumer advertises the configurations it can execute at start; a record of a capture bound to one it cannot execute is deferred (§6.1), never processed with a fallback.

A release that changes the classifier, or that changes **masking output** (a change G5-268 requires the spec changelog and the module changelog to declare), is a **breaking release**, rolled out beside the old consumers; a masker change that preserves output is an ordinary release.
No fence exists on the gateway side: it keeps no durable state, two leaders may overlap during a handover ([STO §4]), and a partitioned old leader drops its `stop` if the bus is unreachable when it stops.
A configuration is therefore **retired** only when every capture bound to it is **producer-quiescent** by one of two tests:
`stop_seen` (set only by a genuine `stop` boundary), or no registration for the **retirement inactivity bound** L (default 7 days) and **neither stream holding a message of the capture**, observed **in this order**: first the traffic stream on the capture's traffic subject, then the parking stream on the capture's parking subjects (the filter `<park prefix>.<tool token>.<capture_id>.>`, §6.1, every per-subject count summed), each by a fresh `StreamInfo` (`Info`, never `CachedInfo`) filtered to that subject and read from `StreamState.Subjects`; a failed observation is not emptiness. Traffic first closes the only transfer there is: a message moves from the traffic stream to the parking stream and nowhere else, its copy is confirmed before the original leaves, and under the inactivity premise nothing new arrives on the traffic stream, so a message absent from the traffic stream at the first observation and from the parking stream at the second did not exist at the second; parking first could read zero, miss a transfer, and read zero again;
a close by inactivity I alone is **not** quiescent, nor is a successor capture, since the old producer may be partitioned and publish again ([STO §4] allows records after a close).
The processing configuration **records** the masker as well (`tracepack-redact/<major>` with the tracepack module version that produced it), beside the classifier, the policy and the keys, so which code masked a capture is never implicit;
under G5-268 ([FMT §14], [STO §4] *Masking*, spec v2.35) the masker is **not** an execution constraint: [SEM §8] leaves a masker no freedom (targets by path rule and whole-text fallback, HMAC-SHA-256 digests, AES-SIV-CMAC-512 with fixed associated data, no randomness, G5-214), so any conforming masker writes the same record bytes, and a consumer executes a configuration whatever masker version it links;
the recorded identity is informational, except across a masking-output change (below), where a capture bound before the change dispatches to the vendored masker of its recorded identity.
Determinism across service upgrades is then a property tracekeep **asks tracepack to guarantee**, not one it re-implements:
for one policy, one key set and one classifier configuration, `redact.Mask` output is a format guarantee covered by corpus golden vectors across tracepack versions within a major (G5-268);
G5-268 (2026-10-25) grants this within format major 1 for the record and its redaction entries (not the pack file's block layout, compression or footer), with the exceptions it lists: a classifier change is a new classifier identity (G5-250, still vendored per capture as below); a change of the output [SEM §8] prescribes is a format major change; a **bug fix** that brings a masker back to [SEM §8] changes committed `redact-*` corpus bytes and is marked a masking-output change in both changelogs, and only such a fix needs the fallback: tracekeep ships it as a breaking release with the previous `redact` and its screening dependencies vendored under the old masker identity, since a capture spanning the fix would otherwise yield two byte-different versions of one record on redelivery ([FMT I-12] conflict); a policy-legality tightening (G5-236, G5-262) and any resource limit or internal error defer the record and never change bytes.

Retiring a configuration never removes the ability to execute it: a tracekeep release **links every classifier implementation** any retained or open capture references (an old go-secs classifier is carried as a vendored package under its implementation name until no capture references it; a go-secs upgrade in the service adds an implementation, it never replaces one), and **every masker implementation a declared masking-output change left in use** by a capture bound before it (a conforming, output-preserving masker upgrade replaces the previous one and dispatches nothing, G5-268),
so the current binary runs against the current schema with the current keys and still executes old configurations; "retired" only means no new capture may bind it.

**Rollouts never create incompatibility**, by an **activation gate with membership leases** instead of a delivery guarantee the bus does not give:
- an `ingest` replica **registers** in `consumers` with the configurations it executes before its first pull and holds a **lease** it renews before every fetch; a replica whose lease lapsed stops fetching until it re-registers, so a paused or partitioned replica never pulls with stale knowledge;
- registration is **refused** unless the replica executes **every configuration not yet retired** (§6.4's retirement needs quiescence and a drained subject, so a stopped or closed capture with pending or parked records still holds its configuration unretired), so an old-version replica cannot join while any record it cannot execute may still be delivered, and a rollback is a roll-forward to a compatible build, never a mix;
- a new capture binds the **newest configuration every leased member supports**, in a transaction that reads the member set, and activation of a newer configuration is the moment the last member lacking it has lapsed or left; capture binding and membership changes serialize on one `counters` row;
- a pull is issued with `MaxWait` no longer than the lease's remaining time, and every delivered message is checked against the member's lease and membership generation **before processing**; a message received after the lease lapsed is nak'd untouched;
  so the eligible pullers are always compatible with every configuration a message may carry, which is the progress guarantee; no routing by the bus is assumed.
The admin command of §10.4 lists the captures and members that hold a configuration active.
The `stop_seen` test also requires both streams drained, by the same ordered observation, so earlier records another consumer has not staged yet are not left behind.
A tool connection can hold one capture for months, so retiring a configuration may require **rotating the gateway's captures**; a descriptor HotReload is the lever (§4.2), and `ListCaptures` names the captures that block a retirement.
Deferred messages reach a compatible consumer by redelivery: the work-queue stream redelivers a naked message to any pull consumer, so with a compatible consumer present delivery is eventual, not immediate;
`ingest` exports the age of its oldest deferred message, and the runbook adds compatible replicas when it grows.
