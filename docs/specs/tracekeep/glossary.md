# tracekeep — glossary

Status: draft (2026-10-25) — split from seed v21

Covers the terms the specs use with a meaning narrower than everyday English, each with the section that defines it.
New text written from seed v21; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read when a spec file uses a term you cannot place; the cited section is authoritative.

## Records and captures

| Term | Meaning | Defined in |
|---|---|---|
| capture | the records of one tool connection and gateway leadership term, written by one producer under one descriptor; its catalog `state` is `pending`, `open`, `stopped`, `closed` or `rejected` | §1 (G5-200), §7.1 |
| descriptor | the capture's identity and settings, carried by its `start` record: `tool_id`, `recorder_instance_name`, `recorder_instance_id`, `device_id`, timers, endpoint, `thinning` | §4.5 |
| device | the gateway's configured `eqp-hsms` device name, carried as `recorder_instance_name`; with `tool_id`, the unit a reader selects by | §4.5, §10.1 |
| overlap | two captures of one device whose spans intersect, as when two leaders run at once; both are flagged, neither is closed while live | §7.3, §8.2 |
| seq | a record's position in its capture, from 0, assigned by the producer in capture order; 2^63 − 4 is the last ordinary seq and 2^63 − 1 the close sentinel | §2, §4.2 |
| gap | a hole in a capture's producer coverage between seq 0 and `highest_seq` | §7.3 |
| unknown tail | the seqs above `highest_seq`, whatever the capture's state | §7.3 |
| evicted | seqs whose packs retention deleted, reported as their own class, never as a gap | §7.3 |
| deferred | seqs of unresolved parked versions, reported as their own class until staged or terminated, never as a gap | §7.3 |
| unknown capture | a capture with no catalog row, or with a `pending` placeholder row that parking inserted before any `start` | §6.1 |
| admitted capture | a capture whose descriptor was accepted: state `open`, `stopped` or `closed` | §6.1 |

## Storage and the catalog

| Term | Meaning | Defined in |
|---|---|---|
| pack | one tracepack file in storage, with a catalog row; a **segment** is written by one flush of `ingest`, an **archive** by a merge | §6.2, §8.1 |
| scope | one capture's UTC hour: the unit of merging, with state `open`, `settled` or `reopened` and a `revision` that every registration increments | §2, §7.1, §7.3 |
| settled | merged, and nothing more expected on the normal path; a scheduling fact, not completeness | §7.3 |
| receipt | the `claim_records` row a registration writes for every record that travelled by claim-check, keyed by capture, seq, the 44-byte record header and the payload's SHA-256 | §6.3 |
| tombstone | the catalog row of an object key deleted by retention or deletion; the reconciler never registers a tombstoned key | §8.3 |
| processing configuration | the immutable classifier, policy, keys and format features a capture binds at registration | §6.4 |

## The bus and parking

| Term | Meaning | Defined in |
|---|---|---|
| claim-check | the path of a record above the inline limit: payload in the claim bucket, a header-only message on the stream | §2, §4.2 *Large records* |
| claim object | a payload in the claim bucket, named `<capture_id>.<seq>` | §4.2, §8.4 |
| version | `capture_id`, `seq`, the 44 record-header bytes as the message body carries them, and one payload digest; two versions of one seq are different records | §6.1 *Parking* ([STO §4] *Termination*, G5-269) |
| deferred | a delivery left unacknowledged on the bus, never terminated by the service | §6.1 |
| parked copy | a copy of a deferred message on the parking stream, on its version's own subject | §6.1 *Parking* |
| resolution | a version's final state, `staged` or `terminated`; every later message of the version is acknowledged under it | §6.1 *Parking* |
| incarnation | one life of a traffic or parking stream, a `bus_streams` row; durable consumers are named for it | §6.1 *Parking* |
| termination order | a record of `capture terminate` that covers the capture's never-parked deferred originals at or below a cut-off sequence of one incarnation | §6.1 *Parking* |
| disposition classifier | the pure function that returns a delivery's final disposition or the next step it needs (evidence or a transactional operation); not the record classifier of a processing configuration | §6.1 |
| parking coordinator | the one owner of the parking protocol's steps: begin under the capture row, publish, confirm, advance, acknowledge | §6.1 *Parking* |
| pending request | a forwarded W-bit primary registered before it is sent: token, key (session id, System Bytes), capture, channel, thin decision | §4.6 |
| token | a pending request's handle, on which eqp-hub reports send progress, the opened wait (`Awaiting`) and one terminal outcome | §4.6 |
| survivor | a claim object retention skipped in an evicted hour, because an unresolved parked version still needs it | §8.4 |

## Tunables

Defaults are T-12 and T-25 in [tracekeep-decisions.md](tracekeep-decisions.md).

| Symbol | Meaning | Default | Defined in |
|---|---|---|---|
| F | `ingest`'s flush interval | 5 min (T-25) | §3.1, §6.2 |
| Q | quiet period: a scope becomes a merge candidate at hour end + Q; also the adjacency delay and the silence a successor close requires | 15 min | §7.3, §8.2 |
| I | inactivity close: a capture with no registration for I is closed | 2 h | §8.2 |
| L | retirement inactivity bound for a processing configuration | 7 days | §6.4 |
| D | deletion delay of a merged pack; exceeds any download URL's lifetime | 1 h | §8.3 |
| H | the producer's retry horizon for a put or publish | 5 min | §4.2 |
| τ | the producer's publish deadline | 10 s | §4.2 |
| ε | the producer's clock-step tolerance: the wall-clock drift against the clock anchor tolerated before a `clock-step` is recorded, carried in the descriptor as `clock_step_tolerance_ns` ([SEM §4]) | 1 s, tracepack's `DefaultClockStepTolerance` (T-29) | §4.2, §10.4 |
