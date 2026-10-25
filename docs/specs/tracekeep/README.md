# tracekeep specifications

The design of tracekeep, one living file per subject.
It arrived as one document, the design seed v21: v20 as approved by external review round 20 (no P0, no P1),
plus that round's one P2, the implementation checklist (now in `docs/ROADMAP.md`) and the owner's decisions of 2026-10-25 (now `tracekeep-decisions.md`).
The review reports, the owner's decision brief and the seed's last text (v21) are kept outside the repository.

| File | Class | Status | Purpose |
|---|---|---|---|
| `positioning.md` | living | draft | what tracekeep is and is not, what rulings settle, the LS rows it answers |
| `architecture.md` | living | draft | roles, consumer settings, data flow, deployment, transport scope |
| `producer.md` | living | draft | the gateway producer: capture, seq and clock, critical-path rules, descriptor, thinning |
| `producer-sdk.md` | living | draft | the producer SDK module: packages, construction, publication policy, the five guarantees |
| `storage.md` | living | draft | write and read seams, `ObjectStore`, the reconciler |
| `ingest.md` | living | draft | message disposition, batching, the registration transaction |
| `parking.md` | living | draft | parking, resolution, incarnations, termination, the parking tables |
| `rollouts.md` | living | draft | processing configurations, retirement, the activation gate |
| `catalog.md` | living | draft | tables, operations, completeness and the cursor, engines and migrations |
| `compactor.md` | living | draft | merge, close, deletion, retention, claim objects |
| `masking-keys.md` | living | draft | policy, keys, bindings, providers, restore and digest |
| `api.md` | living | draft | machine API, auth and roles, frontend |
| `operations.md` | living | draft | admin commands, runbook, tunables, metrics, health |
| `glossary.md` | living | draft | terms and tunables, with their defining sections |
| `tracekeep-decisions.md` | living | draft | rulings relied on, the owner's decisions T-1 onward, service defaults |
| `proposals/p1-interfaces.md` | finite | draft | module interface changes from the deep-module review |

## Read order

`positioning.md`, then `architecture.md`, then the file for the work under way (`producer.md` and `producer-sdk.md` for phase 0).
Before touching `producer/`, `internal/ingest/` or `internal/compact/`, read `.agents/rules/600-invariants.md`.
Each subject file opens with an *Interface* section, a non-normative summary; the text below it is authoritative.

## Section numbers

The files keep the seed's section numbers, so a `§N` anywhere in the specs resolves through this table.

| Section | File |
|---|---|
| §1, §2, §14 | `positioning.md` |
| §3 (§3.1–§3.4) | `architecture.md` |
| §4.1–§4.6 | `producer.md` |
| §4.7 | `producer-sdk.md` |
| §5 (§5.1–§5.4) | `storage.md` |
| §6.1 (except *Parking*), §6.2, §6.3 | `ingest.md` |
| §6.1 *Parking* (protocol, confirmation, resolution, incarnations, operator termination); the parking rows of §7.1 | `parking.md` |
| §6.4 | `rollouts.md` |
| §7 (§7.1–§7.4) | `catalog.md` |
| §8 (§8.1–§8.4) | `compactor.md` |
| §9 | `masking-keys.md` |
| §10.1–§10.3 | `api.md` |
| §10.4 | `operations.md` |
| §11 | `docs/guides/repository.md` |
| §12 | `docs/ROADMAP.md` |
| §13 (item N is T-N) | `tracekeep-decisions.md` |

## Citations

Rulings cited as `G5-n` are in go-secs `docs/specs/tracepack/tracepack-decisions.md`,
`[STO §n]` is the tracepack storage profile, `[FMT §n]` the format, `[SEM §n]` the semantics,
`LS-n` a row of `ve-log-service.md`, and `V-n` a VE decision.
Where a ruling already settles a point, the specs apply it and do not re-derive it.
Facts about eqp-hub are as of its `develop` branch at fc67e5f;
facts about nats.go are as of v1.40.1, the version eqp-hub pins.

Next free proposal ID: P3.
