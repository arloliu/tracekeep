# Capture identity — equipment, source and device

Status: active (2026-11-06) — phase 1, the specs

The work that delivers T-59 (`docs/specs/tracekeep/tracekeep-decisions.md`) before the host log feed (T-60):
`tool_id` becomes `equipment_id` throughout tracekeep,
a capture names its source and its device,
and a series is one source's recording of one device of one equipment.
The design goes into the specs in phase 1 and the rationale is T-59's;
this plan only orders the work.

It is breaking, with no compatibility (T-59):
tracepack v0.15.1 (G5-303..305, its spec v2.44), producer v0.6.0, and one catalog migration on each engine.
v0.15.1 fixes v0.15.0's text and tests with no API or format change, so it is the version to require.

## Phases

| Phase | Work | State |
|---|---|---|
| 1 — specs and rules | amend the specs and rules to T-59's identity and term, one docs pull request | in-progress |
| 2 — SDK | `producer/` on tracepack v0.15.1 with `Config.Source` and the descriptor's `Device`; its own pull request (T-46), then `producer/v0.6.0` tagged on `main` | pending |
| 3 — service | the service on `producer/v0.6.0` and tracepack v0.15.1: catalog migration, admission, reads and selector, proto, metrics, alerts and runbook, web, simulators | pending |
| 4 — gate and close | the gate cases below on both engines; the eqp-hub session told of v0.6.0; this plan closed | pending |

The service cannot raise tracepack before the SDK is tagged:
its `tracepack.PackMeta.ToolID` stops compiling at v0.15, and it builds with `GOWORK=off` against a tagged SDK (T-46),
so phase 3's one pull request raises both, renames and migrates.

## Phase 1 — specs and rules

One pull request on `docs/specs/`, `.agents/rules/` and the `README.md` files, no code.
The specs state the new design in place and never cite this plan, which closes.
The guides in `docs/guides/` describe the code as it runs, its alerts, metrics, flags and procedures,
so they change in phase 3 with the code, where `scripts/alerts-check.sh` checks the runbook's anchors against the Helm alerts.

### What each file gains

- `producer.md` §4.5: `equipment_id` is the gateway's configured equipment identity (its `toolID`, mapped at the integrator's boundary);
  `device` (tag 0x0035) is the unit the link serves, required;
  `recorder_instance_name` is the source, the gateway deployment's name from `Config.Source`;
  `recorder_instance_id` is the UUIDv5 of (`equipment_id`, device, source), each length-prefixed as today, under the unchanged namespace.
  The paragraph on one hub's devices is rewritten to T-59's layers:
  a unit's connection (`eqp`, `tlp`, a sensor's) is a device and the producer records it;
  a host-facing shadow or parallel-mode connection (the `pdf_*` ones) is no unit, belongs to the unit it serves, and is not recorded;
  one source records one link per device.
  The overlap paragraph names two deployments under one source name as the case it reports.
- `producer.md` §4.2 (*Rotation*): a capture's `previous_capture_id` names the series' previous capture only:
  a `Rotate` or a `Begin` after `End` whose descriptor changes the equipment or the device starts a new series with none.
- `producer-sdk.md` §4.7: `Config` requires `Source`;
  the descriptor's fields are `EquipmentID` and `Device` (today `ToolID`, `DeviceName`);
  the first guarantee names `equipment_id`, the device and the source;
  the integrator enables the producer only on a unit's connection.
- `architecture.md` §3.4 and `ingest.md` §6.1: the descriptor admission accepts names a valid device and a valid source (below),
  and its `recorder_instance_id` is their derivation;
  any other start takes the disposition an unaccepted descriptor takes today (durable rejection, its version parked), after resolution as now.
- `catalog.md` §7.1: `captures.equipment_id`, `captures.source`, `packs.equipment_id`, the renamed indexes;
  §7.3: chaining, successor close, unregistered intervals and overlap per series (`recorder_instance_id`);
  a predecessor of another series is no evidence for a capture's unregistered interval;
  two sources recording one device are two series, never an overlap;
  §7.4: T-59's migration is the stated exception to expand-then-contract (next list, item 2).
- `architecture.md` §3.3 (*Packaging*): the upgrade leg's baseline is the newest passing ancestor at or after the identity reset, named by its migration;
  while none exists, as in phase 3's own pull request, the leg is skipped and says so, as for the first run today.
- `api.md` §10.1 and §10.3: the selector below; every result that names a capture names its source;
  `ListDevices` keeps its name and answers one entry per series, gaining `source`;
  the browser keys a row by (device, source) and filters devices and sources independently;
  an export job keeps its selector's sources, and its manifest names each capture's source.
- `compactor.md` §8.2, `rollouts.md` §6.4, `operations.md` (status, metrics, simulator flags), `positioning.md` §2: the term, and the series where "device" meant it.
- `glossary.md`: equipment, equipment ID, source, device (told apart from `device_id`, the SECS-II session number), series; "tool" removed.
- `.agents/rules/600-invariants.md` (the first guarantee) and `100-overview.md`, with the other rules' prose: the term.

### What phase 1 settles in the specs

1. **Values**: `device` and `source` are valid UTF-8 of 1 to 255 bytes, not whitespace only, stored and hashed byte for byte;
   the SDK's `Validate` refuses any other device and `New` any other source, and admission refuses such a descriptor.
   SQLite does not bound a `varchar`, so the bound is the code's on both engines.
2. **The migration**: one migration per engine renames `tool_id` to `equipment_id` in `captures` and `packs` and their indexes, and adds `captures.source` nullable, as `device` is, since a pending row has neither.
   Nothing written before it is kept (T-59): `tracekeep migrate` refuses to apply it to a catalog holding any capture row, changing nothing,
   naming a fresh catalog, bucket and traffic streams as the way on, as a `tracekeep dev` directory without revisions is refused today (§7.4).
   The reset is offline: every tracekeep process of the installation stops, and on Kubernetes its Deployments scale to zero, before `migrate` runs,
   since the migrator's lock excludes other migrators and not an old ingester's admission;
   the check and the migration run under that lock, and only the new release starts after it.
   It is generated by `make migrate-diff`, edited to rename rather than drop and add, rehashed into `atlas.sum`, and passes `make migrate-check`.
3. **The selector**: `equipment_id` required; `devices` and `sources` optional, each up to the device bound, each empty selecting all, their intersection with `capture_id` as devices' is today;
   `sources` part of the selector digest `QueryRecords`' cursor names, so a cursor issued under one selection is refused under another.
4. **Operator-visible renames**, said in `operations.md` and, in phase 3, the runbook:
   the metric label `tool_id` becomes `equipment_id` (the service's ingest metrics and the SDK's `current_capture` info gauge), per equipment, not per series, to keep cardinality;
   `TracepubToolSilent` becomes `TracepubEquipmentSilent`, its anchor `#equipment-silent`;
   `tracekeep status --json` names `equipment` and `equipment_id` (today `tools`, `tool_id`);
   `cmd/tracekeep-sim`'s `-tool` becomes `-equipment` and gains `-source`;
   the web's URL parameter `tool` becomes `equipment` beside `devices` and `sources`, and its remembered list `tracekeep.tools` becomes `tracekeep.equipment`.
5. **The term in prose**: "equipment" is a mass noun;
   "piece of equipment" or "equipment ID" where a countable noun is needed, as go-secs writes.

### What keeps "tool"

- `tracekeep-decisions.md`, append-only: earlier entries keep their text.
- Software tools in prose ("toolchain", "decision tool"), which are no equipment.
- `docs/ROADMAP.md`'s milestone table, seed v21 §12 moved unchanged (phase G's `toolID`).
- `proposals/p3-host-log-feed.md`: T-60 already reads its source as the equipment;
  it is rewritten as `logfeed.md` and closed, so its three occurrences stay.
- The applied migrations, whose bytes never change, and the new one's rename operands.
- veq and eqp-hub, which keep `tool_id` (T-59).

### Done when

`make docs-check` and `make lint` pass, and every match of `tool` in `docs/specs/` and `.agents/` is reviewed and is one that keeps it.

## Later phases, one line each

- **Phase 2**: `producer/go.mod` and the nested `producer/internal/bustest` module require tracepack v0.15.1;
  `bus.ToolToken` → `bus.EquipmentToken`; `Config.Source`; `RecorderInstanceID` of the triple, with fixed test vectors; the predecessor rule;
  `producertest` and the SDK's tests; `make lint` and `make test`, which visits bustest too, pass in a checkout with no `go.work`, as CI's,
  since the Makefile lets the service discover a workspace and the service still uses the old names until phase 3;
  `release-readiness` for `producer/v0.6.0`.
- **Phase 3**: `schema.hcl`, the migration and its refusal; `internal/ingest` admission; `internal/catalog` reads (`ListDevices` and gaps per series, the peer and predecessor queries, scope changes);
  `proto/tracekeep/v1` and `make proto`; `internal/api` (exports' stored selector and manifest), `internal/admin`, `internal/metrics`;
  the Helm alerts, their test, the runbook and the other guides (`production-admission.md`'s tool type is the equipment type), with the offline reset in `kubernetes.md` and `standalone.md`;
  `internal/service/dev.go`'s and `internal/sim`'s producer with a fixed synthetic source; `cmd/tracekeep-sim`; `scripts/packaging`; `web/`; `catalogtest`'s fleet with derived recorder ids.
  CI's `ci`, `catalog` (MariaDB, with `make migrate-check`), `web` and `packaging-checks` (with `make alerts-check`) jobs pass, and `make packaging-cluster` before the merge (`500-workflow.md`).
  `go get` of the tagged modules needs the sandbox off (the sum database's cache).
- **Phase 4**: the gate, then the eqp-hub session told that v0.6.0 requires `Source`, renames the descriptor's fields and records unit connections only.

## Gate cases

On SQLite and MariaDB unless the case is the SDK's:

- the SDK: fixed `recorder_instance_id` vectors, each component changing it, length-prefixing keeping concatenations apart;
  a rotation or `Begin` within a series chains, one changing the equipment or the device does not,
  and after an abandoned identity-changing `Rotate`, the next ordinary and fault rotations keep the last committed identity and never name the abandoned candidate;
- two devices × two sources of one equipment read as four series, with distinct endpoints, gaps and last records, none overlapping another;
  two captures of one series that overlap are flagged, as today;
- another source's later capture never closes a capture as its successor;
  another source's earlier capture, whose end would erase a capture's unregistered interval, leaves it unchanged;
  a capture naming an admitted predecessor of another series keeps its unknown-start interval (replacing `internal/catalog/reads_test.go`'s fixture that expects the opposite);
  a same-series predecessor outside the window still counts;
  of two selected captures naming one admitted predecessor, only the one of another series keeps its unknown-start interval;
- `devices`, `sources` and `capture_id` intersect on every selector-taking RPC, a stored export job replays its sources, and a cursor is refused under another selection;
- a start whose raw descriptor bytes carry a nil, empty, whitespace-only, invalid UTF-8 or 256-byte device or source, or a `recorder_instance_id` that is not the triple's derivation, is rejected durably and parked, and nothing is admitted;
  255 bytes, multibyte included, is admitted; values differing only in case, surrounding spaces or Unicode spelling are distinct series through the selector and the browser's URL;
  a terminated version's start is still acknowledged under its termination;
- a pending capture admits in place with its equipment, device and source;
- the migration applies to an empty catalog at the previous head, and a later `migrate` on the populated new catalog applies nothing;
  a catalog at the previous head holding an admitted, a pending or a rejected capture is refused through the migration executor, before Atlas runs, with its schema, rows and every revision row unchanged;
- a `cmd/tracekeep-sim` and a `tracekeep dev` capture are readable with the tracepack CLI at v0.15.1 and name their device and source;
- a search of the tracked code for `tool_id`, `ToolID`, `toolId` and `-tool` finds only the applied migrations and the new one's rename.
