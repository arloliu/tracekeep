# P2 — device selection

Status: draft (2026-10-25) — awaiting the owner, an eqp-hub check and a tracepack tag

Covers how a person or the VE service names the connection it wants, and how every read operation selects by it.
Read before changing the descriptor, the producer SDK's `Config`, the `captures` table or any read operation of `api.md`.
Nothing here reopens a decision in [tracekeep-decisions.md](../tracekeep-decisions.md); accepted items amend the spec files they name.

## Problem

One gateway serves one tool through several `eqp-hsms` devices, each a separate HSMS connection with its own capture series (`producer.md` §4.5):
the equipment link `eqp`, `tlp`, sensor connections, and in parallel mode connections to a legacy host system.
A reader thinks in those terms: "tool T1, the `eqp` connection, the last two hours".

The read operations of `api.md` §10.1 cannot say that.
`ListCaptures` and `ListPacks` take a tool and a window and return every device's captures;
`QueryRecords` can name a capture but not a device;
`ListScopeChanges` takes only a cursor and returns the whole service's events.
A reader must list captures, work out which device each belongs to, and query capture by capture,
while capture ids change at every `Start`, descriptor change, fault and leadership change.

The descriptor cannot tell a reader the device either.
`recorder_instance_id` is a UUID ([FMT §5], tag 0x001B), not a name,
and a descriptor holding any top-level tag other than the producer-owned ones does not decode ([STO §4] *Capture descriptor and pack metadata*),
so a producer cannot add a name of its own.
`producer.md` §4.5 says `recorder_instance_id` is "hub name and device name" but not how two names become one UUID.

## Owner input (2026-10-25)

- The hub name must not change when gateway leadership changes hands; this is to be confirmed in eqp-hub.
- The device name is the `eqp-hsms` device name in the hub's configuration.

## 1. The device key

A reader selects by **(`tool_id`, device)**, where device is the configured `eqp-hsms` device name (`eqp`, `tlp`, `dpm`, …).
The other descriptor fields do not serve:
`device_id` is the SECS session or device id, which two connections may share;
`equipment_endpoint` changes with an address change;
`recorder_instance_id` is a UUID and includes the hub.

## 2. Carrying the name

Proposal: tracepack adds an optional producer-owned descriptor tag, `recorder_instance_name` (utf8),
the human-readable identity of the deployment whose stable UUID `recorder_instance_id` is.
tracekeep's producer writes it as `<hub name>/<device name>`;
the SDK refuses a hub or device name that is empty or contains `/`.
This is a request to the tracepack maintainers, in the manner of T-10, and a format minor change on their side.

If tracepack declines, the fallback is a tracekeep header on the `start` message, kept only in the catalog;
it must first be verified that `CheckBusMessage` accepts a header outside its own set,
and a pack read on its own (through `PackURL`) would not name its device.

## 3. Deriving `recorder_instance_id`

`recorder_instance_id` = UUIDv5 (RFC 9562) of `<hub name>/<device name>` under one fixed namespace UUID that the SDK defines.
The SDK computes it, so the producer's `Config` takes the hub name and the device name instead of `RecorderInstanceID`,
and guarantee (2) of `producer-sdk.md` §4.7 becomes "a stable hub name and device name, and the configured `toolID`".
The same names always yield the same UUID, across restarts and leadership changes,
which the successor lookup by (`tool_id`, `recorder_instance_id`) of `catalog.md` §7.3 and `compactor.md` §8.2 already relies on.

## 4. Catalog

`captures` gains `hub` and `device`, decoded from `recorder_instance_name` at admission,
and an index on (`tool_id`, `device`, start time).
A `pending` row has neither, as it has no descriptor (`ingest.md` §6.1).

## 5. One selector for every read

Every read operation takes the same selector:

```json
{
  "tool_id": "T1",
  "devices": ["eqp"],
  "from": "2026-10-25T08:00:00Z",
  "to": "2026-10-25T10:00:00Z"
}
```

- `tool_id` is required; `devices` omitted means every device of the tool, which is today's behaviour;
  `capture_id` remains for an exact choice.
- `ListCaptures`, `ListPacks`, `QueryRecords`, `Export` and `ListScopeChanges` accept it.
  `ListScopeChanges` keeps its one global cursor and returns only the selected events,
  so the VE service follows its own devices without filtering the whole service's stream.
- A window that spans several captures of a device is answered across all of them;
  results mark each capture boundary, gap and unknown tail where it falls,
  so a reader can tell "no traffic" from "data missing".

A new operation lists what can be selected:

| Operation | Answers |
|---|---|
| `ListDevices(tool, from, to)` | per device: hub, name, `equipment_endpoint`, `device_id`, whether a capture is open, the last record time, whether the window holds a gap |

## 6. Record predicates of `QueryRecords`

```json
{
  "selector": { "tool_id": "T1", "devices": ["eqp"], "from": "...", "to": "..." },
  "messages": ["S6F11", "S5F1", "S2F*"],
  "direction": "to_host",
  "kinds": ["data"],
  "include_transport_events": false,
  "limit": 5000,
  "order": "time"
}
```

- `messages` takes `SxFy`, with `*` for any function, instead of separate stream and function fields.
- `direction` is `to_host` (sent by the equipment) or `to_equipment`, named for the tool's side the records are observed on (`producer.md` §4.1).
- `kinds` selects among data, control, transport-event and annotation records ([FMT] record header `kind`);
  transport events are excluded unless asked for.
- Filtering on payload content (a CEID, an ALID) is left for a later version.

## 7. The VE service

The VE service's configuration names (`tool_id`, device) pairs.
It lists packs, follows scope changes and fetches `PackURL`s through the selector,
and needs no knowledge of capture ids beyond what `ListPacks` returns.

## Open

- Whether the hub name is stable across leadership changes, in eqp-hub (owner input above).
- Whether tracepack adds `recorder_instance_name` (§2).
- How this meets P1 item 4, which also reshapes the SDK's `Config` and lifecycle vocabulary.

## Applying it

Items amend `producer.md` §4.5, `producer-sdk.md` §4.7, `catalog.md` §7.1, `api.md` §10.1, `glossary.md`
and `.agents/rules/600-invariants.md` (guarantee 2), each with its interface section, in one change per accepted item.
