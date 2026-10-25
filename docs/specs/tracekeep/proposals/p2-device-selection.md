# P2 — device selection

Status: draft (2026-10-25) — awaiting the owner; the tracepack tag is ratified (G5-270)

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

## Owner input and eqp-hub facts (2026-10-25)

- Owner: the gateway's identity must not change when leadership changes hands;
  the device name is the `eqp-hsms` device name in the hub's configuration.
- eqp-hub (`develop` at fc67e5f, read-only check) has **no hub name** in code or configuration.
  Its stable per-gateway identity is `toolID` (required, at least 4 characters),
  which leader and standby share because they run one configuration;
  the pod name and pod IP differ between them and change at every failover and reschedule.
  The `eqp-hsms` process inherits `TOOL_ID` from the hub's environment and receives its own name from `devices[].name`.
- Device names are required and unique within one configuration, with no character set enforced, so `/` is allowed.
  `toolID` can change through a configuration update.
- A planned handover is sequential, but two leaders can overlap when the election agent is unreachable:
  the old leader keeps its devices running while a standby wins the next term and starts the same devices under the same names.

So `producer.md` §4.5's "hub name and device name" names a value eqp-hub does not have;
`toolID` takes the hub name's place below.

## 1. The device key

A reader selects by **(`tool_id`, device)**, where device is the configured `eqp-hsms` device name (`eqp`, `tlp`, `dpm`, …).
The other descriptor fields do not serve:
`device_id` is the SECS session or device id, which two connections may share;
`equipment_endpoint` changes with an address change;
`recorder_instance_id` is a UUID.

## 2. Carrying the name

tracepack ratified the tag as G5-270 (spec v2.37, format 1.2, released in tracepack v0.8.0):
`recorder_instance_name`, tag 0x0034, utf8, optional, a producer-owned descriptor value that the consumer copies into every segment as decoded.
tracepack never interprets it, checks no relation to `recorder_instance_id` and requires no uniqueness;
merge inputs carry the same value or none.
In Go it is `CaptureDescriptor.RecorderInstanceName` and `PackMeta.RecorderInstanceName`, both `*string`.

tracekeep's producer writes the device name, unchanged;
`tool_id` already names the tool, so the value needs no separator.
The SDK refuses an empty name, which tracepack accepts.
The value counts toward the 4096-byte header block of the `start` message.

What the tag brings with it:
- A pack carrying it is format minor 2, and its canonical export is `tracepack-jsonl/3`;
  the format features of `rollouts.md` §6.4 and the export schemas of `positioning.md` §2 and `api.md` §10.1 gain that minor.
- Consumers and mergers upgrade before producers:
  a v0.7.x consumer refuses the descriptor and leaves the `start` deferred (G5-252), and a v0.7.x merger refuses a minor-2 input.
  T-23 adopts v0.8.0 in both modules before any code exists,
  and the SDK release that eqp-hub links, writing the tag, follows the service's.

## 3. Deriving `recorder_instance_id`

`recorder_instance_id` = UUIDv5 (RFC 9562), under one fixed namespace UUID that the SDK defines,
of `tool_id` and the device name, each prefixed by its byte length,
so no choice of characters in either can make two pairs collide.
The SDK computes it from `ToolID` and a new `DeviceName` in `Config`, which replaces `RecorderInstanceID`,
and guarantee (2) of `producer-sdk.md` §4.7 becomes "the configured `toolID` and the configured device name".
The same names always yield the same UUID, across restarts and leadership changes,
which the successor lookup by (`tool_id`, `recorder_instance_id`) of `catalog.md` §7.3 and `compactor.md` §8.2 relies on.
A `toolID` change is a descriptor change, so the producer rotates (§4.2) and the new captures form a new series.

## 4. Catalog

`captures` gains `device`, decoded from `recorder_instance_name` at admission,
and an index on (`tool_id`, `device`, start time).
A `pending` row has none, as it has no descriptor (`ingest.md` §6.1).

## 4a. Overlapping captures of one device

Two leaders running at once produce two open captures with the same `tool_id`, device and `recorder_instance_id`,
which [STO §4] allows (each producer publishes only its own capture, G5-200, G5-242).
A read by device returns both, each with its own completeness, and never interleaves them into one series;
`ListDevices` and `ListCaptures` flag a device with more than one open capture.
The successor lookup of `catalog.md` §7.3 and `compactor.md` §8.2 must tolerate two candidates; how it chooses is open.

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
| `ListDevices(tool, from, to)` | per device: name, `equipment_endpoint`, `device_id`, how many captures are open, the last record time, whether the window holds a gap |

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

- How the successor lookup chooses between two overlapping captures of one device (§4a).
- Whether the gateway's `AppID` belongs in the identity: the election name is `<AppID>-<toolID>`, so two deployments with different `AppID`s could serve one tool.
- How this meets P1 item 4, which also reshapes the SDK's `Config` and lifecycle vocabulary.

## Applying it

Items amend `producer.md` §4.5, `producer-sdk.md` §4.7, `catalog.md` §7.1, `api.md` §10.1, `rollouts.md` §6.4, `positioning.md` §2, `glossary.md`
and `.agents/rules/600-invariants.md` (guarantee 2), each with its interface section, in one change per accepted item.
