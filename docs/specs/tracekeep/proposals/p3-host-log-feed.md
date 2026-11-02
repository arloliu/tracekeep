# P3 — host log feed

Status: draft (2026-11-02) — ingesting an equipment host's SML text log as `log` captures; awaiting the owner

Covers a new tracekeep feature: a log-shipping pipeline routes the SECS traffic an equipment host prints to its text log as SML to tracekeep,
which parses and pairs the entries, builds the frames and writes `capture_method = log` captures.
Read it before designing or reviewing the feature, and before changing a spec it amends (listed per section).

## Problem

An equipment host (the program that drives a tool as the SECS host) logs every message it exchanges as SML text.
Where the gateway's capture is missing (before the gateway records a tool, during a gap, for a tool behind no recording gateway) that log is the only record of the conversation,
and people troubleshooting a tool read it today by hand.
tracekeep consumes only layout 1 of the traffic stream (`ingest.md` §6.1; `TRACEKEEP_TRAFFIC`, `producer/busconfig/busconfig.go:13`),
and layout 1 serves only `raw-stream` and `decoded-message` captures on the capture clock (G5-249, [STO §4] *Scope of the bus*);
a log conversion writes packs by [STO §7] and never publishes over the bus.
The owner decided (2026-11-02) that tracekeep itself converts the log:
it consumes a stream of log entries, parses and pairs them, builds the frames, and writes and registers the captures;
a log-shipping pipeline, required, tails the host's files, sends the traffic entries to tracekeep and everything else to the deployment's log store.

## Positioning

- A host-log capture is a separate source: `capture_method = log`, `vantage = host`, records `reconstructed` ([SEM §2]), `time_source = source-log`.
  It never stands in for the gateway's capture, which is `raw-stream` from vantage `intermediary` with `wire-exact` records (V-35, LS-1).
  The two are never merged, never deduplicated against each other and never compared seq by seq.
- It shows the host's side: the gateway rewrites the session ID (V-8), so a logged session ID is the host's, never the tool's (LS-2).
- It has no transport events: no connect, Select, Linktest, Separate or T3 lines, so every record is in epoch 0 and `correlation-incomplete` ([STO §7] item 8).
- Its loss evidence differs: tracekeep assigns its seqs, so it has no producer seq gap;
  what the pipeline lost shows as a declared hole (below), and only when the pipeline sends range markers.
- It is not a general log platform (`positioning.md` §1): only the entries the routing profile names as traffic reach tracekeep.

Recommendation: the machine API's selection reads frame captures only unless the caller asks for `log` captures (open question 1),
so the VE generation service never receives a log capture where it expects the gateway's.

## Data flow

```
host pod
  host program → text log files
  fluent-bit sidecar: tail (path, offset, inode keys, DB checkpoint) → multiline grouping into entries → forward
Vector forwarder
  routing transform (the routing profile): traffic entries → NATS sink (JetStream) → TRACEKEEP_LOGFEED, subject tklog.<source>
                                           every other entry → the deployment's log store (+ a range marker to tracekeep, recommended)
tracekeep ingest, the log stage (one lease per source)
  order by (file, offset) → dedup → pair header and body → build frame → classify + mask → batch per (capture, UTC hour)
    → tracepack.Writer → ObjectStore.Put (staging/) → RegisterPack → ack each entry
tracekeep compact, api: unchanged, except as stated under Storage and catalog
```

Each hop owns one duty:
fluent-bit tails, checkpoints and groups lines into entries;
Vector routes and passes fields through, adding only the profile version;
tracekeep parses, pairs, builds, writes and registers.
Vector must not regroup lines (`reduce`): grouping that depends on event arrival across two hops splits or merges entries silently.

## The pipeline contract

**Entries.**
An entry is one timestamped line (`YYYY-MM-DD HH:MM:SS.ffff`, host local time, no zone) and the untimestamped lines after it.
fluent-bit groups them with a multiline parser whose start state matches the timestamp at the start of a line;
its flush timeout must exceed the host's longest pause inside one body, or a body is split into two entries.

**Fields** every message carries, as one JSON object:

| Field | From | Meaning |
|---|---|---|
| `v` | Vector | the feed schema version, 1 |
| `profile` | Vector | the routing profile's id and version |
| `source` | fluent-bit (configured) | the host or pod the file was tailed on; tracekeep's `logfeed.sources` key |
| `file` | fluent-bit `Path_Key` | the file's path as tailed |
| `inode` | fluent-bit, where the deployed version can add it | the file's inode |
| `offset` | fluent-bit `Offset_Key` | the entry's byte offset in the file |
| `log` | fluent-bit | the entry's lines, joined by `\n` |
| `kind` | Vector | `entry` (routed traffic) or `range` (a range marker: `offset` and `len` only, no `log`) |
| `len` | Vector, on a range marker | the byte length of the unrouted entry |

tracekeep computes an entry's byte length from `log` and the line terminator the profile declares,
so an entry covers [`offset`, `offset` + length) of its file.
Which byte `Offset_Key` reports for a multiline group (its first line's start, or the read position after its last line) is fixed by the profile, and checked when the profile is written (open question 2).

**Routed entries.**
The routing profile forwards, by their role:
header lines of received messages; header lines of sent messages, primaries and replies;
print-body entries, the line naming `'SxFy'` with an optional `W` followed by the SML tree up to the line holding `.`;
and encrypted-body lines.
Lines of other threads interleaved between a header and its body go to the log store.
The literal patterns live in the routing profile shipped with the deployment, never in this spec:
they name the host software's own loggers and tokens.

**Range markers** (recommended).
For every entry it does not route, Vector also sends a range marker, `kind = range` with the entry's `offset` and `len`, never its text.
Then the entries and markers of a file tile it, and tracekeep can tell a hole from an unrouted range, which [STO §7] item 7 needs (below).
Without markers, an unrouted range and a lost entry look alike: holes are not detectable and the capture's `notes` say its coverage is unchecked.
The cost is one small message per unrouted entry (open question 3).

**The routing profile** is versioned:
the fluent-bit multiline parser, the Vector routing transform, the line terminator and the offset convention, as one versioned artefact of the deployment.
tracekeep accepts the versions `logfeed.profiles` lists;
an entry of another version holds its source (nak with delay, alert `log-profile-unknown`), never terminated, as T-51 treats an unknown layout.

**Subjects and stream** (point g).
One stream, `TRACEKEEP_LOGFEED` by default, subjects `<logprefix>.<source>` with `logprefix` `tklog` by default;
`source` is a configured id of `[A-Za-z0-9_-]`, so Vector writes it into the subject from the field, unescaped.
The stream has the traffic stream's settings (work-queue, file storage, no age, size or count limit, `AllowDirect` off; `StreamConfig`, `busconfig.go:37`).
`ValidatePrefixes` (`busconfig.go:99`) refuses a logprefix whose first token equals the traffic or park prefix's,
and `bus.Preflight` and `migrate` check `SubjectsOverlap` (`busconfig.go:110`) between the feed stream and both existing streams, as `CheckParked` does (`internal/bus/bus.go:94`).
The feed shares the traffic cluster (V-30) or uses its own; the gateway's bus never.

**Delivery.**
The Vector sink publishes to JetStream, so a message is in the stream before Vector drops it from its buffer,
and Vector's end-to-end acknowledgements reach back to fluent-bit's forward input where the deployed versions support them.
Where Vector's NATS sink can set headers, it sets `Nats-Msg-Id` to a hash of (`source`, `inode` or else `file`, `offset`);
the stream's two-minute duplicate window (`busconfig.DuplicateWindow`, `busconfig.go:24`) then drops only the quick repeats,
and tracekeep's own dedup (below) is the one that holds across a fluent-bit restart or a rotation re-read.

## Ordering, holes and dedup

**What tracekeep may assume** (point c): at-least-once delivery per entry, in no particular order, across two hops (fluent-bit forward, Vector's NATS sink, either with retries or concurrency above 1).
It assumes no order within a file and never asks the pipeline for concurrency 1.

**Order.**
The log stage holds a source under a catalog lease, one holder at a time, and consumes its subject with per-message acks (`AckExplicit`), as `ingest` does (`architecture.md` §3.1).
It orders a file's entries by `offset` and sequences an entry once every byte before it is accounted for (an entry, a marker or a final hole) or once the reorder window W (`logfeed.reorder_window`, default 5 min) has passed since it arrived.
W must exceed `ack_wait`, so a redelivery after a lost holder fills its range before the range is declared a hole.

**When a hole is final.**
With range markers, a byte range of a file that no entry or marker covers, below a covered byte, is a hole;
it is final when W has passed since the first entry or marker above it arrived.
A final hole becomes one `skipped-bytes` annotation with an empty value and the hole's source range, at the time of the entry before it,
so the seq stays dense and the hole is a declared loss (tracepack change 3).
An entry that arrives after its range was sequenced or declared a hole is a late entry: it is converted, takes the next seq of its capture, is marked `ordering-uncertain`,
and is counted (`logfeed_late_entries_total`); the hole's annotation stays, so that range is covered twice (tracepack change 1).

**Dedup** (point d).
An entry is a duplicate when its range lies outside every declared hole and below the **registered end** of the capture it converts into:
a column of the capture row, the end offset of the last entry a registered pack of the capture holds, updated by `RegisterPack` under the capture row (`ingest.md` §6.3),
since a capture registers its batches in seq order and seq follows offset.
A duplicate is acknowledged as already staged;
an entry below the registered end that lies inside a declared hole is late and converted (above).
Without range markers no hole is declared, so a late entry below the registered end cannot be told from a duplicate and is lost.
A file is identified by (`source`, `inode`) where the inode is carried, else (`source`, `file`),
plus the digest of its first entry, so a reused inode or a path reused for new content opens a new file identity instead of colliding with the old one.

**Acknowledgement.**
An entry is acknowledged once every record and annotation it went into is in a registered pack;
a header waiting for its body is not, so while the stage holds an entry it sends in-progress acks every `ack_wait/3` (`architecture.md` §3.1),
and `max_ack_pending` is sized for the peak rate × (W + F + upload).
A marker is acknowledged once every byte before its range is sequenced.

## Capture identity

- **One capture per (source, file identity, connection)** (points d and e).
  [STO §4] *Merge* requires every pack of a capture to carry the same ordered `source_ref` list,
  and an unfinished file's list cannot grow, so a capture never spans files;
  daily rotation therefore starts new captures, each naming its predecessor of the same source and connection in `previous_capture_id` ([FMT I-7]).
  [STO §7] item 7 adds one capture per file for the entries of no connection, whose `tool_id` is the source's host identity (`logfeed.sources[].host_tool_id`), distinct from every tool's.
- **Connection** (point e): `device=N` when the header line carries the host's device-number token, else `session=N` from the message's own logged header ([STO §7] item 4);
  a reply takes its primary's connection, paired by direction, stream, function and the host's transaction id.
  The gateway's rewrite makes a logged session ID the host's, not the tool's, but it is stable for one host configuration, so it names a connection, never a tool.
- **Tool** (point e): the operator maps each connection of a source to its `tool_id` (`logfeed.sources[].connections`), as [STO §7] item 7 asks of a converter.
  A connection the map lacks cannot fail a live feed:
  its entries go to the host's capture, as messages of no known connection, and raise `log-connection-unmapped`;
  a mapping added later applies from the next file, since re-attributing needs a new capture ([STO §7] item 8) (open question 4).
  Several connections in one log are normal: the tool, a load-port controller on its own session, a peer on stream 99.
- **capture_id**: a UUIDv7 whose time is the capture's first record's and whose other bits hash the source, the file identity, the connection, the `tool_id`, the routing profile version and `source_tz`;
  the catalog row fixes it at the capture's creation, so a restart finds it instead of deriving it again.
- **recorder_instance_id**: one per source, configured and stable, never the gateway's,
  so a source's captures form their own (`tool_id`, `recorder_instance_id`) series for successor lookup and unregistered intervals (`catalog.md` §7.3);
  `device` (`catalog.md` §7.1) is the connection key.
- **End**: the log stage writes the capture's `stop` boundary itself, as the converter it is ([STO §7] item 8),
  once its file is final: a newer file of the source has entries and the old one has had none for `logfeed.file_quiet` (default 1 h), or no entry for the inactivity period I.
  An entry of the file after the stop starts a continuation capture of the same file and connection, whose `previous_capture_id` names the stopped one.
  The compactor's close of a capture left open (`compactor.md` §8.2) never applies:
  it needs `clock_step_tolerance_ns`, which a `source-log` capture lacks ([FMT §5], G5-249).
- **Version**: a capture binds the conversion it was started under (parser version, routing profile, `source_tz`, connection map);
  a log stage that runs another stops the source's open captures and continues in successors at the next entry,
  so the capture-level `recorder` value stays equal across its packs ([STO §4] *Merge*).

## Conversion

tracekeep's own parser reads the format, `internal/logfeed/eapsml` (point h, below).
[STO §7] items 1–6 apply, with these dialect rules:

- **Pairing**: a header waits as the pending header of its SxFy; a body entry takes the pending header of its SxFy if one is within the pairing window
  (`logfeed.pairing_window`, default 3 entries of the file, counted over entries and markers), whatever their timestamps.
  A second header of an SxFy before its body ends the first as a header without a body.
- **Trust**: the W-bit, Session ID and System Bytes come from the header only when the header names the same stream and function as the body;
  some host versions log a reply under its primary's header, and then those fields stay invalid (`field_validity` clear).
  A W in the body line is source text, so a W on a reply fails the parse (`parse-failed`); a W only from the header fails the build (`build-rejected`).
  The transaction id has no field; it stays in the source text.
- **A values** may hold unescaped single quotes: the parser reads an ASCII value by its declared length `[n]`, else up to the last `' >` on its line.
- **What becomes what**:

| Entry | Becomes |
|---|---|
| header and body that parse and build | one `data` record, `reconstructed`, `reconstructed-ok`, at the header's time, direction from the header kind |
| body that does not parse, or not as one item (e.g. two root items) | `unparsed-entry`, `parse-failed`, its raw text kept |
| encrypted body | `unparsed-entry`, `parse-failed`; the hex stays as logged |
| header with no body within the window | `unparsed-entry`, `parse-failed` |
| body with no header | `unparsed-entry`, never a record: the host built it and never sent it (open question 5) |
| routed entry that matches no rule | `unrecognised-line` |
| final hole | `skipped-bytes`, empty value, the hole's range |

  Every annotation carries its `source_index`, `source_offset` and `source_len`;
  each data record's ranges are its header and body entries.
- **Nothing dropped** (point a), [STO §7] item 7 restated over the feed:
  every byte range a file's routed entries cover is in exactly one record or annotation of exactly one capture of that file, except a late entry's range under its hole's annotation;
  the ranges the markers cover are the log store's, and with markers every byte of the file up to its last covered byte is either.
  The log stage counts, per capture, its records per `decode_status` and the bytes they cover, and per file the marker and hole bytes.
- `source_ref`: one entry per capture, the file's base name and its identity (`source`, inode or first-entry digest), never a path or a content digest, since the file is still growing (tracepack change 2).
- `source_dialect`: tracekeep's parser and its version, a value distinct from any other converter's of the same format (tracepack change 4).

## Time

`time_source = source-log`, `source_tz` per source (`logfeed.sources[].tz`, an IANA name, required), never the capture clock and never fluent-bit's or Vector's event time (point f).
[STO §7] item 5 applies: a local time that occurs twice or never in a daylight-saving change takes the earlier reading and is `ordering-uncertain`.
The pack has no `capture_origin_*` and no `clock_step_tolerance_ns` ([FMT §5]).
A record's hour, and so its scope, is the UTC hour of its log time ([STO §4] *Scope and order*), as `ingest.md` §6.2 places every record.
The host's clock is not the gateway's; nothing compares a log capture's times with a gateway capture's beyond the reader's view.

## Storage and catalog

- **Admission is internal.** The log stage creates the capture with a new catalog operation, `AdmitLog`:
  the capture row inserted `open` with the capture-level metadata the stage built, stored as the descriptor bytes so a later creation compares them,
  and the processing configuration bound as `Admit` binds it (`catalog.md` §7.2, `rollouts.md` §6.4).
  It has no delivery identity, so no termination or parking re-check; nothing about it is ever `pending` or `rejected`.
- **Registration** is `RegisterPack` with feed evidence instead of delivery evidence:
  the capture's registered end offset, written in the same transaction; no receipt and no `parked` state.
- **What applies of `ingest.md`**: §6.2 (batching per capture and hour, sort by seq, one segment per flush, masking per record, the metadata budget, bounded flushes) and §6.3 (the crash points and the registration transaction).
  Not §6.1: the disposition table is layout 1's; the log stage's dispositions are the rows of this proposal, and nothing of the feed is parked.
- **Segments**: generation-0 segments under [STO §3] keys, flushed every F like the traffic's, with `flush_interval_ns`, merged by the compactor as any scope (tracepack change 1).
- **Completeness** (`catalog.md` §7.3): a log capture has no seq gap; its holes are declared losses, recorded beside `pack_losses` and counted;
  its span starts at its first record, since it has no `capture_origin_utc_ns`;
  its unknown tail is empty once the stage writes its `stop`.
- **Reconciler, retention, deletion**: unchanged; an orphan of an admitted log capture is adopted as any (`storage.md` §5.4).
- **Masking** (`masking-keys.md` §9) applies as to every capture: the bound policy masks the reconstructed frames' items,
  so a process program body in a data record is masked or encrypted as in a gateway capture.
  The raw text of an `unparsed-entry` can hold one too (a body that did not parse, an encrypted body), and an annotation rule can only zero it ([SEM §8]):
  a configuration that admits log captures must add annotation rules for `unparsed-entry` and `unrecognised-line`,
  which `config stage` checks as it checks the baseline (`operations.md` §10.4), and such annotations are never restorable (tracepack change 6).
- **Reads**: `ListCaptures` returns each capture's `capture_method`, `vantage` and `source_dialect`, and the selector takes a method filter (open question 1).

## Operations

**Configuration** (`docs/guides/configuration.md`, one table):
`logfeed.enabled` (default false), `logfeed.stream` (`TRACEKEEP_LOGFEED`), `logfeed.prefix` (`tklog`), `logfeed.profiles` (the accepted profile versions),
`logfeed.reorder_window` W (5 min), `logfeed.pairing_window` (3 entries), `logfeed.file_quiet` (1 h),
and `logfeed.sources[]`: `id`, `recorder_instance_id`, `tz`, `host_tool_id`, `connections` (`device=N` or `session=N` → `tool_id`).
A source missing `tz`, `recorder_instance_id` or `host_tool_id` refuses the start.

**Metrics** (ingest; `source` is a label, bounded by the hosts):
`logfeed_entries_total{source,outcome=record,annotation,duplicate,late}`, `logfeed_markers_total{source}`,
`logfeed_holes_total{source}`, `logfeed_hole_bytes_total{source}`, `logfeed_last_hole_timestamp_seconds{source}`,
`logfeed_unmapped_entries_total{source}`, `logfeed_deferred_messages{source,reason=profile-unknown,incompatible-configuration,key-refused,key-unavailable}`,
`logfeed_lag_seconds{source}` (wall clock minus the last sequenced entry's log time), `logfeed_lease_held{source}`,
and the stream's backlog beside the traffic stream's (`stream_backlog_messages{stream}`).

**Alerts and runbook sections**:

| Alert | Fires when | Runbook section |
|---|---|---|
| `TracekeepLogFeedHoles` | a hole declared on a source within the hour | `log-feed-holes` |
| `TracekeepLogFeedSilent` | a source's lag above `logfeed.file_quiet` | `log-feed-silent` |
| `TracekeepLogProfileUnknown` | an entry of an unaccepted profile deferred for 5 min | `log-profile-unknown` |
| `TracekeepLogConnectionUnmapped` | an entry of an unmapped connection within the hour | `log-connection-unmapped` |

`tracekeep status` lists each source with its lease holder, open files, lag and deferred entries.
A source's deferral (an unknown profile, a configuration it cannot execute, a refused key) holds that source only:
its isolation unit is the source, since its entries are converted in file order.

## Where the parser lives

Point h, settled by the owner: **tracekeep's own parser**, an internal package (`internal/logfeed/eapsml`), built on go-secs `sml` for the item tree.
veq's dialect description (`eapsml-dialect.md`) stays a description of the format; nothing of tracekeep imports or requires veq.
The format facts this proposal relies on are stated here and in the parser's tests, on synthetic entries only.

## Changes tracepack needs

To go back to the go-secs session; tracekeep does not edit go-secs.

1. **[STO §7] admits a live converter over a routed entry stream**:
   coverage (item 7) counted over the ranges the feed delivers, holes included, not the whole file;
   a late range under its hole's annotation as the one allowed double coverage;
   output (item 9) as generation-0 segments flushed from unbounded input, with `flush_interval_ns`;
   start and stop boundaries written by the converter once per capture, a continuation capture for entries after the stop.
2. **`source_ref` for a growing file** ([FMT §5], [STO §7] item 8): an identity (source, base name, inode or first-entry digest) instead of a content digest,
   and capture-id derivation from that identity; "a source file's name alone never stands for its contents" kept by the identity's digest part.
3. **A hole annotation**: `skipped-bytes` with an empty value naming source bytes never received (or a new `annotation_kind`), distinct from bytes skipped after a parse failure ([STO §7] item 3).
4. **`source_dialect`**: a value naming tracekeep's parser, distinct from veq's converter's (`eapsml/1 sml.Parse`), with the format's version.
5. **A source reference for data records** ([FMT §8], deferred in [OVW §6]): without it, coverage of a log capture cannot be checked from its packs alone.
6. **Masking of log annotations** ([SEM §8] *Baseline rules*): a baseline annotation rule for `unparsed-entry` in `log` captures, and optionally an annotation rule scoped by the S/F its entry names, so a malformed heartbeat need not be zeroed with a malformed process program.
7. **G5-249 wording**: confirm that a live converter consuming a non-tracepack feed stream and writing packs by [STO §7] is a log conversion that does not publish over the bus.

## Open questions for the owner

1. Should the machine API's default selection exclude `log` captures, so a reader asks for them by method (recommended), or return every capture with its method?
2. fluent-bit's `Offset_Key` with a multiline parser: which byte does it report for a grouped entry, and does the deployed version carry the inode? Decides how the profile computes ranges.
3. Range markers: accept one marker message per unrouted entry for detectable holes (recommended), or route traffic only,
   which leaves holes undetectable and loses a late entry as a duplicate?
4. An unmapped connection: into the host's capture with an alert (recommended), or held on the stream until mapped, which holds the whole source?
5. A body logged without a header: veq's converter keeps one printed by the main body logger as a record of unknown direction, while the relayed format facts say such a body was never sent.
   Which holds for the hosts in scope?
6. Should the log stage run in the `ingest` role behind `logfeed.enabled` (recommended), or as a role of its own?

## What it does not do

- No offline import of log files; a later proposal can add one over the same parser.
- No use of the host log to fill, confirm or correct a gateway capture.
- No transport events, epochs or thinning in a log capture.
- No parking of feed messages, and no termination of a source's deferred entries beyond `capture terminate` of a capture already created.
- No format change in the pipeline's other traffic: everything else the host logs goes to the log store unchanged.
