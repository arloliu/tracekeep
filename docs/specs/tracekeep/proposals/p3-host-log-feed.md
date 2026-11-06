# P3 — host log feed

Status: active (2026-11-03) — a live feed of an equipment host's SML log as `log` captures that claim no coverage

Covers a new tracekeep feature: a log pipeline parses the SECS traffic an equipment host prints to its text log as SML,
and tracekeep pairs the entries, builds the frames and writes `capture_method = log` captures as they arrive.
Read it before designing or reviewing the feature, and before changing a spec it amends (named per section).

## Problem

An equipment host (the program that drives a tool as the SECS host) logs every message it exchanges as SML text.
Where the gateway's capture is missing (before the gateway records a tool, during a gap, for a tool behind no recording gateway),
that log is the only record of the conversation, and people troubleshooting a tool read it by hand.
tracekeep consumes only layout 1 of the traffic stream (`ingest.md` §6.1), which serves only `raw-stream` and `decoded-message` captures on the capture clock (G5-249, [STO §4] *Scope of the bus*).
The owner decided (2026-11-02) that tracekeep converts the log live, from a stream of entries a log pipeline has already parsed.

## Positioning

- A host-log capture is a separate source: `capture_method = log`, `vantage = host`, records `reconstructed` ([SEM §2]), `time_source = source-log`.
  It never stands in for the gateway's capture, which is `raw-stream` from vantage `intermediary` with `wire-exact` records (V-35, LS-1).
  The two are never merged, never deduplicated against each other and never compared seq by seq.
- It shows the host's side: the gateway rewrites the session ID (V-8), so a logged session ID is the host's, never the tool's (LS-2).
- It has no transport events, so every record is in epoch 0 and `correlation-incomplete` ([STO §7] item 8).
- **It claims no coverage.** A live feed's captures do not claim byte-exact coverage of the log, and each capture's `notes` say so.
  What the pipeline lost shows only as missing records; nothing declares a hole.
- It is not a general log platform (`positioning.md` §1): only the entries the pipeline classifies as traffic reach tracekeep.
- veq is out of scope: tracekeep depends on nothing in veq.
  veq's description of the host's log dialect (kept in veq) may be cited as a description of the format only.

## Data flow

```
host pod
  host program → dated text log files
  fluent-bit sidecar: tail (DB checkpoint) → multiline grouping into entries → forward
Vector forwarder
  route: traffic entries → parse each entry statelessly into fields → NATS sink → TRACEKEEP_LOGFEED, subject tklog.<source>
         every other entry → the deployment's log store
tracekeep ingest, the log stage (logfeed.enabled; one converter per source under its group's catalog lease)
  reorder window → dedup by offset → pair header and body → sml.Parse → build frame → classify + mask
    → batch per (capture, UTC hour) → tracepack.Writer → ObjectStore.Put (staging/) → register → ack
tracekeep compact, api: unchanged, except as stated under Storage and catalog
```

## Pipeline

**fluent-bit** tails the host's files, checkpoints them in its DB and groups lines into entries.
An entry is one timestamped line and the untimestamped continuation lines after it,
found by a multiline parser whose start state matches the timestamp at the start of a line;
its flush timeout must exceed the host's longest pause inside one body, or a body is split into two entries.
fluent-bit sends one file's entries in order.

**Vector** routes the entries and parses each one statelessly into the fields below, with no `reduce`:
an entry is complete when fluent-bit sends it, so Vector never joins events.
Traffic entries go to tracekeep; everything else goes to the deployment's log store.
Traffic entries are, by their role: the header lines of received and of sent messages, primaries and replies;
print-body entries, a line naming the SxFy with an optional W followed by the SML tree up to its closing line;
and encrypted-body lines.
Lines of other threads interleaved between a header and its body go to the log store.
The literal patterns live in the pipeline configuration shipped with the deployment, never in this spec:
they name the host software's own loggers and tokens.

**tracekeep needs no host-format parser.**
It pairs the parsed entries and calls go-secs `sml.Parse`, non-strict, which reads an ASCII value by its declared length `[n]`,
so an unescaped single quote inside a value parses.

## The field schema

The contract between Vector and tracekeep is a versioned field schema, one JSON object per entry:

| Field | Meaning |
|---|---|
| `v` | the schema version, 1 |
| `source` | the pod or host the file was tailed on; tracekeep's `logfeed.sources` key, of `[A-Za-z0-9_-]` |
| `file` | the file's base name, dated by the host |
| `offset` | the entry's byte offset in its file, as fluent-bit reports it |
| `ts` | the entry's timestamp string as logged, host local time; Vector converts no time |
| `kind` | `header`, `body`, `encrypted` or `unclassified` |
| `dir` | on a header: sent or received |
| `device` | on a header that carries the host's device-number token: its value |
| `header` | on a header: the logged message header, hex |
| `tid` | on a header or body that carries one: the host's transaction id |
| `sxfy` | on a header, body or encrypted body: the stream and function it names |
| `text` | on a body: the SML text; on an encrypted body: the ciphertext as logged; on an unclassified entry: its raw lines; absent on a header |

**JSON size.**
A message carries the extracted fields and one text: the SML text for a body, the raw lines only for an entry Vector could not classify.
Never the raw text and the SML text together for every entry: that doubles the volume (*Sizing*).

**An entry tracekeep cannot read** (an unknown schema version, a missing or malformed field for its kind) is counted (`logfeed_schema_unknown_total`)
and kept as an `unrecognised-line` annotation holding the message as received, in the host capture of its subject's source;
it is never dropped silently and never holds the source.

## Stream

- Its own stream, `TRACEKEEP_LOGFEED` by default, subjects `<logprefix>.<source>`, `logprefix` `tklog` by default.
  `ValidatePrefixes` refuses a logprefix whose first token equals the traffic or parking prefix's,
  and `bus.Preflight` and `migrate` check `busconfig.SubjectsOverlap` between the feed stream and both existing streams.
- Work-queue retention and file storage, like `TRACEKEEP_TRAFFIC`: a message is deleted once acked, and nothing is kept after the ack.
- Unlike the traffic stream, a size limit: `max_bytes` from *Sizing*, with discard-new, so a full stream refuses Vector's publish instead of losing old entries silently.
- The feed shares the traffic cluster (V-30) or uses its own; the gateway's bus never.

## Conversion

The log stage converts in real time, as `ingest` does for HSMS traffic.

- **Consume** continuously, with per-message acks (`AckExplicit`) and **no in-progress acks**:
  the feed consumer's `ack_wait` exceeds F plus a flush's upload and registration (default 2 × F),
  since at the held entries *Sizing* counts an in-progress ack every `ack_wait` / 3 would send more acks a second than the feed sends entries.
  A crash redelivers after `ack_wait`, and pairing rebuilds from the redelivery.
  The traffic consumer's heartbeat (`architecture.md` §3.1) keeps every held message in progress the same way, so at a fleet of this size it meets the same load: noted for the traffic ingest, not changed here.
- **Order.** Entries are converted in arrival order, through a short reorder window (`logfeed.reorder_window`, seconds) that releases a file's entries in offset order;
  it covers the jitter of several Vector instances, since fluent-bit sends one file in order.
- **Dedup and late entries.** The catalog keeps the last converted offset per (source, file), written in the registration transaction.
  An entry at or below it, or below an entry the stage has already converted, is counted as a duplicate or a late entry, acked, and never converted.
  Offsets only need to increase within a file.
- **Pairing**, in memory.
  A header waits as the pending header of its SxFy for at most the pairing window (`logfeed.pairing_window`, a few entries of its file);
  a body takes the pending header of its SxFy, and of its TID when both carry one.
  A second header of an SxFy before its body ends the first as a header without a body.
  Entries after a pending header wait with it, so the converted entries of a file are always a prefix of it.
- **Trust.** A header whose logged header names the same stream and function as its body gives the W bit, the session ID and the system bytes;
  some host versions log a reply under its primary's header, and then those fields stay invalid (`field_validity` clear).
  A W in the body's SML is source text, so a W on a reply fails the parse.
- **Restarts.** Pairing survives a restart because an entry is acked only after the pack holding it is registered:
  a restart redelivers every unacked header and body, and pairing rebuilds from the redelivery.
  No pairing state is persisted.
- **Seq** is assigned at conversion, dense per capture, in conversion order, continuing from the capture's `highest_seq` after a restart.
- **What becomes what**, each at its entry's time:

| Entry | Becomes |
|---|---|
| header and body that parse and build | one `data` record, `reconstructed`, `reconstructed-ok`, direction from the header |
| body whose SML does not parse, or not as exactly one message | `unparsed-entry`, `parse-failed`, holding its SML text |
| encrypted body | `unparsed-entry` holding the ciphertext (*Encrypted bodies*) |
| header with no body within the window | `unparsed-entry`, `parse-failed`, holding the header's fields as received |
| body with no header | `unparsed-entry`, never a record: the host built it and never sent it |
| unclassified entry | `unrecognised-line`, holding its raw lines |
| entry tracekeep cannot read | `unrecognised-line`, holding the message as received |

- **Flush and registration.** The stage batches per (capture, UTC hour), flushes a generation-0 segment every F and registers it, as `ingest.md` §6.2–§6.3 do.
  A source's flush registers its segments in one transaction, which also sets each of its files' last converted offset to the last entry the flush holds,
  so the offset never passes an entry that is not in a registered pack.
- **Ack rule**, ingest's: an entry is acked once every record or annotation it went into is in a registered pack.
  A duplicate or late entry is acked at once.

## Captures

- **One capture per (source, connection, file).**
  A catalog row keyed uniquely by (source, connection, file) holds the capture's `capture_id` from its creation;
  the id is never derived again, so the file needs no inode or digest: file names are dated.
- **One converter per source at a time**, under the catalog lease of the source's group (*Sizing*).
- **Boundaries.** The converter writes the capture's start with its first record,
  and its stop when the source's next dated file appears or when the source has been idle past `logfeed.idle_period`;
  an entry of the file after its stop is late (*Dedup and late entries*), and never reopens it.
- **Connection**: the header's device-number token where the host logs one, else the session ID of its logged header ([STO §7] item 4).
  The logged session ID is the host's, never a tool's, because the gateway rewrites it (V-8).
  Several connections in one log are normal: the tool, a load-port controller on its own session, a peer on stream 99.
- **Tool**: the operator maps each connection of a source to its `tool_id` (`logfeed.sources[].connections`), as [STO §7] item 7 asks of a converter.
  An entry of an unmapped connection goes to the host's own capture of its file, whose `tool_id` is the source's id, and raises `TracekeepLogConnectionUnmapped`;
  the host capture is created lazily, on its file's first such entry, so a file with none has none.
  So do the entries of no connection: a body without a header, an unclassified entry.
- **recorder_instance_id**: derived from the source's id and stable, never the gateway's,
  so a source's captures form their own series for successor lookup (`catalog.md` §7.3);
  a capture of the next file of a source and connection names its predecessor in `previous_capture_id` ([FMT I-7]).
- `source_ref`: `<source>/<base name>`, since dated base names repeat across sources and a pack read outside tracekeep (an export, an archive) must say whose file it was; `source_dialect`: `host-sml-feed/1`, the suffix being the field schema version.
- The compactor's close of a capture left open (`compactor.md` §8.2) never applies; the converter writes the stop.

## Time

`time_source` is the log's own local timestamps, `source-log`, with a required `tz` per source (`logfeed.sources[].tz`, an IANA name);
never the capture clock, and never fluent-bit's or Vector's event time.
tracekeep parses `ts` in that zone and resolves daylight saving itself:
a local time that occurs twice or never takes the earlier reading and is `ordering-uncertain` ([STO §7] item 5).
The pack has no `capture_origin_*` and no `clock_step_tolerance_ns` ([FMT §5]).
A record's hour, and so its scope, is the UTC hour of its log time ([STO §4] *Scope and order*).

## Encrypted bodies

Decided: option A1.

- An encrypted body becomes an `unparsed-entry` annotation holding the ciphertext.
  Its header line is plaintext, so the direction, S/F, system bytes and time are known from the header's own annotation or its fields.
- The source-text masking baseline ([SEM §8], its annotation rules in domain `source-text`) zeroes the ciphertext under a policy that zeroes source text.
  No new secret.
- The host's encryption is deterministic: the first 32 ciphertext bytes are equal across messages.
  A1 inherits none of that weakness:
  tracekeep holds no host key, derives nothing from the ciphertext and encrypts nothing under the host's scheme,
  so it exposes nothing the host's log file does not, and a policy that zeroes source text removes even that.

## Storage and catalog

- **Admission is internal.** The log stage creates a capture with a new catalog operation, `AdmitLog`:
  the capture row inserted `open` with the capture-level metadata the stage built, and the processing configuration bound as `Admit` binds it (`catalog.md` §7.2, `rollouts.md` §6.4).
  It has no delivery identity, so nothing about it is ever parked, `pending` or `rejected`.
- **Registration** follows `ingest.md` §6.3 with feed evidence instead of delivery evidence:
  the files' last converted offsets, written in the same transaction; no receipt and no `parked` state.
  Two catalog tables are new: the capture key (source, connection, file → `capture_id`) and the file offset (source, file → last converted offset).
- **Segments**: generation-0, under [STO §3] keys, flushed every F with `flush_interval_ns`, merged by the compactor as any scope.
  The library needs no change: the writer's `PackFacts.DurableRecorder` is set, under which `PackMeta.Validate` takes `flush_interval_ns`,
  and each record is built and written with `SegmentWriter.Append` (fidelity `reconstructed`, its `decode_status` and `field_validity` set), since `AppendFrame` refuses `capture_method=log`.
- **Completeness** (`catalog.md` §7.3): a log capture's seq is dense by construction, so it shows no gap, and that proves nothing about the log;
  its span starts at its first record.
- **Masking** (`masking-keys.md` §9) applies as to every capture: the bound policy masks the reconstructed frames' items.
  The text of an `unparsed-entry` or `unrecognised-line` can hold any source content, and only an annotation rule in domain `source-text` zeroes it ([SEM §8]).
- **Reads**: `ListCaptures` returns each capture's `capture_method`, `vantage` and `source_dialect`;
  the selector's default leaves `log` captures out, and a reader asks for them by method (applies to `api.md` §10.1 when P3 is accepted).
- Reconciler, retention and deletion: unchanged.

## Sizing

The load: 2000 tools.
A 69-minute sample of one tool gives about 3.3 MB/h and 0.8 messages/s, header and body being separate entries, so about 570 B an entry.
Across 2000 tools: about 6.6 GB/h (1.8 MB/s) before JSON overhead, 1,600 messages/s, 3,200 entries/s, about 160 GB/day.

### The stream's size

**JSON size.**
The field names, quoting and added values (`v`, `source`, `file`, `offset`, `kind`) add about 150–200 B an entry,
and JSON escaping of the SML text's newlines and quotes a little more;
JetStream stores each message with its subject and headers, about 50 B more.
A header entry's message is about twice its line; a body's is 10–20% above its lines.
The assumption here: **1.5 × the raw bytes**, about 850 B an entry.
Sending the raw lines beside the SML text for every entry would add about 500 B an entry, 1.9 × the raw bytes.

**max_bytes** = the tracekeep outage tolerated × 6.6 GB/h × 1.5:
for 4 h, 26.4 GB raw, about **40 GB**, with discard-new.

**When the stream is full**, the NATS sink's publish fails and Vector's disk buffer for that sink fills.
Per their documentation:

- `when_full: block` propagates backpressure to the source.
  That stalls the shared forward input, and so the log-store leg too;
  then fluent-bit buffers (its filesystem storage) and finally pauses tailing.
  Files stay on disk and the checkpoint resumes them, provided rotation does not delete a file before it is read.
- `when_full: drop_newest` keeps the log-store leg flowing but drops traffic entries.
  That shows only as missing records, because no coverage is claimed.

Recommended: **`block`**, sized so it rarely engages:
`max_bytes` and the buffers cover the outage tolerated, and the host keeps its files on disk longer than that, so an outage beyond it loses nothing and delays the log store instead.
Where the log store must never stall, `drop_newest` is the choice.
The choice is the deployment's.

### Ingest scaling

2000 sources mean 2000 converters' state, one lease per source group (below) and about 2000 segments every F, one transaction per source flush.
A pack holds one capture ([FMT §4]'s header names one `capture_id`; [STO §7] item 9; the segment key under `<capture_id>`, [STO §3]),
so several sources cannot share a segment.
Writer time on SQLite (single writer) from phase 5's *Baselines*:
a registration costs about 1.7 ms fixed, its synchronization included, plus about 30 µs a delivery (`RegisterPack/evidence` at 100 and 1,000 deliveries),
a delivery being an entry, two a record; the feed's evidence writes no per-delivery rows, so this is an upper bound.
A segment's deletion after its merge is one synchronized transaction, unmeasured, taken as `RegisterPack/miss` (0.6 ms).
`Flush` of 100 records takes about 4.3 ms with its put and synchronization, and the ingester flushes due batches one after another (P9).

| F | registrations/s | entries a registration | writer: registration | writer: deletion | serial flush time a cycle | segments/h | merge inputs a scope | held entries | batched bytes |
|---|---|---|---|---|---|---|---|---|---|
| 10 s | 200 | 16 | ≈ 440 ms/s | ≈ 120 ms/s | ≈ 3.3 s (33%) | 720,000 | 360 | 32,000 | ≤ 18 MB |
| 60 s | 33 | 96 | ≈ 150 ms/s | ≈ 20 ms/s | ≈ 5.6 s (9%) | 120,000 | 60 | 192,000 | ≤ 110 MB |
| 300 s | 6.7 | 480 | ≈ 110 ms/s | ≈ 4 ms/s | ≈ 16 s (5%) | 24,000 | 12 | 960,000 | ≤ 550 MB |

- **The limit**: on SQLite, **F ≥ 60 s** for 2000 sources (about 17% of the writer, on top of the traffic ingest's own registrations); F = 10 s takes about 56% and needs MariaDB, whose rows are not measured yet.
  Recommended: F = 300 s, ingest's default (T-25).
  The per-entry part, 3,200 × 30 µs ≈ 96 ms/s, does not shrink with F.
- **The overlap probe** (P2) scans one capture's earlier ranges, and a capture is one dated file: 86,400 / F ranges a day at most, 1,440 at F = 60 s, where a probe costs about 0.1 ms.
- **Objects**: the segments above an hour, plus 2000 archives an hour; the compactor merges 2000 scopes an hour, each of the inputs above.
  The merge's baselines reach 64 inputs, so F = 10 s lies outside them; the orphan pass costs about 10 µs a `staging/` object.
- **Leases**: one row per source renewed every 10 s is 200 commits/s, about 120 ms/s of the writer whatever F.
  Decided: **a lease per source group** (the source's id hashed into `logfeed.lease_groups`, default 32): 3.2 commits/s,
  one consumer per group filtering its sources' subjects, and still one converter per source, since a source is in one group.
  The alternative, one transaction renewing all a holder's per-source leases, would keep 2000 rows and 2000 consumers.
- **Memory**: pairing state, a few entries a source, about 2 KB, 4 MB in all;
  the reorder window, about 5 KB a source at 5 s, 10 MB in all;
  the batches up to the bytes above, an upper bound, since a built frame is smaller than its SML;
  and the held entries above, about 300 B each, which `max_ack_pending` must exceed, split across the groups' consumers.

## Operations

**Configuration** (`docs/guides/configuration.md`, one table):
`logfeed.enabled` (default false), `logfeed.stream` (`TRACEKEEP_LOGFEED`), `logfeed.prefix` (`tklog`),
`logfeed.pairing_window` (3 entries), `logfeed.reorder_window` (5 s), `logfeed.flush_interval` F (ingest's F), `logfeed.idle_period` (24 h), `logfeed.lease_groups` (32),
and per source, `logfeed.sources[]`: `id`, `tz` and `connections` (`device=N` or `session=N` → `tool_id`).
A source without `tz` refuses the start.

**Alerts**:

| Alert | Fires when | Runbook section |
|---|---|---|
| `TracekeepLogFeedSilent` | a source sent no entry for 15 min (a threshold of the alert rule, not of the idle period), or the stream's backlog grows | `log-feed-silent` |
| `TracekeepLogConnectionUnmapped` | an entry of an unmapped connection within the hour | `log-connection-unmapped` |
| `TracekeepLogSchemaUnknown` | an entry tracekeep cannot read within the hour | `log-schema-unknown` |

**Metrics** (ingest; `source` a label, bounded by the hosts):
`logfeed_entries_total{source,outcome=record|annotation|duplicate|late}`, `logfeed_unpaired_total{source,side=header|body}`,
`logfeed_annotations_total{source,kind}`, `logfeed_schema_unknown_total{source}`,
and the stream's backlog beside the traffic stream's (`stream_backlog_messages{stream}`).

## Decided by the owner

On 2026-11-06 the owner accepted the proposal on a new capture identity and settled its open points, superseding decisions 1, 4 and 9 among others: `tracekeep-decisions.md` T-59 and T-60.

On 2026-11-02:

1. The machine API's default selection leaves `log` captures out; a reader asks for them by method.
2. An offset only needs to increase within a file.
3. No range markers.
4. An unmapped connection goes to the host capture, with an alert.
5. A body without a header becomes an annotation.
6. The stage runs in the `ingest` role, behind `logfeed.enabled`.

On 2026-11-03:

7. A lease per source group, `logfeed.lease_groups` groups, default 32 (*Sizing*).
8. An entry after an idle stop is counted and acked as late: its file's capture is stopped and its key taken, so it never reopens;
   `logfeed.idle_period` (24 h) is long enough that only a source that stopped logging meets it.
9. The host capture is created lazily, on its file's first entry of no mapped connection (*Captures*).
10. One registration transaction per source flush, a new catalog operation beside `RegisterPack`, which registers per segment:
    several captures of one file share its offset row, so a per-segment update could pass an entry not yet registered.

## Changes tracepack needs

Done in go-secs as G5-302 (tracepack spec v2.42, documentation only), covering the four items below and a [FMT §5] `flush_interval_ns` row.
The feed builds on the tracepack release T-59 requires: `PackFacts.DurableRecorder` set, `flush_interval_ns` written, records written with `SegmentWriter.Append`.

1. **[STO §7], one paragraph**:
   "A live converter MAY consume an unbounded stream of entries and write generation-0 `segment` packs as they arrive, flushed at an interval with `flush_interval_ns`,
   writing each capture's start and stop boundaries itself.
   Its captures claim no byte-exact coverage, and their `notes` say so:
   item 7's coverage, item 8's source file set, content digest and `capture_id` derivation, and item 9's finite input do not apply to it;
   each pack's `source_ref` names the capture's one source file by its source and base name, and a capture keeps the `capture_id` it was created with."
2. **G5-249, one sentence**:
   "A live converter that consumes a non-tracepack feed and writes packs by [STO §7] is a log conversion that never publishes over the bus."
3. **`tracepack-go.md` §2**: a row in the producer table for tracekeep's log ingest, a live converter of `capture_method=log`.
4. **`PackFacts.DurableRecorder`'s godoc**: a live converter sets it too (today it says a standalone recorder and a converter never do).

## What it does not do

- No byte-exact coverage, holes or range markers; no offline import of log files.
- No use of the host log to fill, confirm or correct a gateway capture.
- No transport events, epochs or thinning in a log capture.
- No parking of feed messages.
- No decrypting of encrypted bodies.
  Possible later work, as a separate proposal: a per-source `source-decryption` key kind resolved through `internal/keys` providers, never a masking key;
  the EAP maintainers specify the scheme;
  and the plaintext is masked by the gateway's S7F26 rule before any write.
- No format change in the pipeline's other traffic: everything else the host logs goes to the log store unchanged.
