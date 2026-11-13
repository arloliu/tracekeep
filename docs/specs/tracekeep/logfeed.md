# tracekeep — host log feed

Status: current (2026-11-07) — from P3 as T-60 accepted it, delivered by the host log feed plan (#101–#112)

Covers the host log feed: a log pipeline parses the SECS traffic a host program prints to its text log,
and tracekeep pairs the entries, builds the frames and writes `capture_method = log` captures as they arrive.
New text, section 15, written from P3 (host log feed), T-59, T-60 and T-62; [README.md](README.md) maps each section to its file.
Read before touching `internal/logfeed`, the feed's streams or their configuration, or the pipeline guide (`docs/guides/log-pipeline.md`),
and before changing a part of another spec this one names.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Positioning**: the host program's log is the legacy route, converted best effort; the gateway's capture stays the preferred record.
  A log capture is its own series, never merged with, deduplicated against or compared to a gateway's, and it claims no coverage (§15.1).
- **The pipeline's contract**: one JSON entry per message on `tklog.<equipment token>`, published with a publish acknowledgement and a message id,
  its in-flight time bounded below the reorder window (§15.2, §15.3).
- **Streams**: one a lease group, `TRACEKEEP_LOGFEED_<n>`, of one replica by default and placed round-robin by the servers' tags,
  work-queue, file storage without compression, `max_bytes` with discard-new, a duplicate window;
  the servers' account mapping, which the operations team maintains, partitions the equipment into the groups' subjects;
  group consumers whose state is in memory; ensured by `migrate`, no incarnation (§15.4).
- **Conversion**: per lease group, under its catalog lease: reorder by offset, join duplicates, drop stale entries below a file's watermark, pair header and body, `sml.Parse`, build the frame, classify, mask, batch (§15.5).
- **Captures**: one per (source, `equipment_id`, device, file); the converter writes the start and the stop; the compactor never closes one (§15.6).
- **Registration**: one transaction per equipment's flush, its capture rows locked in `capture_id` order, the group's fence checked, the file watermarks raised;
  an entry is acknowledged after the pack holding it is registered (§15.7).
- **Recovery**: a crash redelivers; an orphan segment is deleted, never adopted (§15.8).
- **Cost**: tracekeep's CPU is not the limit; the catalog writer, the bus's message count and the held acknowledgements are (§15.10).

## 15. Host log feed

### 15.1 Positioning

- A host log capture records what a host program logged of the SECS traffic it exchanged with the equipment.
  Its records are `reconstructed` ([SEM §2]), its `time_source` is `source-log`, its vantage `host` and its directions the host program's own.
- The host program's log is the **legacy route**, converted best effort; the gateway's capture is the preferred record of an equipment (T-60).
  The two are two series of one device (T-59): never merged, never deduplicated against each other, never compared seq by seq, never chained, never flagged as overlapping.
- A logged session ID is the host program's, never the equipment's, since the gateway rewrites it (V-8, LS-2).
- A log capture has no transport events, so every record is in epoch 0 and `correlation-incomplete` ([STO §7] item 8).
- **It claims no coverage.**
  Its captures claim no byte-exact coverage of the log, and each capture's `notes` say so;
  what the pipeline lost shows only as missing records, and nothing declares a hole (G5-302).
- It is not a general log platform (`positioning.md` §1): only the entries the pipeline classifies as traffic reach tracekeep.
- The gateway's own source is an operator-set, required key of its configuration, never its application id (T-59, T-62).
- tracekeep depends on nothing in veq; veq's description of the host program's log format may be cited as a description of the format only.

### 15.2 Data flow and the pipeline

```
host pod
  host program → dated text log files
  tailer: tail with a checkpoint → group lines into entries → forward
forwarder
  route: traffic entries → parse each entry statelessly into fields → NATS, subject tklog.<equipment token>
         every other entry → the deployment's log store
NATS: the account's mapping tklog.<equipment token> → tklog.<group>.<equipment token>, group n's stream TRACEKEEP_LOGFEED_<n>
tracekeep ingest, the log stage (logfeed.enabled; one converter per lease group, under its catalog lease)
  reorder by offset → dedup → pair header and body → sml.Parse → build frame → classify + mask
    → batch per (capture, UTC hour) → tracepack writer → ObjectStore.Put (staging/) → register → ack
tracekeep compact, api: as §15.8 states
```

The deployment runs a tailer (fluent-bit) beside the host program and a forwarder (Vector);
`docs/guides/log-pipeline.md` ships their configuration with placeholder patterns only,
since the literal patterns name the host software's own loggers and tokens and never enter this repository.

**The tailer** tails the host program's files, checkpoints them and groups lines into entries.
An entry is one timestamped line and the untimestamped continuation lines after it;
the grouping's flush timeout exceeds the host program's longest pause inside one body, or a body is split into two entries.
The tailer sends one file's entries in order.

**The forwarder** routes entries and parses each one statelessly into the fields of §15.3, joining no events:
an entry is complete when the tailer sends it.
Traffic entries are the header lines of sent and received messages, primaries and replies;
print-body entries, a line naming the SxFy with an optional W followed by the SML tree to its closing line;
and encrypted-body lines.
Lines of other threads interleaved between a header and its body go to the log store.

**What the pipeline guarantees** tracekeep, whatever its tools:

- one entry per message, never a batch of entries and never a header joined to its body;
- a JetStream publish whose acknowledgement it awaits, retried on failure;
- the message id of §15.3 on every message;
- an **in-flight bound**: an entry is published, retries included, within the reorder window (§15.5) of the next entry its source tailed for the equipment being published, across a file's rotation too,
  so concurrent publishes and retries reorder an equipment's entries only inside the window (T-62);
  an entry published later is stale and never converted (§15.5 *Stale entries*);
- its buffers sized for the outages the deployment accepts: a full stream's, and a server's loss, which stalls the whole feed (§15.10).

The forwarder's sink publishes one event a message with no batching or compression,
awaits each publish's acknowledgement in JetStream mode, retries every failed publish, and holds one request in flight unless configured otherwise,
so its concurrency is raised to reach the fleet's rate, its retries are left unbounded in number, since a bound drops entries,
and each retry's backoff is capped well below the reorder window, so entries retried through an outage are stored close behind their file's later ones;
the guide states both.
A publish to a group whose server is lost fails and is retried, keeping its request's slot,
so the lost groups' publishes come to hold every slot and the whole feed waits for the server, whatever the concurrency (§15.10).

### 15.3 The entry

The contract between the pipeline and tracekeep is a versioned field schema, one JSON object a message (T-62: JSON, not a binary codec, since tracekeep's decoding is not the cost, §15.10).

| Field | Meaning |
|---|---|
| `v` | the schema version, 1 |
| `equipment_id` | the equipment the host program drives, as `equipment_id` names it everywhere (T-59) |
| `source` | the feed deployment's name (T-59), stable across restarts, never a host or pod name; at most 128 characters |
| `tz` | the IANA time zone the host program logs in |
| `file` | the file's base name, dated by the host program; at most 128 characters |
| `offset` | the entry's byte offset in its file, as the tailer reports it |
| `ts` | the entry's logged local wall time, rewritten to `YYYY-MM-DD HH:MM:SS[.fraction]` without a zone; the pipeline changes its layout, never its value |
| `kind` | `header`, `body`, `encrypted` or `unclassified` |
| `dir` | on a header: `sent` or `received` |
| `device` | on a header that carries the host program's device-number token: its value |
| `header` | on a header that logs the HSMS header: the 10 header bytes, hex |
| `tid` | on a header or body that carries one: the host program's transaction id |
| `sxfy` | on a header, body or encrypted body: the stream and function it names |
| `text` | on a body: the SML text; on an encrypted body: the ciphertext as logged; on an unclassified entry: its raw lines; absent on a header |

- **Subject**: `tklog.<token>`, the token being `bus.EquipmentToken(equipment_id)` ([STO §4] *Bus message*);
  an entry whose `equipment_id` the token does not spell is one tracekeep cannot read.
- **Message id** (`Nats-Msg-Id`): `<source>/<token>/<file>/<offset>`, so the stream drops a retried publish within its duplicate window (T-62).
- **Size**: an entry carries the extracted fields and one text, never the raw lines beside the SML text, which would add about 500 B an entry.
- **An entry tracekeep cannot read** is counted (`outcome="unreadable"`, §15.11), acknowledged and never converted when tracekeep cannot place or time it:
  JSON that does not parse, an unknown `v`, a missing or malformed `equipment_id`, `source`, `file`, `offset` or `ts` (a `source` or `file` of more than 128 characters included, which the catalog's keys cannot hold), a `tz` that names no zone, or a token that does not spell the equipment.
  One it can place and time, whose kind's own fields are missing or malformed, is an `unrecognised-line` annotation holding the message as received, in its file's capture of the main device.
  Neither holds the equipment's conversion.

### 15.4 Stream and consumers

- **The streams**, one a lease group (T-67): group n's is `<stream>_<n>`, `logfeed.stream` being `TRACEKEEP_LOGFEED` by default,
  its subjects `<logprefix>.<n>.*`, `logprefix` being `tklog` by default (`logfeed.prefix`), and n running below G, `logfeed.lease_groups` (32).
  The service refuses a log prefix whose first token equals the traffic or parking prefix's, and a feed stream named as either stream,
  and `migrate` and preflight check every group stream's subjects against both existing streams (`busconfig.SubjectsOverlap`);
  the check is the service's, never the SDK's `busconfig` validation (T-60).
- Work-queue retention and file storage, as `TRACEKEEP_TRAFFIC`: a message is deleted once acknowledged.
- **No compression** (T-64), which `migrate` cannot change in a running store, so it refuses a compressed feed stream, to be drained, deleted and migrated again:
  a work queue's acknowledgement into a compressed block makes the server decompress the whole block,
  which under the fleet's load took about 1.1 GB of memory a server and raised the publish latency's 99th percentile from 51 ms to 620 ms (§15.10).
- **`max_bytes`**: each stream's is `logfeed.max_bytes` / G × 1.25 (§15.10, T-68), with discard-new, so a full stream refuses the pipeline's publish instead of losing old entries silently;
  the limit counts a message's logical bytes, which the disk holds uncompressed on each replica.
  `logfeed.max_bytes` is the feed's total, and the streams' limits sum to 1.25 times it, the headroom for a group fuller than the mean;
  a group past its own limit refuses publishes while the others have room.
- **Replicas** (`logfeed.replicas`): one by default (T-67), so a server's loss stalls the whole feed for the outage, the entries waiting in the pipeline's buffers (§15.10, T-68).
  Three keep a group's stream taking publishes once a remaining server leads it, at about three times the bus's resources (T-67).
- **Placement** (`logfeed.placement_tags`): group n's stream is placed on the server tagged with the list's tag n mod its length, so the streams spread round-robin over the servers;
  an empty list places none, for a single server, since unplaced streams of one replica all landed on one server (T-68).
  The operations team tags each server (`server_tags`), each tag naming one server's: two tags on one server put both shares on it.
  The list places streams of one replica only: a stream's tags apply to each of its replicas, so a tag naming one server leaves three replicas nowhere to go,
  and `migrate` refuses a list with `logfeed.replicas` above one, the cluster then spreading each stream's replicas over its servers.
  `migrate` places a stream when it creates it; changing the list is a `migrate` step on a drained feed, as changing G is.
- **A duplicate window** (`logfeed.duplicate_window`, 2 min), which the message id keys (T-62), longer than a retry's delay while the servers answer, the acknowledgement's timeout and the backoff's cap;
  an outage longer than it can store an entry twice whose first publish was stored but not acknowledged.
- **The mapping** (T-62, T-67, T-68): the servers' account mapping, in the account the pipeline publishes in,
  maps `<logprefix>.*` to `<logprefix>.{{partition(G,1)}}.{{wildcard(1)}}`,
  so the server assigns each equipment to one group by a deterministic hash of its token, and the publish lands on that group's stream.
  The operations team maintains it in the servers' configuration files (`docs/guides/log-pipeline.md` *The servers*), and tracekeep never writes it;
  under `tracekeep dev` and in the tests, `internal/testbus` gives the embedded server's account the same mapping.
  Its partition count is G: a larger count sends some publishes to subjects no stream holds, which the pipeline retries, stalling the whole feed as a lost server does,
  and a smaller one leaves the higher groups' streams empty and loads the others more;
  so preflight checks that the mapping's count is G, its probe publishes reaching every group's stream and no subject without one (§15.11).
  Group n's durable pull consumer, `logfeed-<n>`, is on group n's stream.
  tracekeep keeps no list of equipment and computes no hash: a group's equipment are those its consumer delivers.
- **Changing G** is a `migrate` step with the operations team's change of the mapping's count,
  run with the pipeline stopped, the feed drained and every replica's log stage stopped; `migrate` refuses it while a group stream holds a message.
  It creates the new groups' streams and consumers and deletes the retired groups', then, judged from the lease rows alone so a run interrupted between the two completes on the next,
  increments the fence of every `logfeed/<n>` lease, deleting those of retired groups, so no converter of the old G registers again.
  An open capture keeps the group that last registered it until a converter of its new group registers it (§15.6);
  group 0's converter stops the files of captures a retired group last registered once they are idle (§15.6 *Boundaries*).
- **The single stream's replacement**: a store holding the earlier layout's one stream, named `logfeed.stream` with a subject transform,
  has `migrate` replace it by the group streams once it holds no message, and refuse while it holds one (T-68).
- **No incarnation row**: `migrate` ensures the streams and their consumers, and a recreated stream needs none of parking's checks, since nothing is ever parked from it (T-60).
- The feed shares the traffic cluster (V-30), never the gateway's bus, never a cluster of its own (T-60).
- **Consumers**: each takes ingest's `ack_wait` and its in-progress-ack heartbeat (`architecture.md` §3.1, T-60),
  `AckExplicit` and per-message acknowledgements, `max_deliver` unlimited,
  and `max_ack_pending` (`logfeed.max_ack_pending`, 100,000) above its group's held entries (§15.10);
  their state is kept in memory, at the stream's replicas, which a work queue requires (T-64):
  a file consumer writes its whole pending state up to ten times a second on each replica, which under the fleet's load took a third or more of the bus servers' CPU,
  and a consumer lost with the cluster is re-created as a takeover re-creates it (§15.5), delivering every unacknowledged message again from the oldest.
  A group whose server is lost fails its fetches at once: its converter gives the group up, releasing its lease,
  and a converter taking it again after its pause, once the server is back with its storage, converts the redelivered entries afresh (T-68).
- **Lease groups**: group n's lease is the `leases` row `logfeed/<n>`, with a fence token, renewed and lost as the compactor's lease is (`compactor.md` §8.1).
  An `ingest` replica with the feed enabled takes free group leases up to its share, the groups divided by the live members of `members` (`rollouts.md` §6.4), rounded up,
  and consumes only the groups it holds; a replica that loses a lease stops that group's converter at once, acknowledging nothing more.

### 15.5 Conversion

The log stage converts in real time, as `ingest` does for the gateway's traffic, one converter per lease group.

**Order.**
A converter releases a file's entries in offset order through a reorder window (`logfeed.reorder_window`, 30 s, T-62):
an entry is released once it has waited the window, since the offsets of a file's traffic entries are not contiguous and a gap never proves an entry missing;
the window covers the pipeline's concurrency and retries (§15.2 *in-flight bound*), never the consumer's redelivery, which *Taking a lease* covers.

**Duplicates.**
An entry at an offset the converter holds, not yet registered, is a **duplicate** of it, such as a redelivery after a missed in-progress acknowledgement:
its handle joins the held entry's and is acknowledged with it, after the pack holding the entry is registered, never before,
since an acknowledgement deletes the message from the work queue and a crash would lose the only copy.
The stream's duplicate window drops most retried publishes before they arrive (§15.4).

**Stale entries.**
The catalog keeps each file's **watermark**, the offset its conversion has settled up to, per (source, `equipment_id`, file), written in the registration transaction (§15.7).
An entry at or below it, or below an entry the converter has already released, is **stale**: counted (`outcome="stale"`), acknowledged at once, never converted.
A stale entry is a late first arrival or a redelivery of an entry already registered, which a scalar watermark cannot tell apart;
the second happens only between a registration's commit and its acknowledgements, and the stream drops retried publishes,
so a stale count that grows means the pipeline's in-flight time exceeds the window (`TracekeepLogEntriesLate`, T-62).
An entry of a stopped file, or of a stopped capture's key, is stale too, and never reopens it.
Offsets only need to increase within a file.

**Taking a lease.**
A converter that takes a group's lease reads the group's open log captures, their files' watermarks and their `highest_seq`,
then **deletes and re-creates the group's consumer** before it fetches:
the stream keeps every message no one acknowledged, so the new consumer delivers them from the oldest, the previous holder's unacknowledged deliveries first,
and the previous holder's fetches end with its consumer (T-62).
No newer entry can therefore move a watermark past an older one still owed a redelivery, and no timed wait is needed.
The previous holder's late acknowledgements still reach the new consumer, which keeps its name and applies them by stream sequence, and they are harmless:
it acknowledges only entries registered under its fence, which are stale here;
its in-progress acknowledgements only extend a delivery the new holder holds;
and its negative acknowledgements only repeat one, which the new holder joins as a duplicate.
An acknowledgement the deletion lost belongs to an entry already registered, whose redelivery is stale.
A converter that gives up a flush (§15.7 *The bound*) or meets an outcome it cannot resolve gives up the lease too:
it stops, negatively acknowledges everything it holds, drops its state and takes the lease again by this paragraph, never continuing on state a lost flush left behind.

**Beyond retention.**
An entry whose UTC hour is beyond the retention horizon is counted and acknowledged as beyond retention, never admitted.

**Devices** (T-60).
One global rule table (`logfeed.devices`) maps a header's device-number token where the host program logs one, else its logged session ID, to a device name,
with per-equipment overrides (`logfeed.device_overrides`) for exceptions.
An entry no rule matches belongs to the main device (`logfeed.main_device`, `eqp`);
so do a body without a header and an unclassified entry, as annotations.
A reply takes its primary's device through pairing.
Where the host program logs no device number, the main machine and a peer on stream 99 share a session and are not separated.
A peer link that is no unit, such as the host program's leg to an FDC system, belongs to the unit it serves, the main device,
and shares its capture where the log does not separate them (T-59, T-62).
There is no host capture and no unmapped-device alert.

**Pairing**, in memory, per file.
A header waits as the pending header of its SxFy for at most the pairing window (`logfeed.pairing_window`, 3 entries of its file);
a body takes the pending header of its SxFy, and of its TID when both carry one.
A second header of an SxFy before its body ends the first as a header without a body.
Entries after a pending header wait with it, so the entries a flush settles are always the received entries of a file up to its new watermark.
No pairing state is persisted: an entry is acknowledged only after the pack holding it is registered,
so a restart redelivers every unacknowledged header and body, and pairing rebuilds from the redelivery.

**Trust.**
A header whose logged header names the same stream and function as its body gives the W bit, the session ID and the system bytes;
some host program versions log a reply under its primary's header, and then those fields stay invalid (`field_validity` clear).
So do they for an entry with no logged HSMS header, such as the FDC leg's: a log capture's records claim no HSMS header they did not log.
A W in a body's SML is source text, so a W on a reply fails the parse.
`sml.Parse` runs non-strict, reading an ASCII value by its declared length, so an unescaped quote inside a value parses.

**Time** (T-60).
tracekeep reads `ts` as a wall time in `tz` and stores its UTC instant as `ts_utc_ns`, as traffic does;
a wall time that occurs twice or never takes the earlier reading and is `ordering-uncertain` ([STO §7] item 5).
There is no time-zone setting in tracekeep, and the pipeline converts no time.
A record's hour, and so its scope, is the UTC hour of its logged time ([STO §4] *Scope and order*).

**Seq** is assigned at conversion, dense per capture in conversion order, continuing from the capture's `highest_seq` after a restart.

**What becomes what**, each at its entry's time:

| Entry | Becomes |
|---|---|
| header and body that parse and build | one `data` record, `reconstructed`, `reconstructed-ok`, its direction from the header |
| body whose SML does not parse, or not as exactly one message | `unparsed-entry`, `parse-failed`, holding its SML text |
| encrypted body | `unparsed-entry` holding the ciphertext (§15.9) |
| header with no body within the window | `unparsed-entry`, `parse-failed`, holding the header's fields as received |
| body with no header | `unparsed-entry`, never a record: the host program built it and never sent it |
| unclassified entry | `unrecognised-line`, holding its raw lines |
| entry whose kind's fields tracekeep cannot read | `unrecognised-line`, holding the message as received (§15.3) |

Each record is classified with the capture's classifier and masked with its processing configuration before it joins a batch, as `ingest.md` §6.2 does;
records are written with `SegmentWriter.Append` (fidelity `reconstructed`, `decode_status` and `field_validity` set), since `AppendFrame` refuses `capture_method=log`.

### 15.6 Captures

- **The key**: one capture per (source, `equipment_id`, device, file) (T-60), held by a `logfeed_captures` row from its creation with the capture's `capture_id`, a UUIDv7,
  never derived again, so a file needs no inode or digest: file names are dated.
  The row also names the lease group that last registered the capture;
  a converter of another group takes the capture over at its next registration, after a change of G.
- **Never two devices in one capture** ([STO §7] items 7–8, tracepack v0.15.1).
- **The series** (T-59): `recorder_instance_id` is the UUIDv5 of (`equipment_id`, device, source) under the SDK's namespace and `recorder_instance_name` the source,
  so a log capture's series is never a gateway's.
  A capture of a series' next file names its predecessor in `previous_capture_id` ([FMT I-7]).
- **Admission**: the converter admits a capture with `AdmitLog` (§15.7) on the first entry of its key,
  only while an active processing configuration holds the masking baseline; until then nothing is admitted or acknowledged and the entries wait in the stream within its `max_bytes` (T-60).
- **The descriptor**, the capture-level metadata every segment of the capture carries: `capture_method` `log`, vantage `host`, `time_source` `source-log`, `equipment_id`, `device`, the series' recorder fields, `source_tz`, and `notes` saying it claims no coverage;
  no HSMS session fields (`device_id`, endpoint, timers), which a log does not give, and no `capture_origin_*` or `clock_step_tolerance_ns` ([FMT §5]).
  The capture's row holds no descriptor bytes, since a bus descriptor serves bus captures only and tracepack refuses one of `capture_method` `log`;
  its `origin_ns` is its first record's time.
- `source_ref`: `<source>/<file>`, since dated base names repeat across sources and a pack read outside tracekeep must say whose file it was;
  `source_dialect`: `host-sml-feed/1`, the suffix being the schema version.
- **Boundaries**: the converter writes the start with the capture's first record,
  so the capture's span starts at its first record ([STO §7]);
  it **stops a file**, every capture of it at once, once the first entry of the source's next dated file for the equipment has left the reorder window and no entry of the old file remains in it:
  the old file's pending headers then end as headers without a body, and the first following flush registers their output and the stop together,
  so every entry of the old file the in-flight bound allows is settled before it.
  It stops a file, too, when the equipment has sent no entry of it for `logfeed.idle_period` (24 h).
  A stopped file's watermark row says so, and any later entry of the file, a new device's included, is stale;
  the row is never deleted, as capture rows are not, so a stopped file never reopens, whatever retention does.
  An operator's `capture stop` stops one capture (§15.11).
  On taking a lease, a converter stops the files of its group's open log captures idle past the period;
  group 0's converter also covers the captures a retired group last registered (§15.4).
  An **idle stop** names the watermark it read; its registration re-reads the file's row and aborts if the watermark moved or the file stopped since,
  since another converter may have registered the file meanwhile, and the segment it put is then an orphan (§15.8).
- **No close by the compactor**: `compactor.md` §8.2 never applies to a log capture; the converter writes its stop (T-60).

### 15.7 Flush, registration and acknowledgement

- **Batches** per (capture, UTC hour of the record), as `ingest.md` §6.2 states;
  but a flush takes **every batch of its equipment**, whatever triggered it (F, `logfeed.flush_interval`, ingest's F, T-25; `ingest.batch_max_bytes`; the metadata budget; an hour change),
  so the watermark it writes covers everything converted before it, across devices and hours;
  an equipment has at most one flush registering at a time;
  generation-0 segments under [STO §3] keys with `flush_interval_ns`, the writer's `PackFacts.DurableRecorder` set ([FMT §5], G5-302).
- **One registration transaction per equipment's flush** (`RegisterLogFlush`, `catalog.md` §7.2), never one per segment:
  the captures of one file share its watermark row, so a per-segment update could move the watermark past an entry not yet registered.
  It runs the prologue of `ingest.md` §6.3 on each capture the flush touches, **in ascending `capture_id` order**, so two flushes never wait on each other's rows in opposite orders;
  then checks the group's fence (`leases … WHERE name = 'logfeed/<n>' AND holder = ? AND fence_token = ?`, affected rows checked);
  then each touched file's watermark row, in ascending key order, refusing a file already stopped, and for an idle stop a watermark other than the one it read;
  then registers each segment as `RegisterPack` does without delivery evidence, its guards and its scope rows included;
  then raises each touched file's watermark to the last entry the flush settles, never lowering it, and moves each capture key's group;
  and allocates the event seq last.
  A failed fence or guard rolls the whole flush back.
  A flush is idempotent as a whole: its first segment's `pack_id` already registered, checked before the file rows, means the flush committed, and it changes nothing.
- **The bound** of `ingest.md` §6.2 applies: a flush not registered within `ingest.ack_wait` is given up, its messages negatively acknowledged and their in-progress acknowledgements stopped;
  a remote call's error is an unknown outcome, and an object it may have put is left to the reconciler (§15.8).
- **The acknowledgement rule** is ingest's: an entry is acknowledged once every record or annotation it went into is in a registered pack, its duplicates with it;
  a stale, beyond-retention or unreadable entry at once.
- **Held entries keep no payload**: once decoded, a delivery keeps only its acknowledgement handle, its reply subject, until its pack registers (§15.10).
- **The group's bound** (T-65): a group whose batches hold more payload bytes than `logfeed.group_max_bytes`, or which holds more deliveries than half its consumer's `max_ack_pending`,
  flushes its equipment with the most batched bytes first, again until it is back under both.
  Without it a converter held everything its group received for F, bounded only by `max_ack_pending`, which counts messages, never bytes:
  an equipment logging fast, or a burst of large messages, filled its group's consumer and stalled every equipment of the group until F.
  The bound keeps a group's batches within `logfeed.group_max_bytes`, whatever the messages' sizes; its reorder windows hold its rate times the reorder window beside them.
- **Delivery and validation** follow T-60: one phase of five steps, synthetic test entries, and a gate of 2,000 simulated pieces of equipment beside traffic at K = 100 on both catalog engines.
- **Crash points** are ingest's (`ingest.md` §6.3), except that an orphan is deleted, never adopted (§15.8).
- `internal/logfeed` shares with `internal/ingest` only what holds no delivery: batching, masking and the flush's writer and bound, never the disposition classifier or delivery evidence (T-60).

### 15.8 Catalog, reconciler, compactor and reads

- **Tables** (`catalog.md` §7.1): `logfeed_captures`, the capture key with its group, and `logfeed_files`, the watermark per (source, `equipment_id`, file);
  and on `captures`, `capture_method`, `vantage` and `source_dialect`, for every capture.
  Neither is ever deleted, as capture rows are not (T-60): about one row a file and device a day, some 2 million a year at 2,000 pieces of equipment.
- **The reconciler** deletes a log capture's orphan segment and never adopts it (T-60):
  an adoption would raise `highest_seq` without the file offsets, and the redelivered entries would be converted again under new seqs.
  The orphan's entries were never acknowledged, so the redelivery converts them.
- **The compactor** merges a log capture's scopes as any scope's and never closes one (§15.6);
  `capture close` and `capture terminate` refuse a log capture, and a retirement's quiescence check asks only for its stop (`rollouts.md` §6.4, T-60).
- **Completeness** (`catalog.md` §7.3): a log capture's seq is dense by construction, so it shows no gap, which proves nothing about the log;
  its span starts at its first record.
- **Masking** (`masking-keys.md` §9) applies as to every capture: the bound policy masks the reconstructed frames' items,
  and only an annotation rule in domain `source-text` ([SEM §8]) zeroes the text of an `unparsed-entry` or `unrecognised-line`.
- **Reads**: T-59's selector reads log captures like any other, its `sources` telling them apart; no method is left out by default (T-60).
  Every result naming a capture names its `capture_method`, `vantage` and `source_dialect` (`api.md` §10.1).
  The web UI shows a capture's source and method, filters by method, and marks a log capture as reconstructed, claiming no coverage.
- Retention and deletion are otherwise unchanged.

### 15.9 Encrypted bodies

- An encrypted body becomes an `unparsed-entry` annotation holding the ciphertext;
  its header line is plaintext, so the direction, SxFy, system bytes and time are known from the header's annotation or fields.
- A policy that zeroes source text zeroes the ciphertext ([SEM §8]); no new secret.
- The host program's encryption is deterministic, so its first 32 ciphertext bytes repeat across messages.
  tracekeep holds no host key, derives nothing from the ciphertext and encrypts nothing under the host program's scheme,
  so it exposes nothing the log file does not, and a policy that zeroes source text removes even that.

### 15.10 Sizing and performance

**The load**: 2,000 pieces of equipment.
A 69-minute sample of one equipment's log gives about 3.3 MB/h and 0.8 messages/s, header and body being separate entries, so about 570 B an entry;
across the fleet, 1,600 messages/s, **3,200 entries/s**, about 6.6 GB/h of log text.

**Measured** (T-62): entries generated from go-secs messages, sized to the sample, encoded as §15.3 states (a probe kept outside the repository):

| Quantity | Per entry or body | Across the fleet |
|---|---|---|
| JSON over the raw log text | × 1.31, 707 B for the probe's 539 B: a header about 250 B, a body about 1,160 B for 890 B of SML | about 750 B for the sample's 570 B, and 880 B a message in the store's accounting, whose record, subject and message-id header add about 130 B: **2.8 MB/s, 10.1 GB/h** |
| JSON decoding | 3 µs an entry | 10 ms/s |
| `sml.Parse` and the frame's build | 4.6 µs a body | 7 ms/s |
| the built frame against its SML | 282 B against 893 B | a pack holds about a quarter of the feed's bytes |
| S2, by store block | × 4 | not used (T-64): an acknowledgement into a compressed block decompresses it whole |
| compression of one message | × 1.7 (S2) to × 2.4 (zstd) | too little to justify a codec in the pipeline |

tracekeep's CPU for the feed is about 2% of one core.
The limits are the catalog writer, the bus's message count and the held entries.

**The bus.**
3,200 publishes a second, each acknowledged by the stream, 3,200 acknowledgements and about 3,700 in-progress acknowledgements a second,
beside the traffic stream's own.
Measured at ingest's own times on a three-node cluster in one process beside the traffic at K = 100 (the host log feed plan's step 1, `git show 3c11e5e:docs/plans/host-log-feed.md`, T-64):
the feed sustained with a publish latency of 4.3 ms at the 99th percentile, the traffic sustained, the process at about two cores and 43 MiB/s of disk writes,
the busiest group holding about 21,000 entries and each server's store about 700 MiB.
Header and body are separate messages, and the forwarder's sink cannot batch, so the count is the entries'.
At the owner's fleet (the host log feed plan's step 10b, `git show 3c11e5e:docs/plans/host-log-feed.md`, T-66), about 24,750 entries and 95 MB/s a second, one stream of one replica took the fleet with every publish stored;
a burst of 117 MB/s more waited: the stream is the work of one server, which takes its publishes and acknowledgements in turn,
so the publishes were acknowledged in about 0.9 s at the 99th percentile, the harness's publishers made 65% to 89% of the burst's rate,
and the stage digested what they published within 2m19s to 4m03s of the burst's end.
A peak is the forwarders' buffers' to hold (`docs/guides/log-pipeline.md` *Peaks*), as they held a minute of the fleet while a server was lost (*A server lost* below).
**Why the stream waits**, profiled on 2026-11-08 with the servers' Go profiles (`scripts/load-feed.sh`), every stream of one replica and all of them on one server:
a work-queue stream whose subjects hold a wildcard removes an acknowledged message only after loading it, to match its subject against the other consumers' filters,
under the stream's exclusive lock, which storing a publish takes too (nats-server v2.15.1, `server/stream.go`: `ackMsg` → `noInterest` → `checkForInterest`).
At the fleet's steady rate the stream's inbound loop waits on that lock about a third of the time; outside a dip, the burst's publishes were stored at about 70% of their rate.
Once every F, about a million entries held since the previous F are acknowledged within a minute,
and loading their messages reloads the store's blocks no longer cached, under the lock, the disk reading up to about 660 MB/s;
the inbound loop is then blocked about 90% of the time, and the stream stores 5,000 to 8,000 publishes a second instead of 24,750 for about a minute, catching up after it.
Inferred, not measured: these are the light equipment's entries, which the group's bound rarely flushes early, their flushes due F after their previous ones (§15.7) from a common start, so falling within the same minute.
The server used about 1.5 of its 16 cores, a quarter of them the garbage collector's, so its cores in all were not the ceiling;
the traffic stream shared the server and the disk, and these runs do not tell its part apart, neither in the reads nor in the work serialized on one core.
A stream a group splits the lock, not the loads: measured on the same host on 2026-11-08 (`docs/ROADMAP.md` *Open items*, T-67),
a stream a group of one replica carried the fleet without the dip at every flush, its lowest ten seconds 22,305 entries a second and its publish p99 134 ms, against 5,623 and 712 ms on one stream;
three replicas carried about a third of the fleet on one stream and 85 % on a stream a group, the latter at about three times the CPU, network and writes.
Spreading the equipment's flushes over F would spread the reloads, and whether it would lessen them is unmeasured.
**A server lost**, measured on one host on 2026-11-10 (`git show 41a448c:docs/plans/feed-stream-per-group.md`, T-68):
three servers, the 32 group streams of one replica placed round-robin by tags, and Vector 0.61 publishing the fleet's rate to all three,
its retries unbounded and backing off at most 3 s, its buffer a 20 GiB disk buffer blocking when full, its concurrency 256 or adaptive;
one server killed and restarted with its storage 60 s later, in five runs.
The whole feed stalled for the outage: the groups on the other servers took nothing in the two runs that killed a server Vector was not connected to,
and about 500 entries in the minute, against a million before it, in the three that killed the one it was connected to,
since a publish to a lost group is retried keeping its concurrency slot (§15.2).
Vector's buffer grew to 1.56 million entries, about 5.8 GiB, for the minute;
the lost groups took entries 1 s after the server was healthy, and the buffer was below 1,000 entries 11 to 15 s after the restart.
No entry was discarded or stored twice, none was missing but ten a disk buffer held (`docs/guides/log-pipeline.md` *A server lost*),
and entries of one file were stored at most 2.9 s out of order.
**`max_bytes`** = the outage tolerated × 10.1 GB/h, logical bytes: `logfeed.max_bytes` defaults to 40 GiB, about 4 h.
Each group's stream holds `max_bytes` / G × 1.25 (§15.4): at the defaults 1.5625 GiB, 50 GiB in all, about 17 GiB on a server holding 11 streams of one replica.
A group fuller than 1.25 times the mean fills first: the plan's probe, its fleet uniform, put 60 to 66 equipment in a group against a mean of 62.5,
and a group holds at least its heaviest equipment's share, so one equipment above 1.25/G of the fleet's bytes fills its stream alone before the feed holds `max_bytes`.
When a group's stream is full, the forwarder's publish to it fails and is retried as a lost server's is, so the whole feed waits, inferred and not measured on the group streams, and its buffer fills;
with its buffer set to block, backpressure reaches the tailer, which stops tailing,
and the files on disk resume from its checkpoint, provided the host program keeps them longer than the outage.
Dropping instead keeps the log store flowing and loses traffic entries, which show only as missing records.
The deployment chooses; the guide recommends blocking.

**The catalog writer** (SQLite, single writer), from phase 5's baselines:
a registration costs about 1.7 ms fixed, its synchronization included, plus about 30 µs an entry, an upper bound since the feed writes no per-entry rows;
a merged segment's deletion about 0.6 ms (`RegisterPack/miss`, unmeasured for deletion).

| F | flushes/s | entries a flush | writer: registration | writer: deletion | segments/h | merge inputs a scope | held entries | batched bytes |
|---|---|---|---|---|---|---|---|---|
| 10 s | 200 | 16 | ≈ 440 ms/s | ≈ 120 ms/s | 720,000 | 360 | 128,000 | ≈ 5 MB |
| 60 s | 33 | 96 | ≈ 150 ms/s | ≈ 20 ms/s | 120,000 | 60 | 288,000 | ≈ 29 MB |
| 300 s | 6.7 | 480 | ≈ 110 ms/s | ≈ 4 ms/s | 24,000 | 12 | 1,056,000 | ≈ 150 MB |

- **The limit**: on SQLite, F ≥ 60 s for 2,000 pieces of equipment, about 17% of the writer beside the traffic ingest's own registrations;
  F = 10 s needs MariaDB.
  F defaults to ingest's 300 s (T-25).
  The per-entry part, about 96 ms/s, does not shrink with F.
- **Held entries** = 3,200/s × (F + the reorder window), split across the groups' consumers: about 33,000 a consumer at F = 300 s, under `logfeed.max_ack_pending`;
  a faster fleet flushes before F by the group's bound (§15.7, T-65), and `max_ack_pending` stays above twice its group's rate times the reorder window.
- **Leases**: G group rows renewed every 10 s, 3.2 commits a second at G = 32, whatever the fleet.
- **The overlap probe** scans one capture's ranges, a capture being one dated file: at most 86,400 / F ranges.
- **Objects**: the segments above, plus 2,000 archives an hour; the compactor merges 2,000 scopes an hour.
  The merge's baselines reach 64 inputs, so F = 10 s lies outside them.
- **Memory**, across the replicas:
  the reorder window about 35 KB an equipment, 70 MB;
  pairing about 2 KB an equipment, 4 MB;
  the batches as above, built frames being smaller than their SML;
  and the held entries' acknowledgement handles, about 300 B each, 320 MB at F = 300 s, which is why a converted entry keeps no payload (§15.7): with it, about 800 MB more.

### 15.11 Operations

**Configuration** (`docs/guides/configuration.md`):

| Key | Default | Meaning |
|---|---|---|
| `logfeed.enabled` | false | runs the log stage in the `ingest` role |
| `logfeed.stream` | `TRACEKEEP_LOGFEED` | the feed streams' name: group n's is `<stream>_<n>` |
| `logfeed.prefix` | `tklog` | its subject prefix |
| `logfeed.lease_groups` | 32 | G, the lease groups and their streams, the servers' mapping partitioning into as many; changed only by `migrate` with the mapping (§15.4) |
| `logfeed.max_bytes` | 40 GiB | the feed's total; each group's stream holds `max_bytes` / G × 1.25, discard-new |
| `logfeed.replicas` | 1 | each group stream's replicas (§15.4) |
| `logfeed.placement_tags` | none | the servers' tags, group n's stream placed by tag n mod their number (§15.4) |
| `logfeed.duplicate_window` | 2 min | the stream's duplicate window, longer than a retry's delay while the servers answer |
| `logfeed.max_ack_pending` | 100,000 | each group consumer's |
| `logfeed.group_max_bytes` | 128 MiB | a group's batched payload before its largest equipment flushes (§15.7 *The group's bound*) |
| `logfeed.reorder_window` | 30 s | §15.5 *Order*; the pipeline's in-flight bound stays below it |
| `logfeed.pairing_window` | 3 entries | §15.5 *Pairing* |
| `logfeed.flush_interval` | ingest's F | F |
| `logfeed.idle_period` | 24 h | §15.6 *Boundaries* |
| `logfeed.main_device` | `eqp` | the device of an entry no rule matches |
| `logfeed.devices` | none | the rule table: a device-number token or a session ID → a device name |
| `logfeed.device_overrides` | none | per `equipment_id`, rules that replace the table's |

The feed's consumers take `ingest.ack_wait` and its heartbeat.
Preflight checks that the servers' mapping partitions the feed's subjects into G, every group's stream reached (§15.4):
it publishes 32(G+1) probes under tokens `_tracekeep_probe_<i>`, every one whatever the first reach, so a count even one above G shows, each expecting a last sequence no stream reaches, so the group stream that takes one refuses to store it,
and a core subscription of its own reads the group the mapping chose;
`tracekeep status`, the metrics and readiness report each group's stream.

**Admin**:
`capture stop <id>` on a log capture records a stop request on its `captures` row, which the converter holding the group's lease reads at its next flush and writes as the stop;
an entry of its key after it is stale.
With no converter holding the lease, the command writes the stop to the row itself, having locked the group's lease row after the capture row, so a converter taking the lease meanwhile waits for it (T-60, `catalog.md` §7.2 `StopLogCapture`).
On a gateway's capture it is refused.
`tracekeep status` shows the feed's backlog, each group stream's and their sum, a stream that cannot be read marked so, and the number of equipment silent for 15 minutes.

**Alerts**:

| Alert | Fires when | Runbook section |
|---|---|---|
| `TracekeepLogFeedSilent` | an equipment that sent entries in the past day sent none for 15 min, or the feed's backlog grows for 15 min | `log-feed-silent` |
| `TracekeepLogEntryUnreadable` | an entry tracekeep cannot read within the hour | `log-entry-unreadable` |
| `TracekeepLogEntriesLate` | stale entries growing within the hour: the pipeline's in-flight time exceeds the reorder window (§15.5 *Stale entries*) | `log-entries-late` |

**Metrics** (`equipment_id` a label):
`tracekeep_logfeed_entries_total{outcome=record|annotation|duplicate|stale|beyond-retention|unreadable}`,
`tracekeep_logfeed_unpaired_total{side=header|body}`, `tracekeep_logfeed_annotations_total{kind}`,
`tracekeep_logfeed_last_entry_timestamp_seconds` (an entry's arrival),
`tracekeep_logfeed_last_outcome_timestamp_seconds{outcome}` (an outcome's last count, which the unreadable and late alerts read, as no scrape misses it),
and each group stream's backlog (`tracekeep_logfeed_stream_backlog_messages{stream}`, `_bytes{stream}`) beside the traffic stream's, the alert reading their sum.

### 15.12 What it does not do

- No byte-exact coverage, holes or range markers; no offline import of log files.
- No use of the host program's log to fill, confirm or correct a gateway capture.
- No transport events, epochs or thinning in a log capture.
- No parking, no incarnation, no termination of feed messages.
- No `leg` field, which nothing in the logs fills today (T-60).
- No decrypting of encrypted bodies.
  Possible later work, as its own proposal: a per-source `source-decryption` key kind resolved through `internal/keys` providers, never a masking key,
  the host program's maintainers specifying the scheme, the plaintext masked before any write.
- No change to the host program's other log lines: everything else goes to the log store unchanged.
