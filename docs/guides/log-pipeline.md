# Log pipeline guide

Status: current (2026-11-07) — the entry a host program's log pipeline publishes, its subject, message id, retries and in-flight bound

How to build the pipeline that tails a host program's SECS log and publishes it to tracekeep's host log feed.
Read before deploying or changing such a pipeline, or when `log-entry-unreadable` or `log-entries-late` fires.
The design is `docs/specs/tracekeep/logfeed.md` §15; tracekeep's side is configured by the `logfeed.*` keys in `configuration.md`.
The feed is the legacy route, best effort: a gateway recording the HSMS link is preferred wherever one can run (T-60).

Every pattern below is a placeholder.
A host program's log layout is its own: write the patterns against its files, and keep its names, paths and real lines out of any shared repository.

## What the pipeline does

1. Tails each dated log file of the host program, reporting each entry's byte offset in its file.
2. Groups an entry's lines, and classifies it as a header, a body, an encrypted body, or unclassified.
3. Extracts the entry's fields, rewrites its timestamp's layout, and builds one JSON object.
4. Publishes it to the traffic cluster's JetStream, one entry a message, awaiting each publish's acknowledgement.

tracekeep does the rest: order, duplicates, pairing headers with bodies, devices, time zones and records.
The pipeline never pairs, reorders or converts.

## The entry

One JSON object a message, schema version 1 (`logfeed.md` §15.3):

```json
{"v": 1, "equipment_id": "<EQUIPMENT>", "source": "<PIPELINE-NAME>", "tz": "<Area/City>",
 "file": "<BASE-NAME-WITH-DATE>.log", "offset": 123456, "ts": "2026-11-07 08:00:00.123",
 "kind": "header", "dir": "received", "header": "<20 HEX DIGITS>", "sxfy": "S6F11", "tid": "<TID>"}
```

| Field | On | From the log |
|---|---|---|
| `v` | every entry | `1` |
| `equipment_id` | every entry | the equipment the host program drives, as tracekeep names it |
| `source` | every entry | the pipeline deployment's name, stable across restarts, never a host or pod name; at most 128 characters |
| `tz` | every entry | the IANA zone the host program logs in |
| `file` | every entry | the file's base name, at most 128 characters |
| `offset` | every entry | the tailer's byte offset of the entry's first line |
| `ts` | every entry | the logged local time as `YYYY-MM-DD HH:MM:SS[.fraction]`, no zone: change the layout, never the value |
| `kind` | every entry | `header`, `body`, `encrypted` or `unclassified` |
| `dir` | header | `sent` or `received` |
| `device` | header | the host program's device-number token, when its line carries one |
| `header` | header | the 10 HSMS header bytes as 20 hex digits, when logged |
| `tid` | header, body | the host program's transaction id, when logged |
| `sxfy` | header, body, encrypted | the stream and function the line names, `S<n>F<n>` |
| `text` | body, encrypted, unclassified | the SML text; the ciphertext as logged; the raw lines |

An entry without a parsable `v`, `equipment_id`, `source`, `file`, `offset`, `ts` or `tz` is counted `unreadable` and dropped (`log-entry-unreadable`);
one whose kind's own fields are wrong is kept as an `unrecognised-line` annotation.
Send the extracted fields and one text, never the raw lines beside the SML text: they would add about 500 B to every entry.

A placeholder of the grouping and classification, written against your host program's layout:

```text
entry start   ^<TIMESTAMP-PATTERN> <LEVEL-PATTERN>
header        <DIRECTION-WORD> S(?P<s>\d+)F(?P<f>\d+) .* <TID-PATTERN> .* <HEADER-HEX-PATTERN>
body          ^S(?P<s>\d+)F(?P<f>\d+)( W)?$  … up to the line holding only "."
encrypted     <ENCRYPTED-MARKER-PATTERN>
anything else unclassified
```

## The message

- **Subject**: `<prefix>.<token>`, the prefix `logfeed.prefix` (`tklog` by default) and the token the equipment ID with every byte outside `A-Z a-z 0-9 - _ ~` written `%XX`, uppercase hex, `.` included.
  An ID of only those bytes is its own token.
- **Message id** (`Nats-Msg-Id` header): `<source>/<token>/<file>/<offset>`, so a publish retried after a lost acknowledgement is stored once.
- **Acknowledgement**: publish to JetStream and await each acknowledgement, retrying until it comes; a full stream refuses a publish until the feed drains.

## Retries and the in-flight bound

tracekeep holds each file's entries for `logfeed.reorder_window` (30 s) to put them back in offset order;
an entry arriving after a later one of its file has been registered is stale, counted and dropped (`log-entries-late`).
So, for every entry:

- the time from sending it to its stored publish, retries included, stays below `logfeed.reorder_window`:
  a wait in a first-in, first-out buffer before it is sent delays the entries tailed after it alike, so it does not count (*Peaks*);
- the retry duration stays below `logfeed.duplicate_window` (2 min), or a late retry is stored twice;
- publishes run concurrently enough to reach the fleet's rate: one publish in flight reaches about 1,200 entries a second, far below a large fleet's;
  concurrent publishes may reorder an equipment's entries, which tracekeep puts back in order as long as each stays within the reorder window
  of the next entry tailed for the equipment, across a file's rotation too (`logfeed.md` §15.2).

With Vector, for example, a `nats` sink with `jetstream.enabled` awaits each acknowledgement and retries every error,
and `jetstream.headers.message_id` sets the message id from a field the transform built;
its `request.concurrency` defaults to one request in flight for the whole sink, so raise it to the fleet's rate,
and bound its retries below the windows above, its buffer first-in, first-out.

## Peaks

The feed is one stream, whose publishes one server takes in turn (`logfeed.md` §15.10, T-66):
a fleet's steady rate it takes, but a peak above it, many pieces of equipment logging large messages at once, is acknowledged more slowly,
and every forwarder of the fleet waits on its acknowledgements meanwhile.
So the forwarder's buffer holds a peak's worth of entries, its rate above the steady one times its length, and sends them once the stream catches up.
An entry is stale only when it arrives behind an entry of its file the converter has already released, one that arrived a reorder window or more before it (`logfeed.md` §15.5):
a first-in, first-out buffer delays an entry and the entries after it alike, however long it waits, so only the time from sending to the stored publish, retries included, stays within the window.
A buffer set to block stops the tailer when it fills, and the files on disk resume from its checkpoint; one set to drop loses entries, which show only as missing records.
Holding a whole peak this way is the design's, not yet measured: T-66's runs delayed publishes by about a second, never by the reorder window.

## Checking a pipeline

1. Publish a few entries of a test equipment to a non-production tracekeep with `logfeed.enabled`.
2. `tracekeep_logfeed_entries_total{equipment_id,outcome}` counts each entry's outcome: `record` and `annotation` are converted, any other is the pipeline's to fix.
3. `tracekeep_logfeed_unpaired_total{side}` rising means headers and bodies are not grouped or classified as the host program writes them.
4. The browser lists the equipment's log series, its method `log · reconstructed`.
