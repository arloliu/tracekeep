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

- it is stored within `logfeed.reorder_window` of the next entry tailed after it for its equipment (`logfeed.md` §15.2):
  a wait that delays the entries after it alike does not count, in a first-in, first-out buffer before it is sent or in retries through an outage that hold every publish (*Peaks*, *A server lost*);
  what counts is its delay beyond the next entry's, a retry's backoff above all, so cap the backoff well below the window;
- a publish stored but not acknowledged is retried within `logfeed.duplicate_window` (2 min), or the retry is stored twice:
  while the servers answer, a retry comes within the acknowledgement's timeout and the backoff's cap;
  Vector cannot bound a retry's total duration, so through an outage longer than the window it may come later, which is unmeasured;
- publishes run concurrently enough to reach the fleet's rate, the publishes in flight about the rate times the round trip to the bus:
  one in flight reached 24,750 entries a second at a 40 µs round trip and about 670 at 2 to 3 ms, so the fleet needs about 72 at 3 ms;
  concurrent publishes may reorder an equipment's entries, which tracekeep puts back in order as long as each stays within the reorder window
  of the next entry tailed for the equipment, across a file's rotation too (`logfeed.md` §15.2).

With Vector, for example, a `nats` sink with `jetstream.enabled` awaits each acknowledgement and retries every error,
its acknowledgement timing out after async-nats's 5 s whatever `request.timeout_secs` says,
and `jetstream.headers.message_id` sets the message id from a field the transform built.
Measured with Vector 0.61 at the fleet's rate on 2026-11-08 and 2026-11-10 (the measurements kept outside the repository), set:

- `request.concurrency: 256`, fixed, since its default holds one request in flight for the whole sink;
  `adaptive` also reached the fleet's rate, its limit settling between 36 and 87;
- `request.retry_attempts` unset, so unlimited, since an exhausted retry drops its entry;
- `request.retry_max_duration_secs: 3`, which caps each backoff:
  while a full stream refused publishes, the default 30 s stored entries up to 47 s behind later ones of their file, and 3 s kept them within 5 s;
- `buffer.type: disk` with `when_full: block`, first-in, first-out, since a memory buffer loses what it holds when Vector restarts, up to millions of entries;
- `buffer.max_size` for the outage the deployment accepts (*A server lost*).

Watch `vector_buffer_size_events` and `component_discarded_events_total`; `component_errors_total` stays 0 through retries.

**One aggregator a host**: a host's files go through one aggregator, or a delay of one of them reorders a file by that delay.
fluent-bit's `Upstream` sends each flush to the next node, and `out_forward` runs two workers by default;
spread over two aggregators, a 40 s stall of one stored 138,000 entries 30 s or more behind a later entry of their file, which the converter drops as stale.
Point each host's forward output at one aggregator with `workers 1`, or at a balancer that pins a host's connections.

## Peaks

Each lease group's stream is the work of one server, and the streams spread over the servers (`logfeed.md` §15.4, T-67):
a peak above the fleet's steady rate, many pieces of equipment logging large messages at once, is acknowledged more slowly,
and every forwarder of the fleet waits on its acknowledgements meanwhile.
So the forwarder's buffer holds a peak's worth of entries, its rate above the steady one times its length, and sends them once the streams catch up.
An entry is stale only when it arrives behind an entry of its file the converter has already released, one that arrived a reorder window or more before it (`logfeed.md` §15.5):
a first-in, first-out buffer delays an entry and the entries after it alike, however long it waits, so only an entry's delay beyond the next one's stays within the window.
A buffer set to block stops the tailer when it fills, and the files on disk resume from its checkpoint; one set to drop loses entries, which show only as missing records.
Measured with Vector 0.61 on 2026-11-08: a 60 s stall at the fleet's rate filled a memory buffer with 1.46 million entries, 6.5 GB;
they were stored within 20 s of its end, each delayed by at most the stall, none 30 s or more behind a later entry of its file, none lost or stored twice;
a buffer too small for the stall blocked the source, which then caught up at 86,000 entries a second.
On one stream the feed also slowed for about a minute once every flush interval, as held entries were acknowledged together; a stream a group removes that dip (`logfeed.md` §15.10).

## A server lost

With each group's stream of one replica (`logfeed.replicas`, the default), a server's loss stalls the whole feed until the server is back (`logfeed.md` §15.10, T-68):
a publish to a group on the lost server fails and is retried keeping its request's slot,
so those publishes, about a third of the entries with three servers, come to hold every slot, whether the concurrency is fixed or adaptive.
Measured with Vector 0.61 at the fleet's rate, 24,700 entries a second, 95 MB/s, on 2026-11-10:
a minute's outage grew its disk buffer to 1.56 million entries, about 5.8 GiB, and its container's memory, page cache included, to about 6.2 GiB;
the buffer was below 1,000 entries 11 to 15 s after the server's restart, with nothing discarded or stored twice.
So size `buffer.max_size` at about 6 GiB a minute of the outage the deployment accepts, at the fleet's rate, and give Vector's host the memory beside it:
20 GiB holds about 3 minutes.
Past it the buffer blocks and the tailer stops, resuming from its checkpoint as long as the host program keeps its files.

Vector 0.61's disk buffer can leave a burst's tail unread until the next entry arrives (Vector issues #23456 and #23605, their fixes not merged):
in bursts of random size to one server with no fault, it held a tail of 1 to 150 entries in 13 and in 17 of 200 bursts, and a memory buffer in none of 200.
Nothing is lost: under a fleet's traffic the next entry comes within milliseconds, and an aggregator gone idle holds its last entries until one does.

## The servers

The operations team configures the traffic cluster's servers for the feed (`logfeed.md` §15.4, T-68); every value below is a placeholder.

The **mapping**, in the account the pipeline publishes in and tracekeep reads from,
partitions the feed's subjects into G groups, G being `logfeed.lease_groups` (32), with `logfeed.prefix` (`tklog`) on both sides:

```text
accounts {
  <ACCOUNT> {
    jetstream: enabled
    users: [{user: <USER>, password: <PASSWORD>}]
    mappings { "<PREFIX>.*": "<PREFIX>.{{partition(<G>,1)}}.{{wildcard(1)}}" }
  }
}
```

Its count is G exactly: a larger one sends some publishes to subjects no stream holds, which the pipeline retries until the whole feed stalls,
and a smaller one leaves the higher groups' streams empty; tracekeep's preflight refuses either.
Changing G changes the mapping together with a `migrate` run, on a drained feed (`configuration.md`).

The **tags**: with streams of one replica, each server carries a tag of its own, and `logfeed.placement_tags` lists them, so the groups' streams spread round-robin over the servers;
with three replicas the list stays empty:

```text
server_tags: ["<SERVER-TAG>"]
```

## Checking a pipeline

1. Publish a few entries of a test equipment to a non-production tracekeep with `logfeed.enabled`.
2. `tracekeep_logfeed_entries_total{equipment_id,outcome}` counts each entry's outcome: `record` and `annotation` are converted, any other is the pipeline's to fix.
3. `tracekeep_logfeed_unpaired_total{side}` rising means headers and bodies are not grouped or classified as the host program writes them.
4. The browser lists the equipment's log series, its method `log · reconstructed`.
