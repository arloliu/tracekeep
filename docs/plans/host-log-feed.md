# Host log feed

Status: active (2026-11-07) — step 3, protections

The work that delivers the host log feed, `docs/specs/tracekeep/logfeed.md` (§15), as T-60 shapes it,
with its two riskiest costs measured before the converter is built (T-63): the feed's load on the bus, and its registration's on the catalog writer.
The design is the spec's and the rationale the decision log's; this plan only orders the work and records its measurements.

## Steps

| Step | Work | State |
|---|---|---|
| 1 — the feed stream's load | the feed's stream and group consumers under the fleet's rate on a three-node cluster, beside traffic, before any converter exists | done (#102) |
| 2 — the catalog | `logfeed_captures`, `logfeed_files` and the `captures` columns; `AdmitLog`, `RegisterLogFlush`, `StopLogCapture`, on both engines; their benchmarks and first baselines, judged against §15.10's writer budget | done (#103) |
| 3 — protections | the reconciler deletes a log capture's orphan; the compactor's close leaves log captures out; `capture close` and `capture terminate` refuse one; retirement's stop-only quiescence (§15.8) | in-progress |
| 4 — stream, configuration and preflight | `migrate` ensures the stream and consumers, a change of G included; the `logfeed.*` configuration; the prefix and overlap checks (§15.4, §15.11) | pending |
| 5 — conversion | `internal/logfeed`'s pure part: entry decoding, reorder, duplicates and stale entries, pairing, devices, time, records and annotations (§15.3, §15.5) | pending |
| 6 — the converter | batches, flush and registration, lease groups and the consumer's re-creation, file stops, the service's wiring; `internal/logfeed` in a `scripts/catalog.sh` leg (§15.6, §15.7) | pending |
| 7 — reads and admin | the reads' method fields, `capture stop`, `tracekeep status`'s feed lines (§15.8, §15.11) | pending |
| 8 — web UI and operations | the UI's method filter and badge; alerts, metrics, runbook, configuration and pipeline guides (§15.8, §15.11) | pending |
| 9 — the gate | the gate cases below | pending |

Each step is one pull request.
Steps 1 and 2 record their numbers here before step 5 starts;
a number past its budget stops the plan for the owner's decision.

## Step 3 — protections

Each refuses or leaves out a capture whose `capture_method` is `log`, tested on both engines, before any converter can write one:

- **The reconciler** (`storage.md` §5.4): a log capture's orphan is tombstoned and its object deleted after the commit, never adopted.
- **The compactor's close** (`compactor.md` §8.2): `CloseCandidates` and `CloseIntent` leave log captures out.
- **`capture close` and `capture terminate`** (`operations.md` §10.4): refused for a log capture, the request or the termination writing nothing.
- **Retirement's quiescence** (`rollouts.md` §6.4): a log capture is quiescent once its row is `stopped`, whichever stop wrote it, and neither stream is observed for it.

## Gate cases

Acceptance of the whole phase, each to be detailed with its barriers when its step is next (`.agents/rules/300-testing.md`),
every catalog-touching schedule on both engines;
"converted once" means the synthetic input's expected records and annotations, each in exactly one registered pack, and each entry's disposition as expected.

- **Load** (T-60): 2,000 simulated equipment beside traffic at K = 100, on SQLite and then MariaDB, from the per-operation benchmarks and one short real run:
  the feed sustained, the traffic ingest's gate still met, nothing converted twice or lost uncounted.
- **Flush and registration** (§15.7): a multi-device, multi-hour flush rolled back by a lost fence and by a guard;
  a failed put before registration; a registration whose commit's answer is lost, once committed and once not, each recovered;
  a committed flush retried; packs, watermarks and `highest_seq` agreeing after each.
- **Crashes and takeover** (§15.5 *Taking a lease*, §15.8): a crash after the put and before registration, its orphan deleted by the reconciler;
  a crash after the commit and before the acknowledgements, its redeliveries stale;
  a takeover with older pending entries, newer backlog and the old holder's acknowledgements arriving late.
- **Captures** (§15.6): admission waiting for a masking baseline;
  a rotation with an old-file tail inside the reorder window, and one past it, stale;
  an entry of a stopped file from a device the file never had;
  an idle stop racing another group's registration after a change of G;
  `capture stop` with the group's lease held and with it lapsed.
- **Entries** (§15.3, §15.5): duplicates by redelivery and by retried publish;
  an unreadable entry against one whose kind's fields are malformed;
  an entry beyond retention;
  pairing's failures: a header without a body, a body without a header, a second header of one SxFy, a reply logged under its primary's header;
  a DST fall-back hour and a spring-forward gap, `ordering-uncertain`.
- **Masking** (§15.8, §15.9): an encrypted body's, an `unparsed-entry`'s and an `unrecognised-line`'s text zeroed under a policy that zeroes source text.

## Measurements

**Step 1** (#102), on one machine, the three servers, dev and the harness in one process, the catalog on SQLite.
At T-61's times (F 10 s), the feed as `logfeed.md` §15.4 then specified it passed every budget:
3,200 entries a second acknowledged, publish latency 532 µs at the 99th percentile, the busiest group's pending 707, the traffic sustained.
At ingest's own times (F 300 s, `ack_wait` 7 min, reorder window 30 s), one main case a run:

| Run | Feed sustained | Publish p99 | Publisher queue slope / bound a cycle | Traffic beside it | Process CPU (cores) | Max RSS | Disk writes /s | Busiest group's pending |
|---|---|---|---|---|---|---|---|---|
| S2, file consumers | yes | 617 ms | 4,050 / 4,777 | fell behind, backlog minima +116/s | 4.0 | 15.7 GiB | 145 MiB | 21,081 |
| S2, file consumers, profiled | yes | 624 ms | 4,523 / 4,778 | sustained | 4.1 | 15.9 GiB | 147 MiB | 21,095 |
| no compression, file consumers | yes | 51 ms | 2,987 / 4,785 | fell behind, backlog minima +214/s | 3.5 | 13.5 GiB | 152 MiB | 21,120 |
| no compression, consumers in memory | yes | 4.3 ms | 1,055 / 4,795 | sustained | 2.0 | 14.6 GiB | 43 MiB | 21,167 |

The profiles: an acknowledgement into an S2 block decompressed it whole, 3.4 GB of the heap across the servers;
the file consumers' state encoding took 36% to 46% of the process's CPU, about 8% once the feed's consumers kept theirs in memory, the traffic consumer's remaining.
The owner chose the last row (T-64).
Its takeover redelivered the 18,042 held entries, oldest first, in 188 ms; the old handles' late acknowledgements were applied, 7 of 10, as §15.5 now states.
Controls: a publisher capped at half the rate failed the rate budget; one connection with one publish in flight reached 1,237 a second;
a full 64 MiB stream refused the 69,997th publish with error 10077, evicted nothing, and took it after a drain;
a publish retried after a lost acknowledgement was stored once.
The duplicate window's 384,000 ids cost about 180 MiB of heap across the three servers at T-61's times; at ingest's own, the difference was lost in the process's 2.9 GB heap.

**Step 2** (#103): the transactions' first baselines, `-count 5`, the median of each row, in ms;
SQLite in WAL with `synchronous(FULL)` on an ext4 disk, MariaDB at its defaults with its data on its container's disk.

| Benchmark | SQLite | MariaDB |
|---|---|---|
| `RegisterLogFlush`, 16 entries, one device | 0.47 | 3.7 |
| `RegisterLogFlush`, 96 entries, one device | 0.46 | 3.5 |
| `RegisterLogFlush`, 480 entries, one device | 0.47 | 3.7 |
| `RegisterLogFlush`, 16 entries, three devices | 0.81 | 7.3 |
| `RegisterLogFlush`, 96 entries, three devices | 0.82 | 7.4 |
| `RegisterLogFlush`, 480 entries, three devices | 0.83 | 7.4 |
| `AdmitLog` | 0.38 | 2.9 |
| `StopLogCapture` | 0.26 | 2.1 |

A fresh 480-entry flush takes 0.47 ms on SQLite over one device and 0.83 ms over three, within its 16.4 ms budget:
6.7 flushes a second take about 6 ms a second of SQLite's writer.
A flush's cost is its segments', one a device, not its entries'.
