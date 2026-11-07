# Host log feed

Status: active (2026-11-07) — step 10b, the gate's load, its traffic criterion awaiting the owner

The work that delivers the host log feed, `docs/specs/tracekeep/logfeed.md` (§15), as T-60 shapes it,
with its two riskiest costs measured before the converter is built (T-63): the feed's load on the bus, and its registration's on the catalog writer.
The design is the spec's and the rationale the decision log's; this plan only orders the work and records its measurements.

## Steps

| Step | Work | State |
|---|---|---|
| 1 — the feed stream's load | the feed's stream and group consumers under the fleet's rate on a three-node cluster, beside traffic, before any converter exists | done (#102) |
| 2 — the catalog | `logfeed_captures`, `logfeed_files` and the `captures` columns; `AdmitLog`, `RegisterLogFlush`, `StopLogCapture`, on both engines; their benchmarks and first baselines, judged against §15.10's writer budget | done (#103) |
| 3 — protections | the reconciler deletes a log capture's orphan; the compactor's close leaves log captures out; `capture close` and `capture terminate` refuse one; retirement's stop-only quiescence (§15.8) | done (#104) |
| 4 — stream, configuration and preflight | `migrate` ensures the stream and consumers, a change of G included; the `logfeed.*` configuration; the prefix and overlap checks (§15.4, §15.11) | done (#105) |
| 5 — conversion | `internal/logfeed`'s pure part: entry decoding, reorder, duplicates and stale entries, pairing, devices, time, records and annotations (§15.3, §15.5) | done (#106) |
| 6 — the converter | a lease group's converter: deliveries through windows and pairing into admitted captures, seq, masking, batches, segments, `RegisterLogFlush`, acknowledgements, file stops and stop requests; `internal/logfeed` in a `scripts/catalog.sh` leg (§15.5–§15.7) | done (#107) |
| 7 — lease groups and wiring | taking, renewing and losing group leases, the consumer's re-creation and the fetch loop, giving up and taking again, the heartbeat, the stage started by `ingest` with its readiness and metrics (§15.4, §15.5 *Taking a lease*) | done (#108) |
| 8 — reads and admin | the reads' method fields, `capture stop`, `tracekeep status`'s feed lines (§15.8, §15.11) | done (#109) |
| 9 — web UI and operations | the UI's method filter and badge; alerts, metrics, runbook, configuration and pipeline guides (§15.8, §15.11) | done (#110) |
| 10a — the gate's correctness | every gate case below but the load, through the converter or the stage, on both engines | done (#111) |
| 10b — the gate's load | the load case: the owner's fleet through the stage itself, beside traffic, on SQLite then MariaDB | in-progress |

Each step is one pull request.
Steps 1 and 2 record their numbers here before step 5 starts;
a number past its budget stops the plan for the owner's decision.

## Step 10a — the gate's correctness

Most gate cases were met piecemeal by steps 2 to 9, at the catalog or the pure part; step 10a meets the rest through the converter, or the stage where the bus is the case,
"converted once" read back from the registered packs alone, payloads included, each record matched to its entry (its system bytes or its text), with the file's watermark and each capture's `highest_seq`:

| Gate case | Test |
|---|---|
| a two-device, two-hour flush refused by a lost fence and by a guard; a failed put; a commit's answer lost, committed and not; a committed flush retried | `TestGateFlushRefusedAndRecovered`: nothing acknowledged, a duplicate included, a new converter converting the redeliveries once, those of a committed registration stale; a registration replaced through the converter's test seam, as ingest's flusher's is, the fence lost once every segment was put |
| a crash after the put, before the registration | the same test counts the puts left unregistered for the reconciler, which deletes a log capture's orphan (`TestLogOrphanDeleted`) |
| a crash after the commit, before the acknowledgements | the same test's lost committed answer: every redelivery stale |
| a takeover with older pending entries, a newer backlog and the old holder's late acknowledgements | `TestGateTakeoverWithHeldEntriesAndBacklog`: a dead holder's deliveries pending, a new replica getting them back before ack_wait by re-creating the consumer, the late acknowledgements after; a live converter acknowledges nothing after its lease (`TestConverterAcknowledgesNothingAfterItsLease`) |
| admission waiting for a masking baseline | `TestGateAdmissionWaitsForABaseline` |
| a rotation's tail inside the window, and past it | `TestGateRotation`; `TestConverterStops` |
| an entry of a stopped file from a device the file never had | `TestGateRotation`, to the converter that stopped the file and to a new one |
| an idle stop racing another group's registration after a change of G | `TestLogIdleStopRacesAnotherGroup`, both orders under a barrier |
| `capture stop` with the lease held and lapsed | `TestStopLogCapture`, `TestCaptureStop`, `TestConverterStops` |
| duplicates by redelivery and by a retried publish | `TestGateFlushRefusedAndRecovered` (a duplicate acknowledged only with its entry's registration), `TestConverterEndToEnd`, `TestConverterMetrics`; `TestGateRetriedPublish` |
| an unreadable entry against a malformed one; pairing's failures | `TestDecode`, `TestPairer`, `TestBuild` (a reply logged under its primary's header), `TestBuildAnnotations`, `TestConverterEndToEnd`, `TestConverterMetrics` |
| an entry beyond retention; DST fall-back and spring-forward | `TestGateEntries` |
| an encrypted body's, an unparsed-entry's and an unrecognised-line's text zeroed | `TestConverterMasks`, the encrypted body's whole text checked zero |

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
Each run's history is its own iterations: a flush row registered 1,356 to 2,565 flushes on SQLite and 156 to 381 on MariaDB, so its catalog held at most about 7,700 packs.

**Step 3** (#104): `RetireConfig` takes the quiescent captures from its caller, and no command computes them yet, so the stop-only rule for a log capture lands with that command.

**Step 4** (#105): no measurement.

**Step 5** (#106): no measurement.

**Step 6** (#107): no measurement.

**Step 7** (#108): no measurement.

**Step 8** (#109): no measurement.

**Step 9** (#110): no measurement.

**Step 10a** (#111): no measurement.

**Step 10b**: the owner's fleet, at ingest's own times (F 300 s, `ack_wait` 7 min, reorder window 30 s), `TRACEKEEP_LOAD=feed-convert`:
1,980 pieces of equipment at the sampled log rate, 1.2 messages a second of about 6 KB, a message being a header entry and a body entry;
20 heavy ones at 500 messages a second, about 20 GB a day zipped each, their IDs in 16 of the 32 groups, at most three in one, as random IDs about fall;
and a burst of 200 at 10 messages of about 53 KB a second for 3 minutes, half a flush into the traffic's judged cycles.
About 24,750 entries a second, 95 MB/s, the burst's 200 MB/s more, converted by dev's log stage on a three-node cluster in one process beside the traffic at K = 100;
the feed stream R1, as dev's traffic stream is, where step 1 measured R3; the process run with `GOMEMLIMIT=28GiB`.
`logfeed.max_ack_pending` at 250,000, above twice the busiest group's entries in a reorder window, about 3,150 a second (§15.10).

Three findings came first, each fixed before the runs below:
- The stage's pull, cut short by its own context, dropped what the server had already sent: redelivered after `ack_wait`, those entries were stale, acknowledged unconverted.
  Each pull now ends at the server and every message sent is read.
- The converter held everything its group received until its equipment's flush, bounded only by `max_ack_pending`, a message count (T-65):
  past 50 GB in five minutes, eight groups stopped at their 100,000 held entries, the host's CPU 93% idle.
  A group past `logfeed.group_max_bytes` of batched payload, or holding half its `max_ack_pending`, now flushes its largest equipment first, and a held delivery keeps only its reply subject.
- The fleet's first heavy IDs hashed five into each of two groups, 5,100 entries a second, whose reorder windows alone held more than 100,000 entries.

| Run | Catalog | Made / offered entries/s | Failed | Publish p99 | Burst made | Burst's backlog peak | Digested after it | Drained | Record entries, all registered | Traffic: minima slope / bound | Traffic ack age max | CPU (cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | SQLite | 25,095 / 24,752 | 16 | 550 ms | before the judged cycles | — | — | 6 left | 45,273,508 | −56.5 / 8.0, sustained | 5m06s | 4.2 |
| 2 | SQLite | 25,109 / 24,752 | 0 | 556 ms | before the judged cycles | 17,392 MiB | 2m08s | 964 left | 41,239,386 | +41.1 / 8.0 | 5m06s | 4.1 |
| 3 | SQLite | 25,129 / 24,752 | 0 | 471 ms | 88.6% | 18,260 MiB | 3m07s | 0 in 5m31s | 42,217,330 | +376.6 / 8.0 | 5m06s | 4.0 |
| 4 | MariaDB | 25,039 / 24,752 | 0 | 489 ms | 86.4% | 16,356 MiB | 4m03s | 0 in 5m32s | 53,874,944 | +98.4 / 8.0 | 5m07s | 4.3 |

Every run converted every stored entry once, into a registered record of its equipment's capture, with no stale, duplicate, unreadable or late entry and no annotation but run 1's.
Run 1's 16 failed publishes were the harness's own, retried with the publishers' cancelled context at their stop, each leaving its pair's other entry an annotation or a header held;
run 2's remainder was the drain check's, which took a remainder unchanged for a second as final while equipment were still to flush, up to the reorder window and F after the publishers stopped.
Runs 3 and 4 have the final harness; their two failures are the bus's, one process holding three servers, the stage and the traffic:
- the publishers made 86% to 89% of the burst's rate, the servers acknowledging its 200 MB/s more slowly; what the burst sent was digested within F of its end;
- the traffic's cycle verdict failed with the burst inside its cycles, its last minimum high (325,054 and 157,717 records), the others near zero,
  while every traffic record was acknowledged within F and 7 seconds, none redelivered and none dropped.
Run 2's verdict, its burst before the cycles, failed on one high last minimum as well (130,190), and run 1's passed; the verdict's minima follow where the run's last cycle ends.
Open for the owner: whether the gate's traffic criterion is this cycle verdict on one host, or the traffic's acknowledgement ages, or a run on separate hosts.
