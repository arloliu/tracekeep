# Host log feed

Status: active (2026-11-07) — step 1, the feed stream's load

The work that delivers the host log feed, `docs/specs/tracekeep/logfeed.md` (§15), as T-60 shapes it,
with its two riskiest costs measured before the converter is built (T-63): the feed's load on the bus, and its registration's on the catalog writer.
The design is the spec's and the rationale the decision log's; this plan only orders the work and records its measurements.

## Steps

| Step | Work | State |
|---|---|---|
| 1 — the feed stream's load | the feed's stream and group consumers under the fleet's rate on a three-node cluster, beside traffic, before any converter exists | in-progress |
| 2 — the catalog | `logfeed_captures`, `logfeed_files` and the `captures` columns; `AdmitLog`, `RegisterLogFlush`, `StopLogCapture`, on both engines; their benchmarks and first baselines, judged against §15.10's writer budget | pending |
| 3 — protections | the reconciler deletes a log capture's orphan; the compactor's close leaves log captures out; `capture close` and `capture terminate` refuse one; retirement's stop-only quiescence (§15.8) | pending |
| 4 — stream, configuration and preflight | `migrate` ensures the stream and consumers, a change of G included; the `logfeed.*` configuration; the prefix and overlap checks (§15.4, §15.11) | pending |
| 5 — conversion | `internal/logfeed`'s pure part: entry decoding, reorder, duplicates and stale entries, pairing, devices, time, records and annotations (§15.3, §15.5) | pending |
| 6 — the converter | batches, flush and registration, lease groups and the consumer's re-creation, file stops, the service's wiring; `internal/logfeed` in a `scripts/catalog.sh` leg (§15.6, §15.7) | pending |
| 7 — reads and admin | the reads' method fields, `capture stop`, `tracekeep status`'s feed lines (§15.8, §15.11) | pending |
| 8 — web UI and operations | the UI's method filter and badge; alerts, metrics, runbook, configuration and pipeline guides (§15.8, §15.11) | pending |
| 9 — the gate | the gate cases below | pending |

Each step is one pull request.
Steps 1 and 2 record their numbers here before step 5 starts;
a number past its budget stops the plan for the owner's decision.

## Step 1 — the feed stream's load

A new mode of the load harness, `TRACEKEEP_LOAD=feed` (`.agents/rules/300-testing.md` *Running*), with `-smoke` and `-real` as its other modes have.

- **The bus**: `internal/testbus`'s three-node cluster, R3, shared by the traffic harness and the feed through a seam;
  the seam lets dev use a bus it is given instead of starting its own, and gives the harness the cluster's servers.
- **The stream** and its 32 consumers as `logfeed.md` §15.4 specifies them, provisioned by the harness:
  work-queue, file storage, S2, discard-new, a duplicate window of 2 min, the transform `{{partition(32,1)}}`;
  `max_bytes` 40 GiB for the main case.
- **Publishers**: 2,000 equipment tokens at 1.6 entries a second each, 3,200 a second, each entry about 880 B in the store's accounting and generated from go-secs messages.
  Each is published with its message id and its publish acknowledgement awaited, from several connections with requests in flight.
  Offered entries, acknowledged publishes, failed publishes, retries and the publishers' queue are counted apart.
- **Consumers**: one per group; each entry waits the reorder window, then joins its equipment's batch, which is acknowledged entry by entry at its equipment's flush every F,
  the equipments' flushes spread over F, held entries kept in progress every `ack_wait / 3`.
  Times: `-real` ingest's (F 300 s, `ack_wait` 7 min, reorder window 30 s, `max_ack_pending` 100,000);
  the default T-61's ratio, 1/30 (F 10 s, `ack_wait` 14 s, reorder window 1 s, `max_ack_pending` 3,400);
  `-smoke` the default's times at 1/10 of the rate.
- **Traffic beside it**: the production mode's traffic at K = 100 on the same cluster, through the same run.
- **Cycles**: one warm-up flush cycle, then at least three judged ones.
- **Measured**, each judged cycle:
  the acknowledged publish rate, the publish latency's median and 99th percentile, the publishers' queue;
  the stream's backlog and logical bytes, each consumer's pending acknowledgements, the busiest group's apart;
  the acknowledgements and in-progress acknowledgements a second;
  the process's CPU, resident memory and disk writes, all three servers and the harness together;
  each server's store on disk, its sealed blocks apart from its active ones and its metadata;
  the traffic harness's own verdict.
- **Budgets**, over the judged cycles:
  acknowledged publishes at least 98% of the mode's offered rate, the publishers' queue and the backlog with no rising slope, as the traffic harness judges its own;
  publish latency at the 99th percentile within 1 s;
  every consumer's pending acknowledgements under `max_ack_pending`;
  sealed blocks within a third of their logical bytes;
  the traffic harness's verdict sustained.
  The process's CPU, memory and disk writes have no budget, since one process holds three servers and the harness:
  they are recorded here and the owner judges them with step 2's numbers before step 5.
- **Controls**, each its own case:
  - a constrained publisher, capped at half the mode's offered rate, which must fail the rate budget;
  - one request in flight from one connection, measured and not judged;
  - a full stream: a `max_bytes` of a few megabytes, consumers paused until publishes fail, the entries accepted before intact, then a drain after which the same publishes succeed;
  - a lost publish acknowledgement: a publish retried under the same message id within the window is stored once;
  - the duplicate window's memory: the main case run once with message ids and once with an inert header of the same length in their place, resident memory compared at the window's full population, about 384,000 ids at the full rate;
  - a takeover: one group's consumer deleted and re-created while it holds its share of pending acknowledgements, with stale acknowledgements and in-progress acknowledgements from its old handles;
    every held message redelivered, oldest first, within `ack_wait`, and the publishers' latency meanwhile.

The three servers share one process and one machine, so the process's figures bound the three together, not one server of a deployment;
the gate's real run (step 9) checks them.

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

None yet.
