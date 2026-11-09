# Feed stream a group

Status: active (2026-11-09) — step 1 next

The work that gives each of the host log feed's lease groups a stream of its own, of one replica, its entries acknowledged one by one as today (T-67),
the servers' subject mapping partitioning the publishes so the pipeline names no stream.
The design goes to `docs/specs/tracekeep/logfeed.md` and the rationale to the decision log; this plan only orders the work and records its probes.
Out of scope: spreading the flushes over F, leaving an earlier hour to the next flush, and the log-style use (`docs/ROADMAP.md` *Open items*).

## Steps

| Step | Work | State |
|---|---|---|
| 1 — probes | the risks the design rests on, on a three-node cluster, before the spec changes: the mapping in a configuration-file account, the streams' placement, a server's loss, each stream's limit | pending |
| 2 — spec and decision | `logfeed.md` §15.2, §15.4, §15.10 and §15.11 for a stream a group; the owner's answers below and each choice step 1 leaves to the owner as decisions; the pipeline and configuration guides, the servers' mapping with placeholder tokens for the operations team that maintains it | pending |
| 3 — streams and `migrate` | `logfeed.replicas`; the group streams, their consumers and limits in `internal/bus`; `migrate` replacing the one stream once drained, and changing G with the mapping; preflight's overlap checks over every group stream | pending |
| 4 — the stage, preflight and status | each group's converter on its own stream; a preflight probe that the mapping routes the feed's subjects; `tracekeep status`, metrics and readiness per stream | pending |
| 5 — the gate | the correctness cases the change touches, on the cluster and both engines; the load case on the new layout, on SQLite then MariaDB | pending |
| 6 — operations | the runbook for a server lost with streams of one replica, the alerts, the guides' sizing | pending |

Each step is one pull request.
A probe of step 1 that fails, or a choice it leaves open, stops the plan for the owner's decision.

## Step 1 — probes

Each probe runs on a three-node cluster as `scripts/load-feed.sh` starts it, unless it says otherwise, and records its outcome here.

- **The mapping**:
  the account mapping `<logprefix>.*` to `<logprefix>.{{partition(G,1)}}.{{wildcard(1)}}` with G group streams, in a configuration-file account;
  every publish acknowledged by its group's stream, a retried publish dropped by that stream's duplicate window,
  and each token's group equal to the one the stream transform gives today, which the open captures' `lease_group` rows hold.
- **Placement**: where 32 streams of one replica land with no placement given, counted per server, and with a placement tag per server;
  the measurements' gain came from streams spread over the three servers, and the earlier runs' unplaced streams all landed on one.
- **A server lost**: what a publish to a group stream on a stopped server returns, how long the pipeline's retries hold it with the probed forwarder settings,
  what the stage does with that group meanwhile, and that the stream and its consumer resume, with their messages, when the server returns.
- **Each stream's limit**: the fleet's bytes per group, its heavy equipment hashed unevenly, against `logfeed.max_bytes` divided by G;
  whether a group's stream fills first under discard-new while the others have room.

## The owner's answers (2026-11-09)

Step 2 records them in the spec and the decision log; until then they bound the probes and the steps.

- Accounts are in the servers' configuration files, the operations team maintaining the mapping.
- The feed is deployed nowhere, so `migrate` replaces the one stream once drained.
- The measurement harness of 2026-11-08 (the feed-streams mode, the flush and series switches) is on an unmerged branch; step 5 takes the parts the gate needs.
