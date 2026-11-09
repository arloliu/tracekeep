# Feed stream a group

Status: active (2026-11-09) — step 2 awaits the owner's choices

The work that gives each of the host log feed's lease groups a stream of its own, of one replica, its entries acknowledged one by one as today (T-67),
the servers' subject mapping partitioning the publishes so the pipeline names no stream.
The design goes to `docs/specs/tracekeep/logfeed.md` and the rationale to the decision log; this plan only orders the work and records its probes.
Out of scope: spreading the flushes over F, leaving an earlier hour to the next flush, and the log-style use (`docs/ROADMAP.md` *Open items*).

## Steps

| Step | Work | State |
|---|---|---|
| 1 — probes | the risks the design rests on, on a three-node cluster, before the spec changes: the mapping in a configuration-file account, the streams' placement, a server's loss, each stream's limit | done #PR |
| 2 — spec and decision | `logfeed.md` §15.2, §15.4, §15.10 and §15.11 for a stream a group; the owner's answers below and each choice step 1 leaves to the owner as decisions; the pipeline and configuration guides, the servers' mapping with placeholder tokens for the operations team that maintains it | pending |
| 3 — streams and `migrate` | `logfeed.replicas`; the group streams, their consumers and limits in `internal/bus`; `migrate` replacing the one stream once drained, and changing G with the mapping; preflight's overlap checks over every group stream | pending |
| 4 — the stage, preflight and status | each group's converter on its own stream; a preflight probe that the mapping routes the feed's subjects; `tracekeep status`, metrics and readiness per stream | pending |
| 5 — the gate | the correctness cases the change touches, on the cluster and both engines; the load case on the new layout, on SQLite then MariaDB | pending |
| 6 — operations | the runbook for a server lost with streams of one replica, the alerts, the guides' sizing | pending |

Each step is one pull request.
A probe of step 1 that fails, or a choice it leaves open, stops the plan for the owner's decision.

## Step 1 — probes

The probes ran on three nats-server processes of the module's version, each tagged with its name, in a configuration-file account,
as `TestFeedGroupProbes` on the unmerged branch `test/feed-group-probes` (9b473af), with the harness's fleet of 2,000 equipment tokens; step 5 takes what the gate needs.

- **The mapping** passes:
  every token's group under the account mapping equals the one the stream transform gives today, for all 2,000 tokens, both being the server's one partition function;
  each publish is acknowledged by its group's stream, and its retry under the same message id is acknowledged as a duplicate of the same sequence, stored once.
- **Placement** needs tags:
  32 streams of one replica with no placement all landed on one server, in every round and every run, the server differing from cluster to cluster;
  tagged round-robin, they landed 11, 11 and 10.
- **A server lost** loses nothing, and stalls its groups for the outage:
  a publish to a group on the killed server fails after about 0.5 s (`no response from stream`), and the other servers' groups take theirs meanwhile;
  the group's fetch fails at once with no responders, which the stage's loop does not pass over, so its converter gives up, releases the lease and retries after its pause;
  restarted, the server is healthy and takes the group's publishes again within about 1.3 s;
  the consumer is there, its in-memory state reset, which the stage's start replaces anyway,
  and the six entries held unacknowledged before the loss are redelivered at once, joining their held copies (the converter's duplicate path), acknowledged at the flush.
- **Each stream's limit** is the stream's own:
  a full group's stream refuses publishes (error 10077) while the others take theirs;
  the harness's fleet is uniform, 60 to 66 equipment a group against a mean of 62.5, so the fullest group reaches `logfeed.max_bytes` / G at about 95 % of the total;
  a real fleet's heavy equipment is not measured: a group holds at least its heaviest equipment's share, so one equipment above 1/G of the fleet's bytes fills its stream alone.
- On a cluster just formed, a stream's creation went unanswered for a minute in about one run in six, as `testbus.Retry` notes; step 3's `migrate` retries it.

## Step 2 — the owner's choices first

Step 2 starts once the owner chooses:

- **Placement**: `migrate` applying a list of the servers' tags round-robin, a `logfeed` setting, or the operations team placing the streams.
- **Each stream's limit**: `logfeed.max_bytes` / G with a headroom factor, or each stream at `logfeed.max_bytes`, the total bounded by the servers' disks.
- **A server lost**, the cost of one replica: inferred from Vector's source, not measured, the sink's concurrency limit wraps its retries,
  so requests to the lost server's groups, about a third, keep their slots retrying until every slot is theirs,
  and the whole feed stalls for the outage, its buffer and then the hosts' files holding the entries, none lost;
  a stream of three replicas rides a server's loss out in seconds.
  The owner accepts it, or asks for the stall measured first.

## The owner's answers (2026-11-09)

Step 2 records them in the spec and the decision log; until then they bound the probes and the steps.

- Accounts are in the servers' configuration files, the operations team maintaining the mapping.
- The feed is deployed nowhere, so `migrate` replaces the one stream once drained.
- The measurement harness of 2026-11-08 (the feed-streams mode, the flush and series switches) is on an unmerged branch; step 5 takes the parts the gate needs.
