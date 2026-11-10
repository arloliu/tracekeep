# Feed stream a group

Status: active (2026-11-09)

The work that gives each of the host log feed's lease groups a stream of its own, of one replica, its entries acknowledged one by one as today (T-67),
the servers' subject mapping partitioning the publishes so the pipeline names no stream.
The design goes to `docs/specs/tracekeep/logfeed.md` and the rationale to the decision log; this plan only orders the work and records its probes.
Out of scope: spreading the flushes over F, leaving an earlier hour to the next flush, and the log-style use (`docs/ROADMAP.md` *Open items*).

## Steps

| Step | Work | State |
|---|---|---|
| 1 — probes | the risks the design rests on, on a three-node cluster, before the spec changes: the mapping in a configuration-file account, the streams' placement, a server's loss, each stream's limit | done #121 |
| 2 — spec and decision | `logfeed.md` §15.2, §15.4, §15.10 and §15.11 for a stream a group; the owner's answers below and each choice step 1 leaves to the owner as decisions; the pipeline and configuration guides, the servers' mapping with placeholder tokens for the operations team that maintains it | done #123 |
| 3 — streams and `migrate` | `logfeed.replicas`; the group streams, their consumers and limits in `internal/bus`; `migrate` replacing the one stream once drained, and changing G with the mapping; preflight's overlap checks over every group stream | pending |
| 4 — the stage, preflight and status | each group's converter on its own stream; a preflight probe that the mapping routes the feed's subjects; `tracekeep status`, metrics and readiness per stream | pending |
| 5 — the gate | the correctness cases the change touches, on the cluster and both engines; the load case on the new layout, on SQLite then MariaDB | pending |
| 6 — operations | the runbook for a server lost with streams of one replica, and a lost server's replacement; the alerts; the guides' sizing of Vector's buffer for the outage, and its disk buffer's held tail | pending |

Each step is one pull request.
A probe of step 1 that fails, or a choice it leaves open, stops the plan for the owner's decision.

## Step 1 — probes

The probes ran on three nats-server processes of the module's version, each tagged with its name, in a configuration-file account, with the harness's fleet of 2,000 equipment tokens.
They are `TestFeedGroupProbes` on the unmerged branch `test/feed-group-probes` (9b473af); step 5 takes what the gate needs.

- **The mapping** passes:
  every token's group under the account mapping equals the one the stream transform gives today, for all 2,000 tokens, both being the server's one partition function;
  each publish is acknowledged by its group's stream, and its retry under the same message id is acknowledged as a duplicate of the same sequence, stored once.
- **Placement** needs tags:
  32 streams of one replica with no placement all landed on one server, in every round and every run, the server differing from cluster to cluster;
  tagged round-robin, they landed 11, 11 and 10.
- **A server lost** and restarted with its storage loses no stored entry, and stalls its groups for the outage:
  a publish to a group on the killed server fails after about 0.5 s (`no response from stream`), and the other servers' groups take a client's publishes meanwhile, if it does not wait on the failed ones (Vector does, below);
  the group's fetch fails at once with no responders, which the stage's loop does not pass over, so its converter gives up, releases the lease and retries after its pause;
  restarted, the server was healthy after about 1.3 s, and the group's first publish after that was taken;
  the consumer is there, its in-memory state reset, which the stage's start replaces anyway,
  and the six entries fetched but unacknowledged before the loss are redelivered at once.
  The probe fetched them itself: the stage, having given the group up, abandons its held entries, and the converter taking the group again converts their redeliveries afresh.
- **Each stream's limit** is the stream's own:
  a full group's stream refuses publishes (error 10077) while the others take theirs;
  the harness's fleet is uniform, 60 to 66 equipment a group against a mean of 62.5, so the fullest group reaches `logfeed.max_bytes` / G at about 95 % of the total;
  a real fleet's heavy equipment is not measured: a group holds at least its heaviest equipment's share, so one equipment above 1/G of the fleet's bytes fills its stream alone.
- On a cluster just formed, a stream's creation went unanswered for a minute in about one run in six, as `testbus.Retry` notes; step 3's `migrate` retries it.

## The server-loss measurement (2026-11-10)

Measured on one host: three nats-server processes as step 1's, the 32 group streams tagged round-robin,
and Vector 0.61 publishing to all three servers at the fleet's rate, 24,700 entries a second, 95 MB/s.
Its retries were unbounded, backing off at most 3 s, and its buffer was a 20 GiB disk buffer blocking when full;
its concurrency was fixed at 256 in four runs and adaptive in one.
In each of the five runs, one server was killed and restarted with its storage 60 s later.

- **The whole feed stalls** for the outage, with a fixed or an adaptive concurrency.
  From 1 s after the kill until the server was back, the groups on the other servers took nothing in the two runs that killed a server Vector was not connected to,
  and about 500 entries in the minute, against a million before it, in the three that killed the server Vector was connected to.
  Publishes to the lost groups fail at once (`no stream found for given subject`) and are retried;
  by Vector's source, each retry keeps its concurrency slot, so the lost groups, about a third of the entries, come to hold every slot.
- **The cost is the buffer**: up to 1.56 million entries, about 5.8 GiB, for a minute of outage at the fleet's rate.
  The lost groups took entries 1 s after the server was healthy, and the buffer was below 1,000 entries 11 to 15 s after the restart.
  No entry was discarded or stored twice, and none was missing but the ten below;
  entries of one file were stored at most 2.9 s out of order.
- One run ended with its last 10 entries still in Vector's buffer, unrelated to the outage.
  Vector 0.61's disk buffer can leave a burst's tail unread until the next entry arrives:
  with no fault, bursts of random size to one server held their tail in 13 and in 17 of 200 with a disk buffer, and in none of 200 with a memory buffer
  (Vector issues #23456 and #23605, their fixes not merged).
  Under the fleet's traffic the next entry comes within milliseconds; an aggregator gone idle holds its last entries until one does.

## The owner's answers

Step 2 recorded them in the spec and the decision log (T-68).

On 2026-11-09:

- Accounts are in the servers' configuration files, the operations team maintaining the mapping.
- The feed is deployed nowhere, so `migrate` replaces the one stream once drained.
- The measurement harness of 2026-11-08 (the feed-streams mode, the flush and series switches) is on an unmerged branch; step 5 takes the parts the gate needs.

On 2026-11-10, the choices step 1 left:

- **Placement**: `migrate` places the streams, applying a `logfeed` setting's list of the servers' tags round-robin over the groups;
  the operations team tags each server.
- **Each stream's limit**: `logfeed.max_bytes` / G × 1.25.
  The limits sum to 1.25 × `logfeed.max_bytes`, which the servers' disks must hold,
  and a group past its own limit refuses publishes while the others have room.
- **A server lost**: one replica, its stall as measured above, accepted;
  Vector's buffer is sized for the outage a deployment accepts, and `logfeed.replicas` remains for one that wants three.
