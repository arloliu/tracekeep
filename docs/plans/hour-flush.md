# Hour flush

Status: active (2026-11-23) — the owner's two decisions of 2026-11-23 (T-72) and ingest's design (T-73); steps 1 and 3 done, steps 2 and 4 waiting for the host

The work behind the owner's two decisions on the ROADMAP's open item *A fleet's flushes align, and their load with them*:
stop the hour's change from flushing every equipment's earlier hour at once, and explain why spreading the flushes measured worse on a stream a group before spreading is taken up.
The design goes to `docs/specs/tracekeep/logfeed.md` and `ingest.md`, the rationale to the decision log; this plan orders the work and holds the report the work needs.
The measurements it cites are the flush-evidence cells of 2026-11-08 (kept outside the repository): A1 to A3 on the log stage, B1 to B5 and P1 to P4 on the bus alone.

## Steps

| Step | Work | State |
|---|---|---|
| 1 — the log stage holds the hour | the converter's hour trigger and its load switch removed, each equipment keeping its flush times across the hour; `logfeed.md` §15.7 | done #135 |
| 2 — measure the hold | the before and after cells below, on today's layout, the log stage aligned and dispersed | pending |
| 3 — ingest holds the hour | a capture's flush time in `Batcher.Due`, its earlier hour registered first, the gap alert's `for:` (`alerts.gapsFor`); `ingest.md` §6.2 | done #139 |
| 4 — explain the spread | the bus-only cells of *Spreading* below, with the harness's switch for a phase a group | harness switch done; cells pending |
| 5 — decide the spread | the spec and the decision for spreading by group, or none, from step 4 | pending |

Each step is one pull request; steps 2 and 4 need the host alone and are scheduled by the owner.

## Step 1 — the log stage holds the hour

- `Converter.Tick` no longer flushes an equipment whose batches hold two hours;
  an earlier hour's batches wait for the equipment's next flush, which comes F after its previous one, or at its phase.
- The `HoldHours` switch, its wiring in `internal/service` and the load harness's `TRACEKEEP_LOAD_HOURS` go:
  holding is the only behaviour, so the step 2 "before" cells run the commit before this step.
- The test that encoded the hour's flush becomes `TestConverterKeepsItsCadenceAcrossTheHour`, on both engines.
- No false gap can come of it here:
  an equipment's flush registers all its batches in one transaction (`RegisterLogFlush`), and the log stage records no gap observation.
- Ingest is unchanged: holding there needs a capture's own flush time, which the task stopped at (*Ingest* below).

## Step 2 — measuring the hold

The A cells measured the hold only together with spreading (A3), on the one partitioned stream of that day.
Hold alone is unmeasured, and on an aligned fleet it is not expected to lower the hour's busiest second (*What the hold can do* below).

Each cell is the feed-convert mode on today's layout, 32 group streams of one replica, as the A cells were run otherwise:
MariaDB and the S3 store, the series recorded, the bus on its own disk and cores, block profiles on.

```
TRACEKEEP_LOAD_ENGINE=mariadb TRACEKEEP_MARIADB_DSN=<dsn> \
TRACEKEEP_LOAD_S3=1 TRACEKEEP_S3_ENDPOINT=<endpoint> TRACEKEEP_S3_ACCESS_KEY=<key> TRACEKEEP_S3_SECRET_KEY=<secret> \
TRACEKEEP_LOAD_SERIES=<cell>/ BUS_LOGS=<cell>/logs BUS_DIR=<bus disk> BUS_CPUS=<ccd 0> TEST_CPUS=<ccd 1> BUS_BLOCK_RATE=<as the A cells> \
RUN_FOR=1800 scripts/load-feed.sh feed-convert
```

Each starts at HH:52:30, off the F grid, so the cell's ordinary flushes fall near :57:30, :02:30 and :07:30
and the hour's change at about 450 s is apart from them; the burst starts at F + `ack_wait` + F/2, 870 s.
The A cells started at HH:50, which put an ordinary flush seconds after the hour and the hour's own a reorder window (30 s) later:
A1's minute after the hour registered 5,926 packs against 2,263 at an ordinary flush, about three an equipment.

| Cell | Commit | `TRACEKEEP_LOAD_PHASE` | Order |
|---|---|---|---|
| H1 before, aligned | step 1's parent | unset | 1 and 3 |
| H2 after, aligned | step 1's head | unset | 2 and 4 |
| H3 before, dispersed | step 1's parent | `spread` | 5 |
| H4 after, dispersed | step 1's head | `spread` | 6 |

The aligned pair runs twice, alternating, for its repeatability; the dispersed pair once.
The dispersed cells spread by equipment, which loads the bus worse on this layout (*Spreading*):
they judge the hold's effect on the store and the catalog, not the bus.

Judged over 420–840 s, the hour's change to past its F + 60 s and clear of the burst, the busiest second against the same cell's ordinary flush (300–420 s):

- aligned, before: a flush at about :00:30, the earlier hour's last three minutes and the later hour's first entries, two packs an equipment, then one at :05:30;
  after: none at the hour, the two packs at the ordinary :02:30;
  so the busiest second about unchanged, and one pack an equipment fewer in the window;
- dispersed: the hour's busiest second near the cell's ordinary one, as A3's 32 puts a second against A2's 997;
- both: the mode's own judgment passes, every entry stored and accounted.

In a long run the old trigger had already moved an aligned fleet's flush times onto the hour plus the reorder window,
where the trigger and the ordinary flush come together; so in steady state the hold saves at most one wave an hour, unmeasured.

## Report

### What the hold can do

On an aligned fleet the hour's flush is one more wave a fleet already makes every F.
Before step 1 an equipment flushes once its later hour's first entry leaves the reorder window, its earlier hour's tail and that entry's hour, two packs,
and its flushes then fall F apart from there;
after it, the equipment's next ordinary flush writes the same two packs, and its flush times stay where they were.
So the busiest second stays about the same; at most one wave an hour goes, a pack an equipment with it.
The hold's value is that it keeps whatever dispersion a fleet has, which the hour's change otherwise erases every hour:
the 997 to 32 puts of A2 to A3 is that, a dispersed fleet kept dispersed, and spreading of any kind needs it.

### Ingest

Holding in ingest is not separable from a capture's own flush time in `Batcher.Due`, so the task stopped there; the findings below are of the code before step 3.

- `Batcher.Add` stamps each batch's opening, and `Due` compared each batch's alone.
  With the later-hour clause dropped and nothing more, a capture's earlier hour flushes at its own time,
  but every capture's later-hour batch opens with the hour's first record and is due F after it,
  so the fleet realigns at the hour's start plus F instead of at its start: as many waves, moved.
- Keeping a capture's flush times needs its batches due together, at its earliest open batch's opening plus F: a capture-level due time.
- Dropping the clause alone would also widen the false gap:
  a later hour's batch closed by `ingest.batch_max_bytes` or the metadata budget is flushed at once, inside the fetch (`Ingester.stepFlushing`),
  and registers before the earlier hour's batch, which would then wait up to F;
  that would happen at every hour to a capture that fills `ingest.batch_max_bytes` (64 MiB) within F, where before step 3 it needed the size reached within one fetch.

Step 3's design, which the owner chose (T-73) and step 3 implements: a capture's batches are due together, at its earliest open batch's opening plus F;
a batch closed early takes its capture's earlier-hour batches with it; `Due`, `Closed` and the stop's `All` return them ordered by capture and hour (they iterated a map),
which the ingester's loop already flushes one at a time in order.
The bound readers rely on holds: no batch waits more than F after its own opening.

### The false gap

- Registering a capture's earlier hour first removes the false gap at its cause within one replica,
  and it touches no transaction or lock: each batch registers in its own transaction, under the same prologue (`.agents/rules/600-invariants.md`).
- It cannot remove the class: every ingest replica pulls from the one consumer of the stream's incarnation (`bus.Consumer`, no filter),
  so one capture's records are split across the replicas' batches and their registrations interleave, whatever the hour.
  `gap_observations_total` is an event count by its definition (`operations.md` §10.4);
  before step 3, `TracekeepGapsGrowing` compared `open_gaps` with 30 minutes before at a single evaluation, with no `for:`, so a hole open at that instant fired it.
- Step 3 takes both (T-73): the ordering, and a `for:` on the alert, the chart's `alerts.gapsFor`, 10 minutes by default, at least F and a flush,
  since the replicas' case does not depend on the hold; F reaches the chart only through `config` or the environment, so the value is set beside it.
  The `for:` delays the rule's declared-loss arm as long.
  The log stage needs neither (step 1).

### Why spreading measured worse on a stream a group

B5 (spread) and B2v2 (aligned) ran the bus-only harness (`loadstreams_test.go`) on 32 group streams of one replica, acknowledged entry by entry, F of 5 minutes, 2,000 equipment, one cell each.

| Window | Bus disk reads, MB/s, B2v2 → B5 | Disk utilisation | Entries on the streams not yet delivered |
|---|---|---|---|
| before the burst, 60–750 s | 35 → 226 | 16 → 35 % | 5,300 → 2,100 |
| the burst, 750–930 s | 389 → 712 | 50 → 70 % | 108,000 → 97,000 |
| after it, 930–1200 s | 258 → 710 | 38 → 63 % | 256,000 → 659,000 |
| the run, 60–1200 s | 144 → 417 | 27 → 47 % | 77,000 → 165,000 |

The ROADMAP's figures are the run's; the reads are 6.5 times aligned's before the burst too, so the burst does not explain them.
Publish p99 (461 against 134 ms) was recorded for the whole run only and cannot be split by window.

**Hypothesis 1, the block cache (confidence: medium-high for the mechanism, low for its threshold).**
nats-server v2.15.1 loads a message's block to remove it (`filestore.go`, `removeMsgFromBlock`, "Figure out a way not to have to load it in"),
under the store's lock;
a block loaded for a removal is then held only weakly (`finishedWithCache`, an elastic pointer), so the next garbage collection may drop it,
and a held cache expires 10 s after its last read (`defaultCacheBufferExpiration`; a removal's lookup counts as one).
An equipment's acknowledged entries lie in nearly every block its group's stream wrote over its last F, interleaved with the group's other equipment.
- Aligned, a group's equipment flush in one sweep: each block is read once a F.
- Spread over one stream (A2), 2,000 equipment flush 0.15 s apart on average, so every block is touched again before it can be dropped: A2 read 1 against A1's 144 MB/s.
- Spread over a stream a group, 60 to 65 equipment share a stream, 4.8 s apart on average, at random,
  and a block outlives a gap only if no collection and no expiry falls in it;
  from the servers' partition function and the harness's phase, each group's flushes leave 6 to 10 gaps longer than 10 s a F (236 in all, 7.4 a group, the longest 25 s), and more that span a collection.
  After such a gap the next equipment's flush reads the group's blocks again: 6.5 reads of the held data a F against one aligned, measured.
  Which of the two drops them, and so the threshold a gap must stay under, is not established.
- The servers' block profiles agree (tb-2, the run's end): the publish path's waits for the stream's lock 250 s against 35 s, `IsClustered`'s 436 s against 158 s,
  and contention in `removeMsg`, `fetchMsg` and `tryExpireCache` in B5 only.
- Settling by purge removes no entry one by one, and there spreading did not multiply the reads
  (P4 spread against P2b aligned, both at 120 GiB: mean 71 against 50 MB/s, p99 367 against 2,642).

**Hypothesis 2, a stream's stalls spread through F (medium).**
A removal reading a block holds the store's lock that a publish's `StoreMsg` needs, while the publish holds the stream's lock.
Aligned, a stream's stalls fall within one sweep a F and delay only the publishes of those seconds; spread, they fall all through F, and the p99 sees them.
Purging shows the same with another stall, the purge under the stream's lock:
P4's publish p99 was 89 ms against P2b's 14, its retries 12.8 against 1.4 million, though its reads were not higher.
It rests on the profiles and that pair; the per-window p99 that would show it was not recorded.

**Other signs, not explained (low).**
B5's clients logged 481 write errors on 25 connections, against 36 on 9 (connections the servers reset);
its acknowledgements exceed the entries removed by 124,000, against 46,500, which suggests more redeliveries.
Both follow from slower servers as readily as they could cause them.

**The harness, checked for confounds.**
- Spread and aligned are as intended: a phase from the equipment's ID modulo F, kept across flushes; aligned, due F after the first release and after each flush.
- Each entry is acknowledged alone in both modes; spreading changes only how many flushes share a moment.
- The group's bound (128 MiB, half of max_ack_pending) relieves heavy equipment, 500 entries a second in 17 groups, about every 50 s in both modes, so they are never aligned; light groups stay under it.
- One cell each; B5 ran last in a seven-hour queue, B2v2 four hours earlier; the first B2's record is incomplete.
  Repeatability is unmeasured.
- A2's one-stream result came from the log stage, not this harness, which never ran one stream spread.

**Conclusion.**
Spreading by equipment is the wrong granularity on a stream a group:
it multiplies each stream's sweeps of its cold blocks by the gaps between its few equipment's phases, and spreads each stream's stalls through F.
Spreading **by group** keeps one sweep a stream a F, as aligned does, and still splits the fleet's wave for the store and the catalog 32 ways;
it is the direction to test, and it needs the hold, without which every hour realigns it.

### Spreading: the cells to run (step 4)

The harness first gains a `TRACEKEEP_LOAD_FLUSHES=group` switch, the phase from the group's number instead of the equipment's,
and its report a publish p99 per window (before, during and after the burst).

```
TRACEKEEP_LOAD_STREAMS=<32 or 1> TRACEKEEP_LOAD_REPLICAS=1 TRACEKEEP_LOAD_FLUSHES=<unset, spread or group> \
TRACEKEEP_LOAD_SERIES=<cell>/ BUS_LOGS=<cell>/logs BUS_DIR=<bus disk> BUS_CPUS=<ccd 0> TEST_CPUS=<ccd 1> BUS_BLOCK_RATE=<as the B cells> \
BUS_FEED_STREAMS=<32 for 32 streams, unset for 1> scripts/load-feed.sh feed-streams
```

| Cell | Streams | Flushes | Expected, if hypothesis 1 holds |
|---|---|---|---|
| S1 | 32 | aligned | B2v2 again: reads about 35 MB/s before the burst |
| S2 | 32 | spread | B5 again: about six times S1 |
| S3 | 32 | group | about S1's reads, publish p99 at or below S1's |
| S4 | 1 | spread | about no reads before the burst, as A2 |
| S5 | 32 | aligned | S1 again |

Run in that order, S1 and S5 bracketing the rest for the host's drift.
If S3 reads like S2, hypothesis 1 is wrong and spreading stays parked;
if S3 reads like S1 but its publish p99 is S2's, hypothesis 2 is wrong, and the p99 needs another explanation before spreading is taken up.
