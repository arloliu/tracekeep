# Phase 2 — compact and machine API

Status: draft (2026-10-27) — awaiting the owner

Milestone 2 of the [roadmap](../ROADMAP.md):
the compactor's state machines, the catalog reads they make answerable, and the machine API the VE service reads them through.
Its scope and gate are the roadmap's phase 2 row and the implementation checklist under it;
the design is `docs/specs/tracekeep/compactor.md`, `catalog.md` §7.2–§7.3, `api.md` §10.1–§10.2, `operations.md` §10.4 and `storage.md` §5.2 *Downloads*.
This plan only orders the work.

## Before it starts

- T-7's gate: connect-go's overhead against plain gRPC, measured for `ListPacks` and a streamed `QueryRecords` shape before phase 2.5 starts,
  the result recorded in the decision log.
  It runs beside phases 2.1–2.4, which need no API.
- tracepack v0.9.0 is already the service's (T-36), so every close is written uncompressed (G5-272):
  phase 2.2 drops `capture_closes.codec` and the codec from `compactor.md` §8.2 and `catalog.md` §7.1,
  which closes the roadmap's open item on it.
- Not in this phase:
  `QueryRecords`, `FindTransaction`, `Export` and an export bundle's retention, people's OIDC, the frontend and holds are phase 4's;
  `Restore`, `Digest` and the `keys` module are phase 3's;
  configuration retirement waits for the first release that needs it.

## Phases

| Phase | Scope | State |
|---|---|---|
| 2.1 — merge and conflicts | the merge of §8.1 with `CommitMerge`, `RecordConflict` confirming or clearing suspects, `scope merge`, `conflict acknowledge`, `scope exclude-input`, and their metrics | pending |
| 2.2 — close | `CloseIntent` and `PublishClose` of §8.2 with both triggers, `abandoned` intents (checklist 7's close), `capture close`, crash gaps in completeness, `closes_total{trigger}` | pending |
| 2.3 — deletion and retention | `DeletePack` after D, `EvictHour` with `evicted_ranges` and the `parked → evicted-hour` move, §8.4's predicate as one function, survivors deleted by the reconciler, `object list --survivors` and `--unattributed`, and their metrics | pending |
| 2.4 — reads and cursor | `ListCaptures`, `ListPacks` and `ListScopeChanges` as catalog reads (§7.3): `no-data` hours, unregistered intervals, overlap, the watermark read first, `low_water` and `epoch`, pruning by seq prefix | pending |
| 2.5 — api | `proto/`, connect-go, the `api` role in `all` and `dev` with its readiness and metrics, the three reads, `PackURL` with the deadline clamp and `pack-superseded`, presigning checked at start, `audit_log` for API actions | pending |
| 2.6 — service auth | client credentials against the IdP's JWKS with lazy discovery, mTLS, the static-token flag, the roles the phase's operations check, `auth_failures_total` | pending |
| 2.7 — admin and status | `tracekeep status`, `deferred list`, admin through the `api` process on SQLite, `gap_observations_total` and `captures_with_unknown_tail`, the cost of ingest's metrics read measured at fleet size | pending |
| 2.8 — gate | the roadmap row's cases and the checklist the phases before left open, audited against the tests as 1.7 did | pending |

Only the phase under way is detailed; the others are detailed when they start.
Every compactor transaction starts with the capture-row helper and checks the lease after it (T-32, `.agents/rules/600-invariants.md`);
every catalog step runs on both engines, and every concurrent schedule is barrier-controlled.
Each phase lands the metrics of `operations.md` §10.4 for the code it adds.
The roadmap's gate case "a parked message replays in order after its blocker clears" is the parked consumer's own retry after `parking.redelivery_delay`,
staged through a flush sorted by seq (T-40 withdrew an early replay to proposal P1).

## Phase 2.1 — merge and conflicts

Work on the branch `feat/compact/merge`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The merge runs in the compactor's cycle beside the reconciler, under the one lease;
merged inputs stay `merged` until phase 2.3 deletes them.
The quiet period Q (default 15 minutes, T-12) becomes `compact.quiet_period` in the configuration and its guide.

| Step | Scope | State |
|---|---|---|
| 1 | catalog: the candidate scan (`open` or `reopened`, hour end + Q passed, `staged` packs, no `conflicts` row), the snapshot (revision r and the `staged` inputs, segments and archives alike), `CommitMerge` with the revision and lease compare-and-sets, zero affected rows rolling back the whole transaction, the `settled` event last; barrier schedules on both engines: a registration between snapshot and commit in `open` and in `reopened`, a lease takeover between statements | pending |
| 2 | the merge pass: `tracepack.Merge` over `ReaderAt`s of the inputs, the archive written to the working directory and put under its `archive/` key, then `CommitMerge`; a rolled-back merge or a crash after the put leaves the archive to the reconciler, which adopts it as a `staged` input of the next merge ([FMT I-12]); a merge failure leaves the scope as it was and is counted | pending |
| 3 | `RecordConflict`: a suspect's overlapping records compared by bytes and redaction entries, cleared for equal copies and confirmed for differing ones only while the row names exactly the compared packs (otherwise `changed`); a merge failing on a conflict writes the row; every touched scope's revision advanced and a `conflict` event each | pending |
| 4 | admin: `scope merge`, `conflict acknowledge` (a state the `conflicts` check gains; the scope stays unmerged), `scope exclude-input` (the pack `excluded` under the capture row, the revision advanced, an event emitted), each audited | pending |
| 5 | metrics: `cycle_seconds{task=merge}`, `last_success_timestamp{task=merge}`, `merges_total{outcome}`, `scopes{state}`, `conflicts_open`, `oldest_unmerged_scope_age_seconds` | pending |
| 6 | the gate's merge cases: registration between snapshot and commit in both states, lease takeover between statements, a crash after the put adopted and merged with its duplicate records once | pending |

## Owner decisions

To settle before this plan is `active`:

1. Downloads with `storage.presign` off need a download-token key, a key purpose of the phase 3 `keys` module (`masking-keys.md` §9):
   phase 2 serves presigned URLs only and the `api` role refuses to start with presigning off or on a store without it,
   or it brings the download-token purpose forward with a file provider.
2. `ListDevices` is in `api.md` §10.1 but not in the roadmap row: in phase 2.5, or with the people's pages in phase 4.
3. Applying the catalog's migrations from the binary, left to phase 2 by phase 1: in phase 2.7, or after the first release, the Atlas CLI staying `tracekeep migrate`'s engine (`catalog.md` §7.4).
4. `object adopt` has no defined semantics: dropped from `operations.md` §10.4, or kept until a proposal specifies it.
