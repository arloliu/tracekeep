# tracekeep — operations

Status: draft (2026-10-25) — split from seed v21

Covers the admin commands, the runbook's shape, the tunables, metrics, health and logs;
the source the runbook and configuration guides are written from.
From seed v21 §10.4; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before adding an admin command, an alert, a metric or a tunable, or writing `docs/guides/runbook.md` or `docs/guides/configuration.md`.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Admin commands**: `tracekeep admin …`, every one audited (§10.4 table).
- **Runbook**: one section per named incident, each with its alert, metric, recovery command and terminal alternative, shipped as `docs/guides/runbook.md`.
- **Tunables**: one table in `docs/guides/configuration.md`, with defaults and the consequence of moving each;
  the defaults are T-12 in `tracekeep-decisions.md`.
- **Metrics**: `tracekeep_` and `tracepub_` prefixes;
  `tool_id` is a label, `capture_id` never is, except on one info gauge per producer.
- **Health**: `/healthz`, `/readyz`, and `tracekeep status` per tool.
- **Logs**: one structured line per state change, never one per record.

### 10.4 Operations: admin commands, runbook, metrics

**Admin commands** (`tracekeep admin …`, every one audited):

| Command | For |
|---|---|
| `capture terminate <id>` | the operator action of G5-251 and of every `open incident` row of §6.1: terminates the capture's deferred and parked messages, writes every unresolved parked version `terminated`, the declared losses, the audit row and a **termination order** with the traffic stream's `LastSeq` as cut-off for originals never parked (§6.1 *operator termination*); the acknowledgements follow at each message's next delivery; for a `pending` capture it ends the messages and leaves the placeholder row and its version resolutions in place; audited |
| `capture close <id>` | force a close intent now instead of waiting for I; a request the compactor executes at its next cycle (T-41) |
| `scope merge <capture> <hour>` | re-run a merge after an operator resolved its inputs, an `unmergeable` scope's included; a request the compactor executes at its next cycle (T-41, T-45) |
| `conflict acknowledge <capture> <seq>` | closes the incident; the scope stays unmerged for its lifetime (readers take the union and report the conflict, exports flag it, retention evicts it on time); a merge over differing versions is impossible by [STO §4], so no command promises one |
| `scope exclude-input <pack>` | sets the pack `excluded` under the capture-row serialization, bumps the scope's revision and emits an event, so the next merge snapshot (which takes `staged` packs only) leaves it out; it stays readable and is never deleted by the merge relation; audited |
| `policy list` | the stored redaction policies (`masking-keys.md` §9): id, version, hash, rule counts and when each was stored |
| `policy show <policy_id> <version>` | a stored policy's canonical form |
| `config stage <digest key id> <encryption key id> [--policy <file>]` | stores the policy (the embedded baseline without `--policy`), resolves both key ids, which binds them, both required since a pack masked at write encrypts, and stages the configuration of this binary's classifier and frame limit with the policy and the key ids' fingerprints (`rollouts.md` §6.4); the file is read by the process that runs the command; refused outside `dev` for a policy lacking a baseline rule, for another body under a stored id and version, and for a key id undeclared, declared with another kind or not matching its binding; audited |
| `config list` | every processing configuration: id, state (staged, active, retired), classifier, frame limit, policy and key ids |
| `config activate <config_id>` | activates a staged processing configuration under the membership serialization (`rollouts.md` §6.4); nothing else, neither a start nor loading it, activates one; an active one is unchanged; refused for an unknown or retired one, and, in a release whose ingest does not mask, for one naming a policy; audited |
| `key verify <key_id>` | re-resolves a key and compares its fingerprint with the binding, for the `key-binding-mismatch` incident |
| `parked list` / `parked terminate <capture>` | the parking stream (§6.1): `list` shows each version's state, `evicted-hour` included, its messages with their states and its survivor object; `terminate` writes the version `terminated` with the audit row; the messages are acknowledged under it at their next delivery (G5-269) |
| `bus adopt-stream <role>` | the last step of the stop-the-world replacement procedure of §6.1: allocates the new incarnation, marks the retired one's copy rows `lost` and its termination orders `spent`; refuses to run while any tracekeep lease is live; audited |
| `capture show <id>` | the capture's versions, copies, survivors and termination orders with their states |
| `object list --unattributed` / `object list --survivors` | the objects the reconciler leaves to the operator: those without the SDK's metadata, and survivors with their evicted hour and bytes |
| `config list --blocking-retirement` | the captures that keep a configuration alive (§6.4) |
| `deferred list [--capture]` | what is waiting, since when, why: the unresolved parked versions (`parking`, `parked`, `evicted-hour`) by capture and reason, each group's count and oldest parking time; an indefinite deferral is parked (`ingest.md` §6.1), and a short delay that never parks lives only in ingest's metrics |

**Runbook**: one section per named incident
(`capture-rejected`, `descriptor-conflict`, `claim-conflict`, `write-failure`, `incompatible-configuration`, `seq-conflict`, `beyond-retention`, `capture_fault`, `key-binding-mismatch`, `key-refused`, `cannot-screen`, `damaged-input`, `merge-over-budget`, `parked`, `bus-incarnation-changed`),
each row of a compact table incident → metric → recovery command → terminal alternative
(for example `cannot-screen`: `cannot_screen_total` → fix the consumer or the policy, then let the parked consumer retry after `parking.redelivery_delay` → `capture terminate`),
each with the alert that raises it, the metric to look at, the admin command that resolves it, and what is lost if it is ignored;
shipped in `docs/guides/runbook.md` and referenced by the alert annotations.

**Tunables**, all in one table in `docs/guides/configuration.md` with their defaults and the consequence of moving each:
F, Q, I, L, D, H, τ, ε, the lane sizes, the key-set cap, the send path's reporting grace (default 5 s) and unawaited-entry bound (default 30 s, T-31), `parking.enabled` (default true; false documents the service-wide stall it allows)
`storage.presign` (default on, T-33), the download URL's lifetime (default 15 min, T-41), margin (default 2 min) and floor (default 1 min), an export bundle's retention (default 24 h) and the export job's bounds, lease and put timeout (`exports.*`, `api.md` §10.1 *Exports*),
people's session lifetime (default 8 h), login lifetime (default 10 min) and token exchange deadline (default 10 s, `api.md` §10.2),
and `parking.redelivery_delay` (default 60 s, the parked consumer's scheduled nak delay;
how soon a termination or a cleared blocker takes effect follows from it only while that consumer is up and has delivery capacity,
and the completion signal is `parked list`, §6.1) (§13 lists the defaults).

**Metrics** (Prometheus names, `tracekeep_` and `tracepub_` prefixes;
`tool_id` is a label where it is bounded, about 1,500 values;
`capture_id` is never a label except on one info gauge per producer;
everything per capture is served by `tracekeep status` and the admin commands, not by metrics):

| Component | Metrics |
|---|---|
| producer (SDK, through the `Metrics` interface the producer wires to its own registry; `current_capture` is one series per producer: the SDK deletes the previous label set when it rotates) | `records_observed_total{kind,dir}`, `records_published_total`, `records_dropped_total{reason=lane_full,bus_down,publish_failed,claim_failed,capture_fault,after_stop}`, `dropped_bytes_total`, `ring_depth`, `ring_bytes`, `reserved_lane_depth`, `publish_ack_seconds` (histogram), `bus_connected`, `capture_rotations_total{reason=start,hotreload,fault,stop,seq_limit}`, `clock_steps_total`, `claim_puts_total{outcome}`, `pending_requests`, `pending_reports_ignored_total{reason=duplicate_progress,duplicate_awaiting,sealed,removed,expired}`, `pending_timeouts_dropped_total{reason=unbound,ambiguous,unattributed}`, `thinned_total`, `thinning_header_mismatches_total` (a registration whose sent frame had another stream or function, `producer.md` §4.6 *Header check*), `ambiguous_total`, `thinning_enabled` (1 while the current capture has a sampling rule and no registration has found the ambiguity set full), `current_capture` (info gauge: `capture_id`, `tool_id`, `recorder_instance_id`), `preflight_ok` |
| ingest | `messages_total{disposition}`, `deferred_messages{reason}`, `deferred_oldest_age_seconds{reason}`, `quarantined_total`, `segments_total{tool_id}`, `segment_bytes_total{tool_id}`, `put_seconds`, `registration_seconds`, `mask_seconds`, `in_progress_acks_total`, `captures_open{tool_id}`, `conflicts_total{tool_id}`, `gap_observations_total{tool_id}` (a hole first seen at registration; it may be filled later, so it is an event count, not a loss count), `open_gaps{tool_id}` (gauge: holes currently unfilled), `declared_losses_total{tool_id}`, `captures_with_unknown_tail{tool_id}` (gauge), `parked_messages{reason}`, `key_binding_mismatches_total`, `cannot_screen_total`, `damaged_inputs`, `stream_backlog_messages`, `stream_backlog_bytes`, `consumer_ack_pending`, `redelivered_total` |
| compact | `cycle_seconds{task=merge,close,delete,retention,reconcile,exports}`, `last_success_timestamp{task}`, `lease_held`, `scopes{state}`, `merges_total{outcome=merged,stale,conflicted,failed,refused}` (`refused` a merge over the reader's metadata budget, which withholds no task's success), `closes_total{trigger}`, `deletions_total`, `tombstones_total`, `orphans_adopted_total`, `orphans_quarantined_total`, `claim_objects`, `claim_object_bytes`, `conflicts_open`, `oldest_unmerged_scope_age_seconds` (of `open` and `reopened` scopes), `retirement_blockers`, `export_jobs{state}`, `export_objects_deleted_total`, `export_cleanup_failures_total` |
| api | request rate, errors and duration per operation, a stream's counted at its end with its code; `record_reads_in_flight`, `record_reads_refused_total` (`resource_exhausted`), `records_streamed_total`, `record_reads_incomplete_total{reason=gone,corrupt,crash-gap,footer,read-limit,too-many-packs,evicted}`; `exports_total{outcome=done,failed,cancelled}`, `export_bytes_total`; `restores_total{outcome}`, `digests_total{outcome}`, `digest_inputs_total`; `auth_failures_total{reason}`, `logins_total{step=start,callback,outcome=ok,refused,unavailable}`; `download_urls_total{kind=presigned,api}`, `download_urls_refused_total` (`pack-superseded`) |
| keys (every role that resolves a key; `kind` the only per-key label, never a key id, a reference or a credential) | `key_resolutions_total{kind,outcome=cached,resolved,unavailable,refused,cancelled}`, `key_provider_seconds{provider,op=login,renew,read}`, `key_provider_errors_total{provider,reason=unreachable,deadline,auth,missing,malformed,server}`, `key_credential_expiry_timestamp{provider}` (the current token's expiry, for a provider with credentials) |
| catalog | pool usage, transaction retries (`busy`, `deadlock`), migration version |

Health: `/healthz` (process) and `/readyz` (catalog reachable, lease state for `compact`, consumer bound for `ingest`);
`tracekeep status` lists `pending` captures by capture id first, outside any per-tool grouping
(they have no tool yet), then prints per tool the last record time, open captures, deferred messages and backlog.
Deferred messages are the tool's unresolved parked versions, as `deferred list` counts them;
backlog is the traffic stream's messages on the tool's subjects, whose token after the prefix is the tool's (tracepack's `bus.Subject`),
and the stream's bytes, which no subject divides, once for all tools.
The Helm chart ships alert rules:
deferred age above an hour, backlog bytes above a threshold, a tool with a connected gateway and no records for ten minutes, producer drop rate, lease lost, no successful merge for two hours, a merge refused over the metadata budget, `open_gaps` or `declared_losses_total` increasing on a tool, `preflight_ok` false, a key refused, keys unavailable for five minutes.
Logs are structured, one line per state change with `capture_id`, `tool_id` and `seq` where they apply, never one line per record.
