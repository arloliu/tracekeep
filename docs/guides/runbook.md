# Runbook

Status: current (2026-10-31) — every alert the Helm chart ships, key, masking and sign-in incidents, refused restores and digests, the pages

One section per named incident of `docs/specs/tracekeep/operations.md` §10.4:
the alert that raises it, the metric to look at, the command that resolves it, and what is lost if it is ignored.
Read when an alert names an incident; each alert the Helm chart ships links its section here as its `runbook_url`.

| Incident | Metric | Recovery | Terminal alternative |
|---|---|---|---|
| `key-binding-mismatch` | `key_resolutions_total{outcome="refused"}` | restore the bound material, then `key verify` | stop using the key id: rotate to a new one |
| `key-refused` | `key_resolutions_total{outcome="refused"}`, `key_provider_errors_total{reason}` | fix the declaration or restore the bound version | rotate to a new key id; `capture terminate` for deferred records that need the old one |
| `merge-over-budget` | `merges_total{outcome="refused"}`, `scopes{state="unmergeable"}` | lower the policy's masking density for new captures; an hour already refused merges by `scope merge <capture> <hour>` only once a release can merge it within the budget | leave it: the hour's segments stay readable and retention evicts it on time |
| `cannot-screen` | `cannot_screen_total`, `deferred_messages{reason="cannot-screen"}` | fix the producer or the policy, then let the redelivery retry | `capture terminate` for the records masking cannot screen |
| `incompatible-configuration` | `deferred_messages{reason="incompatible-configuration"}` | stage and activate a masking configuration; upgrade a member that cannot execute it | `capture terminate` for the capture that waits |
| `parked` (`TracekeepDeferredOld`) | `deferred_oldest_age_seconds{reason}`, `deferred list` | the section of the deferral's reason | `capture terminate` for the capture that waits |
| `backlog` (`TracekeepBacklogHigh`) | `stream_backlog_bytes`, `consumer_ack_pending`, `put_seconds` | restore ingest's throughput: replicas, the store, the catalog | none: past the stream's limit, producers drop |
| `lease-lost` (`TracekeepCompactLeaseLost`) | `compact_lease_held` | bring a `compact` replica up and ready | none: merges, closes, deletions and retention wait |
| `merge-stalled` (`TracekeepMergeStalled`) | `compact_last_success_timestamp{task="merge"}`, `cycle_seconds` | fix what fails the merge cycle (its log line) | none: segments accumulate unmerged |
| `gaps-and-losses` (`TracekeepGapsGrowing`) | `open_gaps{tool_id}`, `declared_losses_total{tool_id}` | find the producer's or the bus's fault for the tool | the gaps stay, said by every read |
| `keys-unavailable` (`TracekeepKeysUnavailable`) | `key_resolutions_total{outcome="unavailable"}`, `key_provider_errors_total{reason}` | restore the key provider's reachability or credentials | records that need an uncached key stay deferred |
| `tool-silent` (`TracepubToolSilent`) | the gateway's `records_published_total`, `bus_connected` | check the tool's traffic and the gateway's capture | none |
| `producer-drops` (`TracepubDropping`) | the gateway's `records_dropped_total{reason}` | remove the drop's reason at the gateway or the bus | the dropped seqs stay gaps |

## key-binding-mismatch

**Alert**: a key id's material no longer matches its binding's fingerprint, or a key id is used under another kind than it was bound with.
Every use of that key id fails closed: ingest defers the records that need it, and the API refuses the operations that need it.
Nothing is ever written with the wrong key.

1. Run `tracekeep admin key verify <key id>`.
   It reads the material at the binding's reference and version, past every cache, and prints both fingerprints and whether they match.
2. If they differ, the material at the bound reference and version was changed:
   for `file`, the file was replaced; for `vault`, the version's material cannot change, so look for a changed declaration or mount.
   Put the bound material back, then run `key verify` again until it matches.
   A running process recovers by itself at its next use; nothing was cached by the failure.
3. If they match, the mismatch is of kind: a configuration names the key id under another purpose.
   Fix the declaration or the policy; a key id keeps the purpose it was first bound with.

**If ignored**: records needing the key id stay deferred until they park, and then until the operator terminates them;
`Restore` and `Digest` under the key id stay refused.
If the bound material is lost for good, rotate to a new key id for new captures;
the captures bound to the old one can no longer be masked or restored.

## parked

**Alert** `TracekeepDeferredOld`: a message has been deferred for more than an hour, for any reason (`ingest.md` §6.1, `parking.md`).

1. `tracekeep admin deferred list` shows what waits, by capture and reason, with each group's count and oldest parking time;
   `deferred_messages{reason}` and `deferred_oldest_age_seconds{reason}` say the same by reason.
2. Resolve by the reason's own section: `cannot-screen`, `incompatible-configuration`, `key-refused` or `keys-unavailable` above and below;
   an `unknown-capture` waits for its capture's start, which the producer sends at its next rotation;
   a `write-failure` is the store's or the catalog's: their errors are in ingest's log.
3. The waiting messages are redelivered every `parking.redelivery_delay` and resolve at their next delivery once the cause is gone.

**If ignored**: the messages stay parked, their seqs gaps in their captures, until an operator terminates them with `capture terminate`, which declares them lost;
retention ends nothing that is parked.

## backlog

**Alert** `TracekeepBacklogHigh`: the traffic stream holds more bytes than the threshold (default 10 GiB) for 15 minutes: ingest takes less than producers publish.

1. Check ingest's replicas are up and ready, and its log for failing flushes.
2. `consumer_ack_pending` near `ingest.max_ack_pending` means the replicas hold all they may: add replicas.
   A slow `put_seconds` or `registration_seconds` is the store's or the catalog's: fix that first, since more replicas only press on it.
3. A backlog that drains once fixed needs nothing more; the messages are acknowledged as their batches flush.

**If ignored**: past the stream's own limits, the bus refuses publications, and producers drop their records (`producer-drops`), which become gaps.

## lease-lost

**Alert** `TracekeepCompactLeaseLost`: no `compact` replica holds the lease for five minutes (`compactor.md` §8).

1. Check the `compact` Deployment's pod, or the standalone unit, is running and ready; its log says why it does not hold the lease.
2. A catalog the replica cannot reach, or a clock far from the catalog's, keeps it from renewing: restore either.
3. A second replica waits as a standby and takes the lease once the first's lapses (`compact.lease_ttl`).

**If ignored**: nothing merges, closes captures, deletes merged segments or applies retention, so storage grows;
reads and ingest go on.

## merge-stalled

**Alert** `TracekeepMergeStalled`: the lease holder's merge task has completed no cycle for two hours, or none since it took the lease.
A cycle that merged nothing counts as completed; a scope that stays unmerged shows instead in `oldest_unmerged_scope_age_seconds`.

1. The `compact` log names each failed cycle and why: the store, the catalog, a pack that does not read.
2. `cycle_seconds{task="merge"}` growing toward the interval means the cycle runs but takes too long: a store or catalog slowed down, or a backlog of scopes after an outage.
3. A pack that does not read is an input to set aside: `tracekeep admin scope exclude-input <pack>` once it is investigated.

**If ignored**: segments accumulate unmerged; reads open more packs per hour, and retention still evicts on time.

## merge-over-budget

**Alert** `TracekeepMergeRefused`: a merge was refused because its inputs' metadata passes the reader's budget (T-45); the scope is left `unmergeable`.

1. `scopes{state="unmergeable"}` counts the scopes refused; `tracekeep admin capture show <capture>` names a capture's.
2. The hour's segments stay readable as they are. Lower the policy's masking density for new captures, which keeps their hours under the budget.
3. An hour already refused merges by `tracekeep admin scope merge <capture> <hour>` only once a release can merge it within the budget.

**If ignored**: the hour stays unmerged and readable, and retention evicts it on time.

## gaps-and-losses

**Alert** `TracekeepGapsGrowing`: on a tool, the holes in its open captures (`open_gaps`) grew over 30 minutes, or ingest declared seqs lost (`declared_losses_total`).

1. `tracekeep status` and the browser's completeness for the tool show which captures, hours and seqs.
2. Gaps come from the producer: its drops (`producer-drops`) or a bus it could not reach; check the gateway's metrics for the tool.
3. Declared losses are records that arrived past retention, or that an operator terminated: the audit rows say which.

**If ignored**: the gaps stay in their captures, and every read and export of them says so.

## keys-unavailable

**Alert** `TracekeepKeysUnavailable`: key resolutions have failed as unavailable for five minutes: the key provider does not answer, or refuses the service's credentials.

1. `key_provider_errors_total{reason}` says how: `unreachable` or `deadline` (the provider's address or network), `auth` (the Kubernetes role or AppRole, an expired secret id), `server`.
2. `key_credential_expiry_timestamp{provider}` shows when the current token expires.
3. Restore the provider's reachability or the credentials; resolutions resume with no restart.

**If ignored**: keys already resolved keep serving; a record that needs an unresolved key stays deferred, and an `api` replica started meanwhile does not become ready.

## key-refused

**Alert**: a key id cannot be used for a reason that a retry will not cure:
it is not declared, or not declared with the kind its use needs;
its material is malformed or shorter than 32 bytes;
or its pinned version is missing from the provider.

1. Read the error in the log: it names the key id, its kind, its provider, reference and version, and the reason.
2. For a declaration: declare the key id with its kind and restart.
   For malformed or short material: the material at the bound version cannot be fixed in place; rotate to a new key id.
3. For a missing version in Vault: a deleted version is undeleted with `vault kv undelete -versions=<n> secret/<path>`,
   after which the next use resolves.
   A destroyed version is gone.

**If ignored, or if the version was destroyed**: the records that need the key id stay deferred and park;
terminate them with `capture terminate` once nothing can bring the material back, which declares them lost.
Packs already written under the key id stay readable masked, but their originals can no longer be restored,
and for a `digest` key id, `Digest` under it is refused.

## cannot-screen

**Alert**: ingest cannot mask a record before writing it: a data record whose message text nests lists deeper than the masking walker's limit,
an annotation whose payload does not decode while the policy has annotation rules, or a record that already arrives marked redacted.
The record is never written unscreened: it is nak'd and redelivered, and the capture's later records are written around it.

1. The alert's log line names the message (`msg_id`, the capture and seq) and why masking failed;
   `cannot_screen_total` and `deferred_messages{reason="cannot-screen"}` count the records waiting.
   These records are nak'd and redelivered, not parked, so `deferred list` does not list them.
2. A malformed record is the producer's: fix it, and the redelivery is masked.
   A rule the policy should not apply to that message is the policy's: stage a corrected policy version and activate it for new captures.

**If ignored**: the record stays deferred, and its seq a gap in the capture, until an operator terminates it with `capture terminate`, which declares it lost.

## incompatible-configuration

**Alert**: a record cannot be processed under a configuration this replica executes,
or, outside `tracekeep dev`, a capture's start finds no masking configuration it may bind.

1. `tracekeep admin config list` shows each configuration's state.
2. With no masking configuration active, stage one and activate it (`docs/guides/production-admission.md`);
   the waiting starts are admitted at their next delivery.
3. With one active but not bindable, a leased member does not execute it: upgrade or stop that member; it becomes bindable once the member lapses or leaves.

**If ignored**: the capture's records wait on the stream, bounded by the capture, until a configuration binds or an operator terminates the capture.

## People cannot sign in

This raises no alert of its own: `logins_total{outcome}` and `auth_failures_total{reason}` say why.
Neither a log line nor the error page holds a code, a token, a secret or a cookie.

| Signal | Meaning and action |
|---|---|
| `logins_total{outcome="unavailable"}` (a login started or a callback), reason `idp-unavailable` | the IdP's discovery or keys cannot be fetched: people already signed in keep working until their session expires, new logins fail; restore the IdP's reachability |
| reason `exchange` | the token endpoint refused the code or did not answer within `auth.people.exchange_timeout`: check the client id and secret, the redirect URL registered at the IdP (`<api.public_url>/auth/callback`) and the IdP's latency |
| reason `login` | a callback without its login cookie or with another state: a login left open past `auth.people.login_lifetime`, a callback replayed, or a browser refusing cookies; a person sees it once and signs in again |
| reason `nonce`, `audience`, `issuer` | the ID token is not for this login or client: check `auth.people.client_id` and the IdP's client |
| reason `session` | a session cookie past its expiry or under a dropped key id: the person signs in again; many at once after a rotation mean the old key id was dropped too soon |
| reason `csrf`, `session-scope` | a cookie-borne request from another origin or on an operation the browser does not use: a misconfigured `api.public_url` or a page that should not be calling |
| a person signed in and refused every operation (`forbidden`) | the ID token's `auth.people.roles_claim` holds no value `auth.people.roles` maps: map the person's group |

## The pages

The pages raise no alert; what they show says why (`api.md` §10.3).

| Signal | Meaning and action |
|---|---|
| a page saying the frontend was not built | the binary was built without `-tags webdist`: build it with `make build`, as releases are |
| the sign-in page again while a page was open | the session expired or its key id was dropped: sign in again; see *People cannot sign in* |
| "Your roles do not admit this" where a result would be | the person's roles lack `viewer`, or `exporter` for exports: map their group in `auth.people.roles` |
| a record list marked interrupted with `unavailable` | the store or the catalog failed mid-stream: the items that came stay, and *Continue* reads on from the last batch |
| a capture's start saying its completeness was cut to fit | the completeness is longer than a stream message: the browser's link lists it whole by `ListPacks` |

## Refused restores and digests

These raise no alert: the caller gets a Connect code, and `restores_total{outcome}` and `digests_total{outcome}` count them.
Neither the error nor the log line holds a byte of the record or an original.

| Outcome | Code | Meaning and action |
|---|---|---|
| `not-found` | `not_found` | the named hour holds no copy of the record, or the capture is unknown: name the hour the record's time falls in (`ListPacks`) |
| `conflict` | `failed_precondition`, detail `RecordConflict` | the copies of the record differ, in some hour: no version is restored, an acknowledged conflict included; investigate the conflict as `ListPacks` reports it |
| `not-restorable` | `failed_precondition` | an annotation, an unobserved or unmasked record, a pack without encryption, a pack naming another encryption key id than its capture's configuration, or a record that does not authenticate; the last two mean the stored pack is not what was written: investigate the store |
| `key-refused` | `failed_precondition` | the key the request needs is refused: `key-binding-mismatch` or `key-refused` above |
| `unavailable` | `unavailable` | the key provider, the catalog or a copy's object could not be read: the client retries; a copy that stays unreadable is the store's |
| `audit-failed` | `internal` | the audit row could not be written, so nothing was returned: the catalog's write path |
| `rate-limited` (`Digest`) | `resource_exhausted` | the principal's inputs a second passed `api.digest_rate`, or the request is larger than the bucket left: the scorer backs off; raise the limit in the configuration if the load is expected |
| `invalid` | `invalid_argument` | a malformed request, or a `Digest` input whose kind and domain are no rule of the key id's configurations: the error names the input's index |

## Record reads refused or incomplete

These raise no alert: the viewer's page shows the refusal or the incomplete part,
and `record_reads_refused_total`, `record_reads_incomplete_total{reason}` and `record_reads_in_flight` count them (`api.md` §10.1 *Records*).
A stream holds masked records only, so neither the error nor the log line holds a clear byte.

| Signal | Meaning and action |
|---|---|
| `resource_exhausted` on `QueryRecords` or `FindTransaction`, `record_reads_refused_total` rising | the replica serves `api.records.max_reads` reads already: the person retries; add `api` replicas, or raise the bound if their memory allows (each read holds up to `held_bytes` and its packs' windows and metadata) |
| `resource_exhausted` on `FindTransaction` naming an hour | the hour holds more packs than `api.records.max_lookup_packs`: an hour left unmergeable (T-45); merge it with `scope merge` once its cause is fixed, or raise the bound |
| reason `too-many-packs` | a cluster of overlapping packs above `api.records.max_readers` was not read: the same unmergeable hour, or many redeliveries overlapping in seq |
| reason `gone`, `evicted` | a pack's object vanished during the read and stayed gone after one re-read, or retention evicted the hour: re-query; a `gone` that repeats is the store's |
| reason `corrupt`, `crash-gap`, `footer` | tracepack found a damaged block, a crash gap or an unusable footer: the records it could read were streamed; investigate the pack |
| reason `read-limit` | a read passed tracepack's budget, `held_bytes` or its conflict bound: raise `api.records.held_bytes` if the replica's memory allows |
| a capture shows `withheld` | its configuration has no policy, so it was admitted before the cutover to masking (T-48): its records are never shown to people |

## Export jobs

These raise no alert: `exports_total{outcome}`, `export_jobs{state}` and `export_cleanup_failures_total` count them (`api.md` §10.1 *Exports*).
No error, reason, manifest or log line names an object key; a cleanup failure is logged by job and fence.

| Signal | Meaning and action |
|---|---|
| `resource_exhausted` on `StartExport` | the selection passes `exports.max_packs` or `exports.max_bytes`: a narrower window or fewer devices |
| a job `failed`, its reason naming a bound | the read sets grew past a bound while it ran, or its JSONL passed `exports.max_bundle_bytes`: narrow it, or raise the bound if the replica's disk allows |
| a job `failed`, "its attempts are exhausted" | `exports.max_attempts` attempts each ended without completing: the `api` log's `exports: attempt ended` lines say why (the store, the catalog, a lost lease) |
| `export_jobs{state="running"}` steady and no job completing | attempts losing their lease: replicas overloaded, or a catalog clock far from theirs; `exports.max_running` and the lease |
| `export_cleanup_failures_total` rising | the store refuses deletions under `exports/`: the objects stay, their jobs too, retried every cycle; fix the store's permissions |
| a done job answered `expired` | its bundle's deadline is too near for a URL: start it again |

## tool-silent

**Alert** `TracepubToolSilent`, from the gateway's metrics: a producer connected to the bus has published no record for ten minutes.

1. A tool that sends nothing, idle or down, publishes nothing: check the tool's own state first.
2. Otherwise the gateway's capture stopped: its log and `capture_rotations_total{reason}` say whether it faulted or was stopped.

**If ignored**: nothing is lost that was sent; a capture that stopped records nothing until it starts again.

## producer-drops

**Alert** `TracepubDropping`, from the gateway's metrics: a producer is dropping records (`records_dropped_total{reason}`), each a seq its capture will lack.

1. `lane_full`: the producer's ring is full, the bus taking less than it publishes: the bus's latency (`publish_ack_seconds`) or the ring's size.
2. `bus_down`, `publish_failed`, `claim_failed`: the bus or its claim bucket refuse or do not answer; past the stream's limits, see `backlog`.
3. `capture_fault`, `after_stop`: the capture ended while records were in flight; a fault's reason is in the gateway's log.

**If ignored**: every dropped record is a gap in its capture (`gaps-and-losses`).
