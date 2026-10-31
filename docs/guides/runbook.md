# Runbook

Status: current (2026-10-30) — key, masking and sign-in incidents, refused restores and digests, merges over budget; the others join with their alerts

One section per named incident of `docs/specs/tracekeep/operations.md` §10.4:
the alert that raises it, the metric to look at, the command that resolves it, and what is lost if it is ignored.
Read when an alert names an incident.

| Incident | Metric | Recovery | Terminal alternative |
|---|---|---|---|
| `key-binding-mismatch` | `key_resolutions_total{outcome="refused"}` | restore the bound material, then `key verify` | stop using the key id: rotate to a new one |
| `key-refused` | `key_resolutions_total{outcome="refused"}`, `key_provider_errors_total{reason}` | fix the declaration or restore the bound version | rotate to a new key id; `capture terminate` for deferred records that need the old one |
| `merge-over-budget` | `merges_total{outcome="refused"}`, `scopes{state="unmergeable"}` | lower the policy's masking density for new captures; an hour already refused merges by `scope merge <capture> <hour>` only once a release can merge it within the budget | leave it: the hour's segments stay readable and retention evicts it on time |
| `cannot-screen` | `cannot_screen_total`, `deferred_messages{reason="cannot-screen"}` | fix the producer or the policy, then let the redelivery retry | `capture terminate` for the records masking cannot screen |
| `incompatible-configuration` | `deferred_messages{reason="incompatible-configuration"}` | stage and activate a masking configuration; upgrade a member that cannot execute it | `capture terminate` for the capture that waits |

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
