# Phase 3 — masking and keys

Status: active (2026-10-31) — phase 3.6 in progress

Milestone 3 of the [roadmap](../ROADMAP.md):
the redaction policy bound to each capture and applied before anything is written,
the keys it needs and their bindings, `Restore` and `Digest`, and thinning in the producer SDK.
Its scope and gate are the roadmap's phase 3 row;
the design is `docs/specs/tracekeep/masking-keys.md` §9, `ingest.md` §6.2, `rollouts.md` §6.4, `compactor.md` §8.2, `api.md` §10.1–§10.2, `operations.md` §10.4, `producer.md` §4.6 and `producer-sdk.md`.
The owner's answers to the points it raised before starting are T-42.
This plan only orders the work.

## Settled before it started

The owner's answers, recorded as T-42; the phases below cite them by number.

1. **Thinning takes its provenance from a per-request origin, not a channel** (answered 2026-10-29):
   a dedicated interval-only channel (`fdc_poll-eqp`, `producer.md` §4.6) changes the production environment too much.
   Instead `fdc_grpc`'s `buildRequest` marks a request interval-only when every context it consolidates is a kernel DC task,
   and the mark travels on the hub's message to `eqp_hsms`'s send path,
   where the SDK's pending-table registration takes it in place of the channel;
   a request holding a proxy context, and everything not built by `buildRequest` (drain resends, post-event and MES commands), is unmarked and never thinned.
   The SDK thins only a marked `S1F3` and its reply.
   A lost mark costs a thin, never a wrong one.
   **Every kernel DC task counts as interval polling**, event-started and timer-started alike, since each is sampled at its `SampleFreq`;
   this answers T-3's open question.
   **Parallel mode never thins**: the legacy host's polls carry no provenance and never pass `buildRequest`,
   and only the tool-facing session's traffic is the tool's.
   The eqp-hub side (the origin on `DCTask` and on the hub's message, the plugin protocol's field) is phase G's;
   until it ships, nothing is marked and nothing thins.
2. **Exports are checked in phase 4** (answered 2026-10-29):
   `Export` is phase 4's, so phase 3's gate checks storage, the working directories, the compactor's outputs, logs, audit rows and error responses,
   and phase 4's gate gains "no clear recipe body in an export".
3. **`Restore` addresses a record by `(capture, seq, hour)`** (answered 2026-10-29), the addressing `FindTransaction` uses.
4. **The `session` key purpose waits for phase 4** (answered 2026-10-29):
   it signs a cookie only phase 4's people's OIDC issues; phase 3 adds `digest` and `master`.
5. **Vault in tests** (answered 2026-10-29):
   a Vault dev-mode container in CI beside MinIO and Ceph RGW, through a make target like `make test-catalog`,
   with an `httptest` fake for the failures a real server will not produce on demand.
6. **The production-admission check of a tool type** (T-6, answered 2026-10-29):
   an operator procedure in `docs/guides/`, not code;
   the service refuses to start outside `tracekeep dev` with a policy that lacks any baseline row.
7. **The split below** into six sub-phases (answered 2026-10-29).
8. **The cutover to masking** (answered 2026-10-29):
   outside `tracekeep dev`, a capture is admitted only under a configuration whose policy holds every baseline row;
   with none available it is not admitted, and its records wait as `incompatible-configuration` does.
   A capture admitted before the cutover keeps its configuration without a policy, and its traffic is still processed under it
   (phase 1 ran only on data that holds nothing sensitive).
   A consumer that cannot mask cannot hold a membership outside `tracekeep dev` once a masking configuration is active,
   and a compactor that cannot write a masked capture's close cannot hold the compactor lease.
   An existing capture's binding never changes,
   so the cutover has a prerequisite the guide states and the gate checks:
   before sensitive traffic starts, every producer whose capture predates the cutover is stopped or rotated,
   old leaders that may still publish included,
   and the new captures are confirmed bound to a masking configuration;
   a pre-cutover capture's synthetic backlog still drains under its own configuration.
9. **`Restore` and conflicting versions** (answered 2026-10-29):
   a record is restored only when every non-excluded copy of its `(capture, seq)` is equal, in any hour,
   since a conflict spans hours (`ingest.md` §6.2) and the hour only says where to look;
   differing versions are refused and reported as a conflict, an acknowledged conflict included, and no version selector is offered.

The commit that activated this plan amended:
`producer.md` §4.6 and the roadmap's phase 3 row for points 1 and 2;
T-3's channel clause and T-12's "thinning disabled until the poll channel exists" marked superseded, `positioning.md`'s LS-7 summary no longer pending a channel, and T-3's open item dropped from the roadmap;
`api.md` §10.1 for points 3 and 9 and for the interim admin commands below, and `operations.md` §10.4 for those commands;
the roadmap's phase 4 row for points 2 and 4, and phase G's row for point 1;
`rollouts.md` §6.4 for point 8.
Each sub-phase's spec step then writes the detail its row names.

Not in this phase:
`Export`, `QueryRecords`, `FindTransaction`, people's OIDC, the `session` purpose and the frontend are phase 4's;
typed admin operations for policies and key ids stay phase 4's, the admin commands below serving through `Admin(words)` meanwhile.

## Phases

| Phase | Scope | State |
|---|---|---|
| 3.1 — keys | detailed below: Vault's configuration, credential lifecycle and outage behaviour specified, then the `digest` and `master` purposes, `EncryptionKey`, the `vault` provider, a context-bearing `redact.Keys`, `key verify` and the keys guide | done (#33) |
| 3.2 — policy and configuration | spec first (`rollouts.md` §6.4, `masking-keys.md` §9, `operations.md` §10.4): the configuration's full identity (classifier, frame limit, policy id, version and content hash, both key ids with their fingerprints, format features) and its reconstruction from stored bytes; policy persistence in the existing `policies` table (an incremental migration only if one is needed); a policy in YAML, compiled once; the default policy as the [SEM §8] baseline transcribed (every row, both paths where a row has two, the `source-text` annotation rules), a test holding it to the table; the refusal to start outside `dev` without the baseline; the `policy` admin commands and `config activate <config_id>`, their words, audit rows and refusals, direct and through `Admin(words)`; a configuration's states (staged: stored and loadable, neither required of members nor bindable; active; retired), membership requiring only active and bound ones, a staged one activated by `config activate` alone, under the membership serialization, and never by a start or by `Executable` loading it; the production-admission guide with the cutover prerequisite of point 8. A masking configuration is staged here, never activated | done (#34) |
| 3.3 — masking at ingest | spec first (`ingest.md` §6.2, `compactor.md` §8.2, `rollouts.md` §6.4): the cutover of point 8 (activation of a masking configuration waiting for, or ending, the memberships that cannot execute it; admission refusing a configuration without a policy; a fence at the compactor lease that an old binary cannot pass: 3.3's migration moves the lease to a row the old binary does not name, so an old compactor can neither take nor renew it and every lease check of its transactions fails, and no policy-less close of a masked capture becomes durable; the same fence at the membership tables for old ingest replicas, T-43; the guide's prerequisite that every compactor and ingest replica is upgraded at that migration); classify and `Mask` before the `Writer` is built, `PackMeta.Redaction` and the capture's `redaction_policy` set; claim-check bodies masked like inline ones; a key failure deferring every record that needs the key, with its alert; a close's metadata (policy, classifier, frame limit) taken from the capture's bound configuration before its intent is persisted, intents already persisted replaying their bytes; raw delivery identity (receipts, termination evidence) kept on the original bytes; the cutover gate (a capture spanning the cutover, an old leader publishing after its successor, an old compactor holding the lease when the migration lands and an old standby trying to take it after, an upgraded compactor then closing a masked capture with its bound metadata, the migration against a compactor transaction held before and after its lease check, on both engines) | done (#35) |
| 3.4 — `Restore` and `Digest` | spec first (`api.md` §10.1, `masking-keys.md` §9): the record locator of points 3 and 9 (every non-excluded version of the seq compared across hours from one catalog snapshot, record bytes and redaction entries alike; equal copies once; differing versions refused; a candidate that cannot be read refusing the restore; a pack merged, deleted or registered during the lookup); the hour required and naming an hour that holds a copy, otherwise not found, while the conflict check spans every hour whichever hour is named; the whole record authenticated before any original leaves; annotation masks, unobserved records and records without encryption answered as not restorable; the audit row written first, a failed audit returning nothing; `Digest` over §9's three input forms with `kind` checked by the service, its rate limit's scope and bounds; the proto operations wired to the existing `restorer` and `scorer` roles | done (#36) |
| 3.5 — thinning | spec first (`producer-sdk.md`, `producer.md` §4.6): `Register` taking the origin and the primary's stream and function beside the session and System Bytes, an old caller's registration unmarked, a supplied header that disagrees with the observed one never thinned, every registration in the ambiguity set whatever its mark; the sampling rule's key and parameters (defaults 60 and 60 s) owned by the SDK's configuration; the descriptor's `thinning` entries generated by the SDK from that configuration, never taken from the caller; then the thin decision, the ambiguity set and its cap, the thinned record zero-filled with `quality.unobserved`, rotation on a parameter change, `cmd/tracekeep-sim` driving it, its metrics | done (#39, #41) |
| 3.6 — gate | the roadmap row's cases and the strengthened ones below, audited against the tests as 2.8 did; the thinned records' path through 3.3's masking and the merge | in-progress |

Only the phase under way and the next are detailed; the others are detailed when they start.
3.5 is the producer module's and may run beside 3.1–3.4; its records meet the service's masking only in 3.6's integration test,
which adds a policy rule matching `S1F3` and `S1F4` (the baseline matches neither) so that a wrong `unobserved` argument to `Mask` shows.
Every catalog step runs on both engines, and every concurrent schedule is barrier-controlled.
Each phase lands the metrics of `operations.md` §10.4 for the code it adds.

**Gate cases and their owners:**

- "two first uses of one key id with different material": 3.1, two real `Keys` instances resolving different material before either binds, on both engines;
  the loser returns, caches and derives nothing.
- "a restore round-trips": 3.4, the restored payload equal to the record as received, and `Digest` equal to the digest ingest wrote for each input form.
- "rotation between two deliveries of one record": 3.3, widened beyond the digest key:
  a digest-key rotation, a master-key rotation and a provider version advanced between the deliveries, each with fresh processes and caches;
  a policy activated between them, the existing capture keeping its configuration while a new capture binds the new one;
  through every crash point of `ingest.md` §6.3, inline and claim-check, a different batching on the retry, an orphan adopted before the redelivery and partial acknowledgements;
  the serialized records and their complete redaction entries compared, then merged without a false conflict.
- "no clear recipe body anywhere": each phase asserts its own boundary and 3.6 audits the coverage.
  3.1: provider errors carrying synthetic secret sentinels never reach an error, a log line or a metric.
  3.3: working-directory files inspected while held at the write and crash barriers, decoded pack payloads and metadata checked for every baseline carrier and the three `source-text` annotation kinds, malformed items' whole-text fallback, `ErrCannotScreen`, a failure after part of a batch was masked, a failed put, an interrupted merge.
  3.4: audit rows hold only an allowlist of fields, never original bytes or credentials; error responses and logs of a failed restore hold no original.
  The traffic and parking streams and the claim bucket stay the permitted clear-text locations (`masking-keys.md` §9).

## Phase 3.1 — keys

Work on the branch `feat/keys/vault`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
`internal/keys` already holds the binding's insert-or-compare, the order that checks before it caches, the `file` and `env` providers and the `download` purpose (2.5);
this phase adds the purposes masking needs and the `vault` provider.

| Step | Scope | State |
|---|---|---|
| 1 | spec, `masking-keys.md` §9 and the configuration guide: the `vault` provider's configuration (address, CA, KV v2 mount, auth method and role), its credential lifecycle (Kubernetes auth and AppRole login, renewal before expiry, a fresh login when renewal fails), a deadline on every provider call; a warm cache serving bound material through an outage and a cold cache failing closed; material requirements for `digest` and `master` keys; old provider versions kept while a binding or a capture needs them; errors that name the key id and reference but never the material or a token; `redact.Keys` given a context per use, since its methods take none; the key metrics (names, bounded labels, cache hits), which `operations.md` §10.4 gains | done (#33) |
| 2 | `digest` and `master` purposes: `DigestKey` returning the bound digest material unchanged, one key across captures (the same key id giving the same key for every capture); `EncryptionKey` deriving 64 bytes from the bound master material by §9's HKDF recipe, checked against independent vectors (the raw UUID byte order, the key id's UTF-8, distinct captures); a request under another purpose failing closed across `download`, `digest` and `master`; a failed resolution caching nothing; the first-use race of two real `Keys` instances on both engines, with different material (the loser gets nothing) and with equal material at different provider versions (one binding wins and a fresh process follows it) | done (#33) |
| 3 | the `vault` provider: KV v2 read at the binding's pinned version, an unbound key id at the declared or current one; Kubernetes auth and AppRole; renewal and re-login; tests against the dev-mode container (a make target and its CI job) and an `httptest` fake for an expired or revoked credential, a deleted pinned version beside a newer one, a malformed or non-base64 response, a stalled server against the deadline, and secret sentinels kept out of errors and logs | done (#33) |
| 4 | the `redact.Keys` adapter: a context bounded by the call deadline, warm and cold caches through a provider outage, a cancelled context returning no material, concurrent uses whose cancellations stay their own | done (#33) |
| 5 | `key verify <key_id>`: re-resolves bypassing the cache and compares the fingerprint with the binding, audited, direct and through `Admin(words)`; the keys guide (declaring a key, rotating `digest` and `master` keys by declaration, Vault incident recovery) and the runbook's `key-binding-mismatch` section | done (#33) |
| 6 | metrics: the key metrics step 1 specifies, provider errors and credentials never a label | done (#33) |

## Phase 3.2 — policy and configuration

Work on the branch `feat/policy/config`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The steps settle four points the row leaves open:

- **A start stages, it never activates.** Today every ingest start inserts and activates the configurations it executes (`ActivateConfig`).
  From this phase a start stores them as staged (insert-or-compare on the content hash, so replicas staging one content share its row),
  `tracekeep dev` activates its own policy-less configuration, and everywhere else only `config activate <config_id>` does.
  A policy-less configuration's stored bytes stay as they are, so a deployment's existing active row is the one a start finds;
  a changed `ingest.max_frame_len` now needs `config activate` before captures bind it.
- **Where a masking configuration comes from.** A policy is the rules, written in YAML, the embedded baseline when no file is given;
  its key ids are not part of the policy's bytes.
  `config stage <digest key id> <encryption key id> [--policy <file>]` stores the policy (insert-or-compare on its id and version by hash, a differing body refused),
  resolves both key ids, which binds them as 3.1's first use, and stages the configuration naming this binary's classifier and frame limit, the policy, and both key ids with their fingerprints.
  The operator stages, not a start, because the fingerprints need the keys resolved and a start never waits on a provider (`masking-keys.md` §9);
  a start reads them back from the catalog's bindings only.
- **Nothing masked can be activated yet.** This phase's `config activate` refuses a configuration that names a policy, since ingest does not mask until 3.3;
  3.3 lifts the refusal with the cutover.
- **Members and staged configurations.** Registration requires every active configuration, never a staged one;
  a member lists every configuration it executes, staged ones included, so an activation is checked against the members (`bindableConfig`);
  a replica loads every active configuration and every one a capture is bound to, refusing to start on one it cannot execute,
  and every staged one it can execute, skipping the others, so an older replica never stops on a newer one's staged configuration.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `rollouts.md` §6.4 (the configuration's content: classifier, frame limit, policy id, version and hash, both key ids with fingerprints, format features, the masker as a record; its reconstruction from the stored bytes and the catalog's bindings; staged, active and retired; a start staging its policy-less configuration, an operator staging a masking one; activation by `config activate` alone, under the membership sentinel; registration on active configurations; members listing what they execute; the refusal of a configuration with a policy until masking ships), `masking-keys.md` §9 (the policy's YAML form, its key ids given at staging, its storage, the embedded baseline, the refusal to stage, and to start outside `dev`, with a policy lacking a baseline row), `operations.md` §10.4 (`policy list`, `policy show <policy_id> <version>`, `config stage`, `config list`, `config activate <config_id>`: words, outputs, audit rows, refusals, and the policy file read by the process that runs the command), the configuration guide (the frame limit's activation) | done (#34) |
| 2 | `internal/policy`: the YAML form parsed strictly (unknown fields refused), turned into a `redact.Policy` with the configured key ids and compiled once; the canonical bytes and their hash; the embedded default, [SEM §8]'s baseline transcribed (every row, both paths where a row has two, the three `source-text` annotation rules), held by a test to an independent table written in the test; the baseline check (every baseline rule present, by stream, function, path and domain); a fuzz target on the parser | done (#34) |
| 3 | catalog: `StorePolicy` and `Policy`; `StageConfig` (insert-or-compare, not activated); activation of a staged configuration by id under the membership sentinel, refusing an unknown or retired one, an active one unchanged; `Register` requiring active configurations only; `RequiredConfigs` on active and bound ones, and a listing of every configuration with its state; on both engines, barrier-controlled: an activation racing an admission (the capture binds the old or the new configuration, never none), an activation racing a registration, two replicas staging one content at once | done (#34) |
| 4 | ingest and service: the configuration's content (new fields omitted when empty, so a policy-less configuration's bytes are unchanged); its reconstruction with no provider call (the policy read from the catalog and its hash checked, both key ids declared and their fingerprints equal to their bindings', any mismatch refused); a start staging its policy-less configuration; `dev` activating its own; `Executable` on active, bound and executable staged configurations; the refusal to start outside `dev` with an active or bound policy lacking a baseline row | done (#34) |
| 5 | admin: `policy list`, `policy show`, `config stage` (the policy parsed, checked for the baseline outside `dev`, stored, the key ids resolved and bound, the configuration staged; refusing a differing body under a stored id and version, an undeclared or wrong-kind key id, a fingerprint mismatch), `config list` (each configuration's id, state, classifier, frame limit, policy and key ids) and `config activate` (refusing an unknown, retired or masking configuration), audited, direct and through `Admin(words)` with equal answers | done (#34) |
| 6 | guides: the production-admission guide (`docs/guides/production-admission.md`: a tool type's manual checked against the policy's carriers, a site's extra rules, the cutover prerequisite of point 8) and the configuration guide's rows | done (#34) |

## Phase 3.3 — masking at ingest

Work on the branch `feat/ingest/masking`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The steps settle these points the row leaves open:

- **Old consumers are fenced at the migration, like old compactors** (the owner's answer of 2026-10-30, recorded as T-43).
  A phase 1 or 2 replica decodes a configuration's content leniently and would run a masking configuration as a policy-less one,
  and a 3.2 replica rebuilds it but writes clear text, so neither may hold a membership once masking exists.
  3.3's migration moves the compactor lease to a row old binaries do not name and the membership tables to names they do not use,
  so an old compactor can neither take nor renew the lease and an old ingest replica can neither register nor renew;
  every compactor and every ingest replica is upgraded at that migration, the old ones failing closed.
- **Masking runs per delivery**, as a step after classification (`StepMask`), with `keys.ForUse` over the delivery's context,
  so `ErrCannotScreen` and a key failure defer that record alone (`ingest.md` §6.1);
  the batch, the working files and the packs hold masked records only.
  `unobserved` is the bus record's quality bit 7.
- **Delivery identity stays on the bytes as received**: the version key, receipts and termination evidence are the original's;
  equal originals mask to equal records and entries, since a capture's configuration, and so its keys, never changes.
- **A key failure**: a refusal (`keys.Refused`) is an indefinite deferral, which parks, alerting `key-binding-mismatch` or `key-refused`;
  an unavailable key is a short deferral, nak'd with delay.
- **Admission outside `tracekeep dev`** binds the newest configuration every leased member supports whose policy holds every baseline rule;
  with none, the capture waits as `incompatible-configuration` does. Under `dev` any bindable configuration serves.
  `config activate` stops refusing a masking configuration (`procconfig.Masks` becomes true).

| Step | Scope | State |
|---|---|---|
| 1 | spec and T-43: `ingest.md` §6.1 (`StepMask` in the decision order, the rows for a refused and an unavailable key beside `ErrCannotScreen`) and §6.2 (masking per delivery, the original's identity, `unobserved`, the pack's `RedactionPolicy` and `Redaction`), `compactor.md` §8.2 (a close's classifier, frame limit and redaction policy from the capture's bound configuration, a persisted intent replaying its bytes), `rollouts.md` §6.4 (the fence at the lease and the membership tables, admission outside `dev`, activation of a masking configuration), `catalog.md` (the renamed tables and lease row) | done (#35) |
| 2 | the fence: a migration renaming the compactor lease row and the membership tables, the catalog on the new names; tests on both engines that an old binary's statements (lease take, renew and check; registration, renewal, the binding read) fail or change nothing on the migrated schema, an old compactor holding the lease when the migration lands and an old standby trying to take it after, and the migration against a compactor transaction held before and after its lease check | done (#35) |
| 3 | a carrier fixture: a synthetic record for every baseline row and both paths of a two-path row, the three `source-text` annotation kinds, a malformed item for the whole-text fallback and a text nested past the walker's limit, each with the original bytes a masked pack must not hold | done (#35) |
| 4 | masking at ingest: `StepMask` after `StepDerive`, inline and claim-check alike, the masked record and its entries on the delivery beside the original; the refused and unavailable key routes, `ErrCannotScreen`, their metrics (`mask_seconds`, `deferred_messages{reason}`); the segment's `RedactionPolicy` from the configuration and its `Redaction` in seq order, conflict segments included; `format_minor` 1 | done (#35) |
| 5 | admission and activation: the binding's predicate (newest bindable configuration with a complete baseline policy outside `dev`), `ErrNoBindableConfig` waiting as `incompatible-configuration`, `procconfig.Masks` true and `config activate` accepting a masking configuration; a capture spanning the cutover keeping its configuration, a pre-cutover capture's backlog drained under it, an old leader publishing after its successor | done (#35) |
| 6 | the close: `intend` setting the classifier, frame limit and redaction policy from the capture's bound configuration before the intent is persisted, an intent persisted before the upgrade replaying its bytes; an upgraded compactor closing a masked capture with its bound metadata | done (#35) |
| 7 | rotation between two deliveries of one record: a digest-key rotation, a master-key rotation and a provider version advanced between the deliveries, each with fresh processes and caches; a policy activated between them, the existing capture keeping its configuration while a new capture binds the new one; through every crash point of `ingest.md` §6.3, inline and claim-check, a different batching on the retry, an orphan adopted before the redelivery and partial acknowledgements; the serialized records and their complete redaction entries compared, then merged without a false conflict | done (#35) |
| 8 | the no-clear-text boundary: working-directory files inspected while held at the write and crash barriers; decoded pack payloads and metadata checked for every carrier of step 3; the whole-text fallback; `ErrCannotScreen`; a failure after part of a batch was masked; a failed put; an interrupted merge | done (#35) |
| 9 | guides and runbook: the production-admission guide's upgrade of every compactor and ingest replica at the migration, the runbook's `key-refused`, `key-binding-mismatch` and `cannot-screen` rows for ingest | done (#35) |

## Phase 3.4 — `Restore` and `Digest`

Work on the branch `feat/api/restore`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The steps settle these points the row leaves open:

- **`Digest` takes a list** (the owner's answer of 2026-10-30, recorded as T-44):
  `Digest(digest_key_id, inputs)`, each input a domain, a kind and the original, at most 1,000 inputs and 8 MiB of originals a request,
  since a scorer comparing one recipe digests many items at once.
  One audit row a request; the rate limit counts inputs.
- **The copies of a record** are read from one catalog snapshot, a single read:
  every pack of the capture whose range holds the seq, in any hour, in any state but `excluded` and `deleted`;
  a merged input is still read, as its archive is.
  A pack registered after the snapshot is not seen, which answers as of the snapshot;
  a pack merged after it stays readable for D (`compactor.md` §8.3);
  a copy whose object cannot be opened or read refuses the restore as unavailable, and the client retries into a fresh snapshot.
- **The hour names where to look, not what to compare**:
  it is an hour start, and a request whose hour holds no copy is not found;
  the comparison spans every copy of the snapshot whatever hour is named.
- **Copies are compared whole**: the stored record, header and payload, as `MergeIterate` compares versions,
  and the seq's redaction entries in each pack's metadata, as the compactor's comparison does;
  equal copies answer once, and differing versions are refused as a conflict, an acknowledged one included.
- **Not restorable**: an annotation, an unobserved record (`Reader.Unobserved`), an unmasked record, and a pack without encryption;
  a pack whose `encryption_key_id` is not the capture's bound configuration's is refused too, since the service trusts no value a pack names.
  The key is `keys.Encryption` for the capture under the bound configuration's key id; `redact.Restore` authenticates every entry before a byte leaves.
- **The audit row comes after the authentication and before the answer**, as `PackURL`'s does:
  locate, compare, restore in memory, write the row, answer; a failed audit answers internal and returns nothing.
  The row holds only an allowlist: the actor, the capture, the seq, the hour, the pack and the encryption key id.
  A refusal writes no row; it is counted by outcome, and its error and log line name the request and the reason, never a byte of the record.
- **`Digest`'s checks** run before any key is resolved, and one failed input refuses the whole request:
  `digest_key_id` is the digest key id of an active or bound configuration,
  and each input's kind and domain are a rule of that configuration's policy:
  `item` an item rule's domain, its original exactly one encoded SECS-II item, header included;
  `text` the domain `message-text`;
  `annotation` an annotation rule's domain.
  The audit row holds the actor, the key id, the input count and the distinct domains, never an original or a digest.
- **The rate limit** is per principal subject and per replica, a token bucket over inputs,
  `api.digest_rate` (default 200 a second) and `api.digest_burst` (default 2,000), the burst at least 1,000 so that a full request can pass;
  a request that would exceed it is refused whole as `resource_exhausted`.
- **Codes**: `not_found` (no copy in the named hour, or an unknown capture); `failed_precondition` (a conflict, not restorable, a key refused);
  `unavailable` (a key or the catalog unavailable, a copy unreadable); `invalid_argument` (a malformed request or input); `resource_exhausted`; `internal` (a failed audit).
- **The round trip is tested in `internal/ingest`**, whose tests may import `internal/api` (it imports nothing of ingest):
  the carriers masked by ingest's pipeline, then restored and digested through the API.

| Step | Scope | State |
|---|---|---|
| 1 | spec and T-44: `api.md` §10.1 (the `Restore` and `Digest` rows, their wire, codes and audit rows), `masking-keys.md` §9 (the copies of a record and the snapshot, the hour, the comparison, what is not restorable, the key check, the audit order and allowlist; `Digest`'s list, checks and rate limit), `operations.md` §10.4 (`restores_total{outcome}`, `digests_total{outcome}`, `digest_inputs_total`), the configuration guide (`api.digest_rate`, `api.digest_burst`) | done (#36) |
| 2 | proto and catalog: `Restore` and `Digest` on `MachineService`, regenerated; the catalog's copies of `(capture, seq)` in one read, on both engines, with a pack registered, merged, deleted and excluded around the read, barrier-controlled | done (#36) |
| 3 | `Restore`: the copies opened and compared, the hour checked, the not-restorable cases, the key check and `redact.Restore`, the audit row, the codes; refusals with a byte of a carrier's original absent from the error and the log; a failed audit returning nothing | done (#36) |
| 4 | `Digest`: the checks, the rate limiter per subject, the audit row, the codes; a refused request digesting nothing | done (#36) |
| 5 | wiring: both operations in the API's operation list and the per-role refusal matrix, `Keys` and the rate limit from configuration, the metrics | done (#36) |
| 6 | the gate's round trip, on both engines: every carrier masked by ingest restored to the record as received, from a segment and from an archive; `Digest` equal to the entry ingest wrote for each input form; a differing copy in another hour refusing the restore; the audit rows checked against the allowlist | done (#36) |
| 7 | guides and runbook: the keys guide's restore and digest, the runbook's rows for a refused restore and an exhausted digest rate | done (#36) |

## Phase 3.5 — thinning

Work on the branch `feat/producer/thinning`, one commit per step, test first.
The steps settle these points the row leaves open:

- **Two pull requests** (the owner's answer of 2026-10-30, recorded as T-46):
  the service builds the SDK at a tagged version with `GOWORK=off`, and a rebase merge rewrites a branch's commits,
  so a tag can only follow the merge.
  The first pull request holds the SDK's steps (1–5); once it merges, `producer/v0.3.0` is tagged on `main`;
  the second raises the service's requirement to it and drives thinning from `cmd/tracekeep-sim` (step 6).
  Every later SDK change the service uses follows the same order.
- **The parameters are descriptor values**, since `producer.md` §4.2 rotates the capture when thinning parameters change and `Tunables` is fixed at `New`:
  `CaptureDescriptor`'s `Thinning []tracepack.Thinning` gives way to a sampling rule (keep every N, keep interval T, a zero field taking its default of 60 and 60 s),
  nil leaving thinning off;
  the SDK generates the `S1F3` and `S1F4` entries and their `source` text from it,
  so `Rotate`'s descriptor comparison rotates on a parameter change with no new path.
  The integrator's configuration keys that fill it are named in `producer-sdk.md`.
- **Registration takes a request, not a channel**:
  a new `Pending().RegisterRequest(Request)` with the session, the System Bytes, the stream, the function, the sending channel and the origin;
  `Origin`'s zero value is unmarked, so an integrator that never sets it thins nothing, and `Register` stays as an unmarked registration.
  Every registration enters the ambiguity set, marked or not.
- **The decision is taken once, at registration, and kept on the entry**:
  a marked `S1F3` registered while the current capture thins takes the sampling rule's decision there;
  the count and the last kept time belong to that capture, so each capture's first poll keeps its bodies;
  the reply inherits its primary's decision.
- **The observed header is checked**: the outbound observation compares the frame's stream and function with the registration's,
  and a disagreement makes the entry never thin, counted, without marking it ambiguous, which would also drop its T3.
- **The wire callback attaches the entry** found by `Lookup` to the lane entry, a pointer and no allocation;
  the publisher decides at dequeue under `producer.md` §4.6's conditions (decision *thin*, not ambiguous, the binding matching the record's socket, the record's capture declaring its stream and function, the set not capped),
  checks an `S1F4` for empty text or a zero-length item, zero-fills from byte 14 of the lane's own copy and sets `quality.unobserved`.
- **Metrics**: `thinned_total` and `thinning_enabled` of `operations.md` §10.4, with the header disagreement counted beside them.
- **Not in this phase**: the catalog parsing a `thinning` entry's `source` at registration (`producer.md` §4.6) is the service's, in 3.6;
  the eqp-hub side, which marks requests, is phase G's.

| Step | Scope | State |
|---|---|---|
| 1 | spec and T-46: `producer.md` §4.6 (the request a registration takes, the header check, the decision's scope and its place on the entry, the publisher's order of checks), `producer-sdk.md` (`RegisterRequest`, `Request`, `Origin`, the sampling rule in the descriptor, its configuration keys), `operations.md` §10.4 (the header disagreement's counter) | done (#39) |
| 2 | descriptor: the sampling rule in place of the entries, its defaults and validation, the generated `S1F3` and `S1F4` entries and `source` text, a parameter change rotating and an equal descriptor not | done (#39) |
| 3 | pending table: `RegisterRequest` and `Origin`, `Register` unmarked, the decision under the capture's count and last kept time, the header check at the binding, every registration in the ambiguity set | done (#39) |
| 4 | thin path: the entry attached by the wire callback, the publisher's conditions, the abnormal `S1F4`, the zero-fill offset's regression test, the cap read at dequeue, a rotation between the send and the reply, a reconnect, a reused key | done (#39) |
| 5 | metrics and the SDK's documentation; `producertest` registering through `RegisterRequest` | done (#39) |
| 6 | second pull request: the service's requirement raised to `producer/v0.3.0`, `cmd/tracekeep-sim` marking its interval polls and a test reading the thinned records back | done (#41) |

## Phase 3.6 — gate

Work on the branch `test/gate/phase-3`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review, and closes phase 3's gate;
the phase's close (the roadmap row done, this plan closed) is a pull request of its own after it.
The audit below read every gate case of the roadmap's phase 3 row and the strengthened cases above against the tests;
the steps add what it found missing or partial, each catalog schedule on both engines and each concurrent one barrier-controlled.
One case is a defect, not a missing test: `masking-keys.md` §9 has a refused `Restore` name the request and the reason in a log line,
and the API logs only `internal` and `unavailable` failures, so a refusal leaves no line.
The catalog's reading of a `thinning` entry's `source` is settled in step 1:
the parameters are parsed from the stored descriptor when a capture is read, which needs no migration,
and a text that does not parse never refuses a capture, its parameters then absent and its text kept.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `producer.md` §4.6 and `api.md` (`ListCaptures`' thinning parameters parsed from each entry's `source` when read, absent when it does not parse), `masking-keys.md` §9's refusal log line kept, the decision recorded | pending |
| 2 | the no-clear-text helpers made strict: a stored object that does not open fails, every check counts what it read, a directory check that finds no file fails, and a policy-less pack shows the scan catches a secret; then one run fed every carrier through ingest, a failed put, a refused item, the merge, the close and the deletions, scanning every stored object, both working directories at every flush barrier, the logs of ingest, the compactor and the API, every error returned and every audit row | pending |
| 3 | keys: a sentinel in a key file, a key file of the wrong mode and an environment variable kept out of the error and the metrics; a failing Vault through `Keys` into ingest's alert log and a metrics scrape, on both engines; a master key id refused as `download`; the equal-material race returning the winner's key to the loser and caching it | pending |
| 4 | rotation between deliveries: every pack of the capture compared on its redaction policy, classifiers and frame limits, and every carrier required to carry entries; the policy rotation's new rule matching a carrier the traffic holds, the new capture fed one and bound to the staged configuration; an orphan adopted after the redelivery; the partial acknowledgement stopping after a masked carrier and the redelivery carrying only what was not acknowledged | pending |
| 5 | `Restore` and `Digest`: the refusal's log line (the defect above); a request without an hour; a copy whose bytes do not read; the entries' comparison on both engines; a retired configuration's key id accepted while a capture is bound to it and refused once none is; a second scorer's own budget through the API; the digest rate and burst bounds and defaults | pending |
| 6 | thinning through masking and the merge: the simulator's thinned polls ingested under a policy with an `S1F3` and an `S1F4` rule, a thinned record stored unobserved with no entry and a kept one masked, merged without a false conflict, a thinned record refused by `Restore` as not restorable, and `ListCaptures` naming the sampling rule's parameters | pending |

### The audit

Read 2026-10-31 at f586dec, from the test bodies.
Covered: "two first uses of one key id with different material" (both engines, the loser returning, caching and deriving nothing, and equal material at another provider version);
"a restore round-trips" (every carrier from the segments and from the archive alone, and `Digest` equal to every entry ingest wrote, in each input form).
Partial, each with what the steps add:
"no clear recipe body anywhere" (step 2: a working file that does not yet open is scanned only as compressed bytes, a stored object that fails to open passes, the crash leg scans a directory already emptied, and a merged archive, a close, a whole run's logs and the errors of a flush and a merge are never scanned; step 3: the file and environment providers and a failure's path into a log and a scrape);
"rotation between two deliveries of one record" (step 4: the policy rotation's new rule matches no carrier, so a redelivery wrongly bound to the new configuration writes the same records, and no pack-level comparison would see it; the partial acknowledgement acknowledges only the start; an orphan adopted after the redelivery runs only without a rotation);
the 3.4 behaviours (step 5, as listed);
the thinned records' path through masking and the merge, and the catalog's reading of `source` (step 6: neither is tested).
