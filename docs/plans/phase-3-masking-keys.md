# Phase 3 — masking and keys

Status: active (2026-10-29) — phase 3.1 next

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
| 3.1 — keys | detailed below: Vault's configuration, credential lifecycle and outage behaviour specified, then the `digest` and `master` purposes, `EncryptionKey`, the `vault` provider, a context-bearing `redact.Keys`, `key verify` and the keys guide | pending |
| 3.2 — policy and configuration | spec first (`rollouts.md` §6.4, `masking-keys.md` §9, `operations.md` §10.4): the configuration's full identity (classifier, frame limit, policy id, version and content hash, both key ids with their fingerprints, format features) and its reconstruction from stored bytes; policy persistence in the existing `policies` table (an incremental migration only if one is needed); a policy in YAML, compiled once; the default policy as the [SEM §8] baseline transcribed (every row, both paths where a row has two, the `source-text` annotation rules), a test holding it to the table; the refusal to start outside `dev` without the baseline; the `policy` admin commands and `config activate <config_id>`, their words, audit rows and refusals, direct and through `Admin(words)`; a configuration's states (staged: stored and loadable, neither required of members nor advertised; active; retired), membership requiring only active and bound ones, a staged one activated by `config activate` alone, under the membership serialization, and never by a start or by `Executable` loading it; the production-admission guide with the cutover prerequisite of point 8. A masking configuration is staged here, never activated | pending |
| 3.3 — masking at ingest | spec first (`ingest.md` §6.2, `compactor.md` §8.2, `rollouts.md` §6.4): the cutover of point 8 (activation of a masking configuration waiting for, or ending, the memberships that cannot execute it; admission refusing a configuration without a policy; a fence at the compactor lease that an old binary cannot pass: 3.3's migration moves the lease to a row the old binary does not name, so an old compactor can neither take nor renew it and every lease check of its transactions fails, and no policy-less close of a masked capture becomes durable; the guide's prerequisite that every compactor is upgraded before a masking configuration is activated); classify and `Mask` before the `Writer` is built, `PackMeta.Redaction` and the capture's `redaction_policy` set; claim-check bodies masked like inline ones; a key failure deferring every record that needs the key, with its alert; a close's metadata (policy, classifier, frame limit) taken from the capture's bound configuration before its intent is persisted, intents already persisted replaying their bytes; raw delivery identity (receipts, termination evidence) kept on the original bytes; the cutover gate (a capture spanning the cutover, an old leader publishing after its successor, an old compactor holding the lease when the migration lands and an old standby trying to take it after, an upgraded compactor then closing a masked capture with its bound metadata, the migration against a compactor transaction held before and after its lease check, on both engines) | pending |
| 3.4 — `Restore` and `Digest` | spec first (`api.md` §10.1, `masking-keys.md` §9): the record locator of points 3 and 9 (every non-excluded version of the seq compared across hours from one catalog snapshot, record bytes and redaction entries alike; equal copies once; differing versions refused; a candidate that cannot be read refusing the restore; a pack merged, deleted or registered during the lookup); the hour required and naming an hour that holds a copy, otherwise not found, while the conflict check spans every hour whichever hour is named; the whole record authenticated before any original leaves; annotation masks, unobserved records and records without encryption answered as not restorable; the audit row written first, a failed audit returning nothing; `Digest` over §9's three input forms with `kind` checked by the service, its rate limit's scope and bounds; the proto operations wired to the existing `restorer` and `scorer` roles | pending |
| 3.5 — thinning | spec first (`producer-sdk.md`, `producer.md` §4.6): `Register` taking the origin and the primary's stream and function beside the session and System Bytes, an old caller's registration unmarked, a supplied header that disagrees with the observed one never thinned, every registration in the ambiguity set whatever its mark; the sampling rule's key and parameters (defaults 60 and 60 s) owned by the SDK's configuration; the descriptor's `thinning` entries generated by the SDK from that configuration, never taken from the caller; then the thin decision, the ambiguity set and its cap, the thinned record zero-filled with `quality.unobserved`, rotation on a parameter change, `cmd/tracekeep-sim` driving it, its metrics | pending |
| 3.6 — gate | the roadmap row's cases and the strengthened ones below, audited against the tests as 2.8 did; the thinned records' path through 3.3's masking and the merge | pending |

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
| 1 | spec, `masking-keys.md` §9 and the configuration guide: the `vault` provider's configuration (address, CA, KV v2 mount, auth method and role), its credential lifecycle (Kubernetes auth and AppRole login, renewal before expiry, a fresh login when renewal fails), a deadline on every provider call; a warm cache serving bound material through an outage and a cold cache failing closed; material requirements for `digest` and `master` keys; old provider versions kept while a binding or a capture needs them; errors that name the key id and reference but never the material or a token; `redact.Keys` given a context per use, since its methods take none; the key metrics (names, bounded labels, cache hits), which `operations.md` §10.4 gains | pending |
| 2 | `digest` and `master` purposes: `DigestKey` returning the bound digest material unchanged, one key across captures (the same key id giving the same key for every capture); `EncryptionKey` deriving 64 bytes from the bound master material by §9's HKDF recipe, checked against independent vectors (the raw UUID byte order, the key id's UTF-8, distinct captures); a request under another purpose failing closed across `download`, `digest` and `master`; a failed resolution caching nothing; the first-use race of two real `Keys` instances on both engines, with different material (the loser gets nothing) and with equal material at different provider versions (one binding wins and a fresh process follows it) | pending |
| 3 | the `vault` provider: KV v2 read at the binding's pinned version, an unbound key id at the declared or current one; Kubernetes auth and AppRole; renewal and re-login; tests against the dev-mode container (a make target and its CI job) and an `httptest` fake for an expired or revoked credential, a deleted pinned version beside a newer one, a malformed or non-base64 response, a stalled server against the deadline, and secret sentinels kept out of errors and logs | pending |
| 4 | the `redact.Keys` adapter: a context bounded by the call deadline, warm and cold caches through a provider outage, a cancelled context returning no material, concurrent uses whose cancellations stay their own | pending |
| 5 | `key verify <key_id>`: re-resolves bypassing the cache and compares the fingerprint with the binding, audited, direct and through `Admin(words)`; the keys guide (declaring a key, rotating `digest` and `master` keys by declaration, Vault incident recovery) and the runbook's `key-binding-mismatch` section | pending |
| 6 | metrics: the key metrics step 1 specifies, provider errors and credentials never a label | pending |
