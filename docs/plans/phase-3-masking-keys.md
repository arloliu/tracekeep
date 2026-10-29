# Phase 3 — masking and keys

Status: draft (2026-10-29) — awaiting the owner

Milestone 3 of the [roadmap](../ROADMAP.md):
the redaction policy bound to each capture and applied before anything is written,
the keys it needs and their bindings, `Restore` and `Digest`, and thinning in the producer SDK.
Its scope and gate are the roadmap's phase 3 row;
the design is `docs/specs/tracekeep/masking-keys.md` §9, `ingest.md` §6.2, `rollouts.md` §6.4, `api.md` §10.1–§10.2 and `producer.md` §4.6.
This plan only orders the work.

## Before it starts

The owner's answers to the points below become one decision-log entry when the plan is activated, as T-41 did for phase 2.

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
   Activation amends §4.6, `producer-sdk.md`'s `Register`, `positioning.md`'s thinning row and the roadmap row,
   marks T-3's channel clause superseded, and drops T-3's open item from the roadmap.
2. **Exports are checked in phase 4** (answered 2026-10-29):
   `Export` is phase 4's, so phase 3's gate checks storage, the working directories and the compactor's outputs,
   and phase 4's gate adds "no clear recipe body in an export".
3. **`Restore`'s request** (answered 2026-10-29), not specified in `api.md` before:
   `Restore(capture, seq, hour)`, the addressing `FindTransaction` uses,
   reading the record and its redaction entries through the pack that holds the seq, merged or not,
   and returning each masked range's original.
4. **The `session` key purpose** (answered 2026-10-29) signs a cookie that only phase 4's people's OIDC issues,
   so it waits for phase 4; phase 3 adds `digest` and `master`.
5. **Vault in tests** (answered 2026-10-29), which the CI matrix lacked:
   a Vault dev-mode container in CI beside MinIO and Ceph RGW, through a make target like `make test-catalog`,
   with an `httptest` fake for the error paths a real server will not produce.
6. **The production-admission check of a tool type** (T-6, answered 2026-10-29), which had no specified form:
   an operator procedure in `docs/guides/`, not code;
   the service refuses to start in production mode with a policy that lacks any baseline row.
7. **The split below** into six sub-phases (answered 2026-10-29).

Not in this phase:
`Export`, `QueryRecords`, people's OIDC, the `session` purpose and the frontend are phase 4's;
the typed admin operations for policies and key ids stay phase 4's, `Admin(words)` serving them meanwhile.

## Phases

| Phase | Scope | State |
|---|---|---|
| 3.1 — keys | the `digest` and `master` purposes in `internal/keys`; `EncryptionKey` by §9's HKDF recipe; a `redact.Keys` over the module; the `vault` provider (KV v2, the binding's pinned version, Kubernetes auth and AppRole, material in memory only); a wrong-purpose request and a fingerprint mismatch failing closed | pending |
| 3.2 — policy | `policies` in the catalog with its migration; a policy in YAML, versioned by content hash, compiled once; the default policy as the [SEM §8] baseline table transcribed (every row and the `source-text` annotation rules), a test holding it to the table; the policy bound through the processing configuration at admission (`rollouts.md` §6.4) | pending |
| 3.3 — masking at ingest | classify and `Mask` before the `Writer` is built, `PackMeta.Redaction` and the capture's `redaction_policy` set (`ingest.md` §6.2); a binding failure deferring every record that needs the key, with its alert; claim-check bodies masked like inline ones; no clear body in a working-directory file; merges and closes carrying ciphertext through | pending |
| 3.4 — `Restore` and `Digest` | the `restorer` and `scorer` roles; `Restore` writing its `audit_log` row before it answers, the original only in the response; `Digest` over §9's three input forms, rate-limited and audited | pending |
| 3.5 — thinning | the SDK's thin decision at registration in the pending table for a request marked interval-only and an `S1F3`, the N/T sampling rule (defaults 60 and 60 s, configurable per channel), the ambiguity set and its cap, the thinned record zero-filled with `quality.unobserved`, the descriptor's `thinning` declaration and rotation on a parameter change; `cmd/tracekeep-sim` driving it; its metrics | pending |
| 3.6 — gate | the roadmap row's cases, audited against the tests as 2.8 did | pending |

Only the phase under way is detailed; the others are detailed when they start.
3.5 is the producer module's and depends on nothing before it, so it may run beside 3.1–3.4.
Every catalog step runs on both engines, and every concurrent schedule is barrier-controlled.
Each phase lands the metrics of `operations.md` §10.4 for the code it adds.
The gate's cases each have an owner:
"two first uses of one key id with different material" is 3.1's binding race on both engines;
"a restore round-trips" is 3.4's;
"rotation between two deliveries of one record" is 3.3's, phase 1's byte-identical redelivery with a digest-key rotation between the deliveries,
the record masked under the key its capture bound;
"no clear recipe body anywhere" is 3.6's scan of the store, the working directories and the compactor's outputs for every baseline carrier.
