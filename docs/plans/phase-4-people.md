# Phase 4 — people

Status: active (2026-10-31) — 4.1 next

Milestone 4 of the [roadmap](../ROADMAP.md):
people's sign-in through the API, the first pages, `QueryRecords` and `FindTransaction`, `Export` bundles, and the packaging a standalone host and a cluster run.
Its scope and gate are the roadmap's phase 4 row;
the design is `docs/specs/tracekeep/api.md` §10.1–§10.3, `masking-keys.md` §9, `catalog.md` §7.1–§7.3, `compactor.md` §8.3–§8.4, `architecture.md`, `storage.md` §5.2 and `operations.md` §10.4.
This plan only orders the work.

## Settled before it started

The owner's answers, recorded as T-48; the phases below cite them by number.

1. **No holds** (settled by T-4):
   LS-13 was decided (b), the VE copies what it promotes, so tracekeep keeps no holds.
   The roadmap row still names them, from seed v21.
2. **`FindTransaction` joins `QueryRecords`** (answered 2026-10-31):
   `api.md` §10.1 lists it, but no roadmap row owns it.
   tracepack provides the lookup, and the record viewer needs a request's transaction,
   so 4.2 serves it over a pack source that answers every hour the lookup asks for.
   `FindTransactions` (a range) stays unadopted.
3. **`Export` ships `kind=canonical` only** (answered 2026-10-31):
   tracepack's exporter writes one pack unfiltered, and its per-record JSONL writer is not exported.
   The payload format lives in go-secs, never here,
   so `kind=filtered` is refused until go-secs exports a filtered writer, which is asked of it as T-45's change was.
4. **The first pages** (answered 2026-10-31): the tool and time browser with scope completeness, the record viewer with SML rendering, and export.
   The restore and admin pages wait for a later phase;
   `Restore` and `Admin(words)` keep serving their callers meanwhile.
   SML is rendered by the server from go-secs, carried beside each record of `QueryRecords`, so the browser never parses SECS-II.
5. **People need an IdP** (answered 2026-10-31):
   the static-token provider stays a service credential and gains no browser path,
   so a standalone host where people sign in names an IdP;
   the gate's standalone run uses a test IdP.
6. **Every accepted session key is resolved at start** (answered 2026-10-31; corrected after review):
   the `api` role resolves the session purpose's issuing id and each accepted id before it is ready, and a failure fails the start,
   as a bad download issuing key does.
   A started replica then validates every cookie without the provider;
   a replica started while the provider is down does not start, and the replicas already running keep serving.
   This amends `masking-keys.md` §9's cold-resolution rule.
7. **The frontend build is optional for the Go build** (answered 2026-10-31):
   the embedding package embeds a tracked fallback page beside `web/dist`,
   so `GOWORK=off make ci` stays green on a checkout without Node,
   and a binary built without `make web` serves the fallback, which says the frontend was not built;
   a release binary and the image are built after `make web` and refuse to ship the fallback.
   Node joins `mise.toml` (T-13 already names it), and a CI job of its own builds and tests `web/`.
8. **The gate runs the real packaging** (answered 2026-10-31; the standalone leg corrected after review):
   the cluster leg installs the Helm chart on kind, with NATS, MariaDB, MinIO and a test IdP in the cluster;
   the standalone leg installs the systemd unit on the CI runner and starts it;
   both drive the browser with Playwright.
   These are CI jobs of their own, beside the containers CI already runs for MinIO, Ceph RGW, Vault and MariaDB,
   and `.agents/rules/300-testing.md` gains them as its exception to "no test reaches a network".
9. **Human reads see masked packs only, from one read set** (answered 2026-10-31; the read set corrected after review):
   outside `tracekeep dev`, a pack whose capture's configuration has no policy is withheld from `QueryRecords`, `FindTransaction` and `Export`,
   and named as withheld wherever the answer reports incompleteness, so a capture admitted before the cutover never shows a clear body.
   `QueryRecords`, `FindTransaction` and `Export` read the same set, defined once in `catalog.md` §7.3:
   an hour's `staged` packs (its archive and the segments registered after a reopen) and its `excluded` packs, which are read and marked,
   never a `merged` input during the deletion delay.
10. **Typed admin operations wait** (answered 2026-10-31), with the admin page: `Admin(words)` serves policies, configurations and key ids meanwhile.
11. **The image** (answered 2026-10-31) is a static, distroless build of the one binary, for amd64 and arm64, published as `ghcr.io/arloliu/tracekeep`.
12. **The split below** into six sub-phases (answered 2026-10-31).

The commit that activated this plan amended:
the roadmap's phase 4 row (holds dropped, `FindTransaction` named) and its open items (the filtered writer asked of go-secs);
for point 1, `api.md` §10.1's admin row and *Downloads* deadline, `compactor.md` §8.3's hold sentence and §8.4's "holds included", `catalog.md` §7.2's `EvictHour` precondition, and `positioning.md` §14, LS-13 settled by T-4;
`api.md` §10.1's `Export` row for point 3 and its typed-admin sentence for point 10;
`api.md` §10.3's first pages and embedding for points 4 and 7, and §10.2's static-token provider for point 5;
`masking-keys.md` §9 for point 6;
`.agents/rules/300-testing.md` for point 8.
Each sub-phase's spec step then writes the detail its row names.

Not in this phase:
holds (point 1), `kind=filtered` (point 3), the restore and admin pages and typed admin operations (points 4 and 10),
payload-content predicates of `QueryRecords` (`api.md` §10.1), and the eqp-hub side of phase G.

## Phases

| Phase | Scope | State |
|---|---|---|
| 4.1 — people's sign-in | detailed below | pending |
| 4.2 — `QueryRecords` and `FindTransaction` | spec first (`api.md` §10.1–§10.2, `catalog.md` §7.3): connect-go's overhead on a long stream measured again with real-shaped synthetic records, which settles the batching (T-7), each batch capped in encoded bytes and holding copies, never callback-owned data; the read set of point 9 with its snapshot, a pack merged or evicted between the selection and its read, and the withheld packs; overlapping captures answered side by side, which neither of tracepack's merge orders gives, so one merge per capture; `SxF*` and seq ranges, which tracepack's filter lacks, applied in tracekeep; capture boundaries, gaps and the unknown tail in the stream, and its terminal incompleteness after partial output; the limit, its default and a cursor stable across a merge and a late registration; memory bounds beyond `MaxHeldBytes` (readers open at once, metadata-heavy unmergeable hours of T-45, concurrent streams); the roles of both operations, and a completeness view for `viewer` that grants no `PackURL`; streaming requests observed by the request metrics; SML beside each record (point 4); `FindTransaction` across an hour boundary, a conflict in the second hour and a backward clock step | pending |
| 4.3 — `Export` | spec first (`api.md` §10.1, `catalog.md` §7.1–§7.2, `compactor.md` §8.3): the jobs table and its migration on both engines, its transactions and locks; start, get and cancel, each checking `exporter`, which jobs a principal sees, and their audit rows; job ownership by a fenced worker, an upload intent persisted before the put, an attempt key never reused, completion by compare-and-set, cancel against completion; partial and orphaned objects deleted by a durable step that survives a crash and a late put; the bundle's deletion deadline persisted at completion and shared by the URL and the compactor (T-33), the URL's margin and floor; the read set and withheld packs of point 9; the manifest's conflicts (suspect, confirmed, acknowledged) across two hours; `export_jobs{state}` and the cleanup's failures; no clear recipe body in a bundle, checked on the unpacked JSONL (payloads, annotation bodies, header and redaction entries) against every baseline carrier, with an unmasked negative control | pending |
| 4.4 — frontend | the embedding of point 7 first, then the layout and the sign-in flow over 4.1, then point 4's pages over 4.2 and 4.3 | pending |
| 4.5 — packaging | spec first (`architecture.md`, `operations.md` §10.4): the image (point 11) built after `make web`; the Helm chart under `deploy/helm/` (a Deployment per role, `migrate` as a hook ordered after the bus and the catalog are reachable, probes, alert rules with their runbook links, `work_dir` as a sized `emptyDir`); the systemd unit for `all`, its credentials and directories, and its install and upgrade order (stop, migrate, start, since SQLite has one writer); the CI jobs of point 8 with HTTPS endpoints the browser and the service trust, two `api` replicas with a callback crossing them, a fresh install and an upgrade; the deployment guides | pending |
| 4.6 — gate | audit every gate case against the test bodies, then one step per gap, as phase 3.6 did | pending |

## Phase 4.1 — people's sign-in

Spec first (`api.md` §10.2, `masking-keys.md` §9, `operations.md` §10.4), then the code.

- The `session` key purpose: its configuration, issuing and accepted ids, minimum material and rotation; every accepted id resolved at start (point 6).
- The backend-for-frontend:
  authorization code with PKCE (S256) against the configured IdP, its client id and secret by reference, the redirect URL under `api.public_url`;
  the verifier, state, nonce and return path in a short-lived login cookie under the session key, so any replica answers the callback,
  consumed once and refused after its expiry;
  the ID token's checks of its own (issuer, the client id as audience and authorized party, the login's nonce, expiry with the leeway),
  sharing the service-token verifier's lazy discovery and key cache, which never block startup;
  the token exchange bounded by a deadline, an IdP down answering `idp-unavailable` without hanging;
  routes for login, callback, logout and the signed-in principal.
- The session cookie: its form (version, purpose, key id, subject, roles, expiry, authenticated and encrypted together, so a login cookie never passes as a session), lifetime, flags, path and size bound;
  re-login at expiry, no refresh token kept.
- Roles from a configured claim of the ID token, mapped by configuration, as service tokens map theirs.
- The authenticator's cookie branch:
  one credential chosen by precedence (a verified client certificate, else a bearer token, else the session cookie), with no fallback once the chosen one fails;
  the operations a cookie admits, named;
  CSRF: a cookie admitted only on a Connect POST whose `Origin` is `api.public_url`'s, no state-changing GET but the callback, which state and nonce protect;
  `/download/` stays token-only whatever cookie comes with it;
  the new `auth_failures_total` reasons.
- IdP down before start and after start: existing sessions keep working until expiry, a login answers `idp-unavailable`.
- The configuration guide and the runbook for a session key's rotation and an IdP outage.
- Tests: a login on one replica with its callback on another; a wrong audience, nonce or state; swapped login cookies; a replayed callback; a rotation during a login;
  two callbacks of one login racing on two replicas; a rotation between a login and its callback;
  a bad bearer token beside a valid cookie; cross-origin and absent-`Origin` requests; a download token beside an absent, invalid or expired cookie;
  a start with the key provider down, and an old accepted cookie first used after a good start and a provider outage;
  the IdP down cold and warm, and a stalled token exchange.

## Gate cases and their owners

| Gate case | Owner |
|---|---|
| a troubleshooting session end to end on a standalone host | 4.4, 4.5; checked by 4.6 |
| the same on a cluster | 4.5; checked by 4.6 |
| IdP down before start and after start | 4.1; end to end in 4.6 |
| export of a capture spanning two hours with a conflict | 4.3 |
| no clear recipe body in an export | 4.3 |
