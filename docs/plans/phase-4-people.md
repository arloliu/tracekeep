# Phase 4 — people

Status: draft (2026-10-31) — awaiting the owner

Milestone 4 of the [roadmap](../ROADMAP.md):
people's sign-in through the API, the first pages, `QueryRecords`, `Export` bundles, and the packaging a standalone host and a cluster run.
Its scope and gate are the roadmap's phase 4 row;
the design is `docs/specs/tracekeep/api.md` §10.1–§10.3, `masking-keys.md` §9, `compactor.md` §8.3, `architecture.md`, `storage.md` §5.2 and `operations.md` §10.4.
This plan only orders the work.

## Before it starts

The owner's answers to the points below become one decision-log entry when the plan is activated, as T-42 did for phase 3.
Each point gives the recommendation the plan assumes until the owner answers.

1. **No holds** (settled by T-4):
   LS-13 was decided (b), the VE copies what it promotes, so tracekeep keeps no holds.
   The roadmap row still names them, from seed v21;
   activation drops "holds" from the row and the conditional hold clauses of `api.md` §10.1 (the admin row, *Downloads*) and `compactor.md` §8.3–§8.4.
2. **`FindTransaction` joins `QueryRecords`**:
   `api.md` §10.1 lists it, but no roadmap row owns it.
   tracepack provides the lookup, and the record viewer needs a request's transaction,
   so 4.2 serves it over a pack source built from the catalog's packs of an hour.
   `FindTransactions` (a range) stays unadopted.
3. **`Export` ships `kind=canonical` only**:
   tracepack's exporter writes one pack unfiltered,
   and its per-record JSONL writer is not exported.
   The payload format lives in go-secs, never here,
   so `kind=filtered` waits for go-secs to export a filtered writer (asked as T-45's change was), and is refused until then.
4. **The first pages**: the tool and time browser with scope completeness, the record viewer with SML rendering, and export.
   The restore and admin pages wait for a later phase;
   `Restore` and `Admin(words)` keep serving their callers meanwhile.
   SML is rendered by the server from go-secs, carried beside each record of `QueryRecords`, so the browser never parses SECS-II.
5. **People need an IdP**:
   the static-token provider stays a service credential and gains no browser path,
   so a standalone host where people sign in names an IdP;
   the gate's standalone run uses a test IdP.
6. **The session key is resolved at start**, as the download purpose's issuing key is:
   a restart while the key provider is down would otherwise refuse every login and every existing cookie.
   This amends `masking-keys.md` §9's cold-resolution rule.
7. **The frontend build is optional for the Go build**:
   `web/dist` is embedded from a tracked placeholder,
   so `GOWORK=off make ci` stays green on a checkout without Node;
   a binary built without `make web` serves a page saying the frontend was not built.
   Node joins `mise.toml` (T-13 already names it), and a CI job builds and tests `web/`.
8. **The cluster leg of the gate runs on kind with the Helm chart**,
   with NATS, MariaDB, MinIO and a test IdP in the cluster and the browser driven by Playwright;
   the standalone leg runs the binary as `all` under its systemd unit's command line.
9. **An export holds only masked packs**:
   outside `tracekeep dev`, a pack whose capture's configuration has no policy is left out of the bundle and named in the manifest as withheld,
   so a capture admitted before the cutover never exports a clear body.
   A bundle reads an hour's packs as `QueryRecords` does:
   a merged hour's archive, never its inputs during the deletion delay, and no `excluded` pack.
10. **Typed admin operations wait**, with the admin page: `Admin(words)` serves policies, configurations and key ids meanwhile.
11. **The image** is a static, distroless build of the one binary, for amd64 and arm64, published as `ghcr.io/arloliu/tracekeep`.
12. **The split below** into six sub-phases.

Not in this phase:
holds (point 1), `kind=filtered` (point 3), the restore and admin pages and typed admin operations (points 4 and 10),
payload-content predicates of `QueryRecords` (`api.md` §10.1), and the eqp-hub side of phase G.

## Phases

| Phase | Scope | State |
|---|---|---|
| 4.1 — people's sign-in | detailed below | pending |
| 4.2 — `QueryRecords` and `FindTransaction` | spec first (`api.md` §10.1–§10.2): connect-go's overhead on a long stream measured again with real records, which settles the batching (T-7); the roles that admit each operation; how capture boundaries, gaps and the unknown tail ride the stream; side-by-side captures; the limit, its default and cursor; window and memory bounds; SML beside each record (point 4) | pending |
| 4.3 — `Export` | spec first (`api.md` §10.1, `compactor.md` §8.3, `catalog.md` §7.1): the jobs table and its migration on both engines; start, get and cancel; the bundle staged under `work_dir`, then put under a key unique to the job; the manifest; a failed or cancelled job's partial objects deleted, after a crash too; the compactor deleting a bundle at the end of its retention by its job row; the withheld packs of point 9; `export_jobs{state}`; a capture spanning two hours with a conflict; no clear recipe body in a bundle | pending |
| 4.4 — frontend | the embedding of point 7 first, then the layout and the sign-in flow, then point 4's pages over 4.2 and 4.3 | pending |
| 4.5 — packaging | the image (point 11); the Helm chart under `deploy/helm/` (a Deployment per role, `migrate` as a hook, probes, alert rules, `work_dir` as a sized `emptyDir`); the systemd unit for `all` with its configuration and credentials; the kind job of point 8 | pending |
| 4.6 — gate | audit every gate case against the test bodies, then one step per gap, as phase 3.6 did | pending |

## Phase 4.1 — people's sign-in

Spec first (`api.md` §10.2, `masking-keys.md` §9, `operations.md` §10.4), then the code.

- The `session` key purpose: its configuration, issuing and accepted ids, minimum material and rotation; resolved at start (point 6).
- The backend-for-frontend:
  authorization code with PKCE against the configured IdP, its client id and secret by reference, the redirect URL under `api.public_url`;
  the verifier, state and nonce in a short-lived login cookie under the session key, so any replica answers the callback;
  the ID token checked by the service-token verifier's discovery and key cache, which never block startup;
  routes for login, callback, logout and the signed-in principal.
- The session cookie: its form (version, key id, subject, roles, expiry, authenticated and encrypted together), lifetime, flags and size bound;
  re-login at expiry, no refresh token kept.
- Roles from a configured claim of the ID token, mapped by configuration, as service tokens map theirs.
- The authenticator's cookie branch, its precedence against mTLS and a bearer token, and a cookie accepted on browser-reachable operations only;
  CSRF refused on every state-changing request that carries a cookie;
  the new `auth_failures_total` reasons.
- IdP down before start and after start: existing sessions keep working until expiry, a login answers `idp-unavailable` without hanging.

## Gate cases and their owners

| Gate case | Owner |
|---|---|
| a troubleshooting session end to end on a standalone host | 4.4, 4.5; checked by 4.6 |
| the same on a cluster | 4.5; checked by 4.6 |
| IdP down before start and after start | 4.1; end to end in 4.6 |
| export of a capture spanning two hours with a conflict | 4.3 |
| no clear recipe body in an export | 4.3 |
