# Phase 4 — people

Status: active (2026-10-31) — 4.1, 4.2 and 4.3 done; 4.4 next

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
| 4.1 — people's sign-in | detailed below | done (#46) |
| 4.2 — `QueryRecords` and `FindTransaction` | spec first (`api.md` §10.1–§10.2, `catalog.md` §7.3): connect-go's overhead on a long stream measured again with real-shaped synthetic records, which settles the batching (T-7), each batch capped in encoded bytes and holding copies, never callback-owned data; the read set of point 9 with its snapshot, a pack merged or evicted between the selection and its read, and the withheld packs; overlapping captures answered side by side, which neither of tracepack's merge orders gives, so one merge per capture; `SxF*` and seq ranges, which tracepack's filter lacks, applied in tracekeep; capture boundaries, gaps and the unknown tail in the stream, and its terminal incompleteness after partial output; the limit, its default and a cursor stable across a merge and a late registration; memory bounds beyond `MaxHeldBytes` (readers open at once, metadata-heavy unmergeable hours of T-45, concurrent streams); the roles of both operations, and a completeness view for `viewer` that grants no `PackURL`; streaming requests observed by the request metrics; SML beside each record (point 4); `FindTransaction` across an hour boundary, a conflict in the second hour and a backward clock step | done (#48) |
| 4.3 — `Export` | spec first (`api.md` §10.1, `catalog.md` §7.1–§7.2, `compactor.md` §8.3): the jobs table and its migration on both engines, its transactions and locks; start, get and cancel, each checking `exporter`, which jobs a principal sees, and their audit rows; job ownership by a fenced worker, an upload intent persisted before the put, an attempt key never reused, completion by compare-and-set, cancel against completion; partial and orphaned objects deleted by a durable step that survives a crash and a late put; the bundle's deletion deadline persisted at completion and shared by the URL and the compactor (T-33), the URL's margin and floor; the read set and withheld packs of point 9; the manifest's conflicts (suspect, confirmed, acknowledged) across two hours; `export_jobs{state}` and the cleanup's failures; no clear recipe body in a bundle, checked on the unpacked JSONL (payloads, annotation bodies, header and redaction entries) against every baseline carrier, with an unmasked negative control | done (#49) |
| 4.4 — frontend | the embedding of point 7 first, then the layout and the sign-in flow over 4.1, then point 4's pages over 4.2 and 4.3 | pending |
| 4.5 — packaging | spec first (`architecture.md`, `operations.md` §10.4): the image (point 11) built after `make web`; the Helm chart under `deploy/helm/` (a Deployment per role, `migrate` as a hook ordered after the bus and the catalog are reachable, probes, alert rules with their runbook links, `work_dir` as a sized `emptyDir`); the systemd unit for `all`, its credentials and directories, and its install and upgrade order (stop, migrate, start, since SQLite has one writer); the CI jobs of point 8 with HTTPS endpoints the browser and the service trust, two `api` replicas with a callback crossing them, a fresh install and an upgrade; the deployment guides | pending |
| 4.6 — gate | audit every gate case against the test bodies, then one step per gap, as phase 3.6 did | pending |

## Phase 4.1 — people's sign-in

Work on the branch `feat/auth/people-sign-in`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
`internal/auth` already holds the service-token verifier with its lazy discovery and key cache, the authenticator and the download token;
`internal/keys` already knows the `session` kind, which the catalog's bindings accept.

| Step | Scope | State |
|---|---|---|
| 1 | spec, `api.md` §10.2–§10.3, `masking-keys.md` §9, `operations.md` §10.4, the configuration guide and the runbook: people's sign-in optional, on the IdP of `auth.oidc`, with a client of its own (client id, secret by reference, roles claim and map); discovery read lazily for the authorization and token endpoints too; the token exchange a request of tracekeep's own, bounded by a deadline; the login cookie (PKCE S256 verifier, state, nonce, a same-origin return path) and the session cookie, their form, lifetimes, flags and names, and sign-in needing TLS, so none under `tracekeep dev`; the callback consumed once by clearing the login cookie and the IdP's single-use code; the ID token's checks; the routes; one credential by precedence, no fallback; the operations a cookie admits; CSRF by `Origin`; the new failure reasons and `Via`; the `session` key's material minimum, rotation interval and resolution at start; nothing of a code, token, secret or cookie in a log or an error page | done (#46) |
| 2 | the `session` purpose: its configuration, issuing and accepted ids, every accepted id resolved at start and a failure failing it; a wrong-purpose request failing closed | done (#46) |
| 3 | the cookie codec: login and session cookies encrypted and authenticated under the session key, the purpose and key id bound, the key id checked against the accepted ones before its key is resolved, a size bound before decoding; a fuzz target | done (#46) |
| 4 | discovery's endpoints and the ID token's verification beside the service token's, sharing its key cache; the token exchange with its deadline; an `httptest` IdP with single-use codes, a stall and an outage | done (#46) |
| 5 | the routes: login, callback, logout and the signed-in principal; a callback crossing replicas, two callbacks of one code racing, a replayed one, a rotation between a login and its callback, the IdP down cold and warm | done (#46) |
| 6 | the authenticator's cookie branch: precedence without fallback, the admitted operations, the `Origin` check, `/download/` token-only, the new reasons counted; nothing of a code, token, secret or cookie in the log, scanned with sentinels | done (#46) |

## Phase 4.2 — `QueryRecords` and `FindTransaction`

Work on the branch `feat/api/query-records`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The steps after the spec read tracepack's read API at the v0.9.0 the service requires, which has all they need;
they adopt v0.12.0 (its `Item.HSMSHeader`, `Item.Clone` and `Unobserved`, and its renames) once it is tagged.

| Step | Scope | State |
|---|---|---|
| 1 | spec, `api.md` §10.1–§10.2, `catalog.md` §7.3, `operations.md` §10.4, T-7: the predicates (`SxF*` and the whole `messages` list applied by tracekeep, seq range, limit, cursor), the read set and withheld captures, the order (capture, hour, seq, a seq's versions together) and one merge per cluster of overlapping envelopes, the close sentinel's envelope, the items, the excluded flag by ranges, SML as valid UTF-8, the markers and each capture's completeness at its start, batches of 128 KiB from T-7's measurement again, the end and the cursor, the snapshot per hour, the bounds, `FindTransaction`'s pack source and codes, the roles (`viewer` reads `ListPacks`, `QueryRecords` and `FindTransaction`) and the operations a cookie admits, the metrics | done (#48) |
| 2 | the catalog's read set on both engines: an hour's `staged` and `excluded` packs from one snapshot with their spans, the hour's scope and eviction, and the capture's withholding; a merge or an eviction between two reads | done (#48) |
| 3 | the record reader: clusters of overlapping envelopes merged in seq order, a close record after every producer seq, the filters tracepack lacks, the excluded flag, the markers, an object gone re-read once from the last seq streamed, the bounds; over synthetic packs in an `fs` store | done (#48) |
| 4 | `QueryRecords` on the wire: the proto, batches capped in encoded bytes never splitting a seq's versions, the limit, the cursor and its refusals, a resumption past a conflict, `end` and an error after partial output, SML beside each data record, the concurrent reads and the metrics | done (#48) |
| 5 | `FindTransaction`: the pack source over the read set with tracepack's release rule, the hour bound, an hour boundary, a conflict in the second hour, a backward clock step, the codes | done (#48) |
| 6 | the roles and the cookie's operations; the configuration guide's `api.records.*` rows and the runbook | done (#48) |

## Phase 4.3 — `Export`

Work on the branch `feat/api/export`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The bundle's packs come from the read set and the records reader's opening of 4.2 (`internal/records`), withheld captures included.
The sandbox has no Docker, so the migrations are written by hand after the newest ones and hashed with `atlas migrate hash`; the `catalog` CI job's `migrate-check` validates them on both engines.

| Step | Scope | State |
|---|---|---|
| 1 | spec, `api.md` §10.1–§10.2, `catalog.md` §7.1–§7.2, `compactor.md` §8.3, `storage.md` §5.2, `operations.md` §10.4: the four operations and the job's owner (`Via` and subject, an empty subject refused), the bounds checked at the start and over what the attempt reads, the work directory's lifecycle, the claim and its fence, the lease on the catalog's clock guarding every step, the hour as the unit of retry, the bundle's layout and manifest, the attempt keys, the upload intent and its put deadline, completion and cancel by compare-and-set, failure and attempts, the fetch's deadline, the cleanup's order and its crash points, the metrics | done (#49) |
| 2 | the `export_jobs` and `export_objects` tables and their migrations on both engines; the export transactions with their guards; a stale fence, cancel against completion, a claim after the lease, the attempts' bound | done (#49) |
| 3 | the bundle: the read sets per capture and hour, withheld captures, an evicted hour named, each pack exported whole, the hour as the unit of retry, the manifest with completeness, conflicts and incompleteness; no key in an error or the manifest; no clear body in the unpacked JSONL against every baseline carrier, with an unmasked control | done (#49) |
| 4 | the worker: claim, renew, intent, a put bounded by its deadline, completion; a lost lease, a cancel mid-attempt, a failed attempt retried, the attempts exhausted | done (#49) |
| 5 | the API: the four operations, the job's owner, the bounds at the start, `kind=filtered` refused, the fetch's URL with the bundle's deadline and `expired`, the audit rows, the roles and the cookie's operations, the metrics; *Downloads*' and `PackURL`'s store and presigning errors made keyless; a download the API serves ended at its token's expiry plus the margin | done (#49) |
| 6 | the compactor's cleanup: an attempt marked `deleting` after its put deadline, a completion racing it, a bundle at its deadline, ended jobs, a crash after `deleting`, a failure counted and logged without a key; the configuration guide's `exports.*` rows and the runbook | done (#49) |

## Gate cases and their owners

| Gate case | Owner |
|---|---|
| a troubleshooting session end to end on a standalone host | 4.4, 4.5; checked by 4.6 |
| the same on a cluster | 4.5; checked by 4.6 |
| IdP down before start and after start | 4.1; end to end in 4.6 |
| export of a capture spanning two hours with a conflict | 4.3 |
| no clear recipe body in an export | 4.3 |
