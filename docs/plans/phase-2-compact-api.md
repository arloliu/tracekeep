# Phase 2 — compact and machine API

Status: active (2026-10-28) — phase 2.7 in progress

Milestone 2 of the [roadmap](../ROADMAP.md):
the compactor's state machines, the catalog reads they make answerable, and the machine API the VE service reads them through.
The owner's answers to the points it raised before starting are T-41.
Its scope and gate are the roadmap's phase 2 row and the implementation checklist under it;
the design is `docs/specs/tracekeep/compactor.md`, `catalog.md` §7.2–§7.3, `api.md` §10.1–§10.2, `operations.md` §10.4 and `storage.md` §5.2 *Downloads*.
This plan only orders the work.

## Before it starts

- T-7's gate: connect-go's overhead against plain gRPC, measured for `ListPacks` and a streamed `QueryRecords` shape before phase 2.6 starts,
  the result recorded in the decision log.
  It runs beside phases 2.1–2.5, which serve no API.
  Done in phase 2.5: recorded under T-7, with response compression off for the api role.
- tracepack v0.9.0 is already the service's (T-36), so every close is written uncompressed (G5-272):
  phase 2.2 drops `capture_closes.codec` and the codec from `compactor.md` §8.2 and `catalog.md` §7.1,
  which closes the roadmap's open item on it.
- Not in this phase:
  `QueryRecords`, `FindTransaction`, `Export` and an export bundle's retention, people's OIDC, the frontend and holds are phase 4's;
  `Restore`, `Digest` and the `keys` module's other purposes are phase 3's;
  configuration retirement waits for the first release that needs it.

## Phases

| Phase | Scope | State |
|---|---|---|
| 2.1 — merge and conflicts | the merge of §8.1 with `CommitMerge`, `RecordConflict` confirming or clearing suspects, `scope merge`, `conflict acknowledge`, `scope exclude-input`, and their metrics | done (#22) |
| 2.2 — close | `CloseIntent` and `PublishClose` of §8.2 with both triggers, `abandoned` intents (checklist 7's close), `capture close`, crash gaps in completeness, `closes_total{trigger}`; the reconciler adopting a close's orphan, which phase 1 reports and keeps (`internal/compact/orphans.go`), as a pack holding no producer range that moves neither coverage nor `highest_seq`, racing the publication in both orders | done (#23) |
| 2.3 — deletion and retention | each object's deletion deadline as one function (hour end + retention, `merged_at` + D for a merged input), D above the download URL's lifetime, and a shortened retention kept from evicting an hour before an issued URL's promise across restarts and processes (specified in `compactor.md` §8.3 first, then built); `DeletePack` after D; `EvictHour` with `evicted_ranges` and the `parked → evicted-hour` move; the claim bucket listed and its receipt-less objects attributed by the SDK's metadata (hour, capture, seq, the bare object name against the bucket-qualified `Tpk-Payload-Ref`), with their `claim_objects` rows, and hours that hold claim objects but no scope, before eviction reads them; §8.4's predicate as one function; survivors deleted by the reconciler; `object list --survivors` and `--unattributed`; and their metrics | done (#24) |
| 2.4 — reads and cursor | `ListDevices`, `ListCaptures`, `ListPacks` and `ListScopeChanges` as catalog reads (§7.3): `no-data` hours, unregistered intervals, overlap, the watermark read first, `low_water` and `epoch`, pruning by seq prefix | done (#25) |
| 2.5 — service auth | client credentials against the IdP's JWKS with lazy discovery and one refresh on an unknown `kid`, mTLS, the static-token flag never enabled by a failure, the role each operation checks, `auth_failures_total`; and the `download` key purpose of `masking-keys.md` §9 brought forward whole (T-41): the binding's insert-or-compare, the `file` and `env` providers, issuing and accepted key ids, purpose separation | done (#26) |
| 2.6 — api | `proto/`, connect-go behind 2.5's checks from its first served request, the `api` role in `all` and `dev` with its readiness and metrics, the four reads, `PackURL` with 2.3's deadline, a 15-minute lifetime, its margin and floor and `pack-superseded`, presigned where the store presigns and `storage.presign` is on, otherwise served by the API with a download token (`HEAD` and ranged `GET`), presigning checked at start, `audit_log` for API actions | done (#27) |
| 2.7 — admin and status | `tracekeep status`, `deferred list`, admin through the `api` process on SQLite, `tracekeep migrate` applying the catalog's migrations through Atlas's Go library with its own revision table (T-41), tested on a fresh catalog, an upgrade and a failed application, `gap_observations_total` and `captures_with_unknown_tail`, the cost of ingest's metrics read measured at fleet size | in-progress |
| 2.8 — gate | the roadmap row's cases and the checklist the phases before left open, audited against the tests as 1.7 did | pending |

Only the phase under way is detailed; the others are detailed when they start.
Every compactor transaction starts with the capture-row helper and checks the lease after it (T-32, `.agents/rules/600-invariants.md`);
every catalog step runs on both engines, and every concurrent schedule is barrier-controlled.
Each phase lands the metrics of `operations.md` §10.4 for the code it adds.
Each phase's detail names the gate and checklist cases it owns:
the survivor, shared-object and unknown-capture races in 2.3, the cursor's commit order and pruning in 2.4, a promised download's lifetime in 2.6;
2.8 audits them all, the membership and old-version cases phase 1 built included.
The roadmap's gate case "a parked message replays in order after its blocker clears" is the parked consumer's own retry after `parking.redelivery_delay`,
staged through a flush sorted by seq (T-40 withdrew an early replay to proposal P1).

## Phase 2.7 — admin and status

Work on the branch `feat/admin/status`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The catalog's migrations get one path: `migrations.Apply`, which tests, `catalogtest` and `dev` use, runs Atlas's executor (`ariga.io/atlas` v1.3.0, `sql/migrate` with its `mysql` and `sqlite` drivers, built without cgo)
over the embedded directories with their `atlas.sum`, recording every revision in a table of the service's own, `tracekeep_revisions`;
a catalog holding tables but no revision of its own was migrated by another tool and is refused.
`tracekeep migrate` applies the catalog before the bus, whose incarnations it records in the catalog.
`deferred list` is a view of the catalog's unresolved parked versions, by capture and reason, with the oldest's time:
an indefinite deferral is parked (`ingest.md` §6.1), and a short nak that never parks lives only in ingest's metrics.
Admin through the `api` process is a transport for the same commands, not a new API:
the CLI tries the catalog and, finding a running service holding its SQLite catalog, sends the command's words to that process's `AdminService`,
under the `admin` role with the principal's subject as the audited actor; the typed admin operations of §10.1's table stay phases 3 and 4.
The F8 measurement is a section of this plan, below the table.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `catalog.md` §7.4's revision table and the refusal of a catalog another tool migrated; `api.md` §10.1's `AdminService` (the command's words in, the CLI's JSON out, `admin` role, the principal as actor, `--actor` the direct path's only); `operations.md` §10.4's `tracekeep status` fields, each tool's backlog in messages from the traffic stream's subjects and the stream's bytes once, and `deferred list` as the parked view; the CLI's credential for the API path | done (6640b24, c7a367b, e4473ea) |
| 2 | migrations: `Apply` over Atlas's executor with the service's revision table: a fresh catalog, an upgrade from the first migrations to the rest, a failed application resumed from the statement that failed once its cause is gone, a catalog another tool migrated refused, a changed `atlas.sum` refused, on both engines; `tracekeep migrate` applying the catalog before the bus, `dev` and `catalogtest` through it | done (aa3d6dc, c7a367b, e4473ea) |
| 3 | ingest metrics: `gap_observations_total{tool_id}`, a hole first seen at a registration and counted on `Registered` only, and `captures_with_unknown_tail{tool_id}` from the refresh's completeness pass, a closed capture dropping out | done (5614ed5, 3f178fd, fad02f2, 4d40032, 5bde77f) |
| 4 | `tracekeep status` and `deferred list`: `pending` captures first by id, then per tool the last record time, open captures, deferred messages and backlog; the parked view by capture and reason; both engines | done (400611b, 3f178fd, 8a79d9e, e2de103) |
| 5 | admin through the API: `AdminService` in `proto/tracekeep/v1`, its procedure behind auth as `OpAdmin`, the CLI's fallback on `ErrSecondWriter` with the configured admin token, a non-admin principal refused, the audit row's actor the principal's, a command run through a dev loop | done (24d8122, 8a79d9e, 28691f6) |
| 6 | F8: ingest's metrics read (`readCatalog`) seeded with 1,500 tools' open captures and packs, benchmarked on both engines, recorded below; tuned only if it does not fit the 30-second refresh | done (5b75c65) |

### F8 — ingest's metrics read at fleet size

Measured 2026-10-28 with `BenchmarkReadCatalog` (`internal/metrics/fleet_test.go`):
the catalog part of ingest's metrics refresh (`readCatalog`: the parked counts, the open captures, and each open capture's completeness),
over 1,500 tools with one open capture each and a segment in each of a day's 24 hours, 36,000 packs.
One process on a 32-thread x86-64 host, the MariaDB 11.8 server in a local container, five reads per run, three runs:

| Engine | One read |
|---|---|
| SQLite | 115–116 ms |
| MariaDB | 580–595 ms |

Against the 30-second refresh interval the read takes 0.4 and 2 percent; nothing is tuned.
The cost is one completeness read per open capture, so it grows with open captures and their packs;
a fleet many times larger, or a slower catalog server, is where the refresh would first need a read of its own, such as one statement for every capture's gaps.

## Phase 2.6 — api

Work on the branch `feat/api/machine-api`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The machine API serves 2.4's reads and `PackURL` over connect-go, behind 2.5's checks from its first served request:
authentication and the role check are HTTP middleware in front of every handler, since a client certificate's verified chain is on the `*http.Request` and not on a Connect peer,
and a refused request never reaches a handler.
Response compression is off on every handler (T-7).
The generated Go is committed, so the build needs no protoc;
`make proto` regenerates it with protoc and the plugins pinned at `protoc-gen-go` v1.36.12 (the `protobuf` module's) and `protoc-gen-connect-go` v1.21.1 (the `connect` module's), built under `tmp/bin`.
A `PackURL` is the one API action phase 2 audits: the VE service is a privileged reader of packs (T-5), so every issued URL writes an `audit_log` row.
Before the compactor's first retention record no deadline exists and no URL is issued (`compactor.md` §8.3).
`QueryRecords`, `FindTransaction`, `Export`, `Restore`, `Digest`, admin over the API and people's sessions are later phases'.
The gate case this phase owns is a promised download's lifetime.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `api.md` §10.1 made exact where it is silent: the `tracekeep.v1` service and its messages as the catalog's reads, times as `Timestamp` and ids as a UUID's text; a selector refused as invalid without a tool, with an empty window, above 1,000 devices or, for `ListPacks`, above 8,784 hours; `ListScopeChanges` filtered by tool and devices and not by the window; the Connect code of each refusal (`unauthenticated`, `permission_denied`, `invalid_argument`, `not_found`, `unavailable` before the first retention record) and `pack-superseded` as `failed_precondition` with a named detail; the download URL's form (the token in the path under the configured public URL, `Cache-Control: no-store`, `Referrer-Policy: no-referrer`, never logged); compression off; the `api` role's readiness and its start-time presigning check, a refused signature failing the start; the `audit_log` row of each `PackURL`; the `api.*` keys | done (ab3fe69, 4198274) |
| 2 | `proto/tracekeep/v1` and its generated Go, `make proto` with the pinned plugins | done (215fc51) |
| 3 | api: the Connect handler behind the auth middleware, each procedure mapped to its operation, the principal in the context, refusals as Connect errors, compression off, and the request, error and duration metrics per operation; every operation refused unauthenticated and without its role before its handler runs | done (040d8f3, 6191121) |
| 4 | api: `ListDevices`, `ListCaptures`, `ListPacks` and `ListScopeChanges`, mapped from the catalog's results, the selector validated at the handler, the cursor's round trip with its staleness | done (7ebd249, 4198274, 6191121) |
| 5 | catalog and api: `PackURL`: the pack's key and deadline from one snapshot, the expiry at the earlier of now plus the lifetime and the deadline less the margin, `pack-superseded` below the floor or for a deleted pack, a presigned URL where the store presigns and `store.presign` is on and otherwise an API URL with a download token, `download_urls_total{kind}` and `download_urls_refused_total`, the audit row | done (9c56d14, 8dddb2f, 6191121, 80eef43, c224a8e) |
| 6 | api: the download endpoint: the token verified for its object, `HEAD` and ranged `GET` (`206`) through `ObjectStore.Open`, an expired token or one of another object refused, nothing of the URL logged | done (80d51b9, 6191121) |
| 7 | service: the `api` role: its listener with TLS and client certificates verified when given, or plain HTTP/2 without TLS for `dev`, its readiness, the presigning check at start; `api` in `all` and `dev`, `dev` generating its static token and download key under its directory; a `dev` client listing packs and downloading one | done (55ceb11, 6e666e9, 6191121, db528fb, 6717a94) |
| 8 | schedules and the gate case: a URL issued, the retention shortened, and the token and object valid until the URL's expiry, a second api process validating the first's token; `pack-superseded` after a merge's deletion; the generated client against the served API over TLS with a client certificate | done (e84653f, 5577bc6) |

## Phase 2.5 — service auth

Work on the branch `feat/service/auth`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
It builds the checks phase 2.6 puts in front of the API's first served request, and serves nothing itself:
no listener, no interceptor and no download endpoint come with it.
`internal/auth` authenticates a request to a principal and authorizes an operation by the role table;
`internal/keys` resolves the `download` purpose's keys, checked against their bindings, from the `file` and `env` providers.
The `vault` provider and the `digest`, `master` and `session` purposes are phase 3's and 4's; people's OIDC is phase 4's.
The download token's sign and verify come here, since rotation is testable only on a credential that carries its key id;
the API serving a download by it is phase 2.6's.
JWS and JWK parsing is `github.com/go-jose/go-jose/v4`, which phase 4's `coreos/go-oidc` builds on;
the discovery and JWKS fetcher is the service's own, since the spec caches keys with a TTL.
Issuers, audiences and certificate subjects in tests and docs are synthetic.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `api.md` §10.2 made exact where it is silent: a service token is a JWS with an asymmetric `alg` from a fixed list, a `kid`, the configured issuer and audience, `exp` and `nbf` within a leeway, and roles mapped from a configured claim; discovery from the issuer, JWKS cached with a TTL, fetched lazily, at most one fetch in flight and a minimum interval between fetches, one refresh on an unknown `kid` and then rejection, cached keys verifying through an outage until their TTL; an mTLS principal from the verified chain's URI or DNS names mapped to roles; static tokens by reference, compared in constant time, consulted only under the explicit flag; the role of each operation phase 2 serves; `auth_failures_total{reason}`'s reasons; §10.1 *Downloads*' token form; `masking-keys.md` §9: `env` refused outside `tracekeep dev`, material as base64 text, an empty provider version for `file` and `env`, the issuing key id one of the accepted | done (e222b53, ab7afcc) |
| 2 | catalog: `BindKey`, the insert-or-compare of a binding on both engines, no lease and no capture row: a first binding, a matching one, a fingerprint mismatch and a kind mismatch failing closed, and two first bindings of one key id racing, barrier-controlled, with equal material agreeing and with different material one of them failing | done (76f0224, d66db4d) |
| 3 | keys: the `file` provider (mode 0600, else refused) and the `env` provider (refused outside development), the fingerprint, §9's order of a resolution (binding, material at the binding's version, fingerprint, insert-or-compare, then the cache), a failed check caching nothing, a key id under another kind failing closed, and the issuing and accepted key ids; no material in an error or a log | done (e04d91a, d758865) |
| 4 | auth: the service-token verifier against an IdP served by `httptest`: no fetch at construction, discovery and JWKS on first use, a TTL, one fetch in flight for concurrent requests, an unknown `kid` refreshing once then rejected, a refused `alg`, issuer, audience, expiry and not-before with the leeway, a warm cache verifying through an outage and a cold one rejecting with `idp-unavailable` | done (b4ce3fd, de70873, 29308eb, 1edaa84) |
| 5 | auth: the mTLS principal from a verified client certificate, the static-token provider behind its flag, the role table and `Authorize`, `auth_failures_total`; the IdP down with the flag off rejecting, never falling back to a static token | done (15a708d) |
| 6 | auth: the download token signed under the issuing key id and verified under the accepted ones, naming one object key and an expiry; a token of another object, an expired one, an unknown key id and a key id of another purpose refused; rotation's three steps, the old token refused once its id is dropped | done (c96b1a7, de70873) |
| 7 | config: the `auth` and `keys` keys, validated at load (the static flag explicit, the issuing key id accepted and declared, `env` refused outside development) and described in `docs/guides/configuration.md` | done (4bc654e, 287b236, 6aa0e49) |
| 8 | T-7's gate: connect-go's overhead against plain gRPC for a `ListPacks`-sized unary response and a streamed `QueryRecords` shape, measured in a scratch module outside the repository, the method and numbers recorded under T-7 | done (7d7d61b) |

## Phase 2.4 — reads and cursor

Work on the branch `feat/catalog/reads`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The reads are the catalog's domain results the API of phase 2.6 serves unchanged:
they take no capture row and no lease, and each reads one snapshot, as `Completeness` does.
Pruning is the one write: it runs in the retention pass under the lease, with the configured retention period, and allocates no seq.
No proto, no transport and no operator truncation of the cursor come with it; a test sets `counters.epoch` by SQL.
The gate and checklist cases this phase owns are the cursor's commit order and pruning:
the VE service reads a settled hour and learns of a reopen, and an event cursor with an uncommitted earlier transaction and with pruning.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `catalog.md` §7.3 made exact where it is silent: the captures a window selects (admitted, of the tool and devices or the one capture, origin before the window's end and span reaching its start, an `open` capture's span unbounded), a capture's span (origin to `highest_seq_ts`, origin alone before any record) for overlap and as a predecessor, the first capture of a recorder (no interval unless `previous_capture_id` names no admitted capture), a `no-data` hour's flag from the spans, an hour without a scope whose ranges were evicted reported evicted, a scope-less hour's parked seqs read from the capture's completeness, `ListDevices`' fields (the latest capture's endpoint and device id, open captures, overlap, last record time, a gap whose interval meets the window, an unknown bound counting as meeting it), the cursor as (epoch, seq), stale when the epoch differs or a pruned seq lies above it (`after < low_water − 1`), a page's next cursor past the events it scanned and at the snapshot's watermark when the page is short, the tool and device filter, pruning with the configured retention period in the retention pass under the lease, `low_water` never lowered; §7.2's pruning, which checks the lease | done (a210c4a, 0de0d2d, 1cc6af5) |
| 2 | catalog: the window's captures and `ListCaptures`: state, device, recorder, predecessor, the descriptor's thinning entries, processing configuration, overlap from the spans of one (`tool_id`, `recorder_instance_id`), unregistered intervals against Q, tested at Q and past it, with overlapping predecessors, a missing predecessor and a capture with no record | done (576333b) |
| 3 | catalog: `ListDevices`: per device its name, the latest capture's `equipment_endpoint` and `device_id`, open captures, overlap, last record time and whether the window holds a gap | done (a3bb346) |
| 4 | catalog: `ListPacks`: one read snapshot whose first statement reads the watermark and epoch, then per requested hour the scopes of the window's captures with state, revision and packs with their ranges, a scope-less hour `no-data` with its flag, evicted, or holding deferred seqs and declared losses, and each capture's completeness and unregistered intervals | done (6550341, 2a8dbd2) |
| 5 | catalog: `ListScopeChanges`: events after a cursor for the tool and devices, paginated, the next cursor, stale by epoch and by `low_water`, tested at `after = low_water − 1` and `− 2` | done (4ee868a) |
| 6 | catalog and the retention pass: `PruneEvents` under the lease, `low = MIN(seq)` over events younger than the period, nothing written without one, the prefix deleted and `low_water` raised monotonically in one transaction | done (ed0788c) |
| 7 | schedules, barrier-controlled on both engines: an event allocated and uncommitted while a listing reads its watermark, then read after it; an event committed between the watermark and the listing's rows, replayed above the watermark; a settled hour listed, reopened by a registration, the `reopened` event read with its revision; pruning racing a reader; a stale cursor re-listing and continuing from the watermark; a lease lost before the prune commits | done (a4c04cd) |

## Phase 2.3 — deletion and retention

Work on the branch `feat/compact/retention`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
Deletion, the claim bucket's listing, retention and survivor cleanup run in the compactor's cycle under the one lease, after the close pass, in that order,
so the listing's rows exist before eviction reads them.
D becomes `compact.delete_delay` (default 1 hour) and the download URL's lifetime `store.download_lifetime` (default 15 minutes, T-41),
D validated above it by more than twice two minutes of clock skew;
the margin and floor are phase 2.6's.
A changed check constraint is renamed with its change (phase 2.2).
The gate and checklist cases this phase owns are the survivor race, the shared-object race and the unknown-capture race.

| Step | Scope | State |
|---|---|---|
| 1 | spec: `compactor.md` §8.3 gains the deadline as one function and the retention record: the compactor records its configured period in an append-only catalog row under the lease before any eviction, a shortened period keeping the longest period in force within the last D (D above every URL's lifetime) for D, so the api and compact roles compute one deadline from the catalog alone, across restarts and processes, and before the first record no deadline exists; a merged input's deadline from its own merge's `merges.delete_at`, `merges.at` + D fixed at the commit (so a changed D applies to later merges), not the scope's `merged_at`; the registration's horizon guard on the configured period at once (refusing early is safe; only eviction waits); eviction's rows spelled out (an evicted pack's `pack_ranges`, `pack_losses`, `merge_inputs`, `merges` and the `conflicts` rows naming it go with it; `evicted_ranges` coalesced within its hour, keyed by capture, hour and first seq); `unattributed_objects` for the bucket's objects without the SDK's metadata; the listing's transaction taking no capture row; the clock assumption the horizon guard and eviction share; a refused eviction or deletion counted every cycle; `catalog.md` §7.1–§7.2 and the configuration guide to match | done (13198b8, ddda182) |
| 2 | schema: `retention_periods` (period, the period held and until when, one row per change), `unattributed_objects` (key, size, first seen), `merges.delete_at`, `evicted_ranges` keyed by capture, hour and first seq | done (1a40f89) |
| 3 | catalog: the retention record under the lease, and the deadline and eviction horizon as one function of its row, tested at, inside and past a shortening's window, a lengthening, two shortenings inside one window and a restart reading the row | done (88645c5) |
| 4 | catalog: the deletion candidates (a `merged` input of a `committed` merge whose output is `staged`, `merged` or, merged in turn, `deleted`, its merge's `delete_at` passed, so a chain of merges deletes in any order) and `DeletePack` under the capture row and the lease, no other `packs` row naming the key, the pack `deleted` and its tombstone, the object removed after the commit and, after a crash, by the orphan pass that deletes tombstoned keys; a pack's deadline for phase 2.6 | done (8002f60) |
| 5 | catalog: §8.4's predicate as one function of a (capture, seq)'s `parked` versions, their copies and each role's current incarnation; the eviction candidates (hours of admitted captures past the eviction horizon holding a scope, a pack, a `live` claim object or a `parked` version) and `EvictHour` under the capture row and the lease: the hour's ranges into `evicted_ranges`, its rows deleted, its `claim_objects` rows deleted except survivors, marked with the hour, its `parked` rows moved `parked → evicted-hour`, the tombstones, the objects removed after the commit; a conflict's other scope advanced with a `conflict` event | done (3535afb) |
| 6 | catalog and the claim pass: the bucket listed, an object without a `claim_objects` row attributed by its name (`<capture>.<seq>`, against the bucket-qualified key) and the SDK's metadata (the 44-byte header, its seq and time matching, the hour from the time) as a `live` row whatever its capture's state, and one without that metadata recorded in `unattributed_objects`, removed once gone; survivor cleanup: `MarkSurvivorDeletable` under the capture row and the lease on the predicate, the object deleted (an absent one counts), the row removed under the lease, a `deletable` row resumed | done (995fc03, f0a7bed) |
| 7 | the compactor's passes in the cycle: deletion, the listing, retention (the record first), survivors; the claim objects of an evicted hour deleted from the bucket after its commit, a crash between converging through the next listing; `compact.delete_delay` and `store.download_lifetime`, D validated above the lifetime by more than twice two minutes of clock skew | done (e1e91a4) |
| 8 | admin: `object list --survivors` (key, capture, seq, evicted hour, size) and `--unattributed` | done (520e884) |
| 9 | metrics: `cycle_seconds{task=delete,retention}`, `last_success_timestamp{task}`, `deletions_total`, `tombstones_total` for deletion and retention, `claim_objects`, `claim_object_bytes`, `claim_objects_survivors` | done (10eabdd) |
| 10 | schedules, barrier-controlled on both engines, asserting durable state: a registration and a deletion of one key in both orders; an unresolved `parked` row committed before the eviction (a survivor) and after it (the horizon guard terminates or moves it to `evicted-hour`); two versions of one (capture, seq), the object kept until the last satisfies the predicate (staged needs no acknowledgement, terminated needs every current-incarnation copy `acked`, a retired incarnation's `lost` copy not blocking); a claim object of a `pending`, `rejected` or unknown capture kept, then evicted once admitted; a survivor released after its version resolves, and an interrupted release resumed; a crash after each commit; a retention shortened with a restart inside its window evicting nothing early; a lease lost before each commit; a refused eviction counted | done (ddda182) |

## Phase 2.2 — close

Work on the branch `feat/compact/close`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The close runs in the compactor's cycle under the one lease, after the merge pass, so a close's segment is merged with its hour's like any segment.
The inactivity period I (default 2 hours, T-12) becomes `compact.inactivity` in the configuration and its guide.
A changed check constraint is renamed with its change, since Atlas v1.3.0 writes MySQL's `DROP CHECK` for a modified check, which MariaDB refuses,
and `DROP CONSTRAINT` for a dropped one.

| Step | Scope | State |
|---|---|---|
| 1 | schema: `capture_closes.codec` dropped with the codec from `compactor.md` §8.2 and `catalog.md` §7.1 (T-36, closing the roadmap's open item), and a third trigger kind, `operator`, for `capture close` | done (e16ffd9) |
| 2 | catalog: the candidate scan, captures `open` without `stop_seen`, by inactivity (no registration for I, the capture's first sighting standing in for a capture never registered) or by successor (a later capture of its `tool_id` and `recorder_instance_id`, by origin, with a registration at least Q old, and none of its own for Q); `CloseIntent` under the capture row and the lease, eligibility re-read for its trigger inside, the intent row with its fixed `pack_id`, `writer_time`, serialized `PackMeta`, highest seq and its time and the gap bounds, the capture `closed`; an operator's request ended in the same commit; the pending intents read | done (5b2fc80) |
| 3 | catalog: `PublishClose` under the capture row and the lease, on the intent's `pack_id`: registered as a pack holding no range, or found registered (adopted first), and the intent `published`, the scope's event last; the horizon guard refusing it tombstones the key and abandons the intent, and a key already tombstoned abandons it too; neither coverage nor `highest_seq` moves | done (27eff21) |
| 4 | the close pass: candidates to intents, then every pending intent written by `tracepack.WriteClose` from exactly its stored options, put under its `staging/` key (a put whose result was lost checked by its size and its bytes' SHA-256 against the written close), and published; `capture close` as a request decided under the capture row (not open or `stop_seen`: failed), its intent ending it | done (8a7cd29) |
| 5 | the reconciler adopting a close's orphan as a pack holding no range, before and after the publication | done (b953cf3) |
| 6 | metrics: `closes_total{trigger}`, `cycle_seconds{task=close}`, `last_success_timestamp{task=close}`; `compact.inactivity` | done (32942a7) |
| 7 | schedules, barrier-controlled on both engines, asserting durable state: a registration between the candidate scan and the intent (no close), and one after it (staged, the capture stays `closed`, its close's bytes unchanged); a late `stop` before the intent (no close) and after it (staged; `stop_seen` set, the capture stays `closed`); the successor trigger refusing to close an overlapping capture still registering, and closing it once silent for Q; a crash after the intent, after the put and after the publication, each retried to the same bytes; the close's object adopted before its publication; a horizon refusal leaving the intent `abandoned` and never retried (checklist 7); a capture closed after seq 100 keeping `highest_seq` 100, its crash gap read by completeness; a lease lost before each commit | done (10f7d1c) |

## Phase 2.1 — merge and conflicts

Work on the branch `feat/compact/merge`, one commit per step, test first;
the branch merges by pull request once the whole phase passes an external post-implementation review.
The merge runs in the compactor's cycle beside the reconciler, under the one lease;
merged inputs stay `merged` until phase 2.3 deletes them.
The quiet period Q (default 15 minutes, T-12) becomes `compact.quiet_period` in the configuration and its guide.

| Step | Scope | State |
|---|---|---|
| 1 | catalog: the candidate scan (`open` or `reopened`, hour end + Q passed, `staged` packs, no `conflicts` row in any state), the snapshot (revision r and the `staged` inputs, segments and archives alike, read together with the eligibility), `CommitMerge` re-reading under the capture row that the scope has no `conflicts` row and every input is still `staged`, then the revision and lease compare-and-sets, zero affected rows or a failed re-read rolling back the whole transaction, the `settled` event last | done (258e353) |
| 2 | the merge pass: `tracepack.Merge` over `ReaderAt`s of the inputs, the archive written to the working directory and put under its `archive/` key, then `CommitMerge`; a rolled-back merge or a crash after the put leaves the archive to the reconciler, which adopts it as a `staged` input of the next merge ([FMT I-12]); a merge failure leaves the scope as it was and is counted | done (706a9e5, af8af1f) |
| 3 | `RecordConflict`: a suspect's overlapping records compared by bytes and redaction entries, cleared for equal copies and confirmed for differing ones only while the row names exactly the compared packs (otherwise `changed`); a merge failing on a conflict writes the row; every touched scope's revision advanced and a `conflict` event each | done (f895ce1) |
| 4 | admin: `conflict acknowledge` (a state `acknowledged` the `conflicts` check and `catalog.md` §7.1 gain; the scope stays unmerged), `scope exclude-input` (the pack `excluded` under the capture row, the revision advanced, an `excluded` event emitted, the schema's event check widened, T-41), each audited; `scope merge` as a `compact_requests` row the compactor holding the lease executes at its next cycle (the table added to the schema, T-41), tested with no compactor running, a lease lapsing or taken over while the request executes, and a crash during it, the request's acceptance told from its execution and a retry after a lost response executing it once | done (4a543a8) |
| 5 | metrics: `cycle_seconds{task=merge}`, `last_success_timestamp{task=merge}`, `merges_total{outcome}`, `scopes{state}`, `conflicts_open`, `oldest_unmerged_scope_age_seconds`; `compact.quiet_period` validated strictly above `flush_interval` + `ack_wait` + `compact.redelivery_slack` (default 2 minutes, `catalog.md` §7.3, T-25, T-41), tested at, below and above that bound with the defaults and `dev`'s | done (1d324db) |
| 6 | the merge's schedules, barrier-controlled on both engines, each asserting the durable rows and objects, not only the result: a registration between snapshot and commit, in `open` and in `reopened`, failing the stale commit and merged by the next cycle; a lease takeover before the lease row's lock and one blocked behind it, and a lease lapsed without takeover, the stale worker writing nothing and never deleting its archive; a losing commit leaving no write of its own (archive row, range, loss, merge relation, input change or event); a commit durable whose result was lost, then no second merge relation or `settled` event; the archive adopted by the reconciler before `CommitMerge` (the adopted rows stay, the stale commit adds nothing) and after it (the reconciler finds it registered and changes nothing); a crash after the put, a new lease holder adopting the archive two passes later, its suspects cleared and a fresh merge settling each record once, and with a termination order on the capture the archive tombstoned instead and the inputs intact; a staged archive merged again with a late segment, ranges, endpoint times and losses kept; a merge that writes no receipt, no suspect against its own inputs and no `last_registered_at`; a damaged input, a failed read, a failed put and a put visible despite its error, the scope and inputs unchanged and no conflict fabricated | done (f7427ce) |
| 7 | the conflict and exclusion schedules, likewise: a conflict raised, confirmed or cleared on the scope, from another hour included, between the snapshot and the commit, failing the stale commit; one cleared before the snapshot, after which the merge proceeds; one raised before the snapshot, or acknowledged, keeping the scope unmerged; a suspect acknowledged while its comparison runs, which then neither clears it nor makes the scope mergeable; the compared pair's packs changed before `RecordConflict` commits, returning `changed` with no state, revision or event written; equal records with equal and with differing redaction entries; a third pack registered while a comparison runs, its pair rows kept; a comparison that fails to read, or whose lease is lost before its transaction, clearing and confirming nothing, and a later comparison converging; a pack excluded before the snapshot, never an input, and after it, failing the stale commit; an excluded pack still readable, its exclusion's pack state, revision, audit row and event committed together or not at all | done (29ce22e) |
