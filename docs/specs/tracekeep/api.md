# tracekeep — API, auth and frontend

Status: draft (2026-10-25) — split from seed v21

Covers the machine API and its operations, authentication and roles for people and services, and the embedded frontend.
From seed v21 §10.1–§10.3; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `proto/`, `internal/api`, `internal/auth` or `web/`.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Contract**: protobuf served by connect-go, gRPC for Go clients and JSON over HTTP for the browser, from one definition (§10.1).
- **Operations**: `ListDevices`, `ListCaptures`, `ListPacks`, `ListScopeChanges`, `PackURL`, `QueryRecords`, `FindTransaction`, `Export`, `Restore`, `Digest`, admin (§10.1 table).
- **Selector**: reads name (`tool_id`, devices, window), the device being the gateway's `eqp-hsms` device name;
  omitting the devices selects every device of the tool (§10.1 *Selector*).
- **`PackURL`**: a download URL that expires before its object can be deleted, or `pack-superseded`; it makes the VE service a privileged reader of packs (T-5).
- **Downloads**: a presigned URL where the store presigns, otherwise a URL on the API carrying a signed download token;
  either is issued only after the role check, expires before its object's deletion deadline minus a margin, and serves ranged reads (§10.1 *Downloads*).
- **Exports**: an asynchronous job producing a bundle with one JSONL file per source pack, by that pack's `format_minor`;
  a filtered `.tpk` is not offered (§10.1).
- **People**: OIDC authorization code with PKCE in a backend-for-frontend;
  a signed, encrypted session cookie any replica validates (§10.2).
- **Services**: OAuth2 client credentials validated against the IdP's JWKS, or mTLS in the cluster;
  startup never blocks on the IdP (§10.2).
- **Roles**: `pack-reader`, `scorer`, `viewer`, `exporter`, `restorer`, `admin`, granted separately (§10.2).
- **Frontend**: `web/` built into the binary and served at `/`; no pages in the first version (§10.3).
- **Gate**: connect-go's overhead against plain gRPC is measured before the API is implemented (T-7).

## 10. API, auth and frontend (requirements 5 and 6)

### 10.1 Machine API first

The VE service is the main client, so the contract is protobuf served by connect-go:
gRPC for Go clients, JSON over HTTP for the browser, one definition.

| Operation | Answers |
|---|---|
| `ListDevices(tool, from, to)` | per device: name, `equipment_endpoint`, `device_id`, how many captures are open, whether any captures overlap (§7.3 *Overlap*), the last record time, whether the window holds a gap |
| `ListCaptures(selector)` | captures with state, device, `recorder_instance_id`, `previous_capture_id`, unregistered intervals and the `overlap` flag (§7.3), thinning parameters, processing configuration ids |
| `ListPacks(selector)` | per hour and scope: state, revision, packs with their ranges, gaps with time intervals, deferred seqs, declared losses with their action ids, crash gaps, conflicts, unknown tail, `no-data` hours (§7.3) — LS-10 |
| `ListScopeChanges(after seq, selector)` | `scope_events` (`created`, `settled`, `reopened`, `conflict`, `terminated`, `excluded`) after a cursor, paginated, with the epoch — LS-11; one global cursor, returning only the selected tool's and devices' events |
| `PackURL(pack_id)` | a download URL (expiry clamped to the object's deadline, or `pack-superseded`, *Downloads* below) so the VE reads packs with tracepack's own reader; this makes the VE service a privileged reader of packs that hold ciphertext in place (§13) |
| `QueryRecords(selector, predicates)` | streamed masked records, via `MergeIterate`; the predicates below, plus capture, seq range and epoch |
| `FindTransaction(capture, seq, hour)` | tracepack's lookup |
| `Export(filter, kind)` | an asynchronous job producing a **bundle** (tar) under `exports/`, fetched by a download URL (*Downloads* below): one JSONL file **per source pack**, `tracepack-jsonl/1`, `/2` or `/3` by that pack's `format_minor` 0, 1 or 2 (G5-233, G5-270), plus `manifest.json` naming packs, captures, hours, each pack's incompleteness from its read, conflicts, and the filter. `kind=canonical` uses the canonical exporter unfiltered; `kind=filtered` writes the same record schema with the filter applied and is declared non-canonical in the manifest. A filtered `.tpk` is an extract, a v1.0 non-goal, and is not offered. A cancelled or failed job deletes its partial objects; a completed bundle is kept for its retention (default 24 h) and then deleted by the compactor (T-33) |
| `Restore`, `Digest` | §9 |
| admin | policies, processing configurations, key ids (no material), retention, thinning status, holds if LS-13 (a) is chosen |
| `Admin(words)` | the `tracekeep admin` command those words name, run by the `api` process, its JSON result returned: the transport of §10.4's commands while that process holds a SQLite catalog |

**Admin over the API.**
A running service holds its SQLite catalog alone (§7.4), so `tracekeep admin` finding it held sends the command's words to that service's `AdminService.Admin`,
which runs the same command there and returns the JSON the command prints.
It checks the `admin` role and audits the command as every admin command is audited, the actor being the principal's subject;
the CLI's `--actor` names the actor of the direct path only.
The CLI authenticates with the configuration's static token that holds `admin` (`auth.static`, its flag on),
reaching the service at `api.public_url` and trusting its certificate (`api.tls_cert_file`) beside the system's roots;
without such a token it refuses and says so.
The typed admin operations of the table above stay phases 3 and 4's.

**Selector.**
A reader names the connection it wants by (`tool_id`, device),
the device being the gateway's configured `eqp-hsms` device name (`producer.md` §4.5).
`ListCaptures`, `ListPacks`, `QueryRecords`, `Export` and `ListScopeChanges` take one selector:

```json
{
  "tool_id": "T1",
  "devices": ["eqp"],
  "from": "2026-10-25T08:00:00Z",
  "to": "2026-10-25T10:00:00Z"
}
```

- `tool_id` is required; `devices` omitted means every device of the tool;
  `capture_id` remains for an exact choice.
- A window that spans several captures of a device is answered across all of them;
  results mark each capture boundary, gap and unknown tail where it falls,
  so a reader can tell "no traffic" from "data missing".
  Overlapping captures of one device are returned side by side, each with its own completeness, never interleaved (§7.3 *Overlap*).
- The selector is refused as invalid without a `tool_id`, with an empty or reversed window, or naming more than 1,000 devices;
  `ListPacks` also refuses a window of more than 8,784 hours, a leap year (§7.3).
- `ListScopeChanges` filters its events by the tool, the devices and the capture, not by the window:
  a cursor follows every change of the selected connections, whatever hour it lands in.

**Wire.**
The service is `tracekeep.v1.MachineService` in `proto/tracekeep/v1`, its messages the catalog's read results field for field (§7.3):
times are `google.protobuf.Timestamp` (RFC 3339 in JSON, as the selector above), hours their start, ids the canonical text of a UUID, seqs `uint64`, and a time or a seq the catalog may lack is a field with presence.
Every handler has response compression off: connect's default gzip costs a pack listing and a record stream several times their transfer (T-7).
`ListScopeChanges` answers at most its request's limit, 1,000 when the limit is 0; a limit above 10,000 is refused.
A refused request carries a Connect code:

| Code | When |
|---|---|
| `unauthenticated` | no credential, or one auth refuses (§10.2 *Failures*, every reason but `forbidden`) |
| `permission_denied` | `forbidden`: no role of the principal admits the operation |
| `invalid_argument` | a selector, a cursor or a pack id that does not parse or breaks the bounds above, or a `ListScopeChanges` limit above 10,000 |
| `not_found` | a `PackURL` of a pack the catalog does not know, an evicted one included: eviction deletes its hour's rows (`compactor.md` §8.3) |
| `failed_precondition` | `pack-superseded`, with the error detail `tracekeep.v1.PackSuperseded` naming the pack |
| `unavailable` | a `PackURL` before the compactor's first retention record, when no deadline exists (`compactor.md` §8.3), or any read the catalog fails, which the client retries |

Every issued `PackURL` appends an `audit_log` row naming the principal's subject, the capture, the pack and the URL's expiry, never the URL:
the VE service reads packs that hold ciphertext in place (T-5), one row per URL issued.

**Record predicates of `QueryRecords`.**

```json
{
  "selector": { "tool_id": "T1", "devices": ["eqp"], "from": "...", "to": "..." },
  "messages": ["S6F11", "S5F1", "S2F*"],
  "direction": "to_host",
  "kinds": ["data"],
  "include_transport_events": false,
  "limit": 5000,
  "order": "time"
}
```

- `messages` takes `SxFy`, with `*` for any function, instead of separate stream and function fields.
- `direction` is `to_host` (sent by the equipment) or `to_equipment`,
  named for the tool's side the records are observed on (`producer.md` §4.1).
- `kinds` selects among data, control, transport-event and annotation records ([FMT] record header `kind`);
  transport events are excluded unless asked for.
- Filtering on payload content (a CEID, an ALID) is left for a later version.

The VE service's configuration names (`tool_id`, device) pairs;
it lists packs, follows scope changes and fetches `PackURL`s through the selector,
and needs no knowledge of capture ids beyond what `ListPacks` returns.

**Downloads.**
`PackURL` and an export's fetch return a download URL that never outlives its object (T-33).
Each object has a **deadline**, the earliest time it may be deleted:
for every pack, the time retention may first evict its hour (hour end plus the retention period in force, read from the catalog by the one function of §8.3, pushed out by a hold), `staged` and `excluded` packs included,
and for a merged input the earlier of that and its own merge's `merges.delete_at`, `merges.at` + D fixed at the commit (§8.3);
for a completed export bundle, the end of its retention.
The URL expires at the earlier of now plus its lifetime and the deadline minus a margin (default 2 min) that covers clock skew and a read in flight.
When that leaves less than a floor (default 1 min), issuance is refused with `pack-superseded`:
the client re-lists, and a merged input's archive is listed in its place, so the VE client handles `pack-superseded` by re-listing.
D stays above the lifetime, so a URL issued before a merge is untouched by the merge's deletion,
and a later change of the retention period never shortens what an issued URL was promised (§8.3).
Where the store presigns and `storage.presign` is on (`storage.md` §5.2 *Downloads*), the URL is a presigned GET of the object;
otherwise it is a URL on the API itself carrying a download token, and the API serves the object:

- The token is issued only after the issuing operation's role check (`pack-reader` for `PackURL`, `exporter` for an export);
  it names exactly one object key and an expiry, and authorizes nothing else.
- It is signed with the download-token key, a key purpose of its own (`masking-keys.md` §9),
  so any replica validates it without shared state;
  it carries the key id it was signed with, and rotation follows `masking-keys.md` §9's rule for session and download keys.
  Its form is one URL-safe string holding a form version, the key id, the object key and the expiry in Unix seconds,
  authenticated together by an HMAC-SHA-256 under the key id's material;
  a validator checks the key id against the accepted ones before it resolves the key, and compares the MAC in constant time.
- A request after the expiry is refused;
  the expiry is computed as for a presigned URL,
  so an object outlives every token issued for it as it outlives every presigned URL.
- The API answers `HEAD` and ranged `GET` (`Range`, `206 Partial Content`) as S3 does, reading through `ObjectStore.Open`'s `io.ReaderAt`,
  so tracepack's reader works over either kind of URL unchanged.
- A download URL, presigned or not, is a bearer credential until it expires and is never logged.
- A URL on the API is `<api.public_url>/download/<token>`, the token in the path;
  its responses carry `Cache-Control: no-store` and `Referrer-Policy: no-referrer`, and the server logs neither the path nor the token.
- With presigning on, the `api` role presigns a GET of a key that does not exist at start and fetches it:
  not-found means the store accepts the signature, and any other answer fails the start with the advice to turn `storage.presign` off,
  since every URL it issued would be refused.
- The `api` role is ready once its listener is bound and the catalog answers.

### 10.2 Auth

- **People**: OIDC authorization code with PKCE, handled by the Go backend as a backend-for-frontend (`coreos/go-oidc`, `golang.org/x/oauth2`).
  The session is a **signed, encrypted cookie** (a `session` key from the `keys` module, `masking-keys.md` §9, rotated by its rule there) holding subject, roles and expiry,
  so any replica validates it without shared state;
  roles map from IdP groups or claims in configuration.
- **Services** (the VE generation service, the scorer): OAuth2 client credentials from the same IdP, validated against its JWKS;
  mTLS is the in-cluster alternative.
  A request is authenticated by one credential:
  a client certificate the TLS handshake verified, when mTLS is configured, and otherwise its `Authorization: Bearer` token.
  - A **service token** is a compact JWS whose header names a `kid` and an `alg` among RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384 and ES512;
    `none`, a symmetric `alg` and a missing `kid` are refused.
    Its `iss` is the configured issuer, its `aud` holds the configured audience, and its `exp` is required;
    `exp`, `nbf` and `iat` are checked with a leeway (default 1 min, the clock skew of T-33).
    Its subject is `sub`; its roles are the values of a configured claim (default `roles`, a string or a list of strings),
    each mapped to roles by configuration, an unmapped value granting none.
  - An **mTLS principal** is the client certificate verified against the configured client CA:
    its identity is the certificate's first URI name, else its first DNS name,
    mapped to roles by configuration, an unmapped identity granting none.
- **IdP outage**: discovery and JWKS are fetched lazily with retry and cached with a TTL;
  startup never blocks on the IdP;
  an unknown `kid` triggers one refresh, then rejection;
  existing sessions and cached keys keep working until expiry; new logins fail.
  Exactly:
  discovery reads `<issuer>/.well-known/openid-configuration`, whose `issuer` must equal the configured one, for its `jwks_uri`,
  both fetched over https only, every redirect included;
  a JWKS holding no usable public signing key is a failed fetch, which leaves the keys cached before it;
  the JWKS is cached with a TTL (default 1 h) and fetched by the first request that needs it, never at start;
  one fetch is in flight at a time, and concurrent requests wait on it;
  fetches start at least a minimum interval apart (default 10 s), so neither an outage nor a flood of unknown `kid`s drives the IdP harder;
  an unknown `kid` refreshes once when the interval allows and is then rejected;
  cached keys verify until their TTL ends, and with none the request is rejected as `idp-unavailable`.
  The **static-token provider** for a standalone host without an IdP is an explicit flag and is never activated by a failure:
  with the flag on, a bearer token is compared in constant time with each configured static token, read by reference, which names its subject and roles;
  with it off, none is consulted, whatever the IdP's state.
- **Roles**: `pack-reader` (ListPacks, PackURL), `pack-reader` or `viewer` (ListDevices, ListCaptures, ListScopeChanges, T-41), `scorer` (Digest), `viewer`, `exporter`, `restorer`, `admin`;
  separately granted.
  Each operation names the roles any one of which admits it, `admin` alone admitting admin;
  a principal holding none of them, and any operation the table does not name, is refused as `forbidden`.
  A download served by the API checks its token, not a role (§10.1 *Downloads*).
- **Failures** count in `auth_failures_total{reason}` (`operations.md` §10.4), one reason per refused request:
  `missing` (no credential), `malformed`, `alg`, `unknown-kid`, `signature`, `issuer`, `audience`, `expired`, `not-yet-valid`,
  `idp-unavailable`, `certificate` (an mTLS identity the configuration does not name), `static` (a bearer token that matches no static token while no IdP is configured),
  `forbidden`, and `download` (a download token refused).
  The reason is logged with the subject when one is known, never with the credential.

### 10.3 Frontend

`web/` is Vite + React + TypeScript with a connect-web client generated from the same protos;
`make web` builds `web/dist`, which the binary embeds with `go:embed` and `api` serves at `/`,
so a standalone host ships one file;
in development Vite proxies `/api` to the Go server.
The PoC and the first version ship no pages; the layout, the auth flow and the embedding exist from day one.
First pages, when they come:
tool and time browser with scope completeness, record viewer with SML rendering, export, restore, admin.
