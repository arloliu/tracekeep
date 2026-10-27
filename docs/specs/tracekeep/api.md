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
- A request after the expiry is refused;
  the expiry is computed as for a presigned URL,
  so an object outlives every token issued for it as it outlives every presigned URL.
- The API answers `HEAD` and ranged `GET` (`Range`, `206 Partial Content`) as S3 does, reading through `ObjectStore.Open`'s `io.ReaderAt`,
  so tracepack's reader works over either kind of URL unchanged.
- A download URL, presigned or not, is a bearer credential until it expires and is never logged.

### 10.2 Auth

- **People**: OIDC authorization code with PKCE, handled by the Go backend as a backend-for-frontend (`coreos/go-oidc`, `golang.org/x/oauth2`).
  The session is a **signed, encrypted cookie** (a `session` key from the `keys` module, `masking-keys.md` §9, rotated by its rule there) holding subject, roles and expiry,
  so any replica validates it without shared state;
  roles map from IdP groups or claims in configuration.
- **Services** (the VE generation service, the scorer): OAuth2 client credentials from the same IdP, validated against its JWKS;
  mTLS is the in-cluster alternative.
- **IdP outage**: discovery and JWKS are fetched lazily with retry and cached with a TTL;
  startup never blocks on the IdP;
  an unknown `kid` triggers one refresh, then rejection;
  existing sessions and cached keys keep working until expiry; new logins fail.
  The **static-token provider** for a standalone host without an IdP is an explicit flag and is never activated by a failure.
- **Roles**: `pack-reader` (ListPacks, PackURL), `pack-reader` or `viewer` (ListDevices, ListCaptures, ListScopeChanges, T-41), `scorer` (Digest), `viewer`, `exporter`, `restorer`, `admin`;
  separately granted.

### 10.3 Frontend

`web/` is Vite + React + TypeScript with a connect-web client generated from the same protos;
`make web` builds `web/dist`, which the binary embeds with `go:embed` and `api` serves at `/`,
so a standalone host ships one file;
in development Vite proxies `/api` to the Go server.
The PoC and the first version ship no pages; the layout, the auth flow and the embedding exist from day one.
First pages, when they come:
tool and time browser with scope completeness, record viewer with SML rendering, export, restore, admin.
