# tracekeep — API, auth and frontend

Status: draft (2026-10-25) — split from seed v21

Covers the machine API and its operations, authentication and roles for people and services, and the embedded frontend.
From seed v21 §10.1–§10.3; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `proto/`, `internal/api`, `internal/auth` or `web/`.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes under discussion are in [P1](proposals/p1-interfaces.md).

- **Contract**: protobuf served by connect-go, gRPC for Go clients and JSON over HTTP for the browser, from one definition (§10.1).
- **Operations**: `ListCaptures`, `ListPacks`, `ListScopeChanges`, `PackURL`, `QueryRecords`, `FindTransaction`, `Export`, `Restore`, `Digest`, admin (§10.1 table).
- **`PackURL`**: a presigned read with a lifetime below D; it makes the VE service a privileged reader of packs (T-5).
- **Exports**: an asynchronous job producing a bundle with one JSONL file per source pack, by that pack's `format_minor`; a filtered `.tpk` is not offered (§10.1).
- **People**: OIDC authorization code with PKCE in a backend-for-frontend; a signed, encrypted session cookie any replica validates (§10.2).
- **Services**: OAuth2 client credentials validated against the IdP's JWKS, or mTLS in the cluster; startup never blocks on the IdP (§10.2).
- **Roles**: `pack-reader`, `scorer`, `viewer`, `exporter`, `restorer`, `admin`, granted separately (§10.2).
- **Frontend**: `web/` built into the binary and served at `/`; no pages in the first version (§10.3).
- **Gate**: connect-go's overhead against plain gRPC is measured before the API is implemented (T-7).

## 10. API, auth and frontend (requirements 5 and 6)

### 10.1 Machine API first

The VE service is the main client, so the contract is protobuf served by connect-go: gRPC for Go clients, JSON over HTTP for the browser, one definition.

| Operation | Answers |
|---|---|
| `ListCaptures(tool, from, to)` | captures with state, `recorder_instance_id`, `previous_capture_id`, unregistered intervals (§7.3), thinning parameters, processing configuration ids |
| `ListPacks(tool, from, to)` | per hour and scope: state, revision, packs with their ranges, gaps with time intervals, declared losses, crash gaps, conflicts, unknown tail, `no-data` hours (§7.3) — LS-10 |
| `ListScopeChanges(after seq)` | `scope_events` after a cursor, paginated, with the epoch — LS-11 |
| `PackURL(pack_id)` | a presigned read (lifetime < D) so the VE reads packs with tracepack's own reader; this makes the VE service a privileged reader of packs that hold ciphertext in place (§13) |
| `QueryRecords(filter)` | streamed masked records by tool, window, stream, function, direction, capture, seq range, epoch, via `MergeIterate` |
| `FindTransaction(capture, seq, hour)` | tracepack's lookup |
| `Export(filter, kind)` | an asynchronous job producing a **bundle** (tar) under `exports/`, fetched by presigned URL: one JSONL file **per source pack**, `tracepack-jsonl/1` or `/2` by that pack's `format_minor` (G5-233), plus `manifest.json` naming packs, captures, hours, each pack's incompleteness from its read, conflicts, and the filter. `kind=canonical` uses the canonical exporter unfiltered; `kind=filtered` writes the same record schema with the filter applied and is declared non-canonical in the manifest. A filtered `.tpk` is an extract, a v1.0 non-goal, and is not offered. A cancelled or failed job deletes its partial objects |
| `Restore`, `Digest` | §9 |
| admin | policies, processing configurations, key ids (no material), retention, thinning status, holds if LS-13 (a) is chosen |

### 10.2 Auth

- **People**: OIDC authorization code with PKCE, handled by the Go backend as a backend-for-frontend (`coreos/go-oidc`, `golang.org/x/oauth2`).
  The session is a **signed, encrypted cookie** (key from the `KeyProvider`) holding subject, roles and expiry, so any replica validates it without shared state;
  roles map from IdP groups or claims in configuration.
- **Services** (the VE generation service, the scorer): OAuth2 client credentials from the same IdP, validated against its JWKS; mTLS is the in-cluster alternative.
- **IdP outage**: discovery and JWKS are fetched lazily with retry and cached with a TTL; startup never blocks on the IdP;
  an unknown `kid` triggers one refresh, then rejection; existing sessions and cached keys keep working until expiry; new logins fail.
  The **static-token provider** for a standalone host without an IdP is an explicit flag and is never activated by a failure.
- **Roles**: `pack-reader` (ListPacks, PackURL), `scorer` (Digest), `viewer`, `exporter`, `restorer`, `admin`; separately granted.

### 10.3 Frontend

`web/` is Vite + React + TypeScript with a connect-web client generated from the same protos;
`make web` builds `web/dist`, which the binary embeds with `go:embed` and `api` serves at `/`, so a standalone host ships one file;
in development Vite proxies `/api` to the Go server.
The PoC and the first version ship no pages; the layout, the auth flow and the embedding exist from day one.
First pages, when they come: tool and time browser with scope completeness, record viewer with SML rendering, export, restore, admin.
