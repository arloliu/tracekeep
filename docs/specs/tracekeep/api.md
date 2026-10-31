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
- **People**: OIDC authorization code with PKCE in a backend-for-frontend, on the IdP of `auth.oidc`, optional;
  an encrypted session cookie any replica validates, admitted on the browser's operations only, behind an `Origin` check (§10.2 *People's sign-in*).
- **Services**: OAuth2 client credentials validated against the IdP's JWKS, or mTLS in the cluster;
  startup never blocks on the IdP (§10.2).
- **Roles**: `pack-reader`, `scorer`, `viewer`, `exporter`, `restorer`, `admin`, granted separately (§10.2).
- **Frontend**: `web/` built into the binary and served at `/`; the first pages are the tool and time browser, the record viewer and export (§10.3).
- **Records**: `QueryRecords` streams masked records with their SML, capture by capture and hour by hour, each capture with its completeness, unknown tail, withholding and unreadable parts,
  in batches capped in encoded bytes, up to a limit, resumable by a cursor;
  `FindTransaction` answers tracepack's lookup over the same read set (§10.1 *Records*).
- **Gate**: connect-go's overhead against plain gRPC is measured before the API is implemented (T-7).

## 10. API, auth and frontend (requirements 5 and 6)

### 10.1 Machine API first

The VE service is the main client, so the contract is protobuf served by connect-go:
gRPC for Go clients, JSON over HTTP for the browser, one definition.

| Operation | Answers |
|---|---|
| `ListDevices(tool, from, to)` | per device: name, `equipment_endpoint`, `device_id`, how many captures are open, whether any captures overlap (§7.3 *Overlap*), the last record time, whether the window holds a gap |
| `ListCaptures(selector)` | captures with state, device, `recorder_instance_id`, `previous_capture_id`, unregistered intervals and the `overlap` flag (§7.3), thinning parameters (each entry's stream, function and `source`, with `keep_every_n` and `keep_interval_ms` when its `source` parses as `producer.md` §4.6's convention, T-47), processing configuration ids |
| `ListPacks(selector)` | per hour and scope: state, revision, packs with their ranges, gaps with time intervals, deferred seqs, declared losses with their action ids, crash gaps, conflicts, unknown tail, `no-data` hours (§7.3) — LS-10 |
| `ListScopeChanges(after seq, selector)` | `scope_events` (`created`, `settled`, `reopened`, `conflict`, `terminated`, `excluded`, `unmergeable`) after a cursor, paginated, with the epoch — LS-11; one global cursor, returning only the selected tool's and devices' events |
| `PackURL(pack_id)` | a download URL (expiry clamped to the object's deadline, or `pack-superseded`, *Downloads* below) so the VE reads packs with tracepack's own reader; this makes the VE service a privileged reader of packs that hold ciphertext in place (§13) |
| `QueryRecords(selector, predicates)` | streamed masked records with their SML, capture by capture and hour by hour, each capture opened with its completeness in the window and closed with its unknown tail, withheld captures and unreadable parts in the stream where they fall; the predicates below (*Records* below) |
| `FindTransaction(capture, seq, hour)` | tracepack's lookup of the transaction of the record at (capture, seq) in hour, over the read set of each hour it reads: its outcome, its records with their roles and SML, and what it could not read (*Records* below) |
| `Export(filter, kind)` | an asynchronous job producing a **bundle** (tar) under `exports/`, fetched by a download URL (*Downloads* below): one JSONL file **per source pack**, `tracepack-jsonl/1`, `/2` or `/3` by that pack's `format_minor` 0, 1 or 2 (G5-233, G5-270), plus `manifest.json` naming packs, captures, hours, each pack's incompleteness from its read, conflicts, and the filter. `kind=canonical` uses the canonical exporter unfiltered; `kind=filtered` writes the same record schema with the filter applied and is declared non-canonical in the manifest; it is refused until go-secs exports a filtered writer, since the record schema is tracepack's (T-48). A filtered `.tpk` is an extract, a v1.0 non-goal, and is not offered. A cancelled or failed job deletes its partial objects; a completed bundle is kept for its retention (default 24 h) and then deleted by the compactor (T-33) |
| `Restore(capture, seq, hour)`, `Digest(digest_key_id, inputs)` | `masking-keys.md` §9; `Restore` returns a record only when every non-excluded copy of `(capture, seq)` is equal, in any hour, the hour naming one that holds a copy (otherwise not found); differing versions are refused as a conflict, an acknowledged one included, and none is selectable (T-42); `Digest` digests a list of inputs under one digest key id (T-44) |
| admin | policies, processing configurations, key ids (no material), retention, thinning status (tracekeep keeps no holds, T-4); until these are typed operations, §10.4's `policy`, `config` and `key` commands serve them through `Admin(words)` |
| `Admin(words)` | the `tracekeep admin` command those words name, run by the `api` process, its JSON result returned: the transport of §10.4's commands while that process holds a SQLite catalog |

**Admin over the API.**
A running service holds its SQLite catalog alone (§7.4), so `tracekeep admin` or `tracekeep status` finding it held sends the command's words to that service's `AdminService.Admin`,
which runs the same command there and returns the JSON the command prints.
It checks the `admin` role and audits the command as every admin command is audited, the actor being the principal's subject;
the CLI's `--actor` names the actor of the direct path only.
The CLI authenticates with the configuration's static token that holds `admin` (`auth.static`, its flag on),
reaching the service at `api.public_url` and trusting its certificate (`api.tls_cert_file`) beside the system's roots;
without such a token it refuses and says so.
The typed admin operations of the table above wait for a later phase, with the admin page (T-48).

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
| `invalid_argument` | a selector, a cursor or a pack id that does not parse or breaks the bounds above, a `ListScopeChanges` limit above 10,000, a request that does not decode, a `Restore` or `FindTransaction` hour that is not an hour start, a `QueryRecords` predicate or cursor *Records* refuses, or a `Digest` input its checks refuse (`masking-keys.md` §9), naming the input's index |
| `not_found` | a `PackURL` of a pack the catalog does not know, an evicted one included: eviction deletes its hour's rows (`compactor.md` §8.3); a `Restore` whose hour holds no copy of the record |
| `failed_precondition` | `pack-superseded`, with the error detail `tracekeep.v1.PackSuperseded` naming the pack; a `Restore` of differing versions, with the detail `tracekeep.v1.RecordConflict`, or of a record that is not restorable; a key refused (`masking-keys.md` §9 *Failures*); a `FindTransaction` of a withheld capture, or of a record that is not a transaction's primary and that nothing read explains |
| `unavailable` | a `PackURL` before the compactor's first retention record, when no deadline exists (`compactor.md` §8.3), any read the catalog fails, a key unavailable, or a copy of a record whose object cannot be read, which the client retries |
| `resource_exhausted` | a `Digest` beyond its principal's rate limit, a request message above 16 MiB, refused before it is decoded, or a record read beyond the replica's concurrent reads, a `FindTransaction` hour beyond its pack bound, or tracepack's read limit in a lookup (*Records* below) |
| `internal` | an audit row that could not be written; nothing is returned |

`Restore` answers the original payload with the record's time and kind; `Digest` answers each input's digest in order with the key id.
Neither original nor digest appears in an error, a log line or an audit row;
a request that does not decode is refused with one fixed message, which quotes none of its bytes.

Every issued `PackURL` appends an `audit_log` row naming the principal's subject, the capture, the pack and the URL's expiry, never the URL:
the VE service reads packs that hold ciphertext in place (T-5), one row per URL issued.

**Record predicates of `QueryRecords`.**

```json
{
  "selector": { "tool_id": "T1", "capture_id": "0192f7a4-5c1e-7d3a-9b2e-4f6a8c0d1e2f", "from": "...", "to": "..." },
  "messages": ["S6F11", "S5F1", "S2F*"],
  "direction": "to_host",
  "kinds": ["data"],
  "include_transport_events": false,
  "seq_from": 1200,
  "seq_to": 1800,
  "limit": 5000,
  "cursor": "..."
}
```

- `messages` takes `SxFy`, with `*` for any function, instead of separate stream and function fields;
  a stream or function above 255, or any other form, is refused as invalid.
- `direction` is `to_host` (sent by the equipment) or `to_equipment`,
  named for the tool's side the records are observed on (`producer.md` §4.1).
- `kinds` selects among data, control, transport-event and annotation records ([FMT] record header `kind`);
  transport events are excluded unless asked for, by `include_transport_events` or by naming the kind.
- `seq_from` and `seq_to`, inclusive, require the selector's `capture_id`, since seqs are a capture's, and select producer seqs, so the close record is outside any range;
  a reversed range is refused.
- `limit` counts records only, 5,000 when 0; a limit above 50,000 is refused.
- `cursor` is the token a previous answer of the same selector and predicates returned;
  one that does not parse, or that another selector or other predicates issued, is refused.
- Filtering on payload content (a CEID, an ALID) is left for a later version.
  Records are ordered by capture, hour and seq (*Records* below), so the earlier `order` predicate is not offered.

**Records.**
`QueryRecords` and `FindTransaction` read masked packs only (T-48): the read set of each capture and hour (`catalog.md` §7.3),
never a pack's clear original, which `Restore` alone returns.
Outside `tracekeep dev`, a capture the read set names withheld streams no record and a `FindTransaction` of it is refused; under `tracekeep dev` nothing is withheld.
Every read set a read reads is checked, so a resumed stream, whatever its cursor says, reads no withheld capture's pack.

- **Order**: the selected captures in the order of their origin, then capture id;
  each capture's hours of the window in order, read when the stream reaches them;
  each hour's records in seq order, every version of a seq together.
  Captures are never interleaved, so overlapping captures of a device are answered side by side, each with its own markers (§7.3 *Overlap*).
  An hour is a record's UTC hour (tracepack's), so after a backward clock step a record can follow a higher seq of an earlier hour;
  the order is hour first, then seq, and says so rather than reordering across hours.
- **One merge per cluster**: an hour's read set is split into clusters of packs whose **envelopes** overlap, read in envelope order,
  each one merge of tracepack's in its per-capture seq order,
  so identical copies of a seq collapse to one and differing versions are each streamed, flagged as a conflict.
  A pack's envelope runs from the lowest to the highest seq of its ranges,
  and for an archive or a close pack, which may hold the close sentinel that ranges never name (§7.3), on to the sentinel;
  a pack without ranges, a close pack's usual form, has the sentinel alone,
  so a close record always reads after every producer seq of its hour, and an archive and the segments registered after it share a cluster.
  Overlapping captures are separate merges, since neither of tracepack's merge orders keeps captures apart.
- **Filters**: the window, the kinds and the direction are tracepack's filter.
  The `messages` list and the seq range are applied by tracekeep to each record the merge yields, the list as one or-ed predicate from the record's HSMS header;
  only a list of exact `SxFy` entries alone may also be passed to tracepack's filter, since tracepack would refuse a record a wildcard entry admits.
  An unobserved record (thinned by the producer) carries no message and matches no `messages` entry;
  otherwise it is streamed and flagged unobserved.
- **Items**: a record carries its seq, time, epoch, kind, direction, HSMS header (session id, stream, function, W bit, system bytes) when it has one,
  its flags (conflict, excluded, unobserved), its masked payload, and, for a data record, its SML (point 4 of T-48).
  A producer record is flagged excluded when no `staged` pack of its read set holds its seq, by the ranges the read set carries,
  and a close record when no `staged` archive or close pack is in its read set;
  a conflict's versions are flagged by the conflict, whose packs and their states the item names.
  The SML is go-secs' rendering of the decoded SECS-II body as valid UTF-8, each byte that is not (a JIS8 string's, say) written as U+FFFD, the masked payload keeping the bytes;
  a text above 64 KiB is cut at the last character boundary below it and flagged truncated;
  a body that does not decode, or a rendering that fails, carries the error's text instead, and the record is streamed all the same.
- **Markers**: `capture` opens each capture with its `ListCaptures` fields and its **completeness** (§7.3) as far as it meets the window:
  each gap, evicted run, deferred seq and declared loss whose seqs or time interval meet the window, in seq order, an entry without a seq last,
  so a client places each by its seqs and times, wherever the stream's own order would put it.
  A completeness that would take a message past its size is cut to fit and flagged truncated; `ListPacks` gives it whole.
  `withheld` follows `capture` for a withheld capture, and no record of it follows, only its `unknown_tail`.
  `incomplete` stands where a read stopped, naming the reason, the packs (at most 100, the rest counted) and the seq or time span the stream lacks:
  an object gone, a corrupt block, a crash gap or a footer error that tracepack reports, tracepack's read limit or a block above its limit, a cluster above the reader bound below, or an hour retention evicted, before its read or during it.
  Its detail never names an object key, which a store's errors quote, and neither does a read's error;
  a store that fails a read, even while a pack is opened, fails the stream as `unavailable`, never reads as a corrupt pack.
  `unknown_tail` closes each capture with the seq the frontier leaves unknown and whether the capture is still open.
- **Batches**: each stream message holds items until their encoding in the stream's codec, binary or JSON (payloads in base64), reaches 128 KiB less a kilobyte for its cursor and end, the measurement of T-7;
  a message never splits the versions of one seq, and a seq's versions larger than the cap travel alone.
  Each message carries the cursor after its last item.
- **The end**: the last message is `end`, with the number of records streamed, whether any `incomplete` marker or completeness entry was streamed,
  and, when the limit stopped the stream, the cursor to continue from.
  The limit stops the stream only between seqs, so the versions of its last seq may take it past the limit.
  A stream that stops without `end` was cut short:
  an error after partial output ends it with its code, and the client continues from the last cursor it received.
- **The cursor** names the selector's and predicates' digest, the capture, and either its start or the hour and the last seq streamed, all its versions included;
  it resumes after that seq, or at the capture's start, its markers again, so a merge, which keeps every seq, never moves it.
  On resumption a registration of a later hour, or above the cursor's seq in its hour, is read;
  one at or below it, a new version of the cursor's own seq included, is not, and a client re-queries for it.
- **The snapshot**: each (capture, hour) read set is read from one catalog snapshot when the stream reaches it, and its cluster's packs are opened when the cluster starts.
  An object found gone, at its open or during the merge, re-reads that hour's read set once and reads it again from the hour's last seq streamed, skipping every seq at or below it, so nothing streamed twice;
  gone again, or the hour's scope evicted, the rest of the hour is reported `incomplete`.
  A merged input usually still serves for the deletion delay (`compactor.md` §8.3), but a stream's length is not bounded by it, so the re-read is what a reader relies on.
- **Bounds**: a replica serves at most `api.records.max_reads` reads at once, `QueryRecords` streams and `FindTransaction` lookups together (default 8), refusing one more as `resource_exhausted` before reading.
  A `QueryRecords` merge opens at most `api.records.max_readers` packs (default 16), and a cluster above that is not read and is reported `incomplete`;
  a `FindTransaction` lookup passes an hour's whole read set, and an hour of more than `api.records.max_lookup_packs` packs (default 64) is refused as `resource_exhausted`, naming the hour.
  A merge holds at most about `api.records.held_bytes` of blocks (default 64 MiB, tracepack's `MaxHeldBytes`, which a block may overrun by an eighth), a lookup also its state (tracepack's `MaxStateBytes`, 64 MiB),
  and each open pack its window, up to 4 MiB, and its decoded metadata, whose raw size tracepack's budget caps at 64 MiB (T-45).
  These are counts and tracepack's own budgets, not a ceiling on a read's heap; a replica's memory is sized by `max_reads` times them.
  Segments of one hour rarely overlap in seq, so an hour left unmergeable (T-45) splits into small clusters.
- **`FindTransaction(capture, seq, hour)`** runs tracepack's lookup with a pack source answering each hour it asks for with that hour's read set, opened when asked;
  the objects behind an hour close when the lookup returns,
  or, once tracekeep reads tracepack's release rule (v0.12.0), when it asks for the next hour, the readers staying referenced until it returns;
  it reads at most tracepack's default of two hours.
  It answers the outcome (matched, ambiguous or not found), the transaction's key, each record read with tracepack's roles (the primary, a candidate reply, a possible reply, a same-key or possible primary, a closing or outcome record), whether it is in the window and a valid match, its flags and SML as above,
  the hours searched, and each gap, conflict and unreadable pack tracepack reports.
  A withheld capture, or a record that is not a primary and that nothing read explains, is refused as `failed_precondition`;
  tracepack's read limit is refused as `resource_exhausted`, and an object that cannot be read as `unavailable`.

The VE service's configuration names (`tool_id`, device) pairs;
it lists packs, follows scope changes and fetches `PackURL`s through the selector,
and needs no knowledge of capture ids beyond what `ListPacks` returns.

**Downloads.**
`PackURL` and an export's fetch return a download URL that never outlives its object (T-33).
Each object has a **deadline**, the earliest time it may be deleted:
for every pack, the time retention may first evict its hour (hour end plus the retention period in force, read from the catalog by the one function of §8.3), `staged` and `excluded` packs included,
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

- **People**: OIDC authorization code with PKCE, handled by the `api` role as a backend-for-frontend (*People's sign-in* below).
  The session is an **encrypted, authenticated cookie** (a `session` key from the `keys` module, `masking-keys.md` §9, rotated by its rule there) holding subject, roles and expiry,
  so any replica validates it without shared state;
  roles map from an ID token claim in configuration.
- **Services** (the VE generation service, the scorer): OAuth2 client credentials from the same IdP, validated against its JWKS;
  mTLS is the in-cluster alternative.
  A request is authenticated by one credential, chosen before any is checked:
  a client certificate the TLS handshake verified, when mTLS is configured;
  otherwise its `Authorization` header, when it has one;
  otherwise its session cookie, when people's sign-in is configured.
  A chosen credential that is refused refuses the request: no other credential is tried.
  - A **service token** is a compact JWS whose header names a `kid` and an `alg` among RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384 and ES512;
    `none`, a symmetric `alg` and a missing `kid` are refused.
    Its `iss` is the configured issuer, its `aud` holds the configured audience, and its `exp` is required;
    `exp`, `nbf` and `iat` are checked with a leeway (default 1 min, the clock skew of T-33).
    Its subject is `sub`; its roles are the values of a configured claim (default `roles`, a string or a list of strings),
    each mapped to roles by configuration, an unmapped value granting none.
  - An **mTLS principal** is the client certificate verified against the configured client CA:
    its identity is the certificate's first URI name, else its first DNS name,
    mapped to roles by configuration, an unmapped identity granting none.
    The client CA, like the API's own certificate and key, is read again when its file changes,
    so a renewal needs no restart;
    a change that does not load keeps the previous material.
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
  It is a service credential: people's sign-in never accepts it, so a host where people sign in names an IdP (T-48).
- **Roles**: `pack-reader` (PackURL), `pack-reader` or `viewer` (ListDevices, ListCaptures, ListPacks, ListScopeChanges, T-41), `viewer` (QueryRecords, FindTransaction), `scorer` (Digest), `exporter`, `restorer`, `admin`, separately granted;
  `ListPacks` is the completeness view the browser shows, and grants no download, which `PackURL` alone issues.
  Each operation names the roles any one of which admits it, `admin` alone admitting admin;
  a principal holding none of them, and any operation the table does not name, is refused as `forbidden`.
  A download served by the API checks its token, not a role (§10.1 *Downloads*).
- **Failures** count in `auth_failures_total{reason}` (`operations.md` §10.4), one reason per refused request:
  `missing` (no credential), `malformed`, `alg`, `unknown-kid`, `signature`, `issuer`, `audience`, `expired`, `not-yet-valid`,
  `idp-unavailable`, `certificate` (an mTLS identity the configuration does not name), `static` (a bearer token that matches no static token while no IdP is configured),
  `forbidden`, `download` (a download token refused),
  and for people: `session` (a session cookie that does not decode, names a key id not accepted, does not verify or has expired),
  `session-scope` (a session cookie on an operation a cookie does not admit), `csrf` (a cookie-borne request whose method or `Origin` the check refuses),
  `login` (a callback without a valid login cookie, with another state, or with the IdP's error), `exchange` (the token endpoint's refusal or no answer within its deadline)
  and `nonce` (an ID token whose nonce is not the login's);
  an ID token's other refusals take the service token's reasons.
  The reason is logged with the subject when one is known, never with the credential.

**People's sign-in.**
It is configured by `auth.people.client_id` and needs `auth.oidc.issuer`, `api.public_url` (https) and a `keys.session` key: people sign in at the IdP that issues service tokens, with a client of their own.
An ID token never passes as a service token: the client id differs from `auth.oidc.audience`, which the configuration refuses otherwise,
and a service token whose `aud` holds the client id is refused as `audience`.
Its cookies are `Secure`, so it needs the API served over TLS; `tracekeep dev`, which serves plain HTTP, offers none.
Without a client id, the routes below answer not found, no cookie is read, and people have no way in.
Every answer under `/auth/` carries `Cache-Control: no-store`, and the callback's also `Referrer-Policy: no-referrer`, since its URL holds the code and the state.

- **Discovery** is the service-token verifier's (*IdP outage* above), which reads `authorization_endpoint` and `token_endpoint` beside `jwks_uri`,
  each https, under the same fetch rules and cache;
  a document without either is a failed fetch for a login,
  and a login or a callback with neither cached and no fetch allowed or succeeding answers `idp-unavailable`.
  An ID token is verified against the same cached keys, an unknown `kid` refreshing them under the same rules.
- **`GET /auth/login?return=<path>`** starts a login and changes no server state:
  it draws a PKCE verifier, a state and a nonce, 32 random bytes each, base64url;
  sets the login cookie holding them, the return path and its expiry (`auth.people.login_lifetime`, default 10 min);
  and redirects (302) to the authorization endpoint with `response_type=code`, the client id, the redirect URL `<api.public_url>/auth/callback`,
  the scopes (`auth.people.scopes`, default `openid`, which is always sent), the state, the nonce and the S256 challenge.
  The return path, once decoded from the query, is kept only when it is a path on this origin:
  printable ASCII without a space, a `\` or any control, starting with one `/` not followed by another, and parsing as a URL with no scheme and no host;
  otherwise it is `/`, and the `Location` the callback sends is that checked path, never a value rebuilt from it.
  If discovery cannot be had (above), the login answers 503 with a fixed page, refused as `idp-unavailable`.
  A forged login cannot sign a browser in: the callback needs that browser's own login cookie and state.
- **`GET /auth/callback?code=…&state=…`** finishes it.
  Every answer clears the login cookie.
  It refuses as `login` a request without a valid login cookie (absent, refused like a session cookie, or expired), with a state other than the cookie's (compared in constant time), or carrying the IdP's `error`;
  then it exchanges the code at the token endpoint, a POST of `grant_type=authorization_code`, the code, the redirect URL and the verifier,
  the client authenticated by HTTP Basic with its id and its secret (`auth.people.client_secret_ref`, read by reference),
  over https, bounded by `auth.people.exchange_timeout` (default 10 s), its refusal, non-JSON answer or timeout refused as `exchange`;
  then it verifies the answer's `id_token`, sets the session cookie and redirects (303) to the return path.
  A refusal answers a fixed page: 400 for `login` and the ID token's refusals, 502 for `exchange`, 503 for `idp-unavailable`.
  A code is consumed once: the callback clears the login cookie, and the IdP refuses a second exchange of its code,
  so of two callbacks carrying one code, on one replica or two, at most one signs in.
  An authorization request the browser repeats (two tabs of one login) can bring two codes for the one login cookie, each a sign-in of the same person and nobody else's.
  The access and refresh tokens are discarded: nothing tracekeep serves needs them, and a session ends at its expiry, when the person signs in again.
- **The ID token** is a compact JWS checked as a service token is (`alg`, `kid`, signature, `iss`, `exp` required, `nbf` and `iat`, the leeway),
  except that its `aud` must hold the client id, an `azp` must be the client id and is required when `aud` holds more than one value,
  its `nonce` must be the login cookie's, and its `sub` must not be empty.
  Its roles are the values of `auth.people.roles_claim` (default `groups`, a string or a list of strings), each mapped by `auth.people.roles`, an unmapped value granting none;
  a person with no role signs in and is refused every operation as `forbidden`.
- **`POST /auth/logout`** clears the session cookie (204) after the `Origin` check below; it ends nothing at the IdP.
  A cookie cannot be revoked before its expiry, so `auth.people.session_lifetime` (default 8 h) bounds what a stolen one is worth.
- **`GET /auth/me`** answers the signed-in principal (subject, roles, the session's expiry) as JSON, or 401 without a valid session cookie; it is how the frontend learns who is signed in.
- **Cookies**: `__Host-tk_login` and `__Host-tk_session`, each `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/` and without `Domain`;
  `Lax` lets the IdP's top-level redirect back carry the login cookie, and keeps the session cookie off every cross-site subrequest.
  A value is base64url (no padding) of a form version, the purpose (login or session), the key id, a 12-byte nonce and the AES-256-GCM sealing of the payload,
  the nonce drawn afresh from the system's cryptographic randomness at every sealing, never a counter, so replicas sharing a key never repeat one;
  random 12-byte nonces keep GCM within its bound for 2^32 sealings under one key, far above the logins and sign-ins a session key id issues before its rotation,
  the version, purpose and key id being the additional data, so a login cookie never passes as a session cookie, nor one key id's cookie as another's.
  The AES key is derived from the session key id's material by HKDF-SHA-256 with the purpose in its info, one key per purpose.
  A session's payload holds the subject, roles, issue time and expiry; a login's, the verifier, state, nonce, return path and expiry.
  A value longer than 4,096 bytes is refused before it is decoded;
  a sign-in whose cookie would be longer is refused as `login`, which a role claim with too many values causes.
  A validator checks the form version, the purpose, then the key id against the accepted ones before it resolves the key,
  then opens the sealing, then the expiry, allowing the clock tolerance of `auth.oidc.leeway`.
- **What a cookie admits**: a request authenticated by its session cookie is admitted only
  as a `POST` whose `Origin` is `api.public_url`'s origin (scheme, host and port), so a cross-site page can neither send it nor read its answer, else `csrf`;
  and only on the operations the browser uses: `ListDevices`, `ListCaptures`, `ListPacks`, `ListScopeChanges`, `QueryRecords` and `FindTransaction` now,
  and the export job's operations when they land (phase 4.3), else `session-scope`.
  `PackURL`, `Restore`, `Digest` and `Admin` never admit a cookie.
  The role check follows, as for every principal; the principal's `Via` is `session`.
  `/download/` checks its token and never reads a cookie (§10.1 *Downloads*);
  `/auth/login`, `/auth/callback` and `/auth/me` are GETs that change no server state, and `/auth/logout` is a POST under the `Origin` check.
- **Nothing secret is written**: no code, verifier, state, nonce, token, client secret or cookie value appears in a log line, a metric label or an error;
  the state, the nonce and the challenge leave only in the authorization redirect, and a cookie value only in its `Set-Cookie`.
  The callback's error page is fixed text naming the reason, never the IdP's `error_description`.
- **Counting**: `logins_total{step=start,callback,outcome=ok,refused,unavailable}` counts every login started and every callback answered (`operations.md` §10.4); a refusal also counts its reason in `auth_failures_total`.

### 10.3 Frontend

`web/` is Vite + React + TypeScript with a connect-web client generated from the same protos;
`make web` builds `web/dist`, which the binary embeds with `go:embed` and `api` serves at `/`,
so a standalone host ships one file;
in development Vite, served over https, proxies `/auth/`, `/download/` (ranged `GET` and `HEAD` included) and the services' paths to the Go server, and `api.public_url` names the Vite server, so the cookies, the `Origin` check and the download URLs hold unchanged.
The embedding package embeds a tracked fallback page beside `web/dist`,
so the Go build needs no Node, and a binary built without `make web` serves the fallback, which says the frontend was not built;
a release binary and the image are built after `make web` and never ship the fallback (T-48).
The first pages are the tool and time browser with scope completeness, the record viewer with SML the server renders from go-secs, and export;
the restore and admin pages come later (T-48).
