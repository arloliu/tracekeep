# tracekeep — masking and keys

Status: draft (2026-10-25) — split from seed v21

Covers the redaction policy, the digest and encryption keys, key bindings, key providers, where clear text lives, restore and digest.
From seed v21 §9; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/policy` or `internal/keys`, or anything that handles key material or originals (with `.agents/rules/650-perf-sec.md`).

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Policy**: a `redact.Policy`, versioned with a content hash, compiled once and bound to each capture;
  the default is every row of the baseline table of [SEM §8];
  identifiers stay in clear (§9).
- **Production admission**: a tool type is admitted only after its recipe carriers are checked against the policy (§9; T-6).
- **`redact.Keys` over a `KeyProvider`**: `DigestKey(keyID)` is one key across captures, rotated by declaration;
  `EncryptionKey(capture, keyID)` is derived by the HKDF recipe of §9, so nothing per capture is stored.
- **Bindings**: before a consumer derives or writes with a key it resolves the material and runs an insert-or-compare of its fingerprint;
  a mismatch fails closed, defers every record needing the key and alerts.
- **`keys` module**: `internal/keys` owns HKDF, the fingerprint, the insert-or-compare and the cache;
  a provider only returns material at a reference and version,
  and a bound key id is requested at its pinned version, never as "latest".
  The binding check completes before any derivation or use, and only then is a cache entry published (§9).
- **Purposes**: digest, master, session-cookie and download-token keys are distinct key ids, told apart by the binding's kind (§9).
- **Providers**: `vault` (KV v2, version pinned in the binding), `file` (mode 0600), `env` (development only, refused in production);
  material is held in memory only.
- **Originals**: restored only into an API response, after the audit row is written;
  an export never contains them;
  `Digest` is rate-limited and audited, and the scorer never holds the key.

## 9. Masking, encryption and keys (requirement 7)

- **Policy**: `redact.Policy`, configured in YAML, stored versioned with a content hash, compiled once, bound to each capture at registration (§6.4).
  The **default policy is every row of the baseline table of [SEM §8]** (process-program bodies in S7, data sets in S13, recipes in S15 and S16, S6F25, and the rest of that table), with the paths and domains it gives;
  the table is the normative list and is not repeated here;
  identifiers stay in clear (G5-207).
  A tool type is **admitted to production** only after its manual's recipe carriers are checked against the policy;
  `Mask` leaves an unlisted message unchanged, so the list is the protection.
  Fields beyond recipe bodies are the owner's (LS-8).
- `redact.Keys` is implemented over a `KeyProvider`:
  - `DigestKey(keyID)`: one digest key across captures, tools and message types (G5-220, V-26).
    Rotation is rare and by declaration:
    a new `digest_key_id` applies to new captures, every pack names its digest domain, the old key stays available,
    and digests of two domains are never compared.
  - `EncryptionKey(capture, keyID)` = HKDF-SHA-256 with IKM = the master key of `keyID`, salt = `"tracekeep/capture-key/v1"`, info = the 16 raw bytes of `capture_id` (RFC 9562 order) followed by `keyID` in UTF-8, length 64,
    the `aes-siv-cmac-512` key `redact` expects; deterministic as G5-214 requires, nothing per capture to store.
    The derivation is versioned by its salt; a new version is a new `keyID`.
  - **Bindings, insert-or-compare**: before a consumer derives or writes with a key it resolves the material, computes the fingerprint = HMAC-SHA-256(key = material, message = `"tracekeep/key-fingerprint/v1"`), lowercase hex,
    and runs `INSERT INTO key_bindings` (unique on `key_id`) with provider reference and version;
    on conflict it compares the stored fingerprint with its own;
    a mismatch fails closed: the consumer defers every record needing that key and alerts.
    Two consumers resolving one id for the first time therefore agree or one of them stops.
    A `digest` or `master` `keyID`, like a processing configuration, is retired only under §6.4's condition,
    never merely because its captures are stopped;
    `session` and `download` key ids retire by the rotation rule below.
  - **The `keys` module** (`internal/keys`) owns the HKDF derivation, the fingerprint, the insert-or-compare and the in-memory cache,
    for every role that uses a key;
    a `KeyProvider` only returns the material at a provider reference and version, and derives, compares and caches nothing.
    A key id's provider reference comes from its declaration in configuration;
    the declared version, or the provider's current one when none is declared, applies only while the key id is unbound.
    The binding row records the reference and the version the provider returned and is never updated,
    so it is the key id's immutable mapping:
    a bound key id is always requested at the binding's reference and pinned version, never as "latest".
    `file` and `env` have no versions; for them the fingerprint alone detects changed material.
  - **Order of a resolution**: read the binding;
    resolve the material at the binding's version, or, for an unbound key id, at the declared or current one;
    compute the fingerprint; insert-or-compare.
    Only after the check succeeds does the module derive a capture key, hand out the material or publish a cache entry;
    a failed check caches nothing.
    The order holds for every use: a consumer's write, and the API's restore, digest, session cookie and download tokens.
  - **Which key ids a use may name**: a `session` or `download` key id is one its purpose accepts (below);
    a `digest` or `master` key id is one a policy names (`redact.Policy`'s digest key id and encryption key id),
    and its declaration names that kind.
    A key id that is not declared, or is declared or bound under another kind than the one requested, fails closed before any provider call.
  - **The cache and an outage**: the cache holds bound material and nothing derived;
    a capture key is derived again at each use.
    An entry never expires, since a binding is immutable and so is the material it pins,
    so a warm cache serves every bound key id through a provider outage, and a restart is the only thing that empties it.
    A cold key id is resolved at its first use, never at start, so a start never waits on a provider;
    one resolution of a key id is in flight at a time, and concurrent uses wait on it and share its outcome, failure included, which nothing caches.
  - **Failures** are of two classes, which every caller tells apart:
    *unavailable* (the provider unreachable, its deadline passed, its credential refused or expired) may succeed when retried,
    so ingest defers the records that need the key and the API answers unavailable;
    *refused* (a fingerprint mismatch, a kind mismatch, a key id not declared or not accepted, material that is malformed, shorter than its minimum, or missing at the pinned version)
    stays refused until an operator acts, alerts as `key-binding-mismatch` or, for the rest, as `key-refused`, and defers likewise.
    An error names the key id, its kind, its provider, reference and version, and a provider's status,
    never the material, a token, a secret id or a provider's response body,
    and a log line or a metric holds no more than the error.
  - **`redact.Keys` per use**: its methods take no context,
    so the adapter `redact.Mask` is given is built for one use, a batch's masking or one API request, over that use's context and never kept beyond it;
    a shared resolution runs under the provider's deadline, not under any one use's context,
    so a cancelled use stops waiting and gets no material while the other uses keep waiting.
  - **Key purposes stay distinct**: each key id has one purpose, recorded as its binding's kind:
    `digest` (`DigestKey`), `master` (the IKM of `EncryptionKey`), `session` (the session cookie of `api.md` §10.2) and `download` (the download tokens of `api.md` §10.1).
    A request for a key id under another kind fails closed,
    and no key of one purpose is derived from a key of another.
  - **Session and download keys rotate by configuration** (T-33):
    configuration names, per purpose, one **issuing** key id and a list of **accepted** key ids;
    every cookie and token carries its key id in its authenticated part, and an unknown or wrong-purpose id fails closed.
    Rotation is three steps:
    add the new id to the accepted list on every replica, then switch issuance to it,
    then drop the old id once no replica issues under it and the longest credential lifetime plus the clock tolerance has passed.
    Such a key id is retired when no credential issued under it can still be valid, which says nothing about captures.
- `KeyProvider` implementations:
  `vault` (KV v2 with the version pinned in the binding; Kubernetes auth in the cluster, AppRole on a host; material held in memory only, never on disk, and cached by the `keys` module, below),
  `file` (files a Vault Agent or an operator places, mode 0600, for standalone; a file any other user may read is refused), `env` (development only, refused in production mode: every command but `tracekeep dev` refuses it at load).
  Vault Transit does not fit: the derivation and AES-SIV need the raw master key in process.
  A provider returns the material as base64 text in standard encoding, surrounding white space ignored;
  a `digest`, `master` or `download` key's material holds at least 32 bytes.
- **The `vault` provider**:
  - **Configuration**: the server's address (https only) and the CA that verifies it, the KV v2 mount, and one auth method with its role:
    `kubernetes`, which logs in with the service account token read from its file at each login, since the file rotates,
    or `approle`, whose role id is configuration and whose secret id is read by reference, never written in the file.
  - **A declaration's reference** is the secret's path under the mount;
    the material is the secret's `material` field, and the version is the KV v2 version as a decimal number.
    A version deleted or destroyed is missing, a refusal: a bound key id never falls back to another version.
  - **Credentials**: the provider logs in at its first call, renews its token once two thirds of the token's TTL have passed,
    and logs in afresh when a renewal fails, when the token is not renewable or its maximum TTL is near, and once when a read is refused as forbidden, retrying that read once.
  - **A deadline** bounds every call to the server, login, renewal and read alike (default 10 s), beside the caller's context.
  - **Versions are kept as long as data needs them**: a pinned version of a `digest` or `master` key id is never deleted or destroyed,
    by an operator or by the mount's `max_versions` or `delete_version_after`,
    while a retained pack of a capture bound to it remains, since `Restore` and `Digest` need it after every producer is quiescent;
    a `download` or `session` key id's version may go once the key id is retired.
  `file` and `env` return an empty version, which the binding records.
  A key id's declaration names its provider and reference;
  the issuing key id of a purpose is one of its accepted key ids, and every accepted key id is declared.
- Where the data lives:
  clear text on the traffic stream and in the claim bucket (TLS, encryption at rest, G5-213) and in a consumer's memory before masking;
  in storage only masked packs with the originals as AES-SIV ciphertext in place;
  restored originals only in an API response, never in a file, an export or a temporary object.
- **Restore** (`restorer` role) returns one record's original in the response and writes an `audit_log` row first;
  an export never contains originals.
- **Digest** (`scorer` role, for the VE's protected scorer, V-26):
  `Digest(digest_key_id, domain, kind, original)` with the three input forms `redact.Digest` defines:
  `item` (the encoded SECS-II item, header included, for a resolved path), `text` (the whole message text, for a whole-text mask), `annotation` (the value bytes);
  it returns the digest with the key id and domain it used, rate-limited and audited; the scorer never holds the key.
