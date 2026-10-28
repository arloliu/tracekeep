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
  `vault` (KV v2 with the version pinned in the binding; Kubernetes auth in the cluster, AppRole on a host; material held in memory only, never on disk, and cached by the `keys` module),
  `file` (files a Vault Agent or an operator places, mode 0600, for standalone; a file any other user may read is refused), `env` (development only, refused in production mode: every command but `tracekeep dev` refuses it at load).
  Vault Transit does not fit: the derivation and AES-SIV need the raw master key in process.
  A provider returns the material as base64 text in standard encoding, surrounding white space ignored;
  a `download` key's material holds at least 32 bytes.
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
