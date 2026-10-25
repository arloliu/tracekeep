# tracekeep — masking and keys

Status: draft (2026-10-25) — split from seed v21

Covers the redaction policy, the digest and encryption keys, key bindings, key providers, where clear text lives, restore and digest.
From seed v21 §9; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/policy` or `internal/keys`, or anything that handles key material or originals (with `.agents/rules/650-perf-sec.md`).

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes under discussion are in [P1](proposals/p1-interfaces.md).

- **Policy**: a `redact.Policy`, versioned with a content hash, compiled once and bound to each capture; the default is every row of the baseline table of [SEM §8]; identifiers stay in clear (§9).
- **Production admission**: a tool type is admitted only after its recipe carriers are checked against the policy (§9; T-6).
- **`redact.Keys` over a `KeyProvider`**: `DigestKey(keyID)` is one key across captures, rotated by declaration; `EncryptionKey(capture, keyID)` is derived by the HKDF recipe of §9, so nothing per capture is stored.
- **Bindings**: before a consumer derives or writes with a key it resolves the material and runs an insert-or-compare of its fingerprint; a mismatch fails closed, defers every record needing the key and alerts.
- **Providers**: `vault` (KV v2, version pinned in the binding), `file` (mode 0600), `env` (development only, refused in production); material is held in memory only.
- **Originals**: restored only into an API response, after the audit row is written; an export never contains them; `Digest` is rate-limited and audited, and the scorer never holds the key.
- **Open**: whether derivation and bindings sit above the providers is not fixed by §9; the repository layout (§11, `docs/guides/repository.md`) places bindings and `redact.Keys` in `internal/keys`.

## 9. Masking, encryption and keys (requirement 7)

- **Policy**: `redact.Policy`, configured in YAML, stored versioned with a content hash, compiled once, bound to each capture at registration (§6.4).
  The **default policy is every row of the baseline table of [SEM §8]** (process-program bodies in S7, data sets in S13, recipes in S15 and S16, S6F25, and the rest of that table), with the paths and domains it gives; the table is the normative list and is not repeated here;
  identifiers stay in clear (G5-207).
  A tool type is **admitted to production** only after its manual's recipe carriers are checked against the policy; `Mask` leaves an unlisted message unchanged, so the list is the protection.
  Fields beyond recipe bodies are the owner's (LS-8).
- `redact.Keys` is implemented over a `KeyProvider`:
  - `DigestKey(keyID)`: one digest key across captures, tools and message types (G5-220, V-26).
    Rotation is rare and by declaration: a new `digest_key_id` applies to new captures, every pack names its digest domain, the old key stays available, and digests of two domains are never compared.
  - `EncryptionKey(capture, keyID)` = HKDF-SHA-256 with IKM = the master key of `keyID`, salt = `"tracekeep/capture-key/v1"`, info = the 16 raw bytes of `capture_id` (RFC 9562 order) followed by `keyID` in UTF-8, length 64,
    the `aes-siv-cmac-512` key `redact` expects; deterministic as G5-214 requires, nothing per capture to store.
    The derivation is versioned by its salt; a new version is a new `keyID`.
  - **Bindings, insert-or-compare**: before a consumer derives or writes with a key it resolves the material, computes the fingerprint = HMAC-SHA-256(key = material, message = `"tracekeep/key-fingerprint/v1"`), lowercase hex,
    and runs `INSERT INTO key_bindings` (unique on `key_id`) with provider reference and version; on conflict it compares the stored fingerprint with its own;
    a mismatch fails closed: the consumer defers every record needing that key and alerts.
    Two consumers resolving one id for the first time therefore agree or one of them stops.
    A `keyID`, like a processing configuration, is retired only under §6.4's condition, never merely because its captures are stopped.
- `KeyProvider` implementations: `vault` (KV v2 with the version pinned in the binding; Kubernetes auth in the cluster, AppRole on a host; keys cached in memory, never on disk),
  `file` (files a Vault Agent or an operator places, mode 0600, for standalone), `env` (development only, refused in production mode).
  Vault Transit does not fit: the derivation and AES-SIV need the raw master key in process.
- Where the data lives: clear text on the traffic stream and in the claim bucket (TLS, encryption at rest, G5-213) and in a consumer's memory before masking;
  in storage only masked packs with the originals as AES-SIV ciphertext in place; restored originals only in an API response, never in a file, an export or a temporary object.
- **Restore** (`restorer` role) returns one record's original in the response and writes an `audit_log` row first; an export never contains originals.
- **Digest** (`scorer` role, for the VE's protected scorer, V-26): `Digest(digest_key_id, domain, kind, original)` with the three input forms `redact.Digest` defines:
  `item` (the encoded SECS-II item, header included, for a resolved path), `text` (the whole message text, for a whole-text mask), `annotation` (the value bytes);
  it returns the digest with the key id and domain it used, rate-limited and audited; the scorer never holds the key.
