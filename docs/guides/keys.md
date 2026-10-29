# Keys guide

Status: current (2026-10-29) — the digest, master and download purposes, and the vault, file and env providers

How to declare the keys masking and downloads use, rotate them, and recover when their provider fails.
Read before declaring a key id, rotating one, or answering a `key-binding-mismatch`, `key-refused` or Vault alert.
The design is `docs/specs/tracekeep/masking-keys.md` §9; the configuration keys are in `configuration.md`.

## What a key id is

A key id names one key of one purpose, for good:

- `digest`: the HMAC-SHA-256 key of every digest masking writes, one key across captures.
- `master`: the material each capture's encryption key is derived from.
- `download`: the key that signs the download tokens the API issues.

The first use of a key id binds it in the catalog:
its purpose, the provider reference and version it was read at, and its material's fingerprint.
A binding never changes.
Every later use reads the same reference at the same version and checks the fingerprint,
and a mismatch stops every use of the key id (`key-binding-mismatch`, below).
So a key id is never reused for other material, and rotation always means a new key id.

## Declaring a key id

Each key id is declared under `keys.declared` with its provider, its reference and, for `digest` and `master`, its kind:

```yaml
keys:
  declared:
    digest-2026a: {provider: vault, ref: tracekeep/digest-2026a, kind: digest}
    master-2026a: {provider: vault, ref: tracekeep/master-2026a, kind: master}
    dl-1: {provider: file, ref: /run/keys/dl-1}
  download:
    issuing: dl-1
    accepted: [dl-1]
  vault:
    address: https://vault.example.invalid:8200
    ca_file: /etc/tracekeep/vault-ca.pem
    auth:
      method: kubernetes
      role: tracekeep
```

A `version` pins the version the first use reads; without one, the first use reads the current version and binds it.
The material is at least 32 random bytes, written as base64 text:
the `material` field of a KV v2 secret for `vault`,
a file of mode 0600 for `file`,
an environment variable for `env`, which only `tracekeep dev` accepts.

For `vault`:

1. Give each key id a secret path of its own and write it once:
   `vault kv metadata put -cas-required=true -delete-version-after=0s secret/tracekeep/master-2026a`,
   then `vault kv put -cas=0 secret/tracekeep/master-2026a material="$(head -c 32 /dev/urandom | base64)"`.
   With `cas_required` every later write must name the current version, so the path is never overwritten by accident,
   and declaring the key id with `version: "1"` pins it before its first use.
2. Keep the version a binding pins for as long as data needs it.
   KV v2 keeps only the newest `max_versions` versions of a path, 10 by default, and a path's 0 means the mount's limit, not unlimited;
   a path written once holds one version, which no limit evicts.
   `delete_version_after` must stay 0 on the path and the mount.
   Never delete or destroy a version of a `digest` or `master` key id while a retained pack of a capture bound to it remains:
   `Restore` and `Digest` need it after the capture's producer has long stopped.
   New material is a new key id at a new path, never a new version of a bound one.
3. Give the service's role a policy that reads those paths and nothing else,
   for example `path "secret/data/tracekeep/*" { capabilities = ["read"] }`.
4. Check a key id after its first use with `tracekeep admin key verify <key id>`:
   it reads the material again at the bound version, past every cache, and reports whether its fingerprint still matches.

## Rotating

- **`digest` and `master`** rotate by declaration, rarely.
  Declare the new key id, stage a processing configuration naming it with `tracekeep admin config stage <digest key id> [<encryption key id>]`,
  which binds it, and activate that configuration with `config activate`;
  new captures bind the new key id, and every existing capture keeps the one it was bound with.
  The old key id stays declared, and its versions stay in Vault, as long as a retained pack of a capture bound to it remains.
  Digests of two digest key ids are never compared: each pack names the key id and domain of its digests.
- **`download`** rotates in three steps (the configuration guide's `keys.download`):
  add the new key id to `accepted` on every replica, then switch `issuing` to it,
  then drop the old one once the longest download lifetime plus the clock skew has passed.

## When Vault fails

A process keeps every key it has resolved for its whole life, so a running process masks and serves through an outage with the keys it already holds.
A key it has not resolved yet, in a process started during the outage or for a key id it has not used, waits:
ingest defers the records that need it, and the API answers unavailable.
`key_resolutions_total{outcome="unavailable"}` and `key_provider_errors_total{reason}` show it, and the alert fires after five minutes.

| What failed | `reason` | Recovery |
|---|---|---|
| the server is down, sealed or slow | `unreachable`, `deadline`, `server` | bring Vault back; nothing to do in tracekeep, the deferred records are redelivered and the next use resolves |
| the credential is refused | `auth` | the provider logs in afresh by itself; when that fails too, fix the role or its policy, or issue a new AppRole secret id and restart, since the secret id is read at start |
| the pinned version is deleted | `missing` | `vault kv undelete -versions=<n> secret/<path>`; the alert is `key-refused` (runbook) |
| the pinned version is destroyed | `missing` | the material is gone: see `key-refused` in the runbook |
