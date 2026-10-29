# Runbook

Status: current (2026-10-30) — the key and masking incidents; the others join with their alerts

One section per named incident of `docs/specs/tracekeep/operations.md` §10.4:
the alert that raises it, the metric to look at, the command that resolves it, and what is lost if it is ignored.
Read when an alert names an incident.

| Incident | Metric | Recovery | Terminal alternative |
|---|---|---|---|
| `key-binding-mismatch` | `key_resolutions_total{outcome="refused"}` | restore the bound material, then `key verify` | stop using the key id: rotate to a new one |
| `key-refused` | `key_resolutions_total{outcome="refused"}`, `key_provider_errors_total{reason}` | fix the declaration or restore the bound version | rotate to a new key id; `capture terminate` for deferred records that need the old one |
| `cannot-screen` | `messages_total{disposition="nak"}`, `deferred_messages{reason="cannot-screen"}` | fix the producer or the policy, then let the redelivery retry | `capture terminate` for the records masking cannot screen |
| `incompatible-configuration` | `deferred_messages{reason="incompatible-configuration"}` | stage and activate a masking configuration; upgrade a member that cannot execute it | `capture terminate` for the capture that waits |

## key-binding-mismatch

**Alert**: a key id's material no longer matches its binding's fingerprint, or a key id is used under another kind than it was bound with.
Every use of that key id fails closed: ingest defers the records that need it, and the API refuses the operations that need it.
Nothing is ever written with the wrong key.

1. Run `tracekeep admin key verify <key id>`.
   It reads the material at the binding's reference and version, past every cache, and prints both fingerprints and whether they match.
2. If they differ, the material at the bound reference and version was changed:
   for `file`, the file was replaced; for `vault`, the version's material cannot change, so look for a changed declaration or mount.
   Put the bound material back, then run `key verify` again until it matches.
   A running process recovers by itself at its next use; nothing was cached by the failure.
3. If they match, the mismatch is of kind: a configuration names the key id under another purpose.
   Fix the declaration or the policy; a key id keeps the purpose it was first bound with.

**If ignored**: records needing the key id stay deferred until they park, and then until the operator terminates them;
`Restore` and `Digest` under the key id stay refused.
If the bound material is lost for good, rotate to a new key id for new captures;
the captures bound to the old one can no longer be masked or restored.

## key-refused

**Alert**: a key id cannot be used for a reason that a retry will not cure:
it is not declared, or not declared with the kind its use needs;
its material is malformed or shorter than 32 bytes;
or its pinned version is missing from the provider.

1. Read the error in the log: it names the key id, its kind, its provider, reference and version, and the reason.
2. For a declaration: declare the key id with its kind and restart.
   For malformed or short material: the material at the bound version cannot be fixed in place; rotate to a new key id.
3. For a missing version in Vault: a deleted version is undeleted with `vault kv undelete -versions=<n> secret/<path>`,
   after which the next use resolves.
   A destroyed version is gone.

**If ignored, or if the version was destroyed**: the records that need the key id stay deferred and park;
terminate them with `capture terminate` once nothing can bring the material back, which declares them lost.
Packs already written under the key id stay readable masked, but their originals can no longer be restored,
and for a `digest` key id, `Digest` under it is refused.

## cannot-screen

**Alert**: ingest cannot mask a record before writing it: a data record whose message text nests lists deeper than the masking walker's limit,
an annotation whose payload does not decode while the policy has annotation rules, or a record that already arrives marked redacted.
The record is never written unscreened: it is nak'd and redelivered, and the capture's later records are written around it.

1. `tracekeep admin deferred list --capture <capture>` names the waiting records.
2. A malformed record is the producer's: fix it, and the redelivery is masked.
   A rule the policy should not apply to that message is the policy's: stage a corrected policy version and activate it for new captures.

**If ignored**: the record stays deferred, and its seq a gap in the capture, until an operator terminates it with `capture terminate`, which declares it lost.

## incompatible-configuration

**Alert**: a record cannot be processed under a configuration this replica executes,
or, outside `tracekeep dev`, a capture's start finds no masking configuration it may bind.

1. `tracekeep admin config list` shows each configuration's state.
2. With no masking configuration active, stage one and activate it (`docs/guides/production-admission.md`);
   the waiting starts are admitted at their next delivery.
3. With one active but not bindable, a leased member does not execute it: upgrade or stop that member; it becomes bindable once the member lapses or leaves.

**If ignored**: the capture's records wait on the stream, bounded by the capture, until a configuration binds or an operator terminates the capture.

