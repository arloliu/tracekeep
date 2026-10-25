# tracekeep — architecture

Status: draft (2026-10-25) — split from seed v21

Covers the four service roles and their scaling, the JetStream consumer settings `ingest` depends on, the data flow, deployment and transport scope.
From seed v21 §3; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read second, after `positioning.md`, and before working on any role or on deployment.

## 3. Architecture

### 3.1 Components

One Go module for the service, one binary, four roles selected by subcommand;
`all` runs every role in one process for standalone hosts.
A second module, the producer SDK, is published for producers (§4.7).

| Role | Does | Scaling |
|---|---|---|
| `ingest` | pull consumer of the traffic stream; registers descriptors; batches records per (capture, UTC hour); masks; writes segments; registers packs; acks per message | stateless, N replicas; the work-queue stream balances them |
| `compact` | merges scopes; closes captures left open; deletes merged segments after a delay; applies retention, claim objects included; reconciles `staging/` and `archive/` against the catalog | one active instance under a catalog lease with a fence token (§8); shard by tool later if needed |
| `api` | machine API for the VE service, human API and embedded web UI, exports, restore, admin | stateless, N replicas |
| `migrate` | applies the catalog's versioned migrations; ensures the traffic stream and claim bucket | once per release |

JetStream consumer settings `ingest` depends on:
`ack_wait` longer than the flush interval F plus the upload, and in-progress acks every `ack_wait/3`
while a batch is held ([STO §4] *Stream settings*);
`max_deliver` unlimited; `max_ack_pending` above the peak rate × (F + upload);
**per-message acks only** (`AckExplicit`, never `AckAll`, since a cumulative ack would cover unrelated unstaged deliveries),
and a confirmed ack (`DoubleAck`) for a claim-check message, because the object's deletion depends on it (§8.4).

### 3.2 Data flow

```
eqp-hub (eqp-hsms plugin process)                                    producer SDK, §4
  go-secs observers → seq under the capture lock → capacity reservation → copy frame → bounded ring → publisher goroutine
    → NATS JetStream traffic stream (+ claim bucket for large payloads)
tracekeep ingest                                                     §6
  CheckBusMessage → descriptor registration (start) → per-scope batch
    → sort, dedupe equal copies, split conflicting copies → classify + redact.Mask → tracepack.Writer → temp file
    → ObjectStore.Put (staging/) → catalog registration (pack, ranges with times, losses) → ack each message
tracekeep compact                                                    §8
  snapshot scope (revision r) → tracepack.Merge over ReaderAt → archive/ → one catalog txn guarded by r and the lease → delayed delete
tracekeep api                                                        §10
  catalog (packs, ranges, gaps, scope events) + presigned pack reads + MergeIterate queries + export bundles + restore
```

### 3.3 Deployment

- **Kubernetes** (primary): one Deployment per role, Helm chart under `deploy/helm/`;
  keys from Vault through the Kubernetes auth method (§9);
  the traffic NATS cluster is a separate StatefulSet or an external cluster, never the gateway's.
- **Standalone** (bare metal or VM): the same binary as `tracekeep all` under systemd,
  one YAML config, keys from Vault with AppRole or from files an agent renders (§9),
  object storage any S3 endpoint (MinIO on the same host is enough for a pilot),
  MariaDB on the host, or SQLite for a single-host pilot with every writing role in the one `all` process (§7.4).
- Both read one config schema; the difference is the key provider and the number of processes.
  The schema is one Go struct loaded with fuda (T-22).

### 3.4 Transport scope

Phase 0 records **HSMS-SS connections only**:
go-secs's wire and socket observers report HSMS-SS,
and `eqp-hsms` wraps SECS-I into an HSMS connection whose wire the observers do not see.
A SECS-I tool gets no capture and the producer logs that once at `Start`;
the SECS-I path is designed when a SECS-I tool needs one.
