# Configuration guide

Status: current (2026-10-27) — the keys phase 1 reads

Every key the service reads, its default, its environment override, and what moving it costs.
Read when deploying a role, tuning one, or adding a key.

The configuration is one YAML file, given to every role with `--config`, loaded by `internal/config` with fuda (T-22):
a key's default applies when the file omits it, an environment variable overrides the file,
and a secret is read from a reference (`file:///run/secrets/x` or `env://NAME`) named by its `*_ref` key, never written in the file.
It is read once at start; a change takes a restart.
The tunables of later phases (Q, I, L, D, the masking keys) join this file with the code that reads them.

## Keys

| Key | Default | Environment | Meaning and cost of moving it |
|---|---|---|---|
| `retention` | `720h` | `TRACEKEEP_RETENTION` | whole hours older than this are evicted, and a record older than the horizon is beyond retention; 30 days, a default the specs leave open (`compactor.md` §8.4 sizes the claim bucket for a month) |
| `replica` | the host name | `TRACEKEEP_REPLICA` | names this process in the membership and the compactor lease; unique per process |
| `catalog.engine` | `sqlite` | `TRACEKEEP_CATALOG_ENGINE` | `sqlite` for a single host with every writing role in one process, else `mariadb` (`catalog.md` §7.4); with `sqlite`, `tracekeep admin` runs only while the service is stopped, the running process holding the catalog |
| `catalog.dsn`, `catalog.dsn_ref` | — | `TRACEKEEP_CATALOG_DSN`, `…_DSN_REF` | the SQLite file or the MariaDB DSN; with credentials, by reference |
| `bus.url` | — | `TRACEKEEP_BUS_URL` | the traffic cluster |
| `bus.user`, `bus.password_ref`, `bus.creds_file` | — | `TRACEKEEP_BUS_…` | user and password, or a credentials file; not both |
| `bus.ca_file`, `bus.cert_file`, `bus.key_file` | — | `TRACEKEEP_BUS_…` | TLS: the servers' CA, and a client certificate for mutual TLS, its certificate and key given together |
| `bus.subject_prefix` | — | `TRACEKEEP_BUS_SUBJECT_PREFIX` | the traffic subjects' prefix, the producers' |
| `bus.stream`, `bus.parked_stream`, `bus.park_prefix` | `busconfig`'s | `TRACEKEEP_BUS_…` | the traffic and parking streams and the parking prefix |
| `bus.claim_bucket` | — | `TRACEKEEP_BUS_CLAIM_BUCKET` | the object store large records travel through |
| `store.kind` | `s3` | `TRACEKEEP_STORE_KIND` | `s3`, or `fs` for a single host |
| `store.key_prefix` | — | `TRACEKEEP_STORE_KEY_PREFIX` | prefixes every pack key |
| `store.dir` | — | `TRACEKEEP_STORE_DIR` | the `fs` store's root |
| `store.s3.endpoint`, `.region`, `.bucket`, `.access_key_id`, `.secret_access_key_ref` | — | `TRACEKEEP_S3_…` | an S3-compatible endpoint (T-34); empty endpoint for AWS; the bucket and both credentials are required, the store taking static credentials only |
| `store.presign` | `true` | `TRACEKEEP_STORE_PRESIGN` | readers download by presigned URL (T-33); off for a store whose URLs they cannot use |
| `ingest.flush_interval` | `5m` | `TRACEKEEP_INGEST_FLUSH_INTERVAL` | F (T-25): longer makes fewer, larger segments and a later registration; Q must exceed F plus `ack_wait` plus slack |
| `ingest.ack_wait` | `7m` | `TRACEKEEP_INGEST_ACK_WAIT` | must exceed F plus an upload, and with the redelivery slack stay below Q minus F (T-25); 7 minutes, the plan's middle of that range |
| `ingest.max_ack_pending` | `50000` | `TRACEKEEP_INGEST_MAX_ACK_PENDING` | above the peak message rate times F plus an upload, or delivery stalls |
| `ingest.fetch_batch`, `ingest.max_wait` | `500`, `5s` | `TRACEKEEP_INGEST_…` | one fetch's size and wait |
| `ingest.lease` | `1m` | `TRACEKEEP_INGEST_LEASE` | the membership lease (`rollouts.md` §6.4): a replica that stops renewing leaves the activation gate after it |
| `ingest.batch_max_bytes` | `64MiB` | `TRACEKEEP_INGEST_BATCH_MAX_BYTES` | a batch flushes before F once it holds this many payload bytes |
| `ingest.work_dir` | the system's temporary directory | `TRACEKEEP_INGEST_WORK_DIR` | holds a segment's working file until its put returns, and a merge's archive likewise, each role of each replica in a directory `tracekeep-<role>-<replica>` of its own, cleaned at start; masked bytes only |
| `ingest.max_frame_len` | `0` | `TRACEKEEP_INGEST_MAX_FRAME_LEN` | the processing configuration's `max_frame_len`, 0 for no ceiling; a change makes another configuration, which every replica must execute before captures bind it (`rollouts.md` §6.4); a replica executes every one not retired, and new captures bind the newest until it is retired, so changing back takes that retirement |
| `parking.enabled` | `true` | `TRACEKEEP_PARKING_ENABLED` | off, an indefinite deferral is nak'd and can fill `max_ack_pending` for every capture (`parking.md`); the parked consumer works either way |
| `parking.redelivery_delay` | `60s` | `TRACEKEEP_PARKING_REDELIVERY_DELAY` | how soon a parked copy is tried again; the completion signal is `parked list`, not a time |
| `compact.lease_ttl` | `1m` | `TRACEKEEP_COMPACT_LEASE_TTL` | the compactor lease: renewed every third of it while a cycle runs; a standby takes over once it lapses |
| `compact.interval` | `5m` | `TRACEKEEP_COMPACT_INTERVAL` | the time between two compactor cycles; an orphan is adopted on the second cycle that finds it, and a scope is merged by the first cycle after it became a candidate |
| `compact.quiet_period` | `15m` | `TRACEKEEP_COMPACT_QUIET_PERIOD` | Q (T-12): a scope becomes a merge candidate once its hour ended Q ago; must exceed F + `ack_wait` + the redelivery slack; shorter settles sooner and reopens more often after a late flush |
| `compact.redelivery_slack` | `2m` | `TRACEKEEP_COMPACT_REDELIVERY_SLACK` | the time Q leaves after F and `ack_wait` for a redelivered message to be flushed (T-41) |
| `metrics.listen` | `:9090` | `TRACEKEEP_METRICS_LISTEN` | where `/metrics`, `/healthz` and `/readyz` are served (`operations.md` §10.4); empty serves none |
