# Configuration guide

Status: current (2026-10-28) — the keys phases 1 and 2 read

Every key the service reads, its default, its environment override, and what moving it costs.
Read when deploying a role, tuning one, or adding a key.

The configuration is one YAML file, given to every role with `--config`, loaded by `internal/config` with fuda (T-22):
a key's default applies when the file omits it, an environment variable overrides the file,
and a secret is read from a reference (`file:///run/secrets/x` or `env://NAME`) named by its `*_ref` key, never written in the file.
It is read once at start; a change takes a restart.
The tunables of later phases (the masking keys' other purposes) join this file with the code that reads them.

## Keys

| Key | Default | Environment | Meaning and cost of moving it |
|---|---|---|---|
| `retention` | `720h` | `TRACEKEEP_RETENTION` | whole hours older than this are evicted, and a record older than the horizon is beyond retention; a shortening takes effect for eviction only `compact.delete_delay` after the compactor records it, so no issued download URL outlives its object (`compactor.md` §8.3); 30 days, a default the specs leave open (`compactor.md` §8.4 sizes the claim bucket for a month) |
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
| `store.download_lifetime` | `15m` | `TRACEKEEP_STORE_DOWNLOAD_LIFETIME` | the longest a download URL lives (T-41); `compact.delete_delay` must exceed it by more than 4 minutes |
| `ingest.flush_interval` | `5m` | `TRACEKEEP_INGEST_FLUSH_INTERVAL` | F (T-25): longer makes fewer, larger segments and a later registration; Q must exceed F plus `ack_wait` plus slack |
| `ingest.ack_wait` | `7m` | `TRACEKEEP_INGEST_ACK_WAIT` | must exceed F plus an upload, and with the redelivery slack stay below Q minus F (T-25); 7 minutes, the plan's middle of that range |
| `ingest.max_ack_pending` | `50000` | `TRACEKEEP_INGEST_MAX_ACK_PENDING` | above the peak message rate times F plus an upload, or delivery stalls |
| `ingest.fetch_batch`, `ingest.max_wait` | `500`, `5s` | `TRACEKEEP_INGEST_…` | one fetch's size and wait |
| `ingest.lease` | `1m` | `TRACEKEEP_INGEST_LEASE` | the membership lease (`rollouts.md` §6.4): a replica that stops renewing leaves the activation gate after it |
| `ingest.batch_max_bytes` | `64MiB` | `TRACEKEEP_INGEST_BATCH_MAX_BYTES` | a batch flushes before F once it holds this many payload bytes |
| `ingest.work_dir` | the system's temporary directory | `TRACEKEEP_INGEST_WORK_DIR` | holds a segment's working file until its put returns, and a merge's archive likewise, each role of each replica in a directory `tracekeep-<role>-<replica>-<digest of the replica>` of its own, cleaned at start; masked bytes only |
| `ingest.max_frame_len` | `0` | `TRACEKEEP_INGEST_MAX_FRAME_LEN` | the processing configuration's `max_frame_len`, 0 for no ceiling; a change makes another configuration, which every replica must execute before captures bind it (`rollouts.md` §6.4); a replica executes every one not retired, and new captures bind the newest until it is retired, so changing back takes that retirement |
| `parking.enabled` | `true` | `TRACEKEEP_PARKING_ENABLED` | off, an indefinite deferral is nak'd and can fill `max_ack_pending` for every capture (`parking.md`); the parked consumer works either way |
| `parking.redelivery_delay` | `60s` | `TRACEKEEP_PARKING_REDELIVERY_DELAY` | how soon a parked copy is tried again; the completion signal is `parked list`, not a time |
| `compact.lease_ttl` | `1m` | `TRACEKEEP_COMPACT_LEASE_TTL` | the compactor lease: renewed every third of it while a cycle runs; a standby takes over once it lapses |
| `compact.interval` | `5m` | `TRACEKEEP_COMPACT_INTERVAL` | the time between two compactor cycles; an orphan is adopted on the second cycle that finds it, and a scope is merged by the first cycle after it became a candidate |
| `compact.quiet_period` | `15m` | `TRACEKEEP_COMPACT_QUIET_PERIOD` | Q (T-12): a scope becomes a merge candidate once its hour ended Q ago; must exceed F + `ack_wait` + the redelivery slack; shorter settles sooner and reopens more often after a late flush |
| `compact.inactivity` | `2h` | `TRACEKEEP_COMPACT_INACTIVITY` | I (T-12): an open capture without a registration for I is closed; shorter closes a quiet capture sooner, and a record arriving after its close is still staged |
| `compact.redelivery_slack` | `2m` | `TRACEKEEP_COMPACT_REDELIVERY_SLACK` | the time Q leaves after F and `ack_wait` for a redelivered message to be flushed (T-41) |
| `compact.delete_delay` | `1h` | `TRACEKEEP_COMPACT_DELETE_DELAY` | D (`compactor.md` §8.3): a merged input is deleted D after its merge (a change applies to later merges), and a shortened `retention` evicts nothing early for D; must exceed `store.download_lifetime` by more than 4 minutes: twice the 2 minutes two hosts' clocks may disagree |
| `metrics.listen` | `:9090` | `TRACEKEEP_METRICS_LISTEN` | where `/metrics`, `/healthz` and `/readyz` are served (`operations.md` §10.4); empty serves none |
| `auth.oidc.issuer`, `auth.oidc.audience` | — | `TRACEKEEP_AUTH_OIDC_…` | the IdP service tokens are validated against (`api.md` §10.2), an https URL, and the audience a token must name; empty configures no IdP |
| `auth.oidc.roles_claim`, `auth.oidc.roles` | `roles`, — | `TRACEKEEP_AUTH_OIDC_ROLES_CLAIM` | the claim holding a token's role values, and the roles each value grants; a value it does not name grants none |
| `auth.oidc.jwks_ttl`, `.fetch_interval`, `.leeway` | `1h`, `10s`, `1m` | `TRACEKEEP_AUTH_OIDC_…` | how long cached keys verify, an outage longer than it refusing every token; the least time between two fetches of the JWKS, which bounds the IdP's load from unknown `kid`s and how soon its return is noticed; the clock tolerance of `exp`, `nbf` and `iat` |
| `auth.mtls.client_ca_file`, `auth.mtls.subjects` | — | `TRACEKEEP_AUTH_MTLS_CLIENT_CA_FILE` | the CA client certificates are verified against, and each verified identity's roles (its first URI name, else its first DNS name); given together |
| `auth.static.enabled`, `auth.static.tokens` | `false`, — | `TRACEKEEP_AUTH_STATIC_ENABLED` | the static-token provider of a standalone host without an IdP; only the flag admits the tokens, each a `subject`, a `token_ref` naming a token of at least 32 bytes (never the token itself) and its `roles`; never turned on by a failure |
| `keys.declared.<key id>` | — | — | a key id's `provider` (`file`, mode 0600; `env` only under `tracekeep dev`), `ref` and `version`, empty for both, which have no versions (`masking-keys.md` §9); a key id of at most 128 characters, a provider and ref of at most 255 together; a bound key id is read at its binding's reference whatever its declaration says later |
| `keys.download.issuing`, `keys.download.accepted` | — | — | the download tokens' issuing key id, one of the accepted ones, each declared; rotation adds the new id to `accepted` on every replica, then switches `issuing`, then drops the old id once the longest download lifetime plus the clock skew has passed |
