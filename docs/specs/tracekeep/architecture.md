# tracekeep — architecture

Status: draft (2026-10-31) — split from seed v21; §3.3 gained the image, the chart, the unit and their tests (phase 4.5)

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
| `migrate` | applies the catalog's versioned migrations; ensures the traffic stream and claim bucket, the bucket's stream with `AllowDirect` off (T-28) | once per release |

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
  bus.CheckMessage → descriptor registration (start) → per-scope batch
    → sort, dedupe equal copies, split conflicting copies → classify + redact.Mask → tracepack.Writer → temp file
    → ObjectStore.Put (staging/) → catalog registration (pack, ranges with times, losses) → ack each message
tracekeep compact                                                    §8
  snapshot scope (revision r) → tracepack.Merge over ReaderAt → archive/ → one catalog txn guarded by r and the lease → delayed delete
tracekeep api                                                        §10
  catalog (packs, ranges, gaps, scope events) + pack downloads + MergeIterate queries + export bundles + restore
```

### 3.3 Deployment

- **Kubernetes** (primary): one Deployment per role, Helm chart under `deploy/helm/`;
  keys from Vault through the Kubernetes auth method (§9);
  the traffic NATS cluster is a separate StatefulSet or an external cluster, never the gateway's.
- **Standalone** (bare metal or VM): the same binary as `tracekeep all` under systemd,
  one YAML config, keys from Vault with AppRole or from files an agent renders (§9),
  object storage any S3-compatible endpoint on or off the host (MinIO's open-source server is no longer maintained, T-34),
  MariaDB on the host, or SQLite for a single-host pilot with every writing role in the one `all` process (§7.4).
- Both read one config schema; the difference is the key provider and the number of processes.
  The schema is one Go struct loaded with fuda (T-22).
- **Traffic cluster connections**: every producer holds one dedicated connection (`producer-sdk.md` §4.7),
  and a gateway runs one producer per recorded `eqp-hsms` device process,
  so the cluster serves up to the devices per gateway (fourteen in eqp-hub's `examples/tfdc_common_parallel.yaml`) times the gateways, plus tracekeep's own consumers;
  the cluster's connection limits and file descriptors are sized for that count, not for the number of tools.

**The image** (T-48 point 11, T-50).
`deploy/image/Dockerfile` builds the one binary as `make build` does, the frontend embedded (`api.md` §10.3),
statically (`CGO_ENABLED=0`: the SQLite driver is pure Go), with `-trimpath`,
onto `gcr.io/distroless/static-debian12:nonroot`: no shell, user 65532, the binary at `/tracekeep` as the entrypoint.
One stage installs the frontend's packages from the lockfile and builds it, one builds the binary for the target platform,
so the build needs only Docker, for `linux/amd64` and `linux/arm64`.
`make image` builds it for the host's platform and loads it as `tracekeep:dev`;
`make image-check` builds both platforms without loading or pushing them, which a PR's CI runs.
Its name is `ghcr.io/arloliu/tracekeep`, and nothing publishes it yet (T-50):
a deployment builds it, or pushes it to a registry of its own.
The image writes nothing outside the directories a deployment mounts:
the root filesystem can be read-only, and every work directory and `TMPDIR` is a mounted volume.

**The Helm chart** (`deploy/helm/tracekeep/`) deploys tracekeep alone;
the bus, the catalog's MariaDB, the object store, Vault and the IdP are the cluster's own, named by the chart's values (T-50).
- **One Deployment per role**: `ingest` and `api` with a replica count each, `compact` with one replica (its lease makes a second a standby, §8), each the same image with its subcommand.
  A pod's replica id is its hostname, the pod's name, which `replica` defaults to.
- **One configuration** for every role: the values' `config` rendered as `config.yaml` in a ConfigMap, mounted at `/etc/tracekeep/config.yaml`.
  A credential (the catalog's DSN, the bus's password, the S3 secret, the people client's secret) is a `*_ref` to a file under `/run/secrets/<secret>/`,
  mounted read-only from a Secret the values name, which exists before the install; the chart creates no Secret, and no credential is written in the values or a ConfigMap.
  The API's certificate and key, and the CA files (`auth.oidc.ca_file`, the Vault provider's), are such files.
- **Key material comes from Vault**, never from a mounted file (§9, `masking-keys.md` §9):
  every `keys.declared` id uses the `vault` provider with the `kubernetes` method, logging in with the pod's service account token, of the chart's service account.
  The `file` provider requires a regular file of mode exactly 0600, and the kubelet writes a Secret volume's files as root with the modes it chooses,
  so a mounted key would be unreadable by the non-root process or refused at start.
- **`migrate` is a Job**, a `pre-install` and `pre-upgrade` hook, so the roles' Deployments change only after it succeeds, and a failed migration leaves them as they were.
  It reads a snapshot of the release's configuration: the same `config.yaml` in a ConfigMap of its own, a hook of a lower weight,
  so a fresh install has it before any ordinary resource and an upgrade migrates with the new release's configuration, never the running one's.
  Both hooks are deleted before their next creation and once they succeed; a failed Job stays for its logs.
  The Job mounts the same Secrets and no service account token, since migrate resolves no key.
  An attempt against a bus that does not answer ends at the bus client's connect timeout (2 s, with no retry of a failed first connection);
  against MariaDB, the DSN's `timeout` bounds the dial only, which the deployment guide sets,
  so a server that accepts the connection and never answers holds the attempt until the Job's deadline;
  a held migration lock waits up to its 10 min (`catalog.md` §7.4).
  The Job retries a failed attempt with the cluster's backoff until `backoffLimit` attempts or `activeDeadlineSeconds` pass, whichever comes first,
  defaults 20 attempts and 15 min, so the deadline ends it; Helm's `--timeout` must exceed the deadline.
  An install whose dependencies never answer fails then, its Deployments never changed, and is run again once they do.
- **Probes** on the metrics port (`metrics.listen`, 9090): liveness `/healthz`, readiness `/readyz` (operations.md §10.4 *Health*).
- **Work directories**: every role mounts an `emptyDir` at `/work`, its size limit, ephemeral-storage request and limit the values' per role,
  with `ingest.work_dir` `/work` (read by `ingest` and by `compact`, which builds its archives there), `exports.work_dir` `/work/exports` and `TMPDIR` `/work`.
  The deployment guide sizes each with a margin: `ingest`, the scopes a replica holds open at `ingest.batch_max_bytes` each,
  for the traffic and the parking batchers, plus one fetched batch, each with its records' encoded headers and the pack's own metadata beside the payload bytes the bound counts;
  `compact`, the largest merged pack of an hour, which it writes before the put; `api`, `exports.max_running` bundles of `exports.max_bundle_bytes`.
  Neither the size limit nor the ephemeral-storage limit stops a write: the kubelet evicts a pod past either at its next check, asynchronously,
  and a write fails with no space only when the node's disk itself is full.
  A failed flush or merge is retried; an export whose pack cannot be written leaves it out of the bundle, named in the manifest's `left_out` (`api.md` §10.1 *Exports*), and completes.
  Each role clears its work directory when it starts again, and nothing acknowledged depends on it.
- **The API** behind a Service on port 443 to the pods' 8443, over the API's own TLS (`api.tls_cert_file`);
  `api.public_url` is the URL people and services reach it at, through whatever the cluster routes to that Service.
  Every `api` replica resolves the same session and download keys (`api.md` §10.2), so a login one replica starts another can finish.
- **Security context**: non-root, a read-only root filesystem, no privilege escalation, every capability dropped, the runtime's default seccomp profile.
- **Alert rules** (operations.md §10.4) as a `PrometheusRule` when the values enable it, each alert's `runbook_url` an anchor of `docs/guides/runbook.md`.
- Out of the chart: an Ingress, a PodDisruptionBudget, a ServiceMonitor and NetworkPolicies, which a cluster adds to its own conventions;
  and the dependencies, never subcharts.

**The systemd unit** (`deploy/systemd/`) runs `tracekeep all` on a standalone host.
- `tracekeep.service` runs `/usr/local/bin/tracekeep all --config /etc/tracekeep/config.yaml` as the system user `tracekeep`, which `tracekeep.sysusers` creates,
  with `StateDirectory=tracekeep` (`/var/lib/tracekeep`: the SQLite catalog, an `fs` store, the work directories).
- **Credentials** are files the unit's user reads, each named by a `*_ref` or a key's `ref`, never by systemd's credentials (`LoadCredential`), which an operator's command could not read:
  a credential under `/etc/tracekeep/secrets/`, mode 0640, owned by root with the group `tracekeep`;
  key material under `/etc/tracekeep/keys/`, mode 0600, owned by `tracekeep`, as the `file` provider requires (`masking-keys.md` §9), or from Vault with AppRole.
  `sudo -u tracekeep tracekeep admin …` reads the same files.
- **Migration before every start**: `ExecStartPre` runs `tracekeep migrate` with the same configuration,
  so an install and an upgrade are one order: stop the unit, replace the binary, start it;
  a SQLite catalog has one writer (§7.4), and migrate runs while no role holds it. `migrate` only ensures, so a start without a new release changes nothing.
  `TimeoutStartSec` (15 min) bounds it, a held MariaDB migration lock included.
- **Restarts**: `Restart=on-failure` every 10 s with no start limit (`StartLimitIntervalSec=0`), so a host whose bus or catalog comes up after tracekeep is waited for,
  each failed start logged with why.
- **Hardening**: `NoNewPrivileges`, `ProtectSystem=strict` with the state directory writable, `ProtectHome`, `PrivateTmp`, `PrivateDevices`, no capabilities;
  the API's port is above 1024.
- An operator's command reaches a SQLite catalog the running service holds through its API, authenticated by a static token holding `admin` (`api.md` §10.1 *Admin over the API*);
  without one, the command waits for the unit to be stopped.

**Packaging tests** (T-48 point 8, T-50): two CI jobs drive the real packaging with a browser, each a script under `scripts/packaging/`,
on a push to `main`, on a daily schedule and when dispatched, never on a pull request.
- **Common to both**: Dex as the IdP, its mock connector signing a fixed person in with a `groups` claim (`auth.people.scopes` asks for `groups`), mapped to `viewer` and `exporter`.
  A CA the script generates signs every HTTPS endpoint the tests reach: the service trusts it for Dex (`auth.oidc.ca_file`) and for Vault (the provider's CA file),
  and the runner's system store and the browser's certificate store trust it, so no certificate error is ignored.
  Downloads are served by the API (`store.presign` off), so the object store is the service's only, never the browser's.
  The simulator (`cmd/tracekeep-sim`) is the producer, started only after a masking configuration is staged and active with no ingest member running,
  so every ingest member starts executing it and the simulator's first capture is admitted (`docs/guides/production-admission.md`);
  the leg checks that `tracekeep status` lists the tool's open capture and no deferred message before the browser runs.
  Playwright (`web/e2e/`) then signs in, browses the simulator's tool, reads its records and one transaction, and runs an export to its download, whose manifest it reads;
  a person the mock connector signs in without a mapped group is denied.
- **Cluster** (`cluster.sh`, run locally where Docker is before a change to the packaging merges):
  kind with NATS, MariaDB, MinIO (over HTTP inside the cluster), Vault (over TLS, the `kubernetes` method bound to the chart's service account) and Dex in the cluster,
  the image built and loaded, the chart installed with two `api` replicas and `ingest` at zero replicas until the configuration is active.
  Each endpoint's host name (`tracekeep.test`, `dex.test`) resolves to the same service inside the cluster and on the runner, so the issuer and `api.public_url` are one URL from both sides.
  The login starts on one `api` replica and its callback is answered by the other, each reached on its own port, the cookies being the one host's;
  the test records which pod answered each, and restarts the first before the callback.
  It runs a fresh install of the commit under test; then an upgrade from a baseline:
  the newest strict ancestor of the commit under test, on `main`, that has `deploy/helm/` and whose packaging run passed, never the commit itself or a descendant,
  its chart and image installed fresh and upgraded to the commit under test, the browser run again on its data;
  while no such commit exists, as for the first run after this phase merges, the upgrade step is skipped and the job says so, never replaced by a same-version upgrade.
  The job records both commits.
- **Standalone** (`standalone.sh`, CI only): it refuses to run unless `GITHUB_ACTIONS` is `true` and `RUNNER_ENVIRONMENT` is `github-hosted`,
  and refuses a host that has any of the `tracekeep` user, the unit, `/etc/tracekeep`, `/var/lib/tracekeep` or `/usr/local/bin/tracekeep`,
  so it never writes on a workstation, a self-hosted runner or over an installation; its job runs on a GitHub-hosted runner, discarded after it, so it needs no cleanup.
  It installs the binary `make build` builds with the unit, NATS and Dex in containers, SQLite and an `fs` store:
  the user, the directories and the credentials first, then, with the unit stopped and as `tracekeep`, `tracekeep migrate`, which creates the catalog,
  and the configuration staged and activated; then it starts the unit, whose own migration before the start then changes nothing.
  It then upgrades from the same baseline as the cluster's, in the unit's order (stop, replace, start).

### 3.4 Transport scope

Phase 0 records **HSMS-SS connections only**:
go-secs's wire and socket observers report HSMS-SS,
and `eqp-hsms` wraps SECS-I into an HSMS connection whose wire the observers do not see.
A SECS-I tool gets no capture and the producer logs that once at `Start`;
the SECS-I path is designed when a SECS-I tool needs one.
