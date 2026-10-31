# Kubernetes guide

Status: current (2026-11-01) — the Helm chart of phase 4.5: the image, values, secrets, keys from Vault, install and upgrade, alerts

How to run tracekeep on Kubernetes with its Helm chart, `deploy/helm/tracekeep/`.
Read before a first install, an upgrade, or a change to the chart's values.
The design is `docs/specs/tracekeep/architecture.md` §3.3; every configuration key is in `configuration.md`.

## What the chart installs, and what it does not

One Deployment per role (`ingest`, `compact`, `api`), the `api` Service, one ConfigMap holding `config.yaml`,
a service account, the `migrate` Job as an install and upgrade hook, and, when enabled, the alert rules.
The bus (NATS with JetStream), the catalog's MariaDB, the object store, Vault and the IdP are the cluster's own, and exist before the install (T-50).
An Ingress, a PodDisruptionBudget, a ServiceMonitor and NetworkPolicies are left to the cluster's conventions.

## The image

Nothing publishes the image yet (T-50). Build it and push it to a registry the cluster pulls from:

```
make image                                   # tracekeep:dev for this host's platform
docker tag tracekeep:dev registry.example.invalid/tracekeep:<version>
docker push registry.example.invalid/tracekeep:<version>
```

For both platforms, `docker buildx build -f deploy/image/Dockerfile --platform linux/amd64,linux/arm64 --push -t <name> .` from the repository's root.
The image is the one binary on distroless, user 65532, with the frontend embedded; it writes only to its mounted `/work`.

## Values

```yaml
image: {repository: registry.example.invalid/tracekeep, tag: "<version>"}
secrets: [tracekeep-catalog, tracekeep-s3, tracekeep-tls, tracekeep-people, tracekeep-ca]
ingest: {replicas: 2, workDirSize: 4Gi}
compact: {workDirSize: 4Gi}
api: {replicas: 2, workDirSize: 40Gi}
config:
  catalog: {engine: mariadb, dsn_ref: "file:///run/secrets/tracekeep-catalog/dsn"}
  bus: {url: "nats://nats.traffic:4222", subject_prefix: tpk, claim_bucket: tpk-objects}
  store:
    kind: s3
    s3: {endpoint: "https://s3.example.invalid", bucket: tracekeep, access_key_id: tracekeep, secret_access_key_ref: "file:///run/secrets/tracekeep-s3/secret"}
  api:
    tls_cert_file: /run/secrets/tracekeep-tls/tls.crt
    tls_key_file: /run/secrets/tracekeep-tls/tls.key
    public_url: https://tracekeep.example.invalid
  auth:
    oidc: {issuer: "https://idp.example.invalid", audience: tracekeep-api}
    people:
      client_id: tracekeep-web
      client_secret_ref: "file:///run/secrets/tracekeep-people/secret"
      roles: {troubleshooters: [viewer, exporter]}
  keys:
    vault:
      address: https://vault.example.invalid:8200
      auth: {method: kubernetes, role: tracekeep}
    declared:
      session-1: {provider: vault, ref: tracekeep/session-1}
      download-1: {provider: vault, ref: tracekeep/download-1}
      digest-2026a: {provider: vault, ref: tracekeep/digest-2026a, kind: digest}
      master-2026a: {provider: vault, ref: tracekeep/master-2026a, kind: master}
    session: {issuing: session-1, accepted: [session-1]}
    download: {issuing: download-1, accepted: [download-1]}
```

- **`config`** is tracekeep's configuration, rendered as `config.yaml` for every role and the `migrate` Job.
  The chart sets the work directories (`ingest.work_dir`, `exports.work_dir`) and `TMPDIR` on `/work`.
- **A credential is never written in the values**: name a file under `/run/secrets/<secret>/` by its `*_ref`,
  and list the Secret in `secrets`, each mounted read-only at `/run/secrets/<name>/` in every pod and the Job.
  The chart refuses to render `catalog.dsn`, `bus.password` or `store.s3.secret_access_key` written inline.
  The API's certificate and key, and CA files such as `auth.oidc.ca_file` and `keys.vault.ca_file`, are such files.
- **For MariaDB**, give the DSN a `timeout` (`…?timeout=5s`): it bounds the dial of each `migrate` attempt.
- **Key material comes from Vault** with the `kubernetes` method: the `file` provider needs a regular file of mode 0600 that its process reads,
  which a Secret volume does not give a non-root process (`docs/guides/keys.md`).
  Bind the Vault role to the chart's service account and its namespace: the release's full name, which is the release name when it holds `tracekeep` and `<release>-tracekeep` otherwise, unless `serviceAccount.name` says otherwise.
- **`workDirSize`** is each role's `/work`: its `emptyDir` size limit and its ephemeral-storage request and limit, unless `resources` gives them.
  Size `ingest`'s for the scopes a replica holds open at `ingest.batch_max_bytes` each, for the traffic and the parking batchers, plus one fetched batch,
  with the records' encoded headers and the packs' metadata beside the payload bytes the bound counts, and a margin;
  `compact`'s for the largest merged pack of an hour; `api`'s for `exports.max_running` bundles of `exports.max_bundle_bytes`.
  The kubelet evicts a pod past either limit at its next check; neither stops a write, and an evicted role clears its directory when it starts again.

## Install and upgrade

```
helm upgrade --install tracekeep deploy/helm/tracekeep -n tracekeep -f values.yaml --wait --timeout 20m
```

The `migrate` Job runs first, a `pre-install` and `pre-upgrade` hook with the new release's configuration;
the Deployments change only after it succeeds, and a failed one leaves them as they were, its Job kept for its logs.
It fails fast while the bus or the catalog does not answer and is retried until `migrate.backoffLimit` attempts or `migrate.activeDeadlineSeconds` (default 20, 15 min);
`--timeout` must exceed the deadline. An install whose dependencies never answer fails then, and is run again once they do.

**Before the first capture**, stage and activate a masking configuration (`docs/guides/production-admission.md`):
a capture is admitted only under one, and an `ingest` replica executes the configurations present when it starts.
On a first install, install with `ingest.replicas: 0`, then

```
kubectl -n tracekeep exec deploy/tracekeep-api -- /tracekeep admin --config /etc/tracekeep/config.yaml --actor <you> config stage digest-2026a master-2026a
kubectl -n tracekeep exec deploy/tracekeep-api -- /tracekeep admin --config /etc/tracekeep/config.yaml --actor <you> config activate <config id>
```

and upgrade with the `ingest` replicas wanted (the release `tracekeep` names its objects `tracekeep-<role>`). With `ingest` already running, restart it after staging, before activating.

## Probes and alerts

Every role serves `/healthz` (liveness) and `/readyz` (readiness) on the metrics port, 9090, where Prometheus scrapes `/metrics`.
With `alerts.enabled` and the Prometheus Operator's CRDs, the chart installs a `PrometheusRule`
whose alerts each link a section of `docs/guides/runbook.md` (`alerts.runbookURL`); `alerts.gateway.enabled` adds the rules on the gateways' metrics,
for a Prometheus that scrapes the gateways too. The rules are in `docs/specs/tracekeep/operations.md` §10.4.

## Checks

`make chart-check` lints and renders the chart and validates the manifests; `make alerts-check` tests the alert rules with promtool.
`make packaging-cluster` installs the chart on kind with every dependency and drives a browser through a troubleshooting session,
the fresh install and an upgrade from the newest ancestor on `main` whose packaging run passed (`scripts/packaging/cluster.sh`).
