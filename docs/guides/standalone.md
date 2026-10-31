# Standalone guide

Status: current (2026-11-01) — the systemd unit of phase 4.5: install, credentials and keys, the first configuration, upgrade, operator commands

How to run tracekeep on one host as `tracekeep all` under systemd, `deploy/systemd/`.
Read before a first install, an upgrade, or an operator command on a standalone host.
The design is `docs/specs/tracekeep/architecture.md` §3.3; every configuration key is in `configuration.md`.

## What runs where

One process runs every role (`tracekeep all`) as the system user `tracekeep`, with its state in `/var/lib/tracekeep`:
the SQLite catalog for a single-host pilot (or MariaDB on or off the host), an `fs` store (or any S3-compatible endpoint), and the work directories.
The bus (NATS with JetStream) and the IdP are the host's or the network's.

## Install

1. Build the binary with the frontend embedded, `make build`, and install it: `install -m 0755 dist/tracekeep /usr/local/bin/tracekeep`.
2. Create the user: `install -m 0644 deploy/systemd/tracekeep.sysusers /usr/lib/sysusers.d/tracekeep.conf && systemd-sysusers`.
3. Install the unit: `install -m 0644 deploy/systemd/tracekeep.service /etc/systemd/system/ && systemctl daemon-reload`.
4. The configuration, credentials and keys, each a file the unit's user reads:

   | Path | Mode, owner | Holds |
   |---|---|---|
   | `/etc/tracekeep/config.yaml` | 0640 `root:tracekeep` | the configuration, every credential by its `*_ref` |
   | `/etc/tracekeep/secrets/` | 0640 `root:tracekeep` each | the people client's secret, a MariaDB DSN, an S3 secret, the API's certificate and key, the IdP's CA, an admin token |
   | `/etc/tracekeep/keys/` | 0600 `tracekeep:tracekeep` each | key material for the `file` provider, which refuses any other mode (`docs/guides/keys.md`) |

   systemd's own credentials (`LoadCredential`) are not used: an operator's command reads the same files as the unit's user.
   Keys may come from Vault with AppRole instead (`keys.md`).
5. Create the state directory and migrate, as the unit's user, with the unit stopped:

   ```
   install -d -m 0750 -o tracekeep -g tracekeep /var/lib/tracekeep
   sudo -u tracekeep tracekeep migrate --config /etc/tracekeep/config.yaml
   ```

6. Stage and activate a masking configuration before the first capture (`docs/guides/production-admission.md`), still with the unit stopped,
   so the ingest role executes it from its start:

   ```
   sudo -u tracekeep tracekeep admin --config /etc/tracekeep/config.yaml --actor <you> config stage <digest key id> <encryption key id>
   sudo -u tracekeep tracekeep admin --config /etc/tracekeep/config.yaml --actor <you> config activate <config id>
   ```

7. Start it: `systemctl enable --now tracekeep`. `curl http://127.0.0.1:9090/readyz` answers once every role is ready.

A minimal configuration, SQLite and an `fs` store:

```yaml
catalog: {engine: sqlite, dsn: /var/lib/tracekeep/catalog.db}
bus: {url: "nats://127.0.0.1:4222", subject_prefix: tpk, claim_bucket: tpk-objects}
store: {kind: fs, dir: /var/lib/tracekeep/packs, presign: false}
metrics: {listen: "127.0.0.1:9090"}
api:
  tls_cert_file: /etc/tracekeep/secrets/tls.crt
  tls_key_file: /etc/tracekeep/secrets/tls.key
  public_url: https://tracekeep.example.invalid:8443
auth:
  oidc: {issuer: "https://idp.example.invalid", audience: tracekeep-api}
  people:
    client_id: tracekeep-web
    client_secret_ref: "file:///etc/tracekeep/secrets/people-secret"
    roles: {troubleshooters: [viewer, exporter]}
  static:
    enabled: true
    tokens: [{subject: operator, token_ref: "file:///etc/tracekeep/secrets/admin-token", roles: [admin]}]
keys:
  declared:
    session-1: {provider: file, ref: /etc/tracekeep/keys/session-1}
    download-1: {provider: file, ref: /etc/tracekeep/keys/download-1}
    digest-2026a: {provider: file, ref: /etc/tracekeep/keys/digest-2026a, kind: digest}
    master-2026a: {provider: file, ref: /etc/tracekeep/keys/master-2026a, kind: master}
  session: {issuing: session-1, accepted: [session-1]}
  download: {issuing: download-1, accepted: [download-1]}
```

## Upgrade

Stop the unit, replace the binary, start it:

```
systemctl stop tracekeep
install -m 0755 dist/tracekeep /usr/local/bin/tracekeep
systemctl start tracekeep
```

The unit migrates before every start (`ExecStartPre`), while no role holds a SQLite catalog, which has one writer;
a start without a new release migrates nothing. `TimeoutStartSec` (15 min) bounds the migration, a held MariaDB migration lock included.
The unit restarts a failed start every 10 s with no start limit, so a bus or catalog that comes up later is waited for; `journalctl -u tracekeep` says why each start failed.

## Operator commands

`sudo -u tracekeep tracekeep admin …` and `tracekeep status` run directly against the catalog while the unit is stopped.
While it runs, the SQLite catalog is the service's alone, and the command goes through its API (`api.md` §10.1 *Admin over the API*):
it needs a static token holding `admin` (`auth.static`, as above), reaches the service at `api.public_url` and trusts its certificate.
Without one, the command says so, and runs once the unit is stopped.

## Checks

`make unit-check` verifies the unit with `systemd-analyze`.
The standalone leg of the packaging tests, `make packaging-standalone`, installs the unit, drives a browser through a troubleshooting session and upgrades from the newest ancestor on `main` whose packaging run passed;
it installs a system unit, so it refuses to run outside a GitHub-hosted runner or over an existing installation.
