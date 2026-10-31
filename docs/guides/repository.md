# Repository guide

Status: current (2026-10-31) — from seed v21 §11, the bootstrap decisions and the frontend (T-49)

How the repository is laid out, how its two modules fit together, and how to build, check and release them.
Read when setting up a clone, adding a directory, or cutting a release.

## 11. Repository layout

```
tracekeep/
  go.mod                      the service module
  cmd/tracekeep/              one binary: ingest | compact | api | migrate | all | dev (embedded bus, fake S3, SQLite, sim producer)
  cmd/tracekeep-sim/          reference producer on the SDK: a go-secs loopback pair with the observers, or replay of tracepack packs
  internal/testbus/           embedded nats-server with JetStream for tests and `dev` (service module only, never the SDK)
  internal/bus/               JetStream pull consumer, claim-check fetch, disposition (§6.1)
  internal/ingest/            per-scope batches, sort, dedupe, conflicts, mask, write, register, ack
  internal/compact/           merge, close, deletion, retention, reconciler, leases
  internal/catalog/           sqlx repository, dialect value, statements, CAS helpers, counters
  internal/store/             ObjectStore: s3, fs, mem; ReaderAt adapter; conformance suite
  internal/keys/              KeyProvider: vault, file, env; bindings; redact.Keys
  internal/policy/            redaction policy and processing configuration
  internal/api/               connect handlers, export jobs
  internal/auth/              OIDC BFF, cookie sessions, roles, service tokens
  internal/config/
  proto/tracekeep/v1/
  migrations/mariadb/  migrations/sqlite/  schema.hcl  atlas.hcl
  producer/                   the producer SDK, its own go.mod (§4.7)
    go.mod  producer.go  capture.go  ring.go  pending.go  sampler.go
    secsobs/  busconfig/
  web/                        Vite + React + TypeScript
  deploy/helm/  deploy/systemd/  deploy/compose/
  docs/specs/tracekeep/  docs/plans/  docs/guides/   AGENTS.md  .agents/rules/
```


The service module requires the SDK module by version (no `replace`:
a local, gitignored `go.work` joins the two modules for development, T-27);
the SDK requires tracepack (which itself requires go-secs/v2) and nats.go, nothing else;
a Prometheus `Metrics` adapter is the producer's own few lines, not an SDK package.
Conventions (rules, doc lifecycle, semantic linefeeds, lint) are copied from veq so the two repositories read alike.

The layout is the target; a directory is created when its first code lands (`.agents/rules/100-overview.md`).
The layout's one correction to the seed is the sentence on `replace`:
an owner decision replaced it with a workspace, now a local one (T-27).

## The two modules

- `github.com/arloliu/tracekeep` at the root: the service, one binary.
- `github.com/arloliu/tracekeep/producer` in `producer/`:
  the SDK, its own `go.mod`, depending only on tracepack and nats.go (T-16).

`go.work` names both; `make work` creates it, and it stays out of the repository with `go.work.sum` (T-27),
as in go-secs, so a committed workspace never hides a broken `go.mod` or overrides a developer's own.
The service requires the SDK by version and carries no `replace`, so it also builds with `GOWORK=off`;
until the SDK's first tag the service imports nothing from it and does not require it, since no version resolves without the workspace;
the SDK resolves its own `go.mod`, never the service's graph, so the Makefile runs it with `GOWORK=off` (T-27).

## Setup and checks

1. `mise install` — Go, golangci-lint, Atlas and Node at the versions `mise.toml` pins (T-13);
   `cd web && npm ci` installs the frontend's packages from `web/package-lock.json`.
2. `make hooks` — once per clone.
3. `make ci` — lint, vet, docs-check, data-check and the tests of both modules, then the service built with `GOWORK=off`;
   it needs no Node. `make web-check` — the frontend's lint, type check and tests, which CI's `web` job runs with `make build`.

`.agents/rules/500-workflow.md` lists every make target and the checks before a commit.

## The frontend

`web/` is the pages people use (`api.md` §10.3): Vite, React and TypeScript, its client generated from `proto/` into `web/src/gen/` by `make proto`.
The binary embeds it by build tag:
`make build` runs `make web` and builds `dist/tracekeep` with `-tags webdist`, embedding `web/dist`;
a build without the tag, `go build ./...` and `make ci` included, embeds a page saying the frontend was not built, so Go needs no Node.

To work on the pages, run dev with Vite's server as its public URL, and Vite with dev's certificate:

```
tracekeep dev --data tmp/dev --public-url https://localhost:5173
cd web && TRACEKEEP_DEV_DATA=../tmp/dev npm run dev
```

Then open `https://localhost:5173`, trusting dev's certificate (`tmp/dev/tls/cert.pem`) by an exception or by importing it,
and sign in at dev's test IdP with any subject and roles.
Vite serves the pages with hot reload and proxies `/auth/`, `/download/` and the services to dev's API,
`https://127.0.0.1:8080` unless `TRACEKEEP_DEV_API` names another `--api`;
the session cookie and the Origin check hold because the browser sees one origin, the public URL.
The simulator's tool is `SIM-TOOL`, its device `eqp`.

The pages' tests are Vitest with Testing Library in jsdom, the API answered by a Connect router transport (`.agents/rules/300-testing.md`).

## Releases

- **The SDK**: tag `producer/vX.Y.Z` on a commit where the `producer/` module passes its tests with `GOWORK=off`.
- **The service**: built by `make build`, so the binary carries the frontend;
  raise its requirement on the SDK to a tagged version first;
  `GOWORK=off go build ./...` must pass (`make ci` runs it), so the release never depends on the workspace.
