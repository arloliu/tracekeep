# Repository guide

Status: current (2026-10-25) — from seed v21 §11 and the bootstrap decisions

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

1. `mise install` — Go, golangci-lint and Atlas at the versions `mise.toml` pins (T-13).
2. `make hooks` — once per clone.
3. `make ci` — lint, vet, docs-check, data-check and the tests of both modules, then the service built with `GOWORK=off`.

`.agents/rules/500-workflow.md` lists every make target and the checks before a commit.

## Releases

- **The SDK**: tag `producer/vX.Y.Z` on a commit where the `producer/` module passes its tests with `GOWORK=off`.
- **The service**: raise its requirement on the SDK to a tagged version first;
  `GOWORK=off go build ./...` must pass (`make ci` runs it), so the release never depends on the workspace.
