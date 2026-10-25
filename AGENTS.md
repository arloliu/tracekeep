# tracekeep — Agent Configuration

Guidance for any agent tool working in this repository.
Tool-specific entry points (`CLAUDE.md`) import this file.

## What this project is

**tracekeep** (`github.com/arloliu/tracekeep`) is the traffic-log service of the Virtual Equipment project:
it consumes the SECS/HSMS traffic records an equipment gateway publishes on NATS JetStream,
writes them as tracepack captures into S3-compatible storage,
and serves them to the VE generation service and to people troubleshooting tools.

Two Go modules live here, joined for local development by a gitignored `go.work` that `make work` creates:

- `github.com/arloliu/tracekeep` — the service, one binary (`cmd/tracekeep`);
- `github.com/arloliu/tracekeep/producer` — the producer SDK an equipment gateway links, its own `go.mod`.

Three repositories shape it:
go-secs (SECS-II, HSMS and `tracepack`, which owns the payload format),
eqp-hub (the equipment gateway, the first producer to link the SDK),
and veq (the VE design whose log service tracekeep is, and the source of these conventions).

Start with `docs/ROADMAP.md` for where the work stands.
The design is in `docs/specs/tracekeep/`; start with its `README.md`.

## Working principles

- No sensitive data in the repository:
  no company, site, tool-ID, service or person names, no local paths, no real traffic.
- Surface uncertainty before coding; minimum change that solves the problem; verify, do not guess.
- The payload format lives in `go-secs/tracepack`, never here.

## Git

Never add `Co-Authored-By` or any attribution trailer.
Branches, commit messages and pull requests follow `550-git-conventions.md`.

## Rules

Rules are in `.agents/rules/`.
Read any file whose topic matches the task before editing code or documents.

| File | Topic |
|------|-------|
| `050-principles.md` | Working principles |
| `100-overview.md` | Modules, layout, dependencies, the SDK's boundary, prime directives |
| `200-coding-style.md` | Go idioms, error handling, file layout |
| `300-testing.md` | Test organization, synthetic data, barriers, both catalog engines, the embedded bus |
| `400-documentation.md` | Godoc format, comment and prose line breaking (semantic linefeeds) |
| `450-doc-lifecycle.md` | Document lifecycle: classes, Status line, plans, closing, roadmap, docs-check |
| `500-workflow.md` | Setup, pre-commit checks, make targets |
| `550-git-conventions.md` | Branch, commit-message and pull-request conventions |
| `600-invariants.md` | Rules to know before touching `producer/`, `internal/ingest/` or `internal/compact/` |
| `650-perf-sec.md` | Hot paths and security boundaries |
| `700-lint-after-write.md` | Lint workflow |
