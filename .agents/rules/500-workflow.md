# 500 — Workflow

## Setup

1. `mise install` — Go, golangci-lint and Atlas at the versions `mise.toml` pins.
2. `make hooks` — run `data-check` before every commit; once per clone.

## Before commit

1. `make lint` — fix all issues.
2. `make test` — must pass with the race detector, in every module.
3. Touched `docs/`?
   `make docs-check` must pass.
4. Touched a parser of external input?
   Run its fuzz target.
5. Touched `producer/`, `internal/ingest/` or `internal/compact/`?
   Check the change against `600-invariants.md`.
6. Search the diff for sensitive data (`100-overview.md`, prime directive 1) before staging.

`make ci` runs every gate.

## Before a release

- The service: `GOWORK=off go build ./...` must pass,
  so its pinned requirement on the SDK builds without the workspace (`make ci` runs it).
- The SDK: tagged `producer/vX.Y.Z` from the `producer/` module; a service release that needs it requires that tag.

## Commits

See `550-git-conventions.md` for branch, commit-message, and pull-request conventions.

## Review checklist

- [ ] Correctness
- [ ] No sensitive data: names, tool IDs, paths, real traffic
- [ ] Tests use synthetic data and fail when the behaviour they guard changes
- [ ] The rules of `600-invariants.md` still hold
- [ ] Docs updated under `450-doc-lifecycle.md`; Status lines and subject READMEs in step
- [ ] New comments and prose use semantic linefeeds (`400-documentation.md`)

## Make targets

```bash
make lint           # golangci-lint run, every module
make fmt            # golangci-lint fmt, every module
make vet            # go vet, every module
make check          # lint + vet
make docs-check     # docs/ against 450-doc-lifecycle.md
make data-check     # nothing under data/ tracked but README.md and .gitignore
make hooks          # enable scripts/hooks
make test           # every module, -short, -race
make test-latest    # the SDK and bustest on a copy with the latest nats.go (needs the module proxy; its own CI job)
make bench
make ci             # check + docs-check + data-check + test + the service built with GOWORK=off
```

Run `make` with no target for the help list.
