---
trigger: always_on
glob: "**/*.go"
description: Run linter after modifying Go files
---

# Lint After Write

After modifying any `.go` file:

1. **Modernize:** `go fix ./...` in each module you changed (in `producer/` with `GOWORK=off`, as the Makefile does) to keep the code on current stdlib idioms.
   Review the diff — every change must be behavior-preserving.
2. **Run:** `make lint`.
3. **Fix:** all reported issues before committing.
4. **Re-run:** until clean.

`go fix` modernizers can introduce new lint findings, so always lint after it.
Fix `goimports` / formatting findings with `make fmt`.
