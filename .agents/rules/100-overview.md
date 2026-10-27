# 100 — Overview & Prime Directives

tracekeep (`github.com/arloliu/tracekeep`) is the traffic-log service of the Virtual Equipment (VE) project:
it consumes the SECS/HSMS traffic records an equipment gateway publishes on NATS JetStream,
writes them as tracepack captures into S3-compatible storage,
and serves them to the VE generation service and to people troubleshooting tools.
The design is in `docs/specs/tracekeep/`; the roadmap is `docs/ROADMAP.md`.

## Modules

| Module | Directory | Holds |
|---|---|---|
| `github.com/arloliu/tracekeep` | `.` | the service: `cmd/`, `internal/`, migrations, `web/`, `deploy/` |
| `github.com/arloliu/tracekeep/producer` | `producer/` | the producer SDK, tagged `producer/vX.Y.Z` |
| `github.com/arloliu/tracekeep/producer/internal/bustest` | `producer/internal/bustest/` | the SDK's tests against a real JetStream; test-only, never tagged or published (T-35) |

`go.work` joins the two for local development; `make work` creates it, and it is gitignored with `go.work.sum` (T-27).
The service requires the SDK by version and carries no `replace`, so it also builds with `GOWORK=off`;
until the SDK's first tag it does not require the SDK at all.
The bus-test and simulator modules require the SDK and the service through `replace` directives to this repository, which only a module nothing requires may carry.

The dependency direction is fixed and one-way: `go-secs/tracepack ← tracekeep/producer ← tracekeep`.
**The SDK's boundary**:
it imports the standard library, go-secs (`tracepack`, and `hsms` in `producer/secsobs`) and nats.go, nothing else.
It never imports the service module;
a header or convention both sides need goes into the SDK, never into the service.

## Layout

| Path | Scope |
|---|---|
| `cmd/tracekeep` | the one binary: `ingest`, `compact`, `api`, `migrate`, `all`, `dev` (embedded bus, fake S3, SQLite, simulated producer) |
| `cmd/tracekeep-sim` | the reference producer's command line, over `internal/sim`; back in the service module since phase 1.6 (T-38) |
| `internal/sim` | the reference producer on the SDK: a go-secs loopback pair with the observers, or replay of tracepack packs; run by `cmd/tracekeep-sim` and `tracekeep dev` |
| `internal/testbus` | embedded nats-server with JetStream for tests and `dev`; service module only, never the SDK |
| `internal/bus` | JetStream pull consumers, claim-check fetch, message disposition |
| `internal/ingest` | per-scope batches, sort, dedupe, conflicts, mask, write, register, acknowledge; the parking coordinator and the parked consumer |
| `internal/admin` | the operator operations the admin commands run: parked list and terminate, capture terminate and show, bus adopt-stream |
| `internal/compact` | merge, close, deletion, retention, reconciler, leases |
| `internal/catalog` | sqlx repository, dialects, statements, compare-and-set helpers, counters |
| `internal/store` | `ObjectStore` (s3, fs, mem), the `ReaderAt` adapter, the conformance suite |
| `internal/keys` | `KeyProvider` (vault, file, env), key bindings, `redact.Keys` |
| `internal/policy` | redaction policy and processing configuration |
| `internal/api`, `internal/auth`, `internal/config` | connect handlers and export jobs; OIDC BFF, sessions, roles, service tokens; configuration |
| `proto/tracekeep/v1` | the machine API |
| `migrations/`, `schema.hcl`, `atlas.hcl` | the catalog schema and its Atlas migrations, one directory per engine |
| `producer/` | the SDK: `producer`, `secsobs`, `busconfig`, `producertest` |
| `web/` | the frontend: Vite, React, TypeScript |
| `deploy/` | Helm, systemd and compose packaging |
| `docs/` | specs, plans and guides under `450-doc-lifecycle.md` |
| `scripts/` | repository checks (`docs-check.py`, `data-check.py`) and git hooks (`hooks/`) |
| `data/` | sensitive inputs, owner-only; only its `README.md` and `.gitignore` are tracked (`make data-check`) |
| `tmp/` | local scratch and handoff material; gitignored |

The table is the target layout.
Create a directory when its first code lands, not before.

## Dependencies

- SECS-II and HSMS come from `github.com/arloliu/go-secs/v2`.
- Captures and bus records are tracepack, the nested module `github.com/arloliu/go-secs/tracepack`,
  pinned exactly at a tagged release in both modules; read its changelog before every upgrade.
- nats.go is required at the same version in both modules.
  The SDK's requirement is a floor for every producer that links it (minimal version selection picks the highest),
  so raising it raises the gateway: that is a decision for the decision log, never a side effect of a feature.
- The catalog uses sqlx with hand-written SQL over `database/sql` drivers for SQLite and MariaDB;
  migrations are Atlas: the CLI generates them, the binary applies them through Atlas's Go library (T-41).
- Runtime dependencies are intentionally minimal — check `go.mod` before adding any.
  Prefer stdlib.
  A new dependency of the SDK is a decision, since every producer inherits it.
- Each module's minimum Go version is the `go` directive in its `go.mod`; `mise.toml` pins the development toolchain.

## Prime Directives

1. **No sensitive data in the repository.**
   No company, site, tool-ID, internal-service or person names, no local paths,
   no quoted vendor-manual content, no real traffic.
   Real inputs live only in `data/`; tests and examples use synthetic data.
2. **Neutral terms.**
   The host-side automation program is the EAP;
   the gateway between tools and EAPs is the equipment gateway.
   eqp-hub, the repository that implements it, may be named.
3. **Evidence over assumption.**
   A design claim about the bus, the gateway or a library cites the evidence it rests on (a version and a source location),
   or says it is an assumption.
4. **Small diffs.**
   Don't rewrite files unnecessarily.
5. **The payload format lives in `go-secs/tracepack`, never here.**
   Bus framing, the record header, masking output and the pack file are tracepack's.
   A change tracekeep needs is a request to the tracepack maintainers, never a local variant.
