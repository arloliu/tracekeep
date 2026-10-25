# Repository bootstrap

Status: active (2026-10-25) — phases 1 and 2 done; phase 3 next

tracekeep (`github.com/arloliu/tracekeep`) is the traffic-log service of the Virtual Equipment project:
it consumes the SECS/HSMS traffic records an equipment gateway publishes on NATS JetStream,
writes them as tracepack captures into S3-compatible storage,
and serves them to the VE generation service and to people troubleshooting tools.
The design arrived here as one document, the design seed (v21),
after twenty rounds of external review and the owner's decisions of 2026-10-25.
This plan turns an empty repository into one an AI agent can develop in for years:
conventions first, then the documents, then the module skeleton, then CI.
It holds no design; the seed and, after phase 2, the specification files do.

## Inputs

- The design seed v21, split by phase 2 into `docs/specs/tracekeep/`:
  its §11 became `docs/guides/repository.md`, §12 `docs/ROADMAP.md`, §13 `tracekeep-decisions.md`.
- `tmp/handoff/` (gitignored) — the owner's decision brief, the review reports of rounds 1–20,
  and the v20 text the last round approved.
  Named in plain text wherever a document needs them; never linked, never committed.
- The veq repository: its `.agents/rules/` (050–700), `scripts/docs-check.py`, `scripts/data-check.py`,
  `Makefile`, `mise.toml`, `.golangci.yaml`.
  tracekeep copies these conventions so the two repositories read alike;
  a divergence is a decision, recorded in the decision log.
- The eqp-hub repository: its `AGENTS.md` shape (tool-agnostic entry, `CLAUDE.md` importing it),
  `600-perf-sec.md`, and the memex idea of `150-memex.md`.
- tracepack `v0.7.1` (spec v2.36, requires go-secs v2.8.0 and Go 1.26),
  nats.go `v1.55.0` (the latest release that builds with Go 1.26),
  nats-server `v2.15.1` for `internal/testbus`.

## Decisions this plan applies

Taken by the owner on 2026-10-25 (`tracekeep-decisions.md`), in the bootstrap brief of the same day, or when phase 1 started;
phase 2 carried them into `tracekeep-decisions.md` as T-13…T-21:

| Topic | Decision |
|---|---|
| Toolchain | `mise` manages Go, golangci-lint, Atlas and Node; `mise.toml` is the single pin; `.tool-versions` is not kept |
| Go version | `go 1.26.0` in every `go.mod`, the line the gateway is on; the development toolchain is the latest Go 1.26 patch, 1.26.10 (owner, 2026-10-25, replacing Go 1.27) |
| Lint | golangci-lint 2.15.0, the latest release, built for Go 1.26 |
| Modules | two modules: the service (`github.com/arloliu/tracekeep`) and the SDK (`github.com/arloliu/tracekeep/producer`, its own `go.mod`, depending only on tracepack and nats.go) |
| Workspace | `go.work` at the root names both modules and is committed, with `go.work.sum`; CI also builds the service with `GOWORK=off` so its pinned requirement on the SDK stays buildable; the SDK is never built through the workspace in CI |
| tracepack | `v0.7.1` in both modules |
| nats.go | `v1.55.0` in both modules, nats-server `v2.15.1` for `internal/testbus` (owner, 2026-10-25); this raises the SDK's floor above the gateway's v1.40.1 and so revises `producer-sdk.md` §4.7 *Versions*, see *Open items* |
| Repository name | `github.com/arloliu/tracekeep` (T-8) |
| Stack | sqlx + hand-written SQL + Atlas CLI; connect-go, with its overhead measured against plain gRPC before phase 2's API is implemented; OIDC BFF; Vite + React + TypeScript (T-7) |
| Docs | the veq document lifecycle (living/finite, Status line, subject README, `docs-check`, close by deletion), with the tracekeep additions below |

## Phases

| Phase | Scope | State |
|---|---|---|
| 1 — conventions | the files in *Phase 1* below; `make docs-check data-check` green | done (4bd5ecc) |
| 2 — documents | split the seed into the files in *Phase 2*, each subject file opening with an interface summary; proposal P1; `make docs-check` green; the seed deleted | done (4bd5ecc, reflow 24e6a1b) |
| 3 — skeleton | `go.work`, the two `go.mod`, `producer/`, `cmd/tracekeep-sim/`, `internal/testbus/`, `cmd/tracekeep/` (`dev` and `migrate` as empty commands); `make ci` and `GOWORK=off go build ./...` green | pending |
| 4 — CI | GitHub Actions: lint, vet, test with the race detector, `docs-check`, `data-check`, both builds; the embedded nats-server in tests, no network | pending |
| 5 — close | this plan closes (its outcome in the commit message); `docs/plans/phase-0-producer.md`, created `draft` in phase 2, becomes `active` | pending |

Phases run in order; each is one or two commits.
Only the phase under way is detailed below; later phases are detailed when they start.

## Phase 1 — conventions

Create these files, copying from veq where the row says so and adapting only the project identity:

| File | Source | tracekeep changes |
|---|---|---|
| `AGENTS.md` | eqp-hub's shape, veq's content | identity: what tracekeep is, the two modules, the three repositories it depends on; the rules index; "start with `docs/ROADMAP.md`" |
| `CLAUDE.md` | veq | imports `AGENTS.md`; the language and git lines |
| `.agents/rules/050-principles.md` | veq | unchanged |
| `.agents/rules/100-overview.md` | veq | layout from the seed's §11 (now `docs/guides/repository.md`); dependencies: tracepack pinned exactly at a tagged release, nats.go as a floor in the SDK, `database/sql` drivers; prime directives unchanged (no sensitive data: no company, site, tool-ID, service or person names, no local paths, no real traffic; neutral terms: EAP, equipment gateway; evidence over assumption; small diffs) plus one: **the payload format lives in `go-secs/tracepack`, never here**; the SDK module's boundary (what it may import) stated once |
| `.agents/rules/200-coding-style.md` | veq | unchanged |
| `.agents/rules/300-testing.md` | veq | add: barrier-controlled schedules instead of timing; every catalog test runs on SQLite and MariaDB; `internal/testbus` for anything that touches JetStream; synthetic traffic only, a masked or synthetic pack is the only recorded input a test may use |
| `.agents/rules/400-documentation.md` | veq | unchanged (godoc, semantic linefeeds) |
| `.agents/rules/450-doc-lifecycle.md` | veq | tracekeep additions: the subject is `tracekeep`; decision IDs are `T-<N>`; review reports and archived revisions stay outside the repository; a spec file states on its first lines what it covers and when to read it |
| `.agents/rules/500-workflow.md` | veq | make targets below; `mise install` first; `GOWORK=off` build before a release of the service |
| `.agents/rules/550-git-conventions.md` | veq | unchanged (branch prefixes, Conventional Commits, no attribution trailers) |
| `.agents/rules/600-invariants.md` | new | the rules an agent must know before touching `producer/`, `internal/ingest/` or `internal/compact/`: the SDK's five guarantees and that a callback never blocks on anything external; the capture lock algorithm's commit point; lock order capture row → `counters`; resolution before framing and admission; forward-only states (`parking → parked → staged \| terminated`, `waiting → acked`); the one deletion predicate; stream replacement only as the stop-the-world procedure. Each rule names the spec section that proves it; the file never argues, it points |
| `.agents/rules/650-perf-sec.md` | eqp-hub's `600-perf-sec.md` | hot paths: the producer callback (no allocation, no I/O), the ingest batch; security: keys in memory only, clear text only on the bus and in the bucket, originals only in an API response, never in a file |
| `.agents/rules/700-lint-after-write.md` | veq | unchanged |
| `docs/ROADMAP.md` | — | a stub that indexes this plan, because `docs-check` requires the file and an `active` plan must be linked from it; phase 2 adds the milestones |
| `.agents/rules/150-memex.md` | eqp-hub | **deferred**: a `.knowledges/` bundle earns its place once code exists whose mechanics the specs do not state; revisit at the end of phase 1 of the seed |
| `Makefile` | veq | targets `help lint fmt vet check docs-check data-check hooks test bench ci`; `test` runs both modules; `ci` adds `GOWORK=off go build ./...` for the service |
| `mise.toml` | veq | `go = "1.26.10"`, `golangci-lint = "2.15.0"`, `atlas = "1.3.0"`, `node` pinned when `web/` starts (phase 4 of the seed) |
| `.golangci.yaml` | veq | unchanged |
| `scripts/docs-check.py`, `scripts/data-check.py`, `scripts/hooks/pre-commit` | veq | paths only |
| `data/README.md`, `data/.gitignore` | veq | the same owner-only rule; tracekeep has no sensitive inputs yet, the directory exists so the rule is in force from the first commit |
| `.github/` | — | phase 4 |

Gate: `make docs-check data-check` passes on this tree.
The Makefile is veq's unchanged, so `make ci` needs a `go.mod` and is phase 3's gate (owner, 2026-10-25).
veq has no `.semlf.ini`; the row was dropped (owner, 2026-10-25).
To pass `docs-check`, the seed's Status line was cut to one clause (the rest moved, unchanged, to the paragraph below it)
and the subject README names its file in backticks, as veq's do.

## Phase 2 — documents

The seed splits into `docs/specs/tracekeep/`, one living file per subject, each under 500 lines,
opening with three lines: what the file covers, which seed sections it came from, when an agent should read it.

| File | Seed sections |
|---|---|
| `README.md` | the index, read order, next free proposal ID |
| `positioning.md` | §1, §2, §14 |
| `architecture.md` | §3 |
| `producer.md` | §4.1–§4.6 |
| `producer-sdk.md` | §4.7 |
| `storage.md` | §5 |
| `ingest.md` | §6.1–§6.3 without *Parking* |
| `parking.md` | §6.1 *Parking* (protocol, confirmation, the two tables, resolution, incarnations, termination orders) and the parking rows of §7.1 |
| `rollouts.md` | §6.4 |
| `catalog.md` | §7 |
| `compactor.md` | §8 |
| `masking-keys.md` | §9 |
| `api.md` | §10.1–§10.3 |
| `operations.md` | §10.4, as the source the runbook guide is written from |
| `glossary.md` | new: capture, scope, version, parked copy, incarnation, survivor, termination order, processing configuration, the tunables Q I L D H τ |
| `tracekeep-decisions.md` | §13: the owner's decisions as `T-1` onward, the rulings relied on (G5-200…G5-269) by number, the service defaults; append-only |
| `docs/ROADMAP.md` | §12 as milestones, with the implementation checklist under phase 1; the index of active plans |
| `docs/plans/phase-0-producer.md` | §12 phase 0, as a phase table; `draft` until phase 5 (the checklist has no producer items) |
| `docs/guides/repository.md` | §11; the two modules, `go.work`, `mise`, how to run `make ci`, how a release of each module is cut |
| `proposals/p1-interfaces.md` | new: the interface changes of the deep-module review, as amended by its external review (finite, `draft`) |

Rules for the split: text moves, it is not rewritten;
a sentence that cites a review round is reworded to state the fact and name the round in plain text;
the seed's Status line becomes each file's own; the seed is deleted in the same commit and `README.md` lists every file.
Section numbers stay as the seed had them, so a `§N` in moved text keeps its meaning;
`README.md` maps each number to its file,
and `tracekeep-decisions.md` keeps §13's numbering (T-N is §13 item N).
Line breaking is unchanged by the move;
the long seed paragraphs were reflowed to semantic linefeeds in a separate commit that changes only line breaks.
One correction is applied in the move, because an owner decision already supersedes the text:
§11's "a `replace` to `./producer` for local development" becomes the committed `go.work` (`docs/guides/repository.md`).
The section pointers of `AGENTS.md`, `600-invariants.md` and `650-perf-sec.md` are rewritten to the split files.

Interface sections (owner, 2026-10-25, after the deep-module review and its external review):
each subject file (producer, producer-sdk, storage, ingest, parking, rollouts, catalog, compactor, masking-keys, api, operations)
opens, after its orientation lines, with an *Interface* section: a non-normative summary of what a caller must know,
every bullet traceable to the moved text below it, which stays authoritative.
The summary states what the seed fixes and what it leaves open; it changes no contract.
Contract changes go to `proposals/p1-interfaces.md`, reviewed and applied separately.
Positioning, architecture, glossary and the decision log get no interface section.

Gate: `make docs-check` passes;
each spec file's moved text is byte-identical to its seed lines (a script compares them before the seed is deleted);
no file under `docs/`, `.agents/`, `AGENTS.md` or `CLAUDE.md` names the seed file or points at a seed section (`grep -rE "design-se[e]d|Seed[ ]§"` finds nothing).

## Phase 3 — skeleton

Detailed when phase 2 closes.
Fixed points: `go.work` with `use (. ./producer)`;
the service `go.mod` requires `github.com/arloliu/tracekeep/producer` at a version and carries no `replace`;
`producer/go.mod` requires `github.com/arloliu/go-secs/tracepack v0.7.1` and `github.com/nats-io/nats.go v1.55.0`,
nothing else (go-secs `hsms`, which `secsobs` imports, comes with tracepack's requirement);
`internal/testbus` embeds nats-server with JetStream on a random port and file storage under `t.TempDir()`.

## Open items the new session inherits

- Whether timer-triggered FDC collection contexts count as interval polling (T-3) is with the FDC maintainers;
  thinning stays disabled until the poll channel exists.
- connect-go's overhead against plain gRPC is measured before the API of phase 2 of the seed is implemented;
  the result goes to the decision log.
- The veq branch `docs/ve/log-service-decisions` (commit 7b8a019) carries the LS-row amendments and V-35;
  it is not merged yet.
- The go-secs `main` with spec v2.36 is pushed; tracepack `v0.7.1` is released.
- nats.go `v1.55.0` in the SDK (owner, 2026-10-25) contradicts `producer-sdk.md` §4.7 *Versions*,
  which keeps the SDK's floor at or below the gateway's v1.40.1:
  importing the SDK raises the gateway to v1.55.0 (phase G).
  The nats.go v1.40.1 facts of the specs (`producer.md` §4.2 *Large records*, `producer-sdk.md` §4.7 *Publication policy*, `parking.md` confirmation and reconciler)
  and their nats-server v2.14.5 source citations are re-verified against v1.55.0 and v2.15.1,
  and §4.7 is amended, after phase 2 and before the SDK's first code.
