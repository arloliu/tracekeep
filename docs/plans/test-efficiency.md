# Test efficiency

Status: active (2026-11-06) — phase 1, catalog setup once per process

The work that delivers T-61 (`docs/specs/tracekeep/tracekeep-decisions.md`) before P3's phase plan (T-60):
a catalog test stops paying for twelve migrations on SQLite and for a disk's syncs on MariaDB,
the CI catalog job stops repeating the engine loops' SQLite runs and splits its packages across runners,
the chaos and load harnesses take their time as parameters,
and benchmarks run at `-count 5`.
The rationale is T-61's; this plan only orders the work and records its measurements.
No test loses an engine it runs on today, and none loses `-race`.
Tests that open only SQLite though the catalog's two engines would serve them are not in this plan.

## Phases

| Phase | Work | State |
|---|---|---|
| 1 — catalog setup | SQLite catalogs copied from one migrated template per test process; the test MariaDB server's data on tmpfs | in-progress |
| 2 — the catalog job | the engine loops' SQLite runs left to `ci`; the job's packages complete and split into matrix legs; the two timing flakes fixed | pending |
| 3 — harness time | the chaos harness's settle wait shortened for its failure tests; the load harness's production mode ratio-scaled and on either engine | pending |
| 4 — benchmarks and close | benchmarks at `-count 5`; the rules and guides that name the old behaviour; the measurements after; this plan closed | pending |

Each phase is one pull request.

## Measurements

Before this plan, on `main` at 6de1673.
CI's numbers are the capture identity's pull requests' runs.
The rest are one local machine's: the package times from `go test` and `make test-catalog`,
the setup costs from throwaway tests, not kept, that time 10 or 20 calls of the step one after another and report the mean.

| What | Number |
|---|---|
| `ci` job | 9–14 min wall |
| `catalog` job | 10–14 min wall |
| the service module's tests, `-race` | 68 s wall, 15 min 44 s CPU |
| the same without `-race` | 14 s wall, 67 s CPU |
| a full `make test-catalog` | about 11 min wall; `internal/compact` 626 s, `internal/api` 412 s |
| one SQLite catalog's `migrations.Apply` | 229 ms, 620–667 ms under `-race` |
| the same with `synchronous(OFF)`, under `-race` | 403 ms |
| one SQLite catalog's `catalog.Open` after it | 1.3 ms, 9 ms under `-race` |
| a migrated SQLite file copied | 76 µs |
| SQLite catalogs migrated through `catalogtest` and `internal/catalog`'s harness in one `go test ./... -short` of the service module | 846 |
| one MariaDB catalog created, migrated and dropped, under `-race`, `make test-catalog`'s server | 1.5 s (migration 1.27 s, drop 0.24 s) |
| the same with InnoDB's file per table, doublewrite and log flush at commit off | 1.1 s |
| the same with the server's data directory on tmpfs | 63 ms (migration 54 ms, drop 2.4 ms) |

SQLite's migration stays slow with its syncs off, and a copy of a migrated file costs almost nothing;
MariaDB's setup is fast on tmpfs.
846 SQLite migrations at 620–667 ms are about 9 minutes of setup run one at a time, beside the module's 15 min 44 s of CPU under `-race`.

After phase 1, on the same machine, under `-race`:

| What | Before | After |
|---|---|---|
| the service module's `go test ./... -short -race` | 83 s wall, 19 min 38 s CPU | 29 s wall, 5 min 24 s CPU |
| a full `make test-catalog` | about 11 min wall | 36 s wall: `internal/catalog` 31 s, `internal/compact` 28 s, `internal/ingest` 19 s, `internal/api` 18 s, `internal/service` 13 s, the rest 4 s or less |

Two tests failed once each during a full MariaDB run and passed when run alone:
`TestLeaseLostDuringACycle` (`internal/compact/run_test.go`) and `TestExportWorkerCountsTheLeaseFromItsClaim` (`internal/api/export_worker_test.go`).
Their causes are not yet known.

`TestRunChaosFailsBrokenWiring`'s `no_traffic` and `only_startup_traffic` take 40 s and 30 s under `-race`, `TestRunChaos` 0.5 s:
each failing scenario waits out the 10 s `settleTimeout`.

## Phase 1 — catalog setup

Today `catalogtest.OpenClosable` and `internal/catalog`'s `newCatalog` (`harness_test.go`) each migrate a fresh catalog for every test.
`internal/catalog`'s tests cannot import `catalogtest`, which imports `internal/catalog`,
so the template goes in a new test-support package, `migrations/migrationstest`, which imports only `migrations` and the SQLite driver, never a catalog package.
Tests that test migrations or a historical schema (`migrations`' own, `internal/catalog`'s `preFence` and `migratedBefore`) keep migrating;
`catalog.Open` still runs for every catalog.

### Work

- **The SQLite template**: the first catalog a process asks for migrates a template file in a temporary directory, once;
  it is set to `journal_mode=DELETE` and closed, any error failing every catalog that asks for it, and no `-wal` or `-shm` file may remain.
  The process keeps its bytes and removes the directory, so no test can change the template and nothing outlives the process.
  Every catalog is then a copy of those bytes in the test's own `t.TempDir()`, which `catalog.Open` opens in WAL mode as today.
- **The test MariaDB server**: `scripts/catalog.sh test` starts its container with the data directory on tmpfs;
  `bench` and `bench-smoke` keep the disk, whose durability a baseline records (T-54).
  Every MariaDB catalog stays a database created and migrated for its test alone, as today.
- `300-testing.md` *Catalog* states what the tests rely on: a test's SQLite catalog is a copy of a template migrated once per process and kept in memory;
  a test's MariaDB catalog is a database created and migrated for it alone, on a test server whose data is on tmpfs.

### Done when

- On SQLite, a catalog from the template has the schema (`sqlite_master`, triggers and indexes included) and the rows of a freshly migrated catalog,
  its revision rows compared on every column but `executed_at` and `execution_time`,
  and `migrations.Apply` on it applies nothing;
  the comparison rejects an extra table, a missing index, a changed counter and a changed revision hash.
- Two copies taken in parallel and one taken after the first test ended are independent files: a write to one is absent from the others and from the template.
- Parallel first requests migrate the template once; a template that fails to migrate or to close fails each test that asks for a catalog, and no partial copy is handed out.
- `make test` and `make test-catalog` pass,
  and the measurements gain the after numbers: the service module's CPU under `-race` and `make test-catalog`'s wall time with each package's.

## Gate cases

- Phase 1's cases.
- In the `catalog` job, no two-engine loop runs its `sqlite` subtest, `TestLiveRoleMigration`'s included, the loops honouring `TRACEKEEP_BENCH_ENGINES` in tests as in benchmarks;
  `ci`'s `go test -json` passes and skips the same tests as before the change, opt-in tests such as `TestEngineFacts` skipping as they do today.
- `bench-smoke` still covers every package of `scripts/catalog.sh`'s list, and the `test` leg's server is on tmpfs while the benchmarks' is not.
- The matrix legs' packages, expanded by `go list`, are disjoint, none empty, and together equal `scripts/catalog.sh`'s list, which gains `./migrations/...`, `./internal/auth/...` and `./cmd/tracekeep/...`.
- Each former flake passes `-count 50` under `-race` on MariaDB with its package's tests loading the same server,
  and its fix fails with the mechanism it asserts removed.
- `RunChaos`'s exported signature and 10 s default are unchanged; `TestRunChaosFailsBrokenWiring` fails each broken wiring with that wiring's own error, in under 5 s under `-race`;
  `TestRunChaos` passes with the same short wait and with the default.
- `TRACEKEEP_LOAD=production` runs F 10 s, `ack_wait` 14 s and `max_ack_pending` 20,000 and 1,667 at today's rate, and passes on SQLite;
  `production-smoke` passes on SQLite and on MariaDB;
  a run fails unless both cases are judged with their offered rate held, no drops and no failed pass, the main case sustained and the control reaching its budget;
  on MariaDB the SQL counts print as unavailable, never zero;
  `production-real` runs today's 5 m and 7 m spec, for P3's gate (T-60).
- `scripts/catalog.sh bench` and `bench.yaml` run `-count 5`; `cmd/benchjudge`'s tests judge five-sample rows: pass, regression, measure again, exact metrics, a missing row and a count mismatch.
