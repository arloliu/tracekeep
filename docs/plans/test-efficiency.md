# Test efficiency

Status: active (2026-11-06) — phase 2, the catalog job

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
| 1 — catalog setup | SQLite catalogs copied from one migrated template per test process; the test MariaDB server's data on tmpfs | done (#96) |
| 2 — the catalog job | the engine loops' SQLite runs left to `ci`; the job's packages split into matrix legs; the two timing flakes fixed | in-progress |
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

CI's `catalog` job after phase 1 (#96): `make test-catalog` 7 min 2 s of its 9 min 11 s on a four-core runner, CPU-bound where the local machine has 32 cores;
`internal/catalog` 326 s, `internal/compact` 173 s, `internal/ingest` 170 s, `internal/api` 126 s, `internal/service` 98 s, the rest 12 s or less, run side by side.

Two tests failed once each during a full MariaDB run and passed when run alone:
`TestLeaseLostDuringACycle` (`internal/compact/run_test.go`) and `TestExportWorkerCountsTheLeaseFromItsClaim` (`internal/api/export_worker_test.go`).
Neither failed again in four runs of their packages on tmpfs nor in two on a disk.
Each held a margin a loaded runner can spend: the first cycle of the lease test ran under the 1 s lease only its second cycle needs,
and the export test told 200 ms from 600 ms with a 450 ms bound.
A third, `TestRecordListingLeaseLostBetweenTransactions`'s `sweep pages` on MariaDB, failed once in the run on a disk, its second page deleted though the lease had lapsed;
its barrier sees the lapse's statement in the process list, not that it waits for the lease row, and INNODB_TRX did not show the tests' waits.
Its cause is not proven, and this plan leaves it open.

`TestRunChaosFailsBrokenWiring`'s `no_traffic` and `only_startup_traffic` take 40 s and 30 s under `-race`, `TestRunChaos` 0.5 s:
each failing scenario waits out the 10 s `settleTimeout`.

## Phase 2 — the catalog job

### Work

- Every two-engine test loop (`catalogtest.Engines`, `internal/catalog`'s `engines` and `fenceEngines`, `TestLiveRoleMigration`, `migrations`' `engines`) runs the engines `TRACEKEEP_BENCH_ENGINES` names, as the benchmark loops do;
  `scripts/catalog.sh test` and `bench-smoke` set it to `mariadb`.
  `./migrations/migrationstest/...`, `./internal/auth/...` and `./cmd/tracekeep/...` stay out of the list: none of their tests reaches MariaDB.
- `scripts/catalog.sh` names its packages in legs, `PACKAGES` being every leg's; `legs` prints them for CI and fails when two share a package or one has none;
  `test <leg>` and `make test-catalog LEG=<leg>` run one.
- `ci.yaml`'s `catalog` job is a matrix of those legs, `migrate-check` and `bench-catalog-smoke` in the `rest` leg.
- The lease test runs its first cycle under the default lease and only the lost cycle under a short one;
  the export test's lease and claim delay grow to 2 s and 1.6 s, and it bounds the put's cancellation from when the claim was asked for, at 2 to 2.8 s, where a lease counted from the answer gives 3.6 s.

### Done when

- With `TRACEKEEP_BENCH_ENGINES=mariadb`, `go test -json` of `internal/catalog` and `migrations` passes or skips no `sqlite` subtest; without it, 262 pass.
- `scripts/catalog.sh legs` rejects a package in two legs and a leg with none.
- Each fixed test passes `-count 50` under `-race` on one MariaDB server while its packages' tests run `-count 3` on it,
  and fails with the mechanism it asserts removed: the lease counted from the answer, or the keeper's cancellation on a lost lease.
- CI's `catalog` legs pass, and the measurements gain their times.

## Gate cases

- Phase 1's cases (#96).
- In the `catalog` job, no two-engine loop runs its `sqlite` subtest, `TestLiveRoleMigration`'s included, the loops honouring `TRACEKEEP_BENCH_ENGINES` in tests as in benchmarks;
  `ci`'s `go test -json` passes and skips the same tests as before the change, opt-in tests such as `TestEngineFacts` skipping as they do today.
- `bench-smoke` covers every package of `scripts/catalog.sh`'s list, and `test`'s server is on tmpfs while the benchmarks' is not.
- The matrix legs' packages, expanded by `go list`, are disjoint and none empty, and `PACKAGES` is every leg's.
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
