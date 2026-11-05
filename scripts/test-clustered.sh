#!/usr/bin/env bash
# Runs the clustered family, every test on a bus of three servers (testbus.StartCluster), COUNT times with the race detector,
# then lists each of its tests by name with its passes, failures and skips, so a skipped or missing case shows.
# A catalog test runs on SQLite, and on MariaDB too when TRACEKEEP_MARIADB_DSN names a server; MariaDB then fails rather than skip.
# Usage: scripts/test-clustered.sh   (COUNT, default 20; TIMEOUT, default 60m; GOTESTFLAGS)
set -euo pipefail

count=${COUNT:-20}
# The family's tests as go test -v names them; one renamed or gone runs no time and fails the run.
tests=(
	TestStartClusterReplicates
	TestPublishCopy/clustered
	TestSuppressedPublishConverges/clustered
	TestTwoVersionsInOneWindow/inline/clustered/sqlite
	TestTwoVersionsInOneWindow/inline/clustered/mariadb
	TestTwoVersionsInOneWindow/claim-check/clustered/sqlite
	TestTwoVersionsInOneWindow/claim-check/clustered/mariadb
	TestCopiesFromAFollower/sqlite
	TestCopiesFromAFollower/mariadb
)
run='^(TestStartClusterReplicates|TestPublishCopy|TestSuppressedPublishConverges|TestTwoVersionsInOneWindow|TestCopiesFromAFollower)$/^(clustered|inline|claim-check|sqlite|mariadb)$/^clustered$'

if [ -n "${TRACEKEEP_MARIADB_DSN:-}" ]; then
	export TRACEKEEP_REQUIRE_MARIADB=1
	echo "engines: sqlite, mariadb"
else
	echo "engines: sqlite (TRACEKEEP_MARIADB_DSN unset: mariadb skips)"
fi
log=$(mktemp)
trap 'rm -f "$log"' EXIT
status=0
# shellcheck disable=SC2086 # GOTESTFLAGS splits into flags
GOWORK=off CGO_ENABLED=1 go test ./internal/testbus/ ./internal/ingest/ ./internal/compact/ \
	-race -count "$count" -timeout "${TIMEOUT:-60m}" -v -run "$run" ${GOTESTFLAGS:-} >"$log" 2>&1 || status=$?
if [ "$status" -ne 0 ]; then
	grep -vE '^\s*(=== (RUN|PAUSE|CONT|NAME)|--- (PASS|SKIP))' "$log" || true
fi

awk -v count="$count" -v names="${tests[*]}" -v maria="${TRACEKEEP_MARIADB_DSN:+1}" '
	match($0, /--- (PASS|FAIL|SKIP): [^ ]+/) {
		split(substr($0, RSTART + 4, RLENGTH - 4), w, ": ")
		n[w[2], w[1]]++
	}
	END {
		printf "%-56s %5s %5s %5s\n", "test", "pass", "fail", "skip"
		k = split(names, list, " ")
		for (i = 1; i <= k; i++) {
			t = list[i]
			p = n[t, "PASS"] + 0; f = n[t, "FAIL"] + 0; s = n[t, "SKIP"] + 0
			printf "%-56s %5d %5d %5d\n", t, p, f, s
			if (p != count && !(maria == "" && t ~ /\/mariadb$/ && s == count)) bad = 1
		}
		exit bad
	}' "$log" || status=1
exit "$status"
