#!/usr/bin/env bash
# The catalog's migrations and its MariaDB tests and benchmarks, each needing a MariaDB server in a container.
# Usage:
#   scripts/catalog.sh diff <name>   generate a migration for both engines from schema.hcl
#   scripts/catalog.sh check         validate both migration directories and check schema.hcl for drift
#   scripts/catalog.sh test [leg]    run the catalog's tests on MariaDB alone (TRACEKEEP_BENCH_ENGINES=mariadb), each package within 30 minutes,
#                                    over one leg's packages or every leg's
#   scripts/catalog.sh legs          print the legs as a JSON array, failing if two share a package or one has none
#   scripts/catalog.sh bench <file>  run the catalog's benchmarks on MariaDB alone (TRACEKEEP_BENCH_ENGINES=mariadb),
#                                    -count ${COUNT:-10} -benchtime ${BENCHTIME:-1s} -cpu ${CPU:-nproc},
#                                    over ${BENCH_PACKAGES:-every package reaching the catalog} and -bench ${BENCH:-.},
#                                    then registration at its fixed N when BENCH is . or names RegisterPack, into file without Go's cpu: line
#   scripts/catalog.sh bench-smoke   run them once each, failing rather than skipping without MariaDB
set -euo pipefail

MARIADB_IMAGE=mariadb:11.8
PASSWORD=tracekeep

# The packages whose tests or benchmarks reach MariaDB's catalog, in legs CI runs side by side (ci.yaml's catalog matrix):
# each leg about a CI runner's worth of tests, and PACKAGES every leg's.
declare -A LEGS=(
	[catalog]="./internal/catalog/..."
	[compact-api]="./internal/compact/... ./internal/api/..."
	[ingest-service]="./internal/ingest/... ./internal/service/..."
	[rest]="./migrations/ ./internal/bus/... ./internal/admin/... ./internal/keys/... ./internal/metrics/..."
)
PACKAGES="${LEGS[*]}"

container=
cleanup() { [ -z "$container" ] || docker rm -f "$container" >/dev/null; }
trap cleanup EXIT

# start_mariadb starts a server with the databases dev (Atlas's scratch database) and catalog (the tests'), and sets port;
# its arguments go to docker run.
start_mariadb() {
	# Byte-exact strings, as on SQLite: the generated tables carry the server's default collation.
	# Every test package runs at once, each test with catalogs of its own, beyond the default 151 connections.
	container=$(docker run -d "$@" -p 127.0.0.1::3306 -e MARIADB_ROOT_PASSWORD=$PASSWORD -e MARIADB_DATABASE=dev "$MARIADB_IMAGE" \
		--character-set-server=utf8mb4 --collation-server=utf8mb4_nopad_bin --max-connections=1000)
	port=$(docker port "$container" 3306 | head -1 | cut -d: -f2)
	for _ in $(seq 120); do
		# The image's first start runs a temporary server; healthcheck.sh answers only for the real one.
		if docker exec "$container" healthcheck.sh --connect --innodb_initialized >/dev/null 2>&1; then
			docker exec "$container" mariadb -uroot -p$PASSWORD -e 'CREATE DATABASE IF NOT EXISTS catalog' && return 0
		fi
		sleep 1
	done
	echo "MariaDB not ready" >&2
	docker logs --tail 50 "$container" >&2
	return 1
}

atlas_env() {
	export TRACEKEEP_ATLAS_MARIADB_DEV="maria://root:$PASSWORD@127.0.0.1:$port/dev"
}

case "${1:-}" in
diff)
	name=${2:?usage: catalog.sh diff <name>}
	start_mariadb
	atlas_env
	for engine in sqlite mariadb; do
		atlas migrate diff "$name" --env catalog --var engine=$engine
	done
	;;
check)
	start_mariadb
	atlas_env
	for engine in sqlite mariadb; do
		atlas migrate validate --env catalog --var engine=$engine
		# A schema.hcl change without its migration leaves the directory behind the schema:
		# a diff into a copy of the directory then writes a new file there.
		tmp=$(mktemp -d)
		cp -r "migrations/$engine/." "$tmp"
		atlas migrate diff drift --env catalog --var engine=$engine --dir "file://$tmp" >/dev/null
		if [ "$(ls "$tmp" | wc -l)" -ne "$(ls "migrations/$engine" | wc -l)" ]; then
			echo "$engine: schema.hcl has changes no migration holds:" >&2
			cat "$tmp"/*_drift.sql >&2
			exit 1
		fi
		rm -rf "$tmp"
	done
	;;
legs)
	# go list expands each leg; a package in two legs, or a leg naming none, fails.
	seen=$(for leg in "${!LEGS[@]}"; do GOWORK=off go list ${LEGS[$leg]} | sed "s|^|$leg |"; done)
	dup=$(cut -d' ' -f2 <<<"$seen" | sort | uniq -d)
	[ -z "$dup" ] || { echo "in two legs: $dup" >&2; exit 1; }
	for leg in "${!LEGS[@]}"; do grep -q "^$leg " <<<"$seen" || { echo "leg $leg names no package" >&2; exit 1; }; done
	printf '%s\n' "${!LEGS[@]}" | sort | jq -Rcs 'split("\n") | map(select(. != ""))'
	;;
test)
	leg=${2:-}
	if [ -n "$leg" ] && [ -z "${LEGS[$leg]:-}" ]; then echo "no leg $leg; the legs: ${!LEGS[*]}" >&2; exit 2; fi
	packages=$PACKAGES
	[ -z "$leg" ] || packages=${LEGS[$leg]}
	# The data on tmpfs: a test's database is created, migrated and dropped in 63 ms there, 1.5 s on a disk's syncs (T-61).
	# The benchmarks keep the disk, whose durability a baseline records.
	start_mariadb --tmpfs /var/lib/mysql:rw
	# Only MariaDB's runs of the two-engine loops: the ci job runs their SQLite ones.
	TRACEKEEP_MARIADB_DSN="root:$PASSWORD@tcp(127.0.0.1:$port)/catalog" TRACEKEEP_BENCH_ENGINES=mariadb GOWORK=off CGO_ENABLED=1 \
		go test $packages -race -count=1 -timeout 30m ${GOTESTFLAGS:-}
	;;
bench)
	out=${2:?usage: catalog.sh bench <file>}
	start_mariadb
	cpu=${CPU:-$(nproc)} bench=${BENCH:-.} packages=${BENCH_PACKAGES:-$PACKAGES}
	# Only MariaDB's rows: the engine loops skip SQLite's sub-benchmarks, which a local run measures without this script.
	export TRACEKEEP_MARIADB_DSN="root:$PASSWORD@tcp(127.0.0.1:$port)/catalog" TRACEKEEP_REQUIRE_MARIADB=1 TRACEKEEP_BENCH_ENGINES=mariadb GOWORK=off
	# The engines as a baseline records them (650-perf-sec.md), asked of each engine through the catalog, since the container is gone once the script ends;
	# a tree older than TestEngineFacts is answered by this script's own tree.
	facts() { TRACEKEEP_ENGINE_FACTS=1 go test ./internal/catalog/ -run '^TestEngineFacts$' -count 1 -v | grep -o 'engine facts: .*'; }
	facts || (cd "$(dirname "$0")/.." && echo "engine facts from the script's tree, the measured tree having no TestEngineFacts:" && facts)
	# run writes its pass's parameters as configuration lines before the samples, so benchjudge compares only rows run alike.
	# No race detector: it would measure itself. Go's cpu: line names the vendor's model, which a baseline never records.
	run() {
		local bench=$1 benchtime=$2
		printf 'benchtime: %s\ncount: %s\ncpus: %s\nscale: %s\n' "$benchtime" "${COUNT:-10}" "$cpu" "${TRACEKEEP_BENCH_SCALE:-small}"
		go test $packages -run '^$' -bench "$bench" -benchtime "$benchtime" -benchmem -cpu "$cpu" -count "${COUNT:-10}" -timeout 0 | sed '/^cpu:/d'
	}
	# BenchmarkRegisterPack skips unless -benchtime is Nx with N times a pack's ranges at most a tenth of the seeded ranges (its godoc):
	# its one-range cells (evidence, parked, miss, hit) run at 100x, which every size's 1,000 or more seeded ranges allow,
	# its ranges cells, 100 ranges a pack or more, at 1x, which the cells whose pack fits allow.
	# Only a run that selects registration appends them: bench.yaml's registration group, or a local run of everything.
	{
		run "$bench" "${BENCHTIME:-1s}"
		case "$bench" in
		. | *RegisterPack*)
			run '^BenchmarkRegisterPack$/^(evidence|parked|miss|hit)$' 100x
			run '^BenchmarkRegisterPack$/^ranges$' 1x
			;;
		esac
	} | tee "$out"
	;;
bench-smoke)
	start_mariadb
	TRACEKEEP_MARIADB_DSN="root:$PASSWORD@tcp(127.0.0.1:$port)/catalog" TRACEKEEP_REQUIRE_MARIADB=1 TRACEKEEP_BENCH_ENGINES=mariadb TRACEKEEP_BENCH_SCALE=small GOWORK=off \
		go test $PACKAGES -run '^$' -bench . -benchtime 1x | sed '/^cpu:/d'
	;;
*)
	echo "usage: catalog.sh diff <name> | check | legs | test [leg] | bench <file> | bench-smoke" >&2
	exit 2
	;;
esac
