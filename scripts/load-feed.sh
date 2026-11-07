#!/usr/bin/env bash
# The host log feed's gate load (docs/plans/host-log-feed.md step 10b, T-66) on a bus of its own:
# three nats-server processes, built from the module's own nats-server version, outside the test's process,
# so the bus keeps its own heap, cores and disk, and the test, dev and the catalog keep theirs.
# Usage:
#   scripts/load-feed.sh [mode]   run TRACEKEEP_LOAD=<mode>, feed-convert by default (feed-convert-smoke, feed-convert-idle, ...)
# Environment:
#   BUS_CPUS, TEST_CPUS   taskset CPU lists for the servers and for the test, unpinned when unset;
#                         on a two-CCD host, one CCD each, so neither shares a cache or a core with the other
#   BUS_DIR               where the servers keep their stores, a disk other than the test's temporary directory's at best;
#                         a new temporary directory when unset, removed afterwards
#   BUS_GOMEMLIMIT        each server's GOMEMLIMIT (7GiB)
#   GOMEMLIMIT, TRACEKEEP_LOAD_ENGINE, TRACEKEEP_MARIADB_DSN, TRACEKEEP_LOAD_PROFILES are the test's, passed through
set -euo pipefail

mode=${1:-feed-convert}
work=$(mktemp -d)
bus=${BUS_DIR:-$work/bus}
pids=()
cleanup() {
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	wait 2>/dev/null || true
	rm -rf -- "$work"
	[ -z "${BUS_DIR:-}" ] || rm -rf -- "$BUS_DIR"/tb-0 "$BUS_DIR"/tb-1 "$BUS_DIR"/tb-2
}
trap cleanup EXIT

pin() { # pin <cpus> <command...>: run the command on cpus, or unpinned without them
	local cpus=$1
	shift
	if [ -n "$cpus" ]; then taskset -c "$cpus" "$@"; else "$@"; fi
}

go build -o "$work/nats-server" github.com/nats-io/nats-server/v2
routes=nats-route://127.0.0.1:6310,nats-route://127.0.0.1:6311,nats-route://127.0.0.1:6312
for i in 0 1 2; do
	mkdir -p "$bus/tb-$i"
	GOMEMLIMIT=${BUS_GOMEMLIMIT:-7GiB} pin "${BUS_CPUS:-}" "$work/nats-server" -n "tb-$i" -a 127.0.0.1 -p "431$i" -m "822$i" \
		-js -sd "$bus/tb-$i" --cluster_name testbus --cluster "nats://127.0.0.1:631$i" --routes "$routes" >"$work/server-$i.log" 2>&1 &
	pids+=($!)
done
# Ready once every server reports JetStream healthy: the meta leader elected and the server current with it.
for _ in $(seq 60); do
	ready=0
	for i in 0 1 2; do curl -sf --max-time 2 "http://127.0.0.1:822$i/healthz" >/dev/null && ready=$((ready + 1)); done
	[ "$ready" -eq 3 ] && break
	sleep 1
done
if [ "$ready" -ne 3 ]; then
	echo "load-feed: $ready of 3 servers healthy" >&2
	exit 1
fi

TRACEKEEP_LOAD_BUS=nats://127.0.0.1:4310,nats://127.0.0.1:4311,nats://127.0.0.1:4312 TRACEKEEP_LOAD="$mode" \
	pin "${TEST_CPUS:-}" go test ./internal/service/ -run '^TestLoad$' -count=1 -v -timeout 150m
