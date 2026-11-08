#!/usr/bin/env bash
# The host log feed's gate load (the host log feed plan's step 10b, last text at 3c11e5e, T-66) on a bus of its own:
# three nats-server processes, built from the module's own nats-server version, outside the test's process,
# so the bus keeps its own heap, cores and disk, and the test, dev and the catalog keep theirs.
# Usage:
#   scripts/load-feed.sh [mode]   run TRACEKEEP_LOAD=<mode>, feed-convert by default (feed-convert-smoke, feed-convert-idle, ...)
# Environment:
#   BUS_CPUS, TEST_CPUS   taskset CPU lists for the servers and for the test, unpinned when unset;
#                         on a two-CCD host, one CCD each, so neither shares a cache or a core with the other
#   BUS_DIR               the directory under which the servers keep their stores, in a new directory of their own removed afterwards,
#                         a disk other than the test's temporary directory's at best; the system's temporary directory when unset
#   BUS_GOMEMLIMIT        each server's GOMEMLIMIT (7GiB)
#   BUS_BLOCK_RATE        the servers' prof_block_rate, Go's block profile rate in nanoseconds, off when unset
# Server i serves clients on 431i, monitoring on 822i and Go's pprof on 865i (http://127.0.0.1:865i/debug/pprof/).
#   GOMEMLIMIT, TRACEKEEP_LOAD_ENGINE, TRACEKEEP_MARIADB_DSN, TRACEKEEP_LOAD_PROFILES are the test's, passed through
set -euo pipefail

mode=${1:-feed-convert}
work=$(mktemp -d)
pids=()
bus=
cleanup() {
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	wait 2>/dev/null || true
	rm -rf -- "$work"
	# Only the stores' directory this run made: never what BUS_DIR held before.
	[ -z "$bus" ] || rm -rf -- "$bus"
}
trap cleanup EXIT
bus=$(mktemp -d "${BUS_DIR:-${TMPDIR:-/tmp}}/load-feed-bus.XXXXXX")

# The commands' CPU pinning, empty when unpinned: a prefix, not a function, so a server started in the background is its own process, which cleanup stops.
bus_pin=()
test_pin=()
[ -z "${BUS_CPUS:-}" ] || bus_pin=(taskset -c "$BUS_CPUS")
[ -z "${TEST_CPUS:-}" ] || test_pin=(taskset -c "$TEST_CPUS")

go build -o "$work/nats-server" github.com/nats-io/nats-server/v2
routes=nats-route://127.0.0.1:6310,nats-route://127.0.0.1:6311,nats-route://127.0.0.1:6312
# A configuration file only for what has no flag.
echo "prof_block_rate: ${BUS_BLOCK_RATE:-0}" >"$work/server.conf"
for i in 0 1 2; do
	mkdir -p "$bus/tb-$i"
	GOMEMLIMIT=${BUS_GOMEMLIMIT:-7GiB} "${bus_pin[@]}" "$work/nats-server" -c "$work/server.conf" -n "tb-$i" -a 127.0.0.1 -p "431$i" -m "822$i" --profile "865$i" \
		-js -sd "$bus/tb-$i" --cluster_name testbus --cluster "nats://127.0.0.1:631$i" --routes "$routes" >"$work/server-$i.log" 2>&1 &
	pids+=($!)
done
# Ready once every server started here is running and reports JetStream healthy: the meta leader elected and the server current with it.
for _ in $(seq 60); do
	ready=0
	for i in 0 1 2; do
		kill -0 "${pids[$i]}" 2>/dev/null || { echo "load-feed: server $i exited:" >&2; cat "$work/server-$i.log" >&2; exit 1; }
		curl -sf --max-time 2 "http://127.0.0.1:822$i/healthz" >/dev/null && ready=$((ready + 1))
	done
	[ "$ready" -eq 3 ] && break
	sleep 1
done
if [ "$ready" -ne 3 ]; then
	echo "load-feed: $ready of 3 servers healthy" >&2
	exit 1
fi

TRACEKEEP_LOAD_BUS=nats://127.0.0.1:4310,nats://127.0.0.1:4311,nats://127.0.0.1:4312 TRACEKEEP_LOAD="$mode" \
	"${test_pin[@]}" go test ./internal/service/ -run '^TestLoad$' -count=1 -v -timeout 150m
