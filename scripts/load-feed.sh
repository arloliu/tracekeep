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
#   BUS_LOGS              a directory the servers' logs are copied into when the run ends, none kept when unset
#   BUS_BLOCK_RATE        the servers' prof_block_rate, Go's block profile rate in nanoseconds, off when unset
#   MEM_BUDGET            GiB: the servers' and the test's resident memory in all past which the test is stopped (24)
#   RUN_FOR               seconds after which the test is stopped, a probe of the run's start; the whole run when unset
#   MEM_LOG               a CSV file of the servers', the test's and their total resident memory every 10 s, none when unset;
#                         their peaks are printed at the end either way
#   BUS_FEED_STREAMS      n: the servers map tklog.<equipment> to tklog.<partition of n>.<equipment>, for a stream a group;
#                         the feed-convert modes' 32 groups when unset, no mapping for the feed-streams mode's one stream
# Server i, tb-i and tagged so, serves clients on 431i, monitoring on 822i and Go's pprof on 865i (http://127.0.0.1:865i/debug/pprof/).
#   GOMEMLIMIT, TRACEKEEP_LOAD_ENGINE, TRACEKEEP_MARIADB_DSN, TRACEKEEP_LOAD_PROFILES, TRACEKEEP_LOAD_LOG are the test's, passed through
set -euo pipefail

mode=${1:-feed-convert}
case $mode in feed-convert*) BUS_FEED_STREAMS=${BUS_FEED_STREAMS:-32} ;; esac
work=$(mktemp -d)
pids=()
bus=
tpid=
# stop_test stops the test's process group, go test and its test binary, killing what is left after 30 s;
# the test itself, when stopped before setsid made its group.
stop_test() {
	kill -TERM -- "-$tpid" 2>/dev/null || kill -TERM "$tpid" 2>/dev/null || return 0
	for _ in $(seq 30); do
		kill -0 -- "-$tpid" 2>/dev/null || kill -0 "$tpid" 2>/dev/null || return 0
		sleep 1
	done
	kill -KILL -- "-$tpid" 2>/dev/null || kill -KILL "$tpid" 2>/dev/null || true
}
cleanup() {
	[ -z "$tpid" ] || stop_test
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	wait 2>/dev/null || true
	[ -z "${BUS_LOGS:-}" ] || { mkdir -p "$BUS_LOGS" && cp "$work"/server-*.log "$BUS_LOGS"/; } || true
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
for i in 0 1 2; do
	mkdir -p "$bus/tb-$i"
	# A configuration file only for what has no flag.
	{
		echo "prof_block_rate: ${BUS_BLOCK_RATE:-0}"
		echo "server_tags: [\"tb-$i\"]"
		[ -z "${BUS_FEED_STREAMS:-}" ] || echo "mappings { \"tklog.*\": \"tklog.{{partition($BUS_FEED_STREAMS,1)}}.{{wildcard(1)}}\" }"
	} >"$work/tb-$i.conf"
	GOMEMLIMIT=${BUS_GOMEMLIMIT:-7GiB} "${bus_pin[@]}" "$work/nats-server" -c "$work/tb-$i.conf" -n "tb-$i" -a 127.0.0.1 -p "431$i" -m "822$i" --profile "865$i" \
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
	"${test_pin[@]}" setsid go test ./internal/service/ -run '^TestLoad$' -count=1 -v -timeout 150m &
# setsid gives the test a process group of its own, its id the test's pid, which stop_test stops whole.
tpid=$!

# rss is a process's and its children's resident memory in KiB: go test's own and its test binary's.
rss() {
	local total=0 p kb
	for p in "$1" $(pgrep -P "$1" || true); do
		kb=$(awk '/^VmRSS/ {print $2}' "/proc/$p/status" 2>/dev/null || true)
		total=$((total + ${kb:-0}))
	done
	echo "$total"
}
budget=$((${MEM_BUDGET:-24} * 1024 * 1024))
start=$SECONDS
peaks=(0 0 0 0 0)
stopped=
[ -z "${MEM_LOG:-}" ] || echo "seconds,tb-0,tb-1,tb-2,test,total" >"$MEM_LOG"
while kill -0 "$tpid" 2>/dev/null; do
	row=()
	total=0
	for p in "${pids[@]}" "$tpid"; do
		r=$(rss "$p")
		row+=("$r")
		total=$((total + r))
	done
	row+=("$total")
	for i in "${!row[@]}"; do [ "${row[$i]}" -le "${peaks[$i]}" ] || peaks[i]=${row[$i]}; done
	[ -z "${MEM_LOG:-}" ] || (IFS=,; echo "$((SECONDS - start)),${row[*]}") >>"$MEM_LOG"
	if [ "$total" -gt "$budget" ]; then
		stopped="resident memory $((total / 1024)) MiB past the budget of ${MEM_BUDGET:-24} GiB"
	elif [ -n "${RUN_FOR:-}" ] && [ $((SECONDS - start)) -ge "$RUN_FOR" ]; then
		stopped="RUN_FOR ${RUN_FOR} s reached"
	fi
	if [ -n "$stopped" ]; then
		echo "load-feed: stopping the test: $stopped" >&2
		stop_test
		break
	fi
	sleep 10
done
status=0
wait "$tpid" || status=$?
tpid=
echo "load-feed: peak resident memory, MiB: tb-0 $((peaks[0] / 1024)), tb-1 $((peaks[1] / 1024)), tb-2 $((peaks[2] / 1024)), test $((peaks[3] / 1024)), total $((peaks[4] / 1024))"
[ -z "$stopped" ] || exit 1
exit "$status"
