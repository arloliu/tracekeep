#!/usr/bin/env bash
# unit-check: the systemd unit verified by systemd-analyze, on a copy naming a binary that exists here
# (verify refuses a unit whose commands are missing), and the sysusers line checked for the user the unit runs as.
set -euo pipefail
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
unit=deploy/systemd/tracekeep.service
sed "s|/usr/local/bin/tracekeep|$(command -v true)|g" "$unit" > "$out/tracekeep.service"
# User= names an account this host may lack; verify reports a missing one, which the sysusers file below creates.
systemd-analyze verify --man=no "$out/tracekeep.service" > "$out/verify" 2>&1 || true
grep -v "User.*not found\|Failed to resolve user" "$out/verify" > "$out/issues" || true
if [ -s "$out/issues" ]; then
	cat "$out/issues" >&2
	echo "unit-check: systemd-analyze verify reported the above" >&2
	exit 1
fi

user=$(sed -n 's/^User=//p' "$unit")
grep -qE "^u $user " deploy/systemd/tracekeep.sysusers || { echo "unit-check: tracekeep.sysusers does not create $user" >&2; exit 1; }
grep -q "^ExecStartPre=/usr/local/bin/tracekeep migrate " "$unit" || { echo "unit-check: the unit does not migrate before its start" >&2; exit 1; }
grep -q "^StartLimitIntervalSec=0$" "$unit" || { echo "unit-check: the unit's restarts have a start limit" >&2; exit 1; }
echo "unit-check: ok"
