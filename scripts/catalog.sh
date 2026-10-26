#!/usr/bin/env bash
# The catalog's migrations and its MariaDB tests, each needing a MariaDB server in a container.
# Usage:
#   scripts/catalog.sh diff <name>   generate a migration for both engines from schema.hcl
#   scripts/catalog.sh check         validate both migration directories and check schema.hcl for drift
#   scripts/catalog.sh test          run the catalog's tests on MariaDB
set -euo pipefail

MARIADB_IMAGE=mariadb:11.8
PASSWORD=tracekeep

container=
cleanup() { [ -z "$container" ] || docker rm -f "$container" >/dev/null; }
trap cleanup EXIT

# start_mariadb starts a server with the databases dev (Atlas's scratch database) and catalog (the tests'), and sets port.
start_mariadb() {
	# Byte-exact strings, as on SQLite: the generated tables carry the server's default collation.
	container=$(docker run -d -p 127.0.0.1::3306 -e MARIADB_ROOT_PASSWORD=$PASSWORD -e MARIADB_DATABASE=dev "$MARIADB_IMAGE" \
		--character-set-server=utf8mb4 --collation-server=utf8mb4_nopad_bin)
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
test)
	start_mariadb
	TRACEKEEP_MARIADB_DSN="root:$PASSWORD@tcp(127.0.0.1:$port)/catalog" GOWORK=off CGO_ENABLED=1 \
		go test ./migrations/ ./internal/catalog/... -race -count=1 ${GOTESTFLAGS:-}
	;;
*)
	echo "usage: catalog.sh diff <name> | check | test" >&2
	exit 2
	;;
esac
