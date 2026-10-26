#!/usr/bin/env bash
# Runs the s3 store's tests against each S3-compatible store deployments use (T-34), one container at a time.
# Usage: scripts/test-s3.sh [minio|ceph]...   (default: both)
set -euo pipefail

# MinIO's own images are no longer published; this is the last open-source server release, as Bitnami built it.
MINIO_IMAGE=bitnamilegacy/minio:2025.7.23-debian-12-r5
CEPH_IMAGE=quay.io/ceph/demo:main-30dc8b9a-squid-centos-stream9
KEY=tracekeep
SECRET=tracekeep-secret

container=
cleanup() { [ -z "$container" ] || docker rm -f "$container" >/dev/null; }
trap cleanup EXIT

# wait_for runs its command once a second until it succeeds, for at most $1 seconds.
wait_for() {
	local limit=$1
	shift
	for _ in $(seq "$limit"); do
		"$@" >/dev/null 2>&1 && return 0
		sleep 1
	done
	echo "not ready after ${limit}s: $*" >&2
	docker logs --tail 50 "$container" >&2
	return 1
}

start_minio() {
	container=$(docker run -d -p 127.0.0.1::9000 -e MINIO_ROOT_USER=$KEY -e MINIO_ROOT_PASSWORD=$SECRET "$MINIO_IMAGE")
	port=$(docker port "$container" 9000 | head -1 | cut -d: -f2)
	wait_for 60 curl -sf "http://127.0.0.1:$port/minio/health/live"
}

# The demo cluster binds its monitor to the container's own address, which is known only inside it.
start_ceph() {
	container=$(docker run -d -p 127.0.0.1::8080 \
		-e DEMO_DAEMONS=mon,mgr,osd,rgw -e CEPH_DEMO_UID=$KEY -e CEPH_DEMO_ACCESS_KEY=$KEY -e CEPH_DEMO_SECRET_KEY=$SECRET \
		--entrypoint bash "$CEPH_IMAGE" \
		-c 'export MON_IP=$(hostname -i) && export CEPH_PUBLIC_NETWORK=${MON_IP%.*}.0/24 && exec /opt/ceph-container/bin/demo')
	port=$(docker port "$container" 8080 | head -1 | cut -d: -f2)
	wait_for 300 docker exec "$container" radosgw-admin user info --uid=$KEY
	wait_for 120 curl -sf "http://127.0.0.1:$port"
}

for store in "${@:-minio ceph}"; do
	for s in $store; do
		echo "== $s"
		"start_$s"
		TRACEKEEP_S3_ENDPOINT="http://127.0.0.1:$port" TRACEKEEP_S3_ACCESS_KEY=$KEY TRACEKEEP_S3_SECRET_KEY=$SECRET \
			GOWORK=off CGO_ENABLED=1 go test ./internal/store/s3store/ -race -count=1 ${GOTESTFLAGS:-}
		cleanup
		container=
	done
done
