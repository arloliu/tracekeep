#!/usr/bin/env bash
# Runs the keys module's tests against a dev-mode Vault server in a container, over TLS (masking-keys.md §9).
# Usage: scripts/test-vault.sh
set -euo pipefail

VAULT_IMAGE=hashicorp/vault:1.20.4
ROOT=tracekeep-root

container=
dir=$(mktemp -d)
cleanup() {
	[ -z "$container" ] || docker rm -f "$container" >/dev/null
	rm -rf "$dir"
}
trap cleanup EXIT

# Dev mode with TLS writes its CA beside the certificate, in a directory the image does not create.
container=$(docker run -d --cap-add=IPC_LOCK -p 127.0.0.1::8200 --entrypoint sh "$VAULT_IMAGE" \
	-c "mkdir -p /tmp/tls && exec vault server -dev -dev-tls -dev-listen-address=0.0.0.0:8200 -dev-root-token-id=$ROOT -dev-tls-cert-dir=/tmp/tls -dev-tls-san=127.0.0.1")
port=$(docker port "$container" 8200 | head -1 | cut -d: -f2)
for _ in $(seq 60); do
	if docker cp "$container:/tmp/tls/vault-ca.pem" "$dir/ca.pem" >/dev/null 2>&1 &&
		curl -sf --cacert "$dir/ca.pem" "https://127.0.0.1:$port/v1/sys/health" >/dev/null; then
		break
	fi
	sleep 1
done
curl -sf --cacert "$dir/ca.pem" "https://127.0.0.1:$port/v1/sys/health" >/dev/null || {
	echo "Vault not ready" >&2
	docker logs --tail 50 "$container" >&2
	exit 1
}

TRACEKEEP_VAULT_ADDR="https://127.0.0.1:$port" TRACEKEEP_VAULT_CA_FILE="$dir/ca.pem" TRACEKEEP_VAULT_TOKEN=$ROOT \
	GOWORK=off CGO_ENABLED=1 go test ./internal/keys/ -run 'DevVault' -race -count=1 ${GOTESTFLAGS:-}
