#!/usr/bin/env bash
# The standalone leg of the packaging tests (architecture.md §3.3 *Packaging tests*): the unit installed on this host,
# NATS and Dex in containers, SQLite and an fs store, the simulator's traffic under an active masking configuration,
# and the browser driving a troubleshooting session; then, when a baseline exists, the baseline upgraded in the unit's order.
# It installs a system unit, so it runs only on a GitHub-hosted runner, discarded after the job, and never over an installation.
# Usage: scripts/packaging/standalone.sh   (make packaging-standalone; TRACEKEEP_BASELINE=<sha> as cluster.sh's)
set -euo pipefail
cd "$(dirname "$0")/../.."
. scripts/packaging/lib.sh

if [ "${GITHUB_ACTIONS:-}" != true ] || [ "${RUNNER_ENVIRONMENT:-}" != github-hosted ]; then
	die "refused: the standalone leg installs a system unit and runs only on a GitHub-hosted runner"
fi
for p in /etc/systemd/system/tracekeep.service /etc/tracekeep /var/lib/tracekeep /usr/local/bin/tracekeep; do
	[ ! -e "$p" ] || die "refused: $p exists, an installation this leg would overwrite"
done
! getent passwd tracekeep >/dev/null || die "refused: the user tracekeep exists, an installation this leg would overwrite"

WORK=$PWD/tmp/packaging/standalone
PUBLIC=https://tracekeep.test:8443
ISSUER=https://dex.test:5556
NATS_IMAGE=nats:2.12-alpine
DEX_IMAGE=ghcr.io/dexidp/dex:v2.44.0
ETC=/etc/tracekeep

pids=()
cleanup() {
	local rc=$?
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	if [ $rc -ne 0 ]; then
		sudo systemctl status tracekeep --no-pager >&2 || true
		sudo journalctl -u tracekeep --no-pager -n 80 >&2 || true
	fi
	exit $rc
}
trap cleanup EXIT
rm -rf "$WORK" && mkdir -p "$WORK"

ca_init "$WORK/ca"
ca_leaf "$WORK/ca" tracekeep tracekeep.test
ca_leaf "$WORK/ca" dex dex.test
# The service resolves the issuer, and the operator's commands the API, by the names the browser uses.
echo "127.0.0.1 tracekeep.test dex.test" | sudo tee -a /etc/hosts >/dev/null
sudo cp "$WORK/ca/ca.crt" /usr/local/share/ca-certificates/tracekeep-packaging.crt && sudo update-ca-certificates >/dev/null

dex_config "$ISSUER" "$PUBLIC/auth/callback" > "$WORK/dex.yaml"
docker run -d --name tracekeep-dex -p 127.0.0.1:5556:5556 \
	-v "$WORK/dex.yaml:/etc/dex/config/config.yaml:ro" -v "$WORK/ca/dex.crt:/etc/dex/tls/tls.crt:ro" -v "$WORK/ca/dex.key:/etc/dex/tls/tls.key:ro" \
	"$DEX_IMAGE" dex serve /etc/dex/config/config.yaml >/dev/null
docker run -d --name tracekeep-nats -p 127.0.0.1:4222:4222 "$NATS_IMAGE" -js >/dev/null

# install bin: the binary, the unit, its user, its configuration, credentials (0640 root:tracekeep) and keys (0600 tracekeep).
install_unit() {
	sudo install -m 0755 "$1" /usr/local/bin/tracekeep
	[ -e /etc/systemd/system/tracekeep.service ] && return 0
	sudo install -m 0644 deploy/systemd/tracekeep.sysusers /usr/lib/sysusers.d/tracekeep.conf
	sudo systemd-sysusers
	sudo install -m 0644 deploy/systemd/tracekeep.service /etc/systemd/system/tracekeep.service
	sudo install -d -m 0750 -o root -g tracekeep $ETC $ETC/secrets
	sudo install -d -m 0700 -o tracekeep -g tracekeep $ETC/keys
	sudo install -m 0640 -o root -g tracekeep "$WORK/ca/tracekeep.crt" $ETC/secrets/tls.crt
	sudo install -m 0640 -o root -g tracekeep "$WORK/ca/tracekeep.key" $ETC/secrets/tls.key
	sudo install -m 0640 -o root -g tracekeep "$WORK/ca/ca.crt" $ETC/secrets/ca.crt
	printf '%s' "$PEOPLE_SECRET" | sudo install -m 0640 -o root -g tracekeep /dev/stdin $ETC/secrets/people-secret
	head -c 32 /dev/urandom | base64 -w0 | sudo install -m 0640 -o root -g tracekeep /dev/stdin $ETC/secrets/admin-token
	for id in session-1 download-1 digest-1 master-1; do
		material | sudo install -m 0600 -o tracekeep -g tracekeep /dev/stdin "$ETC/keys/$id"
	done
	sudo tee $ETC/config.yaml >/dev/null <<YAML
catalog: {engine: sqlite, dsn: /var/lib/tracekeep/catalog.db}
bus: {url: "nats://127.0.0.1:4222", subject_prefix: tpk, claim_bucket: tpk-objects}
store: {kind: fs, dir: /var/lib/tracekeep/packs, presign: false}
ingest: {flush_interval: 15s, ack_wait: 2m}
compact: {interval: 1m}
metrics: {listen: "127.0.0.1:9090"}
api:
  listen: ":8443"
  tls_cert_file: $ETC/secrets/tls.crt
  tls_key_file: $ETC/secrets/tls.key
  public_url: $PUBLIC
auth:
  oidc: {issuer: $ISSUER, audience: tracekeep-api, ca_file: $ETC/secrets/ca.crt}
  people:
    client_id: $PEOPLE_CLIENT
    client_secret_ref: "file://$ETC/secrets/people-secret"
    scopes: [groups]
    roles_claim: groups
    roles: {$MAPPED_GROUP: [viewer, exporter]}
  # The operator's commands reach the SQLite catalog the running service holds through its API (api.md §10.1 Admin over the API).
  static:
    enabled: true
    tokens: [{subject: operator, token_ref: "file://$ETC/secrets/admin-token", roles: [admin]}]
keys:
  declared:
    session-1: {provider: file, ref: $ETC/keys/session-1}
    download-1: {provider: file, ref: $ETC/keys/download-1}
    digest-1: {provider: file, ref: $ETC/keys/digest-1, kind: digest}
    master-1: {provider: file, ref: $ETC/keys/master-1, kind: master}
  session: {issuing: session-1, accepted: [session-1]}
  download: {issuing: download-1, accepted: [download-1]}
YAML
	sudo chown root:tracekeep $ETC/config.yaml && sudo chmod 0640 $ETC/config.yaml
	sudo install -d -m 0750 -o tracekeep -g tracekeep /var/lib/tracekeep
	sudo systemctl daemon-reload
}

tk() { sudo -u tracekeep /usr/local/bin/tracekeep "$@" --config $ETC/config.yaml; }

# fresh bin: installed, migrated and its masking configuration active with the unit stopped, then started.
fresh() {
	install_unit "$1"
	tk migrate
	local id
	id=$(sudo -u tracekeep /usr/local/bin/tracekeep admin --config $ETC/config.yaml --actor packaging config stage digest-1 master-1 |
		python3 -c 'import json,sys; print(json.load(sys.stdin)["ConfigID"])')
	sudo -u tracekeep /usr/local/bin/tracekeep admin --config $ETC/config.yaml --actor packaging config activate "$id" >/dev/null
	start
}

start() {
	sudo systemctl start tracekeep
	for _ in $(seq 120); do curl -fs http://127.0.0.1:9090/readyz >/dev/null && return 0; sleep 2; done
	die "tracekeep did not become ready"
}

# traffic: the simulator until the tool's capture is admitted (tracekeep status, over the API while the unit holds the catalog).
traffic() {
	[ -x "$WORK/tracekeep-sim" ] || go build -o "$WORK/tracekeep-sim" ./cmd/tracekeep-sim
	"$WORK/tracekeep-sim" -servers nats://127.0.0.1:4222 -tool $TOOL -device $DEVICE -rate 10 > "$WORK/sim.log" 2>&1 &
	pids+=($!)
	for _ in $(seq 90); do
		if tk status 2>/dev/null | python3 -c '
import json,sys
s=json.load(sys.stdin)
ok=[t for t in s.get("tools") or [] if t["tool_id"]==sys.argv[1] and t["open_captures"]>0 and t["deferred_messages"]==0 and t.get("last_record")]
sys.exit(0 if ok and not s.get("pending") else 1)' $TOOL; then
			sleep 30
			return 0
		fi
		sleep 5
	done
	tk status >&2 || true
	die "the simulator's capture was not admitted"
}

stop_traffic() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; pids=(); }

log "fresh install of $(git rev-parse --short HEAD)"
make build >/dev/null
cp dist/tracekeep "$WORK/tracekeep-candidate"
fresh "$WORK/tracekeep-candidate"
traffic
playwright "$PUBLIC"
stop_traffic

if [ -z "${TRACEKEEP_BASELINE:-}" ]; then
	log "upgrade skipped: no baseline (no strict ancestor on main with the chart whose packaging run passed)"
	exit 0
fi
log "upgrade from $TRACEKEEP_BASELINE to $(git rev-parse --short HEAD)"
sudo systemctl stop tracekeep
sudo rm -rf /var/lib/tracekeep
sudo install -d -m 0750 -o tracekeep -g tracekeep /var/lib/tracekeep
# A fresh bus: the baseline's install starts from nothing.
docker rm -f tracekeep-nats >/dev/null
docker run -d --name tracekeep-nats -p 127.0.0.1:4222:4222 "$NATS_IMAGE" -js >/dev/null
git worktree prune
git worktree add -f --detach "$WORK/src-baseline" "$TRACEKEEP_BASELINE" >/dev/null
(cd "$WORK/src-baseline" && make build >/dev/null)
fresh "$WORK/src-baseline/dist/tracekeep"
traffic
playwright "$PUBLIC"
# The unit's order: stop, replace the binary, start; the migration before the start upgrades the catalog.
sudo systemctl stop tracekeep
install_unit "$WORK/tracekeep-candidate"
start
playwright "$PUBLIC"
stop_traffic
log "upgrade from $TRACEKEEP_BASELINE passed"
