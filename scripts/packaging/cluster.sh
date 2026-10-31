#!/usr/bin/env bash
# The cluster leg of the packaging tests (architecture.md §3.3 *Packaging tests*):
# kind with NATS, MariaDB, MinIO, Vault and Dex, the image built and loaded, the chart installed with two api replicas,
# the simulator's traffic under an active masking configuration, and the browser driving a troubleshooting session.
# A fresh install of the commit under test; then, when a baseline exists, the baseline installed and upgraded to it.
# Usage: scripts/packaging/cluster.sh   (needs docker, kind, kubectl, helm, go, node; make packaging-cluster)
#   TRACEKEEP_BASELINE=<sha>   the upgrade's baseline (scripts/packaging/baseline.sh finds it in CI); none skips the upgrade
#   TRACEKEEP_KEEP=1           keeps the cluster after the run
set -euo pipefail
cd "$(dirname "$0")/../.."
. scripts/packaging/lib.sh

CLUSTER=tracekeep-packaging
NS=tk
RELEASE=tk
FULL=$RELEASE-tracekeep
WORK=$PWD/tmp/packaging/cluster
NATS_IMAGE=nats:2.12-alpine
MARIADB_IMAGE=mariadb:11.8
MINIO_IMAGE=bitnamilegacy/minio:2025.7.23-debian-12-r5
VAULT_IMAGE=hashicorp/vault:1.20.4
DEX_IMAGE=ghcr.io/dexidp/dex:v2.44.0
PUBLIC=https://tracekeep.test:8443
ISSUER=https://dex.test:5556

pids=()
cleanup() {
	local rc=$?
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	if [ $rc -ne 0 ]; then
		log "failed; the cluster's state:"
		kubectl -n $NS get pods -o wide 2>/dev/null || true
		for d in api ingest compact; do kubectl -n $NS logs deploy/$FULL-$d --tail=40 2>/dev/null || true; done
	fi
	[ -n "${TRACEKEEP_KEEP:-}" ] || kind delete cluster --name $CLUSTER >/dev/null 2>&1 || true
	[ ! -d "$WORK/src-baseline" ] || git worktree remove --force "$WORK/src-baseline" >/dev/null 2>&1 || true
	exit $rc
}
trap cleanup EXIT

rm -rf "$WORK" && mkdir -p "$WORK"

# The CA and the endpoints' certificates: the API and Dex under the host names the browser and the pods share, Vault in the cluster.
ca_init "$WORK/ca"
ca_leaf "$WORK/ca" tracekeep tracekeep.test
ca_leaf "$WORK/ca" dex dex.test
ca_leaf "$WORK/ca" vault vault.$NS.svc vault.$NS.svc.cluster.local

# kind, the host's ports mapped to the NodePorts: the API (8443), Dex (5556) and the bus for the simulator (4222).
kind delete cluster --name $CLUSTER >/dev/null 2>&1 || true
kind create cluster --name $CLUSTER --wait 120s --config - <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - {containerPort: 30443, hostPort: 8443, listenAddress: 127.0.0.1}
  - {containerPort: 30556, hostPort: 5556, listenAddress: 127.0.0.1}
  - {containerPort: 30422, hostPort: 4222, listenAddress: 127.0.0.1}
YAML

# dex.test inside the cluster is Dex's Service, as on the host, so the issuer is one URL from both sides.
kubectl -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}' |
	sed "s|^\(\s*\)ready$|\1ready\n\1rewrite name dex.test dex.$NS.svc.cluster.local|" > "$WORK/Corefile"
kubectl -n kube-system create configmap coredns --from-file=Corefile="$WORK/Corefile" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n kube-system rollout restart deploy/coredns >/dev/null

# build_image ref tag: the image of the commit ref, loaded into kind as tracekeep:tag.
build_image() {
	local ref=$1 tag=$2 src=$PWD
	if [ "$ref" != HEAD ]; then
		src=$WORK/src-$tag
		git worktree prune
		git worktree add -f --detach "$src" "$ref" >/dev/null
	fi
	(cd "$src" && docker buildx build -f deploy/image/Dockerfile --build-arg GO_VERSION="$(awk -F'"' '/^go *=/ {print $2}' mise.toml)" \
		--load -t "tracekeep:$tag" . >/dev/null)
	kind load docker-image --name $CLUSTER "tracekeep:$tag" >/dev/null
}

# dependencies: NATS, MariaDB, MinIO, Vault and Dex in the namespace, with tracekeep's Secrets.
dependencies() {
	kubectl create namespace $NS
	kubectl -n $NS create secret tls dex-tls --cert="$WORK/ca/dex.crt" --key="$WORK/ca/dex.key"
	kubectl -n $NS create secret tls vault-tls --cert="$WORK/ca/vault.crt" --key="$WORK/ca/vault.key"
	dex_config "$ISSUER" "$PUBLIC/auth/callback" > "$WORK/dex.yaml"
	kubectl -n $NS create secret generic dex-config --from-file=config.yaml="$WORK/dex.yaml"
	kubectl -n $NS apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: nats}
spec:
  selector: {matchLabels: {app: nats}}
  template:
    metadata: {labels: {app: nats}}
    spec:
      containers:
      - {name: nats, image: $NATS_IMAGE, args: ["-js", "-sd", "/data"], ports: [{containerPort: 4222}]}
---
apiVersion: v1
kind: Service
metadata: {name: nats}
spec:
  type: NodePort
  selector: {app: nats}
  ports: [{port: 4222, targetPort: 4222, nodePort: 30422}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: mariadb}
spec:
  selector: {matchLabels: {app: mariadb}}
  template:
    metadata: {labels: {app: mariadb}}
    spec:
      containers:
      - name: mariadb
        image: $MARIADB_IMAGE
        args: ["--character-set-server=utf8mb4", "--collation-server=utf8mb4_nopad_bin"]
        env:
        - {name: MARIADB_ROOT_PASSWORD, value: root}
        - {name: MARIADB_DATABASE, value: tracekeep}
        - {name: MARIADB_USER, value: tracekeep}
        - {name: MARIADB_PASSWORD, value: tracekeep}
        readinessProbe: {exec: {command: [healthcheck.sh, --connect, --innodb_initialized]}, periodSeconds: 3}
---
apiVersion: v1
kind: Service
metadata: {name: mariadb}
spec: {selector: {app: mariadb}, ports: [{port: 3306}]}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: minio}
spec:
  selector: {matchLabels: {app: minio}}
  template:
    metadata: {labels: {app: minio}}
    spec:
      containers:
      - name: minio
        image: $MINIO_IMAGE
        env:
        - {name: MINIO_ROOT_USER, value: tracekeep}
        - {name: MINIO_ROOT_PASSWORD, value: tracekeep-secret}
        - {name: MINIO_DEFAULT_BUCKETS, value: tracekeep}
        readinessProbe: {httpGet: {path: /minio/health/ready, port: 9000}, periodSeconds: 3}
---
apiVersion: v1
kind: Service
metadata: {name: minio}
spec: {selector: {app: minio}, ports: [{port: 9000}]}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: vault}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: vault-$NS-auth-delegator}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: system:auth-delegator}
subjects: [{kind: ServiceAccount, name: vault, namespace: $NS}]
---
apiVersion: v1
kind: ConfigMap
metadata: {name: vault-listener}
data:
  listener.hcl: |
    disable_mlock = true
    listener "tcp" {
      address       = "0.0.0.0:8200"
      tls_cert_file = "/vault/tls/tls.crt"
      tls_key_file  = "/vault/tls/tls.key"
    }
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: vault}
spec:
  selector: {matchLabels: {app: vault}}
  template:
    metadata: {labels: {app: vault}}
    spec:
      serviceAccountName: vault
      containers:
      - name: vault
        image: $VAULT_IMAGE
        command: [vault, server, -dev, -dev-root-token-id=root, -dev-listen-address=127.0.0.1:8201, -config=/vault/listener/listener.hcl]
        volumeMounts:
        - {name: tls, mountPath: /vault/tls}
        - {name: listener, mountPath: /vault/listener}
        readinessProbe: {tcpSocket: {port: 8200}, periodSeconds: 3}
      volumes:
      - {name: tls, secret: {secretName: vault-tls}}
      - {name: listener, configMap: {name: vault-listener}}
---
apiVersion: v1
kind: Service
metadata: {name: vault}
spec: {selector: {app: vault}, ports: [{port: 8200}]}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: dex}
spec:
  selector: {matchLabels: {app: dex}}
  template:
    metadata: {labels: {app: dex}}
    spec:
      containers:
      - name: dex
        image: $DEX_IMAGE
        args: [dex, serve, /etc/dex/config/config.yaml]
        volumeMounts:
        - {name: config, mountPath: /etc/dex/config}
        - {name: tls, mountPath: /etc/dex/tls}
        readinessProbe: {tcpSocket: {port: 5556}, periodSeconds: 3}
      volumes:
      - {name: config, secret: {secretName: dex-config}}
      - {name: tls, secret: {secretName: dex-tls}}
---
apiVersion: v1
kind: Service
metadata: {name: dex}
spec:
  type: NodePort
  selector: {app: dex}
  ports: [{port: 5556, targetPort: 5556, nodePort: 30556}]
YAML
	for d in nats mariadb minio vault dex; do kubectl -n $NS rollout status deploy/$d --timeout=300s >/dev/null; done

	# Vault: the kubernetes method bound to the chart's service account, and the keys' material.
	local v="kubectl -n $NS exec deploy/vault -- env VAULT_ADDR=http://127.0.0.1:8201 VAULT_TOKEN=root"
	$v vault auth enable kubernetes >/dev/null
	$v vault write auth/kubernetes/config kubernetes_host=https://kubernetes.default.svc:443 >/dev/null
	printf 'path "secret/data/tracekeep/*" { capabilities = ["read"] }\n' | kubectl -n $NS exec -i deploy/vault -- \
		env VAULT_ADDR=http://127.0.0.1:8201 VAULT_TOKEN=root vault policy write tracekeep - >/dev/null
	$v vault write auth/kubernetes/role/tracekeep bound_service_account_names=$FULL bound_service_account_namespaces=$NS \
		policies=tracekeep ttl=1h >/dev/null
	for id in session-1 download-1 digest-1 master-1; do
		$v vault kv put "secret/tracekeep/$id" material="$(material)" >/dev/null
	done

	# tracekeep's credentials, each a file its configuration names by reference.
	kubectl -n $NS create secret generic tracekeep-catalog \
		--from-literal=dsn="tracekeep:tracekeep@tcp(mariadb:3306)/tracekeep?timeout=5s" >/dev/null
	kubectl -n $NS create secret generic tracekeep-s3 --from-literal=secret=tracekeep-secret >/dev/null
	kubectl -n $NS create secret tls tracekeep-tls --cert="$WORK/ca/tracekeep.crt" --key="$WORK/ca/tracekeep.key" >/dev/null
	kubectl -n $NS create secret generic tracekeep-people --from-literal=secret=$PEOPLE_SECRET >/dev/null
	kubectl -n $NS create secret generic tracekeep-ca --from-file=ca.crt="$WORK/ca/ca.crt" >/dev/null
}

# values tag ingest: the chart's values: every credential by reference, keys from Vault, downloads the API serves.
values() {
	cat <<YAML
image: {repository: tracekeep, tag: $1, pullPolicy: Never}
secrets: [tracekeep-catalog, tracekeep-s3, tracekeep-tls, tracekeep-people, tracekeep-ca]
ingest: {replicas: $2, workDirSize: 1Gi}
compact: {workDirSize: 1Gi}
api:
  replicas: 2
  workDirSize: 2Gi
  service: {type: NodePort, port: 8443, nodePort: 30443}
config:
  catalog: {engine: mariadb, dsn_ref: "file:///run/secrets/tracekeep-catalog/dsn"}
  bus: {url: "nats://nats:4222", subject_prefix: tpk, claim_bucket: tpk-objects}
  store:
    kind: s3
    presign: false
    s3: {endpoint: "http://minio:9000", region: us-east-1, bucket: tracekeep, access_key_id: tracekeep, secret_access_key_ref: "file:///run/secrets/tracekeep-s3/secret"}
  ingest: {flush_interval: 15s, ack_wait: 2m}
  compact: {interval: 1m}
  api:
    tls_cert_file: /run/secrets/tracekeep-tls/tls.crt
    tls_key_file: /run/secrets/tracekeep-tls/tls.key
    public_url: $PUBLIC
  auth:
    oidc: {issuer: $ISSUER, audience: tracekeep-api, ca_file: /run/secrets/tracekeep-ca/ca.crt}
    people:
      client_id: $PEOPLE_CLIENT
      client_secret_ref: "file:///run/secrets/tracekeep-people/secret"
      scopes: [groups]
      roles_claim: groups
      roles: {$MAPPED_GROUP: [viewer, exporter]}
  keys:
    vault:
      address: "https://vault.$NS.svc:8200"
      ca_file: /run/secrets/tracekeep-ca/ca.crt
      auth: {method: kubernetes, role: tracekeep}
    declared:
      session-1: {provider: vault, ref: tracekeep/session-1}
      download-1: {provider: vault, ref: tracekeep/download-1}
      digest-1: {provider: vault, ref: tracekeep/digest-1, kind: digest}
      master-1: {provider: vault, ref: tracekeep/master-1, kind: master}
    session: {issuing: session-1, accepted: [session-1]}
    download: {issuing: download-1, accepted: [download-1]}
YAML
}

# install chart tag ingest: the chart at chart installed, or upgraded, with the values.
install() {
	values "$2" "$3" > "$WORK/values.yaml"
	helm upgrade --install $RELEASE "$1" -n $NS -f "$WORK/values.yaml" --wait --timeout 20m >/dev/null
}

admin() { kubectl -n $NS exec deploy/$FULL-api -- /tracekeep "$@" --config /etc/tracekeep/config.yaml; }

# admit: a masking configuration staged and activated before any ingest member runs, so every member starts executing it.
admit() {
	local id
	id=$(kubectl -n $NS exec deploy/$FULL-api -- /tracekeep admin --config /etc/tracekeep/config.yaml --actor packaging config stage digest-1 master-1 |
		python3 -c 'import json,sys; print(json.load(sys.stdin)["ConfigID"])')
	kubectl -n $NS exec deploy/$FULL-api -- /tracekeep admin --config /etc/tracekeep/config.yaml --actor packaging config activate "$id" >/dev/null
	log "masking configuration $id active"
}

# traffic: the simulator on this host, through the bus's NodePort, until the tool's capture is admitted and its records flushed.
traffic() {
	go build -o "$WORK/tracekeep-sim" ./cmd/tracekeep-sim
	"$WORK/tracekeep-sim" -servers nats://127.0.0.1:4222 -tool $TOOL -device $DEVICE -rate 10 > "$WORK/sim.log" 2>&1 &
	pids+=($!)
	for _ in $(seq 90); do
		if admin status 2>/dev/null | python3 -c '
import json,sys
s=json.load(sys.stdin)
ok=[t for t in s.get("tools") or [] if t["tool_id"]==sys.argv[1] and t["open_captures"]>0 and t["deferred_messages"]==0 and t.get("last_record")]
sys.exit(0 if ok and not s.get("pending") else 1)' $TOOL; then
			sleep 30 # a flush or two, so the records are in packs
			return 0
		fi
		sleep 5
	done
	admin status >&2 || true
	die "the simulator's capture was not admitted"
}

# browse: the browser tests through the public URL; then the login crossing the two api replicas:
# the Service selects the first for the login; with the callback held, the first's own metrics must show it started the login,
# the first is deleted (its connections ending with it) and the Service selects the second; the second's log must then record the sign-in.
browse() {
	local pods a b svc=$FULL-api before after
	playwright "$PUBLIC"
	pods=($(kubectl -n $NS get pods -l app.kubernetes.io/component=api -o jsonpath='{.items[*].metadata.name}'))
	[ ${#pods[@]} -eq 2 ] || die "want two api replicas, have ${#pods[@]}"
	a=${pods[0]} b=${pods[1]}
	kubectl -n $NS label pod "$a" tracekeep-packaging=a --overwrite >/dev/null
	kubectl -n $NS label pod "$b" tracekeep-packaging=b --overwrite >/dev/null
	cat > "$WORK/crossing.sh" <<SH
#!/usr/bin/env bash
# crossing.sh route a|b: the Service selects that replica alone, settled once its ready endpoints are that pod's address alone.
# crossing.sh started: the first replica's successful login starts, from its own metrics through the API server's proxy.
set -euo pipefail
case "\$1" in
route)
	pod=\$(kubectl -n $NS get pods -l tracekeep-packaging=\$2 -o jsonpath='{.items[0].metadata.name}')
	ip=\$(kubectl -n $NS get pod "\$pod" -o jsonpath='{.status.podIP}')
	kubectl -n $NS patch service $svc --type merge -p "{\"spec\":{\"selector\":{\"tracekeep-packaging\":\"\$2\"}}}" >/dev/null
	for _ in \$(seq 60); do
		got=\$(kubectl -n $NS get endpointslices -l kubernetes.io/service-name=$svc -o jsonpath='{.items[*].endpoints[?(@.conditions.ready==true)].addresses[*]}')
		[ "\$got" = "\$ip" ] && break
		sleep 1
	done
	[ "\$got" = "\$ip" ] || { echo "crossing: the Service did not settle on \$pod" >&2; exit 1; }
	# The endpoints settle before kube-proxy programs them: wait until the public port answers three times running.
	ok=0
	for _ in \$(seq 60); do
		if curl -sk -o /dev/null --max-time 2 --resolve tracekeep.test:8443:127.0.0.1 https://tracekeep.test:8443/auth/me; then
			ok=\$((ok + 1)); [ \$ok -ge 3 ] && exit 0
		else
			ok=0
		fi
		sleep 1
	done
	echo "crossing: the public port did not answer after the Service settled on \$pod" >&2
	exit 1 ;;
started)
	kubectl get --raw "/api/v1/namespaces/$NS/pods/$a:9090/proxy/metrics" |
		awk '/^tracekeep_api_logins_total\{outcome="ok",step="start"\}/ { n = \$2 } END { print n + 0 }' ;;
esac
SH
	chmod +x "$WORK/crossing.sh"
	local started
	started=$("$WORK/crossing.sh" started)
	before=$(kubectl -n $NS logs "$b" | grep -c '"auth: signed in"' || true)
	log "crossing: the login on $a, the callback on $b after $a is deleted"
	local crossing
	crossing=$(printf %q "$WORK/crossing.sh")
	TRACEKEEP_BEFORE_LOGIN="$crossing route a" \
		TRACEKEEP_BEFORE_CALLBACK="[ \"\$($crossing started)\" -gt $started ] || { echo 'crossing: $a did not start the login' >&2; exit 1; }; kubectl -n $NS delete pod $a --wait=true >/dev/null && $crossing route b" \
		playwright "$PUBLIC" -g "finished by another"
	after=$(kubectl -n $NS logs "$b" | grep -c '"auth: signed in"' || true)
	kubectl -n $NS patch service $svc --type json -p '[{"op":"remove","path":"/spec/selector/tracekeep-packaging"}]' >/dev/null
	kubectl -n $NS rollout status deploy/$svc --timeout=180s >/dev/null
	[ "$after" -gt "$before" ] || die "the callback was not answered by $b"
	log "crossing: $a started the login, $b signed the person in"
}

# stop_traffic stops the simulator and waits for it to end.
stop_traffic() {
	for p in "${pids[@]}"; do
		kill "$p" 2>/dev/null || true
		wait "$p" 2>/dev/null || true
	done
	pids=()
}

log "fresh install of $(git rev-parse --short HEAD)"
build_image HEAD candidate
dependencies
install deploy/helm/tracekeep candidate 0
admit
install deploy/helm/tracekeep candidate 2
traffic
browse

if [ -z "${TRACEKEEP_BASELINE:-}" ]; then
	log "upgrade skipped: no baseline (no strict ancestor on main with the chart whose packaging run passed)"
	exit 0
fi
log "upgrade from $TRACEKEEP_BASELINE to $(git rev-parse --short HEAD)"
stop_traffic
kubectl delete namespace $NS --wait=true >/dev/null
build_image "$TRACEKEEP_BASELINE" baseline
dependencies
install "$WORK/src-baseline/deploy/helm/tracekeep" baseline 0
admit
install "$WORK/src-baseline/deploy/helm/tracekeep" baseline 2
traffic
# The producer stopped and its last records flushed, so the records read now are all there will be in their window.
stop_traffic
sleep 30
TRACEKEEP_EVIDENCE=$WORK/evidence.json browse
install deploy/helm/tracekeep candidate 2
TRACEKEEP_EVIDENCE=$WORK/evidence.json TRACEKEEP_AFTER_UPGRADE=1 browse
log "upgrade from $TRACEKEEP_BASELINE passed, the records read before it read the same after it"
