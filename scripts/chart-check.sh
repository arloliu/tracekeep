#!/usr/bin/env bash
# chart-check: the Helm chart linted, rendered with the tests' values and validated (architecture.md §3.3);
# a credential written in the values and an empty configuration are refused at render.
set -euo pipefail
chart=deploy/helm/tracekeep
values=$chart/ci/test-values.yaml
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

helm lint --strict "$chart" -f "$values"
helm template t "$chart" -f "$values" > "$out/rendered.yaml"
# The PrometheusRule is the operator's CRD, which kubeconform has no schema for.
kubeconform -strict -summary -skip PrometheusRule "$out/rendered.yaml"

refused() {
	local why=$1; shift
	if helm template t "$chart" "$@" > /dev/null 2> "$out/err"; then
		echo "chart-check: rendered $why, want refused" >&2
		exit 1
	fi
	grep -q "$why" "$out/err" || { echo "chart-check: refused without naming $why:" >&2; cat "$out/err" >&2; exit 1; }
}
refused "config.catalog.dsn is a credential" -f "$values" --set config.catalog.dsn=sentinel
refused "config.bus.password is a credential" -f "$values" --set config.bus.password=sentinel
refused "config.store.s3.secret_access_key is a credential" -f "$values" --set config.store.s3.secret_access_key=sentinel
refused "config is tracekeep's configuration and is required" --set image.tag=test

# Every container runs as the image's non-root user on a read-only root, and every role and the Job mount /work.
for want in "readOnlyRootFilesystem: true" "runAsNonRoot: true" "mountPath: /work"; do
	n=$(grep -c "$want" "$out/rendered.yaml")
	[ "$n" -ge 4 ] || { echo "chart-check: \"$want\" in $n pod specs, want the three roles and the migrate Job" >&2; exit 1; }
done
echo "chart-check: ok"
