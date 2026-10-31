#!/usr/bin/env bash
# alerts-check: the chart's alert rules (operations.md §10.4) checked and tested with promtool,
# and every rule's runbook_url an anchor of docs/guides/runbook.md.
set -euo pipefail
chart=deploy/helm/tracekeep
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

# The PrometheusRule's spec is a Prometheus rule file once its two-space indent is gone.
helm template t "$chart" -f "$chart/ci/test-values.yaml" --show-only templates/prometheusrule.yaml |
	awk 'found { sub(/^  /, ""); print } /^spec:$/ { found = 1 }' > "$out/alerts.yaml"
cp "$chart/ci/alerts-test.yaml" "$out/"
promtool check rules --lint=all --lint-fatal "$out/alerts.yaml"
promtool test rules "$out/alerts-test.yaml"

# Each rule is tested, and each runbook section exists.
for alert in $(grep -oE 'alert: [A-Za-z]+' "$out/alerts.yaml" | cut -d' ' -f2); do
	grep -q "alertname: $alert" "$out/alerts-test.yaml" || { echo "alerts-check: $alert has no test" >&2; exit 1; }
done
for anchor in $(grep -oE 'runbook_url: [^ ]+' "$out/alerts.yaml" | sed 's/.*#//'); do
	grep -qx "## $anchor" docs/guides/runbook.md || { echo "alerts-check: docs/guides/runbook.md has no section ## $anchor" >&2; exit 1; }
done
echo "alerts-check: ok"
