#!/usr/bin/env bash
# baseline: the upgrade's baseline (architecture.md §3.3 *Packaging tests*): the newest strict ancestor of HEAD on main
# that has the chart and whose packaging run passed, never HEAD itself or a descendant; nothing when none exists.
# Needs the full history (a checkout with fetch-depth 0), GITHUB_REPOSITORY and GH_TOKEN; without them it prints nothing.
set -euo pipefail
[ -n "${GITHUB_REPOSITORY:-}" ] && [ -n "${GH_TOKEN:-}" ] || exit 0
head=$(git rev-parse HEAD)
curl -fsS -H "Authorization: Bearer $GH_TOKEN" -H "Accept: application/vnd.github+json" \
	"https://api.github.com/repos/$GITHUB_REPOSITORY/actions/workflows/packaging.yaml/runs?branch=main&status=success&per_page=100" |
	python3 -c 'import json,sys; [print(r["head_sha"]) for r in json.load(sys.stdin).get("workflow_runs", [])]' |
	while read -r sha; do
		[ "$sha" != "$head" ] || continue
		git merge-base --is-ancestor "$sha" "$head" 2>/dev/null || continue
		git cat-file -e "$sha:deploy/helm/tracekeep/Chart.yaml" 2>/dev/null || continue
		echo "$sha"
		break
	done
