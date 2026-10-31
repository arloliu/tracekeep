#!/usr/bin/env bash
# baseline: the upgrade's baseline (architecture.md §3.3 *Packaging tests*): the newest strict ancestor of HEAD on main
# that has the chart and whose packaging run passed, never HEAD itself or a descendant; nothing when none exists.
# Needs the full history (a checkout with fetch-depth 0), GITHUB_REPOSITORY and GH_TOKEN, refusing to run without them;
# a failed lookup fails, so it is never taken for a baseline that does not exist.
set -euo pipefail
: "${GITHUB_REPOSITORY:?baseline: GITHUB_REPOSITORY is needed}" "${GH_TOKEN:?baseline: GH_TOKEN is needed}"
head=$(git rev-parse HEAD)
runs=$(mktemp)
trap 'rm -f "$runs"' EXIT
# Every successful run on main, newest first, page by page until a page comes back short.
# GitHub answers a filtered listing's first 1,000 runs only: a full tenth page means the listing may be cut short.
complete=
for page in $(seq 1 10); do
	if curl -fsS --retry 3 -H "Authorization: Bearer $GH_TOKEN" -H "Accept: application/vnd.github+json" \
		"https://api.github.com/repos/$GITHUB_REPOSITORY/actions/workflows/packaging.yaml/runs?branch=main&status=success&per_page=100&page=$page" |
		python3 -c 'import json,sys; rs=json.load(sys.stdin)["workflow_runs"]; [print(r["head_sha"]) for r in rs]; sys.exit(0 if len(rs)==100 else 3)' >> "$runs"; then
		continue
	else
		rc=$?
	fi
	[ $rc -eq 3 ] || exit $rc
	complete=1
	break
done
while read -r sha; do
	[ "$sha" != "$head" ] || continue
	git merge-base --is-ancestor "$sha" "$head" 2>/dev/null || continue
	git cat-file -e "$sha:deploy/helm/tracekeep/Chart.yaml" 2>/dev/null || continue
	echo "$sha"
	exit 0
done < "$runs"
# None among the runs listed: no baseline only when the listing was whole; otherwise fail rather than skip an upgrade that may be due.
[ -n "$complete" ] || { echo "baseline: GitHub listed 1,000 successful runs and none qualifies; the listing may be cut short" >&2; exit 1; }
