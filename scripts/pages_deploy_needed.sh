#!/usr/bin/env bash
set -euo pipefail

artifact_paths=(
  .nojekyll
  CNAME
  index.html
  site-data-loader.js
  scripts/build_pages_artifact.py
  scripts/data_snapshot.py
  scripts/github_cli_retry.py
  scripts/pages_deploy_needed.sh
  .github/workflows/deploy-pages.yml
)

valid_code_sha() {
  [[ "${1:-}" =~ ^[0-9a-f]{40}$ ]]
}

valid_dataset_id() {
  [[ "${1:-}" =~ ^[0-9a-f]{64}$ ]]
}

valid_release_tag() {
  [[ "${1:-}" =~ ^dataset-[A-Za-z0-9._-]+$ ]]
}

latest_pages_deployment_matches_marker() {
  local code_sha=$1
  local dataset_id=$2
  local release_tag=$3
  local public_repository=${GITHUB_REPOSITORY:-}
  local public_token=${GH_TOKEN:-${GITHUB_TOKEN:-}}
  local data_repository=${DATA_REPOSITORY:-}
  local data_token=${DATA_ARCHIVE_TOKEN:-}
  local deployment
  local deployment_id
  local marker_dir
  local marker_path
  local status

  if [ -z "$public_repository" ] || [ -z "$public_token" ] ||
     [ -z "$data_repository" ] || [ -z "$data_token" ]; then
    return 1
  fi
  deployment=$(
    curl \
      --fail \
      --silent \
      --show-error \
      --location \
      --max-time 10 \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${public_token}" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      "https://api.github.com/repos/${public_repository}/deployments?environment=github-pages&per_page=1" \
      2>/dev/null || true
  )
  deployment_id=$(jq -r '.[0].id // empty' <<<"$deployment" 2>/dev/null || true)
  if [[ ! "$deployment_id" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  status=$(
    curl \
      --fail \
      --silent \
      --show-error \
      --location \
      --max-time 10 \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${public_token}" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      "https://api.github.com/repos/${public_repository}/deployments/${deployment_id}/statuses?per_page=1" \
      2>/dev/null |
      jq -r '.[0].state // empty' || true
  )
  if [ "$status" != success ]; then
    return 1
  fi

  marker_dir=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/pages-marker.XXXXXX")
  if ! GH_TOKEN="$data_token" gh release download "$release_tag" \
      --repo "$data_repository" \
      --pattern pages-deployment.json \
      --dir "$marker_dir" \
      >/dev/null 2>&1; then
    rm -rf "$marker_dir"
    return 1
  fi
  marker_path="$marker_dir/pages-deployment.json"
  if [ ! -f "$marker_path" ]; then
    rm -rf "$marker_dir"
    return 1
  fi
  if [ "$(jq -r '.code_sha // empty' "$marker_path" 2>/dev/null || true)" != "$code_sha" ] ||
     [ "$(jq -r '.dataset_id // empty' "$marker_path" 2>/dev/null || true)" != "$dataset_id" ] ||
     [ "$(jq -r '.release_tag // empty' "$marker_path" 2>/dev/null || true)" != "$release_tag" ] ||
     [ "$(jq -r '.deployment_id // empty | tostring' "$marker_path" 2>/dev/null || true)" != "$deployment_id" ]; then
    rm -rf "$marker_dir"
    return 1
  fi
  rm -rf "$marker_dir"
}

ensure_commit() {
  local sha=$1
  if git cat-file -e "${sha}^{commit}" 2>/dev/null; then
    return 0
  fi
  git fetch --no-tags --depth=1 origin "$sha" >/dev/null 2>&1
}

if [ "${1:-}" = --between ]; then
  older_sha=${2:-}
  newer_sha=${3:-}
  if ! valid_code_sha "$older_sha" || ! valid_code_sha "$newer_sha"; then
    echo "invalid comparison SHA" >&2
    exit 2
  fi
  ensure_commit "$older_sha"
  ensure_commit "$newer_sha"
  if git diff --quiet "$older_sha" "$newer_sha" -- "${artifact_paths[@]}"; then
    echo false
  else
    echo true
  fi
  exit 0
fi

code_sha=${1:-}
dataset_id=${2:-}
release_tag=${3:-}
if ! valid_code_sha "$code_sha" ||
   ! valid_dataset_id "$dataset_id" ||
   ! valid_release_tag "$release_tag"; then
  echo "invalid Pages target identity" >&2
  exit 2
fi

host=$(tr -d '[:space:]' < CNAME 2>/dev/null || true)
if [ -z "$host" ]; then
  echo true
  exit 0
fi

headers_file=$(
  mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/pages-manifest-headers.XXXXXX"
)
body_file=$(
  mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/pages-manifest-body.XXXXXX"
)
trap 'rm -f "$headers_file" "$body_file"' EXIT
live_code_sha=""
live_dataset_id=""
if curl \
    --fail \
    --silent \
    --show-error \
    --location \
    --max-time 10 \
    --dump-header "$headers_file" \
    --output "$body_file" \
    -H "Cache-Control: no-cache" \
    "https://${host}/deployment-manifest.json?code=${code_sha}&dataset=${dataset_id}" \
    2>/dev/null; then
  live_code_sha=$(jq -r '.source_sha // empty' "$body_file" 2>/dev/null || true)
  live_dataset_id=$(jq -r '.dataset_id // empty' "$body_file" 2>/dev/null || true)
fi
http_status=$(
  awk '/^HTTP\// { status=$2 } END { print status }' "$headers_file" |
    tr -d '\r'
)

if [ "$live_code_sha" = "$code_sha" ] &&
   [ "$live_dataset_id" = "$dataset_id" ]; then
  echo false
  exit 0
fi

if [ "$http_status" = 403 ] &&
   grep -qiE '^server:[[:space:]]*cloudflare[[:space:]]*$' "$headers_file" &&
   grep -qiE '^cf-mitigated:[[:space:]]*challenge[[:space:]]*$' "$headers_file" &&
   latest_pages_deployment_matches_marker \
     "$code_sha" "$dataset_id" "$release_tag"; then
  echo false
else
  # Missing, mismatched, failed, or unverifiable state must self-heal.
  echo true
fi
