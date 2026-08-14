#!/usr/bin/env bash
set -euo pipefail
code_sha=$(git rev-parse HEAD)
git fetch --no-tags origin main:refs/remotes/origin/main
if [ "$code_sha" != "$(git rev-parse origin/main)" ]; then
  echo "::error::main moved during generation; aborting stale publication"
  exit 1
fi

snapshot_dir=$(mktemp -d "$RUNNER_TEMP/data-snapshot.XXXXXX")
pack_json=$(
  python scripts/data_snapshot.py pack \
    --root "$GITHUB_WORKSPACE" \
    --output-dir "$snapshot_dir" \
    --source-sha "$code_sha" \
    --max-archive-bytes 1932735283
)
echo "$pack_json" | jq -e . >/dev/null
dataset_id=$(jq -er '.dataset_id' <<<"$pack_json")
archive_path=$(jq -er '.archive_path' <<<"$pack_json")
manifest_path=$(jq -er '.manifest_path' <<<"$pack_json")
archive_sha256=$(jq -er '.archive_sha256' <<<"$pack_json")
if [[ ! "$dataset_id" =~ ^[0-9a-f]{64}$ ]] ||
   [[ ! "$archive_sha256" =~ ^[0-9a-f]{64}$ ]] ||
   [ ! -f "$archive_path" ] || [ ! -f "$manifest_path" ]; then
  echo "::error::Packed snapshot returned invalid metadata"
  exit 1
fi

if [ "$dataset_id" = "$BASE_DATASET_ID" ]; then
  # Pull intentionally restores the chronologically newest snapshot
  # for maintenance. Pages instead follows GitHub's latest-release
  # pointer so a verified rollback remains active until this run
  # actually publishes a different dataset.
  active_release_json=$(gh api "/repos/$DATA_REPOSITORY/releases/latest")
  active_release_tag=$(jq -er '.tag_name' <<<"$active_release_json")
  if [[ ! "$active_release_tag" =~ ^dataset-[A-Za-z0-9._-]+$ ]] ||
     [ "$(jq -r '.draft or .prerelease' <<<"$active_release_json")" != false ]; then
    echo "::error::The active private dataset release is invalid"
    exit 1
  fi
  active_manifest_dir=$(mktemp -d "$RUNNER_TEMP/active-manifest.XXXXXX")
  gh release download "$active_release_tag" \
    --repo "$DATA_REPOSITORY" \
    --pattern '*.manifest.json' \
    --dir "$active_manifest_dir"
  if [ "$(find "$active_manifest_dir" -maxdepth 1 -type f | wc -l | tr -d ' ')" -ne 1 ]; then
    echo "::error::The active dataset release must contain exactly one manifest"
    exit 1
  fi
  active_manifest_path=$(find "$active_manifest_dir" -maxdepth 1 -type f)
  active_dataset_id=$(jq -er '.dataset_id' "$active_manifest_path")
  if [[ ! "$active_dataset_id" =~ ^[0-9a-f]{64}$ ]]; then
    echo "::error::The active dataset release contains an invalid dataset ID"
    exit 1
  fi
  echo "Dataset is unchanged; retaining active Pages target $active_release_tag"
  site_changed=$(
    GH_TOKEN="$PUBLIC_GITHUB_TOKEN" \
      DATA_ARCHIVE_TOKEN="$DATA_ARCHIVE_TOKEN" \
      DATA_REPOSITORY="$DATA_REPOSITORY" \
      bash scripts/pages_deploy_needed.sh \
        "$code_sha" "$active_dataset_id" "$active_release_tag"
  )
  echo "code_sha=$code_sha" >> "$GITHUB_OUTPUT"
  echo "release_tag=$active_release_tag" >> "$GITHUB_OUTPUT"
  echo "dataset_id=$active_dataset_id" >> "$GITHUB_OUTPUT"
  echo "site_changed=$site_changed" >> "$GITHUB_OUTPUT"
  exit 0
fi

release_tag="dataset-$(date -u +%Y%m%dT%H%M%SZ)-${dataset_id:0:12}"
archive_name=$(basename "$archive_path")
manifest_name=$(basename "$manifest_path")
gh release create "$release_tag" \
  --repo "$DATA_REPOSITORY" \
  --draft \
  --title "$release_tag" \
  --notes "Validated dataset $dataset_id from public code $code_sha."
gh release upload "$release_tag" \
  "$archive_path" "$manifest_path" \
  --repo "$DATA_REPOSITORY"

remote_dir=$(mktemp -d "$RUNNER_TEMP/remote-snapshot.XXXXXX")
gh release download "$release_tag" \
  --repo "$DATA_REPOSITORY" \
  --dir "$remote_dir"
if [ "$(find "$remote_dir" -maxdepth 1 -type f | wc -l | tr -d ' ')" -ne 2 ]; then
  echo "::error::Draft release does not contain exactly two snapshot assets"
  exit 1
fi
python scripts/data_snapshot.py verify \
  --archive "$remote_dir/$archive_name" \
  --manifest "$remote_dir/$manifest_name"
remote_dataset_id=$(jq -er '.dataset_id' "$remote_dir/$manifest_name")
remote_source_sha=$(jq -er '.source_sha' "$remote_dir/$manifest_name")
if [ "$remote_dataset_id" != "$dataset_id" ] ||
   [ "$remote_source_sha" != "$code_sha" ]; then
  echo "::error::Remote snapshot identity does not match the validated local snapshot"
  exit 1
fi
gh release edit "$release_tag" \
  --repo "$DATA_REPOSITORY" \
  --draft=false \
  --latest
if [ "$(gh release view "$release_tag" --repo "$DATA_REPOSITORY" --json isDraft --jq '.isDraft')" != false ]; then
  echo "::error::Snapshot release remained a draft after publication"
  exit 1
fi
echo "code_sha=$code_sha" >> "$GITHUB_OUTPUT"
echo "release_tag=$release_tag" >> "$GITHUB_OUTPUT"
echo "dataset_id=$dataset_id" >> "$GITHUB_OUTPUT"
echo "site_changed=true" >> "$GITHUB_OUTPUT"
