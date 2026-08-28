#!/usr/bin/env bash
set -euo pipefail

readonly TRANSIENT_MUTATION_EXIT_CODE=75
readonly -a RETRY_DELAYS_SECONDS=(1 3)

gh_read_retry() {
  python scripts/github_cli_retry.py --retry-forbidden-read -- "$@"
}

gh_mutate_once() {
  python scripts/github_cli_retry.py -- "$@"
}

release_state() {
  local tag=$1
  gh_read_retry api --paginate --slurp \
    "/repos/$DATA_REPOSITORY/releases?per_page=100" |
    jq -er --arg tag "$tag" '
      (add // [])
      | map(select(.tag_name == $tag))
      | if length == 0 then "missing"
        elif length == 1 then (.[0].draft | tostring)
        else error("duplicate release tag")
        end
    '
}

sleep_before_retry() {
  local attempt=$1
  if [ "$attempt" -ge "${#RETRY_DELAYS_SECONDS[@]}" ]; then
    return 1
  fi
  sleep "${RETRY_DELAYS_SECONDS[$attempt]}"
}

wait_for_draft_release() {
  local tag=$1
  local observation_attempt
  local observed_release_state

  for observation_attempt in 0 1 2; do
    observed_release_state=$(release_state "$tag")
    if [ "$observed_release_state" = true ]; then
      return 0
    fi
    if [ "$observed_release_state" != missing ]; then
      return 2
    fi
    if ! sleep_before_retry "$observation_attempt"; then
      return 1
    fi
  done
  return 1
}

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
  # Pull intentionally restores the chronologically newest snapshot for
  # maintenance. Pages follows GitHub's latest-release pointer so a verified
  # rollback remains active until a different dataset is actually published.
  active_release_json=$(gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest")
  active_release_tag=$(jq -er '.tag_name' <<<"$active_release_json")
  if [[ ! "$active_release_tag" =~ ^dataset-[A-Za-z0-9._-]+$ ]] ||
     [ "$(jq -r '.draft or .prerelease' <<<"$active_release_json")" != false ]; then
    echo "::error::The active private dataset release is invalid"
    exit 1
  fi
  active_manifest_dir=$(mktemp -d "$RUNNER_TEMP/active-manifest.XXXXXX")
  gh_read_retry release download "$active_release_tag" \
    --repo "$DATA_REPOSITORY" \
    --pattern '*.manifest.json' \
    --dir "$active_manifest_dir" \
    --clobber
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
expected_latest_release_tag=$(
  gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest" --jq '.tag_name'
)
if [[ ! "$expected_latest_release_tag" =~ ^dataset-[A-Za-z0-9._-]+$ ]]; then
  echo "::error::The pre-publication latest release pointer is invalid"
  exit 1
fi
draft_ready=false
for attempt in 0 1 2; do
  mutation_status=0
  gh_mutate_once release create "$release_tag" \
    --repo "$DATA_REPOSITORY" \
    --draft \
    --title "$release_tag" \
    --notes "Validated dataset $dataset_id from public code $code_sha." ||
    mutation_status=$?

  draft_observation_status=0
  wait_for_draft_release "$release_tag" || draft_observation_status=$?
  if [ "$draft_observation_status" -eq 0 ]; then
    draft_ready=true
    break
  fi
  if [ "$draft_observation_status" -eq 2 ]; then
    echo "::error::Snapshot tag collision is not an unpublished draft"
    exit 1
  fi
  # A confirmed mutation is never replayed merely because GitHub's read path
  # remained stale. Only an explicitly uncertain mutation may be attempted
  # again after the bounded observation window.
  if [ "$mutation_status" -ne "$TRANSIENT_MUTATION_EXIT_CODE" ] ||
     ! sleep_before_retry "$attempt"; then
    break
  fi
done
if [ "$draft_ready" != true ]; then
  echo "::error::Snapshot draft could not be reconciled after bounded attempts"
  exit 1
fi

verify_remote_snapshot() {
  local candidate_dir
  local file_count
  local remote_dataset_id
  local remote_source_sha

  candidate_dir=$(mktemp -d "$RUNNER_TEMP/remote-snapshot.XXXXXX")
  if ! gh_read_retry release download "$release_tag" \
      --repo "$DATA_REPOSITORY" \
      --dir "$candidate_dir" \
      --clobber; then
    rm -rf "$candidate_dir"
    return 1
  fi
  file_count=$(find "$candidate_dir" -maxdepth 1 -type f | wc -l | tr -d ' ')
  if [ "$file_count" -ne 2 ] ||
     [ ! -f "$candidate_dir/$archive_name" ] ||
     [ ! -f "$candidate_dir/$manifest_name" ]; then
    echo "::warning::Downloaded release does not contain exactly two snapshot assets"
    rm -rf "$candidate_dir"
    return 1
  fi
  if ! chmod 600 "$candidate_dir/$archive_name"; then
    rm -rf "$candidate_dir"
    return 1
  fi
  if ! python scripts/data_snapshot.py verify \
      --archive "$candidate_dir/$archive_name" \
      --manifest "$candidate_dir/$manifest_name"; then
    rm -rf "$candidate_dir"
    return 1
  fi
  if ! remote_dataset_id=$(jq -er '.dataset_id' "$candidate_dir/$manifest_name") ||
     ! remote_source_sha=$(jq -er '.source_sha' "$candidate_dir/$manifest_name"); then
    rm -rf "$candidate_dir"
    return 1
  fi
  if [ "$remote_dataset_id" != "$dataset_id" ] ||
     [ "$remote_source_sha" != "$code_sha" ]; then
    rm -rf "$candidate_dir"
    return 1
  fi
  rm -rf "$candidate_dir"
}

wait_for_remote_snapshot() {
  local observation_attempt
  for observation_attempt in 0 1 2; do
    if verify_remote_snapshot; then
      return 0
    fi
    if ! sleep_before_retry "$observation_attempt"; then
      return 1
    fi
  done
  return 1
}

snapshot_verified=false
for attempt in 0 1 2; do
  mutation_status=0
  gh_mutate_once release upload "$release_tag" \
    "$archive_path" "$manifest_path" \
    --repo "$DATA_REPOSITORY" \
    --clobber || mutation_status=$?

  if wait_for_remote_snapshot; then
    snapshot_verified=true
    break
  fi
  if [ "$mutation_status" -ne "$TRANSIENT_MUTATION_EXIT_CODE" ] ||
     ! sleep_before_retry "$attempt"; then
    break
  fi
done
if [ "$snapshot_verified" != true ]; then
  echo "::error::Remote snapshot could not be reconciled after bounded attempts"
  exit 1
fi

observe_publication() {
  local observed_draft_state
  local observed_latest_tag

  if ! observed_draft_state=$(
      gh_read_retry release view "$release_tag" \
        --repo "$DATA_REPOSITORY" \
        --json isDraft \
        --jq '.isDraft'
    ); then
    return 3
  fi
  if ! observed_latest_tag=$(
      gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest" --jq '.tag_name'
    ); then
    return 3
  fi
  if [ "$observed_draft_state" != true ] &&
     [ "$observed_draft_state" != false ]; then
    return 3
  fi
  if [ "$observed_draft_state" = false ] &&
     [ "$observed_latest_tag" = "$release_tag" ]; then
    return 0
  fi
  # The two read paths may become consistent in either order. Only the target
  # and the captured pre-publication pointer are acceptable during convergence.
  if [ "$observed_latest_tag" != "$expected_latest_release_tag" ] &&
     [ "$observed_latest_tag" != "$release_tag" ]; then
    return 2
  fi
  return 1
}

wait_for_publication() {
  local observation_attempt
  local observation_status

  for observation_attempt in 0 1 2; do
    observation_status=0
    observe_publication || observation_status=$?
    if [ "$observation_status" -eq 0 ]; then
      return 0
    fi
    if [ "$observation_status" -ne 1 ]; then
      return "$observation_status"
    fi
    if ! sleep_before_retry "$observation_attempt"; then
      return 1
    fi
  done
  return 1
}

publication_verified=false
for attempt in 0 1 2; do
  publication_observation_status=0
  observe_publication || publication_observation_status=$?
  if [ "$publication_observation_status" -eq 0 ]; then
    publication_verified=true
    break
  fi
  if [ "$publication_observation_status" -eq 2 ]; then
    echo "::error::The latest release pointer changed before publication"
    exit 1
  fi
  if [ "$publication_observation_status" -ne 1 ]; then
    break
  fi

  mutation_status=0
  gh_mutate_once release edit "$release_tag" \
    --repo "$DATA_REPOSITORY" \
    --draft=false \
    --latest || mutation_status=$?

  publication_observation_status=0
  wait_for_publication || publication_observation_status=$?
  if [ "$publication_observation_status" -eq 0 ]; then
    publication_verified=true
    break
  fi
  if [ "$publication_observation_status" -eq 2 ]; then
    echo "::error::The latest release pointer changed during publication"
    exit 1
  fi
  # Never replay a confirmed release edit because a read replica remained
  # stale. A replay is allowed only when the helper reports an uncertain
  # mutation and bounded observation still cannot prove the target state.
  if [ "$publication_observation_status" -ne 1 ] ||
     [ "$mutation_status" -ne "$TRANSIENT_MUTATION_EXIT_CODE" ] ||
     ! sleep_before_retry "$attempt"; then
    break
  fi
done
if [ "$publication_verified" != true ]; then
  echo "::error::Snapshot publication could not be reconciled after bounded attempts"
  exit 1
fi

echo "code_sha=$code_sha" >> "$GITHUB_OUTPUT"
echo "release_tag=$release_tag" >> "$GITHUB_OUTPUT"
echo "dataset_id=$dataset_id" >> "$GITHUB_OUTPUT"
echo "site_changed=true" >> "$GITHUB_OUTPUT"
