#!/usr/bin/env bash
set -euo pipefail

readonly TRANSIENT_MUTATION_EXIT_CODE=75
readonly -a RETRY_DELAYS_SECONDS=(1 3)

require_public_tree_unchanged=${REQUIRE_PUBLIC_TREE_UNCHANGED:-false}
if [ "$require_public_tree_unchanged" != true ] &&
   [ "$require_public_tree_unchanged" != false ]; then
  echo "::error::Private publication boundary requirement is invalid"
  exit 1
fi
if [[ ! "${BASE_DATASET_ID:-}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "::error::Restored base dataset ID is invalid"
  exit 1
fi
if [[ ! "${BASE_RELEASE_TAG:-}" =~ ^dataset-[A-Za-z0-9._-]+$ ]]; then
  echo "::error::Restored base release tag is invalid"
  exit 1
fi
if [[ ! "${BASE_ARCHIVE_SHA256:-}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "::error::Restored base archive digest is invalid"
  exit 1
fi
if [[ ! "${BASE_MANIFEST_SHA256:-}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "::error::Restored base manifest digest is invalid"
  exit 1
fi

gh_read_retry() {
  python scripts/github_cli_retry.py --retry-forbidden-read -- "$@"
}

gh_mutate_once() {
  python scripts/github_cli_retry.py -- "$@"
}

release_record() {
  local tag=$1
  gh_read_retry api --paginate --slurp \
    "/repos/$DATA_REPOSITORY/releases?per_page=100" |
    jq -c --arg tag "$tag" '
      (add // [])
      | map(select(.tag_name == $tag))
      | if length == 0 then null
        elif length == 1 then .[0]
        else error("duplicate release tag")
        end
    '
}

release_state() {
  local release_json
  release_json=$(release_record "$1")
  jq -er 'if . == null then "missing" else (.draft | tostring) end' \
    <<<"$release_json"
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
  local observed_release
  local observed_release_id

  for observation_attempt in 0 1 2; do
    observed_release=$(release_record "$tag")
    if jq -e \
        --arg tag "$tag" \
        --arg title "$release_title" \
        --arg notes "$release_notes" \
        '. != null and
         (.id | type == "number" and . > 0 and . == floor) and
         .tag_name == $tag and
         .name == $title and
         .body == $notes and
         .draft == true and
         .prerelease == false and
         (.assets | type == "array" and length == 0)' \
        <<<"$observed_release" >/dev/null &&
       observed_release_id=$(jq -er '.id | tostring' <<<"$observed_release"); then
      owned_release_id=$observed_release_id
      return 0
    fi
    if [ "$observed_release" != null ]; then
      return 2
    fi
    if ! sleep_before_retry "$observation_attempt"; then
      return 1
    fi
  done
  return 1
}

verify_current_main() {
  local current_main_sha

  git fetch --no-tags origin main:refs/remotes/origin/main
  current_main_sha=$(git rev-parse origin/main)
  if [ "$code_sha" != "$current_main_sha" ]; then
    echo "::error::main moved during generation; aborting stale publication"
    return 1
  fi
}

verify_base_snapshot_current() {
  local current_base_json
  local current_base_release_tag
  local current_base_dataset_id
  local current_base_archive_sha256
  local current_base_manifest_sha256

  if ! current_base_json=$(
      python scripts/data_snapshot.py resolve \
        --repository "$DATA_REPOSITORY"
    ); then
    echo "::error::Could not resolve the newest private snapshot"
    return 1
  fi
  if ! current_base_release_tag=$(jq -er '.release_tag' <<<"$current_base_json") ||
     ! current_base_dataset_id=$(jq -er '.dataset_id' <<<"$current_base_json") ||
     ! current_base_archive_sha256=$(jq -er '.archive_sha256' <<<"$current_base_json") ||
     ! current_base_manifest_sha256=$(jq -er '.manifest_sha256' <<<"$current_base_json") ||
     [[ ! "$current_base_release_tag" =~ ^dataset-[A-Za-z0-9._-]+$ ]] ||
     [[ ! "$current_base_dataset_id" =~ ^[0-9a-f]{64}$ ]] ||
     [[ ! "$current_base_archive_sha256" =~ ^[0-9a-f]{64}$ ]] ||
     [[ ! "$current_base_manifest_sha256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "::error::Newest private snapshot returned invalid identity metadata"
    return 1
  fi
  if [ "$current_base_release_tag" != "$BASE_RELEASE_TAG" ]; then
    echo "::error::A newer private snapshot superseded the restored base"
    return 1
  fi
  if [ "$current_base_dataset_id" != "$BASE_DATASET_ID" ] ||
     [ "$current_base_archive_sha256" != "$BASE_ARCHIVE_SHA256" ] ||
     [ "$current_base_manifest_sha256" != "$BASE_MANIFEST_SHA256" ]; then
    echo "::error::The newest private snapshot no longer matches the restored base identity"
    return 1
  fi
}

code_sha=$(git rev-parse HEAD)
verify_current_main
verify_base_snapshot_current

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
manifest_sha256=$(jq -er '.manifest_sha256' <<<"$pack_json")
if [[ ! "$dataset_id" =~ ^[0-9a-f]{64}$ ]] ||
   [[ ! "$archive_sha256" =~ ^[0-9a-f]{64}$ ]] ||
   [[ ! "$manifest_sha256" =~ ^[0-9a-f]{64}$ ]] ||
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

public_tree_unchanged=false
active_release_tag=''
if [ -n "${BASE_PUBLIC_TREE_SHA256:-}" ]; then
  if [[ ! "$BASE_PUBLIC_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "::error::Restored public tree digest is invalid"
    exit 1
  fi
  current_public_dir=$(mktemp -d "$RUNNER_TEMP/current-public-artifact.XXXXXX")
  current_public_json=$(
    python scripts/build_pages_artifact.py \
      --source-root "$GITHUB_WORKSPACE" \
      --output "$current_public_dir" \
      --source-sha 0000000000000000000000000000000000000000 \
      --dataset-id 0000000000000000000000000000000000000000000000000000000000000000 \
      --workers 1
  )
  echo "$current_public_json" | jq -e . >/dev/null
  current_public_tree_sha256=$(jq -er '.tree_sha256' <<<"$current_public_json")
  rm -rf "$current_public_dir"
  if [[ ! "$current_public_tree_sha256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "::error::Current public artifact returned an invalid tree digest"
    exit 1
  fi
  if [ "$current_public_tree_sha256" = "$BASE_PUBLIC_TREE_SHA256" ]; then
    public_tree_unchanged=true
    active_release_json=$(gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest")
    active_release_tag=$(jq -er '.tag_name' <<<"$active_release_json")
    if [[ ! "$active_release_tag" =~ ^dataset-[A-Za-z0-9._-]+$ ]] ||
       [ "$(jq -r '.draft or .prerelease' <<<"$active_release_json")" != false ]; then
      echo "::error::The active private dataset release is invalid"
      exit 1
    fi
  fi
fi

if [ "$require_public_tree_unchanged" = true ] &&
   [ "$public_tree_unchanged" != true ]; then
  echo "::error::Private-only publication requires an unchanged public artifact"
  exit 1
fi

release_tag="dataset-$(date -u +%Y%m%dT%H%M%SZ)-${dataset_id:0:12}"
archive_name=$(basename "$archive_path")
manifest_name=$(basename "$manifest_path")
release_title=$release_tag
release_notes="Validated dataset $dataset_id from public code $code_sha; restored base $BASE_RELEASE_TAG ($BASE_DATASET_ID)."
owned_draft=false
owned_release_id=''

abort_with_owned_draft_cleanup() {
  local message=$1

  echo "::error::$message"
  if [ "$owned_draft" = true ]; then
    # GitHub release deletion has no server-side conditional identity predicate.
    # Preserve the draft rather than risk deleting a same-tag replacement or a
    # release that changed state after a client-side ownership read.
    echo "::error::Owned unpublished snapshot draft requires manual reconciliation; automatic deletion is disabled"
  fi
  exit 1
}

verify_owned_draft_current() {
  local expected_asset_state=$1
  local observed_release

  if [[ ! "$owned_release_id" =~ ^[1-9][0-9]*$ ]] ||
     { [ "$expected_asset_state" != empty ] &&
       [ "$expected_asset_state" != complete ]; }; then
    echo "::error::Exact owned snapshot draft identity is invalid"
    return 1
  fi
  if ! observed_release=$(release_record "$release_tag"); then
    echo "::error::Could not resolve the exact owned snapshot draft"
    return 1
  fi
  if ! jq -e \
      --argjson release_id "$owned_release_id" \
      --arg tag "$release_tag" \
      --arg title "$release_title" \
      --arg notes "$release_notes" \
      --arg expected_asset_state "$expected_asset_state" \
      --arg archive_name "$archive_name" \
      --arg manifest_name "$manifest_name" '
        . != null and
        .id == $release_id and
        .tag_name == $tag and
        .name == $title and
        .body == $notes and
        .draft == true and
        .prerelease == false and
        (.assets | type == "array") and
        (if $expected_asset_state == "empty" then
           (.assets | length == 0)
         else
           (.assets | length == 2) and
           ([.assets[].name] | sort == ([$archive_name, $manifest_name] | sort)) and
           all(.assets[];
             (.id | type == "number" and . > 0 and . == floor) and
             .state == "uploaded")
         end)' \
      <<<"$observed_release" >/dev/null; then
    echo "::error::Remote release is no longer the exact owned snapshot draft"
    return 1
  fi
}

expected_latest_release_tag=$(
  gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest" --jq '.tag_name'
)
if [[ ! "$expected_latest_release_tag" =~ ^dataset-[A-Za-z0-9._-]+$ ]]; then
  echo "::error::The pre-publication latest release pointer is invalid"
  exit 1
fi
if [ "$public_tree_unchanged" = true ] &&
   [ "$expected_latest_release_tag" != "$active_release_tag" ]; then
  echo "::error::The latest release pointer changed before private-only publication"
  exit 1
fi

draft_ready=false
for attempt in 0 1 2; do
  mutation_status=0
  if ! verify_current_main || ! verify_base_snapshot_current; then
    abort_with_owned_draft_cleanup \
      "Publication preconditions changed before snapshot draft creation"
  fi
  gh_mutate_once release create "$release_tag" \
    --repo "$DATA_REPOSITORY" \
    --draft \
    --title "$release_title" \
    --notes "$release_notes" ||
    mutation_status=$?

  draft_observation_status=0
  wait_for_draft_release "$release_tag" || draft_observation_status=$?
  if [ "$draft_observation_status" -eq 0 ]; then
    draft_ready=true
    owned_draft=true
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
  local remote_archive_sha256
  local remote_dataset_id
  local remote_manifest_sha256
  local remote_source_sha
  local remote_verify_json

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
  if ! remote_verify_json=$(python scripts/data_snapshot.py verify \
      --archive "$candidate_dir/$archive_name" \
      --manifest "$candidate_dir/$manifest_name"); then
    rm -rf "$candidate_dir"
    return 1
  fi
  if ! remote_archive_sha256=$(jq -er '.archive_sha256' <<<"$remote_verify_json") ||
     ! remote_dataset_id=$(jq -er '.dataset_id' <<<"$remote_verify_json") ||
     ! remote_manifest_sha256=$(jq -er '.manifest_sha256' <<<"$remote_verify_json") ||
     ! remote_source_sha=$(jq -er '.source_sha' <<<"$remote_verify_json") ||
     [[ ! "$remote_archive_sha256" =~ ^[0-9a-f]{64}$ ]] ||
     [[ ! "$remote_manifest_sha256" =~ ^[0-9a-f]{64}$ ]]; then
    rm -rf "$candidate_dir"
    return 1
  fi
  if [ "$remote_dataset_id" != "$dataset_id" ] ||
     [ "$remote_source_sha" != "$code_sha" ] ||
     [ "$remote_archive_sha256" != "$archive_sha256" ] ||
     [ "$remote_manifest_sha256" != "$manifest_sha256" ]; then
    echo "::warning::Downloaded release does not match the exact local target assets"
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
  if ! verify_current_main || ! verify_base_snapshot_current; then
    abort_with_owned_draft_cleanup \
      "Publication preconditions changed before snapshot upload"
  fi
  if ! verify_owned_draft_current empty; then
    abort_with_owned_draft_cleanup \
      "Exact owned snapshot draft changed before snapshot upload"
  fi
  gh_mutate_once release upload "$release_tag" \
    "$archive_path" "$manifest_path" \
    --repo "$DATA_REPOSITORY" || mutation_status=$?

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
  abort_with_owned_draft_cleanup \
    "Remote snapshot could not be reconciled after bounded attempts"
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
  if [ "$observed_draft_state" = false ]; then
    if [ "${public_tree_unchanged:-false}" = true ] &&
       [ "$observed_latest_tag" = "$active_release_tag" ]; then
      return 0
    fi
    if [ "${public_tree_unchanged:-false}" != true ] &&
       [ "$observed_latest_tag" = "$release_tag" ]; then
      return 0
    fi
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
    owned_draft=false
    break
  fi
  if [ "$publication_observation_status" -eq 2 ]; then
    abort_with_owned_draft_cleanup \
      "The latest release pointer changed before publication"
  fi
  if [ "$publication_observation_status" -ne 1 ]; then
    break
  fi

  mutation_status=0
  publication_latest_flag=--latest
  if [ "${public_tree_unchanged:-false}" = true ]; then
    publication_latest_flag=--latest=false
  fi
  if ! verify_current_main || ! verify_base_snapshot_current; then
    abort_with_owned_draft_cleanup \
      "Publication preconditions changed before snapshot publication"
  fi
  if ! verify_owned_draft_current complete ||
     ! verify_remote_snapshot; then
    abort_with_owned_draft_cleanup \
      "Exact owned snapshot draft or target assets changed before snapshot publication"
  fi
  gh_mutate_once release edit "$release_tag" \
    --repo "$DATA_REPOSITORY" \
    --draft=false \
    "$publication_latest_flag" || mutation_status=$?

  publication_observation_status=0
  wait_for_publication || publication_observation_status=$?
  if [ "$publication_observation_status" -eq 0 ]; then
    publication_verified=true
    owned_draft=false
    break
  fi
  if [ "$publication_observation_status" -eq 2 ]; then
    abort_with_owned_draft_cleanup \
      "The latest release pointer changed during publication"
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
  abort_with_owned_draft_cleanup \
    "Snapshot publication could not be reconciled after bounded attempts"
fi

echo "code_sha=$code_sha" >> "$GITHUB_OUTPUT"
echo "release_tag=$release_tag" >> "$GITHUB_OUTPUT"
echo "dataset_id=$dataset_id" >> "$GITHUB_OUTPUT"
if [ "$public_tree_unchanged" = true ]; then
  echo "site_changed=false" >> "$GITHUB_OUTPUT"
else
  echo "site_changed=true" >> "$GITHUB_OUTPUT"
fi
