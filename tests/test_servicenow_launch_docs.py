from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNBOOK = ROOT / "docs/servicenow-insider-launch.md"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def normalized(text: str) -> str:
    return " ".join(text.split())


def section(text: str, heading: str, next_heading: str) -> str:
    start = text.index(heading)
    end = text.index(next_heading, start + len(heading))
    return normalized(text[start:end])


def test_runbook_has_ordered_operational_sections() -> None:
    text = normalized(read(RUNBOOK))
    headings = (
        "## Scope and non-authority",
        "## Separate private authorities",
        "## Permission gates",
        "## Metadata-only preflight",
        "## Private-only chronological backfill",
        "## Mapping review and private candidate",
        "## Protected Environment and secret",
        "## Private policy-approval dispatch",
        "## Separate manual public materialization",
        "## Exact readback and privacy verification",
        "## Fail-closed recovery",
        "## Ongoing operating boundary",
    )
    positions = [text.index(heading) for heading in headings]
    assert positions == sorted(positions)
    assert all(text.count(heading) == 1 for heading in headings)


def test_runbook_separates_fixed_scope_authorities() -> None:
    text = normalized(read(RUNBOOK))
    required = (
        "CIK `0001373715` only",
        "`approved-issuers-v1`",
        "`approve-servicenow-insider-ingestion.yml`",
        "private-ingestion authority",
        "`publication-policy-v1`",
        "`approve-servicenow-insider-publication.yml`",
        "exact mapping approval",
        "Neither authority materializes or deploys public data.",
    )
    assert all(phrase in text for phrase in required)


def test_runbook_requires_all_external_permissions_and_no_credential_readback() -> None:
    text = normalized(read(RUNBOOK))
    required = (
        "explicit authorization before downloading any SEC quarterly ZIP or artifact",
        "explicit authorization before workflow dispatch",
        "explicit authorization before creating or updating a GitHub Environment or secret",
        "explicit authorization before commit, push, PR, or merge",
        "explicit first-publication authorization",
        "Never expose or read back credentials.",
    )
    assert all(phrase in text for phrase in required)


def test_runbook_preflight_backfill_and_mapping_are_private_and_fail_closed() -> None:
    text = normalized(read(RUNBOOK))
    required = (
        "exact `main` SHA",
        "positive immutable private release ID",
        "tag, title, notes, draft/prerelease, and asset state",
        "dataset, archive, and manifest identities",
        "approved issuer, policy, issuer generation, and public baseline",
        "No raw private corpus or owner data.",
        "exact available quarters chronologically",
        "`publish_insider_publication=false`",
        "public insider subtree byte-for-byte unchanged",
        "over-bound quarter: stop; do not silently truncate",
        "Every observed class exactly once",
        "No ticker-only or fuzzy mapping",
        "owner-only, mode `0600`, non-symlinked candidate",
        "never enter Git or logs",
        "class drift, ambiguity, or incomplete set blocks approval and publication",
    )
    assert all(phrase in text for phrase in required)


def test_runbook_requires_protected_secret_and_private_only_approval() -> None:
    text = normalized(read(RUNBOOK))
    required = (
        "`insider-publication-approval`",
        "`SERVICENOW_INSIDER_PUBLICATION_POLICY_JSON`",
        "via stdin from the owner-only candidate",
        "never a workflow input and is never read back",
        "If required reviewer protection is unavailable, stop rather than downgrade.",
        "`APPROVE_SERVICENOW_PUBLIC_INSIDER_POLICY`",
        "source/release identity/dataset/archive/manifest/current-policy/generation/candidate digests",
        "No SEC fetch, Pages permission, materializer, commit, push, deletion, or blind replay.",
        "no-op creates no replacement snapshot",
        "private-only approval",
    )
    assert all(phrase in text for phrase in required)


def test_runbook_keeps_public_materialization_manual_and_privacy_screened() -> None:
    text = normalized(read(RUNBOOK))
    required = (
        "manual `update-data.yml` dispatch only",
        "exact complete ServiceNow-bound checkpoint",
        "`publish_insider_publication=true`",
        "canonical UTC timestamps",
        "Schedules cannot choose publication.",
        "exact private and public identities and nonzero payloads",
        "reporting-owner CIKs, internal owner IDs, addresses/contact information",
        "stable/private correlators, class keys, raw narratives",
        "screened display name, relationship, and title remain permitted",
    )
    assert all(phrase in text for phrase in required)


def test_runbook_scopes_guarded_public_replacement_and_privacy_semantically() -> None:
    raw = read(RUNBOOK)
    materialization = section(
        raw,
        "## Separate manual public materialization",
        "## Exact readback and privacy verification",
    )
    privacy = section(
        raw,
        "## Exact readback and privacy verification",
        "## Fail-closed recovery",
    )
    approval = section(
        raw,
        "## Private policy-approval dispatch",
        "## Separate manual public materialization",
    )

    assert "atomic public write" not in materialization
    assert "guarded public-tree replacement" in materialization
    assert "exact post-operation readback" in materialization.lower()
    assert "owner identifiers" not in privacy
    assert "screened display name, relationship, and title remain permitted" in privacy
    assert "reporting-owner CIKs" in privacy
    assert "internal owner IDs" in privacy
    assert "No SEC fetch, Pages permission, materializer" in approval
    assert "approval changes only private `publication-policy-v1`" in approval
    assert "must leave public bytes unchanged" in approval


def test_runbook_recovery_retains_last_known_good_and_forbids_blind_replay() -> None:
    text = normalized(read(RUNBOOK))
    required = (
        "stale state: stop and recapture",
        "incomplete checkpoint: resume only the exact resumable checkpoint",
        "retain the last-known-good public output",
        "repeat mapping approval",
        "reconcile exact release/deployment IDs",
        "Never blindly replay.",
        "Never delete a release or tag.",
        "Scheduled private ingestion is optional and separately authorized.",
        "Public materialization remains manual and default-off indefinitely.",
    )
    assert all(phrase in text for phrase in required)


def test_readme_architecture_and_prd_document_current_boundaries() -> None:
    readme = read(ROOT / "README.md")
    architecture = read(ROOT / "ARCHITECTURE.md")
    prd = read(ROOT / "13f-insider-activity-prd/13F_INSIDER_ACTIVITY_PRD.md")

    assert "latest two validated snapshots" not in readme
    assert "preserved for explicit manual reconciliation" in readme
    assert "sole hosted private-ingestion approval path" in readme
    assert "approve-servicenow-insider-publication.yml" in readme
    assert "docs/servicenow-insider-launch.md" in readme
    assert "0001373715" in readme

    assert "approve-servicenow-insider-publication.yml" in architecture
    assert "publication-policy-v1" in architecture
    assert "manual and default-off" in architecture
    assert "docs/servicenow-insider-launch.md" in architecture

    override = "## Current production privacy and topology override"
    assert prd.count(override) == 1
    override_text = normalized(
        prd[prd.index(override) : prd.index("\n---", prd.index(override))]
    )
    for phrase in (
        "supersedes conflicting historical public drawer, database, and API requirements",
        "screened static payload",
        "SEC source link is the complete-record path",
        "owner CIKs",
        "full footnotes",
        "remarks",
        "raw narratives",
        "private provenance",
        "excluded from public payloads",
    ):
        assert phrase in override_text
