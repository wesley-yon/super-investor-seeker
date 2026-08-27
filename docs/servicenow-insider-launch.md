# ServiceNow insider launch and recovery runbook

## Scope and non-authority

This runbook is procedural, not an authority grant. It applies to ServiceNow
CIK `0001373715` only. It does not authorize a local or hosted mutation, does
not authorize public data, and never changes the fixed issuer scope.

Obtain the required authorization immediately before the relevant operation;
do not infer it from an earlier authorization or from a successful prior run.
Never expose or read back credentials.

## Separate private authorities

There are two independent private-state transitions:

- `approved-issuers-v1` through
  `approve-servicenow-insider-ingestion.yml` is private-ingestion authority. It
  permits only bounded private ingestion for CIK `0001373715`.
- `publication-policy-v1` through
  `approve-servicenow-insider-publication.yml` is exact mapping approval. It
  permits only the reviewed mapping transition for the same issuer.

Neither authority materializes or deploys public data. Ingestion approval does
not approve a mapping or publication; mapping approval does not fetch SEC data,
materialize the public tree, or deploy Pages.

## Permission gates

Pause and obtain explicit authorization before downloading any SEC quarterly ZIP
or artifact. Obtain explicit authorization before workflow dispatch, including a
private-only dispatch. Obtain explicit authorization before creating or updating
a GitHub Environment or secret. Obtain explicit authorization before commit,
push, PR, or merge. Obtain explicit first-publication authorization before the
first screened ServiceNow public generation.

Credentials and secrets are supplied only through their protected mechanism.
Do not print them, put them in a workflow input, copy them to Git, or read them
back for verification.

## Metadata-only preflight

Immediately before every external mutation phase, capture metadata only and bind
it to the execution record:

1. exact `main` SHA and no competing snapshot/deployment mutation;
2. positive immutable private release ID and canonical release identity: tag,
   title, notes, draft/prerelease, and asset state;
3. dataset, archive, and manifest identities and their expected digests;
4. approved issuer, policy, issuer generation, and public baseline, including
   the public insider tree/manifest identity and payload count.

No raw private corpus or owner data. Do not open, log, or place in the preflight
raw filings, owner details, raw XML, signatures, addresses, contact information,
private class keys, candidate bytes, or private source paths. If an identity is
stale, missing, noncanonical, or inconsistent, stop and recapture the full
metadata-only preflight.

## Private-only chronological backfill

After the ZIP/artifact and workflow-dispatch gates, identify exact available
quarters from the SEC catalog at execution time. Process exact available quarters
chronologically, one at a time. Each manual maintenance dispatch must set
`publish_insider_publication=false` and the reviewed bound and deadline.

For each quarter, record only bounded metadata: available-quarter evidence,
accession/form/class/ambiguity counts, issuer generation digest, checkpoint
identity, release identity, and public baseline. Require the exact completed
checkpoint before treating a quarter as complete, and verify the public insider
subtree byte-for-byte unchanged after each private run.

For an over-bound quarter: stop; do not silently truncate. Do not claim partial
coverage is complete. Design and independently review a deterministic bounded
continuation protocol before resuming. An ambiguity or a new security class also
blocks approval and publication; ingestion may not be used to bypass that block.

## Mapping review and private candidate

Restore the exact private release into an isolated review workspace and bind it
to the captured release, dataset, archive, and manifest identities. Review
metadata only. Every observed class exactly once must map to one validated current
public identity. Require complete class-set equality, exact identity fields, and
zero unresolved ambiguities.

No ticker-only or fuzzy mapping is permitted. Do not collapse classes without
explicit contract evidence. A mapping target must exist in the validated public
index. Any class drift, ambiguity, or incomplete set blocks approval and
publication.

Create mapping and candidate files only as owner-only, mode `0600`,
non-symlinked candidate files in the approved review directory. Candidate bytes
and private class keys never enter Git or logs. Logs contain only the issuer,
bounded counts, and approved digests; they contain no candidate bytes, owner CIK,
owner name, address/contact information, raw narrative, raw XML, signature, or
private path. Regenerate and independently review the candidate if the issuer
generation, class set, public index identity, or mapping evidence changes.

## Protected Environment and secret

After a separate Environment/secret authorization, configure the protected
`insider-publication-approval` Environment for `main` and required reviewer
protection. If required reviewer protection is unavailable, stop rather than
downgrade. Use a separately reviewed private handoff; never downgrade to a
repository file or public workflow input.

Set `SERVICENOW_INSIDER_PUBLICATION_POLICY_JSON` via stdin from the owner-only
candidate. The candidate is never a workflow input and is never read back. Verify
only that the reviewed local candidate digest remains unchanged and that the
Environment scope/protection is correct; do not retrieve secret bytes.

## Private policy-approval dispatch

After a fresh preflight and explicit dispatch authorization, dispatch the
private-only approval with all inputs bound to exact
source/release identity/dataset/archive/manifest/current-policy/generation/candidate
digests, a positive immutable release ID, and confirmation
`APPROVE_SERVICENOW_PUBLIC_INSIDER_POLICY`. Require exact current `main`, exact
newest private release metadata/assets, canonical deny-all/current policy
identity, issuer generation identity, and candidate digest before secret use or
mutation.

No SEC fetch, Pages permission, materializer, commit, push, deletion, or blind
replay. The approval changes only private `publication-policy-v1`; it must leave
public bytes unchanged. Read back the exact private release/archive/manifest,
issuer-policy generation and approved issuer identities, then read back the
public tree baseline. A repeated exact approved candidate is a verified no-op:
no-op creates no replacement snapshot.

## Separate manual public materialization

The first public generation needs a separate explicit authorization after policy
approval and a fresh private/public preflight. Use manual `update-data.yml`
dispatch only with an exact complete ServiceNow-bound checkpoint,
`publish_insider_publication=true`, and canonical UTC timestamps for
`insider_publication_as_of` and
`insider_publication_latest_successful_sync_at`. Do not use an empty unbound
incremental checkpoint, local/approximate time, or `now`.

Schedules cannot choose publication. Push, merge, and scheduled events cannot
select this gate. The materializer must use the reviewed policy and the exact
completed checkpoint, validate complete mappings before a guarded public-tree
replacement, and leave unrelated processing separate from the insider
publication claim. Exact post-operation readback, not an atomicity assumption,
establishes the outcome.

## Exact readback and privacy verification

Verify exact private and public identities and nonzero payloads after a completed
first publication. Private readback must reconcile exact source SHA, immutable
release ID, tag/title/assets, dataset, archive and manifest digests, approved
issuer state, policy digest, issuer generation, checkpoint, and finalizer/deploy
marker. Public readback must reconcile the deployment/manifest/tree identity,
nonzero ServiceNow security and filing payloads, closed schemas, and exact SEC
source links.

Perform a public privacy scan. Public payloads must not contain reporting-owner
CIKs, internal owner IDs, addresses/contact information, stable/private
correlators, class keys, raw narratives, footnotes, remarks, source paths, or
private hashes. The approved screened display name, relationship, and title
remain permitted. Public payloads must not contain signatures, raw source bytes,
or private corpus provenance. The SEC link is the complete-record path; the
public static payload is a screened projection.

## Fail-closed recovery

For stale state: stop and recapture the complete metadata-only preflight. Do not
reuse a stale `main`, release, asset, policy, issuer generation, candidate, or
public baseline identity.

For an incomplete checkpoint: resume only the exact resumable checkpoint after
revalidating its scope and identities. A completed exact checkpoint may skip/no-op
only after the preceding outcome is reconciled.

For a new class, mapping drift, or ambiguity: retain the last-known-good public
output, repeat mapping approval, and do not materialize until every observed
class is exactly and completely reapproved.

For an ambiguous private release, publication, or deployment outcome: stop and
reconcile exact release/deployment IDs, assets, manifests, and markers by
readback. Never blindly replay. Never delete a release or tag. Preserve drafts,
releases, tags, and ambiguous evidence for explicit manual reconciliation rather
than attempting destructive rollback.

## Ongoing operating boundary

Scheduled private ingestion is optional and separately authorized. It can refresh
only the fixed private ServiceNow scope and must preserve the last approved public
generation until a later deliberate decision.

Public materialization remains manual and default-off indefinitely. Each new
class or changed mapping evidence requires the full private mapping rotation:
metadata-only review, owner-only candidate, fresh digest, protected policy
approval, exact checkpoint review, separate authorization, and optional manual
publication.
