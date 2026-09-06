# Finish GitHub workflow security setup

The workflow changes are prepared in code. The repository owner must configure
the environments below and remove the repository-level production App key before
the branch credential boundary is enforced. A job-level `main` check prevents an
accidental dispatch from another branch; a repository writer can edit that check.
Environment branch rules protect the key independently of branch workflow edits.
[GitHub security guidance](https://docs.github.com/en/actions/reference/security/secure-use#use-secrets-for-sensitive-information)

## 1. Configure environments before merging

Open **wesley-yon/super-investor-seeker → Settings → Environments**. Create or
update these environments. For each, choose **Selected branches and tags**, add
a **Branch** rule with the exact name below, and remove broader branch/tag rules.
Do not choose **Protected branches only**: if there are no branch protections,
that option permits every branch. Leave required reviewers and wait timers off
for these unattended workflows. Preserve any other existing protection that is
compatible with unattended operation.
[Environment configuration](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments)

| Environment | Allowed branch | Jobs using it |
|---|---|---|
| `private-data` | `main` | Routine updates, registry rebuilds, Pages resolve/build and private snapshot finalization |
| `github-pages` | `main` | Existing Pages deployment job |
| `private-data-readonly` | `validation/sec-incremental-candidate-20260906` | Candidate verification only |

The candidate environment is needed only if candidate verification will run.
Keep it limited to the exact reviewed candidate branch; update its rule together
with the workflow when starting a later candidate. Do not add `main` or wildcard
branches merely to bypass an environment rejection.

## 2. Install environment credentials privately

Use your existing secure key storage to enter the values directly in GitHub.
Existing GitHub secret values cannot be read back from the settings page. Do not
put private keys in an issue, pull request, run log, or chat.

| Location | Type and name | Value |
|---|---|---|
| Repository variables | `DATA_ARCHIVE_APP_CLIENT_ID` | Keep the existing production App client ID |
| `private-data` | Secret `DATA_ARCHIVE_APP_PRIVATE_KEY` | Existing production App private key |
| `github-pages` | Secret `DATA_ARCHIVE_APP_PRIVATE_KEY` | Same production App private key; the deploy job requests a read-only installation token |
| `private-data-readonly` | Variable `SIS_READER_APP_ID` | `4844056`, the existing backup reader App ID |
| `private-data-readonly` | Secret `SIS_READER_APP_PRIVATE_KEY` | Private key for that reader App, not the production App |
| Repository secrets | `SEC_USER_AGENT` | Keep the existing SEC contact value |

For App `4844056`, confirm the existing installation can read
`super-investor-seeker-data` and its **Contents** permission is **Read-only**.
Do not broaden its permissions or remove repositories needed by backups.
Candidate token requests are further restricted to that data repository and
`Contents: read`. If the original reader key is unavailable, an additional key
for the same reader App can be installed privately; do not revoke an existing
backup key as part of this migration.

The pinned token action accepts this numeric ID through its supported `app-id`
input. Candidate verification deliberately has no fallback to the production
App key. Until the reader environment is configured, that optional job fails
closed. [Pinned action inputs](https://github.com/actions/create-github-app-token/blob/bcd2ba49218906704ab6c1aa796996da409d3eb1/action.yml)

## 3. Merge, verify, then remove the repository duplicate

After the environment setup and PR checks pass, merge the workflow changes.
Confirm a main-branch update and Pages workflow can authenticate using their
environments. Keeping the repository secret briefly during this transition
preserves compatibility with an older in-flight workflow, but the credential
boundary is not complete while that duplicate exists.

Once older in-flight workflows have finished and the new main workflows have
passed, open **Settings → Secrets and variables → Actions → Repository secrets**
and delete only the repository-level `DATA_ARCHIVE_APP_PRIVATE_KEY` entry.
Keep its environment entries and the existing `SEC_USER_AGENT` entry. Do not
revoke the App key or alter the backup repository's secrets. Ensure there is no
organization-level copy of the same production key available to this repository.

Verify another main-branch update/Pages run after removing the duplicate. Check
the environment settings again: `private-data` and `github-pages` must each allow
only the `main` branch, and the production key must appear only in those
environments. A reference to an environment in YAML alone does not create its
protection rules. [Branch and secret rules](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments)

## 4. Require the PR check on main

Under **Settings → Rules → Rulesets** (or **Branches → Branch protection**), create
or update the rule targeting `main`. Require a pull request and the `unit-tests`
check from the **Test** workflow, using the check name shown by a completed PR
run. Select **GitHub Actions** as the expected source of that check. Require the
branch to be up to date before merging. Restrict direct bypass
and leave force pushes and branch deletion disabled. Run the PR check once
before selecting it if GitHub does not yet list it.

The check runs lint, compilation, Python/Node regression tests and generated-data
privacy checks. Selecting it as required is an owner setting; a passing workflow
does not by itself prevent an unchecked merge. These instructions apply to the
public source repository. Private-repository protection remains subject to the
owner's GitHub plan. [Required status checks](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches#require-status-checks-before-merging)

## 5. Require pinned Actions

After this PR is merged and its workflows have passed, open **Settings → Actions
→ General**. All external actions in these workflows are official `actions/*`
actions and are now pinned to full commit SHAs. Keep **Allow actions created by
GitHub** enabled within the existing selected-actions policy, and enable
**Require actions to be pinned to a full-length commit SHA**. There is no need to
allow all Marketplace actions. Weekly Dependabot PRs maintain the hashes and
Python requirements; merge them through the same required checks.
[Allowed actions and pinning](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository)

## Private data repository limitation

The September 6 read-only check still reports private-repository rules as
restricted by the owner's GitHub plan. This source PR cannot add those owner
protections or replace them with an equivalent YAML check. Keep the data
repository private. Enabling private branch rules would require the owner to
review plan support; no plan or billing change is part of this task. Existing
exact-snapshot checks, atomic publication and separate backups remain useful,
but do not prevent a repository writer from changing private repository state.

## Cache storage remains deliberately limited

The public source repository's Actions cache is readable by fork PR workflows.
Only public filing-level SEC reconstruction data, checksums and compact discovery
journals may be cached. Private normalized master/state, final generated data,
credentials and snapshot archives remain excluded. Removing the cache without
replacement would lose progress from interrupted multi-run rebuilds.
[GitHub cache access rules](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)

No Cloudflare setting is changed by these steps.
