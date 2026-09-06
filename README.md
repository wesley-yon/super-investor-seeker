# Super Investor Seeker

Super Investor Seeker builds and validates a public, searchable view of SEC
13F holdings. Source code lives in this repository; the complete generated
corpus and durable pipeline state live as authenticated snapshots in the
private `YOUR_GITHUB_OWNER/super-investor-seeker-data` repository.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the pipeline, data, website, and
deployment contracts.

## Local setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Most unit tests use fixtures and do not require the private corpus:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

To run the pipeline, full-corpus validation, or a local Pages build, first
authenticate `gh` with an account that can read the private data repository,
then restore the latest validated snapshot:

```bash
gh auth login --git-protocol https
DATA_ARCHIVE_TOKEN="$(gh auth token)" \
  .venv/bin/python scripts/data_snapshot.py pull \
    --repository YOUR_GITHUB_OWNER/super-investor-seeker-data \
    --root . \
    --replace
```

The restored `data/` and `.cache/` state is intentionally ignored by Git.
Never force-add generated data to this repository.

## Data visibility

The complete archives, pipeline state, registries, reports, and operational
caches are private. GitHub Pages publishes only the browser-facing indexes,
security labels, and individually compressed fund and stock payloads. Those
website payloads are public and may be enumerated or scraped; there is no
long-lived public bulk archive. GitHub Pages requires a temporary deployment
artifact, so automation deletes it immediately after the exact live deployment
and private rollback marker are verified, and scheduled recovery removes any
artifact left by an interrupted finalization.

## Automation

Scheduled workflows obtain a short-lived token from the repository-scoped
GitHub App, restore the newest private snapshot, run the pipeline and complete
corpus validation, run the full Python and browser-loader regression suites,
publish a replacement snapshot only when content changes, and deploy that
exact dataset to Pages. Critical schedules run away from the top of the hour;
a twice-monthly empty commit on the dedicated `automation-keepalive` branch
keeps GitHub from disabling inactive public-repository schedules without
changing `main` or triggering deployments. The latest two validated snapshots
are retained for rollback.

Automation stores the App client ID in the repository variable
`DATA_ARCHIVE_APP_CLIENT_ID` and its private key in the repository secret
`DATA_ARCHIVE_APP_PRIVATE_KEY`. Long maintenance jobs mint a fresh,
write-scoped token only when they are ready to publish. Before any release
mutation, the publisher fails closed unless GitHub's repository API confirms
that the configured target is the exact private data repository. Ticker
resolution uses only SEC-hosted evidence: the official Section 13(f) list
defines the security universe, fails-to-deliver archives provide direct dated
CUSIP-symbol pairs, and SEC issuer/class sources corroborate ambiguous cases.
Only an accepted
fails-to-deliver mapping (`sec_ftd`) or an exact Schedule 13D/G-to-periodic-
filing class bridge (`sec_ixbrl`) can be published as ticker proof; the official
list, company/fund ticker files, and filer descriptions supply validation and
display metadata, not a ticker by themselves. The weekday job incrementally
refreshes this private security master; the weekly job reconstructs the derived
master deterministically and runs the complete provenance audit. Missing or
conflicting evidence stays unresolved instead of
inheriting an issuer's common-stock ticker.

The all-history rebuild understands both SEC archive layouts: quarterly ZIP
bundles from the first actual observation on March 22, 2004 through June 2009,
and half-month ZIPs beginning in July 2009. The SEC's disjoint half-month
ownership is A=calendar days 1-14 and B=day 15 through month-end; every one of
the 411 archives available at cutover obeyed that rule across 23,458,560 raw
rows. The loader rejects a discovered subset unless archive periods are
continuous through the latest mature boundary and binds every settlement date
to its URL-encoded period. One audited SEC source
anomaly is handled explicitly: the file named `2004q1` repeats all April 1 rows
from `2004q2`. The rebuild hashes the complete normalized April 1 row multiset,
including multiplicity and rows outside the active CUSIP filter, and requires
the two archives' row counts and hashes to match before excluding the Q1 copy;
Q2 remains the sole owner of April 1. February, dates before March 22, any other
Q1 spillover, and a missing or unequal boundary proof fail closed. Every later
quarterly archive must cover all three calendar months. Official-list discovery
accepts both SEC filename forms currently
present on the landing page (`13flistYYYYqN.txt` and
`13flistYYYYqN-txt.txt`).
The official list is normalized as a set of complete five-field SEC rows.
Byte-identical or whitespace-equivalent repetitions are collapsed
deterministically, while any same-CUSIP difference in issuer, class, status, or
option marker remains visible as separate evidence.

The first SEC-only rebuild restores immutable `reported_*` holding fields from
the SEC's quarterly Form 13F data sets, with exact accession filing documents as
the fallback for periods the bulk files do not cover. That all-history 13F
verification is a one-time legacy-snapshot cutover unless a clean rebuild is
explicitly requested with `--rebuild-security-master` or the weekly workflow's
`rebuild_security_master` manual input. A clean rebuild starts from isolated
empty SEC state under `.cache/sec-security-master-rebuild-work` and promotes it
only after every publication gate passes. Clean EDGAR discovery commits compact,
hash-chained 100-CUSIP evidence journals, then copies, rebuilds, audits, and
writes the large source-state/master pair once. The workflow may cache only the
manifest and those filing-scoped journals so a cooperative timeout can resume
accepted EDGAR batches; the staged source state and master remain process-local
because they contain the complete normalized official-list input. The separate
checksum-bound Form 13F reconstruction index is also cacheable. Existing v2
evidence cannot seed the rebuild. Later runs
fetch only new or changed security-master sources. Structured EDGAR exception
discovery is limited to unresolved/ambiguous securities on the current official
list, identities first reported during the trailing six months, records with
exact FTD evidence inside the current conflict window, and due iXBRL
revalidations. Historical corpus-only gaps remain tickerless. Candidates are
prioritized with due revalidations and changed/current conflicts first, then
bounded to 50 per incremental run or 250 during a clean rebuild. Terminal results
fall out, so an exceptional backlog drains without manual curation. Source
hashes, terminal no-symbol decisions, retryable diagnostics, and accepted EDGAR
proof
are checkpointed inside the SEC source-state file, so normal maintenance does
not require a manual exception list. Resolved iXBRL mappings are revalidated
every 30 days and cannot publish once their last successful check is more than
45 days old. Stable terminal unresolved results reopen only when exact identity,
exact official-list row, class, or FTD/conflict evidence changes; quarterly list
URL, checksum, and period churn alone does not retry them. The cutover also emits
a local, provider-neutral before/after mapping report that is excluded from private
snapshots and workflow artifact uploads. Its frozen baseline is
comparison-only and cannot seed a ticker. Retained as-filed holding identity is
bound into
composition-hash protocol v3, so later mutation of issuer, class, CUSIP,
optional FIGI, accession, or report date fails validation.

FTD history is stored as an archive inventory plus per-CUSIP, time-versioned
symbol-set intervals. Each interval keeps aggregate dates/counts and only a
source-checksummed first boundary plus the last 32 distinct observation dates;
the resolution window is therefore capped at 31 calendar days. The two newest
archives remain reversible until they leave the mutable tail, so a replaced
recent SEC ZIP can be replayed while a checksum mutation in compacted history
fails closed. Archive coverage points into one append-only CUSIP filter log
instead of duplicating the full universe per archive, and ZIP rows are filtered
and aggregated as a stream. This preserves ticker changes, same-date conflicts,
and ticker reuse without duplicating every settlement date in both the source
state and security master or materializing every decompressed row in memory.

The hosted clean path checks disk headroom before downloading when an identity
backfill is required. Its filtered 13F SQLite builder checkpoints each accepted
quarter to a private, plan-addressed work file, inserts exact-accession
fallbacks into that same candidate, and resumes only when normalized source
URLs, target scope, and parser contract still match. The workflows cache only
the checksum-bound Form 13F checkpoint/index plus its narrow sibling
`.cache/sec_13f_bulk_rebuild_checkpoint.accession-discovery.json`; the
security-master source-state stage is never placed in Actions cache. A
completed bulk index remains reusable until the rest of the hosted cutover has
passed. Fund
files are verified corpus-wide first and then rewritten atomically one file at a
time, making an interrupted apply safe to rerun without holding a second copy of
the multi-gigabyte corpus. Each quarter also retains a canonical
`reported_identity_sources` list of exact SEC URLs and SHA-256 checksums, so the
machine-verifiable witness survives deletion of the temporary index.

The fresh hosted peak is the already-restored fund corpus plus one filtered
SQLite candidate, the isolated SEC-master candidate, and one fund-file rewrite
temporary; archive fallbacks never create a second SQLite generation and
snapshot packing never overlaps the bulk index. The preflight conservatively
requires 8 GiB of free headroom for a fresh pass, credits the byte size of a
validated partial index on resume, and always retains at least a 1 GiB floor.
After the entire clean command succeeds, the temporary 13F manifest and SQLite
generation plus any compact EDGAR journals are saved to the private Actions
cache and then the 13F working set is deleted from the runner before validation
and snapshot packing. A failed or cooperatively timed-out command skips deletion,
allowing the `always()` cache-save step to preserve partial work. The large
SEC-master stage is never cached. Neither rebuild checkpoints nor raw SEC
archives enter snapshot v2. A local clean rebuild intentionally leaves this
resumable work set in
`.cache`; after all local follow-up checks succeed it can be removed with
`python -c 'from sec_13f_bulk_backfill import cleanup_13f_bulk_working_set; cleanup_13f_bulk_working_set()'`.

The holdings-preservation digest, filing-chain discovery, pre-apply SEC
verification, and final reported-identity verification use up to six CPU
workers for corpora with at least 32 fund files. Workers read independent
files; the SEC checks use separate read-only connections to the same immutable
SQLite generation. Ordered reduction preserves digests, counts, diagnostics,
and the update manifest. No fund is changed until every pre-apply check passes,
and file updates remain sequential and atomic.
Set `SEC_PIPELINE_WORKERS=1` for serial execution, or a positive integer to
select another worker count (capped at 12). This parallelizes the local digest
and evidence checks; it does not change SEC request pacing or the source
acceptance gates.

Before cutover, save the first successful clean build's master and source-state
files together, run a second independent `--rebuild-security-master`, and prove
that both pairs used identical SEC evidence and produced identical normalized
master output:

```bash
.venv/bin/python verify_security_master_reproducibility.py \
  --first-master /path/to/first/sec_security_master.json \
  --first-source-state /path/to/first/sec_source_state.json \
  --second-master .cache/sec_security_master.json \
  --second-source-state .cache/sec_source_state.json
```

All four arguments are required and must identify distinct regular files. The
offline verifier validates both schemas and exact master/state bindings. It
ignores only validated fetch/check clocks; every SEC URL, checksum, parsed
record, filter boundary, EDGAR decision, and normalized master field must match.
This reproducibility check complements rather than replaces `validate_data.py`.

When live SEC responses change between clean builds, capture the mapping
responses once and replay those exact bytes in a separate empty directory:

```bash
.venv/bin/python scripts/frozen_sec_rebuild.py capture \
  --bundle .cache/frozen-sec/inputs --output .cache/frozen-sec/first
.venv/bin/python scripts/frozen_sec_rebuild.py replay \
  --bundle .cache/frozen-sec/inputs --output .cache/frozen-sec/second
.venv/bin/python verify_security_master_reproducibility.py \
  --first-master .cache/frozen-sec/first/sec_security_master.json \
  --first-source-state .cache/frozen-sec/first/sec_source_state.json \
  --second-master .cache/frozen-sec/second/sec_security_master.json \
  --second-source-state .cache/frozen-sec/second/sec_source_state.json
```

Capture requires runtime-only `SEC_USER_AGENT`. Both builds use the production
mapping stages, including the full historical FTD archive set, series pages,
and bounded EDGAR exceptions, starting without a master, source state, or
exception journal. The reported holding universe is frozen from the separately
validated Form 13F corpus; this mapping check does not repeat that corpus's
independent reconstruction. Replay disables network access and checks every
response's bytes, the complete request inventory, builder code, and input
universe against the sealed bundle. Missing or altered inputs fail the run.
Neither command changes the published dataset or its mapping pair. Keep the
bundle and both build reports privately alongside the strict comparison result.

The only required repository secrets are `SEC_USER_AGENT` and
`DATA_ARCHIVE_APP_PRIVATE_KEY`; the GitHub App client ID remains the
`DATA_ARCHIVE_APP_CLIENT_ID` repository variable. Durable SEC mapping state is
stored privately in `.cache/sec_security_master.json` and
`.cache/sec_source_state.json`.

### Missing holding quantities

Routine ingestion keeps positive SEC-reported quantities unchanged. A positive
position value with a reported zero quantity is not treated as a closed position.
`quantity_estimation.py` uses a verified USD quarter-end closing price when one is
available, then a same-quarter median from at least three other SEC filers. Peer
prices must agree (at least 80% within 1%, or 5% for dollar principal), and zero,
unknown, or estimated quantities cannot contribute. There is no cross-quarter
price fallback. Estimates below one reported unit remain unknown.

Every estimate retains `reported_shares: 0` and a `quantity_estimate` receipt
binding its method, quarter, security, unit, price, and source evidence. CALL and
PUT quantities remain underlying shares with distinct instrument identities;
notes require a verified principal unit, and unsupported securities remain
unknown. Values, actual reported quantities, CUSIPs, and security types are
preserved. The UI marks estimates with `~`, displays unknown quantities as a
dash, and excludes both from exact-share totals and share-based comparisons.

Fiscal.ai prices are an optional separate input; they never resolve CUSIPs or
populate the SEC security master. The connector's split-adjusted close is
converted to the quarter's share basis using documented subsequent forward
splits. Missing session quotes and ambiguous reverse-split or stock-dividend
adjustments are rejected. Routine jobs consume saved validated prices and emit
missing-price requests; unattended Fiscal.ai fetching is not configured.

The local workflow uses the repository Python environment:

```bash
.venv/bin/python scripts/quantity_policy.py plan --output .cache/quantity-plan.json
.venv/bin/python scripts/quantity_policy.py prepare-prices \
  --catalog /path/to/fiscal-company-pages.json \
  --output .cache/fiscal-price-requests.json
# Fetch the requested listings through the Fiscal.ai connector, then import:
.venv/bin/python scripts/quantity_policy.py import-prices \
  --requests .cache/fiscal-price-requests.json \
  --exports /path/to/fiscal-price-exports.json \
  --report .cache/fiscal-price-import-report.json
.venv/bin/python scripts/quantity_policy.py plan --output .cache/quantity-plan.json
.venv/bin/python scripts/quantity_policy.py apply --plan .cache/quantity-plan.json
```

Company exports contain the connector's `companies` pages. Each price export
contains `companyKey`, `fetchedAt`, `listing` (all stock-price response metadata),
`seriesThrough` (the latest returned price date), `prices` (the requested exact
session observations), and `splits` (the full stock-splits response). Keep USD
listing identifiers and dates unchanged. The importer admits only listings
bound to an unambiguous dated SEC symbol and matching SEC issuer evidence.
After a standalone apply, regenerate stock/index outputs before validating or
serving the dataset. Normal and incremental pipeline regeneration already do
this. Automated clean rebuilds explicitly pass `--apply-quantity-policy`; a
manual identity-only clean rebuild leaves quantity maintenance out by default.

The private snapshot preserves `.cache/quantity_estimation_evidence.json`,
`.cache/quarter_close_prices.json`, and `.cache/quarter_close_price_requests.json`
when present. Older snapshots remain readable and remove stale quantity evidence
on restore; estimated holdings without their evidence fail data validation.
