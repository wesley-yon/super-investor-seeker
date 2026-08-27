# Super Investor Seeker

Super Investor Seeker builds and validates a public, searchable view of SEC
13F holdings. Source code lives in this repository; the complete generated
corpus and durable pipeline state live as authenticated snapshots in the
private `YOUR_GITHUB_OWNER/super-investor-seeker-data` repository.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the pipeline, data, website, and
deployment contracts.

## Local setup

Python 3.11 or newer is required. The machine-readable runtime contract is in
`pyproject.toml`; automation and reproducible local setup install the checked-in,
hash-locked dependency sets.

```bash
python3.11 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements.lock
```

For linting and the complete development test environment, install
`requirements-dev.lock` instead. The range-based `requirements*.txt` files are
the human-maintained inputs used to refresh those lockfiles with `uv pip
compile`; they are not automation install targets.

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
security labels, individually compressed fund and stock payloads, and—when a
validated public insider generation is present—its manifest plus individually
compressed security and filing projections. Public insider owners are limited
to a screened name as filed and normalized company relationship/title; owner
addresses, contacts, reporting-owner CIKs, stable owner identifiers, raw filing
content, and private provenance are excluded. Website payloads are public and
may be enumerated or scraped; there is no long-lived public bulk archive.
GitHub Pages requires a temporary deployment artifact, so automation deletes it
immediately after the exact live deployment and private rollback marker are
verified, and scheduled recovery removes any artifact left by an interrupted
finalization.

## Insider activity website

The `#stock/<stock-id>/insiders` and
`#stock/<stock-id>/reporting-insiders` views load only bounded, validated,
same-origin public payloads under `data/insiders/public/`. The browser accepts
the exact site contract 5 / insider public contract 1 shape, rejects unknown
fields and inconsistent filing references, and caps a security response at
5 MB and a filing detail at 1 MB. A missing security payload produces an
explicit empty state. Network, size, decoding, or contract failures produce a
generic error and never fall back to illustrative data.

The local-only `?insiderPreview=fixture` flag remains available for the bundled
illustrative APGE preview and deterministic screenshots; deployed hosts ignore
it. Production filing detail is fetched only from the current validated
security payload's canonical same-origin reference and must match its declared
byte count and SHA-256 digest. The drawer renders only the already published,
privacy-screened owner, transaction, holding, and SEC-source fields.

Client filters and sorting operate on the complete bounded security payload.
The transaction table renders at most 100 rows per URL-backed page while the
summary and transaction timeline continue to use the full filtered set. No
daily market-price, split, or currency provider is approved. Production
therefore renders an explicit transaction-only timeline using prices reported
in each SEC transaction when present; it draws no daily price line, fabricates
no missing price, and makes no browser-to-provider call.

## Automation

Scheduled workflows obtain a short-lived token from the repository-scoped
GitHub App, restore the newest private snapshot, run the pipeline and complete
corpus validation, run the full Python and browser-loader regression suites,
publish a replacement snapshot only when content changes, and deploy that
exact dataset to Pages. Critical schedules run away from the top of the hour;
a twice-monthly empty commit on the dedicated `automation-keepalive` branch
keeps GitHub from disabling inactive public-repository schedules without
changing `main` or triggering deployments. Validated private releases, drafts,
and dataset tags are preserved for explicit manual reconciliation; automatic
release/tag retention cleanup is disabled.

Section 16 maintenance and public materialization have separate opt-in
boundaries. Scheduled insider ingestion is disabled unless its dedicated
repository variables enable one bounded issuer scope. Public insider
materialization is stricter: only a manual `workflow_dispatch` can set
`publish_insider_publication=true`, and it must also supply an exact completed
maintenance scope plus explicit UTC `as_of` and latest-sync values. Merge,
push, and scheduled events cannot select that publication gate.

A legacy private snapshot that predates Section 16 authority state can be
prepared only through the manual
`.github/workflows/initialize-empty-private-insider-authority.yml` genesis
boundary. It pins exact current `main` and the newest private dataset, and it
accepts only missing or byte-equivalent empty `approved-issuers-v1` and
`publication-policy-v1` roots. The resulting private-only snapshot approves no
ingestion issuer, authorizes no public issuer, and must reproduce the same
public artifact tree. An empty publication policy is valid durable deny-all
state, but the public materializer still rejects it because there is no reviewed
issuer corpus to publish.

The fixed-scope `.github/workflows/approve-servicenow-insider-ingestion.yml`
is the sole hosted private-ingestion approval path for ServiceNow issuer CIK
`0001373715`. It requires an exact private dataset ID and explicit confirmation,
then invokes `scripts/approve_insider_issuer.py` through a compare-and-swap
update and publishes a validated private-only snapshot. The workflow keeps
`publication-policy-v1` unchanged, verifies that the public artifact is
unchanged, and has no Pages deployment or public-materialization step. This
approval permits only a later bounded private ingestion run; it does not make
ServiceNow data public.

Publication authority is separate: the fixed-scope
`.github/workflows/approve-servicenow-insider-publication.yml` approves only an
exact reviewed mapping candidate through the protected
`insider-publication-approval` Environment. It is private-only and cannot fetch
SEC data, materialize public payloads, or deploy Pages. The later public
materialization gate remains manual and default-off; follow the
[ServiceNow insider launch runbook](docs/servicenow-insider-launch.md) for the
required authorization, identity binding, and recovery sequence.

Incremental v1 checkpoints carry their durable issuer scope only in queued
accessions. A completed empty incremental checkpoint remains a valid ingestion
no-op, but the publication adapter rejects it because it cannot prove which
issuer was maintained; an operator must use an exact nonempty incremental,
backfill, or issuer-scoped reparse completion before materialization.

The offline materializer reads canonical normalized records by the exact SHA-256
recorded in issuer state, without reparsing the stored filing index or ownership
XML. It then rebuilds every issuer named in the private, reviewed
`publication-policy-v1` allowlist, verifies complete security class mappings,
and admits no more than 15,000 normalized filings or 250 MB of canonical
normalized input across the complete run. It combines the complete policy corpus
in memory and replaces the public insider tree once through the journaled writer.
Ingestion approval alone does not authorize public publication, and the
materializer performs no network fetch, backfill, or reparse. The live browser
adapter remains read-only and cannot activate ingestion or materialization.

Automation stores the App client ID in the repository variable
`DATA_ARCHIVE_APP_CLIENT_ID` and its private key in the repository secret
`DATA_ARCHIVE_APP_PRIVATE_KEY`. Long maintenance jobs mint a fresh,
write-scoped token only when they are ready to publish. The weekly full
OpenFIGI refresh fails closed if any authenticated batch is incomplete or
malformed; routine weekday resolution remains retrying and best-effort so a
transient mapping outage does not block SEC filing updates.
