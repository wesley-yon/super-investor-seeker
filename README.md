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

Section 16 maintenance and public materialization have separate opt-in
boundaries. Scheduled insider ingestion is disabled unless its dedicated
repository variables enable one bounded issuer scope. Public insider
materialization is stricter: only a manual `workflow_dispatch` can set
`publish_insider_publication=true`, and it must also supply an exact completed
maintenance scope plus explicit UTC `as_of` and latest-sync values. Merge,
push, and scheduled events cannot select that publication gate.

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
Ingestion approval alone does not authorize public publication, the materializer
performs no network fetch, backfill, or reparse, and the production browser
remains fixture-backed until the separately reviewed Phase 5 UI integration.

Automation stores the App client ID in the repository variable
`DATA_ARCHIVE_APP_CLIENT_ID` and its private key in the repository secret
`DATA_ARCHIVE_APP_PRIVATE_KEY`. Long maintenance jobs mint a fresh,
write-scoped token only when they are ready to publish. The weekly full
OpenFIGI refresh fails closed if any authenticated batch is incomplete or
malformed; routine weekday resolution remains retrying and best-effort so a
transient mapping outage does not block SEC filing updates.
