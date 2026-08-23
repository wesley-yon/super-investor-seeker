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
write-scoped token only when they are ready to publish. The weekly full
OpenFIGI refresh fails closed if any authenticated batch is incomplete or
malformed; routine weekday resolution remains retrying and best-effort so a
transient mapping outage does not block SEC filing updates.
