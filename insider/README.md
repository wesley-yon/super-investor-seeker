# SEC ownership archive tools

These standard-library tools collect and preserve complete SEC Forms 3, 3/A, 4, 4/A, 5, and 5/A. The one-time market-wide backfill is running in a separate local workspace. Future daily collection belongs in GitHub Actions; no local scheduler is installed.

The source and synthetic tests originate from the separately validated backfill implementation, local commit `8ac5d12`. No collected filing data, inventory database, source archives, or private download is included in this code directory.

The inventory and document archives are separate, immutable components. A combined checkpoint binds their exact scope, complete committed-document membership, and inventory checksum. Restoration preserves the complete queue and checks every original and normalized document. Source audit findings distinguish exact SEC bulk matches, decimal rounding, and other source differences; original XML values remain authoritative and unchanged.

## Cloud verification

`Verify private insider checkpoint` is a manual workflow that runs only trusted `main` code in the existing protected `private-data` environment. It requests a Contents-read token limited to the private data repository. It downloads an explicit published `insider-*` release, requires a separately pinned baseline SHA-256, restores the combined checkpoint, and reproduces its source audit. It does not fetch SEC filings, publish releases, upload raw data as public Actions artifacts, deploy the site, or schedule daily collection.

Draft releases are restricted to users with push access, so a checkpoint must be published privately before the read-only cloud verification. When publishing an insider checkpoint, explicitly keep `make_latest=false`; the existing site's latest dataset release must remain selected. A preflight checkpoint may be a prerelease and still be read by this explicit-tag verifier.

The verifier has a 2 GB download limit and requires 10 GiB free disk before starting. This bounds the initial cloud transport test. Routine maintenance for the completed historical corpus must restore only its inventory and needed incremental data; it must not assume the entire history fits a standard runner. Daily maintenance is not implemented or activated by this verification workflow.

```sh
python -m unittest discover -s tests -q
python -m insider_pipeline.github_read --tag insider-CHECKPOINT --baseline-sha256 PINNED_HASH --output /new/checkpoint-directory
```

Originals, normalized rows, state, and generated evidence belong in private storage. A verified checkpoint of collected documents is not a completed-backfill claim while pending filings or unresolved source issues remain.
