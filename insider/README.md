# SEC ownership archive tools

These standard-library tools collect and preserve complete SEC Forms 3, 3/A, 4, 4/A, 5, and 5/A. The one-time market-wide backfill is running in a separate local workspace. Future daily collection belongs in GitHub Actions; no local scheduler is installed.

The source and synthetic tests originate from the separately validated backfill implementation, local commit `8ac5d12`. No collected filing data, inventory database, source archives, or private download is included in this code directory.

The inventory and document archives are separate, immutable components. A combined checkpoint binds their exact scope, complete committed-document membership, and inventory checksum. Restoration preserves the complete queue and checks every original and normalized document. Source audit findings distinguish exact SEC bulk matches, decimal rounding, and other source differences; original XML values remain authoritative and unchanged.

## Cloud verification

`Verify private insider checkpoint` is a manual workflow that runs only trusted `main` code in the existing protected `private-data` environment. It requests a Contents-read token limited to the private data repository. It downloads an explicit published `insider-*` release, requires a separately pinned baseline SHA-256, restores the combined checkpoint, and reproduces its source audit. It does not fetch SEC filings, publish releases, upload raw data as public Actions artifacts, deploy the site, or schedule daily collection.

Draft releases are restricted to users with push access, so a checkpoint must be published privately before the read-only cloud verification. When publishing an insider checkpoint, explicitly keep `make_latest=false`; the existing site's latest dataset release must remain selected. A preflight checkpoint may be a prerelease and still be read by this explicit-tag verifier.

The verifier has a 2 GB download limit and requires 10 GiB free disk before starting. This bounds the initial cloud transport test. Routine maintenance for the completed historical corpus must restore only its inventory and needed incremental data; it must not assume the entire history fits a standard runner. Daily maintenance is not implemented or activated by this verification workflow.

The optional `inventory_only` workflow input, or `--inventory-only` CLI flag, restores just the inventory component. It still checks the pinned baseline, private release asset metadata, inventory manifest, exact restored database bytes, and complete queue membership. The download budget applies to the selected inventory assets, allowing historical document archives to be larger. It also checks free disk against the inventory's declared uncompressed size. This mode excludes filing documents, source ZIPs, and original-source auditing; its report explicitly leaves full recovery and collection-resume readiness unverified.

```sh
python -m unittest discover -s tests -q
python -m insider_pipeline.github_read --tag insider-CHECKPOINT --baseline-sha256 PINNED_HASH --output /new/checkpoint-directory
python -m insider_pipeline.github_read --tag insider-CHECKPOINT --baseline-sha256 PINNED_HASH --inventory-only --output /new/inventory-directory
```

Originals, normalized rows, state, and generated evidence belong in private storage. A verified checkpoint of collected documents is not a completed-backfill claim while pending filings or unresolved source issues remain.

## Inventory changes

`inventory_delta` stores row changes between consistent SQLite states. Each manifest binds the schema and every row of every inventory table in both the parent and the target. Changes include inserts, updates, deletions, binary metadata, source observations, and retry state. The compressed stream uses bounded, checksummed parts. Neither input is modified, and a fresh output directory is required.

Restoration copies the exact logical parent into a new directory, validates each changed row against its expected old value, then checks every reconstructed row and SQLite integrity before publishing the directory. Missing, repeated, reordered, or corrupted changes cannot pass. Reconstructed database files may have different SQLite page layouts; their complete logical rows and BLOB values must match. Schema changes require a new full baseline. Chained deltas require each preceding state and a separately pinned manifest checksum.

```sh
python -m insider_pipeline.inventory_delta --parent /frozen/parent.sqlite3 --target /frozen/target.sqlite3 --output /new/delta
python -m insider_pipeline.inventory_delta --parent /restored/parent/inventory.sqlite3 --restore-from /downloaded/delta --expected-manifest-sha256 PINNED_HASH --output /new/reconstructed
```

This supports small inventory updates without uploading a full database every day. It does not include filing documents or source ZIPs and cannot establish collection-resume readiness on its own. Cloud orchestration must restore the baseline inventory plus its ordered deltas, retain the corresponding immutable document/source archives, and verify the combined checkpoint before activation. Daily cloud maintenance remains unfinished.

A September 10 local check reconstructed the full 1,766,349-filing inventory after 10,461 changed rows. Independent tuple comparison matched all 5,298,976 rows across its six tables, including binary values. The change assets were 3,982,248 bytes; a full gzip copy of the same target database was 636,508,397 bytes. Creation took 66.815 seconds and restoration took 65.660 seconds, with the latter running alongside the independent full-compression size measurement. Peak process memory was 45.61 MiB. These measurements cover inventory transport and reconstruction, not filing-document recovery or daily cloud execution.

## Incremental checkpoints

`increment` pairs a complete inventory delta with exactly the changed committed document rows. The source audit selects from frozen parent and target databases, comparing every inventory column. Pending and inflight rows remain queue state. Metadata changes to an already collected filing also require a new document envelope. An independent pass over the complete delta derives the required document membership and checks it against the package and source-audit evidence; a smaller audited sample cannot pass as the complete increment.

Unchanged documents and source files remain in the pinned parent checkpoint. New or changed SEC index and quarterly-source bytes are included once. Retry-only updates can have an empty document package. Each increment names its parent's manifest checksum, so later increments can extend it without uploading another full inventory or unchanged historical documents.

Full restoration rebuilds the parent chain from the pinned archives, applies the inventory changes, and materializes the complete target collection. It verifies every inherited and changed document, exact inventory/document rows including compressed BLOBs, complete queue state, and all current source files. It does not accept an arbitrary existing state directory as proof of the parent. Each needed ancestor manifest must be supplied. Full restoration accepts up to 64 increments; longer chains require compaction. It reads historical documents and is separate from selective daily processing.

```sh
python -m insider_pipeline.increment --root state --parent-manifest /parent/baseline.json --parent-sha256 PARENT_PIN --parent-inventory /frozen/parent.sqlite3 --inventory-snapshot /frozen/target.sqlite3 --output /new/increment --workers 4
python -m insider_pipeline.increment --restore-from /new/increment --parent-manifest /parent/baseline.json --expected-manifest-sha256 INCREMENT_PIN --output /new/restored
# For a later increment, pass its immediate parent and the older manifests:
python -m insider_pipeline.increment --restore-from /next/increment --parent-manifest /previous/increment.json --ancestor-manifest /parent/baseline.json --expected-manifest-sha256 NEXT_PIN --output /new/restored-chain
```

Use the command-line module entry point for multiprocessing on macOS. Existing output directories are refused. Failed builds retain their intermediate audit evidence and a `failure.json` file in the reported working directory; they do not publish the requested checkpoint path. Originals remain intact. A successful increment preserves the collected checkpoint and pending work; it does not finish the market-wide backfill. Cloud publication, scheduled discovery and collection, and automatic chain resolution remain separate integration work.

A September 10 local round trip extended the 9,551-document checkpoint to 20,008 documents with eight new assets totaling 69,886,801 bytes. It added 10,457 filings and the full inventory delta, inheriting all 69 unchanged source files. Complete restoration from the pinned parent and increment passed in 104.52 seconds. Independent comparison matched all 20,008 complete document rows, including compressed BLOBs, and the full inventory state matched its target. The changed-document source audit reproduced its archived digest across 2,286,075 checks; a separate full audit passed 4,199,486 checks. Its bulk comparison still recorded 1,262 non-rounding differences for review. This increment remained local during validation; it did not activate daily cloud maintenance.
