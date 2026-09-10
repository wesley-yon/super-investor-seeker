# SEC ownership archive tools

These standard-library tools collect and preserve complete SEC Forms 3, 3/A, 4, 4/A, 5, and 5/A. The one-time market-wide backfill is running in a separate local workspace. Future daily collection belongs in GitHub Actions; no local scheduler is installed.

The source and synthetic tests originate from the separately validated backfill implementation, local commit `8ac5d12`. No collected filing data, inventory database, source archives, or private download is included in this code directory.

The inventory and document archives are separate, immutable components. A combined checkpoint binds their exact scope, complete committed-document membership, and inventory checksum. Restoration preserves the complete queue and checks every original and normalized document. Source audit findings distinguish exact SEC bulk matches, decimal rounding, and other source differences; original XML values remain authoritative and unchanged.

## Cloud verification

`Verify private insider checkpoint` is a manual workflow that runs only trusted `main` code in the existing protected `private-data` environment. It requests a Contents-read token limited to the private data repository. It downloads an explicit published `insider-*` release, requires a separately pinned baseline or incremental transport SHA-256, restores the combined checkpoint, and reproduces its source audit. It does not fetch SEC filings, publish releases, upload raw data as public Actions artifacts, deploy the site, or schedule daily collection.

Draft releases are restricted to users with push access, so a checkpoint must be published privately before the read-only cloud verification. When publishing an insider checkpoint, explicitly keep `make_latest=false`; the existing site's latest dataset release must remain selected. A preflight checkpoint may be a prerelease and still be read by this explicit-tag verifier.

The verifier has a 2 GB download limit and requires 10 GiB free disk before starting. This bounds the initial cloud transport test. Routine maintenance for the completed historical corpus must restore only its inventory and needed incremental data; it must not assume the entire history fits a standard runner. Daily maintenance is not implemented or activated by this verification workflow.

The optional `inventory_only` workflow input, or `--inventory-only` CLI flag, starts with the inventory component. For a legacy baseline it checks the pinned baseline, private release asset metadata, inventory manifest, exact restored database bytes, and complete queue membership. Incremental chains additionally replay and verify every intervening change, as described below. Optional source and document selections add only their required assets to this mode. The combined download budget applies to the selected assets, allowing the complete historical archive to be larger. Full recovery, original-source auditing, and collection-resume readiness remain unverified by this mode.

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

Use the command-line module entry point for multiprocessing on macOS. Existing output directories are refused. Failed builds retain their intermediate audit evidence and a `failure.json` file in the reported working directory; they do not publish the requested checkpoint path. Originals remain intact. A successful increment preserves the collected checkpoint and pending work; it does not finish the market-wide backfill. Scheduled discovery and collection remain separate integration work.

A September 10 local round trip extended the 9,551-document checkpoint to 20,008 documents with eight new assets totaling 69,886,801 bytes. It added 10,457 filings and the full inventory delta, inheriting all 69 unchanged source files. Complete restoration from the pinned parent and increment passed in 104.52 seconds. Independent comparison matched all 20,008 complete document rows, including compressed BLOBs, and the full inventory state matched its target. The changed-document source audit reproduced its archived digest across 2,286,075 checks; a separate full audit passed 4,199,486 checks. Its bulk comparison still recorded 1,262 non-rounding differences for review. This increment remained local during validation; it did not activate daily cloud maintenance.

## Private incremental transport

`github_increment_stage` uploads incremental archives to an explicit monthly bucket such as `insider-archives-202609-001` in the existing private data repository. Each remote blob is named with its complete SHA-256. A small pinned transport descriptor binds the increment manifest to its exact parent locator. Legacy baseline locators retain their existing release tags and manifest pins; later increments refer to the preceding transport descriptor. No release or asset in the `dataset-*` namespace is reused.

The staging command verifies the local increment and the parent's remote metadata, uploads missing blobs with three workers, and independently downloads and verifies every blob before uploading the descriptor. Repeating the same upload reuses identical assets. Conflicting names, corrupt data, and unrelated bucket assets fail without overwriting anything. New buckets are drafts; existing published buckets must be mutable prereleases. Publishing a new bucket is a separate step that must preserve `--prerelease --latest=false`. Serialize writers to a bucket. Keep every referenced parent and do not delete assets to reclaim capacity: the tool stops at 900 assets so the next checkpoint can use a fresh numbered bucket below GitHub's 1,000-asset limit.

`github_chain` follows independently pinned descriptors, confirms each parent against the increment's own parent checksum, and reconstructs the whole checkpoint from private GitHub assets. It uses read-only access. Downloads are limited to 2 GB across the complete chain, with at most 32 checkpoints and a disk check for the declared inventory and restore copies. It reproduces the latest changed-document audit and separately checks every restored document against its original source. These full-recovery checks read historical documents; selective daily processing still needs integration.

```sh
# parent.json: {"layout":"legacy_baseline","tag":"insider-BASELINE","sha256":"BASELINE_PIN"}
# For a later increment, use the preceding stage report's exact locator object.
python -m insider_pipeline.github_increment_stage --increment /new/increment --increment-sha256 INCREMENT_PIN --parent-locator /private/parent.json --bucket-tag insider-archives-YYYYMM-001 --expected-latest-release-id DATASET_ID --evidence /private/stage-evidence
python -m insider_pipeline.github_chain --tag insider-archives-YYYYMM-001 --transport-sha256 TRANSPORT_PIN --output /new/cloud-restore --workers 4
python -m insider_pipeline.github_chain --tag insider-archives-YYYYMM-001 --transport-sha256 TRANSPORT_PIN --inventory-only --output /new/cloud-inventory
python -m insider_pipeline.github_chain --tag insider-archives-YYYYMM-001 --transport-sha256 TRANSPORT_PIN --inventory-only --source-quarters "2026Q2 2026Q3" --output /new/cloud-source-cache
```

The manual verification workflow accepts exactly one of `baseline_sha256` and `transport_sha256`; `inventory_only` works with either. For an incremental chain, the reader downloads the pinned transport/checkpoint metadata, baseline inventory, and ordered inventory deltas. Optional selections determine whether it also loads source files or filing chunks; audit detail files remain excluded from this mode. Every nested manifest and part must match its checkpoint declaration; each delta must match its exact logical parent, and replay verifies every row in every resulting table. Retry state, binary metadata, discovery observations, and correction-history settings are preserved. The final inventory must also match the target checkpoint's counts and collected-document selection.

The chain budget covers all selected metadata and parts across all ancestors, and it is checked before large parts are downloaded. A separate disk check accounts for the baseline, decoded change sizes, temporary copies, and SQLite journal growth. Intermediate inventories are removed after their successor passes verification. The final report distinguishes the byte-identical baseline from the logically identical result of delta replay; it does not claim the final SQLite file has the original physical page layout. This is inventory-state recovery. Optional selected source loading is described below. Selective collection still needs discovery, collection, audit, and publication orchestration. The code adds no schedule, SEC fetch, site publication, or public data artifact. The backfill and daily cloud maintenance remain unfinished.

## Selected SEC source loading

With `--inventory-only`, `--source-quarters` accepts one to 64 distinct, space-separated `YYYYQ1` through `YYYYQ4` keys. The manual workflow exposes the same option as `source_quarters` and requires an incremental transport pin. Each requested quarter loads the source rows present in the target inventory: its quarterly ZIP, its original index, or both. An index-only current quarter does not imply that a quarterly ZIP exists. A key absent from both source tables is rejected.

The loader binds document manifests and source catalogs to the pinned checkpoint chain. It selects the newest declared source version by source key and original checksum, including replacements in later increments and metadata-only changes that retain older source bytes. It verifies each selected asset's declaration and plans the combined inventory/source download budget and disk requirement before large parts are fetched. Unselected source files and historical filing chunks remain in their archives.

After inventory replay verifies every row, every source-catalog row must match the restored source tables exactly. Selected source files are then downloaded with a bounded worker pool and checked against both their compressed archive identity and original byte length/checksum. Index decompression stops if it exceeds its declared length. The ZIP bytes and index cache metadata retain their recorded provenance. A failed catalog, download, or source verification cannot expose a completed restored root or cloud verification report.

The result records which ZIPs and indexes were restored and a digest of all selected source identities. Source loading alone leaves `includes_original_documents`, `full_restore_verified`, `source_audit_performed`, and `collection_resume_ready` false. Optional original-document loading is described next. Complete daily processing still requires discovery refresh, handling changes to previously collected documents, audit, and publication orchestration. Source loading does not resume the collector, write archives, fetch SEC data, or activate a schedule.

## Indexed document recovery

`document_index` creates a separate immutable lookup index for a pinned incremental checkpoint. It verifies the complete frozen inventory state, reads each ancestor's document chunks, and compares the newest envelope for every committed filing with its entire target inventory row. Original and normalized gzip contents must reproduce their recorded lengths and checksums. The index records each filing's exact archive chunk and envelope sizes; original archives are not modified. A previous pinned index can seed an extension, which reads only subsequent checkpoint chunks. Every resulting entry must still match the complete target inventory. Synthetic tests compare an extension with a fresh full rebuild byte for byte while making the older chunks unavailable during extension.

```sh
python -m insider_pipeline.document_index --checkpoint-manifest /latest/increment.json --checkpoint-sha256 CHECKPOINT_PIN --inventory-snapshot /frozen/current.sqlite3 --ancestor-manifest /base/baseline.json --ancestor-manifest /parent/increment.json --output /new/document-index
# To extend an existing index, also supply:
# --parent-index /previous-index/document-index.json --parent-index-sha256 PREVIOUS_INDEX_PIN
```

`github_document_index_stage` appends the index and an explicit private filing selection to the same published checkpoint bucket. Put one accession per line in a private local file. The publisher validates the index against the pinned remote checkpoint and frozen local inventory, preserves every existing asset, and independently downloads the new payloads before uploading the index manifest. Repeated identical publication reuses existing content. No release is created or made latest. Include the index and selection in bucket-capacity planning before publishing the checkpoint; the existing 900-asset guard still applies.

```sh
python -m insider_pipeline.github_document_index_stage --index /new/document-index --index-sha256 INDEX_PIN --accessions-file /private/accessions.txt --inventory-snapshot /frozen/current.sqlite3 --bucket-tag insider-archives-YYYYMM-001 --transport-sha256 TRANSPORT_PIN --expected-latest-release-id DATASET_ID --evidence /private/index-stage
python -m insider_pipeline.github_chain --tag insider-archives-YYYYMM-001 --transport-sha256 TRANSPORT_PIN --inventory-only --document-index-sha256 INDEX_PIN --filing-selection-sha256 SELECTION_PIN --output /new/selected-documents
```

The manual workflow accepts `document_index_sha256` and `filing_selection_sha256` together with `inventory_only` and an incremental transport pin. Accession values remain in the private selection asset. The reader validates the complete index and plans the combined inventory, index, optional source, and selected-chunk budgets before large inventory parts are fetched. After replay, every index row must match its complete committed inventory row before any selected document chunk is downloaded. It retrieves only chunks containing requested filings and materializes exactly the requested envelopes. A downloaded chunk may contain additional filings; those documents are not restored.

Each selected original and normalized document is checked again against its decoded length, checksum, source provenance, and complete inventory row, then compared byte for byte through database readback. Incorrect older envelopes, changed lookup entries, missing filings, corruption, and insufficient download or disk capacity cannot produce a completed restored root. The selection supports one to 1,000 distinct accessions. Indexes are bounded at 250 MB compressed and 1 GB decoded; a single envelope or decoded original/normalized document is bounded at 128 MiB. Exceeding a bound stops explicitly rather than truncating data.

The report distinguishes all committed inventory references from the number of selected documents actually restored, and records only selection hashes, counts, and an exact restored-row digest. It sets `includes_original_documents` for a document selection while leaving full-history recovery, original-field auditing, and collection-resume readiness unverified. This supplies reusable originals for later correction and collection work; discovery, incremental publication orchestration, final backfill completion, and a daily schedule remain separate requirements.

## Collection after a partial restore

The collector reserves shard numbers from every inventory reference as well as files and SQLite sidecars present on disk. Each collection run writes new documents into fresh monthly shard numbers above all reserved numbers and rotates its own files at the existing size threshold. It does not append to historical or partially restored files. A restarted run may therefore add a new shard for a month even if an older file has capacity. The four-digit shard namespace stops at 9,999 files per month and requires archive compaction before another slot can be allocated.

Recovery of an interrupted write opens an existing shard in read-only mode, verifies its document bytes and any recorded inventory hashes, and completes the queue entry without fetching it again. Looking for an uncommitted write in a missing shard does not create an empty database. If the inventory already records collected-document hashes but the document is unavailable or differs, collection stops: that document must be restored from its pinned archive before retrying.

Synthetic tests exercise inventory-only collection with historical shards absent, then build an incremental checkpoint and restore it with the original parent. They compare all reconstructed inventory and document rows, including compressed BLOBs, and check that parent files remain unchanged. This establishes safe allocation and interrupted-write recovery. It does not supply discovery refresh, selective recovery of changed historical documents, publication orchestration, or a daily schedule.

## Targeted index discovery and metadata refresh

`discovery_refresh` advances a frozen inventory's filing-date cutoff through selected SEC quarterly master indexes. By default it reads the most recent two quarters plus every quarter crossed since the previous cutoff. An explicit selection must still cover that cutoff interval. Earlier index coverage and membership must already be complete; the refresh neither downloads unselected indexes nor removes existing filings.

Preparation creates a separate, pinned plan containing the candidate inventory, retained source bytes, and the exact list of committed documents that must be recovered. Use `--fetch` for a fresh, paced SEC request or `--cache` for retained evidence. Cached evidence is labeled explicitly and does not establish coverage through a newly requested cutoff. A valid empty index can start a new quarter; an unexpectedly empty previously populated index or malformed ownership row stops preparation.

```sh
python -m insider_pipeline.discovery_refresh prepare \
  --inventory recovery/restored/inventory.sqlite3 \
  --through 2026-09-10 --fetch --output refresh-plan
```

The returned `plan_sha256` pins the plan. `committed-accessions.json` identifies originals to recover from that exact parent checkpoint, and `required_bulk_quarters` identifies original quarterly ZIPs needed for their later audit. Restore those selected originals and sources into the parent root before materializing the candidate:

```sh
python -m insider_pipeline.discovery_refresh materialize \
  --plan refresh-plan --plan-sha256 EXPECTED_PLAN_SHA256 \
  --restored-parent recovery/restored --output daily-candidate
```

Materialization checks the complete parent inventory, planned source bytes and observations, and every changed committed document. It reads archived originals without a repeat request, preserves their compressed source bytes and original fetch timestamps, rechecks parsed metadata, and writes fresh monthly shards. The parent inventory and historical shards remain unchanged. Unchanged historical documents can remain absent. The candidate can then be passed to the existing bounded collector and incremental archive builder.

Changed index observations retain their earlier source identity and reported values in inventory history. A removed index membership records a review issue and retains the original filing. Canonical metadata for an archived original is preserved while a conflicting new index observation is flagged for source review. An unchanged observation does not reopen a finding that was already resolved against its original. Pending filings can adopt an updated SEC-indexed URL. The complete inventory continues to retain every earlier filing and pending work item.

Tests cover serial/parallel output equivalence and the full refresh, collection, incremental archive, and restore sequence, comparing every inventory row and complete document row including compressed BLOBs. Missing originals or required sources, stale or corrupt plans, incomplete coverage, active writers, and changes to a frozen input stop without publishing a candidate. The CLI does not upload an archive or activate a schedule. Quarterly ownership-ZIP refresh, complete source auditing, final cutoff reconciliation, and daily publication orchestration remain separate integration work.

## Quarterly ownership-source refresh

`bulk_refresh` refreshes one to four explicitly selected published ownership ZIPs
against a frozen inventory with complete prior index discovery. It reuses each
recorded SEC URL; a newly added quarter requires an explicit URL from the
[SEC dataset page](https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets).
The SEC uses both `structureddata` and `datastandardsinnovation` paths. The URL
must identify the selected quarter on the SEC site. Cache reads and fresh fetches
are separate modes, and every response retains its checksum and retrieval time.

```sh
python -m insider_pipeline.bulk_refresh prepare \
  --inventory /frozen/inventory.sqlite3 --output /new/bulk-plan \
  --source-quarters 2026Q1 2026Q2 --fetch-sec --workers 2
python -m insider_pipeline.bulk_refresh materialize \
  --plan /new/bulk-plan --plan-sha256 PLAN_PIN \
  --restored-parent /prepared/parent --output /new/bulk-candidate
```

For a newly published quarter, pass `--source-url YYYYQn=URL` for each selected
quarter, or provide the same mapping as `source_urls` to the Python API. Fetches
share the SEC client's request clock. Quarter parsing uses independent worker
processes and disk-backed intermediate records; `--workers 1` is the serial
fallback. Each ZIP is bounded to 150 MB compressed, 1 GB decoded, and 250,000
in-scope filings. An exceeded bound fails without publishing a candidate.

The plan identifies all committed originals affected by changed source bytes or
provenance, including financial-value changes that leave submission metadata and
table counts unchanged. It preserves earlier source identities and changed bulk
observations in inventory history. Removed bulk associations are flagged and
cleared from the current comparison fields while their original filings remain.
Cross-quarter overlaps retain the existing association and a separate competing
observation. Removals are processed across all selected quarters before additions
so movement is independent of input order. Canonical original filing identity is
preserved when bulk metadata disagrees. New bulk-only filings remain pending
until their original SEC location is established.

Use `session.recover_bulk_refresh(plan_directory, plan_pin,
document_index_pin=index_pin)` after `github_session.open_inventory` to retrieve
the plan's selected originals and any inherited source files. New source bytes
come from the plan. Materialization independently replays the source changes and
checks the entire proposed inventory, then rechecks originals in fresh shards
while preserving their raw and compressed bytes. Existing unchanged ZIPs do not
reopen previously collected documents. Financial-field comparisons remain the
responsibility of the subsequent source audit; a successful refresh is not
approval of bulk values as replacements for original filings.

Tests cover serial/parallel byte equality, count-preserving value changes,
source movement and conflicts, exact compressed-document retention, and the full
session, refresh, incremental archive, and restore sequence. This component does
not advance the filing-date cutoff, publish an archive, or activate maintenance.
The daily orchestrator still needs published-quarter selection, index discovery,
collection, audits, publication, and retention.

## Adaptive recovery after inventory discovery

`github_session.open_inventory` retains the verified archive chain and download
cache after the complete inventory is restored. A caller can inspect the
inventory, prepare a discovery plan, and then recover the required originals and
bulk sources inside the same job:

```python
from insider_pipeline.discovery_refresh import prepare, materialize
from insider_pipeline.github_session import open_inventory

session = open_inventory(tag, transport_pin, output, source_quarters=quarters)
plan = prepare(session.root / 'inventory.sqlite3', plan_directory, through,
               source_quarters=quarters, client=sec_client)
session.recover_refresh(plan_directory, plan['plan_sha256'],
                        document_index_pin=document_index_pin)
materialize(plan_directory, plan['plan_sha256'], session.root, candidate_directory)
```

The session checks the complete discovery plan's parent against its checkpoint
and rejects changes to the restored inventory. Local accession selections require
no private selection upload. Empty selections skip the document index and filing
chunks. A caller with an independently derived list can use `session.recover`
directly. Local lists may exceed the manual selection limit, but remain bounded
by the metadata size and the cumulative 2 GB archive download limit.

Inventory archives are replayed once. Source selections retain the union of
initial and later quarters; existing source bytes and provenance metadata are
checked before reuse. Cached compressed downloads are rehashed before reuse,
and download and free-disk limits cover both stages. Each session accepts one
final recovery operation; a failed operation removes its final success report
and must be discarded. Partial local files are never a successful candidate.

Tests exercise discovery through candidate materialization, exact inventory and
compressed-document equality, single downloads, and rejection of corrupt caches,
changed parent inventories, mismatched plans, and capacity overruns. This API
does not add a workflow schedule or perform publication. A daily orchestrator
still needs quarterly bulk refresh, collection, audits, and incremental
publication; final backfill completion remains unverified.

## Supplementary bulk-source review

`bulk_review` builds a separate evidence ledger for every non-exact comparison in a pinned source audit. It validates the audit's selection and detail checksums, rechecks affected filings against their retained original XML, and reproduces every comparison from the checksummed SEC bulk ZIP. Each entry preserves original field strings and row numbers, raw bulk records and their ordinal, source URLs and checksums, the archived finding hash, and a diagnostic category. It does not change source documents, normalized values, or the existing audit.

The diagnostics distinguish two-decimal rounding, row-association conflicts, and timezone information omitted by bulk calendar dates. Matching column totals cannot pass as matching complete rows. Valid timezone-bearing `xs:date` forms are recognized only for a separately labeled calendar-date projection; offsets are preserved and no UTC conversion or equality-of-instants claim is made. Invalid or unsupported forms and unexplained values remain flagged. The old audit's `INVALID:` comparison marker can mean its date normalizer did not recognize a timezone suffix; it is not sufficient evidence that the original XML date is invalid. See the [W3C date datatype specification](https://www.w3.org/TR/xmlschema-2/#date).

```sh
python -m insider_pipeline.bulk_review --root /restored/checkpoint --audit /pinned/source-audit --audit-semantic-sha256 AUDIT_PIN --output /new/private-review-ledger --workers 2
```

Existing output directories are refused, and failed reviews retain their evidence without publishing the requested output. Quarter workers are independent and bounded by `--workers`; serial execution is available. A diagnostic explanation verifies source retention and helps review a conflict. It does not approve bulk values as replacements, finish collection, or erase outstanding source-review requirements.
