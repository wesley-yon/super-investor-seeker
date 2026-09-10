# Selective derived-data rebuilding

Routine filing updates reuse verified work when its exact inputs have not
changed. The updater still produces complete fund, stock, registry, label,
search-index, and diagnostic outputs. The private-source and full-corpus
publication gates remain mandatory.

## Dependency rules

| Change | Regeneration behavior |
| --- | --- |
| No fund, evidence, policy, or output changes | Reuse expensive calculations and stock/index files; reconcile state and render the complete diagnostic report. |
| Added, amended, renamed, or removed fund | Rebuild registry/diagnostic aggregates for its old and new identifiers from every contributing fund; rebuild affected stock views and complete indexes. |
| Changed cross-fund observations or quantity evidence | Rerun global portfolio-health or quantity calculations; include every resulting fund change in downstream selection. |
| Changed SEC master, source state, or reviewed identity map | Rebuild the full registry and propagate changed identities to dependent fund and stock views. |
| Reporting-quarter rollover | Rebuild all stock views because current-holder counts can change for untouched securities. |
| Missing or corrupt derived stock/index output | Rebuild the complete stock/index generation using the existing atomic replacement and rollback boundary. |
| Changed code, installed dependencies, policy files, or date | Discard cached calculations and use the full builders. |
| Missing, damaged, or incompatible acceleration cache | Use the full builders and create new acceleration state after successful regeneration. |

The registry preserves `CUSIP | instrument_type`: adding a CALL or NOTE does
not turn it into the issuer's common stock. Deletions use both prior and current
dependencies, so removed positions and identifiers do not leave stale outputs.
Ticker-health aggregation preserves original fund/holding order to retain the
same tie-breaking and output bytes as a full scan.

The hosted updater recreates `.cache/cusip_registry.json` from the verified
snapshot's `data/cusip_registry.json` before capturing inputs. The snapshot
deliberately omits that derived mirror. Both copies must still match the saved
hashes for registry reuse; altered published bytes trigger reconstruction.

The cache stores hashes of actual bytes, not modification-time promises. Its
payload has a checksum and is transported only inside the authenticated private
snapshot. The cache is acceleration state, never source evidence or permission
to bypass validation. Old snapshots without the optional file remain usable.
The pre-ingestion marker records only code compatibility. Dependency inventory
comes from the last complete generation, so regeneration does not retain a
second large copy of the corpus metadata in memory.

## Full-rebuild fallback

Choose **full_rebuild** in the `Update 13F Data` workflow's manual dispatch, or
run locally from a restored private snapshot:

```sh
python scripts/incremental_pipeline.py capture
python scripts/incremental_pipeline.py regenerate --full-rebuild
python validate_data.py --incremental
python -m unittest discover -s tests -v
```

The full-rebuild option recomputes derived outputs using the saved SEC evidence.
Refreshing or rebuilding that evidence remains a separate operation.
This is also the fallback for disabling acceleration without changing snapshot
compatibility. A code revert must retain the snapshot reader's support for the
new optional cache file, or restore a snapshot produced before this change.

## Verification method

Tests compare selective and full rebuilding from separate copies of identical
inputs, including amendments, removals, new options, source changes, calendar
rollovers, withheld data, corrupted outputs, and interrupted rendering. They
also verify that unchanged inputs skip expensive phases without changing bytes.

Production-corpus measurements must use the same Python environment, authenticated
snapshot, and controlled input changes. Compare every output file and the
public artifact tree. Report cold cache creation separately from warm unchanged
and changed-filing runs. Hosted verification must include snapshot round-trip,
full validation/tests, Pages publication, finalization, and a warm follow-up run.

## Measured unchanged-corpus result

On the same Mac, Python 3.11 environment, and verified private snapshot
(`da3c3c8434912fcc04c412595b2549ae1a0b6db4b88a36dd34a5f42ebf039a66`),
containing 9,509 funds and 61,127 stock files:

| Phase | Previous updater | Selective updater, warm cache |
| --- | ---: | ---: |
| Capture before ingestion | 21.31 s | 0.007 s |
| Regenerate with no filing changes | 537.67 s | 22.64 s |
| Combined | 558.98 s | 22.65 s |

All 70,643 files under `data/` were byte-identical after both the initial full
generation and the warm selective generation. Warm regeneration reused all seven
expensive phase groups and rebuilt zero stock files. Parent-process peak RSS
was 2.11 GB. The uncompressed optional cache was 178.8 MB; the separate
pre-ingestion compatibility marker was 87 bytes.

Cold cache creation additionally rebuilds every stock file and inspects every
fund for new master identities. It is intentionally more work than the previous
updater's unchanged-fund path; the warm savings do not apply to missing-cache
or changed-code/date runs. The final full-rebuild bootstrap was tested while
independent corpus checks ran in another checkout, so its wall time is excluded
from these serial A/B comparisons. These are regeneration measurements,
excluding ingestion, mandatory publication checks, and deployment.

## Measured filing-change result

A second serial comparison applied the same 13 real fund-file changes from
verified snapshot
`735d761c01123bc9f22dd826a4987d4a7322f0ef282ed75fc0f5e738fe457e33`
to separate copies of the starting corpus. Every incoming and replaced file
was checked against its recorded SHA-256. Eleven existing funds changed and
two funds were added. The batch introduced two new security identities, so both
implementations retained unresolved ticker status and rebuilt the full registry.

| Phase | Previous updater | Selective updater, warm cache |
| --- | ---: | ---: |
| Capture before ingestion | 21.49 s | 0.016 s |
| Regenerate the 13-fund batch | 868.17 s | 815.33 s |
| Combined | 889.66 s | 815.35 s |
| Peak parent-process RSS | 14.22 GB | 15.30 GB |

This batch saved about 8% of capture-plus-regeneration time. Its global source,
registry, portfolio-health, and quantity work limits the savings. Both paths
rebuilt the same 4,539 stock views. The cache uses additional memory; the compact
87-byte capture marker avoids retaining another large inventory during source
extension. These are single paired measurements, not an end-to-end latency promise.

Every one of the resulting 70,647 data files was byte-identical, as were the
private SEC master/source pair, registry, quantity-evidence books, reviewed map,
and pipeline state. Separately packaged public artifacts were byte-identical
across 70,651 files and 738,329,425 bytes. Packaging used the same explicit code
and dataset identifiers in both builds to hold deployment metadata constant;
the comparison artifacts were never published.

The final code passed all 1,194 Python tests against the complete corpus, all
9 Node tests, reviewed-identity validation, the ETF search-description audit,
and full data validation over 9,511 funds and 61,129 stocks. The five existing
corpus-quality warnings were unchanged. The manual full-rebuild path and
optional-cache snapshot round trip are also covered by verification.
