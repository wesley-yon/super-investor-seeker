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

The cache stores hashes of actual bytes, not modification-time promises. Its
payload has a checksum and is transported only inside the authenticated private
snapshot. The cache is acceleration state, never source evidence or permission
to bypass validation. Old snapshots without the optional file remain usable.

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
| Capture before ingestion | 21.31 s | 5.44 s |
| Regenerate with no filing changes | 537.67 s | 22.49 s |
| Combined | 558.98 s | 27.93 s |

All 70,643 files under `data/` were byte-identical after both the initial full
generation and the warm selective generation. Warm regeneration reused all seven
expensive phase groups and rebuilt zero stock files. Parent-process peak RSS
was 3.39 GB. The uncompressed optional cache was 178.8 MB.

Initial cache creation took 800.84 seconds of regeneration, separately from
25.47 seconds of capture. This full fallback additionally rebuilds every stock
file and inspects every fund for new master identities. It is intentionally
more work than the previous updater's unchanged-fund path; the warm savings do
not apply to missing-cache or changed-code/date runs. These are regeneration
measurements, excluding ingestion, mandatory publication checks, and deployment.
