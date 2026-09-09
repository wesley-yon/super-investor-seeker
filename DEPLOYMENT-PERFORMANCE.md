# Deployment throughput verification

The September 9, 2026 changes remove unnecessary data refreshes and queue waits,
then reduce repeated validation work. They preserve the complete dataset,
source-provenance, regression, rollback, retention, and artifact privacy gates.

## Local comparison

Baseline: commit `75984e36c8e3eb1acb5c57309d3060b145eb8b9f`.
Both checkouts received the same authenticated snapshot: 70,649 files,
6,895,933,777 uncompressed bytes, dataset digest
`da3c3c8434912fcc04c412595b2549ae1a0b6db4b88a36dd34a5f42ebf039a66`.
Measurements used CPython 3.11 on the same 12-logical-core, 48-GiB Mac.

| Check | Baseline | Changed code | Reduction |
| --- | ---: | ---: | ---: |
| Complete private SEC provenance gate | 261.92 s | 104.84 s | 60.0% |
| Complete dataset-backed Python discovery | 208.14 s | 50.17 s | 75.9% |

The provenance gate returned the same empty error list. Python discovery passed
all baseline tests and the added workflow tests, including the production-data
contract. The timings are single local measurements, not hosted-runner or
end-to-end deployment promises. The provenance baseline and changed-code runs
used fresh processes with no profiler; separate profiling identified repeated
metadata-key checks, date parsing, URL parsing, and filter-prefix hashing.
Some independent verification ran concurrently on spare cores. Peak resident
memory for provenance was approximately 7.8 GB before and 8.4 GB after, so this
change does not add competing copies of the large master in worker processes.

The same artifact build arguments were run in both checkouts (two compression
workers, identical producing SHA and dataset digest). Every output matched:
70,647 artifact files, 738,287,680 artifact bytes, and tree SHA-256
`e3fdbbfebd61fbfc6e4e43696a8564e91857954f5258ad6f1071ad4381e0f4e1`.

## Reproduce

Use isolated checkouts and the project Python environment. Restore the same
verified snapshot into both; do not publish local benchmark data. Measure
`validate_data.validate_private_sec_security_state()` with the same restored
`data/cusip_registry.json` in fresh Python processes. Require identical error
lists, and run `python validate_data.py --incremental` for the full publication
gate. Run `python -m unittest discover -s tests -v` with Node on `PATH`.

Build both checkouts with `scripts/build_pages_artifact.py`, passing the same
`--source-sha`, `--dataset-id`, and worker count. Compare the entire builder
result, especially `tree_sha256`, rather than comparing only file counts.

Workflow regression coverage verifies the lock order, stale receipt rejection,
unchanged producer identity, frontend compatibility, unknown-input rejection,
complete test discovery, rollback preconditions, retry safety, and cleanup.
Warm-cache mutation tests prove changed source documents and filter coverage
are rechecked. Existing URL, date, provenance, and snapshot failure tests remain.

## Hosted acceptance

Require passing Test CI and a completed post-merge Update or Refresh, Pages
publication, finalization, and artifact cleanup. Reconcile the private manifest
and marker with the exact successful Pages deployment. Check the live manifest
through the public site; a Cloudflare challenge is not independent content
verification. Local timings and workflow configuration alone do not establish
hosted performance or prove that queued deployments can progress.
