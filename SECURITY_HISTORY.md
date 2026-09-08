# Reviewed identifier history

Search uses the dated, source-backed chains in `reviewed_security_history.json` to show one result for each reviewed corporate-action history. It selects the latest available identifier in the chain, regardless of reported holder count or display badge. Distinct identifiers with an unreviewed shared ticker remain separate. Exact CUSIP routes continue to open the original security.

Each affected security page shows the dated identifiers and a primary-source link for each transition. Historical pages are labeled explicitly. If the latest reviewed identifier has no file in the snapshot, its history entry says that no holdings data is available; it has no fabricated zero-holdings page. PFSA exercises this case: its July and August 2026 reverse splits form a three-identifier chain.

## Identity and economic boundaries

The key remains `CUSIP | instrument_type`. This review does not combine fund positions, convert shares, adjust prices, or move option contracts to their underlying equity. It does not prove that a successor is currently listed or grant quote permission. Predecessors receive `price_lookup_allowed: false` and `historical_retired_identity` in the display projection; raw SEC master records remain unchanged. Standalone historical-only identities from the pinned private review also keep historical labels.

Dates document the legal or trading change, not a rule for rewriting a filer's reported CUSIP. A filing may continue to report an old identifier. Security-page amounts remain confined to the selected identifier.

## Maintaining and publishing the review

The initial review contains 45 histories and 46 dated transitions, reviewed September 8, 2026. It covers the audited snapshot's 45 same-ticker multi-CUSIP search groups; it is not an exhaustive list of future corporate actions.

Add a transition only after verifying the exact old and new identifiers, instrument class and effective date against primary evidence. Preserve source URLs. Update the ordered chain and review date, then update `REVIEW_SHA256` in `security_history.py`. Validation rejects conflicting or overlapping identities and invalid transition dates. A ticker match alone is insufficient evidence.

Both data workflows run `python scripts/refresh_security_history.py --pending-only` after the classification repairs. This refresh rebuilds registry-backed outputs while preserving position economics. It records the review checksum in pipeline state only after successful completion. A changed checksum triggers the next rebuild. The public history travels in `security_labels.json`; data validation and the Pages artifact builder reject missing or mismatched history metadata.

The review and code must be deployed with a refreshed private snapshot. A passing code test or a local rebuild does not establish production publication.
