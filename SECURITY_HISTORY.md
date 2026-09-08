# Combined share-class holdings

The investor-facing position is a security or share class, which can have different CUSIPs over time. GOOGL's old Google Class A CUSIP and current Alphabet Class A CUSIP form one holding. GOOG Class C, GOOGM preferred and GOOGN preferred remain separate positions.

The source-backed chains in `reviewed_security_history.json` identify the exact CUSIPs belonging to each reviewed class. Shared company names or tickers alone never establish that relationship.

## Investor views

- Search shows one result per reviewed class.
- Every member CUSIP route opens the same combined security view, including legacy bookmarks.
- Security pages combine positions by manager CIK and report date. A manager reporting multiple member identifiers is counted once; the distinct reported positions contribute their values and shares.
- Fund pages combine member identifiers before ranking holdings, calculating portfolio weights, comparing quarters or detecting new positions and exits. A CUSIP change alone cannot create an exit and a new purchase.
- Combined-holdings, unknown-share-count and unavailable aggregate-trend notices appear as compact footnotes below the security page holdings and summaries.
- Filing details disclose the original contributing CUSIPs, quantities and values. Corporate-action dates and primary-source links are available in a collapsed disclosure.
- A newer identifier with no separate filing records does not erase predecessor holdings. The combined view uses all available member records.
- All indexed member files must load successfully before the combined view renders. A failed request cannot silently lower the totals.

## Quantities and provenance

Original filing storage still uses `CUSIP | instrument_type`. The combined view is derived in memory and leaves the saved fund and security records intact. Calls, puts, different common share classes and preferred series cannot enter an unrelated class's aggregate.

For a given reporting date, quantities and values are summed as reported. An old CUSIP by itself does not establish an old share basis: filers sometimes keep an obsolete identifier after applying a split. Multiplying current reported quantities solely according to CUSIP would introduce a second adjustment.

For quarter-to-quarter comparisons, the application collects verified, exact-date split/exchange factors from all class members and applies each event once. Verified factors also put historical share trends on the latest displayed filing's basis. Changes spanning an action with no verified factor are withheld rather than presented as a manager trade. This does not hide the combined holding or its reported value history.

Current, stale, withheld and historical-manager rules continue to apply after the class history is combined. The quote guard remains attached to the original typed identifiers; a combined holdings view does not authorize a quote lookup for a retired CUSIP.

## Maintaining and publishing the review

The initial review contains 45 histories and 46 dated transitions, reviewed September 8, 2026. It covers the audited snapshot's 45 same-ticker multi-CUSIP search groups; it is not an exhaustive list of future corporate actions.

Add a transition only after verifying the exact old and new identifiers, security class and effective date against primary evidence. Preserve source URLs. Update the ordered chain and review date, then update `REVIEW_SHA256` in `security_history.py`. Validation rejects conflicting or overlapping identities and invalid transition dates.

Both data workflows run `python scripts/refresh_security_history.py --pending-only` after the classification repairs. The refresh preserves original position economics and records its checksum only after successful completion. Public history is carried in `security_labels.json`; data validation and the Pages artifact builder reject missing or mismatched metadata.

The code must be deployed with compatible reviewed metadata. A passing code test or local preview does not establish production publication.
