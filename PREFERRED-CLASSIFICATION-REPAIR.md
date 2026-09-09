# Preferred-interest classification and display

Abbreviated 13F classes such as `DEP SHS REPSTG`, `SER A MAND CNV`, and
`SERIES A PERP PF` previously left preferred interests classified as ordinary
equity. Other rows classified funds by the preferred securities they hold,
or treated corporate units as preferred stock.

This review binds 51 exact CUSIPs to their security classes: 32 preferred
interests, 11 ETFs, seven corporate/tangible equity units, and one ADR
representing common shares. Each preferred series stays separate from issuer
common stock. The ADR uses the broad EQUITY type without a Common Stock badge.
Unreviewed identifiers and ambiguous legacy options retain their identities.

`reviewed_preferred_classifications.json` records the series descriptions,
exact symbols, dated first-party URLs, source hashes, and correction scope.
SEC fails-to-deliver CUSIP/symbol observations were checked against the source
bytes and paired with Nasdaq exchange security descriptions. Apollo's Series A
uses its issuer-hosted SEC filing and conversion announcement because it
converted in July 2026. Its historical identity remains visible with current
price lookup disabled. No new quote permission or issuer-common-symbol
substitution is introduced; the private SEC master and previous display review
remain unchanged.

The checksummed rule runs during ingestion and saved-row classification. The
shared migration engine stages and validates the complete batch before
replacing any fund file. Original filing text, source hashes, quantities,
values, and row counts remain intact; derived composition hashes are updated
with a separate preferred-classification audit record. Serial and parallel
migration results are equivalent, and a failed staged file prevents the batch
from being installed.

The site uses source-case series names in holdings, search, and security pages.
Verified preferred series and corporate units can be found by their symbols or
names. Search descriptions wrap on small screens. Old EQUITY/PREF/NOTE links
redirect only for exact reviewed identities after the corpus has been repaired;
option routes retain their own identities.

Both data workflows run this migration after the note-classification repair.
The completion checksum is persisted only after a successful output rebuild;
ordinary state saves retain both migration checksums. The publication validator
rejects reviewed misclassifications and names/routes that differ from the
reviewed registry. An interrupted rebuild can be resumed without altering
already-repaired rows.

```sh
python scripts/repair_preferred_classifications.py --workers 2 --report .cache/preferred-dry-run.json
python scripts/repair_preferred_classifications.py --apply --rebuild --pending-only --workers 2 --report .cache/preferred-repair.json
python validate_data.py --incremental --refresh-cache --workers 2
```

The September 8 local comparison checked all 9,506 fund files. It found 4,788
classification changes across 2,366 quarters in 759 funds: 3,083 to preferred,
1,516 to ETF equity, 176 to unit equity, and 13 to common-underlying ADR equity.
All source fields, values, shares, and row counts were preserved. There were
2,005 corresponding derived ticker updates, confined to reviewed CUSIPs.

This change builds on the note repair in PR #44. Local tests and rebuilt data
do not establish production publication; that requires merged code, a successful
data workflow, and an independent live check.
