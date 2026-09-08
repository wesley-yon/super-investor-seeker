# Identifying ETF descriptions

The browser prefers full fund product names. If none is available, it retains
the SEC security's class description alongside the registrant, including in
search, holdings, security headings and the full-name dialog.

The registry takes full names from exact SEC fund-series evidence first.
`reviewed_fund_product_names.json` supplies issuer descriptions for reviewed
gaps in that directory. Each record binds a full name to an exact CUSIP, ticker,
retrieval date, issuer URL and source-artifact SHA-256. Most proofs are downloaded
HTML; browser-captured identity fragments are explicitly marked
`rendered_html_fragment`, and the DWS factsheet is marked `pdf`. Innovator's published
Series field is retained because its product title can omit the month or term.

This review cannot resolve a ticker or change an instrument type. It only
applies to an EQUITY entry already classified as an ETF, with an exact CUSIP.
Resolved symbols must match the issuer proof. An unresolved or ambiguous entry can receive
the description only if its published ticker remains null; neither its mapping
status nor ticker changes. Both registry and browser-metadata
validation reject a description copied onto a different security. Position
amounts, quantities, filing text, options and notes are unaffected.

## Revalidation

The reviewed bytes are pinned in `fund_product_names.py`. Publication refuses
an expired review after 90 days, consistent with the existing reviewed-identity
policy. Generate a candidate from the approved issuer URLs with:

```sh
python scripts/review_fund_product_names.py --output /path/to/new-candidate.json
```

The command verifies exact identifiers, checks for conflicting fields, and
writes a candidate only if every page succeeds. It does not modify the approved
review or generated site data. Review changed names and series against the
issuer pages, replace the approved JSON, update its pinned SHA-256, and run the
fund-product-name tests and full regression suite. The next registry rebuild
incorporates the approved descriptions, followed by normal snapshot validation
and Pages publication.

For a description/security-master refresh of the existing validated holdings,
dispatch `Rebuild SEC Security Master` with `reconcile_filings=false` and
`rebuild_security_master=false`. This skips broad filing replay only. It still
runs classification repairs, refreshes and audits the complete security master,
regenerates site data, and runs the usual validation, tests and publication
checks. Scheduled runs and the default manual run continue to replay filings.

If ordinary HTTP access is unavailable, capture fresh issuer HTML or a Fund
Details fragment in a normal browser and retain the exact name, ticker and
CUSIP evidence. `--source-directory /path/to/captures` reads `TICKER.html` files
instead of issuing HTTP requests. Every file must have been captured that UTC
day and every identity must pass the same parser. Review each page's provenance
and capture format before approving the resulting candidate; a file timestamp
alone is not evidence that the issuer facts are current. PDF proofs use
`TICKER.pdf` and require the optional `pypdf` package for source revalidation
(tested with 6.18.0). The PDF parser checks both the header ticker and the ETF
details ticker/CUSIP, excluding the index's own ticker. Normal site generation
and publication do not parse PDFs or require this optional package.
