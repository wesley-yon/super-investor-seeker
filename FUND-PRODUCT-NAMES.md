# Identifying ETF descriptions

The browser prefers full fund product names. If none is available, it retains
the SEC security's class description alongside the registrant, including in
search, holdings, security headings and the full-name dialog.

The registry takes full names from exact SEC fund-series evidence first.
`reviewed_fund_product_names.json` supplies issuer descriptions for reviewed
gaps in that directory. Each record binds a full name to an exact CUSIP, ticker,
retrieval date, issuer URL and downloaded-page SHA-256. Innovator's published
Series field is retained because its product title can omit the month or term.

This review cannot resolve a ticker or change an instrument type. It only
applies to an independently resolved EQUITY entry already classified as an ETF,
with an exact matching CUSIP and symbol. Both registry and browser-metadata
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
