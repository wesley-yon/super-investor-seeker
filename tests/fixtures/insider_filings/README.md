# Section 16 XML fixtures

The XML files directly in this directory are compact, manually authored,
test-only data. The `sec_derived/` subdirectory contains separately documented,
sanitized source-derived SEC examples for Forms 3, 3/A, 4, 4/A, 5, and 5/A;
its manifest pins both source provenance and sanitized hashes, and no raw source
filing is retained in the repository.

The source-derived manifest keeps the real public SEC URLs strictly as provenance.
Because issuer CIKs are deliberately sanitized in the checked-in XML, its separate
`parser_source_*` URLs also replace the archive-path CIK with the fixture issuer CIK.
Offline parser tests use only those clearly labeled sanitized metadata values; no
test fetches either URL family.

The directly contained synthetic XML mirrors SEC ownership-document element
shapes, but the issuers,
reporting owners, CIKs, accessions, addresses, dates, securities, signatures,
and transaction values are synthetic. They are not downloaded SEC filings,
not evidence about any real person or company, and must never be presented as
production filing history.

`expectations.json` is the independent normalized oracle. Tests never generate
its expected values from the parser under test. Source URLs are inert example
metadata and no test performs network access.

The schema labels and fixture roles are deliberate:

- `X0306` fixtures exercise the legacy Form 3, Form 4, and Form 5 ownership
  shapes, including both holding tables, a purchase, and explicit Form 3/A
  and Form 5/A amendment variants.
- `X0408` fixtures exercise a Form 4/A amendment, joint owners, derivative
  and non-derivative rows, country fields, and a foreign trading symbol.
- `X9999-TEST` is an explicitly fictional future-schema label used only to
  prove that unknown codes, elements, attributes, and footnote references are
  retained for review.
- `unsafe_*.xml` are intentionally hostile, non-schema DTD/entity documents;
  they are rejection oracles and are never parsed as filing evidence.

The field shapes follow the approved Phase 2 ownership-document contract.
Because this suite is intentionally offline, these hand-authored fixtures are
not represented as downloaded or independently live-validated SEC schema
documents. Every XML oracle, including the hostile inputs, is SHA-256 pinned in
`expectations.json`.

Addresses intentionally contain only conspicuous placeholders. They exercise
restricted private normalization and must never enter a public projection or
log.
