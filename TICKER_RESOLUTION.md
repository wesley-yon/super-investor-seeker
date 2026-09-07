# Ticker resolution for stocks, ETFs, and ADRs

The resolver uses names to compare security evidence, while keeping the exact
CUSIP and instrument type as the identity. It must distinguish classes of the
same company's stock, a fund's individual products, and an ADR from its local
ordinary shares. Notes, options, and warrants cannot acquire a common-stock
ticker merely because the issuer name matches.

## Matching rules

1. Collect the as-filed CUSIP, issuer, class, instrument type, and dated filing
   evidence. An active SEC Section 13(f) row independently identifies the class.
2. Obtain dated, exact-CUSIP ticker candidates from SEC fails-to-deliver records,
   or the existing exact Schedule 13D/G and periodic-filing class bridge.
3. Normalize bounded presentation differences before comparing issuer names:
   SEC incorporation suffixes such as `/DE/`, `/CA`, and `/NEW/`; recognized
   common-share or ADR/ADS descriptions; and presentation-only `(NEW)` markers.
   Initials may move around an unchanged surname without changing their order.
   These rules do not perform general fuzzy matching or reorder arbitrary words.
4. For ETFs with a unique current SEC series/class identity, compare the trust
   brand without generic fund words and their standard abbreviations. Preserve
   meaningful names and trust numbers, including distinct Roman numerals.
   An ETF trust name alone never supplies a product's symbol.
5. Reconcile a missing share-class separator only when a unique current SEC
   symbol, the active official CUSIP class, and the FTD class all agree. For
   example, raw FTD `LENB` may map to current SEC `LEN-B` for Lennar Class B.
   Preserve the raw FTD symbol, source hashes, and dated observations in private
   proof. Do not strip punctuation from every ticker or map Class B to `LEN`.
6. Replay the same identity and provenance checks when validating saved state.
   Stale, conflicting, malformed, or insufficient evidence remains unresolved.

## Applying changed rules to a saved dataset

The private master records its ticker-resolution rules version. Routine filing
updates normally reuse that master. When the deployed rules are newer, the
incremental pipeline rebuilds it once from the saved SEC evidence, audits the
candidate, and atomically saves the accepted pair before regenerating affected
fund and stock data. It does not need to redownload historical SEC records.
Subsequent runs reuse the current version. An unknown newer version or rejected
candidate fails without replacing the accepted pair.

This matters because deploying code alone does not rewrite a saved ticker
registry. Production completion requires a successful data refresh and deployment
of the resulting dataset, followed by inspection of the published output.

## Official sources for additional evidence

Official ETF sponsors, ADR depositaries, and exchanges are appropriate secondary
sources when SEC evidence is incomplete. A company-level lookup supplies a
candidate, not proof of a particular security class. For example, Lennar Class A
and Class B require separate exact class evidence.

Verified reference cases:

| Security | Exact identifier | Official source |
| --- | --- | --- |
| BOXX ETF | CUSIP 02072L565 | [Alpha Architect fund page](https://funds.alphaarchitect.com/boxetf/) |
| IWM ETF | CUSIP 464287655 | [iShares product page](https://www.ishares.com/us/products/239710/ishares-russell-2000-etf) |
| BioNTech sponsored ADS, BNTX | CUSIP 09075V102 | [BNY depositary record](https://www.adrbny.com/directory/dr-details/_jcr_content/root/drDetailsComponent.overview.overview.09075V102.html) |
| Lennar Class A / Class B | LEN / LEN.B | [Lennar May 2026 Form 10-Q](https://www.sec.gov/Archives/edgar/data/920760/000162828026046019/len-20260531.htm) |

These sources were used for independent checks in the September 2026 audit.
Automated publication in this change continues to use the existing SEC proof
paths. A future external-source adapter must retain its exact CUSIP/class match,
listing and receipt details, retrieval date, content checksum, conflict handling,
and expiry policy in a replayable private evidence record. A live company name
or an unverified search result must not silently become a durable ticker mapping.

## Verification

Regression tests cover Berkshire name variants, both Lennar classes, ADR/ordinary
separation, ETF products sharing a trust, ticker punctuation collisions, altered
evidence, unrelated issuers, and the saved-rules upgrade. Replay the same private
snapshot with the baseline and changed code to measure additional resolutions
and withdrawals independently of changes in source data. Report coverage for a
named quarter as well as the full historical master; historical and malformed
identities are not all currently listed securities awaiting a ticker.
