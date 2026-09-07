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
Production also consumes the approved September 7 review from the private data
repository. `reviewed_ticker_map.py` pins its commit and file checksum. The full
provenance and captured source package remain private. The reviewed layer is a
display projection: it does not rewrite or relabel the original SEC evidence.
Every exact `CUSIP|instrument_type` match may fill an unresolved record; a
resolved SEC ticker that disagrees with the reviewed ticker stops publication.
The original 10,532 reviewed identities survive source refresh omissions.
Exact per-instrument display mappings preserve reviewed keys when historical
row types differ from a CUSIP's single aggregate registry classification.
This does not reclassify retained rows or assign an equity symbol to debt. No
fuzzy name matching runs in this layer. Changing an accepted symbol or extending
the review requires a new verified private package and a reviewed code pin.

Eight explicitly retired identities carry `price_lookup_allowed: false` in the
registry and stock files. Their historical symbols must not be used for current
quotes or replaced with successor-share symbols. This site currently does not
request live prices. Other inherited tickers are not certified as quoteable.

## Refresh and publication policy

- SEC source discovery and a comprehensive registry/provenance audit run daily
  at 04:23 UTC. Immutable history is reused; changed source documents are fetched.
- Ordinary filing updates run hourly during the weekday 07:00–18:00 New York
  window and reuse accepted security evidence.
- A clean download/rebuild is an explicit manual workflow option, not a random
  periodic reset. It still loads the pinned reviewed layer.
- Every data publication verifies the pinned review, checks observed reviewed
  identities against generated holdings/registry, and measures EQUITY holding-row
  coverage. Both the frozen June 2026 reference quarter and the newest observed
  quarter produce an Actions warning below 98%, prompting investigation of
  unresolved identities without blocking publication. The measured figures and
  warnings are saved in the coverage report. Identity conflicts, missing data,
  and other integrity failures still block publication; 98% is an investigation
  threshold, not a publishing requirement.
- Reviewed external sources are due for revalidation after 30 days (an Actions
  warning). After 90 days, publication stops pending an updated reviewed package.
  The daily job does not automatically repeat the manual issuer/exchange review.
  Revalidate at least monthly and after each quarterly filing influx, sooner for
  corporate actions or conflicts. Never renew the date without checking sources.

The reviewed layer is included in private snapshots so deployment validation
can replay the exact same decisions. Daily source checks and the existing SEC
acceptance gates remain mandatory. The public repository contains code and the
pin only; it must never contain the private mapping or captured evidence.

## Verification

Regression tests cover Berkshire name variants, both Lennar classes, ADR/ordinary
separation, ETF products sharing a trust, ticker punctuation collisions, altered
evidence, unrelated issuers, and the saved-rules upgrade. Replay the same private
snapshot with the baseline and changed code to measure additional resolutions
and withdrawals independently of changes in source data. Report coverage for a
named quarter as well as the full historical master; historical and malformed
identities are not all currently listed securities awaiting a ticker.
