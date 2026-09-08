# Next task

- [x] Implement the exact note classification repair for 59 reviewed securities
  (57 ETF shares and two preferred depositary shares), with a staged historical
  migration, ingestion protection, and old-link routing. See
  `NOTE-CLASSIFICATION-REPAIR.md`. Production publication still requires the
  merged code and a successful data workflow; the local repair is not live.

- [ ] Fix preferred-share classification and display. Review preferred and
  depositary-share positions currently labeled as common equity or notes,
  verify the exact series and symbol against first-party evidence, and make
  their labels consistent across holdings, search, and security pages. Preserve
  original filing fields and distinguish each series from issuer common stock.
