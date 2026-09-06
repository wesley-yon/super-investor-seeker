const DATA_CONTRACT_VERSION = 5;

class DataContractMismatchError extends Error {
  constructor(path, actualVersion) {
    super(
      `${path} uses data contract ${String(actualVersion)}; ` +
      `expected ${DATA_CONTRACT_VERSION}`
    );
    this.name = "DataContractMismatchError";
  }
}

class RequiredSiteDataError extends Error {
  constructor(path, cause) {
    super(`${path} is unavailable or invalid`);
    this.name = "RequiredSiteDataError";
    this.cause = cause;
  }
}

function assertCompatibleDataContract(data, path) {
  const actualVersion = data?.data_contract_version;
  if (!Number.isInteger(actualVersion) || actualVersion !== DATA_CONTRACT_VERSION) {
    throw new DataContractMismatchError(path, actualVersion);
  }
}

function assertRequiredSecurityMetadata(data, path) {
  for (const field of ["labels", "kinds", "product_names"]) {
    const value = data?.[field];
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new RequiredSiteDataError(
        path,
        new Error(`${field} must be an object`)
      );
    }
  }
  if (!Array.isArray(data?.fund_identities)) {
    throw new RequiredSiteDataError(
      path,
      new Error("fund_identities must be an array")
    );
  }
}

// ---------- formatters ----------
const _tcKeepUpper = new Set(["LLC","LP","LLP","LTD","INC","CORP","CO","PLC","NV","SA","AG","AB","II","III","IV","VI","VII","VIII","IX","XI","XII","ETF","ADR","MSCI","S&P","SPDR","FTSE","ESG","REIT","CLO","MBS","TIPS","US","USD","UK","EU","AI","IPO","EAFE","ACWI","NASDAQ","NYSE","PGIM","PIMCO","FT","IBONDS","N.V.","L.P.","S.A."]);
const _tcLower = new Set(["OF","AND","THE","A","AN","FOR","TO","WITH","IN","ON","AT","BY","OR","NOR","BUT","AS"]);
function titleCase(s) {
  if (!s) return "";
  return s.replace(/\S+/g, (w, i) => {
    const up = w.toUpperCase();
    const bare = up.replace(/[.,]/g, "");
    if (_tcKeepUpper.has(bare)) return up;
    if (i > 0 && _tcLower.has(up)) return w.toLowerCase();
    if (up === w && w.length > 1) return w.charAt(0) + w.slice(1).toLowerCase();
    return w;
  });
}
const fV = v => {
  if (!v || v === 0) return "$0";
  return v >= 1e12 ? `$${(v/1e12).toFixed(2)}T`
       : v >= 1e9  ? `$${(v/1e9).toFixed(2)}B`
       : v >= 1e6  ? `$${(v/1e6).toFixed(1)}M`
       : v >= 1e3  ? `$${(v/1e3).toFixed(0)}K`
       : `$${v}`;
};
const fS = n => {
  if (!n || n === 0) return "0";
  return n >= 1e9 ? `${(n/1e9).toFixed(2)}B`
       : n >= 1e6 ? `${(n/1e6).toFixed(2)}M`
       : n >= 1e3 ? `${(n/1e3).toFixed(1)}K`
       : `${n}`;
};
const formatShares = (shares, imputed = false, unknown = false) =>
  unknown ? "—" : `${imputed ? "~" : ""}${fS(shares)}`;
const fP = n => {
  if (n == null) return "—";
  if (n === 0) return "0.0%";
  if (n < 0.05) return "<0.1%";
  return `${n.toFixed(1)}%`;
};
const esc = s => (s == null ? "" : String(s))
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;")
  .replace(/'/g, "&#39;");

function sec13fFilingsUrl(fund) {
  const rawCik = String(fund?.cik ?? "").trim();
  if (!/^\d{1,10}$/.test(rawCik)) return "";
  const cik = rawCik.replace(/^0+/, "");
  if (!cik) return "";
  return `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${encodeURIComponent(cik)}&type=13F-HR&owner=exclude&count=100`;
}

// match the pipeline's filename sanitization for stock files
const safeTicker = t => (t || "").toUpperCase().replace(/[^A-Z0-9._-]/g, "_");

function normalizeInstrumentType(type) {
  const t = String(type || "EQUITY").trim().toUpperCase();
  return ["EQUITY", "PREF", "NOTE", "WARRANT", "CALL", "PUT", "OPT"].includes(t) ? t : "EQUITY";
}

let securityLabels = Object.create(null);
let securityKinds = Object.create(null);
let securityProductNames = Object.create(null);
let securityFundIdentities = new Set();
let securityLabelsPromise = null;
const VALID_SECURITY_KINDS = new Set([
  "COMMON",
  "PREFERRED",
  "ETF",
  "ETN",
  "MUTUAL FUND",
  "CLOSED-END FUND",
  "BOND",
  "WARRANT",
  "RIGHT",
  "UNIT",
]);
const FUND_PRODUCT_NAME_KINDS = new Set([
  "ETF",
  "ETN",
  "MUTUAL FUND",
  "CLOSED-END FUND",
]);

function normalizeSecurityTextMap(source) {
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    return Object.create(null);
  }
  const normalized = Object.create(null);
  for (const [rawCusip, rawLabel] of Object.entries(source)) {
    const cusip = String(rawCusip || "").trim().toUpperCase();
    const label = String(rawLabel || "").trim().replace(/\s+/g, " ");
    if (!cusip || !label || label.toUpperCase() === cusip) continue;
    normalized[cusip] = label;
  }
  return normalized;
}

function normalizeSecurityKindPayload(data) {
  const source = data?.kinds;
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    return Object.create(null);
  }
  const normalized = Object.create(null);
  for (const [rawCusip, rawKind] of Object.entries(source)) {
    const cusip = String(rawCusip || "").trim().toUpperCase();
    const kind = String(rawKind || "").trim().toUpperCase().replace(/\s+/g, " ");
    if (!cusip || !VALID_SECURITY_KINDS.has(kind)) continue;
    normalized[cusip] = kind;
  }
  return normalized;
}

function normalizeSecurityFundIdentityPayload(data) {
  const source = data?.fund_identities;
  if (!Array.isArray(source)) return new Set();
  return new Set(
    source
      .map(value => String(value || "").trim().toUpperCase())
      .filter(Boolean)
  );
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-cache" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function ensureSecurityLabels() {
  if (!securityLabelsPromise) {
    securityLabelsPromise = fetchJson("data/security_labels.json")
      .then(data => {
        assertCompatibleDataContract(data, "data/security_labels.json");
        assertRequiredSecurityMetadata(data, "data/security_labels.json");
        securityLabels = normalizeSecurityTextMap(data.labels);
        securityKinds = normalizeSecurityKindPayload(data);
        securityProductNames = normalizeSecurityTextMap(data.product_names);
        securityFundIdentities = normalizeSecurityFundIdentityPayload(data);
        return securityLabels;
      })
      .catch(error => {
        // Kinds now participate in canonical stock routing. Rendering without
        // this versioned metadata could link a filing row to a removed legacy
        // option artifact, so a partial or failed load must fail closed.
        console.error("required security metadata load failed:", error);
        securityLabels = Object.create(null);
        securityKinds = Object.create(null);
        securityProductNames = Object.create(null);
        securityFundIdentities = new Set();
        throw (
          error instanceof DataContractMismatchError
          || error instanceof RequiredSiteDataError
        )
          ? error
          : new RequiredSiteDataError(
              "data/security_labels.json",
              error
            );
      });
  }
  return securityLabelsPromise;
}

function holdingInstrumentType(holding) {
  const putCall = String(holding?.put_call || "").trim().toUpperCase();
  return ["CALL", "PUT"].includes(putCall)
    ? putCall
    : normalizeInstrumentType(
        holding?.holding_type || holding?.instrument_type || holding?.option_type
      );
}

function holdingPublishedInstrumentType(holding) {
  const rawType = holdingInstrumentType(holding);
  const kind = securityKindForCusip(holding?.cusip);
  if (kind === "BOND") return "NOTE";
  if (
    securityHasEquityFundIdentity(holding?.cusip)
    && !["CALL", "PUT", "OPT"].includes(rawType)
  ) {
    return "EQUITY";
  }
  return rawType;
}

function securityDisplayTicker(ticker, instrumentType = "EQUITY") {
  const value = String(ticker || "").trim();
  if (!value) return "";
  return normalizeInstrumentType(instrumentType) === "NOTE"
    ? value
    : value.split(/\s+/)[0];
}

function securityLabelForCusip(cusip) {
  const key = String(cusip || "").trim().toUpperCase();
  const label = String(securityLabels[key] || "").trim();
  return label && label.toUpperCase() !== key ? label : "";
}

function securityKindForCusip(cusip) {
  const key = String(cusip || "").trim().toUpperCase();
  const kind = String(securityKinds[key] || "").trim().toUpperCase();
  return VALID_SECURITY_KINDS.has(kind) ? kind : "";
}

function securityHasEquityFundIdentity(cusip) {
  const key = String(cusip || "").trim().toUpperCase();
  return securityFundIdentities.has(key);
}

function securityProductNameForCusip(cusip) {
  const key = String(cusip || "").trim().toUpperCase();
  const name = String(securityProductNames[key] || "").trim();
  return name && name.toUpperCase() !== key ? name : "";
}

function holdingDisplayKind(holding) {
  const instrumentType = holdingInstrumentType(holding);
  const mappedKind = securityKindForCusip(holding?.cusip);
  // A registry-confirmed bond is stronger structural evidence than a legacy
  // filer row that happened to be parsed as an option.
  if (mappedKind === "BOND") return "BOND";
  if (instrumentType === "CALL" || instrumentType === "PUT") {
    return instrumentType;
  }
  if (instrumentType === "OPT") return "OPTION";
  if (
    securityHasEquityFundIdentity(holding?.cusip)
    && !["CALL", "PUT", "OPT"].includes(instrumentType)
  ) {
    return mappedKind || "EQUITY";
  }
  // Kinds are keyed by CUSIP while holdings are keyed by CUSIP + instrument
  // type.  A COMMON kind must never override an explicit note, preferred, or
  // warrant row that happens to reuse the same identifier.
  const compatibleKind = (
    mappedKind === "COMMON" && instrumentType !== "EQUITY"
  ) ? "" : mappedKind;
  return (
    compatibleKind
    || {
      EQUITY: "EQUITY",
      PREF: "PREFERRED",
      NOTE: "NOTE",
      WARRANT: "WARRANT",
    }[instrumentType]
  );
}

function holdingDisplayKindLabel(holding) {
  const kind = holdingDisplayKind(holding);
  return {
    COMMON: "Common Stock",
    PREFERRED: "Preferred",
    ETF: "ETF",
    ETN: "ETN",
    "MUTUAL FUND": "Mutual Fund",
    "CLOSED-END FUND": "Closed-End Fund",
    BOND: "Bond",
    WARRANT: "Warrant",
    RIGHT: "Right",
    UNIT: "Unit",
    EQUITY: "Equity",
    CALL: "Call",
    PUT: "Put",
    OPTION: "Option",
    NOTE: "Note",
  }[kind] || kind;
}

function holdingDisplayKindClass(holding) {
  const instrumentType = holdingInstrumentType(holding);
  const tagClass = type => (
    type === "EQUITY" ? "stock" : String(type || "EQUITY").toLowerCase()
  );
  const displayKind = holdingDisplayKind(holding);
  if (displayKind === "BOND") return tagClass("NOTE");
  if (["CALL", "PUT", "OPT"].includes(instrumentType)) {
    return tagClass(instrumentType);
  }
  const kindType = {
    COMMON: "EQUITY",
    PREFERRED: "PREF",
    ETF: "EQUITY",
    ETN: "NOTE",
    "MUTUAL FUND": "EQUITY",
    "CLOSED-END FUND": "EQUITY",
    BOND: "NOTE",
    WARRANT: "WARRANT",
    RIGHT: "WARRANT",
    UNIT: "EQUITY",
  }[displayKind];
  return tagClass(kindType || instrumentType);
}

function securityTypeFallbackLabel(instrumentType) {
  const type = normalizeInstrumentType(instrumentType);
  return {
    EQUITY: "Equity security",
    PREF: "Preferred security",
    NOTE: "Note security",
    WARRANT: "Warrant security",
    CALL: "Call option",
    PUT: "Put option",
    OPT: "Option security",
  }[type];
}

function holdingTrustedTicker(holding) {
  const ticker = securityDisplayTicker(
    holding?.ticker,
    holdingPublishedInstrumentType(holding)
  );
  const cusip = String(holding?.cusip || "").trim().toUpperCase();
  return ticker && (!cusip || ticker.toUpperCase() !== cusip) ? ticker : "";
}

function holdingDisplayLabel(holding) {
  if (!holding) return "—";
  const cusip = String(holding.cusip || "").trim().toUpperCase();
  const mappedLabel = securityLabelForCusip(cusip);
  const trustedTicker = holdingTrustedTicker(holding);
  const mappedSymbol = /^[A-Z][A-Z0-9.-]{0,15}(?:\/(?:W|WS|RT))?$/i.test(
    mappedLabel
  );
  // SEC labels describe even mapped stocks. Keep their verified symbol in
  // the primary label, while preserving structured note/preferred labels and
  // legacy symbol labels that correct an obsolete raw alias.
  const tickerFirst = [
    "COMMON", "EQUITY", "ETF", "MUTUAL FUND", "CLOSED-END FUND",
    "CALL", "PUT", "OPTION",
  ].includes(holdingDisplayKind(holding));
  if (trustedTicker && tickerFirst && !mappedSymbol) return trustedTicker;
  if (mappedLabel) return mappedLabel;
  if (trustedTicker) return trustedTicker;

  const informative = value => {
    const text = String(value || "").trim().replace(/\s+/g, " ");
    return text && (!cusip || text.toUpperCase() !== cusip) ? text : "";
  };
  const classText = informative(holding.class);
  const issuer = informative(holding.issuer);
  const genericType = securityTypeFallbackLabel(
    holdingPublishedInstrumentType(holding)
  );
  if (issuer && classText) return `${issuer} · ${classText}`;
  if (classText) return classText;
  if (issuer) return `${issuer} · ${genericType}`;
  return genericType;
}

function holdingDisplayCompany(holding) {
  if (!holding) return "";
  const cusip = String(holding.cusip || "").trim().toUpperCase();
  const mappedName = securityProductNameForCusip(cusip);
  if (
    mappedName
    && FUND_PRODUCT_NAME_KINDS.has(securityKindForCusip(cusip))
  ) return mappedName;
  const issuer = String(holding.issuer || "").trim().replace(/\s+/g, " ");
  return issuer && (!cusip || issuer.toUpperCase() !== cusip) ? issuer : "";
}

function securityLabelNeedsWrap(label) {
  const value = String(label || "").trim();
  return value.length > 12 || /\s/.test(value);
}

function securityTickerMark(ticker) {
  return String(ticker || "").trim().split(/\s+/)[0].slice(0, 6);
}

function stockLookupId(identifier, instrumentType = "EQUITY") {
  const symbol = String(identifier || "").trim().toUpperCase();
  const type = normalizeInstrumentType(instrumentType);
  if (!symbol) return "";
  return type === "EQUITY" ? symbol : `${symbol}|${type}`;
}

function parseStockLookupId(stockId) {
  const raw = String(stockId || "").trim();
  const sep = raw.lastIndexOf("|");
  if (sep === -1) {
    return { stock_id: raw.toUpperCase(), id_base: raw.toUpperCase(), instrument_type: "EQUITY" };
  }
  const idBase = raw.slice(0, sep).toUpperCase();
  const instrumentType = normalizeInstrumentType(raw.slice(sep + 1));
  return {
    stock_id: stockLookupId(idBase, instrumentType),
    id_base: idBase,
    instrument_type: instrumentType,
  };
}

function canonicalStockLookupId(stockId) {
  const parsed = parseStockLookupId(stockId);
  // Canonicalize direct and bookmarked CUSIP routes with the same structural
  // evidence used for fund-table links. This keeps legacy CALL/PUT debt URLs
  // from requesting option artifacts removed by the current data build.
  const kind = securityKindForCusip(parsed.id_base);
  if (kind === "BOND") return stockLookupId(parsed.id_base, "NOTE");
  if (
    securityHasEquityFundIdentity(parsed.id_base)
    && !["CALL", "PUT", "OPT"].includes(parsed.instrument_type)
  ) {
    return stockLookupId(parsed.id_base, "EQUITY");
  }
  return parsed.stock_id;
}

function stockFilePath(stockId) {
  const parsed = parseStockLookupId(stockId);
  const base = safeTicker(parsed.id_base);
  return parsed.instrument_type === "EQUITY"
    ? `data/stocks/${base}.json`
    : `data/stocks/${base}__${parsed.instrument_type}.json`;
}

function holdingHistoryKey(h) {
  return stockLookupId(
    h.cusip || h.ticker || "",
    holdingPublishedInstrumentType(h)
  );
}

function groupHoldingsByKey(holdings) {
  const grouped = new Map();
  for (const holding of (Array.isArray(holdings) ? holdings : [])) {
    const key = holdingHistoryKey(holding);
    const shares = Number(holding?.shares) || 0;
    const value = Number(holding?.value) || 0;
    const existing = grouped.get(key);
    if (!existing) {
      grouped.set(key, { ...holding, shares, value });
      continue;
    }
    existing.shares += shares;
    existing.value += value;
    // A grouped position is estimated if any contributing row is estimated.
    // Dropping this marker would make a partly estimated sum look exact.
    existing.shares_imputed = Boolean(
      existing.shares_imputed || holding?.shares_imputed
    );
    existing.quantity_unknown = Boolean(
      existing.quantity_unknown || holding?.quantity_unknown
    );
    for (const field of ["ticker", "issuer", "cusip", "holding_type", "option_type"]) {
      if (!existing[field] && holding?.[field]) existing[field] = holding[field];
    }
  }
  return [...grouped.values()];
}

function cikKey(cik) {
  const raw = String(cik ?? "").trim();
  if (!/^\d+$/.test(raw)) return "";
  return raw.replace(/^0+/, "") || "0";
}

function reportQuarterCode(dateStr) {
  const m = String(dateStr || "").match(/^(\d{4})-(03-31|06-30|09-30|12-31)$/);
  if (!m) return null;
  const quarter = { "03-31": 1, "06-30": 2, "09-30": 3, "12-31": 4 }[m[2]];
  return Number(`${m[1]}${quarter}`);
}

function quarterCodeLabel(code) {
  const m = String(code || "").match(/^(\d{4})([1-4])$/);
  return m ? `Q${m[2]} ${m[1]}` : "—";
}

function quarterCodeOrdinal(code) {
  const m = String(code || "").match(/^(\d{4})([1-4])$/);
  return m ? (Number(m[1]) * 4) + Number(m[2]) : null;
}

function areAdjacentQuarterCodes(newer, older) {
  const newerOrdinal = quarterCodeOrdinal(newer);
  const olderOrdinal = quarterCodeOrdinal(older);
  return newerOrdinal != null &&
    olderOrdinal != null &&
    newerOrdinal - olderOrdinal === 1;
}

function areAdjacentReportDates(newer, older) {
  return areAdjacentQuarterCodes(
    reportQuarterCode(newer),
    reportQuarterCode(older)
  );
}

function hasContiguousQuarterCodes(calendar) {
  if (!Array.isArray(calendar) || !calendar.length) return false;
  return calendar.every(
    (code, i) => i === 0 || areAdjacentQuarterCodes(calendar[i - 1], code)
  );
}

function modalLatestReportingQuarter(funds) {
  const counts = new Map();
  for (const fund of (Array.isArray(funds) ? funds : [])) {
    // Withheld and invalid calendars cannot define what "current" means for
    // the rest of the site.  Otherwise a broad quarantine wave (or one bad
    // index row) could drag the production baseline backward.
    if (fundWithheldMetadata(fund)) continue;
    const latest = validFundCalendar(fund)?.[0] ?? null;
    if (latest == null) continue;
    counts.set(latest, (counts.get(latest) || 0) + 1);
  }
  let mode = null;
  let modeCount = 0;
  for (const [quarter, count] of counts) {
    // Prefer the newer quarter if an exact tie occurs during a filing-season
    // transition. A manager ahead of the mode remains current; only older
    // managers are stale.
    if (count > modeCount || (count === modeCount && (mode == null || quarter > mode))) {
      mode = quarter;
      modeCount = count;
    }
  }
  return mode;
}

function fundWithheldMetadata(fundIndexEntry) {
  if (!fundIndexEntry || typeof fundIndexEntry !== "object") return null;
  const dateFields = [
    "latest_withheld_report_date",
    "withheld_report_date",
    "latest_quarantined_report_date",
    "quarantined_report_date",
    "latest_pending_report_date",
    "pending_report_date",
  ];
  const dateField = dateFields.find(field => Boolean(fundIndexEntry[field]));
  const flagFields = [
    "withheld",
    "has_withheld_filing",
    "quarantined",
    "has_quarantined_filing",
  ];
  const flagged = flagFields.some(field => fundIndexEntry[field] === true);
  const statusFields = [
    "status",
    "filing_status",
    "data_status",
    "latest_filing_status",
    "quarantine_status",
  ];
  const status = statusFields
    .map(field => String(fundIndexEntry[field] || "").trim().toUpperCase())
    .find(value =>
      !/^(?:NO|NOT|RESOLVED|CLEARED)_/.test(value) &&
      /(?:^|_)(?:WITHHELD|QUARANTINED|PENDING|BLOCKED|FAILED|ERROR|UNKNOWN)(?:$|_)/.test(value)
    ) || "";
  if (!dateField && !flagged && !status) return null;
  return {
    reportDate: dateField ? String(fundIndexEntry[dateField]) : "",
    reason: String(
      fundIndexEntry.withheld_reason ||
      fundIndexEntry.quarantine_reason ||
      fundIndexEntry.status_reason ||
      ""
    ),
    status: status || "WITHHELD",
  };
}

function validFundCalendar(fundIndexEntry) {
  const rawCalendar = fundIndexEntry?.q;
  const calendar = Array.isArray(rawCalendar) ? [...rawCalendar] : [];
  const valid = calendar.length > 0 &&
    calendar.length <= 4 &&
    calendar.every(code => Number.isInteger(code) && /^\d{4}[1-4]$/.test(String(code))) &&
    new Set(calendar).size === calendar.length &&
    calendar.every((code, i) => i === 0 || calendar[i - 1] > code);
  return valid ? calendar : null;
}

function fundUnverifiedReportDates(fundIndexEntry) {
  if (
    !fundIndexEntry ||
    !Object.prototype.hasOwnProperty.call(
      fundIndexEntry,
      "unverified_report_dates"
    )
  ) {
    return [];
  }
  const dates = fundIndexEntry?.unverified_report_dates;
  if (!Array.isArray(dates)) return null;
  if (!dates.length) return [];
  const calendar = validFundCalendar(fundIndexEntry);
  if (
    !calendar ||
    dates.some(
      date => typeof date !== "string" || reportQuarterCode(date) == null
    ) ||
    new Set(dates).size !== dates.length ||
    dates.some((date, i) => i > 0 && dates[i - 1] <= date) ||
    dates.some(date => !calendar.includes(reportQuarterCode(date)))
  ) {
    return null;
  }
  return [...dates];
}

function fundIndexFilingState(fundIndexEntry, currentQuarter) {
  const withheld = fundWithheldMetadata(fundIndexEntry);
  const unverifiedReportDates = fundUnverifiedReportDates(fundIndexEntry);
  const safeUnverifiedReportDates = unverifiedReportDates || [];
  if (withheld) {
    return {
      state: "WITHHELD",
      calendar: validFundCalendar(fundIndexEntry) || [],
      withheld,
      unverifiedReportDates: safeUnverifiedReportDates,
    };
  }
  const calendar = validFundCalendar(fundIndexEntry);
  if (
    !calendar ||
    !Number.isInteger(currentQuarter) ||
    unverifiedReportDates === null
  ) {
    return {
      state: "UNKNOWN",
      calendar: calendar || [],
      withheld: null,
      unverifiedReportDates: safeUnverifiedReportDates,
    };
  }
  return {
    state: calendar[0] < currentQuarter ? "STALE" : "CURRENT",
    calendar,
    withheld: null,
    unverifiedReportDates: safeUnverifiedReportDates,
  };
}

function provenSplitFactorForPeriod(adjustments, previousDate, currentDate) {
  if (!previousDate || !currentDate) return null;
  const rows = Array.isArray(adjustments)
    ? adjustments
    : (adjustments && typeof adjustments === "object" ? [adjustments] : []);
  const matches = rows.filter(row =>
    row?.proven === true &&
    row.from_report_date === previousDate &&
    row.to_report_date === currentDate &&
    typeof row.factor === "number" &&
    Number.isFinite(row.factor) &&
    row.factor > 0
  );
  return matches.length === 1 ? matches[0].factor : null;
}

function looksLikeUnverifiedSplit(currentShares, previousShares) {
  if (!(currentShares > 0) || !(previousShares > 0)) return false;
  const ratio = Math.max(currentShares, previousShares) /
    Math.min(currentShares, previousShares);
  if (ratio < 1.8) return false;
  const commonFactors = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 50, 100];
  return commonFactors.some(factor => Math.abs(ratio - factor) / factor <= 0.05);
}

function shareTrendIsComparable(records, adjustments) {
  if (!Array.isArray(records) || !records.length) return false;
  if (records.some(record =>
    !record ||
    record.shares_imputed === true ||
    record.quantity_unknown === true ||
    typeof record.shares !== "number" ||
    !Number.isFinite(record.shares)
  )) {
    return false;
  }
  for (let i = 1; i < records.length; i += 1) {
    const previous = records[i - 1];
    const current = records[i];
    if (
      provenSplitFactorForPeriod(
        adjustments,
        previous.date,
        current.date
      ) != null ||
      looksLikeUnverifiedSplit(current.shares, previous.shares)
    ) {
      // Raw share counts are not comparable across a corporate action. Keep
      // value history visible, but suppress the share sparkline rather than
      // drawing a false manager trade.
      return false;
    }
  }
  return true;
}

function positionChange(current, previous, options = {}) {
  // An inferred share count cannot support a share-based filing-to-filing
  // change. Suppress NEW and EXIT too when the only side is estimated.
  if (current?.shares_imputed || previous?.shares_imputed ||
      current?.quantity_unknown || previous?.quantity_unknown) return null;
  if (!current) return previous ? { t: "EXIT", p: 100 } : null;
  if (!previous) return { t: "NEW" };
  const currentShares = Number(current.shares) || 0;
  const rawPreviousShares = Number(previous.shares) || 0;
  const splitFactor = Number(options?.splitFactor);
  const hasProvenSplit = Number.isFinite(splitFactor) && splitFactor > 0;
  if (!hasProvenSplit && looksLikeUnverifiedSplit(currentShares, rawPreviousShares)) {
    // A near-integer jump could be a corporate action. Until a generated,
    // explicitly proven adjustment is present, showing a percentage would be
    // more misleading than showing no comparison.
    return null;
  }
  const previousShares = hasProvenSplit
    ? rawPreviousShares * splitFactor
    : rawPreviousShares;
  const equalTolerance = Math.max(1e-9, Math.abs(previousShares) * 1e-9);
  if (Math.abs(currentShares - previousShares) <= equalTolerance) return { t: "SAME" };
  if (previousShares === 0) {
    return { t: currentShares > 0 ? "UP" : "DOWN", p: null };
  }
  const pct = ((currentShares - previousShares) / previousShares) * 100;
  return { t: pct > 0 ? "UP" : "DOWN", p: Math.abs(pct) };
}

// Align a sparse positive-position history to a manager's filing calendar and
// the production-wide current-quarter baseline. Stale, withheld, and invalid
// managers deliberately fail closed and can never enter current aggregates.
function alignHolderHistory(history, fundIndexEntry, currentQuarter, options = {}) {
  const filingState = fundIndexFilingState(fundIndexEntry, currentQuarter);
  const calendar = filingState.calendar;
  if (!calendar.length) {
    return {
      state: filingState.state === "WITHHELD" ? "WITHHELD" : "UNKNOWN",
      calendar: [],
      current: null,
      previous: null,
      reference: null,
      withheld: filingState.withheld,
      ch: null,
      sparkQuarters: [],
      sparkShares: [],
      sparkValues: [],
    };
  }

  const records = Array.isArray(history) ? history : [];
  const byQuarter = new Map();
  for (const record of records) {
    const code = reportQuarterCode(record?.date);
    if (!code || byQuarter.has(code)) {
      return { state: "UNKNOWN", calendar: [], current: null, previous: null, ch: null, sparkQuarters: [], sparkShares: [], sparkValues: [] };
    }
    byQuarter.set(code, record);
  }

  const current = byQuarter.get(calendar[0]) || null;
  const comparisonsVerified = filingState.state !== "UNKNOWN";
  const unverifiedCodes = new Set(
    filingState.unverifiedReportDates.map(reportQuarterCode)
  );
  const hasAdjacentPrevious = comparisonsVerified &&
    calendar.length > 1 &&
    areAdjacentQuarterCodes(calendar[0], calendar[1]) &&
    !unverifiedCodes.has(calendar[0]) &&
    !unverifiedCodes.has(calendar[1]);
  const previous = hasAdjacentPrevious
    ? (byQuarter.get(calendar[1]) || null)
    : null;
  const latestKnown = [...byQuarter.entries()]
    .sort((a, b) => b[0] - a[0])[0]?.[1] || null;
  let state;
  if (filingState.state === "WITHHELD") state = "WITHHELD";
  else if (filingState.state === "STALE") state = "STALE";
  else if (filingState.state === "UNKNOWN") state = "UNKNOWN";
  else {
    state = current
      ? "CURRENT"
      : (calendar.length > 1 && previous ? "EXIT" : "HISTORICAL");
  }
  const chronological = [...calendar].reverse();
  const contiguousCalendar = comparisonsVerified &&
    hasContiguousQuarterCodes(calendar) &&
    calendar.every(code => !unverifiedCodes.has(code));
  const chronologicalRecords = chronological.map(
    code => byQuarter.get(code) || null
  );
  const comparableShareHistory = contiguousCalendar &&
    shareTrendIsComparable(
      chronologicalRecords,
      options?.splitAdjustments
    );
  const splitFactor = provenSplitFactorForPeriod(
    options?.splitAdjustments,
    previous?.date,
    current?.date
  );
  return {
    state,
    calendar,
    current,
    previous,
    reference: current || latestKnown,
    withheld: filingState.withheld,
    ch: state === "CURRENT" && hasAdjacentPrevious
      ? positionChange(current, previous, { splitFactor })
      : null,
    // A missing reporting quarter is not an evenly-spaced 4Q trend.  Hide
    // both series instead of compressing a gap into a misleading sparkline.
    sparkQuarters: contiguousCalendar ? chronological : [],
    sparkShares: comparableShareHistory
      ? chronologicalRecords.map(record => record.shares)
      : [],
    sparkValues: contiguousCalendar
      ? chronological.map(code => Number(byQuarter.get(code)?.value) || 0)
      : [],
  };
}

function partitionHolderStates(classified) {
  const rows = Array.isArray(classified) ? classified : [];
  return {
    current: rows.filter(row => row.state === "CURRENT"),
    exits: rows.filter(row => row.state === "EXIT"),
    stale: rows.filter(row => row.state === "STALE"),
    withheld: rows.filter(row => row.state === "WITHHELD"),
    historical: rows.filter(row => row.state === "HISTORICAL"),
    unknown: rows.filter(row => row.state === "UNKNOWN"),
  };
}

function exactReportedShareTotal(rows) {
  return (Array.isArray(rows) ? rows : [])
    .filter(row => !row.sharesImputed && !row.quantityUnknown)
    .reduce((sum, row) => sum + (Number(row.shares) || 0), 0);
}

function aggregateEligibleHolderTrends(
  classified,
  currentQuarter,
  width = 4
) {
  const eligible = (Array.isArray(classified) ? classified : [])
    .filter(row =>
      row.state === "CURRENT" ||
      row.state === "EXIT" ||
      row.state === "HISTORICAL"
    );
  if (!eligible.length || !Number.isInteger(currentQuarter)) {
    return { quarters: [], values: [], shares: [] };
  }

  const currentOrdinal = quarterCodeOrdinal(currentQuarter);
  const quarters = currentOrdinal == null
    ? []
    : Array.from({ length: width }, (_, index) => {
        const ordinal = currentOrdinal - (width - 1 - index);
        const year = Math.floor((ordinal - 1) / 4);
        const quarter = ((ordinal - 1) % 4) + 1;
        return Number(`${year}${quarter}`);
      });

  // An aggregate trend is only meaningful for a fixed reporting cohort.
  // Mixing managers on different filing calendars makes coverage changes look
  // like ownership changes. The window must also end at the page's stated
  // current-quarter baseline; an ahead-filing cohort cannot silently move the
  // chart endpoint into a newer quarter.
  const sameCalendar = quarters.length === width &&
    eligible.every(row =>
      Array.isArray(row.sparkQuarters) &&
      row.sparkQuarters.length === width &&
      row.sparkQuarters.every((quarter, index) => quarter === quarters[index])
    );
  const completeValues = sameCalendar && eligible.every(row =>
    Array.isArray(row.valueSparkData) &&
    row.valueSparkData.length === width &&
    row.valueSparkData.every(
      value => typeof value === "number" && Number.isFinite(value)
    )
  );
  if (!completeValues) return { quarters: [], values: [], shares: [] };

  const values = Array.from({ length: width }, (_, index) =>
    eligible.reduce(
      (sum, row) => sum + Number(row.valueSparkData[index]),
      0
    )
  );
  const completeShares = eligible.every(row =>
    Array.isArray(row.sparkData) &&
    row.sparkData.length === width &&
    row.sparkData.every(
      shareCount =>
        typeof shareCount === "number" && Number.isFinite(shareCount)
    )
  );
  const shares = completeShares
    ? Array.from({ length: width }, (_, index) =>
        eligible.reduce(
          (sum, row) => sum + Number(row.sparkData[index]),
          0
        )
      )
    : [];
  return { quarters, values, shares };
}

function searchTypeRank(type) {
  const t = normalizeInstrumentType(type);
  if (t === "EQUITY") return 0;
  if (t === "PREF") return 1;
  if (t === "NOTE") return 2;
  if (t === "WARRANT") return 3;
  return 4;
}

function searchEntryTagClass(entry) {
  return holdingDisplayKindClass({
    cusip: entry?.cusip,
    holding_type: entry?.instrument_type,
  });
}

function searchEntryTagLabel(entry) {
  return holdingDisplayKindLabel({
    cusip: entry?.cusip,
    holding_type: entry?.instrument_type,
  });
}

function tickerSearchSymbol(entry) {
  const mappedLabel = securityLabelForCusip(entry?.cusip);
  const mappedSymbol = /^[A-Z][A-Z0-9.-]{0,15}(?:\/(?:W|WS|RT))?$/i.test(
    mappedLabel
  ) ? mappedLabel : "";
  return String(
    mappedSymbol
    || entry?.ticker
    || entry?.cusip
    || entry?.stock_id
    || ""
  ).trim();
}

function tickerSearchVisualKey(entry) {
  return JSON.stringify([
    searchEntryTagClass(entry),
    searchEntryTagLabel(entry),
    tickerSearchSymbol(entry).toUpperCase(),
  ]);
}

function tickerSearchHolderCount(entry) {
  const count = entry?.holder_count;
  return Number.isInteger(count) && count >= 0 ? count : -1;
}

function tickerSearchCurrentHolderCount(entry) {
  const count = entry?.current_holder_count;
  return Number.isInteger(count) && count >= 0 ? count : -1;
}

function tickerSearchLastSeen(entry) {
  const value = String(entry?.last_seen || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : "";
}

function tickerSearchIsActive(entry) {
  const baseline = (
    typeof currentReportingQuarter === "number"
      ? currentReportingQuarter
      : null
  );
  const lastSeenQuarter = reportQuarterCode(tickerSearchLastSeen(entry));
  return baseline != null &&
    lastSeenQuarter != null &&
    lastSeenQuarter >= baseline;
}

function compareTickerAliasRepresentative(a, b) {
  const activeCmp =
    Number(tickerSearchIsActive(b)) - Number(tickerSearchIsActive(a));
  if (activeCmp !== 0) return activeCmp;
  const currentHolderCountCmp =
    tickerSearchCurrentHolderCount(b) -
    tickerSearchCurrentHolderCount(a);
  if (currentHolderCountCmp !== 0) return currentHolderCountCmp;
  const holderCountCmp =
    tickerSearchHolderCount(b) - tickerSearchHolderCount(a);
  if (holderCountCmp !== 0) return holderCountCmp;
  return String(a?.stock_id || "").localeCompare(
    String(b?.stock_id || "")
  );
}

function dedupeVisuallyIdenticalTickerMatches(matches) {
  const orderedKeys = [];
  const winners = new Map();
  for (const entry of matches) {
    const key = tickerSearchVisualKey(entry);
    const incumbent = winners.get(key);
    if (!incumbent) {
      orderedKeys.push(key);
      winners.set(key, entry);
    } else if (compareTickerAliasRepresentative(entry, incumbent) < 0) {
      winners.set(key, entry);
    }
  }
  return orderedKeys.map(key => winners.get(key));
}

function isCommonStockSearchEntry(entry) {
  if (normalizeInstrumentType(entry.instrument_type) !== "EQUITY") return false;
  if (["PREFERRED", "RIGHT", "UNIT", "WARRANT", "BOND"].includes(
    securityKindForCusip(entry.cusip)
  )) return false;
  const ticker = tickerSearchSymbol(entry).toUpperCase();
  if (/\b(?:WT|WARRANT|WARRANTS|RIGHT|RIGHTS)\b/.test(ticker)) return false;
  if (/(?:[-./](?:R|RT|RIGHT|RIGHTS|W|WS|WT|WTS))$/.test(ticker)) return false;
  return true;
}

function normalizeTickerEntry(entry) {
  if (!entry) return null;
  if (typeof entry === "string") {
    return {
      ticker: entry,
      issuer: "",
      cusip: "",
      instrument_type: "EQUITY",
      stock_id: stockLookupId(entry, "EQUITY"),
      last_seen: null,
      current_holder_count: null,
      holder_count: null,
    };
  }
  const ticker = String(entry.ticker || entry.symbol || "").trim();
  const instrumentType = normalizeInstrumentType(entry.instrument_type || entry.type);
  const stockId = String(entry.stock_id || stockLookupId(entry.cusip || ticker, instrumentType)).trim().toUpperCase();
  const parsed = parseStockLookupId(stockId);
  if (!ticker && !parsed.id_base) return null;
  const currentHolderCount = tickerSearchCurrentHolderCount(entry);
  const holderCount = tickerSearchHolderCount(entry);
  const lastSeen = tickerSearchLastSeen(entry);
  return {
    ticker,
    issuer: entry.issuer || "",
    cusip: parsed.id_base,
    instrument_type: parsed.instrument_type,
    stock_id: parsed.stock_id,
    last_seen: lastSeen || null,
    current_holder_count:
      currentHolderCount >= 0 ? currentHolderCount : null,
    holder_count: holderCount >= 0 ? holderCount : null,
  };
}

function compareTickerMatch(a, b) {
  const typeCmp = searchTypeRank(a.instrument_type) - searchTypeRank(b.instrument_type);
  if (typeCmp !== 0) return typeCmp;
  if (a._matchRank !== b._matchRank) return a._matchRank - b._matchRank;
  const aSymbol = tickerSearchSymbol(a);
  const bSymbol = tickerSearchSymbol(b);
  if (aSymbol.length !== bSymbol.length) return aSymbol.length - bSymbol.length;
  const tickerCmp = aSymbol.localeCompare(bSymbol);
  if (tickerCmp !== 0) return tickerCmp;
  return (a.stock_id || "").localeCompare(b.stock_id || "");
}

function resolveStockEntry(stockId) {
  const raw = String(stockId || "").trim();
  if (!raw) return null;
  const exact = (idx.tickers || []).find(entry => String(entry.stock_id || "").toUpperCase() === raw.toUpperCase());
  if (exact) return exact;
  const legacy = parseStockLookupId(raw);
  const matches = (idx.tickers || []).filter(entry =>
    String(entry.ticker || "").toUpperCase() === legacy.id_base &&
    normalizeInstrumentType(entry.instrument_type) === legacy.instrument_type
  );
  if (!matches.length) return null;
  matches.sort(compareTickerAliasRepresentative);
  return matches[0];
}

// ---------- sparkline + badge helpers ----------
function spark(arr) {
  if (!arr || !arr.length || arr.every(v => !v)) {
    return `<span style="color:var(--mt);font-size:11px">—</span>`;
  }
  const positives = arr.filter(v => v > 0);
  const mx = positives.length ? Math.max(...positives) : 0;
  if (!mx) return `<span style="color:var(--mt);font-size:11px">—</span>`;
  const w = 8;
  let s = `<svg width="${arr.length * w + 2}" height="20" style="vertical-align:middle">`;
  arr.forEach((v, i) => {
    if (!v || v <= 0) return;
    const bh = Math.max(1, (v / mx) * 18);
    const pv = i > 0 ? (arr[i - 1] || 0) : v;
    const col = v > pv ? "var(--gn)" : v < pv ? "var(--rd)" : "var(--mt)";
    s += `<rect x="${i * w}" y="${20 - bh}" width="6" height="${bh}" fill="${col}" rx="1"/>`;
  });
  return s + "</svg>";
}

function badge(ch) {
  if (!ch) return `<span class="badge mono" style="color:var(--mt)">—</span>`;
  const positive = ch.t === "NEW" || ch.t === "UP";
  const negative = ch.t === "EXIT" || ch.t === "DOWN";
  const bg = positive ? "var(--gn-soft)" : negative ? "var(--rd-soft)" : "transparent";
  const color = positive ? "var(--gn)" : negative ? "var(--rd)" : "var(--mt)";
  return `<span class="badge mono" style="background:${bg};color:${color}">${changeText(ch)}</span>`;
}

function displayDate(dateStr) {
  if (!dateStr) return "—";
  const value = String(dateStr).trim();
  if (/^\d{4}-\d{2}$/.test(value)) {
    const year = Number(value.slice(0, 4));
    const month = Number(value.slice(5, 7));
    if (year < 1 || month < 1 || month > 12) return value;
    const d = new Date(`${value}-01T00:00:00`);
    return d.toLocaleDateString(undefined, { month: "short", year: "numeric" });
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  const d = new Date(`${value}T00:00:00`);
  const [year, month, day] = value.split("-").map(Number);
  const isExactDate = year >= 1
    && !Number.isNaN(d.getTime())
    && d.getFullYear() === year
    && d.getMonth() + 1 === month
    && d.getDate() === day;
  if (!isExactDate) return value;
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function fundTicker(h) {
  return holdingDisplayLabel(h);
}

// Display-only cleanup for SEC legal names; stored names remain untouched.
const _legalEntitySuffixes = new Set([
  "INC", "INCORPORATED", "CORP", "CORPORATION", "CO",
  "LTD", "LIMITED", "LLC", "LLP", "LLLP", "LP", "PLC",
  "AG", "SA", "NV", "BV", "GMBH", "LTDA",
]);
const _entityTailMarkers = new Set([
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
  "DC", "PR", "DEL", "DELAWARE", "NEW", "N Y", "ADV", "BD", "UK",
  "UNITED KINGDOM", "CAN", "CANADA", "ON", "FI", "CN",
  "MARSHALL ISLANDS", "BERMUDA", "CAYMAN ISLANDS",
]);
const _bareEntityTailMarkers = new Set([
  "DE", "DEL", "DELAWARE", "NEW", "N Y", "NY", "CA", "MA", "IL", "PA", "OH",
]);

function normalizeTerminalLegalInitialism(value) {
  return value
    .replace(/\bL\s*\.?\s*L\s*\.?\s*C\s*\.?$/i, "LLC")
    .replace(/\bL\s*\.?\s*L\s*\.?\s*P\s*\.?$/i, "LLP")
    .replace(/\bP\s*\.?\s*L\s*\.?\s*C\s*\.?$/i, "PLC")
    .replace(/\bL\s*\.?\s*P\s*\.?$/i, "LP");
}

function legalSuffixAtEnd(value) {
  const normalized = normalizeTerminalLegalInitialism(value)
    .replace(/[\s,.;:]+$/g, "")
    .trim();
  return normalized.match(/([A-Za-z.]+)$/)?.[1]?.replace(/\./g, "").toUpperCase() || "";
}

function stripKnownEntityTailMarker(value) {
  const match = value.match(/^(.*?)(?:\s*[\\/]\s*([^\\/()]+?)\s*\/?|\s*\(\s*([^()]+?)\s*\))\s*$/);
  if (!match) return value;
  const marker = String(match[2] || match[3] || "")
    .replace(/[._-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
  return _entityTailMarkers.has(marker) ? match[1].trim() : value;
}

function stripLegalEntitySuffixes(name) {
  const raw = String(name || "").replace(/\s+/g, " ").trim();
  if (!raw) return "";

  let cleaned = stripKnownEntityTailMarker(raw)
    .replace(/[\s,.;:]+$/g, "")
    .trim();
  cleaned = normalizeTerminalLegalInitialism(cleaned);

  cleaned = cleaned.replace(/\bN\s+Y$/i, "NY");
  const jurisdiction = cleaned.match(/^(.*)\s+([A-Za-z]+)$/);
  if (jurisdiction) {
    const candidate = normalizeTerminalLegalInitialism(jurisdiction[1].trim());
    const marker = jurisdiction[2].replace(/\s+/g, " ").trim().toUpperCase();
    if (_bareEntityTailMarkers.has(marker) && _legalEntitySuffixes.has(legalSuffixAtEnd(candidate))) {
      cleaned = candidate;
    }
  }

  while (cleaned) {
    cleaned = normalizeTerminalLegalInitialism(cleaned);
    const match = cleaned.match(/^(.*?)(?:\s*,\s*|\s+)([A-Za-z.]+)$/);
    if (!match) break;
    const prefix = match[1].trim();
    const suffix = match[2].replace(/\./g, "").toUpperCase();
    if (!_legalEntitySuffixes.has(suffix)) break;
    if (suffix === "CO" && /&\s*$/.test(prefix)) break;
    cleaned = prefix.replace(/[\s,.;:]+$/g, "").trim();
  }

  return cleaned || raw.replace(/[\s,.;:]+$/g, "").trim();
}

function displayEntityName(name) {
  return titleCase(stripLegalEntitySuffixes(name))
    .replace(/\bINC\b\.*/g, "Inc.")
    .replace(/\bCORP\b\.*/g, "Corp.")
    .replace(/\bCO\b\.*/g, "Co.")
    .replace(/\bLTD\b\.*/g, "Ltd.");
}

function displayFundName(name) {
  return displayEntityName(name);
}

function fundSearchIdentity(fund) {
  const name = displayFundName(fund?.name);
  const cik = cikKey(fund?.cik);
  return cik ? `${name} · CIK ${cik}` : name;
}

function displayIssuer(name) {
  return displayEntityName(name)
    .replace(/\bIshares\b/g, "iShares")
    .replace(/\bIBONDS\b/g, "iBonds")
    .replace(/\bIpath\b/g, "iPath")
    .replace(/\/([a-z]{2,})\//g, (_, part) => `/${part.toUpperCase()}/`);
}

function displayHolderName(name) {
  return displayFundName(name);
}

function changeText(ch) {
  if (!ch) return "—";
  if (ch.t === "NEW") return "NEW";
  if (ch.t === "EXIT") return "EXIT";
  if (ch.t === "UP") return Number.isFinite(ch.p) ? `+${Math.abs(ch.p).toFixed(1)}%` : "INCREASED";
  if (ch.t === "DOWN") return Number.isFinite(ch.p) ? `-${Math.abs(ch.p).toFixed(1)}%` : "REDUCED";
  return "—";
}

function changeClass(ch) {
  if (!ch) return "";
  if (ch.t === "UP") return "summary-up";
  if (ch.t === "DOWN") return "summary-down";
  if (ch.t === "NEW") return "summary-new";
  if (ch.t === "EXIT") return "summary-exit";
  return "";
}

function miniLine(values) {
  const nums = (values || []).map(v => Number(v) || 0);
  if (!nums.length || nums.every(v => !v)) return "";
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const span = max - min || 1;
  const w = 114, h = 42, pad = 3;
  const step = nums.length > 1 ? (w - pad * 2) / (nums.length - 1) : 0;
  const points = nums.map((v, i) => {
    const x = pad + i * step;
    const y = h - pad - ((v - min) / span) * (h - pad * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return `<svg class="mini" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" aria-hidden="true">
    <polygon points="${pad},${h-pad} ${points} ${w-pad},${h-pad}" fill="var(--ac-soft)"/>
    <polyline points="${points}" fill="none" stroke="var(--ac)" stroke-width="1.8"/>
  </svg>`;
}

function miniBars(values) {
  const nums = (values || []).map(v => Number(v) || 0);
  if (!nums.length || nums.every(v => !v)) return "";
  const mx = Math.max(...nums) || 1;
  const bars = nums.map((v, i) => {
    const bh = Math.max(5, (v / mx) * 36);
    const x = i * 13;
    return `<rect x="${x}" y="${40 - bh}" width="8" height="${bh}" fill="${i === nums.length - 1 ? "var(--ac)" : "var(--bd2)"}" rx="1"/>`;
  }).join("");
  return `<svg class="mini" width="${nums.length * 13}" height="42" viewBox="0 0 ${nums.length * 13} 42" aria-hidden="true">${bars}</svg>`;
}

function donut(pct) {
  const clamped = Math.max(0, Math.min(100, pct || 0));
  return `<div class="mini-donut mini" style="--pct:${clamped}%"></div>`;
}

function bankIcon() {
  return `<svg class="stock-icon" viewBox="0 0 40 40" aria-hidden="true">
    <path d="M5 15h30L20 7 5 15Z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>
    <path d="M8 31h24M6 35h28M10 15v16M17 15v16M23 15v16M30 15v16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
  </svg>`;
}

function holdersIcon() {
  return `<svg class="stock-icon" viewBox="0 0 40 40" aria-hidden="true">
    <path d="M15 18a5 5 0 1 0 0-10 5 5 0 0 0 0 10Zm10 1a4 4 0 1 0 0-8 4 4 0 0 0 0 8ZM6 31c1.2-6 4.3-9 9-9s7.8 3 9 9M21 23c5.4.2 8.6 2.8 9.6 8" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
  </svg>`;
}

function summaryEvent(label, row, emptyText) {
  if (!row) {
    return `<div class="summary-label">${label}</div><div class="summary-list">${emptyText}</div>`;
  }
  return `<div class="summary-label">${label}</div>
    <div class="summary-row">
      <span class="ticker">${esc(fundTicker(row))}</span>
      <span class="summary-company">${esc(displayIssuer(holdingDisplayCompany(row)))}</span>
      <span class="value ${esc(changeClass(row.ch))}">${esc(changeText(row.ch))}</span>
    </div>`;
}

function statCard(kind, label, value, visual) {
  return `<div class="${kind}-stat">
            <div>
              <div class="${kind}-stat-label">${label}</div>
              <div class="${kind}-stat-value mono">${value}</div>
            </div>
            ${visual}
          </div>`;
}

function holdingIdentityCells(h, rowBg = "") {
  const lookupId = stockLookupId(h.cusip || h.ticker, holdingPublishedInstrumentType(h));
  const displayLabel = fundTicker(h);
  const companyName = displayIssuer(holdingDisplayCompany(h)) || "—";
  const background = rowBg ? `background:${rowBg};` : "";
  const securityCell = lookupId
    ? `<td class="mono col-sticky security-label-cell" style="font-weight:600;color:var(--ac);cursor:pointer;${background}white-space:nowrap" ><a class="security-link" href="#stock/${esc(encodeURIComponent(lookupId))}">${esc(displayLabel)}</a></td>`
    : `<td class="mono col-sticky security-label-cell" style="color:var(--mt);font-size:11px;${background}white-space:nowrap">${esc(displayLabel)}</td>`;
  return `${securityCell}
      <td title="${esc(companyName)}" style="font-weight:500;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(companyName)}</td>`;
}

function holderFundCell(holder, rowBg = "") {
  const background = rowBg ? `;background:${rowBg}` : "";
  return `<td class="col-sticky" style="font-weight:600;cursor:pointer;color:var(--ac);max-width:310px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap${background}" ><a class="fund-link" href="#fund/${esc(cikKey(holder.cik))}">${esc(displayHolderName(holder.name || ""))}</a></td>`;
}


// ---------- curated browse lists ----------
// Stable identities that should always lead the popular-filer list when they
// are present. CIK matching keeps placement intact if an SEC filer name changes.
const PINNED_POPULAR_FUND_CIKS = [1940272]; // ADAR1 Capital Management, LLC

// Popular investors that users are likely to look up first. We match the rest
// by substring against the actual index fund names, so any name that isn't in
// the pulled data silently drops out (rather than producing a dead card).
const POPULAR_FUND_NEEDLES = [
  "BERKSHIRE HATHAWAY",
  "PERSHING SQUARE",
  "RENAISSANCE TECHNOLOGIES",
  "CITADEL ADVISORS",
  "BRIDGEWATER ASSOCIATES",
  "SOROS FUND MANAGEMENT",
  "BAUPOST GROUP",
  "GREENLIGHT CAPITAL",
  "APPALOOSA",
  "TIGER GLOBAL",
  "COATUE",
  "D1 CAPITAL",
  "ARK INVESTMENT",
  "RTW INVESTMENTS",
  "SEQUOIA FINANCIAL",
  "TWO SIGMA",
  "MILLENNIUM MANAGEMENT",
  "POINT72",
  "THIRD POINT",
  "DUQUESNE",
  "ICAHN",
  "VIKING GLOBAL",
  "OAKTREE",
  "LONE PINE",
];

// ---------- global state ----------
let idx = null;                  // lightweight fund bootstrap + lazy ticker search
let fundIndexByCik = new Map();  // one O(1) reporting-calendar lookup per holder
let currentReportingQuarter = null; // modal latest quarter across valid fund calendars
let searchDebounce = null;
let searchIndexPromise = null;
let searchIndexError = null;
let dataContractBlocked = false;
// Cap the in-memory JSON caches so a long browsing session can't grow
// unbounded — fund and stock payloads can be ~100KB+ each and a visitor
// jumping through hundreds of filers would otherwise pin all of them in RAM.
// Same .get/.set surface as Map, with LRU eviction on overflow.
class LRUCache {
  constructor(max) { this.max = max; this.m = new Map(); }
  get(k) {
    if (!this.m.has(k)) return undefined;
    const v = this.m.get(k);
    this.m.delete(k); this.m.set(k, v);  // mark most-recently-used
    return v;
  }
  set(k, v) {
    if (this.m.has(k)) this.m.delete(k);
    else if (this.m.size >= this.max) this.m.delete(this.m.keys().next().value);
    this.m.set(k, v);
  }
}
const fundCache = new LRUCache(50);
const stockCache = new LRUCache(50);

// Sort state persists across navigations so users who like a particular
// ordering keep it. Default: largest % holder first.
let fundSort  = { col: "pct",   dir: "desc" };   // % of portfolio, descending
let stockSort = { col: "value", dir: "desc" };   // dollar value, descending (see note in renderStock)

// Current table rows — kept in outer scope so header clicks can re-sort
// without needing to re-fetch or re-enrich.
let curFundRows  = [];
let curStockRows = [];
let fundHoldingsFilter = "";
let stockHoldersFilter = "";
let stockPage = 1;
let stockRowsPerPage = 15;

const $ = id => document.getElementById(id);
const app = () => $("app");

function showLoadingMessage(message) {
  if (dataContractBlocked) {
    showDataMaintenance();
    return;
  }
  app().innerHTML = `
    <div class="empty">
      <div><span class="spinner"></span>${esc(message)}</div>
    </div>`;
}

function showLoadError(title, path) {
  if (dataContractBlocked) {
    showDataMaintenance();
    return;
  }
  app().innerHTML = `
    <div class="empty">
      <h1>${esc(title)}</h1>
      <p>The file <span class="mono">${esc(path)}</span> is missing or unreachable.</p>
      <p style="margin-top:16px"><a href="#">← Back to search</a></p>
    </div>`;
}

function showDataMaintenance() {
  dataContractBlocked = true;
  $("gsearchWrap").style.display = "none";
  $("backBtn").style.display = "none";
  app().innerHTML = `
    <div class="empty">
      <h1>Data update in progress</h1>
      <p>The site is publishing a compatible filings data build. Please check back shortly.</p>
    </div>`;
}

function handleDataContractError(error) {
  if (
    !(error instanceof DataContractMismatchError)
    && !(error instanceof RequiredSiteDataError)
  ) return false;
  console.error("required site data is incompatible or unavailable:", error);
  showDataMaintenance();
  return true;
}

function enterDetailView(kind, id, opts = {}) {
  if (dataContractBlocked) {
    showDataMaintenance();
    return false;
  }
  clearAllSearchInputs();
  $("gsearchWrap").style.display = "";
  $("backBtn").style.display = "";
  if (opts.updateUrl !== false) setUrl(kind, id);
  return true;
}

async function loadCachedJson({
  cache,
  cacheKey,
  url,
  loadingMessage,
  errorTitle,
  logLabel,
}) {
  let data = cache.get(cacheKey);
  if (data) return data;

  showLoadingMessage(loadingMessage);
  try {
    data = await fetchJson(url);
    cache.set(cacheKey, data);
    return data;
  } catch (e) {
    console.error(`${logLabel} fetch failed:`, e);
    showLoadError(errorTitle, url);
    return null;
  }
}

function sortableHeader(handlerName, col, label, align) {
  return `<th class="sort" data-col="${esc(col)}" aria-sort="none" style="text-align:${align}"><button type="button" class="sort-button" data-action="${handlerName === "onFundSort" ? "fund-sort" : "stock-sort"}" data-col="${esc(col)}">${esc(label)}<span class="arr" aria-hidden="true"></span></button></th>`;
}

// ---------- sort helpers ----------
function chgToNum(ch) {
  // Maps filing-to-filing badge states to a sortable number. NEW is "infinitely
  // up" (+Inf), while exits are "infinitely down" (-Inf).
  if (!ch) return 0;
  if (ch.t === "NEW")  return Number.POSITIVE_INFINITY;
  if (ch.t === "EXIT") return Number.NEGATIVE_INFINITY;
  if (ch.t === "UP")   return  (ch.p || 0);
  if (ch.t === "DOWN") return -(ch.p || 0);
  return 0;
}

function sortRows(rows, col, dir, kind) {
  // kind: "fund" or "stock" — same function body, but the field accessors differ
  const mul = dir === "asc" ? 1 : -1;
  const key = (r) => {
    if (kind === "fund") {
      switch (col) {
        case "ticker":  return (holdingDisplayLabel(r) || "~~~").toUpperCase();
        case "issuer":  return (holdingDisplayCompany(r) || "~~~").toUpperCase();
        case "pct":     return r.pct      || 0;
        case "prevPct": return r.prevPct  || 0;
        case "pctChg":  return chgToNum(r.ch);
        case "holdingType": return holdingDisplayKind(r);
        case "value":   return r.value    || 0;
        case "shares":  return r.shares   || 0;
      }
    } else {
      switch (col) {
        case "name":    return (r.name    || "~~~").toUpperCase();
        case "shares":  return r.shares   || 0;
        case "value":   return r.value    || 0;
        case "pct":     return r.pctOfFund || 0;    // pre-computed in stock JSON
        case "qoq":     return chgToNum(r.ch);
        case "asOfDate": return r.asOfDate || "";
      }
    }
    return 0;
  };
  return [...rows].sort((a, b) => {
    const va = key(a), vb = key(b);
    if (va < vb) return -1 * mul;
    if (va > vb) return  1 * mul;
    return 0;
  });
}

function updateSortArrows(tableId, sort) {
  const table = $(tableId);
  if (!table) return;
  table.querySelectorAll("th.sort").forEach(th => {
    const col = th.dataset.col;
    th.setAttribute("aria-sort", col === sort.col ? (sort.dir === "asc" ? "ascending" : "descending") : "none");
    const arr = th.querySelector(".arr");
    if (col === sort.col) {
      th.classList.add("active");
      arr.textContent = sort.dir === "asc" ? "↑" : "↓";
    } else {
      th.classList.remove("active");
      arr.textContent = "";
    }
  });
}

// Default direction when switching to a new column: text cols → asc, numbers → desc
const DEFAULT_DIR = {
  ticker: "asc", issuer: "asc", name: "asc",
  pct: "desc", prevPct: "desc", pctChg: "desc",
  value: "desc", shares: "desc", qoq: "desc",
};

function toggleSort(sort, col) {
  sort.dir = sort.col === col
    ? sort.dir === "asc" ? "desc" : "asc"
    : DEFAULT_DIR[col] || "desc";
  sort.col = col;
}

function onFundSort(col) {
  toggleSort(fundSort, col);
  curFundRows = sortRows(curFundRows, fundSort.col, fundSort.dir, "fund");
  renderFundTbody();
  updateSortArrows("fundTable", fundSort);
}

function onStockSort(col) {
  toggleSort(stockSort, col);
  curStockRows = sortRows(curStockRows, stockSort.col, stockSort.dir, "stock");
  stockPage = 1;
  renderStockTbody();
  updateSortArrows("stockTable", stockSort);
}

function setStockPage(page) {
  stockPage = Math.max(1, parseInt(page, 10) || 1);
  renderStockTbody();
}

function setStockRowsPerPage(value) {
  const next = parseInt(value, 10);
  stockRowsPerPage = [15, 25, 50, 100].includes(next) ? next : 15;
  stockPage = 1;
  renderStockTbody();
}

function focusStockSort(col, dir = "desc") {
  stockSort = { col, dir };
  curStockRows = sortRows(curStockRows, col, dir, "stock");
  stockPage = 1;
  renderStockTbody();
  updateSortArrows("stockTable", stockSort);
  const panel = $("stockPanel");
  if (panel) panel.scrollIntoView({ behavior: "smooth", block: "start" });
}

function stockPaginationPages(totalPages, currentPage) {
  if (totalPages <= 7) return Array.from({ length: totalPages }, (_, i) => i + 1);
  if (currentPage <= 4) return [1, 2, 3, 4, 5, "ellipsis", totalPages];
  if (currentPage >= totalPages - 3) {
    return [1, "ellipsis", totalPages - 4, totalPages - 3, totalPages - 2, totalPages - 1, totalPages];
  }
  return [1, "ellipsis", currentPage - 1, currentPage, currentPage + 1, "ellipsis", totalPages];
}

// ---------- bootstrap + lazy search index ----------
function normalizeSearchTickers(data) {
  return Array.isArray(data?.tickers)
    ? data.tickers.map(normalizeTickerEntry).filter(Boolean)
    : [];
}

async function fetchBootstrapIndex() {
  try {
    const data = await fetchJson("data/funds-index.json");
    assertCompatibleDataContract(data, "data/funds-index.json");
    if (!data || !Array.isArray(data.funds) || data.funds.length === 0) {
      throw new Error("funds-index.json has no funds");
    }
    data.tickers = [];
    return data;
  } catch (error) {
    if (error instanceof DataContractMismatchError) throw error;
    // Safe rollout fallback for a stale Pages deployment or an older checkout.
    console.warn("funds-index.json load failed; falling back to index.json:", error);
    const data = await fetchJson("data/index.json");
    assertCompatibleDataContract(data, "data/index.json");
    data.tickers = normalizeSearchTickers(data);
    return data;
  }
}

async function ensureSearchIndex() {
  if (Array.isArray(idx?.tickers) && idx.tickers.length) return idx.tickers;
  if (!searchIndexPromise) {
    searchIndexError = null;
    searchIndexPromise = fetchJson("data/index.json")
      .then(data => {
        assertCompatibleDataContract(data, "data/index.json");
        const tickers = normalizeSearchTickers(data);
        if (!tickers.length) throw new Error("index.json has no ticker entries");
        idx.tickers = tickers;
        return tickers;
      })
      .catch(error => {
        searchIndexError = error;
        searchIndexPromise = null;
        throw error;
      });
  }
  return searchIndexPromise;
}

function warmSearchIndex() {
  if (Array.isArray(idx?.tickers) && idx.tickers.length) return;
  const load = () => ensureSearchIndex().catch(error => {
    if (!handleDataContractError(error)) {
      console.error("background search index load failed:", error);
    }
  });
  if ("requestIdleCallback" in window) {
    window.requestIdleCallback(load, { timeout: 1500 });
  } else {
    setTimeout(load, 0);
  }
}

function stockIdNeedsSearchIndex(stockId) {
  const parsed = parseStockLookupId(stockId);
  return !/^[A-Z0-9]{9}$/.test(parsed.id_base);
}

// ---------- init ----------
(async function init() {
  try {
    const securityLabelsReady = ensureSecurityLabels();
    idx = await fetchBootstrapIndex();
    if (!idx || !Array.isArray(idx.funds) || idx.funds.length === 0) {
      showEmpty();
      return;
    }
    fundIndexByCik = new Map(
      idx.funds
        .map(fund => [cikKey(fund?.cik), fund])
        .filter(([key]) => key)
    );
    currentReportingQuarter = modalLatestReportingQuarter(idx.funds);
    if (!currentReportingQuarter) {
      throw new Error("funds-index.json has no valid reporting-quarter baseline");
    }
    await securityLabelsReady;
    wireGlobalSearch();
    wireSiteInteractions();
    wireUrlRouting();
    // Fund pages and the home page no longer wait for the full ticker index.
    // Direct ticker hashes resolve through ensureSearchIndex() in loadStock().
    routeFromHash();
    warmSearchIndex();
  } catch (e) {
    if (!handleDataContractError(e)) {
      console.error("bootstrap index load failed:", e);
      showEmpty();
    }
  }
})();

// ---------- URL routing ----------
// Hash-based routing: #fund/1067983, #stock/AAPL, or no hash for home.
// We use the URL fragment rather than real paths because GitHub Pages serves
// the same index.html for any path, and real paths would require a 404.html
// redirect trick. The fragment is invisible to the server and lets us route
// client-side cleanly.
function wireSiteInteractions() {
  // Dynamic views use links and a finite action vocabulary. Dataset values
  // never become JavaScript source, and native buttons support Enter/Space.
  document.addEventListener("click", event => {
    const target = event.target.closest("a[href^='#'], button[data-action]");
    if (!target) return;
    if (target.tagName === "A") {
      if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
      const hash = target.getAttribute("href");
      event.preventDefault();
      closeGlobalSearch();
      if (hash === "#") { goHome(); return; }
      const match = hash.match(/^#(fund|stock)\/(.+)$/);
      if (!match) return;
      let id;
      try { id = decodeURIComponent(match[2]); } catch { return; }
      if (match[1] === "fund") {
        if (/^\d{1,10}$/.test(id)) loadFund(Number(id));
      } else {
        loadStock(id);
      }
      return;
    }
    const { action, col, dir, page } = target.dataset;
    switch (action) {
      case "home": goHome(); break;
      case "fund-sort": onFundSort(col); break;
      case "stock-sort": onStockSort(col); break;
      case "stock-focus-sort": focusStockSort(col, dir); break;
      case "stock-page": setStockPage(page); break;
      case "clear-holdings":
        fundHoldingsFilter = "";
        $("fundHoldingsSearch").value = "";
        renderFundTbody();
        $("fundHoldingsSearch").focus();
        break;
      case "clear-holders":
        stockHoldersFilter = "";
        $("stockHoldersSearch").value = "";
        stockPage = 1;
        renderStockTbody();
        $("stockHoldersSearch").focus();
        break;
    }
  });
  document.addEventListener("input", event => {
    const input = event.target;
    if (input.id === "homeSearch") {
      $("gsearch").value = input.value;
      clearTimeout(searchDebounce);
      searchDebounce = setTimeout(() => globalSearch(input.value), 150);
    } else if (input.id === "fundHoldingsSearch") {
      fundHoldingsFilter = input.value;
      renderFundTbody();
    } else if (input.id === "stockHoldersSearch") {
      stockHoldersFilter = input.value;
      stockPage = 1;
      renderStockTbody();
    }
  });
  document.addEventListener("change", event => {
    if (event.target.id === "stockRowsPerPage") setStockRowsPerPage(event.target.value);
  });
  document.addEventListener("focusin", event => {
    if (event.target.id === "homeSearch" && event.target.value) globalSearch(event.target.value);
  });
  document.addEventListener("keydown", event => {
    if (event.target.id !== "homeSearch") return;
    if (event.key === "Escape") {
      clearTimeout(searchDebounce);
      event.target.value = "";
      $("gsearch").value = "";
      closeGlobalSearch();
    } else if (event.key === "Enter") {
      const first = document.querySelector(".gsearch-results.open .gsearch-item");
      if (first) { event.preventDefault(); first.click(); }
    }
  });
}

function wireUrlRouting() {
  // User pressed back/forward, or edited the hash in the URL bar
  window.addEventListener("hashchange", () => routeFromHash());
  window.addEventListener("popstate",   () => routeFromHash());
}

function routeFromHash() {
  if (dataContractBlocked) {
    showDataMaintenance();
    return;
  }
  const hash = (location.hash || "").replace(/^#/, "");
  const m = hash.match(/^(fund|stock)\/(.+)$/);
  if (!m) {
    // No hash → home. Don't push another history entry for this (it's the
    // current URL already).
    goHome({ updateUrl: false });
    return;
  }
  const [, kind, id] = m;
  if (kind === "fund") {
    const cik = parseInt(id, 10);
    if (Number.isFinite(cik)) loadFund(cik, { updateUrl: false });
    else goHome({ updateUrl: false });
  } else if (kind === "stock") {
    let stockId;
    try { stockId = decodeURIComponent(id); }
    catch { goHome({ updateUrl: false }); return; }
    loadStock(stockId, { updateUrl: false });
  }
}

function setUrl(kind, id) {
  // Push a new history entry so browser back works. replaceState would
  // swallow the previous entry, which is wrong semantically.
  const newHash = kind === "home" ? "" : `#${kind}/${encodeURIComponent(id)}`;
  const newUrl = location.pathname + location.search + newHash;
  if (location.pathname + location.search + (location.hash || "") !== newUrl) {
    history.pushState({ kind, id }, "", newUrl);
  }
}

// ---------- curated browse lookups ----------
// Run once per index load; results are cached so home-page renders are free.
let _popularFundsCache = null;
function getPopularFunds() {
  if (_popularFundsCache) return _popularFundsCache;
  const out = [];
  const seen = new Set();
  for (const cik of PINNED_POPULAR_FUND_CIKS) {
    const match = idx.funds.find(f => f.cik === cik);
    if (match) {
      out.push(match);
      seen.add(match.cik);
    }
  }
  for (const needle of POPULAR_FUND_NEEDLES) {
    const match = idx.funds.find(
      f => !seen.has(f.cik) && (f.name || "").toUpperCase().includes(needle)
    );
    if (match) {
      out.push(match);
      seen.add(match.cik);
    }
    if (out.length >= 24) break;
  }
  _popularFundsCache = out;
  return out;
}

// ---------- global unified search ----------
// Header search that finds both funds and tickers in one dropdown. Debounced
// input, click-to-navigate, Enter picks the first result, Escape closes,
// outside click closes.
function wireGlobalSearch() {
  const inp = $("gsearch");
  const results = $("gsearchResults");
  if (!inp || !results) return;

  inp.addEventListener("input", () => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => globalSearch(inp.value), 150);
  });

  inp.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      const first = results.querySelector(".gsearch-item");
      if (first) first.click();
    } else if (e.key === "Escape") {
      closeGlobalSearch();
      inp.blur();
    }
  });

  // Outside click closes the dropdown
  document.addEventListener("click", (e) => {
    if (!inp.parentElement.contains(e.target)) closeGlobalSearch();
  });

  // Auto-focus: typing anywhere focuses the visible search input
  document.addEventListener("keydown", (e) => {
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    if (e.key.length !== 1 || e.key === " ") return;
    const active = document.activeElement;
    if (active && (active.tagName === "INPUT" || active.tagName === "TEXTAREA")) return;
    // Find the first visible search input on the page
    for (const el of document.querySelectorAll(".search-bar input, .gsearch-input")) {
      if (el.offsetParent !== null) { el.focus(); return; }
    }
  });
}

function closeGlobalSearch() {
  document.querySelectorAll(".gsearch-results").forEach(el => {
    el.classList.remove("open");
    el.innerHTML = "";
  });
}

function clearAllSearchInputs() {
  document.querySelectorAll(".search-bar input, .gsearch-input").forEach(el => { el.value = ""; });
  closeGlobalSearch();
}

function getVisibleResults() {
  for (const el of document.querySelectorAll(".gsearch-results")) {
    if (el.offsetParent !== null || el.parentElement?.offsetParent !== null) return el;
  }
  return $("gsearchResults");
}

function currentSearchQueryMatches(q) {
  return Array.from(document.querySelectorAll(".search-bar input, .gsearch-input"))
    .some(el => (el.value || "").trim().toUpperCase() === q);
}

function globalSearch(q) {
  const results = getVisibleResults();
  if (!results) return;
  q = (q || "").trim().toUpperCase();
  if (!q) { closeGlobalSearch(); return; }

  // Fund matching is available from the lightweight bootstrap immediately.
  const fundMatches = [];
  for (const f of idx.funds) {
    if ((f.name || "").toUpperCase().includes(q)) {
      fundMatches.push(f);
      if (fundMatches.length >= 8) break;
    }
  }
  const fundBlock = fundMatches.length ? `
    <div class="gsearch-section">Funds</div>
    ${fundMatches.map(f => `
      <a class="gsearch-item" href="#fund/${esc(cikKey(f.cik))}">
        <span class="gsearch-tag fund">FUND</span>
        <span style="font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(fundSearchIdentity(f))}</span>
      </a>
    `).join("")}` : "";

  // The larger ticker index warms after first paint. If a visitor searches
  // before it is ready, show fund results now and refresh this same query once
  // stock search becomes available.
  if (!Array.isArray(idx.tickers) || idx.tickers.length === 0) {
    const stockStatus = searchIndexError
      ? "Stock search is temporarily unavailable."
      : "Loading stock search…";
    results.innerHTML = fundBlock + `<div class="gsearch-empty">${stockStatus}</div>`;
    results.classList.add("open");
    if (!searchIndexError) {
      ensureSearchIndex()
        .then(() => {
          if (currentSearchQueryMatches(q)) globalSearch(q);
        })
        .catch(error => {
          if (handleDataContractError(error)) return;
          console.error("search index load failed:", error);
          if (currentSearchQueryMatches(q)) globalSearch(q);
        });
    }
    return;
  }

  // Tickers: surface common equities plus ticker-based funds and ETNs.
  // Options and non-common capital-structure rows remain reachable from fund
  // pages, but showing them here is noisy for the primary search workflow.
  const tickerMatches = [];
  for (const entry of idx.tickers) {
    if (!isCommonStockSearchEntry(entry)) continue;
    const symbol = tickerSearchSymbol(entry).toUpperCase();
    const productName = securityProductNameForCusip(entry.cusip).toUpperCase();
    if (!symbol) continue;
    if (symbol === q) tickerMatches.push({ ...entry, _matchRank: 0 });
    else if (symbol.startsWith(q)) tickerMatches.push({ ...entry, _matchRank: 1 });
    else if (symbol.includes(q)) tickerMatches.push({ ...entry, _matchRank: 2 });
    else if (productName.includes(q)) tickerMatches.push({ ...entry, _matchRank: 3 });
  }
  tickerMatches.sort(compareTickerMatch);
  const topTickerMatches = dedupeVisuallyIdenticalTickerMatches(
    tickerMatches
  ).slice(0, 8);

  if (!topTickerMatches.length && !fundMatches.length) {
    results.innerHTML = `<div class="gsearch-empty">No matches for "${esc(q)}"</div>`;
    results.classList.add("open");
    return;
  }

  const tickerBlock = topTickerMatches.length ? `
    <div class="gsearch-section">Tickers</div>
    ${topTickerMatches.map(t => `
      <a class="gsearch-item" href="#stock/${esc(encodeURIComponent(t.stock_id || t.ticker))}">
        <span class="gsearch-tag ${esc(searchEntryTagClass(t))}">${esc(searchEntryTagLabel(t))}</span>
        <div style="min-width:0;display:flex;flex-direction:column">
          <span class="mono" style="font-weight:700;color:var(--ac)">${esc(tickerSearchSymbol(t))}</span>
          <span title="${esc(displayIssuer(holdingDisplayCompany(t) || t.cusip || t.stock_id))}" style="font-size:12px;color:var(--mt);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(displayIssuer(holdingDisplayCompany(t) || t.cusip || t.stock_id))}</span>
        </div>
      </a>
    `).join("")}` : "";

  // Exact ticker hits lead; otherwise funds lead.
  const tickersFirst = topTickerMatches.some(t => t._matchRank === 0);
  results.innerHTML = tickersFirst ? tickerBlock + fundBlock : fundBlock + tickerBlock;
  results.classList.add("open");
}

function showEmpty() {
  app().innerHTML = `
    <div class="empty">
      <h1>No data yet</h1>
      <p>The pipeline is running for the first time. Check back in a few hours — this site is fed by weekly SEC EDGAR downloads, and the initial backfill can take a while.</p>
    </div>`;
}

// ---------- navigation ----------
function goHome(opts = {}) {
  if (dataContractBlocked) {
    showDataMaintenance();
    return;
  }
  $("backBtn").style.display = "none";
  $("gsearchWrap").style.display = "none";
  clearTimeout(searchDebounce);
  clearAllSearchInputs();
  if (opts.updateUrl !== false) setUrl("home", "");
  if (!idx) { showEmpty(); return; }
  renderFundsHome();
}

// ---------- FUND: home ----------
function renderFundsHome() {
  const n = idx.funds.length;
  const updated = idx.last_updated ? new Date(idx.last_updated).toLocaleDateString() : "—";
  const popular = getPopularFunds();

  app().innerHTML = `
    <div class="home-hero">
      <div class="home-copy">
        <h1>Track What Institutional Funds Are Buying</h1>
        <p class="home-subtitle">
          Searchable portfolios from <span class="mono home-filer-count">${n.toLocaleString()}</span>
          institutional 13F filers · updated ${esc(updated)}
        </p>
      </div>
      <div class="search-bar-wrap">
        <div class="search-bar">
          <input id="homeSearch" aria-label="Search funds or tickers" placeholder="Search funds or tickers…" autocomplete="off" spellcheck="false"/>
        </div>
        <div class="gsearch-results"></div>
      </div>
    </div>
    <div id="fundBrowse"></div>`;

  if (!popular.length) {
    $("fundBrowse").innerHTML = `
      <div style="text-align:center;color:var(--mt);padding:30px;font-size:13px">
        No popular filers found in this snapshot — use the search above.
      </div>`;
    return;
  }

  $("fundBrowse").innerHTML = `
    <div class="lbl">Popular filers</div>
    <div class="grid">
      ${popular.map(f => `
        <a class="card" href="#fund/${esc(cikKey(f.cik))}">
          <div class="popular-name">${esc(displayFundName(f.name))}</div>
          <div class="mono popular-cik">CIK ${esc(f.cik)}</div>
        </a>
      `).join("")}
    </div>`;
}

// ---------- FUND: detail ----------
async function loadFund(cik, opts = {}) {
  if (!enterDetailView("fund", cik, opts)) return;
  const securityLabelsReady = ensureSecurityLabels();
  const fund = await loadCachedJson({
    cache: fundCache,
    cacheKey: cik,
    url: `data/funds/${cik}.json`,
    loadingMessage: `Loading fund ${cik}...`,
    errorTitle: `Couldn't load fund ${cik}`,
    logLabel: "fund",
  });
  await securityLabelsReady;
  if (dataContractBlocked) {
    showDataMaintenance();
    return;
  }
  if (!fund) return;
  renderFund(fund);
}

function renderFund(f) {
  $("backBtn").style.display = "block";
  fundHoldingsFilter = "";

  const fundIndexEntry = fundIndexByCik.get(cikKey(f.cik));
  const filingState = fundIndexFilingState(
    fundIndexEntry,
    currentReportingQuarter
  );
  let fundStatusNotice = "";
  if (filingState.state === "WITHHELD") {
    const withheldDate = filingState.withheld?.reportDate;
    fundStatusNotice = `
      <div class="info-note"><strong>Newer filing data is withheld from current statistics.</strong>
      ${withheldDate ? `The unresolved filing reports through ${esc(displayDate(withheldDate))}. ` : ""}
      Holdings shown below are the last published snapshot and remain excluded from sitewide current-holder totals until validation succeeds.</div>`;
  } else if (filingState.state === "STALE") {
    fundStatusNotice = `
      <div class="info-note"><strong>Stale filing snapshot.</strong>
      This manager's latest published quarter is ${esc(quarterCodeLabel(filingState.calendar[0]))}, behind the current site baseline of ${esc(quarterCodeLabel(currentReportingQuarter))}.
      These holdings remain available for historical reference but are excluded from sitewide current-holder totals.</div>`;
  } else if (filingState.state === "UNKNOWN") {
    fundStatusNotice = `
      <div class="info-note"><strong>Reporting status could not be verified.</strong>
      This fund's filing calendar is missing or invalid, so its holdings are excluded from sitewide current-holder totals.</div>`;
  }
  if (
    filingState.state !== "WITHHELD" &&
    filingState.unverifiedReportDates.length
  ) {
    fundStatusNotice += `
      <div class="info-note"><strong>Some historical filing data is unverified.</strong>
      Quarter-over-quarter changes and 4Q trends crossing ${esc(filingState.unverifiedReportDates.map(displayDate).join(", "))} are hidden until SEC replay succeeds. Current verified holdings remain available.</div>`;
  }

  const rawQuarters = Array.isArray(f.quarters) ? f.quarters : [];
  if (!rawQuarters.length) {
    app().innerHTML = `
      <div style="margin-bottom:20px">
        <h2>${esc(displayFundName(f.name || ""))}</h2>
        <span class="mono" style="color:var(--mt);font-size:13px">CIK ${esc(f.cik)}</span>
      </div>
      ${fundStatusNotice}
      <div class="info-note">No quarter data available for this fund yet.</div>`;
    return;
  }

  // Canonicalize duplicate instrument rows before every comparison. All fund
  // page counts, changes, exits, and trends then share the same position key.
  const quarters = rawQuarters
    .slice(0, 4)
    .map(q => ({
      ...q,
      holdings: groupHoldingsByKey(q.holdings),
    }));

  // Pipeline emits quarters newest-first; reverse for chart axis (oldest-first)
  const cur = quarters[0];
  const priorCandidate = quarters[1] || null;
  const comparisonsVerified = filingState.state !== "UNKNOWN";
  const unverifiedReportDates = new Set(
    filingState.unverifiedReportDates
  );
  const prev = comparisonsVerified &&
    priorCandidate &&
    areAdjacentReportDates(cur.report_date, priorCandidate.report_date) &&
    !unverifiedReportDates.has(cur.report_date) &&
    !unverifiedReportDates.has(priorCandidate.report_date)
    ? priorCandidate
    : null;
  const quartersAsc = [...quarters].reverse();
  const contiguousQuarterHistory = comparisonsVerified &&
    quarters.every(
    (q, i) => i === 0 ||
      areAdjacentReportDates(quarters[i - 1].report_date, q.report_date)
  ) && quarters.every(
    q => !unverifiedReportDates.has(q.report_date)
  );

  // Build per-security history across filings for changes and sparklines.
  const holdingHistories = {}; // position key -> date -> quantity/value record
  quarters.forEach(q => {
    (q.holdings || []).forEach(h => {
      const key = holdingHistoryKey(h);
      if (!holdingHistories[key]) holdingHistories[key] = {};
      holdingHistories[key][q.report_date] = {
        date: q.report_date,
        shares: h.shares || 0,
        value: h.value || 0,
        shares_imputed: h.shares_imputed === true,
        quantity_unknown: h.quantity_unknown === true,
      };
    });
  });

  // Enrich current quarter's holdings
  const enriched = (cur.holdings || []).map(h => {
    const key = holdingHistoryKey(h);
    const history = holdingHistories[key] || {};
    const prevRec = prev ? history[prev.report_date] : null;
    const pct = cur.total_value > 0 ? (h.value / cur.total_value) * 100 : 0;
    const prevPct = (prevRec && prev && prev.total_value > 0)
      ? (prevRec.value / prev.total_value) * 100
      : 0;

    // Use the same independently proven, exact-date split adjustment that is
    // published on stock pages. The bootstrap map avoids one network request
    // per holding while keeping unproven corporate actions fail-closed.
    const splitFactor = provenSplitFactorForPeriod(
      idx?.proven_split_adjustments?.[key],
      prev?.report_date,
      cur.report_date
    );
    const ch = prev
      ? positionChange(h, prevRec, { splitFactor })
      : null;

    const shareHistory = quartersAsc.map(
      q => history[q.report_date] || null
    );
    const sparkData = contiguousQuarterHistory &&
      shareTrendIsComparable(
        shareHistory,
        idx?.proven_split_adjustments?.[key]
      )
      ? shareHistory.map(record => record.shares)
      : [];
    return { ...h, pct, prevPct, ch, sparkData };
  });
  enriched.sort((a, b) => (b.value || 0) - (a.value || 0));

  // A position present in the immediately prior published filing but absent
  // from the latest one is an exit. Keep these rows separate so they cannot
  // leak into current position counts, values, or concentration statistics.
  const currentKeys = new Set((cur.holdings || []).map(holdingHistoryKey));
  const exited = prev
    ? (prev.holdings || [])
        .filter(h => !currentKeys.has(holdingHistoryKey(h)))
        .map(h => {
          const key = holdingHistoryKey(h);
          const history = holdingHistories[key] || {};
          const prevPct = prev.total_value > 0 ? ((h.value || 0) / prev.total_value) * 100 : 0;
          const shareHistory = quartersAsc.map(
            q => history[q.report_date] || null
          );
          return {
            ...h,
            pct: 0,
            prevPct,
            ch: positionChange(null, h),
            sparkData: contiguousQuarterHistory &&
              shareTrendIsComparable(
                shareHistory,
                idx?.proven_split_adjustments?.[key]
              )
              ? shareHistory.map(record => record.shares)
              : [],
          };
        })
        .sort((a, b) => (b.value || 0) - (a.value || 0))
    : [];

  const top5 = enriched.slice(0, 5);
  const top5Pct = top5.reduce((sum, h) => sum + (h.pct || 0), 0);
  const top = top5[0] || null;
  const valuesAsc = contiguousQuarterHistory
    ? quartersAsc.map(q => q.total_value || 0)
    : [];
  const positionsAsc = contiguousQuarterHistory
    ? quartersAsc.map(q => (q.holdings || []).length)
    : [];
  const largestIncrease = enriched.filter(h => h.ch?.t === "UP")
    .sort((a, b) => (b.ch.p || 0) - (a.ch.p || 0))[0];
  const largestReduction = [
    ...enriched.filter(h => h.ch?.t === "DOWN"),
    ...exited,
  ].sort((a, b) =>
    ((b.ch?.p || 0) - (a.ch?.p || 0)) ||
    ((b.value || 0) - (a.value || 0))
  )[0];
  const newPositions = enriched.filter(h => h.ch?.t === "NEW").slice(0, 5);
  const reducedPositions = enriched.filter(h => h.ch?.t === "DOWN").slice(0, 8);

  // Save the enriched rows into outer scope so header clicks can re-sort
  // without needing to re-run the whole fund-enrichment pipeline.
  curFundRows = sortRows(enriched, fundSort.col, fundSort.dir, "fund");
  const sec13fUrl = sec13fFilingsUrl(f);
  const fundExitRows = exited.map((h, i) => {
    return `<tr>
      <td class="mono col-rank" style="text-align:center;color:var(--mt);font-size:11px">${i + 1}</td>
      ${holdingIdentityCells(h)}
      <td class="mono" style="text-align:right;color:var(--mt)">${fP(h.prevPct)}</td>
      <td class="mono" style="text-align:right;font-weight:600">${fV(h.value)}</td>
      <td class="mono" style="text-align:right">${formatShares(h.shares, h.shares_imputed, h.quantity_unknown)}</td>
      <td style="text-align:center">${badge(h.ch)}</td>
      <td style="text-align:center">${spark(h.sparkData)}</td>
      <td style="color:var(--tx);white-space:nowrap">${esc(holdingDisplayKindLabel(h))}</td>
    </tr>`;
  }).join("");
  const fundExitPanel = prev ? `
        <section class="fund-panel exit-panel">
          <div class="fund-panel-head">
            <div class="fund-panel-title">Exited in Latest Filing<span class="panel-count">${exited.length.toLocaleString()}</span></div>
          </div>
          <div class="tbl-wrap"><table id="fundExitTable"><thead><tr style="background:var(--sf)">
            <th class="col-rank" style="text-align:center;width:36px">#</th>
            <th class="col-sticky" style="text-align:left;background:var(--sf)">Security</th>
            <th style="text-align:left">Company</th>
            <th style="text-align:right">Prior %</th>
            <th style="text-align:right">Prior Value</th>
            <th style="text-align:right">Prior Shares</th>
            <th style="text-align:center">Change</th>
            <th style="text-align:center">Trend (4Q)</th>
            <th style="text-align:left">Type</th>
          </tr></thead><tbody>${fundExitRows || `<tr><td colspan="9" style="text-align:center;color:var(--mt);padding:30px">No positions exited in this filing.</td></tr>`}</tbody></table></div>
          <div class="fund-foot">Compared with the immediately prior published filing (${esc(displayDate(prev.report_date))}). Prior values are shown for context and are not included in current totals.</div>
        </section>` : "";

  let html = `
    <div class="fund-page">
      <main class="fund-main">
        <div class="fund-title">
          <h1>${esc(displayFundName(f.name || ""))}</h1>
          <div class="fund-meta">
            <span>CIK ${esc(f.cik)}</span>
            <span>Source date ${esc(displayDate(cur.report_date))}</span>
            <span>Filed ${esc(displayDate(cur.filing_date))}</span>
            <span>Quarterly 13F</span>
            ${sec13fUrl ? `<span><a href="${esc(sec13fUrl)}" target="_blank" rel="noopener noreferrer">View SEC 13F filings ↗</a></span>` : ""}
          </div>
        </div>

        ${fundStatusNotice}
        <div class="fund-stat-grid">
          ${statCard("fund", "13F Equity Value", fV(cur.total_value), miniLine(valuesAsc))}
          ${statCard("fund", "Positions", enriched.length.toLocaleString(), miniBars(positionsAsc))}
          <div class="fund-stat">
            <div class="top-position-copy">
              <div class="fund-stat-label">Top Position</div>
              <div class="fund-stat-value mono top-position-value ${top && securityLabelNeedsWrap(fundTicker(top)) ? "security-label-position" : ""}"><span class="top-position-symbol">${esc(fundTicker(top))}</span><span class="top-position-percent">${top ? fP(top.pct) : "—"}</span></div>
            </div>
          </div>
          ${statCard("fund", "Top 5 Concentration", fP(top5Pct), donut(top5Pct))}
        </div>

        <section class="fund-panel">
          <div class="fund-panel-head">
            <div class="fund-panel-title">Holdings</div>
            <div class="holdings-tools">
              <input id="fundHoldingsSearch" class="holdings-search" aria-label="Search holdings" placeholder="Search holdings..." autocomplete="off" spellcheck="false"/>
              <button class="icon-btn" title="Clear holdings search" aria-label="Clear holdings search"
                data-action="clear-holdings">×</button>
            </div>
          </div>
          <div class="tbl-wrap"><table id="fundTable"><thead><tr style="background:var(--sf)">
            <th class="col-rank" style="text-align:center;width:36px">#</th>
            <th class="sort col-sticky" data-col="ticker" aria-sort="none" style="text-align:left;background:var(--sf)"><button type="button" class="sort-button" data-action="fund-sort" data-col="ticker">Security<span class="arr" aria-hidden="true"></span></button></th>
            ${sortableHeader("onFundSort", "issuer", "Company", "left")}
            ${sortableHeader("onFundSort", "pct", "% Portfolio", "right")}
            ${sortableHeader("onFundSort", "value", "Value", "right")}
            ${sortableHeader("onFundSort", "shares", "Shares", "right")}
            ${sortableHeader("onFundSort", "pctChg", "Change vs Prior", "center")}
            ${sortableHeader("onFundSort", "prevPct", "Prev %", "right")}
            <th style="text-align:center">Trend (4Q)</th>
            ${sortableHeader("onFundSort", "holdingType", "Type", "left")}
          </tr></thead><tbody id="fundTbody"></tbody></table></div>
          <div id="fundFoot" class="fund-foot"></div>
        </section>
        ${fundExitPanel}
      </main>

      <aside class="fund-sidebar">
        <section class="side-panel">
          <div class="side-title">Portfolio Summary</div>
          <div class="summary-label">Top 5 Holdings <span class="value" style="float:right">${fP(top5Pct)}</span></div>
          ${top5.map(h => `
            <div class="summary-row">
              <span class="ticker">${esc(fundTicker(h))}</span>
              <span></span>
              <span class="value">${fP(h.pct)}</span>
            </div>
          `).join("")}
          <div class="summary-line"></div>
          ${summaryEvent("Largest Increase", largestIncrease, "No increased positions")}
          <div class="summary-line"></div>
          ${summaryEvent("Largest Reduction", largestReduction, "No reduced or exited positions")}
          <div class="summary-line"></div>
          <div class="summary-label">New Position</div>
          <div class="summary-list">${newPositions.length ? newPositions.map(h => `<span class="summary-new">${esc(fundTicker(h))}</span> ${esc(displayIssuer(holdingDisplayCompany(h)))}`).join("<br>") : "No new positions"}</div>
          <div class="summary-line"></div>
          <div class="summary-label">Positions Reduced</div>
          <div class="summary-list">${reducedPositions.length ? reducedPositions.map(h => `<span class="summary-down">${esc(fundTicker(h))}</span>`).join(", ") : "No reduced positions"}</div>
          <div class="summary-line"></div>
          <div class="summary-label">Positions Exited</div>
          <div class="summary-list">${exited.length ? exited.slice(0, 8).map(h => `<span class="summary-exit">${esc(fundTicker(h))}</span>`).join(", ") : "No exited positions"}</div>
        </section>

      </aside>
      <div class="fineprint fund-data-footer">Data note: 13F filings primarily report US equities, ADRs, options, and certain reportable convertible notes; cash, T-bills, most bonds, foreign securities, and private investments are excluded. Large swings in total value can reflect position sales into cash, not AUM loss. All values are in USD and numbers may not sum to 100% due to rounding.</div>
    </div>`;

  app().innerHTML = html;
  renderFundTbody();
  updateSortArrows("fundTable", fundSort);
}

function renderFundTbody() {
  // Called on initial render AND on every header click to re-sort. Reads
  // curFundRows (already sorted by the caller) and writes #fundTbody.
  const tbody = $("fundTbody");
  if (!tbody) return;
  const needle = fundHoldingsFilter.trim().toUpperCase();
  const visibleRows = needle
    ? curFundRows.filter(h => {
        const haystack = `${fundTicker(h)} ${h.ticker || ""} ${holdingDisplayCompany(h)} ${h.issuer || ""} ${h.class || ""} ${h.cusip || ""}`.toUpperCase();
        return haystack.includes(needle);
      })
    : curFundRows;

  const rows = visibleRows.map((h, i) => {
    const rowBg = i%2 ? "var(--sf)" : "#06101a";
    return `<tr style="background:${i%2 ? "var(--sf)" : "transparent"}">
      <td class="mono col-rank" style="text-align:center;color:var(--mt);font-size:11px">${i+1}</td>
      ${holdingIdentityCells(h, rowBg)}
      <td class="mono" style="text-align:right;color:${h.pct >= 5 ? "var(--ac)" : "var(--mt)"}">${fP(h.pct)}</td>
      <td class="mono" style="text-align:right;font-weight:600">${fV(h.value)}</td>
      <td class="mono" style="text-align:right">${formatShares(h.shares, h.shares_imputed, h.quantity_unknown)}</td>
      <td class="mono ${esc(changeClass(h.ch))}" style="text-align:center;font-weight:800">${esc(changeText(h.ch))}</td>
      <td class="mono" style="text-align:right;color:var(--mt)">${h.prevPct > 0 ? fP(h.prevPct) : "—"}</td>
      <td style="text-align:center">${spark(h.sparkData)}</td>
      <td style="color:var(--tx);white-space:nowrap">${esc(holdingDisplayKindLabel(h))}</td>
    </tr>`;
  }).join("");
  tbody.innerHTML = rows || `<tr><td colspan="10" style="text-align:center;color:var(--mt);padding:34px">No holdings match this search.</td></tr>`;
  const foot = $("fundFoot");
  if (foot) {
    const total = curFundRows.length.toLocaleString();
    const shown = visibleRows.length.toLocaleString();
    foot.textContent = needle ? `${shown} of ${total} positions shown` : `${total} positions total`;
  }
}

// ---------- STOCK: detail ----------
async function loadStock(stockId, opts = {}) {
  if (dataContractBlocked) {
    showDataMaintenance();
    return;
  }
  const securityLabelsReady = ensureSecurityLabels();
  if ((!Array.isArray(idx?.tickers) || idx.tickers.length === 0) && stockIdNeedsSearchIndex(stockId)) {
    showLoadingMessage("Loading security...");
    try {
      await ensureSearchIndex();
    } catch (error) {
      if (handleDataContractError(error)) return;
      console.error("ticker resolution index load failed:", error);
    }
  }
  await securityLabelsReady;
  const canonicalId = canonicalStockLookupId(stockId);
  const parsed = parseStockLookupId(canonicalId);
  const stockEntry = resolveStockEntry(canonicalId);
  const resolvedId = stockEntry ? stockEntry.stock_id : canonicalId;
  if (!enterDetailView("stock", resolvedId, opts)) return;
  const stockPath = stockFilePath(resolvedId);
  const loadingSecurityLabel = holdingDisplayLabel({
      ...stockEntry,
      cusip: parsed.id_base,
      instrument_type: parsed.instrument_type,
    });
  const stock = await loadCachedJson({
    cache: stockCache,
    cacheKey: resolvedId,
    url: stockPath,
    loadingMessage: `Loading ${loadingSecurityLabel}...`,
    errorTitle: `Couldn't load ${loadingSecurityLabel}`,
    logLabel: "stock",
  });
  if (dataContractBlocked) {
    showDataMaintenance();
    return;
  }
  if (!stock) return;
  renderStock(stock, stockEntry);
}

function renderStock(sd, stockEntry = null) {
  $("backBtn").style.display = "block";
  stockHoldersFilter = "";
  stockPage = 1;

  const holders = Array.isArray(sd.holders) ? sd.holders : [];
  const stockMeta = stockEntry || normalizeTickerEntry(sd) || {
    stock_id: String(sd.stock_id || ""),
    ticker: String(sd.ticker || ""),
    issuer: String(sd.issuer || ""),
    cusip: String(sd.cusip || ""),
    instrument_type: normalizeInstrumentType(sd.instrument_type),
  };

  // Managers must be at least as recent as the production-wide modal quarter.
  // Stale, withheld, and invalid calendars are visible for reference but fail
  // closed: none can enter current counts, totals, or ownership trends.
  const classified = holders.map(h => {
    const aligned = alignHolderHistory(
      h.history,
      fundIndexByCik.get(cikKey(h.cik)),
      currentReportingQuarter,
      { splitAdjustments: sd.split_adjustments }
    );
    const base = {
      cik: h.cik,
      name: h.name,
      state: aligned.state,
      ch: aligned.ch,
      sparkQuarters: aligned.sparkQuarters,
      sparkData: aligned.sparkShares,
      valueSparkData: aligned.sparkValues,
      reportQuarter: aligned.calendar[0] || null,
      withheld: aligned.withheld || null,
    };

    if (["CURRENT", "STALE", "WITHHELD"].includes(aligned.state)) {
      const record = aligned.state === "CURRENT" ? aligned.current : aligned.reference;
      return {
        ...base,
        shares: Number(record?.shares) || 0,
        sharesImputed: record?.shares_imputed === true,
        quantityUnknown: record?.quantity_unknown === true,
        value: Number(record?.value) || 0,
        pctOfFund: Number(record?.pct_of_fund) || 0,
        asOfDate: record?.date || null,
      };
    }
    if (aligned.state === "EXIT") {
      const previous = aligned.previous;
      return {
        ...base,
        shares: 0,
        value: 0,
        pctOfFund: 0,
        priorShares: Number(previous?.shares) || 0,
        priorSharesImputed: previous?.shares_imputed === true,
        priorQuantityUnknown: previous?.quantity_unknown === true,
        priorValue: Number(previous?.value) || 0,
        priorPctOfFund: Number(previous?.pct_of_fund) || 0,
      };
    }
    return base;
  });

  const partitions = partitionHolderStates(classified);
  const currentHolders = partitions.current
    .sort((a, b) => (b.value || 0) - (a.value || 0));
  const recentExits = partitions.exits
    .sort((a, b) => (b.priorValue || 0) - (a.priorValue || 0));
  const staleRecords = partitions.stale
    .sort((a, b) => (b.value || 0) - (a.value || 0));
  const withheldRecords = partitions.withheld
    .sort((a, b) => (b.value || 0) - (a.value || 0));
  const historicalCount = partitions.historical.length;
  const unknownCount = partitions.unknown.length;
  const estimatedCurrentSharesCount = currentHolders
    .filter(h => h.sharesImputed).length;
  const unknownCurrentSharesCount = currentHolders
    .filter(h => h.quantityUnknown).length;

  const totV = currentHolders.reduce((s, h) => s + (h.value || 0), 0);
  const totS = exactReportedShareTotal(currentHolders);
  const {
    values: aggregateValues,
    shares: aggregateShares,
  } = aggregateEligibleHolderTrends(
    classified,
    currentReportingQuarter
  );

  const topHolder = currentHolders[0] || null;
  const topConviction = currentHolders
    .filter(h => (h.pctOfFund || 0) > 0)
    .sort((a, b) => (b.pctOfFund || 0) - (a.pctOfFund || 0))
    .slice(0, 5);
  const largestIncreases = currentHolders
    .filter(h => h.ch?.t === "UP")
    .sort((a, b) => (b.ch.p || 0) - (a.ch.p || 0))
    .slice(0, 5);
  const largestReductions = currentHolders
    .filter(h => h.ch?.t === "DOWN")
    .sort((a, b) => (b.ch.p || 0) - (a.ch.p || 0))
    .slice(0, 5);

  // The display date is informational; current classification is governed by
  // the modal reporting-quarter baseline and then retains each source date.
  const dateCounts = {};
  for (const h of currentHolders) {
    if (h.asOfDate) dateCounts[h.asOfDate] = (dateCounts[h.asOfDate] || 0) + 1;
  }
  const modeDate = Object.keys(dateCounts)
    .sort((a, b) => dateCounts[b] - dateCounts[a])[0] || null;

  const instrumentType = normalizeInstrumentType(
    stockMeta.instrument_type || sd.instrument_type
  );
  const cusipText = sd.cusip || stockMeta.cusip || "";
  const securityHolding = {
    ...sd,
    ...stockMeta,
    ticker: stockMeta.ticker || sd.ticker,
    issuer: sd.issuer || stockMeta.issuer,
    cusip: cusipText,
    instrument_type: instrumentType,
  };
  const issuerText = displayIssuer(
    holdingDisplayCompany(securityHolding)
  );
  const securityText = holdingDisplayLabel(securityHolding);
  const securityKindText = holdingDisplayKindLabel(securityHolding);
  const securityKindClass = holdingDisplayKindClass(securityHolding);
  const mappedSecurityLabel = securityLabelForCusip(cusipText);
  const trustedTicker = holdingTrustedTicker(securityHolding);
  const tickerMark = securityTickerMark(
    securityText || instrumentType
  );
  const securityMetadataLabel = mappedSecurityLabel || (
    instrumentType !== "EQUITY" || !trustedTicker ? securityText : ""
  );
  const latestDateText = modeDate ? displayDate(modeDate) : "Most recent filings";

  const summaryRows = (rows, valueFn, classFn = () => "") => {
    if (!rows.length) return `<div class="summary-list">No matching holders</div>`;
    return rows.map((h, i) => `
      <div class="stock-summary-row">
        <span class="mono stock-summary-rank">${i + 1}</span>
        <a class="stock-summary-name" href="#fund/${esc(cikKey(h.cik))}">${esc(displayHolderName(h.name || ""))}</a>
        <span class="stock-summary-value ${esc(classFn(h) || "")}">${esc(valueFn(h))}</span>
      </div>`).join("");
  };
  const summarySection = (label, rows, valueFn, sortCol, dir = "desc", classFn = () => "") => `
    <div class="stock-summary-head">
      <span>${esc(label)}</span>
      <button class="summary-action" data-action="stock-focus-sort" data-col="${esc(sortCol)}" data-dir="${esc(dir)}">View all</button>
    </div>
    ${summaryRows(rows, valueFn, classFn)}`;
  const stockExitRows = recentExits.map((h, i) => `
    <tr>
      <td class="mono col-rank" style="text-align:center;color:var(--mt);font-size:12px">${i + 1}</td>
      ${holderFundCell(h)}
      <td class="mono" style="text-align:right;font-weight:600">${fV(h.priorValue)}</td>
      <td class="mono" style="text-align:right">${formatShares(h.priorShares, h.priorSharesImputed, h.priorQuantityUnknown)}</td>
      <td class="mono" style="text-align:right;color:var(--mt)">${h.priorPctOfFund > 0 ? fP(h.priorPctOfFund) : "—"}</td>
      <td style="text-align:center">${badge(h.ch)}</td>
      <td style="text-align:center">${spark(h.sparkData)}</td>
      <td class="mono" style="text-align:right;color:var(--mt);font-size:11px">${esc(quarterCodeLabel(h.reportQuarter))}</td>
    </tr>`).join("");
  const stockExitPanel = recentExits.length ? `
        <section class="fund-panel stock-panel exit-panel">
          <div class="fund-panel-head">
            <div class="fund-panel-title">Exited in Latest Filing<span class="panel-count">${recentExits.length.toLocaleString()}</span></div>
          </div>
          <div class="tbl-wrap"><table id="stockExitTable"><thead><tr style="background:var(--sf)">
            <th class="col-rank" style="text-align:center;width:36px">#</th>
            <th class="col-sticky" style="text-align:left;background:var(--sf)">Fund</th>
            <th style="text-align:right">Prior Value</th>
            <th style="text-align:right">Prior Shares</th>
            <th style="text-align:right">Prior % of Fund</th>
            <th style="text-align:center">Change</th>
            <th style="text-align:center">Trend (4Q)</th>
            <th style="text-align:right">Exit Report</th>
          </tr></thead><tbody>${stockExitRows}</tbody></table></div>
          <div class="fund-foot">Each exit is an immediately prior-filing position absent from that manager's latest published filing. Prior values are shown for context and are not included in current totals.</div>
        </section>` : "";
  const excludedRows = (rows, stateLabel) => rows.map((h, i) => `
    <tr>
      <td class="mono col-rank" style="text-align:center;color:var(--mt);font-size:12px">${i + 1}</td>
      ${holderFundCell(h)}
      <td class="mono" style="text-align:right;font-weight:600">${h.asOfDate ? fV(h.value) : "—"}</td>
      <td class="mono" style="text-align:right">${h.asOfDate ? formatShares(h.shares, h.sharesImputed, h.quantityUnknown) : "—"}</td>
      <td class="mono" style="text-align:right;color:var(--mt);font-size:11px">${esc(h.asOfDate || "—")}</td>
      <td class="mono" style="text-align:right;color:var(--mt);font-size:11px">${esc(quarterCodeLabel(h.reportQuarter))}</td>
      <td style="text-align:center"><span class="badge mono" style="color:var(--mt)">${esc(stateLabel(h))}</span></td>
    </tr>`).join("");
  const excludedPanel = (title, rows, stateLabel, note, className) => rows.length ? `
        <section class="fund-panel stock-panel ${className}">
          <div class="fund-panel-head">
            <div class="fund-panel-title">${esc(title)}<span class="panel-count">${rows.length.toLocaleString()}</span></div>
          </div>
          <div class="tbl-wrap"><table><thead><tr style="background:var(--sf)">
            <th class="col-rank" style="text-align:center;width:36px">#</th>
            <th class="col-sticky" style="text-align:left;background:var(--sf)">Fund</th>
            <th style="text-align:right">Last Reported Value</th>
            <th style="text-align:right">Last Reported Shares</th>
            <th style="text-align:right">Last Positive Date</th>
            <th style="text-align:right">Manager Latest</th>
            <th style="text-align:center">Status</th>
          </tr></thead><tbody>${excludedRows(rows, stateLabel)}</tbody></table></div>
          <div class="fund-foot">${note}</div>
        </section>` : "";
  const stalePanel = excludedPanel(
    "Stale / Excluded Records",
    staleRecords,
    () => "STALE",
    `These managers' latest published quarter predates the current site baseline (${quarterCodeLabel(currentReportingQuarter)}). Last-reported values are historical context only and are excluded from every current count, total, and trend.`,
    "stale-panel"
  );
  const withheldPanel = excludedPanel(
    "Withheld / Unverified Records",
    withheldRecords,
    h => h.withheld?.reportDate ? `WITHHELD ${h.withheld.reportDate}` : "WITHHELD",
    "A newer or unresolved filing is quarantined. Last-published values are reference only and are excluded from every current count, total, and trend until validation succeeds.",
    "withheld-panel"
  );
  const warningParts = [];
  if (unknownCount) {
    warningParts.push(`<strong>${unknownCount.toLocaleString()} holder ${unknownCount === 1 ? "record could" : "records could"} not be classified.</strong> Their fund reporting calendar is missing or invalid.`);
  }
  if (estimatedCurrentSharesCount) {
    warningParts.push(`<strong>${estimatedCurrentSharesCount.toLocaleString()} current share ${estimatedCurrentSharesCount === 1 ? "count is" : "counts are"} estimated.</strong> Estimated rows are marked with ~, excluded from exact-share totals, and receive no quarter-over-quarter share comparison.`);
  }
  if (unknownCurrentSharesCount) {
    warningParts.push(`<strong>${unknownCurrentSharesCount.toLocaleString()} current share counts are unknown.</strong> A dash preserves the reported position value without treating a missing quantity as an exit. These rows are excluded from exact-share totals and share comparisons.`);
  }
  const classificationWarning = warningParts.length ? `
        <div class="info-note">${warningParts.join(" ")} Excluded or estimated data is never silently treated as exact current data.</div>` : "";
  const aggregateTrendNotice = currentHolders.length &&
    !aggregateValues.length
    ? `<div class="info-note">Aggregate 4Q trends are hidden because manager histories do not share one complete reporting calendar ending at ${esc(quarterCodeLabel(currentReportingQuarter))}.</div>`
    : "";

  let html = `
    <div class="stock-page">
      <main class="stock-main">
        <div class="stock-title">
          <div class="ticker-mark">${esc(tickerMark)}</div>
          <div>
            <div class="stock-title-line">
              <h1>${esc(issuerText || securityText)}</h1>
              <span class="ht-tag ${esc(securityKindClass)}">${esc(securityKindText)}</span>
            </div>
            <div class="stock-meta">
              ${securityMetadataLabel ? `<span>Security <span class="mono">${esc(securityMetadataLabel)}</span></span>` : ""}
              <span>CUSIP <span class="mono">${esc(cusipText || "—")}</span></span>
              <span>Institutional Holder View</span>
              <span>Latest 13F Positions</span>
            </div>
          </div>
        </div>

        ${classificationWarning}
        ${aggregateTrendNotice}
        <div class="stock-stat-grid">
          ${statCard("stock", "Current Institutional Holders", currentHolders.length.toLocaleString(), holdersIcon())}
          ${statCard("stock", "Total Held Value", fV(totV), miniLine(aggregateValues))}
          ${statCard("stock", "Total Exact Shares", fS(totS), miniBars(aggregateShares))}
          <div class="stock-stat">
            <div class="stock-largest">
              ${bankIcon()}
              <div style="min-width:0">
                <div class="stock-stat-label">Largest Holder</div>
                <div class="stock-largest-name">${esc(displayHolderName(topHolder?.name || "—"))}</div>
                <div class="stock-largest-meta">${topHolder ? `${formatShares(topHolder.shares, topHolder.sharesImputed, topHolder.quantityUnknown)} shares  •  ${fV(topHolder.value)}` : "—"}</div>
              </div>
            </div>
          </div>
        </div>`;

  // Only currently reported holders enter the sortable table.
  curStockRows = sortRows(currentHolders, stockSort.col, stockSort.dir, "stock");

  html += `<section class="fund-panel stock-panel" id="stockPanel">
          <div class="fund-panel-head">
            <div class="fund-panel-title">Current Holders<span class="panel-count">${currentHolders.length.toLocaleString()}</span></div>
            <div class="holdings-tools">
              <input id="stockHoldersSearch" class="holdings-search" aria-label="Search holders" placeholder="Search holders..." autocomplete="off" spellcheck="false"/>
              <button class="icon-btn" title="Clear holder search" aria-label="Clear holder search"
                data-action="clear-holders">×</button>
            </div>
          </div>
          <div class="tbl-wrap"><table id="stockTable"><thead><tr style="background:var(--sf)">
    <th class="col-rank" style="text-align:center;width:36px">#</th>
    <th class="sort col-sticky" data-col="name" aria-sort="none" style="text-align:left;background:var(--sf)"><button type="button" class="sort-button" data-action="stock-sort" data-col="name">Fund<span class="arr" aria-hidden="true"></span></button></th>
    ${sortableHeader("onStockSort", "value", "Value", "right")}
    ${sortableHeader("onStockSort", "shares", "Shares", "right")}
    ${sortableHeader("onStockSort", "pct", "% of Fund", "right")}
    ${sortableHeader("onStockSort", "qoq", "Change vs Prior", "center")}
    <th style="text-align:center">Trend (4Q)</th>
    ${sortableHeader("onStockSort", "asOfDate", "Source Date", "right")}
    </tr></thead><tbody id="stockTbody"></tbody></table></div>
          <div id="stockFoot" class="stock-table-footer"></div>
        </section>
        ${stockExitPanel}
        ${stalePanel}
        ${withheldPanel}
        <div class="fineprint" style="padding:18px 0 0">${currentHolders.length.toLocaleString()} current holders&nbsp;&nbsp; • &nbsp;&nbsp;${recentExits.length.toLocaleString()} latest-filing exits&nbsp;&nbsp; • &nbsp;&nbsp;${staleRecords.length.toLocaleString()} stale records&nbsp;&nbsp; • &nbsp;&nbsp;${withheldRecords.length.toLocaleString()} withheld records&nbsp;&nbsp; • &nbsp;&nbsp;Values in USD</div>
      </main>

      <aside class="stock-sidebar">
        <section class="side-panel">
          <div class="side-title">Ownership Summary</div>
          ${summarySection("Top Holders by Value", currentHolders.slice(0, 5), h => fV(h.value), "value")}
          <div class="summary-line"></div>
          ${summarySection("Highest Conviction (% of Fund)", topConviction, h => fP(h.pctOfFund), "pct")}
          <div class="summary-line"></div>
          ${summarySection("Largest Increases vs Prior Filing", largestIncreases, h => changeText(h.ch), "qoq", "desc", h => changeClass(h.ch))}
          <div class="summary-line"></div>
          ${summarySection("Largest Reductions vs Prior Filing", largestReductions, h => changeText(h.ch), "qoq", "asc", h => changeClass(h.ch))}
        </section>

      </aside>
      <div class="fineprint stock-data-footer">Data note: 13F aggregates only include institutional managers that file Form 13F-HR; retail and smaller institutions are not captured. Current holders must have a valid, non-withheld filing calendar at least as recent as the modal site baseline (${esc(quarterCodeLabel(currentReportingQuarter))}). Source dates can still differ during filing season. Latest common current-holder date: ${esc(latestDateText)}.${historicalCount ? ` ${historicalCount.toLocaleString()} older former-holder ${historicalCount === 1 ? "record is" : "records are"} omitted.` : ""}</div>
    </div>`;

  app().innerHTML = html;
  renderStockTbody();
  updateSortArrows("stockTable", stockSort);
  // pct_of_fund and fund calendars are already in generated JSON; no per-fund
  // fetches are needed to classify or render this stock page.
}

function renderStockTbody() {
  // curStockRows contains current holders only; exits render in their own
  // explicitly historical table.
  const tbody = $("stockTbody");
  if (!tbody) return;
  const needle = stockHoldersFilter.trim().toUpperCase();
  const visibleRows = needle
    ? curStockRows.filter(h => String(h.name || "").toUpperCase().includes(needle))
    : curStockRows;
  const totalPages = Math.max(1, Math.ceil(visibleRows.length / stockRowsPerPage));
  stockPage = Math.min(Math.max(1, stockPage), totalPages);
  const start = (stockPage - 1) * stockRowsPerPage;
  const pageRows = visibleRows.slice(start, start + stockRowsPerPage);

  const rows = pageRows.map((h, i) => {
    const rowNumber = start + i + 1;
    const pctText = h.pctOfFund > 0 ? fP(h.pctOfFund) : "—";
    const pctColor = h.pctOfFund >= 5 ? "var(--ac)" : "var(--mt)";
    const rowBg = i%2 ? "var(--sf)" : "var(--bg)";
    return `<tr style="background:${i%2 ? "var(--sf)" : "transparent"}">
      <td class="mono col-rank" style="text-align:center;color:var(--mt);font-size:12px">${rowNumber}</td>
      ${holderFundCell(h, rowBg)}
      <td class="mono" style="text-align:right;font-weight:600">${fV(h.value)}</td>
      <td class="mono" style="text-align:right">${formatShares(h.shares, h.sharesImputed, h.quantityUnknown)}</td>
      <td class="mono" style="text-align:right;color:${pctColor}">${pctText}</td>
      <td style="text-align:center">${badge(h.ch)}</td>
      <td style="text-align:center">${spark(h.sparkData)}</td>
      <td class="mono" style="text-align:right;color:var(--mt);font-size:11px">${esc(h.asOfDate || "—")}</td>
    </tr>`;
  }).join("");
  const emptyText = needle ? "No current holders match this search." : "No current holders reported.";
  tbody.innerHTML = rows || `<tr><td colspan="8" style="text-align:center;color:var(--mt);padding:34px">${emptyText}</td></tr>`;

  const foot = $("stockFoot");
  if (!foot) return;
  const startLabel = visibleRows.length ? (start + 1).toLocaleString() : "0";
  const endLabel = (start + pageRows.length).toLocaleString();
  const totalLabel = visibleRows.length.toLocaleString();
  const pageButtons = stockPaginationPages(totalPages, stockPage).map((p, i) => {
    if (p === "ellipsis") return `<button class="page-btn" disabled aria-hidden="true" key="${i}">...</button>`;
    return `<button class="page-btn ${p === stockPage ? "active" : ""}" data-action="stock-page" data-page="${p}" aria-label="Page ${p}" ${p === stockPage ? 'aria-current="page"' : ""}>${p}</button>`;
  }).join("");
  const rowsOptions = [15, 25, 50, 100].map(n =>
    `<option value="${n}" ${n === stockRowsPerPage ? "selected" : ""}>${n}</option>`
  ).join("");
  foot.innerHTML = `
    <div>${needle ? `${totalLabel} matching current holders` : `Showing ${startLabel} to ${endLabel} of ${totalLabel} current holders`}</div>
    <div class="stock-pagination">
      ${pageButtons}
      <button class="page-btn" ${stockPage >= totalPages ? "disabled" : ""} data-action="stock-page" data-page="${stockPage + 1}" aria-label="Next page">›</button>
    </div>
    <label class="rows-control">Rows per page:
      <select id="stockRowsPerPage">${rowsOptions}</select>
    </label>`;
}
