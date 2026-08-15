/**
 * 13F Super Investor Seeker — Insider Activity
 * Illustrative TypeScript contracts. Adapt to the repository's naming,
 * generated API types, decimal library, and date conventions.
 */

export type FormType = "3" | "3/A" | "4" | "4/A" | "5" | "5/A";
export type BaseFormType = "3" | "4" | "5";
export type SourceTable = "non_derivative" | "derivative";
export type AcquiredDisposedCode = "A" | "D";
export type DirectIndirectOwnership = "D" | "I";
export type TransactionTimeliness = "E" | "L" | null;

export type TransactionCode =
  | "A"
  | "C"
  | "D"
  | "E"
  | "F"
  | "G"
  | "H"
  | "I"
  | "J"
  | "L"
  | "M"
  | "O"
  | "P"
  | "S"
  | "U"
  | "W"
  | "X"
  | "Z"
  | (string & {});

export type NormalizedTransactionCategory =
  | "purchase"
  | "sale"
  | "compensation_acquisition"
  | "derivative_conversion"
  | "issuer_disposition"
  | "derivative_expiration"
  | "tax_or_exercise_withholding"
  | "gift"
  | "discretionary_plan"
  | "other"
  | "small_acquisition"
  | "derivative_exercise"
  | "change_of_control"
  | "inheritance"
  | "voting_trust"
  | "unknown";

export type PlanStatus =
  | "filing_marked"
  | "footnote_confirmed"
  | "not_marked"
  | "unknown";

export type ValueMethod =
  | "reported_total"
  | "calculated_shares_times_price"
  | "unavailable"
  | "not_applicable";

/** Exact decimals should remain strings at the API boundary. */
export type DecimalString = string;
export type ISODate = string;
export type ISODateTime = string;

export interface SecuritySummary {
  id: string;
  issuerCik: string;
  ticker: string;
  companyName: string;
  securityType: string;
  securityTypeLabel: string;
  cusip: string | null;
  marketCap: DecimalString | null;
  sector: string | null;
  latest13FPeriod: string | null;
}

export interface OwnerGroupSummary {
  key: string;
  displayName: string;
  ownerCount: number;
  roles: Array<"Officer" | "Director" | "TenPercentOwner" | "Other">;
  primaryTitle: string | null;
  isJoint: boolean;
}

export interface SummaryMetric {
  value: DecimalString;
  displayValue?: string;
  transactionCount: number;
  ownerGroupCount: number;
  missingValueCount: number;
}

export interface LatestMeaningfulTransaction {
  displayGroupKey: string;
  transactionDate: ISODate;
  ownerGroup: OwnerGroupSummary;
  code: TransactionCode;
  category: "purchase" | "sale";
  shares: DecimalString | null;
  pricePerShare: DecimalString | null;
  value: DecimalString | null;
  displayValue?: string | null;
  relativeAgeLabel?: string;
  filingAccessionNumber: string;
}

export interface InsiderActivitySummary {
  window: "12m";
  purchases: SummaryMetric;
  sales: SummaryMetric & {
    planMarkedKnownValuePercentage: DecimalString | null;
    unknownPlanStatusCount: number;
  };
  netPS: {
    value: DecimalString;
    displayValue?: string;
    directionLabel:
      | "Net reported buying"
      | "Net reported selling"
      | "Balanced reported activity"
      | "No valued P/S activity";
    salesToPurchasesRatio: DecimalString | null;
    ratioState?: "normal" | "sales_only" | "no_valued_activity";
    ratioDisplay?: string;
  };
  latestMeaningfulTransaction: LatestMeaningfulTransaction | null;
}

export interface PricePoint {
  date: ISODate;
  close: DecimalString;
}

export type ChartMarker =
  | "triangle-up-filled"
  | "triangle-down-filled"
  | "triangle-down-outline"
  | "circle-neutral";

export interface InsiderChartEvent {
  displayGroupKey: string;
  transactionDate: ISODate;
  plotDate: ISODate;
  ownerGroupDisplayName: string;
  roleLabel: string | null;
  code: TransactionCode;
  category: NormalizedTransactionCategory;
  marker: ChartMarker;
  shares: DecimalString | null;
  pricePerShare: DecimalString | null;
  plotPrice: DecimalString | null;
  plotPriceMethod:
    | "reported_transaction_price"
    | "daily_close_fallback"
    | "split_adjusted_transaction_price"
    | "unavailable";
  value: DecimalString | null;
  postTransactionShares: DecimalString | null;
  formType: FormType;
  filingDate: ISODate;
  planStatus: PlanStatus;
  accessionNumber: string;
}

export interface InsiderTransactionRow {
  id: string;
  displayGroupKey: string;
  transactionDate: ISODate;
  deemedExecutionDate: ISODate | null;
  ownerGroup: OwnerGroupSummary;
  sourceTable: SourceTable;
  securityTitleAsFiled: string;
  transactionCode: TransactionCode;
  transactionLabel: string;
  normalizedCategory: NormalizedTransactionCategory;
  acquiredDisposedCode: AcquiredDisposedCode;
  shares: DecimalString | null;
  pricePerShare: DecimalString | null;
  priceDisplay?: string | null;
  priceIsWeightedAverage: boolean;
  value: DecimalString | null;
  valueMethod: ValueMethod;
  valueDisplay?: string | null;
  postTransactionShares: DecimalString | null;
  percentChange: DecimalString | null;
  percentChangeDisplay?: string | null;
  directIndirectOwnership: DirectIndirectOwnership;
  natureOfOwnership: string | null;
  planStatus: PlanStatus;
  transactionTimeliness: TransactionTimeliness;
  isAmended: boolean;
  isSuperseded: boolean;
  formType: FormType;
  filingDate: ISODate;
  acceptedAt: ISODateTime | null;
  accessionNumber: string;
  secDocumentUrl: string;
}

export interface RankedOwnerValue {
  rank: number;
  ownerGroupKey: string;
  displayName: string;
  roleLabel: string | null;
  value: DecimalString;
  displayValue?: string;
  planMarkedKnownValuePercentage?: DecimalString | null;
  missingValueTransactionCount?: number;
}

export interface LatestReportedHolding {
  displayName: string;
  roleLabel: string | null;
  shares: DecimalString;
  ownershipPercentage: DecimalString | null;
  containsIndirectOwnership: boolean;
  asOfDate?: ISODate;
}

export interface InsiderActivitySidebar {
  topBuyers: RankedOwnerValue[];
  topSellers: RankedOwnerValue[];
  latestReportedHoldings: {
    officersAndDirectors: LatestReportedHolding[];
    tenPercentOwnersAndEntities: LatestReportedHolding[];
    sharesOutstanding: DecimalString | null;
    sharesOutstandingDate: ISODate | null;
  };
  rule10b51: {
    window: "12m";
    planMarkedSalesValue: DecimalString;
    planMarkedSalesDisplayValue?: string;
    distinctOwnerGroupCount: number;
    latestPlanAdoptionDate: ISODate | null;
    latestPlanAdoptionDateSource:
      | "structured"
      | "footnote_high_confidence"
      | null;
  };
}

export interface DataQualitySummary {
  partial: boolean;
  missingValueTransactionCount: number;
  unknownPlanStatusSaleCount: number;
  unresolvedAmendmentCount: number;
  unmappedSecurityRowCount: number;
  priceCoverageStart: ISODate | null;
  priceCoverageEnd: ISODate | null;
  latestSecAcceptedAt?: ISODateTime | null;
  latestSuccessfulSyncAt?: ISODateTime | null;
}

export interface InsiderActivityFilters {
  range: "6m" | "1y" | "2y" | "5y" | "all" | "custom";
  transactionScope: "ps" | "all";
  ownerScope: "all" | "officers-directors" | "ten-percent";
  includeTenPercentOwners: boolean;
  plan: "all" | "10b5-1" | "not-10b5-1" | "unknown";
  securityScope: "primary-common" | "all";
  start: ISODate | null;
  end: ISODate | null;
  search: string;
  sort: "tradeDate" | "value" | "shares" | "holdingsAfter" | "percentChange";
  order: "asc" | "desc";
}

export interface InsiderActivityPageResponse {
  security: SecuritySummary;
  asOf: ISODateTime;
  dataFreshness: {
    latestSecAcceptedAt: ISODateTime | null;
    latestSuccessfulSecSyncAt: ISODateTime | null;
    latestPriceDate: ISODate | null;
    status: "current" | "stale" | "partial" | "error";
  };
  filters: InsiderActivityFilters;
  methodologyBanner: {
    tone: "informational" | "positive" | "warning";
    text: string;
    actionLabel: string;
  };
  summary: InsiderActivitySummary;
  priceSeries: PricePoint[];
  chartEvents: InsiderChartEvent[];
  transactions: {
    items: InsiderTransactionRow[];
    nextCursor: string | null;
    totalApproximate: number | null;
  };
  sidebar: InsiderActivitySidebar;
  dataQuality: DataQualitySummary;
}

export interface FilingOwnerDetail {
  ownerId: string;
  reportingOwnerCik: string | null;
  displayName: string;
  ownerOrder: number;
  isDirector: boolean;
  isOfficer: boolean;
  isTenPercentOwner: boolean;
  isOther: boolean;
  officerTitle: string | null;
  otherText: string | null;
}

export interface FieldFootnoteReference {
  fieldName: string;
  footnoteIds: string[];
}

export interface FilingTransactionDetail extends InsiderTransactionRow {
  sourceRowIndex: number;
  equitySwapInvolved: boolean | null;
  reportedTotalValue: DecimalString | null;
  conversionOrExercisePrice: DecimalString | null;
  exerciseDate: ISODate | null;
  expirationDate: ISODate | null;
  underlyingSecurityTitle: string | null;
  underlyingShares: DecimalString | null;
  underlyingValue: DecimalString | null;
  fieldFootnotes: FieldFootnoteReference[];
}

export interface InsiderFilingDetailResponse {
  accessionNumber: string;
  formType: FormType;
  baseFormType: BaseFormType;
  isAmendment: boolean;
  issuerCik: string;
  issuerNameAsFiled: string;
  issuerTradingSymbolAsFiled: string | null;
  schemaVersion: string | null;
  periodOfReport: ISODate | null;
  filingDate: ISODate;
  acceptedAt: ISODateTime | null;
  originalSubmissionDate: ISODate | null;
  aff10b5One: boolean | null;
  notSubjectToSection16: boolean | null;
  noSecuritiesOwned: boolean | null;
  remarks: string | null;
  owners: FilingOwnerDetail[];
  nonDerivativeTransactions: FilingTransactionDetail[];
  derivativeTransactions: FilingTransactionDetail[];
  nonDerivativeHoldings: unknown[];
  derivativeHoldings: unknown[];
  footnotes: Array<{ id: string; text: string }>;
  signatures: Array<{ name: string; date: ISODate }>;
  amendmentHistory: Array<{
    accessionNumber: string;
    formType: FormType;
    filingDate: ISODate;
    isCurrentEffectiveVersion: boolean;
    matchConfidence: "high" | "medium" | "low" | "unresolved" | null;
  }>;
  source: {
    indexUrl: string;
    documentUrl: string;
  };
  lineage: {
    parserVersion: string;
    ingestedAt: ISODateTime;
    reprocessedAt: ISODateTime | null;
  };
}
