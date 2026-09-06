import json
import os
import subprocess
import unittest
from pathlib import Path
from typing import cast


ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = ROOT / "index.html"


class FrontendSemanticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = INDEX_HTML.read_text()
        start = cls.html.index("function normalizeInstrumentType(")
        end = cls.html.index("// ---------- sparkline", start)
        cls.logic = cls.html[start:end]

    def run_javascript(self, body: str) -> object:
        completed = subprocess.run(
            ["node", "-e", f"{self.logic}\n{body}"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def test_display_date_preserves_legacy_month_precision(self) -> None:
        date_start = self.html.index("function displayDate(")
        date_end = self.html.index("function fundTicker(", date_start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                (
                    self.html[date_start:date_end]
                    + """
                    console.log(JSON.stringify({
                      exact: displayDate("2026-05-14"),
                      monthOnly: displayDate("2026-05"),
                      malformed: displayDate("not-a-date"),
                      impossible: displayDate("2026-02-31"),
                      invalidYear: displayDate("0000-02-29"),
                    }));
                    """
                ),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "LANG": "en_US.UTF-8",
                "LC_ALL": "en_US.UTF-8",
            },
        )
        result = cast(
            dict[str, str],
            json.loads(completed.stdout),
        )

        self.assertEqual("May 14, 2026", result["exact"])
        self.assertEqual("May 2026", result["monthOnly"])
        self.assertEqual("not-a-date", result["malformed"])
        self.assertEqual("2026-02-31", result["impossible"])
        self.assertEqual("0000-02-29", result["invalidYear"])

    def test_homepage_pins_adar1_before_other_popular_filers(self) -> None:
        constants_start = self.html.index(
            "const PINNED_POPULAR_FUND_CIKS ="
        )
        constants_end = self.html.index(
            "// Top 20 by market cap", constants_start
        )
        lookup_start = self.html.index("let _popularFundsCache = null;")
        lookup_end = self.html.index(
            "// ---------- global unified search ----------", lookup_start
        )
        completed = subprocess.run(
            [
                "node",
                "-e",
                (
                    self.html[constants_start:constants_end]
                    + "\nlet idx = {funds: ["
                    + '{cik: 1067983, name: "BERKSHIRE HATHAWAY INC"},'
                    + '{cik: 1940272, name: "Renamed SEC Filer"},'
                    + '{cik: 1336528, name: "Pershing Square Capital"}'
                    + "]};\n"
                    + self.html[lookup_start:lookup_end]
                    + "\nconst present = getPopularFunds().map(f => f.cik);"
                    + "\n_popularFundsCache = null;"
                    + "\nidx = {funds: idx.funds.filter("
                    + "f => f.cik !== 1940272)};"
                    + "\nconst absent = getPopularFunds().map(f => f.cik);"
                    + "\nconsole.log(JSON.stringify({present, absent}));"
                ),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        result = json.loads(completed.stdout)

        self.assertEqual([1940272, 1067983, 1336528], result["present"])
        self.assertEqual([1067983, 1336528], result["absent"])

    def test_fund_search_identity_always_includes_the_authoritative_cik(
        self,
    ) -> None:
        formatter_start = self.html.index("const _tcKeepUpper")
        formatter_end = self.html.index("function changeText(", formatter_start)
        cik_start = self.html.index("function cikKey(")
        cik_end = self.html.index("function reportQuarterCode(", cik_start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                (
                    self.html[formatter_start:formatter_end]
                    + self.html[cik_start:cik_end]
                    + """
                    console.log(JSON.stringify({
                      first: fundSearchIdentity({
                        cik: 1765681,
                        name: "Thrive Capital Management, LLC",
                      }),
                      second: fundSearchIdentity({
                        cik: 1845943,
                        name: "Thrive Capital Management, LLC",
                      }),
                    }));
                    """
                ),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        result = json.loads(completed.stdout)

        self.assertEqual(
            "Thrive Capital Management · CIK 1765681",
            result["first"],
        )
        self.assertEqual(
            "Thrive Capital Management · CIK 1845943",
            result["second"],
        )
        self.assertIn("${esc(fundSearchIdentity(f))}", self.html)

    def test_fund_product_name_formatting_preserves_brand_and_index_name(
        self,
    ) -> None:
        formatter_start = self.html.index("const _tcKeepUpper")
        formatter_end = self.html.index("const fV", formatter_start)
        entity_start = self.html.index("const _legalEntitySuffixes")
        entity_end = self.html.index("function changeText(", entity_start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                (
                    self.html[formatter_start:formatter_end]
                    + self.html[entity_start:entity_end]
                    + "\nconsole.log(JSON.stringify({"
                    "ewy: displayIssuer("
                    "'ISHARES MSCI SOUTH KOREA ETF'"
                    "),"
                    "spdr: displayIssuer("
                    "'SPDR S&P 500 ESG REIT ETF'"
                    "),"
                    "fixedIncome: displayIssuer("
                    "'ISHARES USD CLO MBS TIPS ETF'"
                    "),"
                    "termFund: displayIssuer("
                    "'ISHARES IBONDS DEC 2026 TERM CORPORATE ETF'"
                    "),"
                    "bufferFund: displayIssuer("
                    "'PGIM S&P 500 BUFFER 12 ETF - JANUARY'"
                    ")}));"
                ),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {
                "ewy": "iShares MSCI South Korea ETF",
                "spdr": "SPDR S&P 500 ESG REIT ETF",
                "fixedIncome": "iShares USD CLO MBS TIPS ETF",
                "termFund": (
                    "iShares iBonds Dec 2026 Term Corporate ETF"
                ),
                "bufferFund": "PGIM S&P 500 Buffer 12 ETF - January",
            },
            json.loads(completed.stdout),
        )

    def test_note_labels_remain_visible_without_changing_stock_identity(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            console.log(JSON.stringify({
              note: securityDisplayTicker(
                "RIVN 3.625 10/15/30",
                "NOTE"
              ),
              equity: securityDisplayTicker("AAPL USD", "EQUITY"),
              holding: holdingDisplayLabel({
                ticker: "BILL 0 04/01/30",
                cusip: "090043AF7",
                holding_type: "NOTE",
              }),
              fallback: holdingDisplayLabel({
                ticker: null,
                cusip: "26210CAC8",
                holding_type: "NOTE",
              }),
              mark: securityTickerMark("RIVN 3.625 10/15/30"),
              lookup: stockLookupId("76954AAD5", "NOTE"),
            }));
            """
        )

        self.assertEqual("RIVN 3.625 10/15/30", result["note"])
        self.assertEqual("AAPL", result["equity"])
        self.assertEqual("BILL 0 04/01/30", result["holding"])
        self.assertEqual("Note security", result["fallback"])
        self.assertEqual("RIVN", result["mark"])
        self.assertEqual("76954AAD5|NOTE", result["lookup"])
        self.assertNotIn('h.ticker.split(" ")[0]', self.html)
        self.assertIn(
            'Security <span class="mono">${esc(securityMetadataLabel)}</span>',
            self.html,
        )
        self.assertIn(
            "#fundTable td.security-label-cell,#fundExitTable td.security-label-cell",
            self.html,
        )

    def test_sec_descriptions_preserve_mapped_stock_tickers(self) -> None:
        result = self.run_javascript("""
            securityLabels = {
              "037833100": "APPLE INC — COM",
              "00370M103": "ABIVAX SA — SPONSORED ADS",
              "78462F103": "SPDR S&P 500 ETF TR — TR UNIT",
              "111111111": "UNMAPPED ISSUER — COM",
              "76954AAB9": "RIVIAN AUTOMOTIVE INC — NOTE 4.625% 3/1",
              "65339F655": "NEE.PRS 7.299% CORPORATE UNITS 02/15/29",
              "222222222": "AAPL",
            };
            securityKinds = {
              "037833100": "COMMON", "00370M103": "COMMON",
              "78462F103": "ETF", "111111111": "COMMON",
              "76954AAB9": "BOND", "65339F655": "PREFERRED",
              "222222222": "COMMON",
            };
            const rows = [
              {cusip: "037833100", ticker: "AAPL", holding_type: "EQUITY"},
              {cusip: "00370M103", ticker: "ABVX", holding_type: "EQUITY"},
              {cusip: "037833100", ticker: "AAPL", holding_type: "CALL"},
              {cusip: "78462F103", ticker: "SPY", holding_type: "EQUITY"},
              {cusip: "111111111", ticker: "111111111", holding_type: "EQUITY"},
              {cusip: "76954AAB9", ticker: "RIVN", holding_type: "NOTE"},
              {cusip: "65339F655", ticker: "NEE", holding_type: "EQUITY"},
              {cusip: "222222222", ticker: "OLDALIAS", holding_type: "EQUITY"},
            ];
            console.log(JSON.stringify(rows.map(holdingDisplayLabel)));
        """)
        self.assertEqual([
            "AAPL", "ABVX", "AAPL", "SPY", "UNMAPPED ISSUER — COM",
            "RIVIAN AUTOMOTIVE INC — NOTE 4.625% 3/1",
            "NEE.PRS 7.299% CORPORATE UNITS 02/15/29", "AAPL",
        ], result)

    def test_security_label_map_is_display_only_and_raw_cusips_are_not_labels(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            securityLabels = normalizeSecurityLabelPayload({
              data_contract_version: 4,
              labels: {
                "65339F655": "NEE.PRS 7.299% CORPORATE UNITS 02/15/29",
                "26210CAC8": "26210CAC8",
                "037833100": "AAPL",
                "464286772": "EWY",
                "057071870": "BCOIX",
              },
            });
            securityKinds = normalizeSecurityKindPayload({
              kinds: {
                "65339F655": "PREFERRED",
                "921937827": "ETF",
                "464288687": "ETF",
                "464286772": "ETF",
                "464287655": "ETF",
                "714920113": "RIGHT",
                "G93Y09123": "UNIT",
                "04626A103": "COMMON",
                "808515209": "MUTUAL FUND",
                "343498AC5": "BOND",
                "037833100": "NOT A KIND",
              },
            });
            securityFundIdentities = normalizeSecurityFundIdentityPayload({
              fund_identities: [
                "057071870",
                "464286772",
                "464287655",
                "464288687",
                "808515209",
                "921937827",
              ],
            });
            securityProductNames = normalizeSecurityProductNamePayload({
              product_names: {
                "464286772": "ISHARES MSCI SOUTH KOREA ETF",
                "464287655": "ISHARES RUSSELL 2000 ETF",
                "921937827": "VANGUARD SHORT-TERM BOND ETF",
                "464288687": "ISHARES PREFERRED AND INCOME SECURITIES ETF",
                "808515209": "SCHWAB GOVERNMENT MONEY FUND - SWEEP SHARES",
                "65339F655": "SHOULD NOT OVERRIDE A PREFERRED",
                "26210CAC8": "26210CAC8",
              },
            });
            const unit = {
              ticker: null,
              cusip: "65339F655",
              issuer: "NEXTERA ENERGY INC",
              class: "UNIT 02/15/2029",
              holding_type: "EQUITY",
            };
            const bsv = {
              ticker: null,
              cusip: "921937827",
              issuer: "VANGUARD BD INDEX FDS",
              holding_type: "NOTE",
            };
            const pff = {
              ticker: "PFF",
              cusip: "464288687",
              issuer: "ISHARES TR",
              holding_type: "PREF",
            };
            const iwmPut = {
              ticker: null,
              cusip: "464287655",
              issuer: "ISHARES TR",
              put_call: "PUT",
              holding_type: "EQUITY",
            };
            const ewy = {
              ticker: "EWY",
              cusip: "464286772",
              issuer: "ISHARES INC",
              holding_type: "EQUITY",
            };
            const legacyBondCall = {
              ticker: "FLO",
              cusip: "343498AC5",
              issuer: "FLOWSERVE CORP",
              holding_type: "CALL",
              put_call: "CALL",
            };
            const bcoix = {
              ticker: "BCOIX",
              cusip: "057071870",
              issuer: "BAIRD CORE PLUS BOND INS T",
              holding_type: "NOTE",
            };
            console.log(JSON.stringify({
              unitLabel: holdingDisplayLabel(unit),
              unitKind: holdingDisplayKind(unit),
              unitKindLabel: holdingDisplayKindLabel(unit),
              unitKindClass: holdingDisplayKindClass(unit),
              unitLookup: stockLookupId(
                unit.cusip,
                holdingInstrumentType(unit)
              ),
              unitCompany: holdingDisplayCompany(unit),
              bsvKind: holdingDisplayKind(bsv),
              bsvKindLabel: holdingDisplayKindLabel(bsv),
              bsvKindClass: holdingDisplayKindClass(bsv),
              bsvPublishedType: holdingPublishedInstrumentType(bsv),
              bsvLookup: stockLookupId(
                bsv.cusip,
                holdingPublishedInstrumentType(bsv)
              ),
              bsvDirectNote: canonicalStockLookupId("921937827|NOTE"),
              bsvDirectPref: canonicalStockLookupId("921937827|PREF"),
              bsvDirectCall: canonicalStockLookupId("921937827|CALL"),
              pffKind: holdingDisplayKind(pff),
              pffPublishedType: holdingPublishedInstrumentType(pff),
              pffLookup: stockLookupId(
                pff.cusip,
                holdingPublishedInstrumentType(pff)
              ),
              pffDirectNote: canonicalStockLookupId("464288687|NOTE"),
              pffDirectPut: canonicalStockLookupId("464288687|PUT"),
              legacyBondRawType: holdingInstrumentType(legacyBondCall),
              legacyBondPublishedType:
                holdingPublishedInstrumentType(legacyBondCall),
              legacyBondKind: holdingDisplayKind(legacyBondCall),
              legacyBondKindLabel: holdingDisplayKindLabel(legacyBondCall),
              legacyBondKindClass: holdingDisplayKindClass(legacyBondCall),
              legacyBondLookup: stockLookupId(
                legacyBondCall.cusip,
                holdingPublishedInstrumentType(legacyBondCall)
              ),
              legacyBondDirectCall:
                canonicalStockLookupId("343498AC5|CALL"),
              legacyBondDirectBare:
                canonicalStockLookupId("343498AC5"),
              ordinaryDirectPut:
                canonicalStockLookupId("464287655|PUT"),
              bcoixKind: holdingDisplayKind(bcoix),
              bcoixPublishedType: holdingPublishedInstrumentType(bcoix),
              bcoixDirectNote: canonicalStockLookupId("057071870|NOTE"),
              bcoixDirectCall: canonicalStockLookupId("057071870|CALL"),
              iwmKind: holdingDisplayKind(iwmPut),
              iwmKindLabel: holdingDisplayKindLabel(iwmPut),
              iwmKindClass: holdingDisplayKindClass(iwmPut),
              iwmLookup: stockLookupId(
                iwmPut.cusip,
                holdingInstrumentType(iwmPut)
              ),
              ewyLabel: holdingDisplayLabel(ewy),
              ewyCompany: holdingDisplayCompany(ewy),
              iwmCompany: holdingDisplayCompany(iwmPut),
              bsvCompany: holdingDisplayCompany(bsv),
              swgxxCompany: holdingDisplayCompany({
                ticker: "SWGXX",
                cusip: "808515209",
                issuer: "APPLE INC",
                holding_type: "EQUITY",
              }),
              invalidKind: securityKindForCusip("037833100"),
              rightKind: holdingDisplayKind({
                cusip: "714920113",
                holding_type: "EQUITY",
              }),
              unitKindFallback: holdingDisplayKindLabel({
                cusip: "G93Y09123",
                holding_type: "EQUITY",
              }),
              commonStockKind: holdingDisplayKindLabel({
                cusip: "04626A103",
                holding_type: "EQUITY",
              }),
              commonStockPutKind: holdingDisplayKindLabel({
                cusip: "04626A103",
                holding_type: "PUT",
                put_call: "PUT",
              }),
              commonStockNoteKind: holdingDisplayKindLabel({
                cusip: "04626A103",
                holding_type: "NOTE",
              }),
              commonStockPreferredKind: holdingDisplayKindLabel({
                cusip: "04626A103",
                holding_type: "PREF",
              }),
              commonStockWarrantKind: holdingDisplayKindLabel({
                cusip: "04626A103",
                holding_type: "WARRANT",
              }),
              unknownEquityKind: holdingDisplayKindLabel({
                cusip: "123456789",
                holding_type: "EQUITY",
              }),
              searchEtfTag: searchEntryTagLabel({
                cusip: "921937827",
                instrument_type: "EQUITY",
              }),
              searchEtfClass: searchEntryTagClass({
                cusip: "921937827",
                instrument_type: "EQUITY",
              }),
              equity: holdingDisplayLabel({
                ticker: "AAPL USD",
                cusip: "037833100",
                issuer: "APPLE INC",
                holding_type: "EQUITY",
              }),
              classFallback: holdingDisplayLabel({
                ticker: null,
                cusip: "G4939KAF3",
                issuer: "IQIYI INC",
                class: "NOTE 6.500% 3/1",
                holding_type: "NOTE",
              }),
              rejectedRawMap: holdingDisplayLabel({
                ticker: "26210CAC8",
                cusip: "26210CAC8",
                holding_type: "NOTE",
              }),
              genericFallback: holdingDisplayLabel({
                ticker: null,
                cusip: "123456789",
                holding_type: "WARRANT",
              }),
            }));
            """
        )

        self.assertEqual(
            "NEE.PRS 7.299% CORPORATE UNITS 02/15/29",
            result["unitLabel"],
        )
        self.assertEqual("PREFERRED", result["unitKind"])
        self.assertEqual("Preferred", result["unitKindLabel"])
        self.assertEqual("pref", result["unitKindClass"])
        self.assertEqual("65339F655", result["unitLookup"])
        self.assertEqual("NEXTERA ENERGY INC", result["unitCompany"])
        self.assertEqual("ETF", result["bsvKind"])
        self.assertEqual("ETF", result["bsvKindLabel"])
        self.assertEqual("stock", result["bsvKindClass"])
        self.assertEqual("EQUITY", result["bsvPublishedType"])
        self.assertEqual("921937827", result["bsvLookup"])
        self.assertEqual("921937827", result["bsvDirectNote"])
        self.assertEqual("921937827", result["bsvDirectPref"])
        self.assertEqual("921937827|CALL", result["bsvDirectCall"])
        self.assertEqual("ETF", result["pffKind"])
        self.assertEqual("EQUITY", result["pffPublishedType"])
        self.assertEqual("464288687", result["pffLookup"])
        self.assertEqual("464288687", result["pffDirectNote"])
        self.assertEqual("464288687|PUT", result["pffDirectPut"])
        self.assertEqual("CALL", result["legacyBondRawType"])
        self.assertEqual("NOTE", result["legacyBondPublishedType"])
        self.assertEqual("BOND", result["legacyBondKind"])
        self.assertEqual("Bond", result["legacyBondKindLabel"])
        self.assertEqual("note", result["legacyBondKindClass"])
        self.assertEqual("343498AC5|NOTE", result["legacyBondLookup"])
        self.assertEqual("343498AC5|NOTE", result["legacyBondDirectCall"])
        self.assertEqual("343498AC5|NOTE", result["legacyBondDirectBare"])
        self.assertEqual("464287655|PUT", result["ordinaryDirectPut"])
        self.assertEqual("EQUITY", result["bcoixKind"])
        self.assertEqual("EQUITY", result["bcoixPublishedType"])
        self.assertEqual("057071870", result["bcoixDirectNote"])
        self.assertEqual("057071870|CALL", result["bcoixDirectCall"])
        self.assertEqual("PUT", result["iwmKind"])
        self.assertEqual("Put", result["iwmKindLabel"])
        self.assertEqual("put", result["iwmKindClass"])
        self.assertEqual("464287655|PUT", result["iwmLookup"])
        self.assertEqual("EWY", result["ewyLabel"])
        self.assertEqual(
            "ISHARES MSCI SOUTH KOREA ETF",
            result["ewyCompany"],
        )
        self.assertEqual(
            "ISHARES RUSSELL 2000 ETF",
            result["iwmCompany"],
        )
        self.assertEqual(
            "VANGUARD SHORT-TERM BOND ETF",
            result["bsvCompany"],
        )
        self.assertEqual(
            "SCHWAB GOVERNMENT MONEY FUND - SWEEP SHARES",
            result["swgxxCompany"],
        )
        self.assertEqual("", result["invalidKind"])
        self.assertEqual("RIGHT", result["rightKind"])
        self.assertEqual("Unit", result["unitKindFallback"])
        self.assertEqual("Common Stock", result["commonStockKind"])
        self.assertEqual("Put", result["commonStockPutKind"])
        self.assertEqual("Note", result["commonStockNoteKind"])
        self.assertEqual("Preferred", result["commonStockPreferredKind"])
        self.assertEqual("Warrant", result["commonStockWarrantKind"])
        self.assertEqual("Equity", result["unknownEquityKind"])
        self.assertEqual("ETF", result["searchEtfTag"])
        self.assertEqual("stock", result["searchEtfClass"])
        self.assertEqual("AAPL", result["equity"])
        self.assertEqual(
            "IQIYI INC · NOTE 6.500% 3/1",
            result["classFallback"],
        )
        self.assertEqual("Note security", result["rejectedRawMap"])
        self.assertEqual("Warrant security", result["genericFallback"])

    def test_security_labels_load_before_render_and_keep_search_semantics(
        self,
    ) -> None:
        self.assertIn('"data/security_labels.json"', self.html)
        self.assertGreaterEqual(
            self.html.count("const securityLabelsReady = ensureSecurityLabels();"),
            3,
        )
        self.assertGreaterEqual(
            self.html.count("await securityLabelsReady;"),
            3,
        )
        self.assertNotIn("note-security-cell", self.html)
        self.assertNotIn("note-security-position", self.html)
        self.assertIn("security-label-cell", self.html)
        self.assertIn("security-label-position", self.html)
        self.assertIn("securityKinds = normalizeSecurityKindPayload(data)", self.html)
        self.assertIn(
            "securityProductNames = normalizeSecurityProductNamePayload(data)",
            self.html,
        )
        self.assertIn(
            "securityFundIdentities = normalizeSecurityFundIdentityPayload(data)",
            self.html,
        )
        self.assertIn(
            'assertRequiredSecurityMetadata(data, "data/security_labels.json")',
            self.html,
        )
        self.assertIn(
            'new RequiredSiteDataError(\n'
            '              "data/security_labels.json",',
            self.html,
        )
        self.assertIn(
            '${esc(securityKindClass)}">${esc(securityKindText)}</span>',
            self.html,
        )
        self.assertIn("holdingDisplayCompany(securityHolding)", self.html)
        self.assertNotIn("const displayTicker = h.ticker ?", self.html)
        self.assertIn(
            """onclick="loadStock('${esc(lookupId)}')">${esc(displayLabel)}""",
            self.html,
        )
        self.assertIn(
            """>Security<span class="arr"></span></th>""",
            self.html,
        )
        self.assertIn(
            'case "ticker":  return (holdingDisplayLabel(r) || "~~~")',
            self.html,
        )
        self.assertIn(
            'case "holdingType": return holdingDisplayKind(r);',
            self.html,
        )
        self.assertIn(
            'case "issuer":  return (holdingDisplayCompany(r) || "~~~")',
            self.html,
        )
        self.assertEqual(
            2,
            self.html.count('title="${esc(companyName)}"'),
        )
        self.assertEqual(
            2,
            self.html.count("${esc(holdingDisplayKindLabel(h))}"),
        )
        detail_start = self.html.index("const securityHolding = {")
        detail_end = self.html.index("const latestDateText =", detail_start)
        detail_logic = self.html[detail_start:detail_end]
        self.assertIn(
            "holdingDisplayKindLabel(securityHolding)",
            detail_logic,
        )
        self.assertNotIn(
            "holdingDisplayKind(securityHolding)",
            detail_logic,
        )
        search_start = self.html.index("function globalSearch(")
        search_end = self.html.index("function showEmpty(", search_start)
        search_logic = self.html[search_start:search_end]
        self.assertIn("if (!isCommonStockSearchEntry(entry)) continue;", search_logic)
        self.assertNotIn("securityLabels", search_logic)
        self.assertIn(
            "securityProductNameForCusip(entry.cusip)",
            search_logic,
        )
        self.assertIn(
            "const symbol = tickerSearchSymbol(entry).toUpperCase();",
            search_logic,
        )
        self.assertIn("${esc(tickerSearchSymbol(t))}", search_logic)
        self.assertIn("searchEntryTagLabel(t)", search_logic)
        self.assertIn(
            "dedupeVisuallyIdenticalTickerMatches(\n"
            "    tickerMatches\n"
            "  ).slice(0, 8)",
            search_logic,
        )
        self.assertIn(
            "(?:[-./](?:R|RT|RIGHT|RIGHTS|W|WS|WT|WTS))$",
            self.html,
        )
        init_start = self.html.index("(async function init()")
        init_end = self.html.index("// ---------- URL routing ----------", init_start)
        init_logic = self.html[init_start:init_end]
        self.assertLess(
            init_logic.index("await securityLabelsReady;"),
            init_logic.index("wireGlobalSearch();"),
        )
        stock_start = self.html.index("async function loadStock(")
        stock_end = self.html.index("function renderStock(", stock_start)
        stock_logic = self.html[stock_start:stock_end]
        self.assertLess(
            stock_logic.index("await securityLabelsReady;"),
            stock_logic.index("canonicalStockLookupId(stockId)"),
        )
        self.assertIn("resolveStockEntry(canonicalId)", stock_logic)
        self.assertIn(
            "stockEntry ? stockEntry.stock_id : canonicalId",
            stock_logic,
        )

    def test_required_security_metadata_failure_is_release_blocking(
        self,
    ) -> None:
        contract_start = self.html.index("const DATA_CONTRACT_VERSION")
        contract_end = self.html.index(
            "// ---------- formatters ----------",
            contract_start,
        )
        contract_logic = self.html[contract_start:contract_end]
        completed = subprocess.run(
            [
                "node",
                "-e",
                (
                    f"{contract_logic}\n{self.logic}\n"
                    "global.fetch = async () => {"
                    "  throw new Error('offline');"
                    "};\n"
                    "ensureSecurityLabels().then(\n"
                    "  () => console.log(JSON.stringify({required:false})),\n"
                    "  error => console.log(JSON.stringify({\n"
                    "    required: error instanceof RequiredSiteDataError\n"
                    "  }))\n"
                    ");\n"
                ),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {"required": True},
            json.loads(completed.stdout),
        )

    def test_common_stock_search_keeps_unit_named_issuers(self) -> None:
        result = self.run_javascript(
            """
            console.log(JSON.stringify({
              unitCorp: isCommonStockSearchEntry({
                ticker: "UNTC",
                issuer: "UNIT CORP",
                instrument_type: "EQUITY",
              }),
              right: isCommonStockSearchEntry({
                ticker: "BKT-R",
                issuer: "BLACKROCK INCOME TRUST INC",
                instrument_type: "EQUITY",
              }),
              warrant: isCommonStockSearchEntry({
                ticker: "FLYX/WS",
                issuer: "FLYEXCLUSIVE INC",
                instrument_type: "EQUITY",
              }),
            }));
            """
        )
        self.assertTrue(result["unitCorp"])
        self.assertFalse(result["right"])
        self.assertFalse(result["warrant"])

    def test_ticker_search_chooses_highest_coverage_alias_per_display_kind(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            securityKinds = normalizeSecurityKindPayload({
              kinds: {
                "046090E10": "ETF",
                "46090E103": "ETF",
                "46090E903": "ETF",
                "99999Q999": "COMMON",
              },
            });
            const ranked = [
              {
                ticker: "QQQ",
                issuer: "INVESCO QQQ TRUST",
                cusip: "046090E10",
                instrument_type: "EQUITY",
                stock_id: "046090E10",
                last_seen: "2026-03-31",
                current_holder_count: 1,
                holder_count: 1,
              },
              {
                ticker: "QQQ",
                issuer: "INVESCO QQQ TRUST",
                cusip: "46090E103",
                instrument_type: "EQUITY",
                stock_id: "46090E103",
                last_seen: "2026-06-30",
                current_holder_count: 4000,
                holder_count: 4361,
              },
              {
                ticker: "QQQ",
                issuer: "INVESCO QQQ TRUST",
                cusip: "46090E903",
                instrument_type: "EQUITY",
                stock_id: "46090E903",
                last_seen: "2026-06-30",
                current_holder_count: 3,
                holder_count: 3,
              },
              {
                ticker: "QQQ",
                issuer: "A DISTINCT COMMON QQQ ISSUER",
                cusip: "99999Q999",
                instrument_type: "EQUITY",
                stock_id: "99999Q999",
                last_seen: "2026-06-30",
                current_holder_count: 9999,
                holder_count: 9999,
              },
            ];
            const deduped = dedupeVisuallyIdenticalTickerMatches(
              ranked
            );
            console.log(JSON.stringify({
              stockIds: deduped.map(entry => entry.stock_id),
              firstKey: tickerSearchVisualKey(
                ranked[0]
              ),
              aliasKey: tickerSearchVisualKey(
                ranked[1]
              ),
            }));
            """
        )

        self.assertEqual(
            ["46090E103", "99999Q999"],
            result["stockIds"],
        )
        self.assertEqual(result["firstKey"], result["aliasKey"])

    def test_ticker_alias_ties_use_stable_stock_id_fallback(self) -> None:
        result = self.run_javascript(
            """
            securityKinds = normalizeSecurityKindPayload({
              kinds: {
                "46090E103": "ETF",
                "46090E903": "ETF",
              },
            });
            const aliases = [
              {
                ticker: "QQQ",
                cusip: "46090E903",
                instrument_type: "EQUITY",
                stock_id: "46090E903",
                current_holder_count: 10,
                holder_count: 10,
              },
              {
                ticker: "QQQ",
                cusip: "46090E103",
                instrument_type: "EQUITY",
                stock_id: "46090E103",
                current_holder_count: 10,
                holder_count: 10,
              },
            ];
            console.log(JSON.stringify({
              stockIds: dedupeVisuallyIdenticalTickerMatches(aliases)
                .map(entry => entry.stock_id),
              missingCount: tickerSearchHolderCount({}),
              nullCount: tickerSearchHolderCount({
                holder_count: null,
              }),
              malformedCount: tickerSearchHolderCount({
                holder_count: "10",
              }),
              missingCurrentCount: tickerSearchCurrentHolderCount({}),
              malformedCurrentCount: tickerSearchCurrentHolderCount({
                current_holder_count: "10",
              }),
              missingRecency: tickerSearchLastSeen({}),
              malformedRecency: tickerSearchLastSeen({
                last_seen: "06/30/2026",
              }),
              normalizedCount: normalizeTickerEntry({
                ticker: "QQQ",
                cusip: "46090E103",
                instrument_type: "EQUITY",
                stock_id: "46090E103",
                last_seen: "2026-06-30",
                current_holder_count: 4000,
                holder_count: 4361,
              }),
            }));
            """
        )

        self.assertEqual(["46090E103"], result["stockIds"])
        self.assertEqual(-1, result["missingCount"])
        self.assertEqual(-1, result["nullCount"])
        self.assertEqual(-1, result["malformedCount"])
        self.assertEqual(-1, result["missingCurrentCount"])
        self.assertEqual(-1, result["malformedCurrentCount"])
        self.assertEqual("", result["missingRecency"])
        self.assertEqual("", result["malformedRecency"])
        self.assertEqual(
            4000,
            result["normalizedCount"]["current_holder_count"],
        )
        self.assertEqual(4361, result["normalizedCount"]["holder_count"])
        self.assertEqual(
            "2026-06-30",
            result["normalizedCount"]["last_seen"],
        )

    def test_ticker_alias_current_coverage_precedes_lifetime_coverage(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            const aliases = [
              {
                ticker: "TLRY",
                cusip: "88688T100",
                instrument_type: "EQUITY",
                stock_id: "88688T100",
                last_seen: "2026-03-31",
                current_holder_count: 3,
                holder_count: 353,
              },
              {
                ticker: "TLRY",
                cusip: "88688T209",
                instrument_type: "EQUITY",
                stock_id: "88688T209",
                last_seen: "2026-06-30",
                current_holder_count: 194,
                holder_count: 247,
              },
            ];
            currentReportingQuarter = 20261;
            const duringTransition = [...aliases].sort(
              compareTickerAliasRepresentative
            )[0].stock_id;
            currentReportingQuarter = 20262;
            console.log(JSON.stringify({
              currentWinner: [...aliases].sort(
                compareTickerAliasRepresentative
              )[0].stock_id,
              duringTransition,
            }));
            """
        )

        self.assertEqual("88688T209", result["currentWinner"])
        self.assertEqual("88688T209", result["duringTransition"])

    def test_ticker_routes_choose_current_cusips_but_keep_exact_old_routes(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            idx = {
              tickers: [
                {
                  ticker: "TLRY",
                  cusip: "88688T100",
                  instrument_type: "EQUITY",
                  stock_id: "88688T100",
                  last_seen: "2026-03-31",
                  current_holder_count: 3,
                  holder_count: 353,
                },
                {
                  ticker: "TLRY",
                  cusip: "88688T209",
                  instrument_type: "EQUITY",
                  stock_id: "88688T209",
                  last_seen: "2026-06-30",
                  current_holder_count: 194,
                  holder_count: 247,
                },
                {
                  ticker: "FUBO",
                  cusip: "35953D104",
                  instrument_type: "EQUITY",
                  stock_id: "35953D104",
                  last_seen: "2026-06-30",
                  current_holder_count: 3,
                  holder_count: 286,
                },
                {
                  ticker: "FUBO",
                  cusip: "35953D401",
                  instrument_type: "EQUITY",
                  stock_id: "35953D401",
                  last_seen: "2026-06-30",
                  current_holder_count: 138,
                  holder_count: 147,
                },
              ],
            };
            currentReportingQuarter = 20261;
            console.log(JSON.stringify({
              tlry: resolveStockEntry("TLRY").stock_id,
              fubo: resolveStockEntry("FUBO").stock_id,
              exactOldTlry: resolveStockEntry("88688T100").stock_id,
              exactOldFubo: resolveStockEntry("35953D104").stock_id,
            }));
            """
        )

        self.assertEqual("88688T209", result["tlry"])
        self.assertEqual("35953D401", result["fubo"])
        self.assertEqual("88688T100", result["exactOldTlry"])
        self.assertEqual("35953D104", result["exactOldFubo"])

    def test_ticker_search_uses_cusip_mapped_symbols_for_bad_raw_aliases(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            securityLabels = normalizeSecurityLabelPayload({
              labels: {
                "74347X831": "TQQQ",
                "74350P675": "SQQQ",
                "143658300": "CARNIVAL CORP LTD",
              },
            });
            securityKinds = normalizeSecurityKindPayload({
              kinds: {
                "74347X831": "ETF",
                "74350P675": "ETF",
                "143658300": "COMMON",
              },
            });
            const aliases = [
              {
                ticker: "AGQ",
                issuer: "PROSHARES TRUST",
                cusip: "74347X831",
                instrument_type: "EQUITY",
                stock_id: "74347X831",
                _matchRank: 0,
              },
              {
                ticker: "AGQ",
                issuer: "PROSHARES TRUST",
                cusip: "74350P675",
                instrument_type: "EQUITY",
                stock_id: "74350P675",
                _matchRank: 0,
              },
              {
                ticker: "CCL",
                issuer: "Carnival Corp",
                cusip: "143658300",
                instrument_type: "EQUITY",
                stock_id: "143658300",
                _matchRank: 0,
              },
            ];
            const deduped = dedupeVisuallyIdenticalTickerMatches(aliases);
            console.log(JSON.stringify({
              symbols: aliases.map(tickerSearchSymbol),
              deduped: deduped.map(entry => entry.stock_id),
            }));
            """
        )
        self.assertEqual(["TQQQ", "SQQQ", "CCL"], result["symbols"])
        self.assertEqual(
            ["74347X831", "74350P675", "143658300"],
            result["deduped"],
        )

    def test_modal_quarter_stale_and_withheld_states_fail_closed(self) -> None:
        result = self.run_javascript(
            """
            const baseline = modalLatestReportingQuarter([
              {q: [20261, 20254]},
              {q: [20261, 20254]},
              {q: [20254, 20253]},
              {q: [20262, 20261]},
            ]);
            const tieBaseline = modalLatestReportingQuarter([
              {q: [20261]},
              {q: [20262]},
            ]);
            const quarantineWaveBaseline = modalLatestReportingQuarter([
              {q: [20261, 20254]},
              {q: [20261, 20254]},
              {q: [20254], status: "WITHHELD"},
              {q: [20254], latest_withheld_report_date: "2026-03-31"},
              {q: [20254], quarantined: true},
              {q: [20254, 20254]},
            ]);
            const fresh = alignHolderHistory(
              [{date: "2026-03-31", shares: 20, value: 200}],
              {q: [20261, 20254]},
              baseline
            );
            const ahead = alignHolderHistory(
              [{date: "2026-06-30", shares: 30, value: 300}],
              {q: [20262, 20261]},
              baseline
            );
            const stale = alignHolderHistory(
              [{date: "2025-12-31", shares: 10, value: 100}],
              {q: [20254, 20253]},
              baseline
            );
            const staleFormerHolder = alignHolderHistory(
              [{date: "2025-09-30", shares: 8, value: 80}],
              {q: [20254, 20253]},
              baseline
            );
            const withheld = alignHolderHistory(
              [{date: "2026-03-31", shares: 40, value: 400}],
              {
                q: [20261, 20254],
                status: "quarantined",
                latest_withheld_report_date: "2026-06-30",
              },
              baseline
            );
            const withheldByDate = alignHolderHistory(
              [{date: "2026-03-31", shares: 40, value: 400}],
              {
                q: [20261, 20254],
                latest_withheld_report_date: "2026-06-30",
              },
              baseline
            );
            const invalid = alignHolderHistory(
              [{date: "2026-03-31", shares: 1, value: 1}],
              {q: [20261, 20261]},
              baseline
            );
            console.log(JSON.stringify({
              baseline,
              tieBaseline,
              quarantineWaveBaseline,
              fresh: fresh.state,
              ahead: ahead.state,
              stale: stale.state,
              staleChange: stale.ch,
              staleFormerHolder: staleFormerHolder.state,
              staleReference: staleFormerHolder.reference.date,
              withheld: withheld.state,
              withheldDate: withheld.withheld.reportDate,
              withheldChange: withheld.ch,
              withheldByDate: withheldByDate.state,
              invalid: invalid.state,
            }));
            """
        )

        self.assertEqual(20261, result["baseline"])
        self.assertEqual(20262, result["tieBaseline"])
        self.assertEqual(20261, result["quarantineWaveBaseline"])
        self.assertEqual("CURRENT", result["fresh"])
        self.assertEqual("CURRENT", result["ahead"])
        self.assertEqual("STALE", result["stale"])
        self.assertIsNone(result["staleChange"])
        self.assertEqual("STALE", result["staleFormerHolder"])
        self.assertEqual("2025-09-30", result["staleReference"])
        self.assertEqual("WITHHELD", result["withheld"])
        self.assertEqual("2026-06-30", result["withheldDate"])
        self.assertIsNone(result["withheldChange"])
        self.assertEqual("WITHHELD", result["withheldByDate"])
        self.assertEqual("UNKNOWN", result["invalid"])

    def test_imputed_marker_survives_grouping_and_suppresses_changes(self) -> None:
        result = self.run_javascript(
            """
            const grouped = groupHoldingsByKey([
              {cusip: "123456789", shares: 10, value: 100},
              {
                cusip: "123456789",
                shares: 5,
                value: 50,
                shares_imputed: true,
              },
            ]);
            const aligned = alignHolderHistory(
              [
                {
                  date: "2026-03-31",
                  shares: 15,
                  value: 150,
                  shares_imputed: true,
                },
                {date: "2025-12-31", shares: 10, value: 100},
              ],
              {q: [20261, 20254]},
              20261
            );
            console.log(JSON.stringify({
              grouped,
              alignedChange: aligned.ch,
              alignedSpark: aligned.sparkShares,
              imputedIncrease: positionChange(
                {shares: 15, shares_imputed: true},
                {shares: 10}
              ),
              imputedExit: positionChange(
                null,
                {shares: 10, shares_imputed: true}
              ),
              imputedNew: positionChange(
                {shares: 10, shares_imputed: true},
                null
              ),
            }));
            """
        )

        self.assertEqual(15, result["grouped"][0]["shares"])
        self.assertEqual(150, result["grouped"][0]["value"])
        self.assertIs(result["grouped"][0]["shares_imputed"], True)
        self.assertIsNone(result["alignedChange"])
        self.assertEqual([], result["alignedSpark"])
        self.assertIsNone(result["imputedIncrease"])
        self.assertIsNone(result["imputedExit"])
        self.assertIsNone(result["imputedNew"])

    def test_missing_calendar_quarter_suppresses_change_and_exit(self) -> None:
        result = self.run_javascript(
            """
            const history = [
              {date: "2026-03-31", shares: 20, value: 200},
              {date: "2025-09-30", shares: 10, value: 100},
            ];
            const current = alignHolderHistory(
              history,
              {q: [20261, 20253]},
              20261
            );
            const historical = alignHolderHistory(
              [history[1]],
              {q: [20261, 20253]},
              20261
            );
            console.log(JSON.stringify({
              adjacentYearBoundary: areAdjacentQuarterCodes(20261, 20254),
              gapIsAdjacent: areAdjacentQuarterCodes(20261, 20253),
              currentState: current.state,
              currentPrevious: current.previous,
              currentChange: current.ch,
              historicalState: historical.state,
              fundGuardPresent: areAdjacentReportDates(
                "2026-03-31",
                "2025-09-30"
              ),
            }));
            """
        )

        self.assertIs(result["adjacentYearBoundary"], True)
        self.assertIs(result["gapIsAdjacent"], False)
        self.assertEqual("CURRENT", result["currentState"])
        self.assertIsNone(result["currentPrevious"])
        self.assertIsNone(result["currentChange"])
        self.assertEqual("HISTORICAL", result["historicalState"])
        self.assertIs(result["fundGuardPresent"], False)
        self.assertIn(
            "areAdjacentReportDates(cur.report_date, priorCandidate.report_date)",
            self.html,
        )

    def test_split_changes_require_explicit_proven_adjustment(self) -> None:
        result = self.run_javascript(
            """
            const current = {date: "2025-12-31", shares: 101250, value: 100};
            const previous = {date: "2025-09-30", shares: 10000, value: 90};
            const proven = [{
              from_report_date: "2025-09-30",
              to_report_date: "2025-12-31",
              factor: 10,
              proven: true,
            }];
            const unproven = [{...proven[0], proven: false}];
            const alignedProven = alignHolderHistory(
              [current, previous],
              {q: [20254, 20253]},
              20254,
              {splitAdjustments: proven}
            );
            const alignedUnproven = alignHolderHistory(
              [current, previous],
              {q: [20254, 20253]},
              20254,
              {splitAdjustments: unproven}
            );
            console.log(JSON.stringify({
              ambiguous: positionChange(current, previous),
              adjusted: positionChange(
                current,
                previous,
                {splitFactor: 10}
              ),
              normal: positionChange({shares: 150}, {shares: 100}),
              alignedProven: alignedProven.ch,
              alignedUnproven: alignedUnproven.ch,
              provenSpark: alignedProven.sparkShares,
              unprovenSpark: alignedUnproven.sparkShares,
              valueSpark: alignedProven.sparkValues,
            }));
            """
        )

        self.assertIsNone(result["ambiguous"])
        self.assertEqual("UP", result["adjusted"]["t"])
        self.assertAlmostEqual(1.25, result["adjusted"]["p"])
        self.assertEqual({"t": "UP", "p": 50}, result["normal"])
        self.assertEqual(result["adjusted"], result["alignedProven"])
        self.assertIsNone(result["alignedUnproven"])
        self.assertEqual([], result["provenSpark"])
        self.assertEqual([], result["unprovenSpark"])
        self.assertEqual([90, 100], result["valueSpark"])

    def test_current_aggregates_exclude_stale_withheld_and_estimates(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            const rows = [
              {
                state: "CURRENT",
                shares: 10,
                sparkQuarters: [20254, 20261],
                valueSparkData: [1, 2],
                sparkData: [1, 2],
              },
              {
                state: "CURRENT",
                shares: 999,
                sharesImputed: true,
                sparkQuarters: [20254, 20261],
                valueSparkData: [3, 4],
                sparkData: [null, null],
              },
              {
                state: "EXIT",
                sparkQuarters: [20254, 20261],
                valueSparkData: [5, 0],
                sparkData: [5, 0],
              },
              {
                state: "STALE",
                shares: 1000000,
                sparkQuarters: [20254, 20261],
                valueSparkData: [1000000, 1000000],
                sparkData: [1000000, 1000000],
              },
              {
                state: "WITHHELD",
                shares: 2000000,
                sparkQuarters: [20254, 20261],
                valueSparkData: [2000000, 2000000],
                sparkData: [2000000, 2000000],
              },
              {state: "UNKNOWN"},
            ];
            const parts = partitionHolderStates(rows);
            console.log(JSON.stringify({
              counts: {
                current: parts.current.length,
                exits: parts.exits.length,
                stale: parts.stale.length,
                withheld: parts.withheld.length,
                unknown: parts.unknown.length,
              },
              exactShares: exactReportedShareTotal(parts.current),
              trends: aggregateEligibleHolderTrends(rows, 20261, 2),
            }));
            """
        )

        self.assertEqual(
            {
                "current": 2,
                "exits": 1,
                "stale": 1,
                "withheld": 1,
                "unknown": 1,
            },
            result["counts"],
        )
        self.assertEqual(10, result["exactShares"])
        self.assertEqual([9, 6], result["trends"]["values"])
        self.assertEqual([], result["trends"]["shares"])
        self.assertEqual([20254, 20261], result["trends"]["quarters"])

    def test_aggregate_trends_require_one_complete_reporting_cohort(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            const mixedCalendars = [
              {
                state: "CURRENT",
                sparkQuarters: [20254, 20261],
                valueSparkData: [1, 2],
                sparkData: [3, 4],
              },
              {
                state: "CURRENT",
                sparkQuarters: [20261, 20262],
                valueSparkData: [10, 20],
                sparkData: [30, 40],
              },
            ];
            const sameCalendar = [
              mixedCalendars[0],
              {
                state: "EXIT",
                sparkQuarters: [20254, 20261],
                valueSparkData: [10, 0],
                sparkData: [30, 0],
              },
            ];
            const incompleteValues = [
              sameCalendar[0],
              {...sameCalendar[1], valueSparkData: [10]},
            ];
            const nullValues = [
              sameCalendar[0],
              {...sameCalendar[1], valueSparkData: [10, null]},
            ];
            const aheadOnly = [
              {
                state: "CURRENT",
                sparkQuarters: [20261, 20262],
                valueSparkData: [1, 2],
                sparkData: [3, 4],
              },
              {
                state: "CURRENT",
                sparkQuarters: [20261, 20262],
                valueSparkData: [10, 20],
                sparkData: [30, 40],
              },
            ];
            console.log(JSON.stringify({
              mixed: aggregateEligibleHolderTrends(
                mixedCalendars, 20261, 2
              ),
              same: aggregateEligibleHolderTrends(
                sameCalendar, 20261, 2
              ),
              incomplete: aggregateEligibleHolderTrends(
                incompleteValues, 20261, 2
              ),
              nullValues: aggregateEligibleHolderTrends(
                nullValues, 20261, 2
              ),
              aheadOnly: aggregateEligibleHolderTrends(
                aheadOnly, 20261, 2
              ),
            }));
            """
        )

        self.assertEqual(
            {"quarters": [], "values": [], "shares": []},
            result["mixed"],
        )
        self.assertEqual([20254, 20261], result["same"]["quarters"])
        self.assertEqual([11, 2], result["same"]["values"])
        self.assertEqual([33, 4], result["same"]["shares"])
        self.assertEqual(
            {"quarters": [], "values": [], "shares": []},
            result["incomplete"],
        )
        self.assertEqual(
            {"quarters": [], "values": [], "shares": []},
            result["nullValues"],
        )
        self.assertEqual(
            {"quarters": [], "values": [], "shares": []},
            result["aheadOnly"],
        )

    def test_aggregate_trends_include_older_verified_holders(self) -> None:
        result = self.run_javascript(
            """
            const calendar = [20252, 20253, 20254, 20261];
            const rows = [
              {
                state: "CURRENT",
                sparkQuarters: calendar,
                valueSparkData: [0, 0, 0, 2350],
                sparkData: [],
              },
              {
                state: "HISTORICAL",
                sparkQuarters: calendar,
                valueSparkData: [284739, 0, 0, 0],
                sparkData: [],
              },
              {
                state: "STALE",
                sparkQuarters: calendar,
                valueSparkData: [999999, 999999, 999999, 999999],
                sparkData: [999999, 999999, 999999, 999999],
              },
            ];
            console.log(JSON.stringify(
              aggregateEligibleHolderTrends(rows, 20261, 4)
            ));
            """
        )

        self.assertEqual(
            [20252, 20253, 20254, 20261],
            result["quarters"],
        )
        self.assertEqual([284739, 0, 0, 2350], result["values"])
        self.assertEqual([], result["shares"])

    def test_malformed_unverified_dates_make_filing_state_unknown(
        self,
    ) -> None:
        result = self.run_javascript(
            """
            const currentQuarter = 20261;
            const fixtures = {
              absent: {q: [20261, 20254]},
              empty: {q: [20261, 20254], unverified_report_dates: []},
              wrongType: {
                q: [20261, 20254],
                unverified_report_dates: "2025-12-31",
              },
              duplicate: {
                q: [20261, 20254],
                unverified_report_dates: ["2025-12-31", "2025-12-31"],
              },
              invalidDate: {
                q: [20261, 20254],
                unverified_report_dates: ["2025-12-30"],
              },
              invalidElementType: {
                q: [20261, 20254],
                unverified_report_dates: [["2025-12-31"]],
              },
              wrongOrder: {
                q: [20261, 20254],
                unverified_report_dates: ["2025-09-30", "2025-12-31"],
              },
              outsideCalendar: {
                q: [20261, 20254],
                unverified_report_dates: ["2025-09-30"],
              },
            };
            console.log(JSON.stringify(Object.fromEntries(
              Object.entries(fixtures).map(([name, fixture]) => [
                name,
                fundIndexFilingState(fixture, currentQuarter).state,
              ])
            )));
            """
        )

        self.assertEqual("CURRENT", result["absent"])
        self.assertEqual("CURRENT", result["empty"])
        for key in (
            "wrongType",
            "duplicate",
            "invalidDate",
            "invalidElementType",
            "wrongOrder",
            "outsideCalendar",
        ):
            with self.subTest(key=key):
                self.assertEqual("UNKNOWN", result[key])

    def test_fund_changes_use_bootstrap_split_proof(self) -> None:
        result = self.run_javascript(
            """
            const splitMap = {
              "26923G822": [{
                from_report_date: "2025-09-30",
                to_report_date: "2025-12-31",
                factor: 10,
                proven: true,
              }],
            };
            const factor = provenSplitFactorForPeriod(
              splitMap["26923G822"],
              "2025-09-30",
              "2025-12-31"
            );
            const stringFactor = provenSplitFactorForPeriod(
              [{...splitMap["26923G822"][0], factor: "10"}],
              "2025-09-30",
              "2025-12-31"
            );
            console.log(JSON.stringify({
              factor,
              stringFactor,
              adjusted: positionChange(
                {shares: 10976},
                {shares: 1000},
                {splitFactor: factor}
              ),
              raw: positionChange({shares: 10976}, {shares: 1000}),
            }));
            """
        )

        self.assertEqual(10, result["factor"])
        self.assertIsNone(result["stringFactor"])
        self.assertEqual("UP", result["adjusted"]["t"])
        self.assertAlmostEqual(9.76, result["adjusted"]["p"])
        self.assertAlmostEqual(997.6, result["raw"]["p"])
        self.assertIn(
            "idx?.proven_split_adjustments?.[key]",
            self.html,
        )
        self.assertIn(
            "positionChange(h, prevRec, { splitFactor })",
            self.html,
        )

    def test_missing_quarter_hides_qoq_trends(self) -> None:
        result = self.run_javascript(
            """
            const gap = alignHolderHistory(
              [
                {date: "2026-03-31", shares: 20, value: 200},
                {date: "2025-09-30", shares: 10, value: 100},
              ],
              {q: [20261, 20253]},
              20261
            );
            const contiguous = alignHolderHistory(
              [
                {date: "2026-03-31", shares: 15, value: 200},
                {date: "2025-12-31", shares: 10, value: 100},
              ],
              {q: [20261, 20254]},
              20261
            );
            const unverified = alignHolderHistory(
              [
                {date: "2026-03-31", shares: 20, value: 200},
                {date: "2025-12-31", shares: 10, value: 100},
              ],
              {
                q: [20261, 20254],
                unverified_report_dates: ["2025-12-31"],
              },
              20261
            );
            const unknown = alignHolderHistory(
              [
                {date: "2026-03-31", shares: 20, value: 200},
                {date: "2025-12-31", shares: 10, value: 100},
              ],
              {
                q: [20261, 20254],
                unverified_report_dates: "2025-12-31",
              },
              20261
            );
            console.log(JSON.stringify({
              gapChange: gap.ch,
              gapShares: gap.sparkShares,
              gapValues: gap.sparkValues,
              contiguousShares: contiguous.sparkShares,
              contiguousValues: contiguous.sparkValues,
              unverifiedState: unverified.state,
              unverifiedChange: unverified.ch,
              unverifiedShares: unverified.sparkShares,
              unverifiedValues: unverified.sparkValues,
              unknownState: unknown.state,
              unknownChange: unknown.ch,
              unknownShares: unknown.sparkShares,
              unknownValues: unknown.sparkValues,
            }));
            """
        )

        self.assertIsNone(result["gapChange"])
        self.assertEqual([], result["gapShares"])
        self.assertEqual([], result["gapValues"])
        self.assertEqual([10, 15], result["contiguousShares"])
        self.assertEqual([100, 200], result["contiguousValues"])
        self.assertEqual("CURRENT", result["unverifiedState"])
        self.assertIsNone(result["unverifiedChange"])
        self.assertEqual([], result["unverifiedShares"])
        self.assertEqual([], result["unverifiedValues"])
        self.assertEqual("UNKNOWN", result["unknownState"])
        self.assertIsNone(result["unknownChange"])
        self.assertEqual([], result["unknownShares"])
        self.assertEqual([], result["unknownValues"])

    def test_unknown_quantities_suppress_changes_and_exact_totals(self) -> None:
        result = self.run_javascript("""
            const unknown = {shares: 0, value: 1000, quantity_unknown: true};
            const actual = {shares: 10, value: 1000};
            console.log(JSON.stringify({
              newChange: positionChange(unknown, null),
              exitChange: positionChange(null, unknown),
              currentChange: positionChange(actual, unknown),
              trend: shareTrendIsComparable([unknown, actual], []),
              total: exactReportedShareTotal([
                {state: "CURRENT", shares: 10},
                {state: "CURRENT", shares: 100, quantityUnknown: true}
              ])
            }));
        """)
        self.assertEqual({"newChange": None, "exitChange": None, "currentChange": None, "trend": False, "total": 10}, result)

    def test_rendering_wires_exclusions_and_estimated_share_labels(self) -> None:
        required_fragments = [
            "currentReportingQuarter = modalLatestReportingQuarter(idx.funds)",
            '"Stale / Excluded Records"',
            '"Withheld / Unverified Records"',
            "Total Exact Shares",
            "formatShares(h.shares, h.sharesImputed, h.quantityUnknown)",
            "formatShares(h.shares, h.shares_imputed, h.quantity_unknown)",
            "aggregateEligibleHolderTrends(\n    classified,\n    currentReportingQuarter",
            "Estimated rows are marked with ~",
            "idx?.proven_split_adjustments?.[key]",
            "rawQuarters\n    .slice(0, 4)",
            "date: q.report_date",
            "Some historical filing data is unverified.",
            'const comparisonsVerified = filingState.state !== "UNKNOWN";',
            "shareTrendIsComparable(",
            "Aggregate 4Q trends are hidden because manager histories do not share one complete reporting calendar",
        ]
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.html)
        self.assertNotIn("h.split_adjustments", self.html)


if __name__ == "__main__":
    unittest.main()
