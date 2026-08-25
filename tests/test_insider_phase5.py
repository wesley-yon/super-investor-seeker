from __future__ import annotations

import base64
import copy
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import cast

from insider_publication import (
    InsiderPublicationError,
    build_insider_publication,
    canonical_public_json_bytes,
    write_insider_publication,
)
from security_identity import stock_file_stem
from tests.test_insider_publication import (
    AS_OF,
    STOCK_ID,
    SYNC_AT,
    issuer_state,
    parse_case,
    security_mapping,
)


ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = ROOT / "index.html"


class InsiderPhase5LiveAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = INDEX_HTML.read_text(encoding="utf-8")
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        cls.live_payload = publication.security_payloads[stock_file_stem(STOCK_ID)]
        cls.live_accession, cls.live_filing = next(
            iter(publication.filing_payloads.items())
        )
        cls.live_filing_bytes = canonical_public_json_bytes(cls.live_filing)

    def _adapter_section(self) -> str:
        start = self.html.index("const INSIDER_PUBLIC_SECURITY_STEM_RE =")
        end = self.html.index("function holdingHistoryKey(", start)
        return self.html[start:end]

    def _run_node(self, script: str, payload: dict[str, object]) -> dict:
        completed = subprocess.run(
            ["node", "-e", self._adapter_section() + script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            input=json.dumps(payload),
            text=True,
        )
        return json.loads(completed.stdout)

    def _validate(self, payload: dict[str, object], expected_stock_id: str) -> dict:
        return self._run_node(
            """
            const fs = require("node:fs");
            const input = JSON.parse(fs.readFileSync(0, "utf8"));
            const available =
              typeof validateLiveInsiderSecurityPayload === "function";
            let accepted = false;
            let error = null;
            if (available) {
              try {
                accepted = validateLiveInsiderSecurityPayload(
                  input.payload,
                  input.expectedStockId
                ) === input.payload;
              } catch (caught) {
                error = String(caught?.message || caught);
              }
            }
            console.log(JSON.stringify({ available, accepted, error }));
            """,
            {"payload": payload, "expectedStockId": expected_stock_id},
        )

    def _validate_filing(
        self,
        payload: dict[str, object],
        expected_accession: str,
    ) -> dict:
        return self._run_node(
            """
            const fs = require("node:fs");
            const input = JSON.parse(fs.readFileSync(0, "utf8"));
            const available =
              typeof validateLiveInsiderFilingPayload === "function";
            let accepted = false;
            let error = null;
            if (available) {
              try {
                accepted = validateLiveInsiderFilingPayload(
                  input.payload,
                  input.expectedAccession
                ) === input.payload;
              } catch (caught) {
                error = String(caught?.message || caught);
              }
            }
            console.log(JSON.stringify({ available, accepted, error }));
            """,
            {"payload": payload, "expectedAccession": expected_accession},
        )

    def _load_security(
        self,
        stock_id: str,
        status: int,
        payload: dict[str, object],
        *,
        encoded: bytes | None = None,
    ) -> dict:
        body = encoded if encoded is not None else canonical_public_json_bytes(payload)
        return self._run_node(
            """
            const fs = require("node:fs");
            const input = JSON.parse(fs.readFileSync(0, "utf8"));
            const available =
              typeof loadLiveInsiderSecurityPayload === "function";
            const fetchCalls = [];
            let acceptedId = null;
            let missing = false;
            let error = null;
            (async () => {
              if (available) {
                const body = Buffer.from(input.bodyBase64, "base64");
                const response = {
                  ok: input.status >= 200 && input.status < 300,
                  status: input.status,
                  json: async () => JSON.parse(body.toString("utf8")),
                  arrayBuffer: async () => body.buffer.slice(
                    body.byteOffset,
                    body.byteOffset + body.byteLength
                  ),
                };
                try {
                  const loaded = await loadLiveInsiderSecurityPayload(
                    input.stockId,
                    async (path, options) => {
                      fetchCalls.push({ path, options });
                      return response;
                    }
                  );
                  missing = loaded === null;
                  acceptedId = loaded?.security?.id || null;
                } catch (caught) {
                  error = String(caught?.message || caught);
                }
              }
              console.log(JSON.stringify({
                available,
                acceptedId,
                missing,
                error,
                fetchCalls,
              }));
            })();
            """,
            {
                "stockId": stock_id,
                "status": status,
                "bodyBase64": base64.b64encode(body).decode("ascii"),
            },
        )

    def _fetch_filing(
        self,
        security_payload: dict[str, object],
        accession: str,
        encoded: bytes,
    ) -> dict:
        return self._run_node(
            """
            const fs = require("node:fs");
            const input = JSON.parse(fs.readFileSync(0, "utf8"));
            const available = typeof fetchLiveInsiderFiling === "function";
            const fetchCalls = [];
            let acceptedAccession = null;
            let error = null;
            (async () => {
              if (available) {
                const body = Buffer.from(input.bodyBase64, "base64");
                const response = {
                  ok: true,
                  status: 200,
                  arrayBuffer: async () => body.buffer.slice(
                    body.byteOffset,
                    body.byteOffset + body.byteLength
                  ),
                };
                try {
                  const detail = await fetchLiveInsiderFiling(
                    input.securityPayload,
                    input.accession,
                    async (path, options) => {
                      fetchCalls.push({ path, options });
                      return response;
                    }
                  );
                  acceptedAccession = detail.accessionNumber;
                } catch (caught) {
                  error = String(caught?.message || caught);
                }
              }
              console.log(JSON.stringify({
                available,
                acceptedAccession,
                error,
                fetchCalls,
              }));
            })();
            """,
            {
                "securityPayload": security_payload,
                "accession": accession,
                "bodyBase64": base64.b64encode(encoded).decode("ascii"),
            },
        )

    def test_accepts_exact_generated_phase4_security_payload(self) -> None:
        self.assertEqual(
            {"available": True, "accepted": True, "error": None},
            self._validate(copy.deepcopy(self.live_payload), STOCK_ID),
        )

    def test_accepts_varied_exact_generated_phase4_security_payloads(self) -> None:
        case_names = (
            "form3_amendment",
            "form3_holdings_only",
            "form4_amendment",
            "form4_joint_sale_derivative",
            "form4_unknown_extension",
            "form5_amendment",
            "form5_annual",
        )
        for case_name in case_names:
            with self.subTest(case=case_name):
                filing = parse_case(case_name)
                publication = build_insider_publication(
                    [filing],
                    issuer_state=issuer_state([filing]),
                    security_mappings=security_mapping(filing),
                    as_of=AS_OF,
                    latest_successful_sync_at=SYNC_AT,
                )
                for raw_payload in publication.security_payloads.values():
                    payload = cast(dict[str, object], raw_payload)
                    security = cast(dict[str, object], payload["security"])
                    expected_id = security["id"]
                    self.assertEqual(
                        {"available": True, "accepted": True, "error": None},
                        self._validate(copy.deepcopy(payload), str(expected_id)),
                    )

    def test_checked_in_live_fixtures_match_current_publication_generator(self) -> None:
        security_path = ROOT / "tests/fixtures/phase5-live-security.json"
        filing_path = ROOT / "tests/fixtures/phase5-live-filing.json"
        self.assertEqual(
            canonical_public_json_bytes(self.live_payload),
            security_path.read_bytes(),
        )
        self.assertEqual(self.live_filing_bytes, filing_path.read_bytes())

    def test_checked_in_complex_live_fixtures_match_current_publication_generator(
        self,
    ) -> None:
        filing = parse_case("form4_joint_sale_derivative")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        security_payload = publication.security_payloads[stock_file_stem(STOCK_ID)]
        accession, filing_payload = next(iter(publication.filing_payloads.items()))
        self.assertEqual("0000000001-26-000002", accession)
        self.assertEqual(
            canonical_public_json_bytes(security_payload),
            (ROOT / "tests/fixtures/phase5-live-complex-security.json").read_bytes(),
        )
        self.assertEqual(
            canonical_public_json_bytes(filing_payload),
            (ROOT / "tests/fixtures/phase5-live-complex-filing.json").read_bytes(),
        )

    def test_loads_live_security_payload_without_fixture_fallback(self) -> None:
        result = self._load_security(
            STOCK_ID,
            200,
            copy.deepcopy(self.live_payload),
        )
        self.assertTrue(result["available"])
        self.assertEqual(STOCK_ID, result["acceptedId"])
        self.assertFalse(result["missing"])
        self.assertIsNone(result["error"])
        self.assertEqual(
            [
                {
                    "path": (
                        "data/insiders/public/securities/"
                        f"{stock_file_stem(STOCK_ID)}.json"
                    ),
                    "options": {
                        "cache": "no-store",
                        "credentials": "same-origin",
                    },
                }
            ],
            result["fetchCalls"],
        )
        self.assertNotIn(
            "13f-insider-activity-prd/fixtures",
            result["fetchCalls"][0]["path"],
        )

    def test_live_security_404_is_empty_and_contract_drift_is_generic_error(
        self,
    ) -> None:
        missing = self._load_security(
            STOCK_ID,
            404,
            copy.deepcopy(self.live_payload),
        )
        self.assertTrue(missing["available"])
        self.assertTrue(missing["missing"])
        self.assertIsNone(missing["acceptedId"])
        self.assertIsNone(missing["error"])
        self.assertEqual(1, len(missing["fetchCalls"]))

        malformed = copy.deepcopy(self.live_payload)
        malformed["payloadType"] = "private_insider_corpus"
        failed = self._load_security(STOCK_ID, 200, malformed)
        self.assertTrue(failed["available"])
        self.assertFalse(failed["missing"])
        self.assertIsNone(failed["acceptedId"])
        self.assertEqual(
            "Published insider activity could not be verified",
            failed["error"],
        )
        self.assertEqual(1, len(failed["fetchCalls"]))
        self.assertNotIn(
            "13f-insider-activity-prd/fixtures",
            failed["fetchCalls"][0]["path"],
        )

    def test_live_security_fetch_enforces_public_payload_byte_limit(self) -> None:
        encoded = canonical_public_json_bytes(self.live_payload)
        oversized = encoded + (b" " * (5_000_001 - len(encoded)))
        result = self._load_security(
            STOCK_ID,
            200,
            copy.deepcopy(self.live_payload),
            encoded=oversized,
        )
        self.assertTrue(result["available"])
        self.assertFalse(result["missing"])
        self.assertIsNone(result["acceptedId"])
        self.assertEqual(
            "Published insider activity could not be verified",
            result["error"],
        )
        self.assertEqual(1, len(result["fetchCalls"]))

    def test_accepts_exact_generated_phase4_filing_payload(self) -> None:
        self.assertEqual(
            {"available": True, "accepted": True, "error": None},
            self._validate_filing(
                copy.deepcopy(self.live_filing),
                self.live_accession,
            ),
        )

    def test_accepts_varied_exact_publishable_phase4_filing_payloads(self) -> None:
        case_names = (
            "form3_amendment",
            "form3_holdings_only",
            "form4_amendment",
            "form4_joint_sale_derivative",
            "form5_amendment",
            "form5_annual",
        )
        for case_name in case_names:
            with self.subTest(case=case_name):
                filing = parse_case(case_name)
                publication = build_insider_publication(
                    [filing],
                    issuer_state=issuer_state([filing]),
                    security_mappings=security_mapping(filing),
                    as_of=AS_OF,
                    latest_successful_sync_at=SYNC_AT,
                )
                for accession, raw_payload in publication.filing_payloads.items():
                    payload = cast(dict[str, object], raw_payload)
                    self.assertEqual(
                        {"available": True, "accepted": True, "error": None},
                        self._validate_filing(copy.deepcopy(payload), accession),
                    )

    def test_rejects_generated_but_unpublishable_unknown_extension(self) -> None:
        filing = parse_case("form4_unknown_extension")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        accession, raw_payload = next(iter(publication.filing_payloads.items()))
        payload = cast(dict[str, object], raw_payload)

        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            with self.assertRaisesRegex(
                InsiderPublicationError,
                "equitySwapInvolved must be boolean",
            ):
                write_insider_publication(
                    publication,
                    repository_root=repository_root,
                )
            self.assertFalse(
                (repository_root / "data/insiders/public").exists(),
                "invalid synthetic publication must fail before public mutation",
            )

        result = self._validate_filing(copy.deepcopy(payload), accession)
        self.assertEqual(
            {
                "available": True,
                "accepted": False,
                "error": "Published insider filing contract is invalid",
            },
            result,
        )

    def test_rejects_filing_detail_private_values_and_semantic_drift(self) -> None:
        def mutate_owner_group_reconciliation(payload: dict[str, object]) -> None:
            owner_group = cast(dict[str, object], payload["ownerGroup"])
            owner_group["displayName"] = "SYNTHETIC OWNER ALPHA ALTERED"
            transaction = cast(list[dict[str, object]], payload["transactions"])[0]
            transaction["ownerGroup"] = copy.deepcopy(owner_group)

        mutations = {
            "owner CIK in rendered transaction label": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("transactionLabel", "Owner CIK 0000000002"),
            "address in rendered transaction label": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("transactionLabel", "PRIVATE STREET 100 Main Street"),
            "private path in rendered transaction label": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("transactionLabel", "/private/source.xml"),
            "private correlator in normalized security": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("normalizedSecurityId", "a" * 64),
            "private path in rendered issuer name": lambda payload: cast(
                dict[str, object], payload["issuer"]
            ).__setitem__("nameAsFiled", "/private/source.xml"),
            "private path in issuer symbol": lambda payload: cast(
                dict[str, object], payload["issuer"]
            ).__setitem__("tradingSymbolAsFiled", "/private/source.xml"),
            "invalid filing date": lambda payload: cast(
                dict[str, object], payload["filing"]
            ).__setitem__("filingDate", "2026-02-30"),
            "transaction filing identity drift": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("filingDate", "2026-01-15"),
            "transaction classification drift": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("transactionLabel", "Sale"),
            "noncanonical transaction decimal": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("shares", "0123.450"),
            "transaction value reconciliation drift": lambda payload: cast(
                list[dict[str, object]], payload["transactions"]
            )[0].__setitem__("value", "1"),
            "owner group does not reconcile to owners": mutate_owner_group_reconciliation,
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                payload = copy.deepcopy(self.live_filing)
                mutate(payload)
                result = self._validate_filing(payload, self.live_accession)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider filing contract is invalid",
                    result["error"],
                )

    def test_sec_url_binding_matches_exact_publication_contract(self) -> None:
        source = cast(dict[str, object], self.live_filing["source"])
        issuer = cast(dict[str, object], self.live_filing["issuer"])
        exact = str(source["documentUrl"])
        prefix, filename = exact.rsplit("/", 1)
        cases = {
            "exact": exact,
            "explicit default port": exact.replace("www.sec.gov", "www.sec.gov:443"),
            "uppercase scheme": exact.replace("https://", "HTTPS://"),
            "uppercase host": exact.replace("www.sec.gov", "WWW.SEC.GOV"),
            "mixed-case scheme and host": exact.replace(
                "https://www.sec.gov", "Https://WwW.SeC.Gov"
            ),
            "apex host": exact.replace("www.sec.gov", "sec.gov"),
            "SEC subdomain": exact.replace("www.sec.gov", "archives.sec.gov"),
            "nondefault port": exact.replace("www.sec.gov", "www.sec.gov:444"),
            "query": exact + "?download=1",
            "fragment": exact + "#private-fragment",
            "encoded dot traversal": f"{prefix}/%2e%2e/%2e%2e/2/x.xml",
            "encoded slash": f"{prefix}/%2F..%2F..%2F2%2Fx.xml",
            "encoded backslash": f"{prefix}/%5C..%5C2%5Cx.xml",
            "encoded filename": f"{prefix}/%66{filename[1:]}",
            "literal dot segment": f"{prefix}/./{filename}",
            "double slash": f"{prefix}//{filename}",
        }
        result = self._run_node(
            """
            const fs = require("node:fs");
            const input = JSON.parse(fs.readFileSync(0, "utf8"));
            const available = typeof liveInsiderSecUrlIsBound === "function";
            const decisions = available
              ? Object.fromEntries(Object.entries(input.cases).map(([key, value]) => [
                  key,
                  liveInsiderSecUrlIsBound(value, input.issuerCik, input.accession),
                ]))
              : {};
            console.log(JSON.stringify({ available, decisions }));
            """,
            {
                "cases": cases,
                "issuerCik": issuer["cik"],
                "accession": self.live_accession,
            },
        )
        self.assertEqual(
            {
                "available": True,
                "decisions": {
                    "exact": True,
                    "explicit default port": True,
                    "uppercase scheme": False,
                    "uppercase host": False,
                    "mixed-case scheme and host": False,
                    "apex host": False,
                    "SEC subdomain": False,
                    "nondefault port": False,
                    "query": False,
                    "fragment": False,
                    "encoded dot traversal": False,
                    "encoded slash": False,
                    "encoded backslash": False,
                    "encoded filename": False,
                    "literal dot segment": False,
                    "double slash": False,
                },
            },
            result,
        )

    def test_fetches_exact_digest_bound_filing_detail(self) -> None:
        result = self._fetch_filing(
            copy.deepcopy(self.live_payload),
            self.live_accession,
            self.live_filing_bytes,
        )
        self.assertEqual(True, result["available"])
        self.assertEqual(self.live_accession, result["acceptedAccession"])
        self.assertIsNone(result["error"])
        self.assertEqual(
            [
                {
                    "path": (
                        f"data/insiders/public/filings/{self.live_accession}.json"
                    ),
                    "options": {
                        "cache": "no-store",
                        "credentials": "same-origin",
                    },
                }
            ],
            result["fetchCalls"],
        )

    def test_filing_fetch_fails_closed_on_size_digest_or_reference_drift(
        self,
    ) -> None:
        same_size_wrong_digest = b"[" + self.live_filing_bytes[1:]
        cases = {
            "wrong byte count": (
                self.live_accession,
                self.live_filing_bytes + b" ",
                1,
            ),
            "wrong digest": (
                self.live_accession,
                same_size_wrong_digest,
                1,
            ),
            "unreferenced accession": (
                "0000000001-26-999999",
                self.live_filing_bytes,
                0,
            ),
        }
        for label, (accession, encoded, expected_calls) in cases.items():
            with self.subTest(label=label):
                result = self._fetch_filing(
                    copy.deepcopy(self.live_payload),
                    accession,
                    encoded,
                )
                self.assertTrue(result["available"])
                self.assertIsNone(result["acceptedAccession"])
                self.assertEqual(
                    "Published insider filing could not be verified",
                    result["error"],
                )
                self.assertEqual(expected_calls, len(result["fetchCalls"]))

    def test_filing_path_requires_current_validated_security_reference(self) -> None:
        result = self._run_node(
            """
            const fs = require("node:fs");
            const input = JSON.parse(fs.readFileSync(0, "utf8"));
            const available =
              typeof insiderFilingPathForAccession === "function";
            const cases = available ? {
              exact: insiderFilingPathForAccession(
                input.payload,
                input.accession
              ),
              unknown: insiderFilingPathForAccession(
                input.payload,
                "0000000001-26-999999"
              ),
              malformed: insiderFilingPathForAccession(
                input.payload,
                "../../private"
              ),
            } : {};
            console.log(JSON.stringify({ available, cases }));
            """,
            {"payload": self.live_payload, "accession": self.live_accession},
        )
        self.assertEqual(
            {
                "available": True,
                "cases": {
                    "exact": (
                        f"data/insiders/public/filings/{self.live_accession}.json"
                    ),
                    "unknown": None,
                    "malformed": None,
                },
            },
            result,
        )

    def test_rejects_contract_identity_and_filing_reference_drift(self) -> None:
        mutations = {
            "site contract": lambda payload: payload.__setitem__(
                "data_contract_version", 4
            ),
            "insider contract": lambda payload: payload.__setitem__(
                "insider_public_contract_version", 2
            ),
            "payload type": lambda payload: payload.__setitem__(
                "payloadType", "insider_filing_detail"
            ),
            "security identity": lambda payload: payload["security"].__setitem__(
                "id", "99999X999"
            ),
            "security stem": lambda payload: payload["security"].__setitem__(
                "fileStem", "99999X999"
            ),
            "filing accession": lambda payload: payload["filingRefs"][0].__setitem__(
                "accessionNumber", "not-an-accession"
            ),
            "filing path": lambda payload: payload["filingRefs"][0].__setitem__(
                "path", "filings/../../private.json"
            ),
            "filing digest": lambda payload: payload["filingRefs"][0].__setitem__(
                "sha256", "A" * 64
            ),
            "filing byte bound": lambda payload: payload["filingRefs"][0].__setitem__(
                "bytes", 1_000_001
            ),
            "duplicate filing": lambda payload: payload["filingRefs"].append(
                copy.deepcopy(payload["filingRefs"][0])
            ),
            "filing private field": lambda payload: payload["filingRefs"][
                0
            ].__setitem__("privatePath", "/private/source.xml"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                payload = copy.deepcopy(self.live_payload)
                mutate(payload)
                result = self._validate(payload, STOCK_ID)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider activity contract is invalid",
                    result["error"],
                )

    def test_rejects_security_transaction_classification_drift(self) -> None:
        payload = cast(dict[str, object], copy.deepcopy(self.live_payload))
        transactions = cast(dict[str, object], payload["transactions"])
        items = cast(list[dict[str, object]], transactions["items"])
        transaction = items[0]
        transaction["transactionLabel"] = "Sale"
        transaction["normalizedCategory"] = "sale"
        chart_events = cast(list[dict[str, object]], payload["chartEvents"])
        chart_events[0]["category"] = "sale"
        summary = cast(dict[str, object], payload["summary"])
        summary["latestMeaningfulTransaction"] = copy.deepcopy(transaction)

        result = self._validate(payload, STOCK_ID)

        self.assertTrue(result["available"])
        self.assertFalse(result["accepted"])
        self.assertEqual(
            "Published insider activity contract is invalid",
            result["error"],
        )

    def test_rejects_noncanonical_or_impossible_public_timestamps(self) -> None:
        security_cases = {
            "rolled calendar date": "2026-02-30T20:45:00Z",
            "non-UTC offset": "2026-06-30T20:45:00+00:00",
            "missing seconds": "2026-06-30T20:45Z",
            "excess fractional precision": "2026-06-30T20:45:00.1234567Z",
        }
        for label, timestamp in security_cases.items():
            with self.subTest(surface="security", label=label):
                payload = copy.deepcopy(self.live_payload)
                payload["asOf"] = timestamp
                result = self._validate(payload, STOCK_ID)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider activity contract is invalid",
                    result["error"],
                )

        for label, timestamp in security_cases.items():
            with self.subTest(surface="filing", label=label):
                payload = cast(dict[str, object], copy.deepcopy(self.live_filing))
                filing = cast(dict[str, object], payload["filing"])
                filing["acceptedAt"] = timestamp
                transactions = cast(list[dict[str, object]], payload["transactions"])
                holdings = cast(list[dict[str, object]], payload["holdings"])
                for row in [*transactions, *holdings]:
                    row["acceptedAt"] = timestamp
                result = self._validate_filing(payload, self.live_accession)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider filing contract is invalid",
                    result["error"],
                )

    def test_rejects_private_or_correlating_public_symbols(self) -> None:
        unsafe_values = (
            "file:///private/source",
            "https://intranet.example",
            "intranet.example",
            "0000000002",
            "a" * 64,
        )
        for unsafe_value in unsafe_values:
            with self.subTest(surface="security ticker", value=unsafe_value):
                payload = cast(dict[str, object], copy.deepcopy(self.live_payload))
                security = cast(dict[str, object], payload["security"])
                security["ticker"] = unsafe_value
                result = self._validate(payload, STOCK_ID)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider activity contract is invalid",
                    result["error"],
                )

            with self.subTest(surface="filing issuer", value=unsafe_value):
                payload = cast(dict[str, object], copy.deepcopy(self.live_filing))
                issuer = cast(dict[str, object], payload["issuer"])
                issuer["tradingSymbolAsFiled"] = unsafe_value
                result = self._validate_filing(payload, self.live_accession)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider filing contract is invalid",
                    result["error"],
                )

    def test_digest_bound_filing_must_match_validated_security_issuer(self) -> None:
        detail = cast(dict[str, object], copy.deepcopy(self.live_filing))
        issuer = cast(dict[str, object], detail["issuer"])
        issuer["cik"] = "0000000002"
        source = cast(dict[str, object], detail["source"])
        source["indexUrl"] = str(source["indexUrl"]).replace(
            "/Archives/edgar/data/1/", "/Archives/edgar/data/2/"
        )
        source["documentUrl"] = str(source["documentUrl"]).replace(
            "/Archives/edgar/data/1/", "/Archives/edgar/data/2/"
        )
        transactions = cast(list[dict[str, object]], detail["transactions"])
        holdings = cast(list[dict[str, object]], detail["holdings"])
        for row in [*transactions, *holdings]:
            if row.get("secDocumentUrl") is not None:
                row["secDocumentUrl"] = str(row["secDocumentUrl"]).replace(
                    "/Archives/edgar/data/1/", "/Archives/edgar/data/2/"
                )

        encoded = canonical_public_json_bytes(detail)
        security_payload = cast(dict[str, object], copy.deepcopy(self.live_payload))
        filing_refs = cast(list[dict[str, object]], security_payload["filingRefs"])
        filing_refs[0]["bytes"] = len(encoded)
        filing_refs[0]["sha256"] = hashlib.sha256(encoded).hexdigest()

        result = self._fetch_filing(
            security_payload,
            self.live_accession,
            encoded,
        )

        self.assertTrue(result["available"])
        self.assertIsNone(result["acceptedAccession"])
        self.assertEqual(
            "Published insider filing could not be verified",
            result["error"],
        )

    def test_rejects_security_page_private_and_nested_shape_drift(self) -> None:
        mutations = {
            "private top-level field": lambda payload: payload.__setitem__(
                "privateProvenance", "/private/source.xml"
            ),
            "security private field": lambda payload: payload["security"].__setitem__(
                "ownerAddress", "PRIVATE STREET"
            ),
            "security path in company name": lambda payload: payload[
                "security"
            ].__setitem__("companyName", "/private/var/insiders.json"),
            "filters private field": lambda payload: payload["filters"].__setitem__(
                "ownerAddress", "PRIVATE STREET"
            ),
            "freshness private field": lambda payload: payload[
                "dataFreshness"
            ].__setitem__("sourcePath", "/private/var/insiders.json"),
            "quality private field": lambda payload: payload["dataQuality"].__setitem__(
                "parserVersion", "private-parser-v1"
            ),
            "transaction private field": lambda payload: payload["transactions"][
                "items"
            ][0].__setitem__("privateOwnerId", "a" * 64),
            "transaction owner correlator": lambda payload: payload["transactions"][
                "items"
            ][0]["ownerGroup"].__setitem__("key", "a" * 64),
            "transaction unsafe owner name": lambda payload: payload["transactions"][
                "items"
            ][0]["ownerGroup"].__setitem__("displayName", "a" * 64),
            "transaction SEC subdomain": lambda payload: payload["transactions"][
                "items"
            ][0].__setitem__(
                "secDocumentUrl",
                payload["transactions"]["items"][0]["secDocumentUrl"].replace(
                    "www.sec.gov", "archives.sec.gov"
                ),
            ),
            "transaction SEC query": lambda payload: payload["transactions"]["items"][
                0
            ].__setitem__(
                "secDocumentUrl",
                payload["transactions"]["items"][0]["secDocumentUrl"] + "?download=1",
            ),
            "purchase summary private field": lambda payload: payload["summary"][
                "purchases"
            ].__setitem__("ownerAddress", "PRIVATE STREET"),
            "latest summary private field": lambda payload: payload["summary"][
                "latestMeaningfulTransaction"
            ].__setitem__("privateRowKey", "private-row"),
            "chart event private field": lambda payload: payload["chartEvents"][
                0
            ].__setitem__("ownerGroupKey", "a" * 64),
            "sidebar private field": lambda payload: payload["sidebar"].__setitem__(
                "ownerDirectory", ["PRIVATE OWNER"]
            ),
            "ranking correlator": lambda payload: payload["sidebar"]["topBuyers"][
                0
            ].__setitem__("ownerGroupKey", "f" * 64),
            "holdings private field": lambda payload: payload["sidebar"][
                "latestReportedHoldings"
            ].__setitem__("sourcePath", "/private/var/insiders.json"),
            "rule private field": lambda payload: payload["sidebar"][
                "rule10b51"
            ].__setitem__("remarks", "PRIVATE NARRATIVE"),
            "pagination private field": lambda payload: payload[
                "staticPagination"
            ].__setitem__("sourcePath", "/private/var/insiders.json"),
            "methodology private field": lambda payload: payload[
                "methodologyBanner"
            ].__setitem__("privateSource", "private"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                payload = copy.deepcopy(self.live_payload)
                mutate(payload)
                result = self._validate(payload, STOCK_ID)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider activity contract is invalid",
                    result["error"],
                )

    def test_rejects_filing_contract_identity_and_private_shape_drift(self) -> None:
        mutations = {
            "site contract": lambda payload: payload.__setitem__(
                "data_contract_version", 4
            ),
            "insider contract": lambda payload: payload.__setitem__(
                "insider_public_contract_version", 2
            ),
            "payload type": lambda payload: payload.__setitem__(
                "payloadType", "security_insider_activity"
            ),
            "accession identity": lambda payload: payload.__setitem__(
                "accessionNumber", "0000000001-26-999999"
            ),
            "private top-level field": lambda payload: payload.__setitem__(
                "privateProvenance", "/private/source.xml"
            ),
            "filing narrative": lambda payload: payload["filing"].__setitem__(
                "footnote", "private narrative"
            ),
            "issuer private field": lambda payload: payload["issuer"].__setitem__(
                "ownerAddress", "PRIVATE STREET"
            ),
            "owner-group correlator": lambda payload: payload["ownerGroup"].__setitem__(
                "key", "a" * 64
            ),
            "owner-group unsafe display name": lambda payload: payload[
                "ownerGroup"
            ].__setitem__("displayName", "a" * 64),
            "owner-group unsafe title": lambda payload: payload[
                "ownerGroup"
            ].__setitem__("primaryTitle", "100 Main Street"),
            "owner CIK": lambda payload: payload["owners"][0].__setitem__(
                "cik", "0000000002"
            ),
            "owner unsafe name": lambda payload: payload["owners"][0].__setitem__(
                "nameAsFiled", "a" * 64
            ),
            "owner unsafe title": lambda payload: payload["owners"][0].__setitem__(
                "companyTitle", "100 Main Street"
            ),
            "owner roles shape": lambda payload: payload["owners"][0].__setitem__(
                "roles", "Director"
            ),
            "transaction accession": lambda payload: payload["transactions"][
                0
            ].__setitem__("accessionNumber", "0000000001-26-999999"),
            "transaction private field": lambda payload: payload["transactions"][
                0
            ].__setitem__("privateOwnerId", "a" * 64),
            "holdings shape": lambda payload: payload.__setitem__("holdings", {}),
            "source private field": lambda payload: payload["source"].__setitem__(
                "rawPath", "/private/source.xml"
            ),
            "source origin": lambda payload: payload["source"].__setitem__(
                "documentUrl", "https://evil.example/ownership.xml"
            ),
            "source SEC apex host": lambda payload: payload["source"].__setitem__(
                "documentUrl",
                payload["source"]["documentUrl"].replace("www.sec.gov", "sec.gov"),
            ),
            "source SEC fragment": lambda payload: payload["source"].__setitem__(
                "indexUrl", payload["source"]["indexUrl"] + "#private-fragment"
            ),
            "safeguard disabled": lambda payload: payload[
                "publicationSafeguards"
            ].__setitem__("ownerCiksOmitted", False),
            "safeguard extra field": lambda payload: payload[
                "publicationSafeguards"
            ].__setitem__("privateOwnerKeyOmitted", True),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                payload = copy.deepcopy(self.live_filing)
                mutate(payload)
                result = self._validate_filing(payload, self.live_accession)
                self.assertTrue(result["available"])
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    "Published insider filing contract is invalid",
                    result["error"],
                )

    def test_live_view_does_not_create_a_private_owner_group_key(self) -> None:
        self.assertNotIn("ownerGroupKey:", self.html)

    def test_methodology_dialog_is_truthful_for_live_and_fixture_modes(self) -> None:
        start = self.html.index("function openInsiderMethodology()")
        end = self.html.index("// ---------- STOCK: home ----------", start)
        function_source = self.html[start:end]
        script = (
            """
const fs = require("node:fs");
const input = JSON.parse(fs.readFileSync(0, "utf8"));
let currentInsiderPayloadMode = input.mode;
let insiderDrawerReturnFocus = null;
const focusTarget = { focus() {} };
const host = {
  innerHTML: "",
  querySelector() { return focusTarget; },
};
const document = { activeElement: null, body: host };
const $ = () => host;
const queueMicrotask = callback => callback();
"""
            + function_source
            + "\nopenInsiderMethodology();\n"
            + "console.log(JSON.stringify({ html: host.innerHTML }));\n"
        )

        rendered: dict[str, str] = {}
        for mode in ("live", "fixture"):
            completed = subprocess.run(
                ["node", "-e", script],
                cwd=ROOT,
                check=True,
                capture_output=True,
                input=json.dumps({"mode": mode}),
                text=True,
            )
            rendered[mode] = json.loads(completed.stdout)["html"]

        live = rendered["live"]
        self.assertIn("Validated public filing data", live)
        self.assertIn("Forms 3, 4, and 5", live)
        self.assertIn("transaction-only", live)
        self.assertIn(
            "name as filed and company relationship/title",
            live,
        )
        self.assertNotIn("Phase 1 fixture preview", live)
        self.assertNotIn("illustrative", live)
        self.assertNotIn("APGE", live)

        fixture = rendered["fixture"]
        self.assertIn("Phase 1 fixture preview", fixture)
        self.assertIn("Fixture limitation", fixture)
        self.assertIn("illustrative", fixture)
        self.assertIn("APGE filing evidence", fixture)

    def test_phase5_docs_record_live_adapter_and_price_boundary(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        architecture = (ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")
        plan = (ROOT / "IMPLEMENTATION_PLAN.md").read_text(encoding="utf-8")

        self.assertNotIn("production browser remains fixture-backed", readme)
        self.assertNotIn(
            "production insider UI remains fixture-backed until Phase 5",
            architecture,
        )
        self.assertNotRegex(
            architecture,
            r"activating\s+the\s+visible\s+live\s+insider\s+views\s+remains\s+Phase\s+5",
        )
        self.assertIn("same-origin", readme)
        self.assertIn("transaction-only", readme)
        self.assertIn("100 rows", readme)
        self.assertIn("digest", architecture)
        self.assertIn("manual, default-off", architecture)
        self.assertIn(
            "### Phase 5 - completed as-built history: live integration and hardening",
            plan,
        )
        self.assertIn("No daily market-price provider is approved", plan)
        self.assertIn("no browser-to-provider calls", plan)
        stale_discovery_language = (
            "the future UI may",
            "As-built history and future plan mapped",
            "update architecture prose only in a later authorized phase",
            "The insider logical model must initially become",
            "The insider model additionally needs issuer CIK",
            "should be the immutable public identifier",
            "should be added for insider ownership identity",
            "The implementation must therefore pause",
            "should ship rather than fabricated",
            "Missing infrastructure includes ownership-form discovery",
            "insider work must add that private immutable source store",
            "UI replacement, prices, and any future authorization to automate public materialization remain Phase 5",
            "No dependency should be added until its phase is approved",
        )
        for stale_text in stale_discovery_language:
            with self.subTest(stale_text=stale_text):
                self.assertNotIn(stale_text, plan)


if __name__ == "__main__":
    unittest.main()
