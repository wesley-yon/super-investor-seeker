from __future__ import annotations

import copy
from contextlib import nullcontext, redirect_stderr, redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

from data_contract import DATA_CONTRACT_VERSION
from insider_storage import (
    InsiderStateStore,
    canonical_insider_state_json_bytes,
    issuer_generation_digest,
)
from insider_publication_policy import publication_policy_sha256
from security_identity import section16_security_class_key

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "plan_insider_publication_policy.py"
SERVICENOW_CIK = "0001373715"
CLASS_TITLE = "COMMON STOCK"
CLASS_KEY = section16_security_class_key(
    SERVICENOW_CIK,
    CLASS_TITLE,
    is_derivative=False,
)
STOCK_ID = "81762P102"


def planner_module():
    spec = importlib.util.spec_from_file_location(
        "plan_insider_publication_policy",
        SCRIPT,
    )
    if spec is None or spec.loader is None:
        raise AssertionError("private policy planner script is not importable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def issuer_state_fixture() -> dict[str, object]:
    accessions = [
        {
            "accession_number": "0001373715-26-000001",
            "parser_version": "test-v1",
            "normalized_sha256": hashlib.sha256(
                b"canonical planner test filing"
            ).hexdigest(),
        }
    ]
    return {
        "contract_version": 1,
        "issuer_cik": SERVICENOW_CIK,
        "accessions": accessions,
        "owner_groups": [],
        "security_classes": [
            {
                "security_class_key": CLASS_KEY,
                "derivative": False,
                "title": CLASS_TITLE,
            }
        ],
        "amendments": [],
        "unresolved_ambiguities": [],
        "generation_digest": issuer_generation_digest(
            [
                {
                    **accession,
                    "amendment_resolution": None,
                }
                for accession in accessions
            ]
        ),
    }


def legacy_issuer_state_fixture() -> dict[str, object]:
    accessions = [
        {
            "accession_number": "0001373715-26-000001",
            "parser_version": "test-v1",
            "normalized_sha256": "0" * 64,
        },
        {
            "accession_number": "0001373715-26-000002",
            "parser_version": "test-v1",
            "normalized_sha256": "1" * 64,
        },
    ]
    amendment = {
        "accession_number": accessions[1]["accession_number"],
        "effective_accession": accessions[0]["accession_number"],
        "confidence": "high",
        "reason_code": "single_candidate",
        "candidates": [accessions[0]["accession_number"]],
    }
    generation_material = [
        {
            **accession,
            "amendment_resolution": (
                {
                    "effective_accession": accessions[0]["accession_number"],
                    "confidence": "high",
                    "reason_code": "single_candidate",
                    "candidates": [accessions[0]["accession_number"]],
                }
                if accession["accession_number"] == accessions[1]["accession_number"]
                else None
            ),
        }
        for accession in accessions
    ]
    return {
        "contract_version": 1,
        "issuer_cik": SERVICENOW_CIK,
        "accessions": accessions,
        "owner_groups": [],
        "security_classes": [
            {
                "security_class_key": CLASS_KEY,
                "derivative": False,
                "title": CLASS_TITLE,
            }
        ],
        "amendments": [amendment],
        "unresolved_ambiguities": [],
        "generation_digest": hashlib.sha256(
            b"section16-issuer-generation-v1\0"
            + canonical_insider_state_json_bytes(generation_material)
        ).hexdigest(),
    }


def public_metadata_fixture() -> dict[str, object]:
    return {
        "stockId": STOCK_ID,
        "fileStem": STOCK_ID,
        "ticker": "SYN",
        "companyName": "Synthetic ServiceNow",
        "securityType": "Common Stock",
        "securityTypeLabel": "COMMON STOCK",
        "cusip": STOCK_ID,
        "primary": True,
    }


def split_holder_fixtures() -> list[dict[str, object]]:
    holders: list[dict[str, object]] = []
    for cik in range(25, 0, -1):
        prior_shares = 100 + cik
        current_shares = prior_shares * 10
        holders.append(
            {
                "cik": cik,
                "name": f"Fund {cik:02d}",
                "history": [
                    {
                        "date": "2025-12-31",
                        "shares": current_shares,
                        "value": current_shares * 10,
                        "pct_of_fund": 1.0,
                    },
                    {
                        "date": "2025-09-30",
                        "shares": prior_shares,
                        "value": prior_shares * 100,
                        "pct_of_fund": 1.0,
                    },
                ],
            }
        )
    return holders


def split_adjustment_fixture() -> dict[str, object]:
    return {
        "from_report_date": "2025-09-30",
        "to_report_date": "2025-12-31",
        "factor": 10,
        "proven": True,
        "support": 25,
        "observations": 25,
    }


def _write_json(path: Path, payload: object, *, mode: int = 0o644) -> None:
    path.write_text(
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(mode)


def seed_planner_repository(base: Path) -> tuple[Path, Path, dict[str, object]]:
    root = base / "repository"
    root.mkdir()
    state = InsiderStateStore(root)
    state.write(
        "approved-issuers-v1",
        {
            "contract_version": 1,
            "issuer_ciks": [SERVICENOW_CIK],
        },
    )
    issuer_state = issuer_state_fixture()
    state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)

    data = root / "data"
    stocks = data / "stocks"
    stocks.mkdir(parents=True, exist_ok=True)
    ticker_row = {
        "stock_id": STOCK_ID,
        "cusip": STOCK_ID,
        "ticker": "SYN",
        "issuer": "Synthetic ServiceNow",
        "instrument_type": "EQUITY",
        "holder_count": 1,
        "current_holder_count": 1,
        "last_seen": "2026-06-30",
    }
    _write_json(
        data / "index.json",
        {
            "data_contract_version": DATA_CONTRACT_VERSION,
            "fund_data_revision": "0" * 64,
            "funds": [
                {
                    "cik": 1,
                    "name": "Fund 01",
                    "q": [20262],
                }
            ],
            "last_updated": "2026-06-30T00:00:00Z",
            "proven_split_adjustments": {},
            "tickers": [ticker_row],
            "total_filers": 1,
            "total_tickers": 1,
        },
    )
    _write_json(
        stocks / f"{STOCK_ID}.json",
        {
            "stock_id": STOCK_ID,
            "cusip": STOCK_ID,
            "ticker": "SYN",
            "issuer": "Synthetic ServiceNow",
            "instrument_type": "EQUITY",
            "holders": [
                {
                    "cik": 1,
                    "name": "Fund 01",
                    "history": [
                        {
                            "date": "2026-06-30",
                            "shares": 100,
                            "value": 1000,
                            "pct_of_fund": 1.0,
                        }
                    ],
                }
            ],
            "split_adjustments": [],
        },
    )

    review = base / "review"
    review.mkdir(mode=0o700)
    review.chmod(0o700)
    _write_json(
        review / "mapping.json",
        {CLASS_KEY: public_metadata_fixture()},
        mode=0o600,
    )
    return root, review, issuer_state


class PlanInsiderPublicationPolicyTests(unittest.TestCase):
    def test_plans_complete_candidate_from_canonical_state_and_restored_public_metadata(
        self,
    ) -> None:
        self.assertTrue(SCRIPT.is_file(), "private policy planner script is missing")
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, issuer_state = seed_planner_repository(Path(tmpdir))
            result = module.plan_servicenow_publication_policy(
                repository_root=root,
                issuer_cik=SERVICENOW_CIK,
                review_directory=review,
                mapping_spec_name="mapping.json",
                output_name="candidate.json",
            )

            candidate = {
                "contract_version": 1,
                "issuers": [
                    {
                        "issuer_cik": SERVICENOW_CIK,
                        "security_mappings": {
                            CLASS_KEY: public_metadata_fixture(),
                        },
                    }
                ],
            }
            candidate_path = review / "candidate.json"
            self.assertEqual(
                canonical_insider_state_json_bytes(candidate),
                candidate_path.read_bytes(),
            )
            self.assertEqual(0o600, candidate_path.stat().st_mode & 0o777)
            self.assertEqual(os.geteuid(), candidate_path.stat().st_uid)
            self.assertEqual(1, candidate_path.stat().st_nlink)
            self.assertEqual(
                {
                    "candidate_policy_sha256": publication_policy_sha256(candidate),
                    "issuer_cik": SERVICENOW_CIK,
                    "issuer_generation_digest": issuer_state["generation_digest"],
                    "security_class_count": 1,
                },
                result,
            )

    def test_requires_all_explicit_planner_arguments(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            arguments = [
                "--repository-root",
                str(root),
                "--issuer-cik",
                SERVICENOW_CIK,
                "--review-directory",
                str(review),
                "--mapping-spec",
                "mapping.json",
                "--output",
                "candidate.json",
            ]
            for index in range(0, len(arguments), 2):
                missing = arguments[:index] + arguments[index + 2 :]
                stdout = io.StringIO()
                stderr = io.StringIO()
                with (
                    self.subTest(missing=arguments[index]),
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                ):
                    self.assertEqual(2, module.main(missing))
                self.assertEqual("", stdout.getvalue())
                self.assertEqual(
                    "private insider policy planning configuration is invalid\n",
                    stderr.getvalue(),
                )
                self.assertFalse((review / "candidate.json").exists())

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                self.assertEqual(0, module.main(arguments))
            self.assertEqual("", stderr.getvalue())
            self.assertEqual(
                {
                    "candidate_policy_sha256",
                    "issuer_cik",
                    "issuer_generation_digest",
                    "security_class_count",
                },
                set(json.loads(stdout.getvalue())),
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                self.assertEqual(2, module.main(["--issuer-cik", 1373715]))
            self.assertEqual("", stdout.getvalue())
            self.assertEqual(
                "private insider policy planning configuration is invalid\n",
                stderr.getvalue(),
            )

    def test_rejects_noncanonical_or_foreign_issuer_before_planning(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            for issuer_cik in ("1373715", "0001373714", "0000000000", ""):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with (
                    self.subTest(issuer_cik=issuer_cik),
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                ):
                    result = module.main(
                        [
                            "--repository-root",
                            str(root),
                            "--issuer-cik",
                            issuer_cik,
                            "--review-directory",
                            str(review),
                            "--mapping-spec",
                            "mapping.json",
                            "--output",
                            "candidate.json",
                        ]
                    )
                self.assertEqual(2, result)
                self.assertEqual("", stdout.getvalue())
                self.assertEqual(
                    "private insider policy planning configuration is invalid\n",
                    stderr.getvalue(),
                )
                self.assertFalse((review / "candidate.json").exists())

    def test_requires_real_owner_only_review_directory(self) -> None:
        module = planner_module()
        for case in ("mode", "symlink", "owner"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                review_argument = review
                context = mock.patch.object(
                    module.os,
                    "geteuid",
                    return_value=review.stat().st_uid,
                )
                if case == "mode":
                    review.chmod(0o750)
                elif case == "symlink":
                    review_argument = review.parent / "review-link"
                    review_argument.symlink_to(review, target_is_directory=True)
                else:
                    context = mock.patch.object(
                        module.os,
                        "geteuid",
                        return_value=review.stat().st_uid + 1,
                    )
                with (
                    context,
                    self.assertRaises(module.InsiderPublicationPolicyPlanningError),
                ):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review_argument,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_rejects_absolute_or_traversing_mapping_and_output_names(self) -> None:
        module = planner_module()
        for field, unsafe_kind in (
            ("mapping", "absolute"),
            ("mapping", "traversal"),
            ("output", "absolute"),
            ("output", "traversal"),
        ):
            with (
                self.subTest(field=field, unsafe_kind=unsafe_kind),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                base = Path(tmpdir)
                root, review, _ = seed_planner_repository(base)
                mapping_name = "mapping.json"
                output_name = "candidate.json"
                escaped_output: Path | None = None
                if field == "mapping":
                    if unsafe_kind == "absolute":
                        mapping_name = str(review / "mapping.json")
                    else:
                        outside = base / "outside-mapping.json"
                        outside.write_bytes((review / "mapping.json").read_bytes())
                        outside.chmod(0o600)
                        mapping_name = "../outside-mapping.json"
                elif unsafe_kind == "absolute":
                    escaped_output = base / "absolute-candidate.json"
                    output_name = str(escaped_output)
                else:
                    escaped_output = base / "escaped-candidate.json"
                    output_name = "../escaped-candidate.json"
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name=mapping_name,
                        output_name=output_name,
                    )
                self.assertFalse((review / "candidate.json").exists())
                if escaped_output is not None:
                    self.assertFalse(escaped_output.exists())

    def test_rejects_noncanonical_review_filenames_before_io(self) -> None:
        module = planner_module()
        for field, name in (
            ("mapping", " mapping.json"),
            ("mapping", "mapping\n.json"),
            ("mapping", "mapping\u200b.json"),
            ("mapping", "mapping\u212a.json"),
            ("output", " candidate.json"),
            ("output", "candidate\n.json"),
            ("output", "candidate\u200b.json"),
            ("output", "candidate\u212a.json"),
        ):
            with (
                self.subTest(field=field, name=name),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                root, review, _ = seed_planner_repository(Path(tmpdir))
                mapping_name = "mapping.json"
                output_name = "candidate.json"
                if field == "mapping":
                    (review / mapping_name).rename(review / name)
                    mapping_name = name
                else:
                    output_name = name
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name=mapping_name,
                        output_name=output_name,
                    )
                self.assertFalse((review / output_name).exists())

    def test_requires_owner_only_single_link_regular_mapping_file(self) -> None:
        module = planner_module()
        for case in ("mode", "symlink", "hardlink"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                base = Path(tmpdir)
                root, review, _ = seed_planner_repository(base)
                mapping = review / "mapping.json"
                original = mapping.read_bytes()
                if case == "mode":
                    mapping.chmod(0o640)
                else:
                    mapping.unlink()
                    source = base / "mapping-source.json"
                    source.write_bytes(original)
                    source.chmod(0o600)
                    if case == "symlink":
                        mapping.symlink_to(source)
                    else:
                        os.link(source, mapping)
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_rejects_duplicate_keys_and_oversize_mapping_json(self) -> None:
        module = planner_module()
        metadata = json.dumps(
            public_metadata_fixture(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        duplicate = f'{{"{CLASS_KEY}":{metadata},"{CLASS_KEY}":{metadata}}}\n'.encode()
        for case in ("duplicate", "oversize"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                mapping = review / "mapping.json"
                if case == "duplicate":
                    mapping.write_bytes(duplicate)
                else:
                    rendered = mapping.read_bytes()
                    mapping.write_bytes(rendered + b" " * (1_000_001 - len(rendered)))
                mapping.chmod(0o600)
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_restored_adapter_rejects_empty_or_oversize_mapping_before_io(
        self,
    ) -> None:
        module = planner_module()
        for case in ("empty", "oversize"):
            with self.subTest(case=case):
                mapping_spec = (
                    {}
                    if case == "empty"
                    else {
                        f"class-{index}": {"stockId": STOCK_ID}
                        for index in range(module.MAX_INSIDER_STATE_COLLECTION + 1)
                    }
                )
                with (
                    mock.patch.object(
                        module,
                        "_open_directory",
                        side_effect=AssertionError("public adapter performed I/O"),
                    ),
                    self.assertRaises(module.InsiderPublicationPolicyPlanningError),
                ):
                    module._open_restored_public_index(
                        Path("/synthetic/untrusted-repository"),
                        mapping_spec,
                    )

    def test_public_index_requires_exact_rows_counts_dates_and_identity(self) -> None:
        module = planner_module()
        for case in (
            "extra-root-key",
            "wrong-contract-version",
            "noninteger-contract-version",
            "wrong-count",
            "noncanonical-date",
            "extra-row-key",
            "boolean-count",
            "noncanonical-stock-id",
            "noncanonical-unselected-cusip",
            "untrimmed-unselected-ticker",
            "untrimmed-unselected-issuer",
            "unsafe-unselected-ticker",
            "unsafe-unselected-issuer",
        ):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                index_path = root / "data/index.json"
                index = json.loads(index_path.read_bytes())
                if case == "extra-root-key":
                    index["unexpected"] = None
                elif case == "wrong-contract-version":
                    index["data_contract_version"] = DATA_CONTRACT_VERSION - 1
                elif case == "noninteger-contract-version":
                    index["data_contract_version"] = float(DATA_CONTRACT_VERSION)
                elif case == "wrong-count":
                    index["total_tickers"] = 2
                elif case == "noncanonical-date":
                    index["last_updated"] = "2026-6-30"
                elif case == "extra-row-key":
                    index["tickers"][0]["unexpected"] = None
                elif case == "boolean-count":
                    index["tickers"][0]["holder_count"] = True
                elif case == "noncanonical-stock-id":
                    index["tickers"][0]["stock_id"] = STOCK_ID.lower()
                else:
                    unselected = {
                        "stock_id": "ABC123456",
                        "cusip": "ABC123456",
                        "ticker": "AAA",
                        "issuer": "Unselected Corporation",
                        "instrument_type": "EQUITY",
                        "holder_count": 0,
                        "current_holder_count": 0,
                        "last_seen": "2026-06-30",
                    }
                    if case == "noncanonical-unselected-cusip":
                        unselected["cusip"] = "abc123456"
                    elif case == "untrimmed-unselected-ticker":
                        unselected["ticker"] = "AAA "
                    elif case == "untrimmed-unselected-issuer":
                        unselected["issuer"] = "Unselected Corporation "
                    elif case == "unsafe-unselected-ticker":
                        unselected["ticker"] = "AAA\u200b"
                    else:
                        unselected["issuer"] = "Unselected\u200b Corporation"
                    index["tickers"].insert(0, unselected)
                    index["total_tickers"] = 2
                _write_json(index_path, index)
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_selected_public_stock_must_exist_once_and_match_index(self) -> None:
        module = planner_module()
        for case in ("missing", "duplicate", "disagreement"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                index_path = root / "data/index.json"
                index = json.loads(index_path.read_bytes())
                if case == "missing":
                    index["tickers"] = []
                    index["total_tickers"] = 0
                    _write_json(index_path, index)
                elif case == "duplicate":
                    index["tickers"].append(dict(index["tickers"][0]))
                    index["total_tickers"] = 2
                    _write_json(index_path, index)
                else:
                    stock_path = root / "data/stocks" / f"{STOCK_ID}.json"
                    stock = json.loads(stock_path.read_bytes())
                    stock["ticker"] = "OTHER"
                    _write_json(stock_path, stock)
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_selected_stock_metadata_reconciles_with_holders_and_fund_calendars(
        self,
    ) -> None:
        module = planner_module()
        for case in ("holder-count", "current-holder-count", "last-seen"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                index_path = root / "data/index.json"
                index = json.loads(index_path.read_bytes())
                ticker = index["tickers"][0]
                if case == "holder-count":
                    ticker["holder_count"] = 2
                elif case == "current-holder-count":
                    ticker["current_holder_count"] = 0
                else:
                    ticker["last_seen"] = "2025-12-31"
                _write_json(index_path, index)

                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_rejects_malformed_parsed_public_artifact_sections(self) -> None:
        module = planner_module()
        for case in (
            "non-object-fund",
            "malformed-global-split-proof",
            "non-object-holder",
            "non-object-history",
            "mismatched-stock-split-proof",
        ):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                index_path = root / "data/index.json"
                stock_path = root / "data/stocks" / f"{STOCK_ID}.json"
                index = json.loads(index_path.read_bytes())
                stock = json.loads(stock_path.read_bytes())

                if case == "non-object-fund":
                    index["funds"] = [True]
                    index["total_filers"] = 1
                elif case == "malformed-global-split-proof":
                    index["proven_split_adjustments"] = {"bad": True}
                elif case == "non-object-holder":
                    stock["holders"] = [True]
                elif case == "non-object-history":
                    stock["holders"] = [
                        {
                            "cik": 1,
                            "name": "Fund 01",
                            "history": [True],
                        }
                    ]
                else:
                    holders = split_holder_fixtures()
                    index["funds"] = [
                        {
                            "cik": cik,
                            "name": f"Fund {cik:02d}",
                            "q": [20254, 20253],
                        }
                        for cik in range(1, 26)
                    ]
                    index["total_filers"] = 25
                    index["tickers"][0].update(
                        {
                            "holder_count": 25,
                            "current_holder_count": 25,
                            "last_seen": "2025-12-31",
                        }
                    )
                    index["proven_split_adjustments"] = {
                        STOCK_ID: [split_adjustment_fixture()]
                    }
                    stock["holders"] = holders
                    stock["split_adjustments"] = []

                _write_json(index_path, index)
                _write_json(stock_path, stock)
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_accepts_fully_validated_split_proof_artifacts(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            index_path = root / "data/index.json"
            stock_path = root / "data/stocks" / f"{STOCK_ID}.json"
            index = json.loads(index_path.read_bytes())
            stock = json.loads(stock_path.read_bytes())
            adjustment = split_adjustment_fixture()
            index["funds"] = [
                {
                    "cik": cik,
                    "name": f"Fund {cik:02d}",
                    "q": [20254, 20253],
                }
                for cik in range(1, 26)
            ]
            index["total_filers"] = 25
            index["tickers"][0].update(
                {
                    "holder_count": 25,
                    "current_holder_count": 25,
                    "last_seen": "2025-12-31",
                }
            )
            index["proven_split_adjustments"] = {STOCK_ID: [adjustment]}
            stock["holders"] = split_holder_fixtures()
            stock["split_adjustments"] = [adjustment]
            _write_json(index_path, index)
            _write_json(stock_path, stock)

            result = module.plan_servicenow_publication_policy(
                repository_root=root,
                issuer_cik=SERVICENOW_CIK,
                review_directory=review,
                mapping_spec_name="mapping.json",
                output_name="candidate.json",
            )
            self.assertEqual(1, result["security_class_count"])
            self.assertTrue((review / "candidate.json").is_file())

    def test_parsed_public_section_validators_reject_contract_drift(self) -> None:
        module = planner_module()
        funds = [
            {
                "cik": cik,
                "name": f"Fund {cik:02d}",
                "q": [20254, 20253],
            }
            for cik in range(1, 26)
        ]
        holders = split_holder_fixtures()
        adjustment = split_adjustment_fixture()

        fund_cases: list[tuple[str, object]] = []
        mutated_funds = copy.deepcopy(funds)
        mutated_funds[0]["unexpected"] = None
        fund_cases.append(("unknown-fund-key", mutated_funds))
        mutated_funds = copy.deepcopy(funds)
        mutated_funds[0]["cik"] = True
        fund_cases.append(("boolean-fund-cik", mutated_funds))
        mutated_funds = copy.deepcopy(funds)
        mutated_funds[1]["cik"] = 1
        fund_cases.append(("duplicate-fund-cik", mutated_funds))
        mutated_funds = copy.deepcopy(funds)
        mutated_funds[0]["q"] = [20253, 20254]
        fund_cases.append(("unsorted-fund-calendar", mutated_funds))
        mutated_funds = copy.deepcopy(funds)
        mutated_funds[0]["unverified_report_dates"] = None
        fund_cases.append(("null-unverified-dates", mutated_funds))
        mutated_funds = copy.deepcopy(funds)
        mutated_funds[0]["status"] = "WITHHELD"
        fund_cases.append(("partial-withheld-row", mutated_funds))
        for label, payload in fund_cases:
            with (
                self.subTest(section="funds", case=label),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module._validate_public_funds(payload)

        split_cases: list[tuple[str, object]] = []
        mutated_adjustment = copy.deepcopy(adjustment)
        mutated_adjustment["unexpected"] = None
        split_cases.append(("unknown-adjustment-key", [mutated_adjustment]))
        mutated_adjustment = copy.deepcopy(adjustment)
        mutated_adjustment["to_report_date"] = "2026-06-30"
        split_cases.append(("nonconsecutive-adjustment", [mutated_adjustment]))
        mutated_adjustment = copy.deepcopy(adjustment)
        mutated_adjustment["factor"] = 7
        split_cases.append(("unsupported-factor", [mutated_adjustment]))
        mutated_adjustment = copy.deepcopy(adjustment)
        mutated_adjustment["proven"] = False
        split_cases.append(("unproven-adjustment", [mutated_adjustment]))
        mutated_adjustment = copy.deepcopy(adjustment)
        mutated_adjustment["support"] = True
        split_cases.append(("boolean-support", [mutated_adjustment]))
        for label, payload in split_cases:
            with (
                self.subTest(section="split-proof", case=label),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module._validate_split_adjustments(
                    payload,
                    allow_empty=False,
                    label="public index",
                )
        for label, payload in (
            ("noncanonical-global-stock-id", {"abc123456": [adjustment]}),
            ("empty-global-proof", {STOCK_ID: []}),
            (
                "unsorted-global-stock-ids",
                {STOCK_ID: [adjustment], "594918104": [adjustment]},
            ),
        ):
            with (
                self.subTest(section="global-split-proof", case=label),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module._validate_global_split_adjustments(payload)

        validated_funds = module._validate_public_funds(funds)
        holder_cases: list[tuple[str, object]] = []
        mutated_holders = copy.deepcopy(holders)
        mutated_holders[0]["unexpected"] = None
        holder_cases.append(("unknown-holder-key", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_holders[0]["cik"] = 999
        holder_cases.append(("unknown-holder-cik", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_holders[0]["name"] = "Wrong Fund"
        holder_cases.append(("holder-name-mismatch", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_holders[0]["history"] = []
        holder_cases.append(("empty-history", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_history = mutated_holders[0]["history"]
        assert isinstance(mutated_history, list)
        assert isinstance(mutated_history[0], dict)
        mutated_history[0]["unexpected"] = None
        holder_cases.append(("unknown-history-key", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_history = mutated_holders[0]["history"]
        assert isinstance(mutated_history, list)
        assert isinstance(mutated_history[0], dict)
        mutated_history[0]["shares"] = True
        holder_cases.append(("boolean-position-value", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_history = mutated_holders[0]["history"]
        assert isinstance(mutated_history, list)
        assert isinstance(mutated_history[0], dict)
        mutated_history[0]["shares_imputed"] = False
        holder_cases.append(("false-imputation-marker", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_history = mutated_holders[0]["history"]
        assert isinstance(mutated_history, list)
        mutated_history.reverse()
        holder_cases.append(("unsorted-history", mutated_holders))
        mutated_holders = copy.deepcopy(holders)
        mutated_holders.reverse()
        holder_cases.append(("unsorted-holders", mutated_holders))
        for label, payload in holder_cases:
            with (
                self.subTest(section="holders", case=label),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module._validate_public_holders(payload, validated_funds)

    def test_local_split_inference_matches_generation_contract(self) -> None:
        module = planner_module()
        from pipeline import infer_proven_split_adjustments

        holders = split_holder_fixtures()
        funds = module._validate_public_funds(
            [
                {
                    "cik": cik,
                    "name": f"Fund {cik:02d}",
                    "q": [20254, 20253],
                }
                for cik in range(1, 26)
            ]
        )
        validated_holders = module._validate_public_holders(holders, funds)
        self.assertEqual(
            infer_proven_split_adjustments(holders),
            module._infer_proven_split_adjustments(validated_holders),
        )

    def test_public_inputs_are_bounded_real_regular_files(self) -> None:
        module = planner_module()
        for target, case in (
            ("index", "symlink"),
            ("stock", "symlink"),
            ("index", "oversize"),
            ("stock", "oversize"),
        ):
            with (
                self.subTest(target=target, case=case),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                base = Path(tmpdir)
                root, review, _ = seed_planner_repository(base)
                path = (
                    root / "data/index.json"
                    if target == "index"
                    else root / "data/stocks" / f"{STOCK_ID}.json"
                )
                original = path.read_bytes()
                if case == "symlink":
                    path.unlink()
                    source = base / f"{target}-source.json"
                    source.write_bytes(original)
                    source.chmod(0o644)
                    path.symlink_to(source)
                    context = nullcontext()
                else:
                    limit_name = (
                        "MAX_PUBLIC_INDEX_BYTES"
                        if target == "index"
                        else "MAX_PUBLIC_STOCK_BYTES"
                    )
                    context = mock.patch.object(
                        module,
                        limit_name,
                        len(original) - 1,
                    )
                with (
                    context,
                    self.assertRaises(module.InsiderPublicationPolicyPlanningError),
                ):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_rejects_missing_malformed_or_legacy_issuer_state(self) -> None:
        module = planner_module()
        for case in ("missing", "malformed", "legacy"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                issuer_path = (
                    root
                    / "data/insiders/private/state/issuers"
                    / f"{SERVICENOW_CIK}.json"
                )
                if case == "missing":
                    issuer_path.unlink()
                elif case == "malformed":
                    issuer_path.write_bytes(b"{}\n")
                    issuer_path.chmod(0o600)
                else:
                    issuer_path.write_bytes(
                        canonical_insider_state_json_bytes(
                            legacy_issuer_state_fixture()
                        )
                    )
                    issuer_path.chmod(0o600)
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_candidate_fsyncs_file_then_directory_and_retains_ambiguous_output(
        self,
    ) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            original_fsync = module.os.fsync
            calls: list[str] = []

            def record_fsync(descriptor: int) -> None:
                kind = (
                    "directory"
                    if stat.S_ISDIR(module.os.fstat(descriptor).st_mode)
                    else "file"
                )
                calls.append(kind)
                original_fsync(descriptor)

            with mock.patch.object(module.os, "fsync", side_effect=record_fsync):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertEqual(["file", "directory"], calls)

        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            original_fsync = module.os.fsync

            def fail_directory_fsync(descriptor: int) -> None:
                if stat.S_ISDIR(module.os.fstat(descriptor).st_mode):
                    raise OSError("synthetic directory fsync ambiguity")
                original_fsync(descriptor)

            with (
                mock.patch.object(
                    module.os,
                    "fsync",
                    side_effect=fail_directory_fsync,
                ),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            candidate = review / "candidate.json"
            self.assertTrue(candidate.is_file())
            self.assertEqual(0o600, candidate.stat().st_mode & 0o777)
            self.assertEqual(1, candidate.stat().st_nlink)

    def test_rejects_candidate_content_permission_or_hardlink_race_after_file_fsync(
        self,
    ) -> None:
        module = planner_module()
        for case in ("content", "mode", "hardlink"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                candidate = review / "candidate.json"
                hardlink = review / "candidate-hardlink.json"
                original_fsync = module.os.fsync
                mutated = False

                def mutate_after_file_fsync(descriptor: int) -> None:
                    nonlocal mutated
                    original_fsync(descriptor)
                    if mutated or not stat.S_ISREG(module.os.fstat(descriptor).st_mode):
                        return
                    mutated = True
                    if case == "content":
                        candidate.write_bytes(b"tampered after file fsync\n")
                    elif case == "mode":
                        candidate.chmod(0o640)
                    else:
                        os.link(candidate, hardlink)

                with (
                    mock.patch.object(
                        module.os,
                        "fsync",
                        side_effect=mutate_after_file_fsync,
                    ),
                    self.assertRaises(module.InsiderPublicationPolicyPlanningError),
                ):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )

                self.assertTrue(mutated)
                self.assertTrue(candidate.is_file())
                if case == "content":
                    self.assertEqual(
                        b"tampered after file fsync\n",
                        candidate.read_bytes(),
                    )
                    self.assertFalse(hardlink.exists())
                elif case == "mode":
                    self.assertEqual(0o640, candidate.stat().st_mode & 0o777)
                    self.assertFalse(hardlink.exists())
                else:
                    self.assertTrue(hardlink.is_file())
                    self.assertEqual(2, candidate.stat().st_nlink)
                    self.assertEqual(candidate.stat().st_ino, hardlink.stat().st_ino)

    def test_existing_output_symlink_is_never_followed_or_replaced(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            root, review, _ = seed_planner_repository(base)
            outside = base / "outside.json"
            outside.write_bytes(b"synthetic outside sentinel\n")
            candidate = review / "candidate.json"
            candidate.symlink_to(outside)
            with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertTrue(candidate.is_symlink())
            self.assertEqual(b"synthetic outside sentinel\n", outside.read_bytes())

    def test_rejects_generation_or_security_class_drift_before_output(self) -> None:
        module = planner_module()
        original_builder = module.build_servicenow_publication_policy
        for case in ("generation", "security-classes"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                issuer_path = (
                    root
                    / "data/insiders/private/state/issuers"
                    / f"{SERVICENOW_CIK}.json"
                )
                changed = issuer_state_fixture()
                if case == "generation":
                    accessions = changed["accessions"]
                    assert isinstance(accessions, list)
                    accessions.append(
                        {
                            "accession_number": "0001373715-26-000002",
                            "parser_version": "test-v1",
                            "normalized_sha256": hashlib.sha256(
                                b"planner generation drift"
                            ).hexdigest(),
                        }
                    )
                    changed["generation_digest"] = issuer_generation_digest(
                        [
                            {**accession, "amendment_resolution": None}
                            for accession in accessions
                        ]
                    )
                else:
                    classes = changed["security_classes"]
                    assert isinstance(classes, list)
                    classes.append(
                        {
                            "security_class_key": section16_security_class_key(
                                SERVICENOW_CIK,
                                "PREFERRED STOCK",
                                is_derivative=False,
                            ),
                            "derivative": False,
                            "title": "PREFERRED STOCK",
                        }
                    )
                    classes.sort(key=lambda row: str(row["security_class_key"]))

                def build_then_change(**kwargs):
                    candidate = original_builder(**kwargs)
                    issuer_path.write_bytes(canonical_insider_state_json_bytes(changed))
                    issuer_path.chmod(0o600)
                    return candidate

                with (
                    mock.patch.object(
                        module,
                        "build_servicenow_publication_policy",
                        side_effect=build_then_change,
                    ),
                    self.assertRaises(module.InsiderPublicationPolicyPlanningError),
                ):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_rejects_mapping_or_public_anchor_drift_before_output(self) -> None:
        module = planner_module()
        original_builder = module.build_servicenow_publication_policy
        for target in ("mapping", "index", "stock"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                path = {
                    "mapping": review / "mapping.json",
                    "index": root / "data/index.json",
                    "stock": root / "data/stocks" / f"{STOCK_ID}.json",
                }[target]

                def build_then_change(**kwargs):
                    candidate = original_builder(**kwargs)
                    path.write_bytes(path.read_bytes() + b" ")
                    return candidate

                with (
                    mock.patch.object(
                        module,
                        "build_servicenow_publication_policy",
                        side_effect=build_then_change,
                    ),
                    self.assertRaises(module.InsiderPublicationPolicyPlanningError),
                ):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_rejects_review_directory_rebinding_before_output(self) -> None:
        module = planner_module()
        original_builder = module.build_servicenow_publication_policy
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            root, review, _ = seed_planner_repository(base)
            moved_review = base / "review-moved"

            def build_then_rebind(**kwargs):
                candidate = original_builder(**kwargs)
                review.rename(moved_review)
                review.mkdir(mode=0o700)
                review.chmod(0o700)
                return candidate

            with (
                mock.patch.object(
                    module,
                    "build_servicenow_publication_policy",
                    side_effect=build_then_rebind,
                ),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertFalse((review / "candidate.json").exists())
            self.assertFalse((moved_review / "candidate.json").exists())

    def test_rejects_late_review_rebinding_and_retains_candidate(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            root, review, _ = seed_planner_repository(base)
            moved_review = base / "review-moved"
            original_fsync = module.os.fsync
            rebound = False

            def rebind_after_directory_fsync(descriptor: int) -> None:
                nonlocal rebound
                original_fsync(descriptor)
                if not rebound and stat.S_ISDIR(module.os.fstat(descriptor).st_mode):
                    review.rename(moved_review)
                    review.mkdir(mode=0o700)
                    review.chmod(0o700)
                    rebound = True

            with (
                mock.patch.object(
                    module.os,
                    "fsync",
                    side_effect=rebind_after_directory_fsync,
                ),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertFalse((review / "candidate.json").exists())
            self.assertTrue((moved_review / "candidate.json").is_file())

    def test_rejects_late_review_permission_drift_and_retains_candidate(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            original_fsync = module.os.fsync
            changed = False

            def relax_after_directory_fsync(descriptor: int) -> None:
                nonlocal changed
                original_fsync(descriptor)
                if not changed and stat.S_ISDIR(module.os.fstat(descriptor).st_mode):
                    review.chmod(0o750)
                    changed = True

            with (
                mock.patch.object(
                    module.os,
                    "fsync",
                    side_effect=relax_after_directory_fsync,
                ),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertTrue((review / "candidate.json").is_file())

    def test_rejects_late_source_drift_and_retains_candidate(self) -> None:
        module = planner_module()
        for target in ("mapping", "index", "stock"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                path = {
                    "mapping": review / "mapping.json",
                    "index": root / "data/index.json",
                    "stock": root / "data/stocks" / f"{STOCK_ID}.json",
                }[target]
                original_fsync = module.os.fsync
                changed = False

                def change_after_directory_fsync(descriptor: int) -> None:
                    nonlocal changed
                    original_fsync(descriptor)
                    if not changed and stat.S_ISDIR(
                        module.os.fstat(descriptor).st_mode
                    ):
                        path.write_bytes(path.read_bytes() + b" ")
                        changed = True

                with (
                    mock.patch.object(
                        module.os,
                        "fsync",
                        side_effect=change_after_directory_fsync,
                    ),
                    self.assertRaises(module.InsiderPublicationPolicyPlanningError),
                ):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertTrue((review / "candidate.json").is_file())

    def test_rejects_late_candidate_drift_and_retains_output(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            candidate = review / "candidate.json"
            original_fsync = module.os.fsync
            changed = False

            def change_after_directory_fsync(descriptor: int) -> None:
                nonlocal changed
                original_fsync(descriptor)
                if not changed and stat.S_ISDIR(module.os.fstat(descriptor).st_mode):
                    candidate.write_bytes(candidate.read_bytes() + b" ")
                    changed = True

            with (
                mock.patch.object(
                    module.os,
                    "fsync",
                    side_effect=change_after_directory_fsync,
                ),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertTrue(candidate.is_file())
            self.assertTrue(candidate.read_bytes().endswith(b" "))

    def test_rejects_repository_rebinding_during_final_issuer_read(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            root, review, _ = seed_planner_repository(base)
            moved_root = base / "repository-moved"
            original_read = module._read_canonical_issuer_state
            read_count = 0

            def read_then_rebind(state_store):
                nonlocal read_count
                state = original_read(state_store)
                read_count += 1
                if read_count == 3:
                    root.rename(moved_root)
                    root.mkdir()
                return state

            with (
                mock.patch.object(
                    module,
                    "_read_canonical_issuer_state",
                    side_effect=read_then_rebind,
                ),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertTrue((review / "candidate.json").is_file())

    def test_rejects_late_issuer_drift_and_retains_candidate(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            issuer_path = (
                root / "data/insiders/private/state/issuers" / f"{SERVICENOW_CIK}.json"
            )
            changed = issuer_state_fixture()
            accessions = changed["accessions"]
            assert isinstance(accessions, list)
            accessions.append(
                {
                    "accession_number": "0001373715-26-000002",
                    "parser_version": "test-v1",
                    "normalized_sha256": hashlib.sha256(
                        b"late planner generation drift"
                    ).hexdigest(),
                }
            )
            changed["generation_digest"] = issuer_generation_digest(
                [
                    {**accession, "amendment_resolution": None}
                    for accession in accessions
                ]
            )
            original_fsync = module.os.fsync
            changed_on_disk = False

            def change_after_directory_fsync(descriptor: int) -> None:
                nonlocal changed_on_disk
                original_fsync(descriptor)
                if not changed_on_disk and stat.S_ISDIR(
                    module.os.fstat(descriptor).st_mode
                ):
                    issuer_path.write_bytes(canonical_insider_state_json_bytes(changed))
                    issuer_path.chmod(0o600)
                    changed_on_disk = True

            with (
                mock.patch.object(
                    module.os,
                    "fsync",
                    side_effect=change_after_directory_fsync,
                ),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertTrue((review / "candidate.json").is_file())

    def test_nonregular_public_or_mapping_inputs_fail_without_blocking(self) -> None:
        module = planner_module()
        for target in ("mapping", "index", "stock"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as tmpdir:
                root, review, _ = seed_planner_repository(Path(tmpdir))
                path = {
                    "mapping": review / "mapping.json",
                    "index": root / "data/index.json",
                    "stock": root / "data/stocks" / f"{STOCK_ID}.json",
                }[target]
                path.unlink()
                os.mkfifo(path, 0o600 if target == "mapping" else 0o644)
                with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name="candidate.json",
                    )
                self.assertFalse((review / "candidate.json").exists())

    def test_candidate_is_bounded_and_existing_regular_output_is_preserved(
        self,
    ) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            with (
                mock.patch.object(module, "MAX_POLICY_CANDIDATE_BYTES", 1),
                self.assertRaises(module.InsiderPublicationPolicyPlanningError),
            ):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertFalse((review / "candidate.json").exists())

        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, _ = seed_planner_repository(Path(tmpdir))
            candidate = review / "candidate.json"
            sentinel = b"existing owner-reviewed candidate\n"
            candidate.write_bytes(sentinel)
            candidate.chmod(0o600)
            with self.assertRaises(module.InsiderPublicationPolicyPlanningError):
                module.plan_servicenow_publication_policy(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    review_directory=review,
                    mapping_spec_name="mapping.json",
                    output_name="candidate.json",
                )
            self.assertEqual(sentinel, candidate.read_bytes())

    def test_restored_adapter_accepts_equity_without_split_adjustments(self) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, _, _ = seed_planner_repository(Path(tmpdir))
            stock_path = root / "data/stocks" / f"{STOCK_ID}.json"
            stock = json.loads(stock_path.read_bytes())
            del stock["split_adjustments"]
            _write_json(stock_path, stock)

            snapshot = module._open_restored_public_index(
                root,
                {CLASS_KEY: public_metadata_fixture()},
            )
            try:
                self.assertEqual(
                    STOCK_ID,
                    snapshot.public_index[STOCK_ID]["stockId"],
                )
                snapshot.validate()
            finally:
                snapshot.close()

    def test_restored_adapter_accepts_exact_non_equity_stock_schema(self) -> None:
        module = planner_module()
        option_stock_id = f"{STOCK_ID}|CALL"
        with tempfile.TemporaryDirectory() as tmpdir:
            root, _, _ = seed_planner_repository(Path(tmpdir))
            base_stock = json.loads(
                (root / "data/stocks" / f"{STOCK_ID}.json").read_bytes()
            )
            holders = base_stock["holders"]
            assert isinstance(holders, list)
            index_path = root / "data/index.json"
            index = json.loads(index_path.read_bytes())
            index["tickers"].append(
                {
                    "stock_id": option_stock_id,
                    "cusip": STOCK_ID,
                    "ticker": "SYN",
                    "issuer": "Synthetic ServiceNow",
                    "instrument_type": "CALL",
                    "holder_count": 1,
                    "current_holder_count": 1,
                    "last_seen": "2026-06-30",
                }
            )
            index["total_tickers"] = 2
            _write_json(index_path, index)
            _write_json(
                root / "data/stocks" / f"{STOCK_ID}__CALL.json",
                {
                    "stock_id": option_stock_id,
                    "cusip": STOCK_ID,
                    "ticker": "SYN",
                    "issuer": "Synthetic ServiceNow",
                    "instrument_type": "CALL",
                    "holders": copy.deepcopy(holders),
                },
            )
            mapping = public_metadata_fixture()
            mapping.update(
                {
                    "stockId": option_stock_id,
                    "fileStem": f"{STOCK_ID}__CALL",
                    "securityType": "Call Option",
                    "securityTypeLabel": "CALL OPTION",
                }
            )
            snapshot = module._open_restored_public_index(root, {CLASS_KEY: mapping})
            try:
                self.assertEqual(
                    option_stock_id, snapshot.public_index[option_stock_id]["stockId"]
                )
                snapshot.validate()
            finally:
                snapshot.close()

    def test_restored_adapter_projects_public_and_reviewed_fields_separately(
        self,
    ) -> None:
        module = planner_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root, _, _ = seed_planner_repository(Path(tmpdir))
            mapping = public_metadata_fixture()
            mapping.update(
                {
                    "fileStem": "reviewed-but-untrusted-stem",
                    "ticker": "REVIEWED-BUT-UNTRUSTED-TICKER",
                    "companyName": "Reviewed but untrusted issuer",
                    "cusip": "REVIEWED-BUT-UNTRUSTED-CUSIP",
                    "primary": False,
                }
            )
            snapshot = module._open_restored_public_index(
                root,
                {CLASS_KEY: mapping},
            )
            try:
                self.assertEqual(
                    {
                        "stockId": STOCK_ID,
                        "fileStem": STOCK_ID,
                        "ticker": "SYN",
                        "companyName": "Synthetic ServiceNow",
                        "securityType": "Common Stock",
                        "securityTypeLabel": "COMMON STOCK",
                        "cusip": STOCK_ID,
                        "primary": False,
                    },
                    snapshot.public_index[STOCK_ID],
                )
                snapshot.validate()
            finally:
                snapshot.close()

    def test_candidate_is_deterministic_across_mapping_input_order(self) -> None:
        module = planner_module()
        second_title = "PREFERRED STOCK"
        second_class_key = section16_security_class_key(
            SERVICENOW_CIK,
            second_title,
            is_derivative=False,
        )
        second_stock_id = "594918104"
        second_metadata = {
            "stockId": second_stock_id,
            "fileStem": second_stock_id,
            "ticker": "SYP",
            "companyName": "Synthetic Preferred ServiceNow",
            "securityType": "Preferred Stock",
            "securityTypeLabel": second_title,
            "cusip": second_stock_id,
            "primary": False,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root, review, state = seed_planner_repository(Path(tmpdir))
            classes = state["security_classes"]
            assert isinstance(classes, list)
            classes.append(
                {
                    "security_class_key": second_class_key,
                    "derivative": False,
                    "title": second_title,
                }
            )
            classes.sort(key=lambda row: str(row["security_class_key"]))
            issuer_path = (
                root / "data/insiders/private/state/issuers" / f"{SERVICENOW_CIK}.json"
            )
            issuer_path.write_bytes(canonical_insider_state_json_bytes(state))
            issuer_path.chmod(0o600)

            base_stock = json.loads(
                (root / "data/stocks" / f"{STOCK_ID}.json").read_bytes()
            )
            holders = base_stock["holders"]
            assert isinstance(holders, list)
            index_path = root / "data/index.json"
            index = json.loads(index_path.read_bytes())
            index["tickers"].append(
                {
                    "stock_id": second_stock_id,
                    "cusip": second_stock_id,
                    "ticker": "SYP",
                    "issuer": "Synthetic Preferred ServiceNow",
                    "instrument_type": "EQUITY",
                    "holder_count": 1,
                    "current_holder_count": 1,
                    "last_seen": "2026-06-30",
                }
            )
            index["total_tickers"] = 2
            _write_json(index_path, index)
            _write_json(
                root / "data/stocks" / f"{second_stock_id}.json",
                {
                    "stock_id": second_stock_id,
                    "cusip": second_stock_id,
                    "ticker": "SYP",
                    "issuer": "Synthetic Preferred ServiceNow",
                    "instrument_type": "EQUITY",
                    "holders": copy.deepcopy(holders),
                    "split_adjustments": [],
                },
            )

            mapping_path = review / "mapping.json"
            mappings = {
                CLASS_KEY: public_metadata_fixture(),
                second_class_key: second_metadata,
            }
            results: list[dict[str, object]] = []
            candidates: list[bytes] = []
            for index_number, order in enumerate(
                (
                    (CLASS_KEY, second_class_key),
                    (second_class_key, CLASS_KEY),
                ),
                start=1,
            ):
                mapping_path.write_text(
                    json.dumps(
                        {class_key: mappings[class_key] for class_key in order},
                        allow_nan=False,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=False,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                mapping_path.chmod(0o600)
                output_name = f"candidate-{index_number}.json"
                results.append(
                    module.plan_servicenow_publication_policy(
                        repository_root=root,
                        issuer_cik=SERVICENOW_CIK,
                        review_directory=review,
                        mapping_spec_name="mapping.json",
                        output_name=output_name,
                    )
                )
                candidates.append((review / output_name).read_bytes())

            self.assertEqual(results[0], results[1])
            self.assertEqual(2, results[0]["security_class_count"])
            self.assertEqual(candidates[0], candidates[1])


if __name__ == "__main__":
    unittest.main()
