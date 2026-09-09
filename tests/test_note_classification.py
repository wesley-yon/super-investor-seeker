from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import note_classification as n
import pipeline
from reviewed_ticker_map import public_display_mappings
from scripts.repair_note_classifications import repair_fund, repair_directory, quarter_hash


def row(cusip="921937827", kind="NOTE", cls="SHORT TRM BOND", **extra):
    return {"cusip": cusip, "holding_type": kind, "class": cls,
            "reported_cusip": cusip, "reported_class": cls,
            "reported_issuer": "VANGUARD BD INDEX FDS", "reported_figi": None,
            "accession": "0000000001-26-000001", "report_date": "2026-06-30",
            "put_call": None, "shares": 25, "value": 1000, **extra}


def fund():
    q = {"report_date": "2026-06-30", "total_value": 3000, "num_holdings": 3,
         "composition_version": 2, "composition_hash_version": 3,
         "security_identity_version": 1, "base_accession": "0000000001-26-000001",
         "applied_accessions": ["0000000001-26-000001"],
         "source_filings": [{"accession": "0000000001-26-000001", "source_hash": "a" * 64}],
         "holdings": [row(), row("012653200", cls="7.25% DEP SHS A"),
                      row("26210CAD6", cls="NOTE 0.500% 2/0")]}
    q["composition_hash"] = quarter_hash(q)
    return {"cik": 1, "name": "Test Manager", "quarters": [q]}


class NoteClassificationTests(unittest.TestCase):
    def test_review_has_exact_expected_population_and_primary_proofs(self):
        review = n.classification_review()
        self.assertEqual(59, len(review))
        self.assertEqual(57, sum(x["to_type"] == "EQUITY" for x in review.values()))
        self.assertEqual({"012653200", "49446R687"}, {c for c, r in review.items() if r["to_type"] == "PREF"})
        for entry in review.values():
            self.assertEqual("NOTE", entry["from_type"])
            self.assertTrue(any(p["url"].startswith("https://www.sec.gov/") for p in entry["proofs"]))
            self.assertTrue(any(p["url"].startswith("https://www.nasdaqtrader.com/") for p in entry["proofs"]))

    def test_parser_corrects_verified_ibonds_and_preferred_coupon(self):
        for holding, target in [(row(), "EQUITY"), (row("46438G372", cls="IBONDS DEC 2035"), "EQUITY"),
                                (row("012653200", cls="7.25% DEP SHS A"), "PREF"),
                                (row("49446R687", cls="DP CV CL N 7.25%"), "PREF")]:
            with self.subTest(cusip=holding["cusip"]):
                self.assertEqual("NOTE", pipeline._classify_holding_unreviewed(holding))
                self.assertEqual(target, pipeline._classify_holding(holding))
                self.assertEqual(target, pipeline.classify_saved_holding(holding))

    def test_real_debt_and_unapproved_identifiers_do_not_change(self):
        for holding in [row("26210CAD6", cls="NOTE 0.500% 2/0"), row("921937828"),
                        row("744320888", cls="4.125% JUNIOR SUBORDINATED NOTES")]:
            self.assertEqual("NOTE", pipeline._classify_holding(holding))
            self.assertIsNone(n.reviewed_note_type(holding, "NOTE"))
        self.assertIsNone(n.reviewed_note_type(row(reported_cusip="921937828"), "NOTE"))

    def test_sided_generic_and_legacy_options_are_preserved(self):
        for side in ("CALL", "PUT"):
            self.assertEqual(side, pipeline._classify_holding(row(put_call=side)))
            self.assertEqual(side, pipeline._classify_holding(row(cls=side)))
            self.assertEqual(side, pipeline.classify_saved_holding(row(kind=side)))
        self.assertEqual("OPT", pipeline._classify_holding(row(cls="ETF OPTION")))

    def test_migration_preserves_original_fields_and_records_prior_hash(self):
        before = fund()
        frozen = deepcopy(before)
        after, report = repair_fund(before)
        self.assertEqual(frozen, before)
        self.assertEqual(2, report["rows"])
        old, new = before["quarters"][0], after["quarters"][0]
        self.assertEqual(["EQUITY", "PREF", "NOTE"], [h["holding_type"] for h in new["holdings"]])
        self.assertEqual(old["composition_hash"], new["note_classification_repair"]["prior_composition_hash"])
        self.assertEqual(quarter_hash(new), new["composition_hash"])
        self.assertNotEqual(old["composition_hash"], new["composition_hash"])
        for a, b in zip(old["holdings"], new["holdings"]):
            self.assertEqual({k: v for k, v in a.items() if k != "holding_type"},
                             {k: v for k, v in b.items() if k != "holding_type"})
        self.assertEqual(old["source_filings"], new["source_filings"])
        self.assertEqual(old["total_value"], new["total_value"])
        again, report = repair_fund(after)
        self.assertEqual(after, again)
        self.assertEqual(0, report["rows"])

    def test_migration_rejects_damaged_composition_without_mutation(self):
        before = fund()
        before["quarters"][0]["holdings"][0]["shares"] += 1
        frozen = deepcopy(before)
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            repair_fund(before)
        self.assertEqual(frozen, before)

    def test_dry_run_writes_no_funds_and_one_bad_file_blocks_batch(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "funds"
            path.mkdir()
            original = json.dumps(fund()).encode()
            (path / "1.json").write_bytes(original)
            report = repair_directory(path, apply=False, workers=1)
            self.assertEqual(2, report["rows"])
            self.assertEqual(original, (path / "1.json").read_bytes())
            (path / "2.json").write_text("invalid json")
            with self.assertRaises(ValueError):
                repair_directory(path, apply=True, workers=1)
            self.assertEqual(original, (path / "1.json").read_bytes())

    def test_parallel_and_serial_repairs_are_identical(self):
        with tempfile.TemporaryDirectory() as temp:
            paths = [Path(temp) / "serial", Path(temp) / "parallel"]
            for path in paths:
                path.mkdir()
                for i in range(3):
                    (path / f"{i}.json").write_text(json.dumps(fund()))
            one = repair_directory(paths[0], apply=True, workers=1)
            two = repair_directory(paths[1], apply=True, workers=2)
            self.assertEqual(one, two)
            for i in range(3):
                self.assertEqual((paths[0] / f"{i}.json").read_bytes(), (paths[1] / f"{i}.json").read_bytes())

    def test_display_bridge_is_exact_and_does_not_change_review_input(self):
        display = {"ticker": "BSV", "match_kind": "exact_cusip", "confidence_tier": "A"}
        review = {"display_mappings": {"921937827|NOTE": display}}
        result = public_display_mappings("921937827", review)
        self.assertEqual(display, result["EQUITY"])
        self.assertNotIn("PREF", result)
        self.assertNotIn("921937827|EQUITY", review["display_mappings"])
        self.assertEqual({}, public_display_mappings("921937828", review))
        review["display_mappings"]["921937827|NOTE"]["ticker"] = "OTHER"
        with self.assertRaisesRegex(ValueError, "display conflict"):
            public_display_mappings("921937827", review)

    def test_checksum_is_enforced(self):
        n.classification_review.cache_clear()
        try:
            with patch.object(Path, "read_bytes", return_value=b"{}"):
                with self.assertRaisesRegex(ValueError, "checksum"):
                    n.classification_review()
        finally:
            n.classification_review.cache_clear()

    def test_completion_marker_survives_ordinary_state_saves(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / 'state.json'
            with patch.object(pipeline, 'STATE_PATH', target):
                pipeline.save_state({'note_classification_review_sha256': n.REVIEW_SHA256})
            self.assertEqual(n.REVIEW_SHA256, json.loads(target.read_text())['note_classification_review_sha256'])

    def test_pending_mode_rebuilds_before_marking_and_then_skips(self):
        import sys
        from scripts import repair_note_classifications as script
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            report = path / 'report.json'
            state = {}
            def rebuild(**kwargs):
                self.assertNotIn('note_classification_review_sha256', state)
            with patch.object(sys, 'argv', ['repair', '--apply', '--rebuild', '--pending-only', '--report', str(report)]), \
                 patch.object(pipeline, '_serialize_pipeline_maintenance', side_effect=lambda fn: fn), \
                 patch.object(pipeline, 'load_state', return_value=state), \
                 patch.object(pipeline, 'save_state') as save, \
                 patch.object(pipeline, 'rebuild_registry_backed_outputs', side_effect=rebuild) as build, \
                 patch.object(script, 'repair_directory', return_value={'rows': 0}) as repair:
                # Missing marker must complete a rebuild even after a previous
                # process repaired the rows but stopped before output rebuild.
                script.main()
                self.assertEqual(n.REVIEW_SHA256, state['note_classification_review_sha256'])
                build.assert_called_once()
                save.assert_called_once()
                script.main()
                repair.assert_called_once()
                build.assert_called_once()
                self.assertTrue(json.loads(report.read_text())['already_applied'])


if __name__ == "__main__":
    unittest.main()
