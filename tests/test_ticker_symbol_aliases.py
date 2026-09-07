from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import sec_security_master as master
from tests.test_sec_security_master import ftd_record, source_state


CUSIP = "526057302"
KEY = f"{CUSIP}|EQUITY"
UNIVERSE = [{
    "cusip": CUSIP,
    "instrument_type": "EQUITY",
    "reported_issuer": "LENNAR CORP",
    "reported_class": "CL B",
}]


def alias_state(*, official_class="CL B", ftd_class="CL-B", symbols=None,
                days=("2026-08-03", "2026-08-04")):
    symbols = symbols or ["LEN-B"]
    return source_state(
        rows=[ftd_record(
            day, "LENB", cusip=CUSIP,
            description=f"LENNAR CORPORATION {ftd_class}",
        ) for day in days],
        symbols=symbols,
        symbol_titles={symbol: ["LENNAR CORP /NEW/"] for symbol in symbols},
        symbol_exchanges={symbol: ["NYSE"] for symbol in symbols},
        official_rows=[{
            "cusip": CUSIP, "issuer": "LENNAR CORP",
            "description": official_class, "status": "", "option_indicator": "",
        }],
    )


class TickerSymbolAliasTests(unittest.TestCase):
    def test_canonical_ticker_keeps_raw_ftd_observations_and_class_proof(self):
        state = alias_state()
        built = master.rebuild_security_master(state, UNIVERSE)
        record = built["records"][KEY]
        self.assertEqual(record["ticker"], "LEN-B")
        self.assertEqual(record["candidate_ticker"], "LENB")
        self.assertEqual(record["ticker_source"], "sec_ftd")
        self.assertEqual(record["symbol_validation_alias"]["share_class"], "B")
        self.assertEqual({row["symbol"] for row in record["symbol_evidence"]}, {"LENB"})
        self.assertEqual({row["symbol"] for row in record["symbol_intervals"]}, {"LENB"})
        master.validate_security_master(built)
        master._validate_symbol_alias_source_state(built, master._normalize_source_state(state))

    def test_alias_tampering_is_rejected(self):
        built = master.rebuild_security_master(alias_state(), UNIVERSE)
        for field, replacement in (
            ("ftd_symbol", "LENA"), ("sec_symbol", "LEN-A"),
            ("share_class", "A"), ("sec_sources", []),
        ):
            with self.subTest(field=field):
                altered = copy.deepcopy(built)
                altered["records"][KEY]["symbol_validation_alias"][field] = replacement
                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(altered)
        for field in ("sec_sources", "official_13f"):
            with self.subTest(checksum=field):
                altered = copy.deepcopy(built)
                proof = altered["records"][KEY]["symbol_validation_alias"][field]
                (proof[0] if isinstance(proof, list) else proof)["sha256"] = "f" * 64
                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(altered)
        altered = copy.deepcopy(built)
        altered["records"][KEY].pop("symbol_validation_alias")
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(altered)

    def test_explicit_official_and_ftd_class_are_both_required(self):
        for official, ftd in (("COM", "CL-B"), ("CL A", "CL-B"),
                              ("CL B", "CL-A"), ("CL B", "COMMON")):
            with self.subTest(official=official, ftd=ftd):
                result = master.rebuild_security_master(
                    alias_state(official_class=official, ftd_class=ftd), UNIVERSE,
                )
                self.assertIsNone(result["records"][KEY]["ticker"])
                self.assertNotIn("symbol_validation_alias", result["records"][KEY])

    def test_ambiguous_punctuation_variants_do_not_resolve(self):
        result = master.rebuild_security_master(
            alias_state(symbols=["LEN-B", "LEN.B"]), UNIVERSE,
        )
        self.assertIsNone(result["records"][KEY]["ticker"])
        self.assertNotIn("symbol_validation_alias", result["records"][KEY])

    def test_exact_symbol_collision_does_not_borrow_class_metadata(self):
        state = alias_state(symbols=["LEN-B", "LENB"])
        state["sources"][master.SEC_COMPANY_TICKERS_URL]["symbol_titles"]["LENB"] = ["UNRELATED ISSUER INC"]
        result = master.rebuild_security_master(state, UNIVERSE)
        self.assertIsNone(result["records"][KEY]["ticker"])
        self.assertNotIn("symbol_validation_alias", result["records"][KEY])

    def test_options_and_preferred_shares_do_not_use_equity_alias(self):
        for kind in ("CALL", "PUT", "PREF", "NOTE", "WARRANT"):
            with self.subTest(kind=kind):
                universe = [{**UNIVERSE[0], "instrument_type": kind}]
                record = master.rebuild_security_master(alias_state(), universe)["records"][f"{CUSIP}|{kind}"]
                self.assertIsNone(record["ticker"])
                self.assertNotIn("symbol_validation_alias", record)

    def test_pair_roundtrip_and_deterministic_replay_retain_canonical_symbol(self):
        state = alias_state()
        built = master.rebuild_security_master(state, UNIVERSE)
        with tempfile.TemporaryDirectory() as directory:
            paths = {"master_path": Path(directory) / "master.json",
                     "source_state_path": Path(directory) / "state.json"}
            master.save_security_master_pair(built, state, **paths)
            loaded, loaded_state = master.load_security_master_pair(**paths)
            self.assertEqual(loaded["records"][KEY]["ticker"], "LEN-B")
            rebuilt = master.rebuild_security_master(loaded_state, UNIVERSE)
            self.assertEqual(loaded, rebuilt)

    def test_pair_binding_rechecks_symbol_collision_against_source_state(self):
        built = master.rebuild_security_master(alias_state(), UNIVERSE)
        changed = master._normalize_source_state(alias_state(symbols=["LEN-B", "LENB"]))
        with self.assertRaises(master.SecurityMasterError):
            master._validate_symbol_alias_source_state(built, changed)

    def test_pair_binding_rechecks_the_retained_ftd_class(self):
        built = master.rebuild_security_master(alias_state(), UNIVERSE)
        changed = master._normalize_source_state(alias_state(ftd_class="CL-A"))
        with self.assertRaises(master.SecurityMasterError):
            master._validate_symbol_alias_source_state(built, changed)

    def test_resolution_rule_version_is_explicit_and_legacy_is_readable(self):
        built = master.rebuild_security_master(alias_state(), UNIVERSE)
        self.assertEqual(built["policy"]["resolution_rules_version"], 2)
        legacy = copy.deepcopy(built)
        legacy["policy"].pop("resolution_rules_version")
        master.validate_security_master(legacy)
        for invalid in (None, True, "2", 0, -1):
            with self.subTest(invalid=invalid):
                altered = copy.deepcopy(built)
                altered["policy"]["resolution_rules_version"] = invalid
                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(altered)

    def test_incremental_source_failure_retains_verified_alias(self):
        state = alias_state()
        built = master.rebuild_security_master(state, UNIVERSE)
        retained = master._retain_prior_mappings_with_unresolved_extensions(
            built, state, UNIVERSE,
        )
        self.assertEqual(retained["records"][KEY], built["records"][KEY])
        master.validate_security_master(retained)

    def test_partial_fallback_does_not_claim_a_completed_rule_upgrade(self):
        state = alias_state()
        built = master.rebuild_security_master(state, UNIVERSE)
        for version in (None, 1):
            with self.subTest(version=version):
                prior = copy.deepcopy(built)
                if version is None:
                    prior["policy"].pop("resolution_rules_version")
                else:
                    prior["policy"]["resolution_rules_version"] = version
                retained = master._retain_prior_mappings_with_unresolved_extensions(
                    prior, state, UNIVERSE,
                )
                self.assertEqual(retained["policy"], prior["policy"])

    def test_newer_unconfirmed_raw_symbol_withdraws_old_canonical_claim(self):
        state = alias_state()
        built = master.rebuild_security_master(state, UNIVERSE)
        record = built["records"][KEY]
        new_record = {
            "cusip": "526057104", "mapping_status": "unresolved",
            "candidate_ticker": "LENB", "candidate_as_of": "2026-09-05",
            "symbol_evidence": [{"symbol": "LENB", "settlement_date": "2026-09-05",
                                 "sources": [{"url": "retained SEC source"}]}],
        }
        master._reconcile_current_symbol_cusips(
            {KEY: record, "526057104|EQUITY": new_record}, concurrent_window_days=30,
        )
        self.assertIsNone(record["ticker"])
        self.assertEqual(record["resolution_reason"], "current_symbol_observed_on_newer_cusip")

    def test_candidate_own_alias_supersedes_canonical_ftd_and_ixbrl_claims(self):
        newer = master.rebuild_security_master(
            alias_state(days=("2026-09-05",)), UNIVERSE,
        )
        self.assertEqual(newer["records"][KEY]["mapping_status"], "unresolved")
        self.assertEqual(newer["records"][KEY]["resolution_reason"],
                         "insufficient_distinct_ftd_settlement_dates")
        for source in ("sec_ftd", "sec_ixbrl"):
            with self.subTest(source=source):
                older = {
                    "cusip": "526057104", "mapping_status": "resolved",
                    "ticker": "LEN-B", "ticker_source": source,
                    "ticker_as_of": "2026-07-01",
                }
                newer_record = copy.deepcopy(newer["records"][KEY])
                master._reconcile_current_symbol_cusips(
                    {"526057104|EQUITY": older, KEY: newer_record},
                    concurrent_window_days=30, source_provenance=newer["sources"],
                )
                self.assertIsNone(older["ticker"])
                self.assertEqual(older["superseded_by_cusips"], [CUSIP])
                self.assertIsNone(newer_record["ticker"])

    def test_unverified_candidate_alias_cannot_supersede_canonical_claim(self):
        newer = master.rebuild_security_master(
            alias_state(days=("2026-09-05",)), UNIVERSE,
        )
        for mutation in ("checksum", "issuer", "class", "missing_sources"):
            with self.subTest(mutation=mutation):
                older = {
                    "cusip": "526057104", "mapping_status": "resolved",
                    "ticker": "LEN-B", "ticker_source": "sec_ixbrl",
                    "ticker_as_of": "2026-07-01",
                }
                candidate = copy.deepcopy(newer["records"][KEY])
                if mutation == "checksum":
                    candidate["symbol_validation_alias"]["sec_sources"][0]["sha256"] = "f" * 64
                elif mutation == "issuer":
                    candidate["symbol_validation_titles"] = ["UNRELATED ISSUER INC"]
                elif mutation == "class":
                    candidate["symbol_validation_alias"]["share_class"] = "A"
                sources = [] if mutation == "missing_sources" else newer["sources"]
                master._reconcile_current_symbol_cusips(
                    {"526057104|EQUITY": older, KEY: candidate},
                    concurrent_window_days=30, source_provenance=sources,
                )
                self.assertEqual(older["ticker"], "LEN-B")

    def test_sequential_equivalent_spellings_share_identity_without_pooling_dates(self):
        for latest_symbol in ("LENB", "LEN-B"):
            with self.subTest(latest_symbol=latest_symbol):
                state = alias_state()
                source = next(row for row in state["sources"].values()
                              if row["kind"] == "sec_ftd_archive")
                first_symbol = "LEN-B" if latest_symbol == "LENB" else "LENB"
                source["records"] = master.compact_ftd_records([
                    ftd_record(day, symbol, cusip=CUSIP,
                               description="LENNAR CORPORATION CL-B")
                    for day, symbol in (("2026-08-03", first_symbol),
                                        ("2026-08-04", first_symbol),
                                        ("2026-08-05", latest_symbol),
                                        ("2026-08-06", latest_symbol))
                ])
                source["record_count"] = len(source["records"])
                source["raw_record_count"] = 4
                built = master.rebuild_security_master(state, UNIVERSE)
                record = built["records"][KEY]
                self.assertEqual(record["ticker"], "LEN-B")
                self.assertEqual(record["candidate_ticker"], latest_symbol)
                self.assertEqual(record["ticker_as_of"], "2026-08-06")
                self.assertEqual(record["confirmation_dates"], ["2026-08-05", "2026-08-06"])
                self.assertEqual({row["symbol"] for row in record["symbol_evidence"]}, {"LENB", "LEN-B"})
                master.validate_security_master(built)

    def test_one_date_per_equivalent_spelling_cannot_resolve(self):
        state = alias_state()
        source = next(row for row in state["sources"].values()
                      if row["kind"] == "sec_ftd_archive")
        source["records"] = master.compact_ftd_records([
            ftd_record(day, symbol, cusip=CUSIP, description="LENNAR CORPORATION CL-B")
            for day, symbol in (("2026-08-03", "LENB"), ("2026-08-04", "LEN-B"))
        ])
        source["record_count"] = len(source["records"])
        record = master.rebuild_security_master(state, UNIVERSE)["records"][KEY]
        self.assertIsNone(record["ticker"])
        self.assertEqual(record["resolution_reason"], "insufficient_distinct_ftd_settlement_dates")

    def test_same_date_conflicting_spellings_retain_ambiguous_interval(self):
        state = alias_state()
        source = next(row for row in state["sources"].values()
                      if row["kind"] == "sec_ftd_archive")
        source["records"] = master.compact_ftd_records([
            ftd_record(day, symbol, cusip=CUSIP, description="LENNAR CORPORATION CL-B")
            for day in ("2026-08-03", "2026-08-04") for symbol in ("LENB", "LEN-B")
        ])
        source["record_count"] = len(source["records"])
        source["raw_record_count"] = 4
        record = master.rebuild_security_master(state, UNIVERSE)["records"][KEY]
        self.assertIsNone(record["ticker"])
        self.assertEqual(record["resolution_reason"], "overlapping_ftd_symbol_spellings")


if __name__ == "__main__":
    unittest.main()
