from __future__ import annotations

import copy
import json
import os
import stat
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

import pipeline
from scripts import repair_value_units


FIXTURE_FUNDS_DIR = Path(__file__).resolve().parent / "fixtures/corpus/funds"


class ExplicitHistoricalRepairTests(unittest.TestCase):
    def load_fund(self, cik: int) -> dict:
        return json.loads((FIXTURE_FUNDS_DIR / f"{cik}.json").read_text())

    def source_quarter(self, cik: int, report_date: str) -> dict:
        quarter = copy.deepcopy(next(
            quarter
            for quarter in self.load_fund(cik)["quarters"]
            if quarter["report_date"] == report_date
        ))
        spec = repair_value_units.EXPLICIT_HISTORICAL_REPAIRS[
            (cik, report_date)
        ]
        if quarter["total_value"] == spec["correct_total"]:
            unscaled_cusips = set(spec.get("unscaled_cusips", ()))
            for holding in quarter["holdings"]:
                cusip = str(holding.get("cusip") or "").strip().upper()
                if (
                    spec["operation"] == "multiply_except_cusips"
                    and cusip in unscaled_cusips
                ):
                    continue
                if spec["operation"] != "divide_all":
                    holding["value"] //= 1000
                else:
                    holding["value"] *= 1000
            quarter["total_value"] = sum(
                holding["value"] for holding in quarter["holdings"]
            )
        self.assertEqual(spec["bad_total"], quarter["total_value"])
        self.assertEqual(
            spec["bad_signature"],
            repair_value_units.holding_value_signature(quarter["holdings"]),
        )
        return quarter

    def locked_funds_directory(self, funds_dir: Path) -> int:
        descriptor, _frozen = repair_value_units._prepare_verified_directory(funds_dir)
        repair_value_units.fcntl.flock(descriptor, repair_value_units.fcntl.LOCK_EX)
        return descriptor

    def close_locked_funds_directory(self, descriptor: int) -> None:
        try:
            repair_value_units.fcntl.flock(descriptor, repair_value_units.fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def create_interrupted_transaction(
        self,
        funds_dir: Path,
        phase: str,
    ) -> None:
        funds_fd = self.locked_funds_directory(funds_dir)
        transaction_fd = backup_fd = staged_fd = -1
        try:
            repair_value_units._create_repair_transaction_at(
                funds_fd,
                {"1.json": {"generation": "new"}},
            )
            transaction_fd = repair_value_units._open_repair_dir(
                funds_fd,
                repair_value_units.REPAIR_TRANSACTION_NAME,
                "repair transaction",
            )
            marker = repair_value_units._load_repair_marker_at(transaction_fd)
            backup_fd = repair_value_units._open_repair_dir(
                transaction_fd, "backup", "repair backup directory"
            )
            staged_fd = repair_value_units._open_repair_dir(
                transaction_fd, "staged", "repair staging directory"
            )
            os.replace("1.json", "1.json", src_dir_fd=funds_fd, dst_dir_fd=backup_fd)
            os.replace("1.json", "1.json", src_dir_fd=staged_fd, dst_dir_fd=funds_fd)
            if phase == "published":
                repair_value_units._atomic_write_json_at(
                    transaction_fd,
                    repair_value_units.REPAIR_MARKER_NAME,
                    repair_value_units._repair_marker(
                        phase="published",
                        targets=marker["targets"],
                        present=marker["present"],
                        before_sha256=marker["before_sha256"],
                        after_sha256=marker["after_sha256"],
                    ),
                )
        finally:
            for descriptor in (staged_fd, backup_fd, transaction_fd):
                if descriptor >= 0:
                    os.close(descriptor)
            self.close_locked_funds_directory(funds_fd)

    def test_exact_manifest_repairs_are_idempotent(self) -> None:
        for key, spec in (
            repair_value_units.EXPLICIT_HISTORICAL_REPAIRS.items()
        ):
            with self.subTest(cik=key[0], report_date=key[1]):
                quarter = self.source_quarter(*key)

                self.assertTrue(
                    repair_value_units.repair_explicit_historical_quarter(
                        quarter,
                        spec,
                    )
                )

                self.assertEqual(spec["correct_total"], quarter["total_value"])
                self.assertEqual(
                    spec["correct_signature"],
                    repair_value_units.holding_value_signature(
                        quarter["holdings"]
                    ),
                )
                if spec["value_multiplier"] is None:
                    self.assertNotIn("value_multiplier", quarter)
                    self.assertNotIn("value_unit_policy_version", quarter)
                    self.assertEqual(
                        "sec_verified_historical_migration",
                        quarter["value_unit_repair"]["method"],
                    )
                    self.assertEqual(
                        spec["accession"],
                        quarter["value_unit_repair"]["evidence"][
                            "sec_accession"
                        ],
                    )
                else:
                    self.assertEqual(
                        "sec_verified_historical_migration",
                        quarter["value_unit_method"],
                    )
                    self.assertEqual(
                        spec["accession"],
                        quarter["value_unit_evidence"]["sec_accession"],
                    )
                    self.assertEqual(
                        spec["value_multiplier"],
                        quarter["value_multiplier"],
                    )
                self.assertFalse(
                    repair_value_units.repair_explicit_historical_quarter(
                        quarter,
                        spec,
                    )
                )

    def test_ccla_scales_only_verified_thousands_rows(self) -> None:
        key = (1631562, "2025-06-30")
        spec = repair_value_units.EXPLICIT_HISTORICAL_REPAIRS[key]
        quarter = self.source_quarter(*key)
        before = {
            holding["cusip"]: holding["value"]
            for holding in quarter["holdings"]
        }

        repair_value_units.repair_explicit_historical_quarter(quarter, spec)
        after = {
            holding["cusip"]: holding["value"]
            for holding in quarter["holdings"]
        }

        self.assertEqual(before["002824100"], after["002824100"])
        self.assertEqual(before["594918104"] * 1000, after["594918104"])
        self.assertEqual(6_102_754_336, quarter["total_value"])
        self.assertEqual(
            {"default": 1000, "002824100": 1},
            quarter["value_unit_repair"]["evidence"][
                "row_value_multipliers"
            ],
        )
        self.assertNotIn("value_unit_policy_version", quarter)
        self.assertNotIn(1631562, repair_value_units.KNOWN_REPAIRS[1])

    def test_manifest_fails_closed_if_rows_change(self) -> None:
        key = (1629996, "2025-12-31")
        spec = repair_value_units.EXPLICIT_HISTORICAL_REPAIRS[key]
        quarter = self.source_quarter(*key)
        quarter["holdings"][0]["value"] += 1
        quarter["holdings"][1]["value"] -= 1

        with self.assertRaisesRegex(ValueError, "source row signature changed"):
            repair_value_units.repair_explicit_historical_quarter(
                quarter,
                spec,
            )

    def test_apply_validates_all_targets_before_writing(self) -> None:
        funds = {
            cik: self.load_fund(cik)
            for cik in {key[0] for key in (
                repair_value_units.EXPLICIT_HISTORICAL_REPAIRS
            )}
        }
        for key in repair_value_units.EXPLICIT_HISTORICAL_REPAIRS:
            cik, report_date = key
            source = self.source_quarter(cik, report_date)
            index = next(
                index
                for index, quarter in enumerate(funds[cik]["quarters"])
                if quarter["report_date"] == report_date
            )
            funds[cik]["quarters"][index] = source

        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            for cik, fund in funds.items():
                (funds_dir / f"{cik}.json").write_text(json.dumps(fund))

            with mock.patch.object(
                repair_value_units,
                "FUNDS_DIR",
                funds_dir,
            ):
                self.assertEqual(
                    6,
                    repair_value_units.apply_explicit_historical_repairs(),
                )
                self.assertEqual(
                    0,
                    repair_value_units.apply_explicit_historical_repairs(),
                )

    def test_backfill_skips_current_composed_source_provenance(self) -> None:
        fund = {
            "cik": 999,
            "quarters": [{
                "report_date": "2025-12-31",
                "source_filings": [{
                    "applied": True,
                    "value_unit_policy_version": (
                        repair_value_units.VALUE_UNIT_POLICY_VERSION
                    ),
                    "value_multiplier": 1,
                    "value_unit_confidence": "high",
                }],
            }],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            path = funds_dir / "999.json"
            path.write_text(json.dumps(fund))
            with (
                mock.patch.object(
                    repair_value_units,
                    "FUNDS_DIR",
                    funds_dir,
                ),
                mock.patch.object(
                    repair_value_units,
                    "KNOWN_REPAIRS",
                    {1: {999: ("2025-12-31",)}},
                ),
            ):
                self.assertEqual(
                    0,
                    repair_value_units.backfill_known_repair_provenance(),
                )
            self.assertEqual(fund, json.loads(path.read_text()))

    def test_arithmetic_source_assignment_is_not_high_confidence(self) -> None:
        quarter = {
            "total_value": 2_003,
            "source_filings": [
                {
                    "accession": "0000000001-25-000001",
                    "applied": True,
                    "reported_value_total": 2,
                },
                {
                    "accession": "0000000001-25-000002",
                    "applied": True,
                    "reported_value_total": 3,
                },
            ],
        }

        repair_value_units.backfill_unit_provenance(quarter)

        sources = quarter["source_filings"]
        self.assertEqual([1_000, 1], [
            source["value_multiplier"] for source in sources
        ])
        self.assertTrue(all(
            source["value_unit_policy_version"]
            == repair_value_units.VALUE_UNIT_POLICY_VERSION
            for source in sources
        ))
        self.assertTrue(all(
            source["value_unit_confidence"] == "low"
            and source["value_unit_method"] == "arithmetic_only_migration"
            and source["value_unit_evidence"]["independent_unit_proof"] is False
            for source in sources
        ))

    def test_source_multiplier_inference_is_bounded_for_a_long_valid_chain(
        self,
    ) -> None:
        sources = [
            {
                "accession": f"0000000001-25-{index:06d}",
                "applied": True,
                "reported_value_total": index + 1,
            }
            for index in range(64)
        ]
        quarter = {
            "total_value": sum(source["reported_value_total"] for source in sources),
            "source_filings": sources,
        }

        with mock.patch(
            "itertools.product",
            side_effect=AssertionError("exponential enumeration reached"),
        ):
            assignment = repair_value_units.infer_source_multipliers(quarter)

        self.assertIsNotNone(assignment)
        self.assertEqual([1] * len(sources), [
            multiplier for _source, multiplier in assignment or []
        ])

    def test_atomic_write_json_cleans_temp_on_base_exception_and_fsyncs_success(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "fund.json"
            with mock.patch.object(
                repair_value_units.json,
                "dump",
                side_effect=KeyboardInterrupt("simulated serialization interruption"),
            ):
                with self.assertRaisesRegex(
                    KeyboardInterrupt,
                    "simulated serialization interruption",
                ):
                    repair_value_units.atomic_write_json(path, {"new": True})
            self.assertFalse(path.exists())
            self.assertEqual([], list(root.glob(".fund.json.*.tmp")))

            observed_modes: list[int] = []

            def record_fsync(descriptor: int) -> None:
                observed_modes.append(os.fstat(descriptor).st_mode)

            with mock.patch.object(
                repair_value_units.os,
                "fsync",
                side_effect=record_fsync,
            ):
                repair_value_units.atomic_write_json(path, {"new": True})

            self.assertEqual({"new": True}, json.loads(path.read_text()))
            self.assertTrue(any(stat.S_ISREG(mode) for mode in observed_modes))
            self.assertTrue(any(stat.S_ISDIR(mode) for mode in observed_modes))

    def test_atomic_write_json_keeps_replacement_temp_after_interruption(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "fund.json"
            temporary = root / ".fund.json.owned.tmp"

            def replace_temp_then_interrupt(*args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                temporary.unlink()
                temporary.write_text('{"victim":true}\n', encoding="utf-8")
                raise KeyboardInterrupt("simulated serialization interruption")

            with (
                mock.patch.object(repair_value_units.secrets, "token_hex", return_value="owned"),
                mock.patch.object(repair_value_units.json, "dump", side_effect=replace_temp_then_interrupt),
                self.assertRaisesRegex(KeyboardInterrupt, "serialization interruption"),
            ):
                repair_value_units.atomic_write_json(path, {"new": True})

            self.assertEqual({"victim": True}, json.loads(temporary.read_text()))
            self.assertFalse(path.exists())

    def test_atomic_write_json_rejects_replacement_temp_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "fund.json"
            temporary = root / ".fund.json.owned.tmp"
            path.write_text('{"old":true}\n', encoding="utf-8")
            real_dump = repair_value_units.json.dump

            def replace_temp_after_dump(*args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                real_dump(*args, **kwargs)
                temporary.unlink()
                temporary.write_text('{"victim":true}\n', encoding="utf-8")

            with (
                mock.patch.object(repair_value_units.secrets, "token_hex", return_value="owned"),
                mock.patch.object(repair_value_units.json, "dump", side_effect=replace_temp_after_dump),
                self.assertRaisesRegex(ValueError, "temporary file changed"),
            ):
                repair_value_units.atomic_write_json(path, {"new": True})

            self.assertEqual({"old": True}, json.loads(path.read_text()))
            self.assertEqual({"victim": True}, json.loads(temporary.read_text()))

    def test_private_creation_normalizes_modes_despite_restrictive_umask(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "missing" / "nested" / "state.json"
            funds_dir = root / "funds"
            observed_temp_modes: list[int] = []
            real_dump = repair_value_units.json.dump

            def inspect_temp_mode(*args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                observed_temp_modes.append(stat.S_IMODE(os.fstat(args[1].fileno()).st_mode))
                real_dump(*args, **kwargs)

            previous_umask = os.umask(0o777)
            try:
                with mock.patch.object(repair_value_units.json, "dump", side_effect=inspect_temp_mode):
                    repair_value_units.atomic_write_json(state_path, {"state": True})
                    funds_fd, _frozen = repair_value_units._prepare_verified_directory(funds_dir)
                    try:
                        repair_value_units._create_repair_transaction_at(
                            funds_fd, {"1.json": {"generation": "new"}}
                        )
                    finally:
                        os.close(funds_fd)
            finally:
                os.umask(previous_umask)

            transaction = funds_dir / repair_value_units.REPAIR_TRANSACTION_NAME
            for directory in (
                state_path.parent.parent,
                state_path.parent,
                funds_dir,
                transaction,
                transaction / "staged",
                transaction / "backup",
            ):
                self.assertEqual(0o700, stat.S_IMODE(directory.stat().st_mode))
            for private_json in (
                state_path,
                transaction / "staged" / "1.json",
                transaction / repair_value_units.REPAIR_MARKER_NAME,
            ):
                self.assertEqual(0o600, stat.S_IMODE(private_json.stat().st_mode))
            self.assertTrue(observed_temp_modes)
            self.assertEqual({0o600}, set(observed_temp_modes))

    def test_remove_repair_tree_at_rejects_swapped_admitted_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            parked = root / "parked"
            victim = root / "victim"
            for transaction_name in (
                repair_value_units.REPAIR_PREPARE_NAME,
                repair_value_units.REPAIR_CLEANUP_NAME,
            ):
                with self.subTest(transaction_name=transaction_name):
                    target = funds_dir / transaction_name
                    target.mkdir()
                    if not victim.exists():
                        victim.mkdir()
                    sentinel = victim / "sentinel.json"
                    sentinel.write_text('{"victim":true}\n', encoding="utf-8")
                    funds_fd = os.open(funds_dir, repair_value_units._directory_open_flags())
                    real_open = repair_value_units.os.open
                    swapped = False

                    def swap_after_admission(name, flags, *args, **kwargs):  # type: ignore[no-untyped-def]
                        nonlocal swapped
                        if name == transaction_name and kwargs.get("dir_fd") == funds_fd and not swapped:
                            swapped = True
                            os.replace(target, parked)
                            os.replace(victim, target)
                        return real_open(name, flags, *args, **kwargs)

                    try:
                        with (
                            mock.patch.object(repair_value_units.os, "open", side_effect=swap_after_admission),
                            self.assertRaisesRegex(ValueError, "changed while opening"),
                        ):
                            repair_value_units._remove_repair_tree_at(funds_fd, transaction_name)
                    finally:
                        os.close(funds_fd)

                    self.assertTrue(swapped)
                    self.assertEqual({"victim": True}, json.loads((target / "sentinel.json").read_text()))
                    os.replace(target, victim)
                    os.replace(parked, target)

    def test_atomic_write_json_creates_missing_parent_with_private_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "missing" / "nested" / "state.json"

            repair_value_units.atomic_write_json(path, {"created": True})

            self.assertEqual({"created": True}, json.loads(path.read_text()))
            for directory in (path.parent, path.parent.parent):
                self.assertEqual(0o700, stat.S_IMODE(directory.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))

    def test_atomic_write_json_allows_an_ordinary_tmp_symlink_parent(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
            root = Path(tmpdir)
            target = root / "target"
            target.mkdir()
            link = root / "ordinary-link"
            link.symlink_to(target, target_is_directory=True)

            path = link / "state.json"
            repair_value_units.atomic_write_json(path, {"portable": True})

            self.assertEqual({"portable": True}, json.loads((target / "state.json").read_text()))
            self.assertEqual(0o600, stat.S_IMODE((target / "state.json").stat().st_mode))

    def test_atomic_write_json_rejects_parent_retarget_before_temp_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parent = root / "parent"
            parked_parent = root / "parent-parked"
            victim = root / "victim"
            parent.mkdir()
            victim.mkdir()
            victim_target = victim / "state.json"
            victim_target.write_text('{"sentinel":true}\n', encoding="utf-8")
            path = parent / "state.json"
            real_open = os.open
            replaced = False

            def retarget_before_temp_open(candidate, flags, *args, **kwargs):  # type: ignore[no-untyped-def]
                nonlocal replaced
                if (
                    kwargs.get("dir_fd") is not None
                    and str(candidate).startswith(".state.json.")
                    and not replaced
                ):
                    replaced = True
                    os.replace(parent, parked_parent)
                    parent.symlink_to(victim, target_is_directory=True)
                return real_open(candidate, flags, *args, **kwargs)

            with mock.patch.object(
                repair_value_units.os,
                "open",
                side_effect=retarget_before_temp_open,
            ):
                with self.assertRaisesRegex(ValueError, "parent.*changed|continuity"):
                    repair_value_units.atomic_write_json(path, {"redirected": True})

            self.assertTrue(replaced)
            self.assertEqual({"sentinel": True}, json.loads(victim_target.read_text()))
            self.assertEqual([], list(victim.glob(".state.json.*.tmp")))
            self.assertFalse((parked_parent / "state.json").exists())

    def test_publish_rejects_missing_funds_parent_symlink_retarget(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            intended = root / "intended"
            retarget = root / "retarget"
            intended.mkdir()
            retarget.mkdir()
            intended_sentinel = intended / "sentinel.json"
            retarget_sentinel = retarget / "sentinel.json"
            intended_sentinel.write_text('{"intended":true}\n', encoding="utf-8")
            retarget_sentinel.write_text('{"retarget":true}\n', encoding="utf-8")
            ancestor = root / "ordinary-link"
            ancestor.symlink_to(intended, target_is_directory=True)
            funds_dir = ancestor / "missing-parent" / "funds"
            target = funds_dir / "1.json"
            real_identity = repair_value_units._directory_identity
            checked_missing = False

            def retarget_after_missing_check(candidate: Path) -> tuple[int, int]:
                nonlocal checked_missing
                if candidate == funds_dir and not checked_missing:
                    checked_missing = True
                    with self.assertRaises(FileNotFoundError):
                        real_identity(candidate)
                    ancestor.unlink()
                    ancestor.symlink_to(retarget, target_is_directory=True)
                    raise FileNotFoundError(candidate)
                return real_identity(candidate)

            with (
                mock.patch.object(repair_value_units, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    repair_value_units,
                    "_directory_identity",
                    side_effect=retarget_after_missing_check,
                ),
                self.assertRaisesRegex(ValueError, "continuity|identity changed"),
            ):
                repair_value_units._publish_fund_updates({target: {"new": True}})

            self.assertTrue(checked_missing)
            self.assertEqual({"intended": True}, json.loads(intended_sentinel.read_text()))
            self.assertEqual({"retarget": True}, json.loads(retarget_sentinel.read_text()))
            self.assertFalse((retarget / "missing-parent" / "funds").exists())
            self.assertFalse((retarget / "missing-parent" / "funds" / "1.json").exists())
            self.assertFalse((retarget / "missing-parent" / "funds" / repair_value_units.REPAIR_TRANSACTION_NAME).exists())

    def test_multi_fund_publish_rolls_back_all_files_on_keyboard_interrupt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            first = funds_dir / "1.json"
            second = funds_dir / "2.json"
            first.write_text('{"generation":"old-1"}\n', encoding="utf-8")
            second.write_text('{"generation":"old-2"}\n', encoding="utf-8")
            real_replace = os.replace
            published = 0

            def interrupt_second_publication(source, destination, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                nonlocal published
                source_path = Path(source)
                source_is_staged = source_path.parent.name == "staged"
                source_fd = kwargs.get("src_dir_fd")
                staged_path = (
                    funds_dir
                    / repair_value_units.REPAIR_TRANSACTION_NAME
                    / "staged"
                )
                if source_fd is not None and staged_path.exists():
                    source_is_staged = (
                        os.fstat(source_fd).st_ino == staged_path.stat().st_ino
                    )
                real_replace(source, destination, *args, **kwargs)
                if source_is_staged:
                    published += 1
                    if published == 1:
                        raise KeyboardInterrupt("simulated multi-fund interruption")

            with mock.patch.object(
                repair_value_units,
                "FUNDS_DIR",
                funds_dir,
            ), mock.patch.object(
                repair_value_units.os,
                "replace",
                side_effect=interrupt_second_publication,
            ):
                with self.assertRaisesRegex(
                    KeyboardInterrupt,
                    "simulated multi-fund interruption",
                ):
                    repair_value_units._publish_fund_updates({
                        first: {"generation": "new-1"},
                        second: {"generation": "new-2"},
                    })

            self.assertEqual(
                {"generation": "old-1"},
                json.loads(first.read_text()),
            )
            self.assertEqual(
                {"generation": "old-2"},
                json.loads(second.read_text()),
            )
            self.assertEqual([], list(funds_dir.glob(".value-unit-repair*")))

    def test_published_transaction_does_not_adopt_a_different_valid_generation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            target = funds_dir / "1.json"
            target.write_text('{"generation":"old"}\n', encoding="utf-8")

            with mock.patch.object(repair_value_units, "FUNDS_DIR", funds_dir):
                self.create_interrupted_transaction(funds_dir, "published")
                funds_fd = self.locked_funds_directory(funds_dir)
                try:
                    repair_value_units._atomic_write_json_at(
                        funds_fd,
                        "1.json",
                        {"generation": "different-valid-json"},
                    )
                    transaction_fd = repair_value_units._open_repair_dir(
                        funds_fd,
                        repair_value_units.REPAIR_TRANSACTION_NAME,
                        "repair transaction",
                    )
                    try:
                        self.assertEqual(
                            "published",
                            repair_value_units._load_repair_marker_at(transaction_fd)["phase"],
                        )
                    finally:
                        os.close(transaction_fd)
                finally:
                    self.close_locked_funds_directory(funds_fd)

                with self.assertRaisesRegex(
                    ValueError,
                    "interrupted repair target hash does not match marker",
                ):
                    repair_value_units._publish_fund_updates({})

            self.assertEqual(
                {"generation": "different-valid-json"},
                json.loads(target.read_text()),
            )
            funds_fd = self.locked_funds_directory(funds_dir)
            transaction_fd = backup_fd = -1
            try:
                transaction_fd = repair_value_units._open_repair_dir(
                    funds_fd,
                    repair_value_units.REPAIR_TRANSACTION_NAME,
                    "repair transaction",
                )
                backup_fd = repair_value_units._open_repair_dir(
                    transaction_fd,
                    "backup",
                    "repair backup directory",
                )
                self.assertIsNotNone(repair_value_units._entry_stat(backup_fd, "1.json"))
            finally:
                for descriptor in (backup_fd, transaction_fd):
                    if descriptor >= 0:
                        os.close(descriptor)
                self.close_locked_funds_directory(funds_fd)

    def test_prepared_transaction_is_rolled_back_on_next_invocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            target = funds_dir / "1.json"
            target.write_text('{"generation":"old"}\n', encoding="utf-8")

            with mock.patch.object(repair_value_units, "FUNDS_DIR", funds_dir):
                self.create_interrupted_transaction(funds_dir, "prepared")
                repair_value_units._publish_fund_updates({})

            self.assertEqual(
                {"generation": "old"},
                json.loads(target.read_text()),
            )
            self.assertEqual([], list(funds_dir.glob(".value-unit-repair*")))

    def test_exact_published_transaction_is_adopted_on_next_invocation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            target = funds_dir / "1.json"
            target.write_text('{"generation":"old"}\n', encoding="utf-8")

            with mock.patch.object(repair_value_units, "FUNDS_DIR", funds_dir):
                self.create_interrupted_transaction(funds_dir, "published")
                repair_value_units._publish_fund_updates({})

            self.assertEqual(
                {"generation": "new"},
                json.loads(target.read_text()),
            )
            self.assertEqual([], list(funds_dir.glob(".value-unit-repair*")))

    def test_repair_transaction_symlink_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            victim = root / "victim"
            victim.mkdir()
            sentinel = victim / "sentinel.json"
            sentinel.write_text('{"untouched":true}\n', encoding="utf-8")
            (funds_dir / repair_value_units.REPAIR_TRANSACTION_NAME).symlink_to(
                victim,
                target_is_directory=True,
            )

            with mock.patch.object(
                repair_value_units,
                "FUNDS_DIR",
                funds_dir,
            ):
                with self.assertRaisesRegex(ValueError, "must be a real directory"):
                    repair_value_units._publish_fund_updates({})

            self.assertEqual({"untouched": True}, json.loads(sentinel.read_text()))

    def test_publish_rejects_real_directory_replacement_after_open(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            parked_funds_dir = root / "funds-locked"
            funds_dir.mkdir()
            target = funds_dir / "1.json"
            target.write_text('{"generation":"old"}\n', encoding="utf-8")
            real_flock = repair_value_units.fcntl.flock
            swapped = False

            def swap_after_lock(descriptor: int, operation: int) -> None:
                nonlocal swapped
                real_flock(descriptor, operation)
                if operation == repair_value_units.fcntl.LOCK_EX and not swapped:
                    swapped = True
                    os.replace(funds_dir, parked_funds_dir)
                    funds_dir.mkdir()

            with (
                mock.patch.object(repair_value_units, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    repair_value_units.fcntl,
                    "flock",
                    side_effect=swap_after_lock,
                ),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "identity changed during repair",
                ):
                    repair_value_units._publish_fund_updates({
                        target: {"generation": "new"},
                    })

            self.assertTrue(swapped)
            self.assertEqual(
                {"generation": "old"},
                json.loads((parked_funds_dir / "1.json").read_text()),
            )
            self.assertEqual(
                [],
                list(parked_funds_dir.glob(".value-unit-repair*")),
            )
            self.assertFalse((funds_dir / "1.json").exists())
            self.assertEqual([], list(funds_dir.glob(".value-unit-repair*")))

    def test_publish_rejects_real_directory_replacement_before_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            parked_funds_dir = root / "funds-locked"
            funds_dir.mkdir()
            target = funds_dir / "1.json"
            target.write_text('{"generation":"old"}\n', encoding="utf-8")
            real_open = repair_value_units.os.open
            replaced = False

            def replace_before_open(path, flags, *args, **kwargs):  # type: ignore[no-untyped-def]
                nonlocal replaced
                if Path(path) == funds_dir and not replaced:
                    replaced = True
                    os.replace(funds_dir, parked_funds_dir)
                    funds_dir.mkdir()
                return real_open(path, flags, *args, **kwargs)

            with (
                mock.patch.object(repair_value_units, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    repair_value_units.os,
                    "open",
                    side_effect=replace_before_open,
                ),
                self.assertRaisesRegex(
                    ValueError,
                    "identity changed during repair",
                ),
            ):
                repair_value_units._publish_fund_updates({
                    target: {"generation": "new"},
                })

            self.assertTrue(replaced)
            self.assertEqual(
                {"generation": "old"},
                json.loads((parked_funds_dir / "1.json").read_text()),
            )
            self.assertFalse((funds_dir / "1.json").exists())
            self.assertEqual([], list(funds_dir.glob(".value-unit-repair*")))

    def test_source_multiplier_inference_rejects_ambiguity_and_state_excess(
        self,
    ) -> None:
        ambiguous = {
            "total_value": 1_001,
            "source_filings": [
                {"applied": True, "reported_value_total": 1},
                {"applied": True, "reported_value_total": 1},
            ],
        }
        self.assertIsNone(repair_value_units.infer_source_multipliers(ambiguous))

        bounded = {
            "total_value": 2_999,
            "source_filings": [
                {"applied": True, "reported_value_total": 1},
                {"applied": True, "reported_value_total": 2},
            ],
        }
        with mock.patch.object(
            repair_value_units,
            "MAX_SOURCE_ASSIGNMENT_STATES",
            2,
        ):
            self.assertIsNone(repair_value_units.infer_source_multipliers(bounded))

    def test_policy_cutover_is_marked_only_after_a_clean_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "pipeline_state.json"
            state_path.write_text(json.dumps({"processed": []}))
            inventory = Counter({
                "quarters": 10,
                "legacy_or_low_confidence": 10,
            })
            with (
                mock.patch.object(
                    repair_value_units,
                    "STATE_PATH",
                    state_path,
                ),
                mock.patch.object(
                    repair_value_units,
                    "apply_explicit_historical_repairs",
                    return_value=0,
                ),
                mock.patch.object(
                    repair_value_units,
                    "audit_retained_value_unit_policy",
                    return_value=(inventory, []),
                ),
            ):
                result, explicit = (
                    repair_value_units.migrate_value_unit_policy()
                )

            self.assertEqual(inventory, result)
            self.assertEqual(0, explicit)
            self.assertEqual(
                repair_value_units.VALUE_UNIT_POLICY_VERSION,
                json.loads(state_path.read_text())[
                    "value_unit_migration_version"
                ],
            )

    def test_policy_cutover_fails_closed_on_a_corpus_anomaly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "pipeline_state.json"
            state_path.write_text(json.dumps({"processed": []}))
            with (
                mock.patch.object(
                    repair_value_units,
                    "STATE_PATH",
                    state_path,
                ),
                mock.patch.object(
                    repair_value_units,
                    "apply_explicit_historical_repairs",
                    return_value=0,
                ),
                mock.patch.object(
                    repair_value_units,
                    "audit_retained_value_unit_policy",
                    return_value=(
                        Counter({"quarters": 10}),
                        ["999 2025-12-31 mixed_scale_clusters"],
                    ),
                ),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "found 1 anomaly",
                ):
                    repair_value_units.migrate_value_unit_policy()

            self.assertNotIn(
                "value_unit_migration_version",
                json.loads(state_path.read_text()),
            )

    def test_pipeline_state_preserves_value_unit_cutover_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "pipeline_state.json"
            legacy_state_path = Path(tmpdir) / "legacy-state.json"
            state = {
                "processed": [],
                "_processed_set": set(),
                "quarantined": {},
                "_quarantined": {},
                "amendment_migration_pending": {},
                "amendment_reducer_version": (
                    pipeline.AMENDMENT_REDUCER_VERSION
                ),
                "security_identity_migration_pending": {},
                "security_identity_migration_version": (
                    pipeline.SECURITY_IDENTITY_VERSION
                ),
                "value_unit_migration_version": (
                    pipeline.VALUE_UNIT_MIGRATION_VERSION
                ),
            }
            with mock.patch.multiple(
                pipeline,
                STATE_PATH=state_path,
                LEGACY_STATE_PATH=legacy_state_path,
            ):
                pipeline.save_state(state)
                loaded = pipeline.load_state()

            self.assertEqual(
                pipeline.VALUE_UNIT_MIGRATION_VERSION,
                loaded["value_unit_migration_version"],
            )


if __name__ == "__main__":
    unittest.main()
