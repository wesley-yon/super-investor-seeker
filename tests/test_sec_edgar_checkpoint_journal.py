from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pipeline
from sec_edgar_evidence import (
    DiscoveryDiagnostic,
    DiscoveryResult,
)
from sec_security_master import (
    RefreshResult,
    empty_source_state,
    source_state_sha256,
)


class SecEdgarCheckpointJournalTests(unittest.TestCase):
    CUSIPS = ("111111118", "222222226")
    CURRENT = datetime(2026, 8, 31, tzinfo=timezone.utc)

    @classmethod
    def result(cls) -> RefreshResult:
        records = {
            f"{cusip}|EQUITY": {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "mapping_status": "unresolved",
                "resolution_reason": "no_ftd_symbol_evidence",
            }
            for cusip in cls.CUSIPS
        }
        return RefreshResult(
            master={
                "policy": {
                    "recent_window_days": 31,
                    "max_evidence_age_days": 395,
                    "min_confirmation_dates": 2,
                },
                "records": records,
                "summary": {"resolved": 0, "unresolved": len(records)},
            },
            state={
                "updated_at": "2026-08-01T00:00:00Z",
                "edgar_discovery": {},
                "edgar_evidence": {},
            },
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )

    @classmethod
    def universe(cls) -> list[dict[str, str]]:
        return [
            {"cusip": cusip, "instrument_type": "EQUITY"}
            for cusip in cls.CUSIPS
        ]

    @classmethod
    def fingerprints(cls) -> dict[str, str]:
        return {cusip: str(index + 1) * 64 for index, cusip in enumerate(cls.CUSIPS)}

    @staticmethod
    def discovery_for(cusip: str):
        return SimpleNamespace(
            sources=(),
            to_dict=lambda: {
                "sources": [],
                "diagnostics": [
                    {
                        "cusip": cusip,
                        "status": "no_evidence",
                        "terminal": True,
                        "reason": "no_exact_schedule_cusip",
                    }
                ],
                "fetched_sources": [
                    {
                        "kind": "sec_cusip_search",
                        "url": (
                            "https://efts.sec.gov/LATEST/search-index?q=" + cusip
                        ),
                        "outcome": "fetched",
                        "sha256": "a" * 64,
                    }
                ],
            },
        )

    def run_checkpointed_refresh(
        self,
        root: Path,
        *,
        discover_side_effect,
    ):
        original = self.result()
        rebuilt = {
            **original.master,
            "summary": {"resolved": 0, "unresolved": len(self.CUSIPS)},
        }
        patches = (
            mock.patch.object(pipeline, "_SEC_EDGAR_CLEAN_CHUNK_SIZE", 1),
            mock.patch.object(
                pipeline,
                "_sec_edgar_discovery_candidates",
                return_value=(list(self.CUSIPS), self.fingerprints()),
            ),
            mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                side_effect=discover_side_effect,
            ),
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=rebuilt,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value={"ok": True, "issues": []},
            ),
            mock.patch.object(pipeline, "save_security_master_pair"),
        )
        return original, patches

    def test_batches_materialize_full_pair_only_once(self) -> None:
        calls: list[tuple[str, ...]] = []

        def discover(batch, *, fetcher):
            del fetcher
            calls.append(tuple(batch))
            return self.discovery_for(batch[0])

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            original, patches = self.run_checkpointed_refresh(
                root,
                discover_side_effect=discover,
            )
            with patches[0], patches[1], patches[2], patches[3] as rebuild, patches[
                4
            ], patches[5] as save_pair:
                refreshed = pipeline._refresh_sec_edgar_exceptions(
                    original,
                    self.universe(),
                    refreshed_at=self.CURRENT,
                    fetcher=object(),
                    checkpoint_batches=True,
                    checkpoint_root=root,
                )

            self.assertEqual(
                [(self.CUSIPS[0],), (self.CUSIPS[1],)],
                calls,
            )
            # One provisional rebuild determines the post-application record
            # fingerprints; the second binds the master to that final state.
            self.assertEqual(2, rebuild.call_count)
            self.assertEqual(1, save_pair.call_count)
            self.assertEqual(2, len(refreshed.state["edgar_discovery"]["records"]))
            fetched = refreshed.state["edgar_discovery"]["fetched_sources"]
            self.assertEqual(2, len(fetched))
            self.assertTrue(
                all(record["sha256"] == "a" * 64 for record in fetched.values())
            )
            first = json.loads(
                pipeline._sec_edgar_journal_path(root, 0).read_text(
                    encoding="utf-8"
                )
            )
            second = json.loads(
                pipeline._sec_edgar_journal_path(root, 1).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(first["entry_sha256"], second["prior_entry_sha256"])

    def test_post_application_fingerprint_persists_a_bound_pair(self) -> None:
        cusip = self.CUSIPS[0]
        universe = [{"cusip": cusip, "instrument_type": "EQUITY"}]
        state = empty_source_state()
        state["updated_at"] = "2026-08-01T00:00:00Z"
        initial_master = pipeline.rebuild_sec_security_master(state, universe)
        original = RefreshResult(
            master=initial_master,
            state=state,
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )
        discovery = DiscoveryResult(
            sources=(),
            diagnostics=(
                DiscoveryDiagnostic(
                    cusip=cusip,
                    status="no_evidence",
                    terminal=True,
                    reason="no_exact_schedule_cusip",
                ),
            ),
            fetched_sources=(),
        )
        pre_application = "1" * 64
        post_application = "2" * 64
        candidate_results = [
            ([cusip], {cusip: pre_application}),
            ([], {cusip: post_application}),
            ([], {cusip: post_application}),
        ]
        real_rebuild = pipeline.rebuild_sec_security_master

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            with (
                mock.patch.object(
                    pipeline,
                    "_sec_edgar_discovery_candidates",
                    side_effect=candidate_results,
                ) as select_candidates,
                mock.patch.object(
                    pipeline,
                    "discover_sec_edgar_sources",
                    return_value=discovery,
                ),
                mock.patch.object(
                    pipeline,
                    "rebuild_sec_security_master",
                    wraps=real_rebuild,
                ) as rebuild,
                mock.patch.object(
                    pipeline,
                    "audit_security_master",
                    return_value={"ok": True, "issues": []},
                ),
            ):
                refreshed = pipeline._refresh_sec_edgar_exceptions(
                    original,
                    universe,
                    refreshed_at=self.CURRENT,
                    fetcher=object(),
                    master_path=master_path,
                    source_state_path=state_path,
                )

            persisted_master, persisted_state = (
                pipeline.load_security_master_pair(
                    master_path=master_path,
                    source_state_path=state_path,
                )
            )

        self.assertEqual(2, rebuild.call_count)
        self.assertEqual(3, select_candidates.call_count)
        self.assertEqual(
            post_application,
            persisted_state["edgar_discovery"]["records"][cusip][
                "record_sha256"
            ],
        )
        expected_digest = source_state_sha256(persisted_state)
        self.assertEqual(expected_digest, persisted_master["source_state_sha256"])
        self.assertEqual(expected_digest, refreshed.master["source_state_sha256"])

    def test_unstable_post_application_fingerprint_is_not_published(self) -> None:
        cusip = self.CUSIPS[0]
        original = self.result()
        fingerprints = ("1" * 64, "2" * 64, "3" * 64)
        candidate_results = [
            ([cusip], {cusip: fingerprints[0]}),
            ([], {cusip: fingerprints[1]}),
            ([], {cusip: fingerprints[2]}),
        ]
        with (
            mock.patch.object(
                pipeline,
                "_sec_edgar_discovery_candidates",
                side_effect=candidate_results,
            ) as select_candidates,
            mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                return_value=self.discovery_for(cusip),
            ),
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=original.master,
            ) as rebuild,
            mock.patch.object(pipeline, "save_security_master_pair") as save_pair,
            self.assertRaisesRegex(
                pipeline.SecurityMasterRefreshError,
                "fingerprints did not reach a stable state",
            ),
        ):
            pipeline._refresh_sec_edgar_exceptions(
                original,
                [{"cusip": cusip, "instrument_type": "EQUITY"}],
                refreshed_at=self.CURRENT,
                fetcher=object(),
                _candidate_cusips=(cusip,),
            )

        self.assertEqual(3, select_candidates.call_count)
        self.assertEqual(2, rebuild.call_count)
        save_pair.assert_not_called()

    def test_interrupted_batches_resume_without_refetching_committed_prefix(
        self,
    ) -> None:
        first_calls: list[tuple[str, ...]] = []

        def interrupted_discover(batch, *, fetcher):
            del fetcher
            first_calls.append(tuple(batch))
            if batch[0] == self.CUSIPS[1]:
                raise KeyboardInterrupt
            return self.discovery_for(batch[0])

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            original, patches = self.run_checkpointed_refresh(
                root,
                discover_side_effect=interrupted_discover,
            )
            with patches[0], patches[1], patches[2], patches[3] as rebuild, patches[
                4
            ], patches[5] as save_pair:
                with self.assertRaises(KeyboardInterrupt):
                    pipeline._refresh_sec_edgar_exceptions(
                        original,
                        self.universe(),
                        refreshed_at=self.CURRENT,
                        fetcher=object(),
                        checkpoint_batches=True,
                        checkpoint_root=root,
                    )
            self.assertEqual(
                [(self.CUSIPS[0],), (self.CUSIPS[1],)],
                first_calls,
            )
            self.assertTrue(pipeline._sec_edgar_journal_path(root, 0).is_file())
            self.assertFalse(pipeline._sec_edgar_journal_path(root, 1).exists())
            rebuild.assert_not_called()
            save_pair.assert_not_called()

            resumed_calls: list[tuple[str, ...]] = []

            def resumed_discover(batch, *, fetcher):
                del fetcher
                resumed_calls.append(tuple(batch))
                return self.discovery_for(batch[0])

            original, patches = self.run_checkpointed_refresh(
                root,
                discover_side_effect=resumed_discover,
            )
            with patches[0], patches[1], patches[2], patches[3] as rebuild, patches[
                4
            ], patches[5] as save_pair:
                refreshed = pipeline._refresh_sec_edgar_exceptions(
                    original,
                    self.universe(),
                    refreshed_at=self.CURRENT,
                    fetcher=object(),
                    checkpoint_batches=True,
                    checkpoint_root=root,
                )

            self.assertEqual([(self.CUSIPS[1],)], resumed_calls)
            self.assertEqual(2, rebuild.call_count)
            self.assertEqual(1, save_pair.call_count)
            self.assertEqual(
                set(self.CUSIPS),
                set(refreshed.state["edgar_discovery"]["records"]),
            )

    def test_tampered_journal_is_discarded_and_refetched(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stale = pipeline._sec_edgar_journal_path(root, 0)
            stale.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "sequence": 0,
                        "entry_sha256": "0" * 64,
                    }
                ),
                encoding="utf-8",
            )
            calls: list[tuple[str, ...]] = []

            def discover(batch, *, fetcher):
                del fetcher
                calls.append(tuple(batch))
                return self.discovery_for(batch[0])

            original, patches = self.run_checkpointed_refresh(
                root,
                discover_side_effect=discover,
            )
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[
                5
            ]:
                pipeline._refresh_sec_edgar_exceptions(
                    original,
                    self.universe(),
                    refreshed_at=self.CURRENT,
                    fetcher=object(),
                    checkpoint_batches=True,
                    checkpoint_root=root,
                )

            self.assertEqual(
                [(self.CUSIPS[0],), (self.CUSIPS[1],)],
                calls,
            )
            repaired = json.loads(stale.read_text(encoding="utf-8"))
            self.assertNotEqual("0" * 64, repaired["entry_sha256"])

    def test_compatible_workspace_preserves_journal_without_large_pair(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sec-security-master-rebuild-work"
            missing = Path(tmpdir) / "missing.json"
            production_master = pipeline.load_security_master(missing)
            production_state = pipeline.load_source_state(missing)
            with mock.patch.object(
                pipeline,
                "SEC_SECURITY_MASTER_REBUILD_WORK_ROOT",
                root,
            ):
                prepared, complete = pipeline._prepare_security_master_rebuild_work(
                    self.universe(),
                    production_master=production_master,
                    production_state=production_state,
                    current=self.CURRENT,
                )
                self.assertFalse(complete)
                with mock.patch.object(
                    pipeline,
                    "discover_sec_edgar_sources",
                    return_value=self.discovery_for(self.CUSIPS[0]),
                ):
                    entry = pipeline._sec_edgar_batch_journal_payload(
                        [self.CUSIPS[0]],
                        self.fingerprints(),
                        sequence=0,
                        prior_entry_sha256=None,
                        current=self.CURRENT,
                        discovery_fetcher=object(),
                    )
                pipeline._append_sec_edgar_batch_journal(prepared, entry)

                resumed, complete = pipeline._prepare_security_master_rebuild_work(
                    self.universe(),
                    production_master=production_master,
                    production_state=production_state,
                    current=self.CURRENT,
                )

            self.assertEqual(root, resumed)
            self.assertFalse(complete)
            self.assertTrue(pipeline._sec_edgar_journal_path(root, 0).is_file())
            self.assertFalse((root / "sec_source_state.json").exists())
            self.assertFalse((root / "sec_security_master.json").exists())

    def test_workspace_rejects_symlinked_immediate_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            outside = base / "outside"
            outside.mkdir()
            cache_link = base / "cache"
            cache_link.symlink_to(outside, target_is_directory=True)
            root = cache_link / "sec-security-master-rebuild-work"
            missing = base / "missing.json"
            production_master = pipeline.load_security_master(missing)
            production_state = pipeline.load_source_state(missing)

            with (
                mock.patch.object(
                    pipeline,
                    "SEC_SECURITY_MASTER_REBUILD_WORK_ROOT",
                    root,
                ),
                self.assertRaisesRegex(
                    pipeline.SecurityMasterRefreshError,
                    "workspace parent cannot be a symlink",
                ),
            ):
                pipeline._prepare_security_master_rebuild_work(
                    self.universe(),
                    production_master=production_master,
                    production_state=production_state,
                    current=self.CURRENT,
                )

            self.assertEqual([], list(outside.iterdir()))

    def test_workspace_manifest_and_journal_have_private_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir) / "cache"
            parent.mkdir()
            root = parent / "sec-security-master-rebuild-work"
            root.mkdir(mode=0o755)
            orphaned_temp = (
                root / ".edgar-exception-batch-000000.json.abcd1234.tmp"
            )
            orphaned_temp.write_text("partial", encoding="utf-8")
            missing = Path(tmpdir) / "missing.json"
            production_master = pipeline.load_security_master(missing)
            production_state = pipeline.load_source_state(missing)
            with mock.patch.object(
                pipeline,
                "SEC_SECURITY_MASTER_REBUILD_WORK_ROOT",
                root,
            ):
                prepared, complete = pipeline._prepare_security_master_rebuild_work(
                    self.universe(),
                    production_master=production_master,
                    production_state=production_state,
                    current=self.CURRENT,
                )
            self.assertFalse(complete)
            self.assertEqual(0o700, prepared.stat().st_mode & 0o7777)
            self.assertEqual(
                0o600,
                (prepared / "manifest.json").stat().st_mode & 0o7777,
            )
            self.assertFalse(orphaned_temp.exists())

            with mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                return_value=self.discovery_for(self.CUSIPS[0]),
            ):
                entry = pipeline._sec_edgar_batch_journal_payload(
                    [self.CUSIPS[0]],
                    self.fingerprints(),
                    sequence=0,
                    prior_entry_sha256=None,
                    current=self.CURRENT,
                    discovery_fetcher=object(),
                )
            pipeline._append_sec_edgar_batch_journal(prepared, entry)
            self.assertEqual(
                0o600,
                pipeline._sec_edgar_journal_path(prepared, 0).stat().st_mode
                & 0o7777,
            )

    def test_atomic_json_write_removes_exclusive_temp_after_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "authoritative.json"
            original = b'{"generation":"prior"}'
            path.write_bytes(original)

            with (
                mock.patch.object(
                    pipeline.json,
                    "dump",
                    side_effect=KeyboardInterrupt("render interrupted"),
                ),
                self.assertRaisesRegex(KeyboardInterrupt, "render interrupted"),
            ):
                pipeline._atomic_write_json(path, {"generation": "refreshed"})

            self.assertEqual(original, path.read_bytes())
            self.assertEqual([path], list(root.iterdir()))

    def test_pair_persistence_delegates_one_bound_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            master_path = root / "master.json"
            prior = SimpleNamespace(
                state={"generation": "prior"},
                master={"generation": "prior"},
            )
            refreshed = SimpleNamespace(
                state={"generation": "refreshed"},
                master={"generation": "refreshed"},
            )
            with mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair:
                pipeline._persist_sec_edgar_result_pair(
                    prior,
                    refreshed,
                    master_path=master_path,
                    source_state_path=state_path,
                )
            save_pair.assert_called_once_with(
                refreshed.master,
                refreshed.state,
                master_path=master_path,
                source_state_path=state_path,
            )

    def test_pair_persistence_preserves_transaction_interruption(self) -> None:
        prior = SimpleNamespace(state={"prior": True}, master={"prior": True})
        refreshed = SimpleNamespace(
            state={"refreshed": True},
            master={"refreshed": True},
        )
        interrupted = KeyboardInterrupt("pair transaction interrupted")
        with (
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
                side_effect=interrupted,
            ) as save_pair,
            self.assertRaisesRegex(
                KeyboardInterrupt,
                "pair transaction interrupted",
            ) as caught,
        ):
            pipeline._persist_sec_edgar_result_pair(
                prior,
                refreshed,
                master_path=Path("master.json"),
                source_state_path=Path("state.json"),
            )
        self.assertIs(interrupted, caught.exception)
        save_pair.assert_called_once()


if __name__ == "__main__":
    unittest.main()
