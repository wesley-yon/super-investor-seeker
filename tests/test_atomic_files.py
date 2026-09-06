import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import atomic_files
import pipeline
import sec_13f_bulk_backfill as bulk
import sec_edgar_evidence as edgar
import sec_security_master as master


class AtomicWriterCompatibilityTests(unittest.TestCase):
    def test_existing_json_formats_remain_byte_identical(self) -> None:
        payload = {"z": "café\n", "a": [float("nan"), -0.0, 1.5]}
        pretty = '{\n  "a": [\n    NaN,\n    -0.0,\n    1.5\n  ],\n  "z": "café\\n"\n}\n'.encode()
        cases = (
            (pipeline._atomic_write_json, {}, b'{"z":"caf\\u00e9\\n","a":[NaN,-0.0,1.5]}'),
            (pipeline._atomic_write_json, {"sort_keys": True}, b'{"a":[NaN,-0.0,1.5],"z":"caf\\u00e9\\n"}'),
            (pipeline._atomic_write_json, {"indent": 2, "sort_keys": True}, pretty.replace("é".encode(), b"\\u00e9").rstrip(b"\n")),
            (bulk._atomic_write_json, {}, pretty),
            (bulk._atomic_write_fund_json, {}, '{"z":"café\\n","a":[NaN,-0.0,1.5]}\n'.encode()),
            (edgar._atomic_write_json, {}, pretty),
            (master._atomic_write_json, {}, '{"a":[NaN,-0.0,1.5],"z":"café\\n"}\n'.encode()),
        )
        with tempfile.TemporaryDirectory() as temporary:
            for index, (writer, options, expected) in enumerate(cases):
                with self.subTest(writer=writer.__module__, options=options):
                    path = Path(temporary) / str(index) / "output.json"
                    writer(path, payload, **options)
                    self.assertEqual(expected, path.read_bytes())
                    self.assertEqual(0o600, path.stat().st_mode & 0o777)
                    self.assertEqual([path], list(path.parent.iterdir()))

    def test_master_still_rejects_symlink_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "target.json"
            target.write_bytes(b"unchanged")
            link = root / "master.json"
            link.symlink_to(target)
            with self.assertRaises(ValueError):
                master._atomic_write_json(link, {})
            self.assertEqual(b"unchanged", target.read_bytes())
            self.assertTrue(link.is_symlink())
            self.assertEqual({link, target}, set(root.iterdir()))

    def test_deferred_directory_sync_still_syncs_interrupted_temp_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "output.json"
            with mock.patch.object(pipeline, "_fsync_directory") as sync:
                pipeline._atomic_write_json(path, {"prior": True}, fsync_parent=False)
                sync.assert_not_called()
                with (
                    mock.patch.object(pipeline.json, "dump", side_effect=KeyboardInterrupt),
                    self.assertRaises(KeyboardInterrupt),
                ):
                    pipeline._atomic_write_json(path, {}, fsync_parent=False)
                sync.assert_called_once_with(path.parent)
            self.assertEqual(b'{"prior":true}', path.read_bytes())
            self.assertEqual([path], list(path.parent.iterdir()))

    def test_file_open_failure_closes_descriptor_and_removes_temporary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "output.json"
            primary = OSError("cannot open stream")
            with (
                mock.patch.object(atomic_files.os, "fdopen", side_effect=primary),
                mock.patch.object(atomic_files.os, "close", wraps=os.close) as close,
                self.assertRaises(OSError) as raised,
            ):
                bulk._atomic_write_json(path, {})
            self.assertIs(primary, raised.exception)
            close.assert_called_once()
            self.assertEqual([], list(path.parent.iterdir()))

    def test_directory_sync_failure_occurs_after_complete_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "output.json"
            path.write_bytes(b"prior")
            primary = OSError("directory sync failed")
            with (
                mock.patch.object(bulk, "_fsync_directory", side_effect=primary),
                self.assertRaises(OSError) as raised,
            ):
                bulk._atomic_write_fund_json(path, {"new": True})
            self.assertIs(primary, raised.exception)
            self.assertEqual(b'{"new":true}\n', path.read_bytes())
            self.assertEqual([path], list(path.parent.iterdir()))

    def test_master_cleanup_keeps_primary_interruption(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "output.json"
            path.write_bytes(b"prior")
            primary = KeyboardInterrupt("render interrupted")
            with (
                mock.patch.object(master.json, "dump", side_effect=primary),
                mock.patch.object(Path, "unlink", side_effect=OSError("cleanup failed")),
                self.assertRaises(KeyboardInterrupt) as raised,
            ):
                master._atomic_write_json(path, {})
            self.assertIs(primary, raised.exception)
            self.assertEqual(b"prior", path.read_bytes())

    def test_edgar_tolerates_unavailable_directory_open_only(self) -> None:
        with mock.patch.object(edgar.os, "open", side_effect=OSError("unavailable")):
            edgar._fsync_cache_directory(Path("unused"))
        with tempfile.TemporaryDirectory() as temporary:
            primary = OSError("directory sync failed")
            with (
                mock.patch.object(edgar.os, "fsync", side_effect=primary),
                self.assertRaises(OSError) as raised,
            ):
                edgar._fsync_cache_directory(Path(temporary))
            self.assertIs(primary, raised.exception)


if __name__ == "__main__":
    unittest.main()
