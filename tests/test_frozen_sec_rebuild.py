import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import frozen_sec_rebuild
from scripts.frozen_sec_rebuild import FrozenInputError, FrozenResponses


class FrozenResponseTests(unittest.TestCase):
    def test_shared_builder_changes_invalidate_frozen_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in frozen_sec_rebuild.code_hashes():
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((frozen_sec_rebuild.ROOT / name).read_bytes())
            with mock.patch.object(frozen_sec_rebuild, 'ROOT', root):
                original = frozen_sec_rebuild.code_hashes()
                for name in ('sec_http.py', 'atomic_files.py'):
                    with self.subTest(module=name):
                        target = root / name
                        raw = target.read_bytes()
                        target.write_bytes(raw + b'\n# changed builder dependency\n')
                        changed = frozen_sec_rebuild.code_hashes()
                        self.assertEqual(
                            [key for key in original if original[key] != changed[key]],
                            [name],
                        )
                        target.write_bytes(raw)

    def test_capture_once_then_replay_identical_bytes_without_fetcher(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = FrozenResponses(root, capture=True)
            calls = []
            def fetch(url):
                calls.append(url)
                return b'original SEC response'
            self.assertEqual(archive.fetch('https://www.sec.gov/a', fetch), b'original SEC response')
            self.assertEqual(archive.fetch('https://www.sec.gov/a', fetch), b'original SEC response')
            self.assertEqual(len(calls), 1)
            (root / 'sealed.json').write_text('{}')
            replay = FrozenResponses(root, capture=False)
            self.assertEqual(replay.fetch('https://www.sec.gov/a'), b'original SEC response')
            with self.assertRaises(FrozenInputError):
                replay.fetch('https://www.sec.gov/missing', fetch)
            self.assertEqual(len(calls), 1)

    def test_corrupt_blob_cannot_be_swallowed_as_source_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = FrozenResponses(Path(directory), capture=True)
            archive.fetch('https://www.sec.gov/a', lambda _: b'original')
            entry = archive.entries['https://www.sec.gov/a']
            (archive.root / 'blobs' / entry['sha256']).write_bytes(b'modified')
            with self.assertRaises(FrozenInputError):
                try:
                    archive.fetch('https://www.sec.gov/a')
                except Exception:
                    self.fail('Integrity error was swallowed by source fallback')

    def test_failure_outcome_replays_without_persisting_private_error_text(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = FrozenResponses(root, capture=True)
            def failed(_):
                raise TimeoutError('private request details')
            with self.assertRaisesRegex(RuntimeError, 'TimeoutError'):
                archive.fetch('https://www.sec.gov/a', failed)
            self.assertNotIn('private', archive.path.read_text())
            (root / 'sealed.json').write_text(json.dumps({}))
            with self.assertRaisesRegex(RuntimeError, 'TimeoutError'):
                FrozenResponses(root, capture=False).fetch('https://www.sec.gov/a')
            with self.assertRaises(FrozenInputError):
                FrozenResponses(root, capture=True)


if __name__ == '__main__':
    unittest.main()
