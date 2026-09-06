import base64
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.apply_incremental_benchmark_batch import apply_batch, BATCH_NAME


class BenchmarkBatchTests(unittest.TestCase):
    def test_checks_entire_batch_before_mutating_and_applies_exact_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'data/funds').mkdir(parents=True)
            (root / '.cache').mkdir()
            path = root / 'data/funds/1.json'
            original = b'{"cik":1,"quarters":[]}\n'
            incoming = b'{"cik":1,"quarters":[{"report_date":"2026-06-30"}]}\n'
            path.write_bytes(original)
            entry = {'filename': '1.json', 'baseline_sha256': hashlib.sha256(original).hexdigest(),
                     'incoming_sha256': hashlib.sha256(incoming).hexdigest(),
                     'content_base64': base64.b64encode(incoming).decode()}
            def write(entries, source='a' * 40):
                (root / '.cache' / BATCH_NAME).write_bytes(gzip.compress(json.dumps(
                    {'schema_version': 1, 'source_sha': source, 'funds': entries}).encode()))
            for bad in ({**entry, 'filename': '../outside.json'},
                        {**entry, 'incoming_sha256': 'b' * 64},
                        {**entry, 'baseline_sha256': 'b' * 64}):
                write([entry, bad] if bad['filename'].startswith('../') else [bad])
                with self.assertRaises(ValueError):
                    apply_batch(root, 'a' * 40)
                self.assertEqual(original, path.read_bytes())
            write([entry], source='b' * 40)
            with self.assertRaisesRegex(ValueError, 'tested code'):
                apply_batch(root, 'a' * 40)
            self.assertEqual(original, path.read_bytes())
            write([entry])
            self.assertEqual(1, apply_batch(root, 'a' * 40)['funds_applied'])
            self.assertEqual(incoming, path.read_bytes())


if __name__ == '__main__':
    unittest.main()
