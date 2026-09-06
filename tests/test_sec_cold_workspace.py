import json
import io
from pathlib import Path
import tempfile
import unittest

from scripts.prepare_sec_cold_workspace import compact_bytes, compact_stream, prepare
import sec_security_master as sec


class ColdWorkspaceTests(unittest.TestCase):
    def workspace(self, root, *, pair=True):
        (root / 'data/funds').mkdir(parents=True)
        (root / 'data/stocks').mkdir()
        (root / 'data/funds/1.json').write_text('{\n  "value": 12.300, "shares": 0, "label": "a  b"\n}\n')
        (root / 'data/stocks/ABC.json').write_text('{"shares": 0}\n')
        if pair:
            state = sec.empty_source_state()
            state['updated_at'] = '2026-09-06T02:00:00Z'
            master = sec.rebuild_security_master(state, [])
            sec.save_security_master_pair(master, state,
                master_path=root / '.cache/sec_security_master.json',
                source_state_path=root / '.cache/sec_source_state.json')
            for name in ('sec_security_master.json', 'sec_source_state.json'):
                path = root / '.cache' / name
                path.write_text(json.dumps(json.loads(path.read_text()), indent=2) + '\n')

    def test_compaction_preserves_string_escapes_and_number_tokens(self):
        raw = rb'{ "text": "a  b\n\" c \\ d", "amount": 1.2300e+04 }'
        compact = compact_bytes(raw)
        self.assertEqual(json.loads(raw), json.loads(compact))
        self.assertIn(b'1.2300e+04', compact)
        self.assertIn(b'a  b', compact)
        self.assertEqual(compact, compact_bytes(compact))

    def test_streaming_preserves_tokens_across_every_small_chunk_boundary(self):
        raw = rb'{ "text": "a  b\n\" c \\ d", "amount": 1.2300e+04, "nested": [ true, null, "\u00e9" ] }'
        for size in range(1, len(raw) + 1):
            with self.subTest(chunk_size=size):
                output = io.BytesIO()
                compact_stream(io.BytesIO(raw), output, chunk_size=size)
                self.assertEqual(output.getvalue(), compact_bytes(raw))

    def test_cold_preparation_preserves_every_fund_and_pair_value(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.workspace(root)
            files = [root / 'data/funds/1.json', *(root / '.cache').glob('*.json')]
            before = {path: json.loads(path.read_text()) for path in files}
            report = prepare(root, discard_derived_stocks=True)
            self.assertEqual(before, {path: json.loads(path.read_text()) for path in files})
            self.assertIn(b'12.300', (root / 'data/funds/1.json').read_bytes())
            self.assertTrue(report['fund_json_tokens_preserved'])
            self.assertLess(report['fund_bytes_after'], report['fund_bytes_before'])
            self.assertLess(report['master_pair_bytes_after'], report['master_pair_bytes_before'])
            self.assertEqual(report['discarded_derived_stock_files'], 1)
            self.assertEqual(list((root / 'data/stocks').iterdir()), [])
            sec.load_security_master_pair(master_path=root / '.cache/sec_security_master.json',
                source_state_path=root / '.cache/sec_source_state.json')

    def test_invalid_later_fund_fails_before_mutation_or_stock_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.workspace(root)
            fund = root / 'data/funds/1.json'
            before = fund.read_bytes()
            (root / 'data/funds/2.json').write_text('{broken')
            with self.assertRaises(json.JSONDecodeError):
                prepare(root, discard_derived_stocks=True)
            self.assertEqual(fund.read_bytes(), before)
            self.assertTrue((root / 'data/stocks/ABC.json').exists())

    def test_invalid_pair_fails_before_fund_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.workspace(root)
            fund = root / 'data/funds/1.json'
            before = fund.read_bytes()
            master = root / '.cache/sec_security_master.json'
            payload = json.loads(master.read_text())
            payload['source_state_sha256'] = '0' * 64
            master.write_text(json.dumps(payload))
            with self.assertRaises(sec.SecurityMasterError):
                prepare(root, discard_derived_stocks=True)
            self.assertEqual(fund.read_bytes(), before)
            self.assertTrue((root / 'data/stocks/ABC.json').exists())

    def test_stock_symlink_is_rejected_and_legacy_absent_pair_is_supported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.workspace(root, pair=False)
            outside = root / 'outside.json'
            outside.write_text('{"keep": true}')
            link = root / 'data/stocks/link.json'
            link.symlink_to(outside)
            with self.assertRaises(ValueError):
                prepare(root, discard_derived_stocks=True)
            self.assertTrue(outside.exists())
            link.unlink()
            report = prepare(root, discard_derived_stocks=False)
            self.assertFalse(report['master_pair_present'])
            self.assertFalse(report['regeneration_required'])
            self.assertTrue((root / 'data/stocks/ABC.json').exists())


if __name__ == '__main__':
    unittest.main()
