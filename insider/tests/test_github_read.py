import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from insider_pipeline.audit_batches import file_hash
from insider_pipeline.baseline import build
from insider_pipeline.github_read import check_remote_assets, expected_assets, read_checkpoint, validate_release
from insider_pipeline.github_stage import REPOSITORY
from insider_pipeline.inventory import canonical
import test_baseline as fixtures


class GitHubReadTests(unittest.TestCase):
    def test_published_private_release_accepts_read_only_repository_access(self):
        repository = {'full_name': REPOSITORY, 'private': True, 'permissions': {'push': False, 'pull': True}}
        release = {'tag_name': 'insider-checkpoint', 'draft': False, 'published_at': '2026-09-10', 'prerelease': True}
        validate_release(repository, release, 'insider-checkpoint')
        for change in [{'draft': True}, {'published_at': None}, {'tag_name': 'dataset-other'}]:
            with self.assertRaises(ValueError):
                validate_release(repository, {**release, **change}, 'insider-checkpoint')
        with self.assertRaises(ValueError):
            validate_release({**repository, 'private': False}, release, 'insider-checkpoint')

    def test_manifest_paths_sizes_and_remote_membership_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'baseline.json'; path.write_text('{}')
            row = {'file': 'one.zip', 'bytes': 10, 'sha256': 'a' * 64}
            manifest = {'baseline_schema': 1, 'files': [row]}
            expected = expected_assets(manifest, path)
            for change in [{'file': '../outside'}, {'bytes': True}, {'sha256': 'bad'}]:
                with self.assertRaises(ValueError):
                    expected_assets({**manifest, 'files': [{**row, **change}]}, path)
            with self.assertRaisesRegex(ValueError, 'download budget'):
                expected_assets({**manifest, 'files': [{**row, 'bytes': 1_000_000_000},
                                                      {**row, 'file': 'two.zip', 'bytes': 1_000_000_000}]}, path)
            assets = [{'name': name, 'size': item['bytes'], 'state': 'uploaded', 'digest': 'sha256:' + item['sha256']}
                      for name, item in expected.items()]
            check_remote_assets(assets, expected)
            with self.assertRaises(ValueError):
                check_remote_assets(assets[:-1], expected)
            with self.assertRaises(ValueError):
                check_remote_assets([{**assets[0], 'digest': 'sha256:' + 'b' * 64}, *assets[1:]], expected)

    def test_download_restore_and_source_audit_use_only_read_operations(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, inventory, documents = fixtures.BaselineTests().setup(base)
            bundle = base / 'baseline'; built = build(inventory, documents, bundle)
            baseline = json.loads((bundle / 'baseline.json').read_text())
            expected = expected_assets(baseline, bundle / 'baseline.json')
            assets = [{'name': name, 'size': item['bytes'], 'state': 'uploaded', 'digest': 'sha256:' + item['sha256']}
                      for name, item in expected.items()]
            def api(path, **kwargs):
                if path == 'repos/' + REPOSITORY:
                    return {'full_name': REPOSITORY, 'private': True, 'permissions': {'push': False}}
                if '/assets?' in path:
                    return assets
                if path.endswith('/releases/latest'):
                    return {'id': 1, 'tag_name': 'dataset-existing'}
                if path.endswith('/releases/tags/insider-checkpoint'):
                    return {'id': 2, 'tag_name': 'insider-checkpoint', 'draft': False, 'published_at': '2026-09-10'}
                raise AssertionError(path)
            calls = []
            def download(args):
                calls.append(args)
                self.assertEqual(args[:3], ['release', 'download', 'insider-checkpoint'])
                destination = Path(args[args.index('--dir') + 1])
                names = ['baseline.json'] if '--pattern' in args else list(expected)
                for name in names:
                    if not (destination / name).exists():
                        shutil.copyfile(bundle / name, destination / name)
                return ''
            with patch('insider_pipeline.github_read.api', side_effect=api), patch('insider_pipeline.github_read.command', side_effect=download):
                report = read_checkpoint('insider-checkpoint', built['baseline_sha256'], base / 'cloud', workers=1)
            self.assertEqual(len(calls), 2)
            self.assertTrue(report['source_audit_matches_archive'])
            self.assertTrue(report['full_restore_verified'])
            self.assertTrue(report['latest_release_unchanged'])
            self.assertEqual(report['documents'], 2)
            self.assertEqual(report['inventory_filings'], 3)
            self.assertFalse(report['cloud_daily_maintenance_active'])

    def inventory_only_case(self, base, mutate=None):
        root, inventory, documents = fixtures.BaselineTests().setup(base)
        bundle = base / 'baseline'; build(inventory, documents, bundle)
        baseline_path = bundle / 'baseline.json'
        baseline = json.loads(baseline_path.read_text())
        inventory_manifest = json.loads((bundle / 'inventory-manifest.json').read_text())
        permitted = {'baseline.json', 'inventory-manifest.json', *[part['file'] for part in inventory_manifest['parts']]}
        # Unavailable document assets may exceed the full-restore budget. This
        # mode must not read any of them or claim those documents were verified.
        baseline['files'].extend([{'file': f'large-documents-{index}.zip', 'bytes': 1_000_000_000, 'sha256': str(index) * 64}
                                  for index in (1, 2)])
        if mutate:
            mutate(baseline, bundle)
        baseline_path.write_bytes(canonical(baseline))
        expected = expected_assets(baseline, baseline_path, download_budget=None)
        with self.assertRaisesRegex(ValueError, 'download budget'):
            expected_assets(baseline, baseline_path)
        assets = [{'name': name, 'size': item['bytes'], 'state': 'uploaded', 'digest': 'sha256:' + item['sha256']}
                  for name, item in expected.items()]
        def api(path, **kwargs):
            if path == 'repos/' + REPOSITORY:
                return {'full_name': REPOSITORY, 'private': True, 'permissions': {'push': False}}
            if '/assets?' in path:
                return assets
            if path.endswith('/releases/latest'):
                return {'id': 1, 'tag_name': 'dataset-existing'}
            if path.endswith('/releases/tags/insider-checkpoint'):
                return {'id': 2, 'tag_name': 'insider-checkpoint', 'draft': False, 'published_at': '2026-09-10'}
            raise AssertionError(path)
        downloaded = set()
        def download(args):
            self.assertEqual(args[:3], ['release', 'download', 'insider-checkpoint'])
            destination = Path(args[args.index('--dir') + 1])
            names = [args[index + 1] for index, value in enumerate(args) if value == '--pattern']
            self.assertTrue(names)
            for name in names:
                self.assertIn(name, permitted)
                self.assertNotIn(name, downloaded)
                downloaded.add(name)
                shutil.copyfile(bundle / name, destination / name)
            return ''
        with patch('insider_pipeline.github_read.api', side_effect=api), patch('insider_pipeline.github_read.command', side_effect=download):
            report = read_checkpoint('insider-checkpoint', file_hash(baseline_path), base / 'cloud', inventory_only=True)
        self.assertEqual(downloaded, permitted)
        self.assertEqual(report['assets'], len(permitted))
        self.assertTrue(report['inventory_only_restore_verified'])
        self.assertTrue(report['inventory_byte_identical'])
        self.assertFalse(report['full_restore_verified'])
        self.assertFalse(report['includes_original_documents'])
        self.assertFalse(report['source_audit_performed'])
        self.assertFalse(report['collection_resume_ready'])
        self.assertFalse(report['complete_backfill'])
        self.assertEqual(file_hash(inventory / 'inventory-snapshot.sqlite3'), file_hash(base / 'cloud/restored/inventory.sqlite3'))
        self.assertFalse((base / 'cloud/restored/shards').exists())

    def test_inventory_only_mode_skips_documents_even_when_full_checkpoint_exceeds_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            self.inventory_only_case(Path(directory))

    def test_inventory_only_mode_rejects_inconsistent_coverage_or_part_declarations(self):
        def remove_part(baseline, bundle):
            baseline['files'] = [asset for asset in baseline['files'] if not asset['file'].endswith('.gz.part')]
        def change_coverage(baseline, bundle):
            baseline['inventory_filings'] += 1
        def corrupt_manifest(baseline, bundle):
            path = bundle / 'inventory-manifest.json'; path.write_bytes(path.read_bytes() + b' ')
        for mutate in (remove_part, change_coverage, corrupt_manifest):
            with self.subTest(case=mutate.__name__), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    self.inventory_only_case(Path(directory), mutate)
                self.assertFalse((Path(directory) / 'cloud/restored').exists())


if __name__ == '__main__':
    unittest.main()
