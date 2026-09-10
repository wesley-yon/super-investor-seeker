import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from insider_pipeline.github_chain import blob_name, descriptor_name, read_chain
from insider_pipeline.github_increment_stage import stage_increment
from insider_pipeline.increment import MANIFEST, build
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import capture_snapshot
import test_github_chain as fixtures


class GitHubInventoryChainTests(unittest.TestCase):
    bucket = fixtures.GitHubChainTests.bucket

    def seeded(self, base):
        root, target, bundle, pin, parent, github = fixtures.GitHubChainTests().seed(base)
        with github.patches():
            staged = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
            github.publish(self.bucket)
        return root, target, bundle, pin, staged, github

    def inventory_pairs(self, github, transport_pin):
        pairs = set()

        def visit(tag, pin, legacy=False):
            name = 'baseline.json' if legacy else descriptor_name(pin)
            pairs.add((tag, name))
            value = json.loads(github.bodies[tag][name])
            if legacy:
                nested = 'inventory-manifest.json'
            else:
                ref = value['parent']
                visit(ref['tag'], ref['sha256'], ref['layout'] == 'legacy_baseline')
                name = blob_name(value['checkpoint']['manifest_sha256'])
                pairs.add((tag, name))
                value = json.loads(github.bodies[tag][name])
                nested = blob_name(value['inventory_delta_manifest_sha256'])
            pairs.add((tag, nested))
            metadata = json.loads(github.bodies[tag][nested])
            pairs.update((tag, part['file'] if legacy else blob_name(part['sha256'])) for part in metadata['parts'])

        visit(self.bucket, transport_pin)
        return pairs

    def downloads(self, github):
        result = []
        for kind, args in github.calls:
            if kind == 'command':
                self.assertEqual(args[:2], ['release', 'download'])
                result.append((args[2], args[args.index('--pattern') + 1]))
        return result

    def test_inventory_mode_replays_every_row_without_reading_any_document_or_source(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            _, target, _, _, staged, github = self.seeded(base)
            expected = self.inventory_pairs(github, staged['transport_sha256'])
            original = copy.deepcopy(github.bodies)
            github.calls.clear(); github.push = False
            # If this historical document is requested, the download would fail.
            github.corrupt_download = next(name for name in github.bodies['insider-baseline'] if name.startswith('filings-'))
            with github.patches():
                report = read_chain(self.bucket, staged['transport_sha256'], base / 'cloud-inventory', inventory_only=True)
            self.assertEqual(set(self.downloads(github)), expected)
            self.assertEqual(len(self.downloads(github)), len(expected))
            self.assertEqual(report['assets'], len(expected))
            self.assertEqual(report['asset_bytes'], sum(len(github.bodies[tag][name]) for tag, name in expected))
            self.assertEqual(report['documents'], 3)
            self.assertEqual(report['inventory_filings'], 4)
            self.assertTrue(report['inventory_logical_state_verified'])
            self.assertTrue(report['every_inventory_row_verified'])
            self.assertTrue(report['base_inventory_byte_identical'])
            self.assertFalse(report['inventory_byte_identical'])
            for flag in ('source_audit_performed', 'full_restore_verified', 'includes_original_documents',
                         'collection_resume_ready', 'cloud_daily_maintenance_active', 'complete_backfill'):
                self.assertFalse(report[flag], flag)
            self.assertFalse((base / 'cloud-inventory/restored/shards').exists())
            self.assertFalse((base / 'cloud-inventory/restored/sources').exists())
            fixtures.fixtures.IncrementTests().assert_inventory_equal(target, base / 'cloud-inventory/restored/inventory.sqlite3')
            self.assertEqual(github.bodies, original)

    def test_multiple_increments_preserve_retry_binary_metadata_and_correction_history(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, parent_inventory, bundle, pin, staged, github = self.seeded(base)
            db = connect(root)
            db.execute("UPDATE filings SET attempts=3,status='retry',retry_after=99.125,bulk_metadata=? WHERE status='pending'", (b'\x00\xff\x01',))
            db.execute('INSERT INTO settings VALUES(?,?)', ('source_coverage_repairs', canonical({'before': '2025-09-23', 'after': '2024-12-19', 'note': '原始证据'}).decode()))
            db.commit(); db.close()
            target = base / 'second.sqlite3'; capture_snapshot(root, target)
            second_dir = base / 'second'
            second = build(root, bundle / MANIFEST, pin, parent_inventory, target, second_dir, workers=1)
            with github.patches():
                second_stage = stage_increment(second_dir, base / 'second-stage', second['increment_manifest_sha256'], staged['locator'], self.bucket, 1)
                github.calls.clear()
                report = read_chain(self.bucket, second_stage['transport_sha256'], base / 'cloud-inventory', inventory_only=True)
            self.assertEqual(report['checkpoints'], 3)
            self.assertEqual(report['inventory_queue_counts'], {'retry': 1, 'verified': 3})
            self.assertEqual(set(self.downloads(github)), self.inventory_pairs(github, second_stage['transport_sha256']))
            fixtures.fixtures.IncrementTests().assert_inventory_equal(target, base / 'cloud-inventory/restored/inventory.sqlite3')
            self.assertFalse((base / 'cloud-inventory/inventory-work/0').exists())
            self.assertFalse((base / 'cloud-inventory/inventory-work/1').exists())

    def test_inventory_budget_excludes_unrequested_originals_but_includes_every_selected_byte(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            _, _, _, _, staged, github = self.seeded(base)
            pairs = self.inventory_pairs(github, staged['transport_sha256'])
            selected_bytes = sum(len(github.bodies[tag][name]) for tag, name in pairs)
            with github.patches(), patch('insider_pipeline.github_chain.MAX_DOWNLOAD_BYTES', selected_bytes):
                report = read_chain(self.bucket, staged['transport_sha256'], base / 'fits', inventory_only=True)
                self.assertEqual(report['asset_bytes'], selected_bytes)
                with self.assertRaisesRegex(ValueError, 'chain exceeds'):
                    read_chain(self.bucket, staged['transport_sha256'], base / 'full-too-large')
            github.calls.clear()
            with github.patches(), patch('insider_pipeline.github_chain.MAX_DOWNLOAD_BYTES', selected_bytes - 1):
                with self.assertRaisesRegex(ValueError, 'chain exceeds'):
                    read_chain(self.bucket, staged['transport_sha256'], base / 'too-large', inventory_only=True)
            # Only small metadata can have been read before the whole selected
            # chain is budgeted. Neither baseline nor delta parts were downloaded.
            for tag, name in self.downloads(github):
                self.assertTrue(github.bodies[tag][name].startswith(b'{'))
            self.assertFalse((base / 'too-large/restored').exists())

    def replace_delta_metadata(self, github, transport_pin, change, update_target=False):
        transport = json.loads(github.bodies[self.bucket][descriptor_name(transport_pin)])
        outer = json.loads(github.bodies[self.bucket][blob_name(transport['checkpoint']['manifest_sha256'])])
        delta = json.loads(github.bodies[self.bucket][blob_name(outer['inventory_delta_manifest_sha256'])])
        change(delta)
        body = canonical(delta); pin = hashlib.sha256(body).hexdigest()
        github.bodies[self.bucket][blob_name(pin)] = body
        outer['inventory_delta_manifest_sha256'] = pin
        if update_target:
            outer['target_inventory_state'] = delta['target']
        next(row for row in outer['files'] if row['file'] == 'inventory-delta.json').update(sha256=pin, bytes=len(body))
        body = canonical(outer); pin = hashlib.sha256(body).hexdigest()
        github.bodies[self.bucket][blob_name(pin)] = body
        transport['checkpoint']['manifest_sha256'] = pin
        body = canonical(transport); pin = hashlib.sha256(body).hexdigest()
        github.bodies[self.bucket][descriptor_name(pin)] = body
        return pin

    def test_rebound_metadata_cannot_substitute_inventory_state_or_part_membership(self):
        cases = [
            ('coverage differs', lambda delta: delta['target'].update(state_sha256='f' * 64)),
            ('parts differ', lambda delta: delta['parts'].append(dict(delta['parts'][0]))),
            ('parts differ', lambda delta: delta['parts'][0].update(file='../escape')),
            ('baseline parent', lambda delta: delta['parent']['tables']['settings'].update(rows=999)),
        ]
        for message, change in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                _, _, _, _, staged, github = self.seeded(base)
                pin = self.replace_delta_metadata(github, staged['transport_sha256'], change)
                github.calls.clear()
                with github.patches(), self.assertRaisesRegex(ValueError, message):
                    read_chain(self.bucket, pin, base / 'failed', inventory_only=True)
                for tag, name in self.downloads(github):
                    self.assertTrue(github.bodies[tag][name].startswith(b'{'))
                self.assertFalse((base / 'failed/restored').exists())
                self.assertFalse((base / 'failed/cloud-verification.json').exists())

    def test_corrupt_selected_part_or_wrong_pin_cannot_publish_restored_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            _, _, bundle, _, staged, github = self.seeded(base)
            delta = json.loads((bundle / 'inventory-delta.json').read_text())
            github.corrupt_download = blob_name(delta['parts'][0]['sha256'])
            with github.patches():
                with self.assertRaisesRegex(ValueError, 'Downloaded checkpoint bytes'):
                    read_chain(self.bucket, staged['transport_sha256'], base / 'corrupt', inventory_only=True)
                with self.assertRaises(ValueError):
                    read_chain(self.bucket, 'f' * 64, base / 'wrong-pin', inventory_only=True)
            for name in ('corrupt', 'wrong-pin'):
                self.assertFalse((base / name / 'restored').exists())
                self.assertFalse((base / name / 'cloud-verification.json').exists())

    def test_declared_disk_capacity_is_checked_before_inventory_parts_are_downloaded(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            _, _, _, _, staged, github = self.seeded(base)
            github.calls.clear()
            with github.patches(), patch('insider_pipeline.github_chain.MIN_FREE_BYTES', 0), \
                    patch('insider_pipeline.github_chain.shutil.disk_usage', return_value=SimpleNamespace(free=1)):
                with self.assertRaisesRegex(ValueError, 'declared inventory chain'):
                    read_chain(self.bucket, staged['transport_sha256'], base / 'no-space', inventory_only=True)
            for tag, name in self.downloads(github):
                self.assertTrue(github.bodies[tag][name].startswith(b'{'))
            self.assertFalse((base / 'no-space/restored').exists())


if __name__ == '__main__':
    unittest.main()
