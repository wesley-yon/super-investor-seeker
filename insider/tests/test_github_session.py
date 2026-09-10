from collections import Counter
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from insider_pipeline import github_documents
from insider_pipeline.audit_batches import readonly
from insider_pipeline.github_chain import blob_name, restore_parent_inventory
from insider_pipeline.github_session import open_inventory
from insider_pipeline.restore import update_digest
import test_github_documents as fixtures
import test_increment as increments


class GitHubSessionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.fixture = fixtures.GitHubDocumentTests()
        (self.root, self.target, self.bundle, self.index, self.staged,
         self.published, self.github) = self.fixture.seed(self.base)
        self.github.push = False

    def open(self, name='cloud', **kwargs):
        return open_inventory(self.fixture.bucket, self.staged['transport_sha256'], self.base / name, **kwargs)

    def downloads(self):
        return self.fixture.downloads(self.github)

    def chunks(self):
        return {(tag, name) for tag, values in self.github.bodies.items() for name, body in values.items()
                if body.startswith(b'PK') and b'filings/' in body[:200]}

    def test_inventory_then_local_recovery_downloads_once_and_matches_every_selected_blob(self):
        with self.github.patches(), patch('insider_pipeline.github_chain.restore_parent_inventory', wraps=restore_parent_inventory) as replay:
            session = self.open(source_quarters=['2026Q1'])
            before = Counter(self.downloads())
            self.assertFalse(set(before) & self.chunks())
            accessions = [self.fixture.accession, '0001234567-26-000001']
            report = session.recover(accessions, document_index_pin=self.published['document_index_sha256'],
                                     source_quarters=['2026Q1', '2026Q2'])
            self.assertEqual(replay.call_count, 1)
        self.assertTrue(report['adaptive_recovery_verified'] and report['parent_inventory_unchanged'])
        self.assertEqual(report['selected_documents_verified'], 2)
        self.assertEqual(report['selected_source_files_verified'], 3)
        self.assertEqual(report['selected_source_files_reused'], 2)
        calls = Counter(self.downloads())
        self.assertTrue(all(count == 1 for count in calls.values()))
        self.assertTrue(all(calls[key] == 1 for key in before))
        self.assertNotIn((self.fixture.bucket, blob_name(self.published['filing_selection_sha256'])), calls)
        self.assertEqual(report['asset_bytes'], sum(len(self.github.bodies[tag][name]) for tag, name in calls))
        self.assertEqual(report['recovery_added_assets'], len(calls) - len(before))
        increments.IncrementTests().assert_inventory_equal(self.target, session.root / 'inventory.sqlite3')
        db = readonly(self.target)
        digest = hashlib.sha256()
        for accession in sorted(accessions):
            inventory = dict(db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
            original = readonly(self.root / inventory['shard']); recovered = readonly(session.root / inventory['shard'])
            expected = dict(original.execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone())
            actual = dict(recovered.execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone())
            self.assertEqual(expected, actual)
            update_digest(digest, inventory, actual)
            original.close(); recovered.close()
        db.close()
        self.assertEqual(report['selected_documents_rows_sha256'], digest.hexdigest())
        for quarter in ('2026Q1', '2026Q2'):
            self.assertEqual((session.root / (quarter + '.zip')).read_bytes(), (self.root / (quarter + '.zip')).read_bytes())
        self.assertFalse(report['collection_resume_ready'] or report['source_audit_performed'] or report['complete_backfill'])
        self.assertNotIn(accessions[0], json.dumps(report))
        with self.assertRaisesRegex(ValueError, 'only one final selection'):
            session.recover([])

    def test_empty_selection_needs_no_private_index_or_document_download(self):
        with self.github.patches():
            session = self.open()
            calls = list(self.downloads())
            report = session.recover([], document_index_pin='a' * 64)
        self.assertEqual(self.downloads(), calls)
        self.assertEqual(report['recovery_added_assets'], 0)
        self.assertEqual(report['selected_documents_verified'], 0)
        self.assertFalse((session.output / 'document-index').exists())
        self.assertFalse((session.root / 'shards').exists())

    def test_mutated_parent_stops_before_any_additional_remote_reads(self):
        with self.github.patches():
            session = self.open()
            before = list(self.github.calls)
            db = sqlite3.connect(session.root / 'inventory.sqlite3')
            db.execute("UPDATE filings SET attempts=99 WHERE status='pending'"); db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'parent inventory changed'):
                session.recover([self.fixture.accession], document_index_pin=self.published['document_index_sha256'])
        self.assertEqual(self.github.calls, before)
        self.assertFalse((session.root / 'shards').exists())
        self.assertFalse((session.output / 'cloud-verification.json').exists())

    def test_corrupt_download_cache_is_rechecked_before_reuse(self):
        with self.github.patches():
            session = self.open(source_quarters=['2026Q1'])
            key, path = next((key, path) for key, path in session.downloader.downloaded.items()
                             if path.read_bytes().startswith(b'PK'))
            body = path.read_bytes(); path.write_bytes(bytes([body[0] ^ 1]) + body[1:])
            before = list(self.downloads())
            with self.assertRaisesRegex(ValueError, 'Cached checkpoint bytes'):
                session.recover([], source_quarters=['2026Q1'])
        self.assertEqual(self.downloads(), before)
        self.assertFalse((session.output / 'cloud-verification.json').exists())
        with self.assertRaisesRegex(ValueError, 'only one final selection'):
            session.recover([])

    def test_corrupt_materialized_source_and_metadata_fail_before_payload_download(self):
        for suffix in ('body', 'json'):
            with self.subTest(suffix=suffix), self.github.patches():
                session = self.open(suffix, source_quarters=['2026Q1'])
                path = next((session.root / 'sources/indexes').glob('*.' + suffix))
                path.write_bytes(b'corrupt')
                before = list(self.downloads())
                with self.assertRaises(ValueError):
                    session.recover([], source_quarters=['2026Q1'])
                self.assertEqual(self.downloads(), before)
                self.assertFalse((session.output / 'cloud-verification.json').exists())

    def test_combined_budget_accounts_for_initial_inventory_before_new_chunks(self):
        with self.github.patches():
            session = self.open()
            session.downloader.budget = sum(size for size, _ in session.downloader.planned.values()) + 1
            before = list(self.downloads())
            with self.assertRaisesRegex(ValueError, 'download budget'):
                session.recover([self.fixture.accession], document_index_pin=self.published['document_index_sha256'])
        self.assertEqual(self.downloads(), before)
        self.assertFalse((session.output / 'cloud-verification.json').exists())

    def test_disk_limit_rejects_before_document_chunks(self):
        with self.github.patches():
            session = self.open()
            with patch('insider_pipeline.github_session.shutil.disk_usage', return_value=SimpleNamespace(free=0)):
                with self.assertRaisesRegex(ValueError, 'free disk for adaptive'):
                    session.recover([self.fixture.accession], document_index_pin=self.published['document_index_sha256'])
        self.assertFalse(set(self.downloads()) & self.chunks())
        self.assertFalse((session.output / 'cloud-verification.json').exists())

    def test_corrupt_unselected_index_row_rejects_before_document_chunks(self):
        changed = self.fixture.rewrite_index(self.github, self.published,
                    change_rows=lambda rows, _: rows[-1].__setitem__(1, '0' * 64))
        with self.github.patches():
            session = self.open()
            with self.assertRaisesRegex(ValueError, 'complete committed inventory rows'):
                session.recover([self.fixture.accession], document_index_pin=changed['document_index_sha256'])
        self.assertFalse(set(self.downloads()) & self.chunks())
        self.assertFalse((session.output / 'cloud-verification.json').exists())

    def test_unknown_pending_document_and_invalid_selection_do_not_fetch_chunks(self):
        for number, accessions in enumerate(([self.fixture.accession] * 2, ['../escape'], ['0001234567-26-000003'])):
            with self.subTest(accessions=accessions), self.github.patches():
                session = self.open('invalid-' + str(number))
                with self.assertRaises(ValueError):
                    session.recover(accessions, document_index_pin=self.published['document_index_sha256'])
                self.assertFalse(set(self.downloads()) & self.chunks())

    def test_local_list_is_bounded_without_the_small_manual_selection_limit(self):
        values = [f'0001234567-26-{number:06d}' for number in range(1001)]
        self.assertEqual(len(github_documents.local_selection('a' * 64, values)['accessions']), 1001)
        with patch('insider_pipeline.github_chain.MAX_METADATA_BYTES', 100), self.assertRaisesRegex(ValueError, 'bounded metadata size'):
            github_documents.local_selection('a' * 64, values)


class GitHubSessionRefreshTests(unittest.TestCase):
    def setUp(self):
        import test_discovery_refresh as refresh
        from insider_pipeline.audit_batches import run as audit
        from insider_pipeline.baseline import build as baseline
        from insider_pipeline.document_index import build as build_index
        from insider_pipeline.github_document_index_stage import stage_index
        from insider_pipeline.github_increment_stage import stage_increment
        from insider_pipeline.increment import build as increment
        from insider_pipeline.inventory_archive import archive, capture_snapshot
        from insider_pipeline.package import package
        from test_github_chain import FakeGitHub
        self.fixture = refresh.DiscoveryRefreshTests()
        self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.base, self.root = self.fixture.base, self.fixture.root
        self.github = FakeGitHub()
        self.bucket = 'insider-archives-202609-001'
        with contextlib.redirect_stdout(io.StringIO()):
            archive(self.root, self.base / 'inventory-archive')
            snapshot = self.base / 'inventory-archive/inventory-snapshot.sqlite3'
            audit(self.root, self.base / 'parent-audit', workers=1, inventory_snapshot=snapshot)
            package(self.root, self.base / 'parent-audit', self.base / 'parent-documents')
            first = baseline(self.base / 'inventory-archive', self.base / 'parent-documents', self.base / 'baseline')
            self.target = self.base / 'target.sqlite3'
            capture_snapshot(self.root, self.target)
            second = increment(self.root, self.base / 'baseline/baseline.json', first['baseline_sha256'],
                                snapshot, self.target, self.base / 'increment', workers=1)
            indexed = build_index(self.base / 'increment/increment.json', second['increment_manifest_sha256'],
                                  self.target, self.base / 'index', [self.base / 'baseline/baseline.json'])
            value = json.loads((self.base / 'baseline/baseline.json').read_text())
            names = [asset['file'] for asset in value['files']] + ['baseline.json']
            self.github.add('insider-baseline', {name: (self.base / 'baseline' / name).read_bytes() for name in names})
            parent = {'layout': 'legacy_baseline', 'tag': 'insider-baseline', 'sha256': first['baseline_sha256']}
            with self.github.patches():
                self.staged = stage_increment(self.base / 'increment', self.base / 'stage', second['increment_manifest_sha256'],
                                               parent, self.bucket, 1)
                self.github.publish(self.bucket)
                self.published = stage_index(self.base / 'index', self.base / 'index-stage', indexed['document_index_manifest_sha256'],
                                              [refresh.ACCESSION], self.target, self.bucket, self.staged['transport_sha256'], 1)
        self.github.calls.clear(); self.github.push = False

    def test_new_discovery_selects_inherited_originals_then_materializes_without_replay(self):
        import test_discovery_refresh as refresh
        from insider_pipeline.discovery_refresh import materialize, prepare
        with self.github.patches(), patch('insider_pipeline.github_chain.restore_parent_inventory', wraps=restore_parent_inventory) as replay:
            session = open_inventory(self.bucket, self.staged['transport_sha256'], self.base / 'cloud', source_quarters=['2026Q2', '2026Q3'])
            plan = prepare(session.root / 'inventory.sqlite3', self.base / 'plan', '2026-09-04',
                           client=refresh.SourceClient(self.fixture.changed))
            self.assertEqual(plan['committed_documents_to_restore'], 2)
            recovered = session.recover_refresh(self.base / 'plan', plan['plan_sha256'],
                                                 document_index_pin=self.published['document_index_sha256'])
            result = materialize(self.base / 'plan', plan['plan_sha256'], session.root, self.base / 'candidate')
            self.assertEqual(replay.call_count, 1)
        self.assertTrue(recovered['discovery_plan_parent_verified'])
        self.assertEqual(recovered['filing_selection_source'], 'local_discovery_plan')
        self.assertEqual(recovered['selected_documents_verified'], 2)
        self.assertEqual(result['new_filings'], 1)
        self.assertEqual(result['rechecked_committed_documents'], 2)
        increments.IncrementTests().assert_inventory_equal(self.target, session.root / 'inventory.sqlite3')
        calls = Counter(fixtures.GitHubDocumentTests().downloads(self.github))
        self.assertTrue(all(count == 1 for count in calls.values()))
        self.assertFalse(any(kind == 'command' and args[:2] in (['release', 'upload'], ['release', 'create'])
                             for kind, args in self.github.calls))
        for accession in (refresh.ACCESSION, refresh.REMOVED):
            _, original = self.fixture.record(self.root, accession)
            _, candidate = self.fixture.record(self.base / 'candidate', accession)
            for field in ('source_gzip', 'source_sha256', 'source_bytes', 'parsed_gzip', 'parsed_sha256', 'parsed_bytes', 'fetched_at'):
                self.assertEqual(candidate[field], original[field])

    def test_empty_discovery_needs_no_document_index_and_wrong_plan_parent_rejects(self):
        from insider_pipeline.audit_batches import file_hash
        from insider_pipeline.discovery_refresh import MANIFEST, prepare
        from insider_pipeline.inventory import canonical
        with self.github.patches():
            session = open_inventory(self.bucket, self.staged['transport_sha256'], self.base / 'cloud', source_quarters=['2026Q2', '2026Q3'])
            plan = prepare(session.root / 'inventory.sqlite3', self.base / 'plan', '2026-09-04', cache=session.root / 'sources/indexes')
            self.assertEqual(plan['committed_documents_to_restore'], 0)
            manifest = self.base / 'plan' / MANIFEST
            original = manifest.read_bytes(); value = json.loads(original)
            value['parent_inventory_state']['state_sha256'] = '0' * 64
            manifest.write_bytes(canonical(value))
            before = list(self.github.calls)
            with self.assertRaisesRegex(ValueError, 'plan parent differs'):
                session.recover_refresh(self.base / 'plan', file_hash(manifest))
            self.assertEqual(self.github.calls, before)
            self.assertFalse((session.output / 'cloud-verification.json').exists())
            with self.assertRaisesRegex(ValueError, 'only one final selection'):
                session.recover_refresh(self.base / 'plan', plan['plan_sha256'])
            manifest.write_bytes(original)
            session = open_inventory(self.bucket, self.staged['transport_sha256'], self.base / 'empty', source_quarters=['2026Q2', '2026Q3'])
            result = session.recover_refresh(self.base / 'plan', plan['plan_sha256'])
        self.assertEqual(result['selected_documents_verified'], 0)
        self.assertEqual(result['recovery_added_assets'], 0)
        self.assertFalse((session.output / 'document-index').exists())
        self.assertFalse((session.root / 'shards').exists())


if __name__ == '__main__':
    unittest.main()
