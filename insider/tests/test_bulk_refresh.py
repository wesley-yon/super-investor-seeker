import contextlib
import io
import json
import sqlite3
import unittest
from unittest.mock import patch
import zipfile

from insider_pipeline import bulk_refresh as bulk
from insider_pipeline.audit_batches import file_hash, readonly, run as audit
from insider_pipeline.inventory import TABLES, canonical
from insider_pipeline.inventory_delta import schema, state
from insider_pipeline.locking import writer_lock
import test_discovery_refresh as fixtures


def quarter_body(rows, price='10.13', counts=None):
    counts = counts or {'NONDERIV_TRANS': 1, 'REPORTINGOWNER': 1, 'FOOTNOTES': 2, 'OWNER_SIGNATURE': 1}
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('SUBMISSION.tsv', 'ACCESSION_NUMBER\tISSUERCIK\tFILING_DATE\tDOCUMENT_TYPE\n' + ''.join(
            f'{accession}\t{issuer}\t{filed}\t{form}\n' for accession, issuer, filed, form in rows))
        for table in TABLES:
            values = ''.join(f'{accession}\t2.5\t{price}\n' for accession, *_ in rows for _ in range(counts.get(table, 0)))
            archive.writestr(table + '.tsv', 'ACCESSION_NUMBER\tTRANS_SHARES\tTRANS_PRICEPERSHARE\n' + values)
    return output.getvalue()


def client_for(bodies):
    return fixtures.SourceClient({bulk.bulk_url(key): body for key, body in bodies.items()})


class BulkRefreshTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.DiscoveryRefreshTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.base, self.root, self.inventory = self.fixture.base, self.fixture.root, self.fixture.inventory
        self.q3 = [(fixtures.ACCESSION, 64040, '2026-09-03', '4'), (fixtures.REMOVED, 64040, '2026-09-03', '4')]
        self.q2 = [(fixtures.PENDING, 64040, '2026-06-03', '4')]
        self.bodies = {'2026Q2': quarter_body(self.q2), '2026Q3': quarter_body(self.q3)}

    def prepare(self, name='plan', *, bodies=None, inventory=None, workers=1):
        bodies = self.bodies if bodies is None else bodies
        client = client_for(bodies)
        result = bulk.prepare(inventory or self.inventory, self.base / name, source_quarters=list(bodies), client=client, workers=workers, source_urls={key: bulk.bulk_url(key) for key in bodies})
        return self.base / name, result, client

    def seed_attached(self):
        directory, result, _ = self.prepare()
        root = self.base / 'attached'
        bulk.materialize(directory, result['plan_sha256'], self.root, root)
        return root

    def repin(self, directory, **changes):
        path = directory / bulk.MANIFEST
        value = json.loads(path.read_text()); value.update(changes); path.write_bytes(canonical(value))
        return file_hash(path)

    def test_attach_preserves_original_blobs_unselected_history_and_pending_work(self):
        before = self.fixture.rows(self.inventory); original_hash = file_hash(self.inventory)
        directory, result, client = self.prepare()
        self.assertEqual(result['changed_source_quarters'], ['2026Q2', '2026Q3'])
        self.assertEqual(result['committed_documents_to_restore'], 2)
        self.assertEqual(result['required_bulk_quarters'], [])
        self.assertEqual(sorted(client.calls), [(bulk.bulk_url(key), True) for key in sorted(self.bodies)])
        candidate = self.base / 'candidate'
        report = bulk.materialize(directory, result['plan_sha256'], self.root, candidate)
        self.assertTrue(report['bulk_metadata_refresh_verified'])
        self.assertEqual(report['rechecked_committed_documents'], 2)
        self.assertEqual(report['rechecked_documents_in_review'], 0)
        for accession in (fixtures.ACCESSION, fixtures.REMOVED):
            old_row, old = self.fixture.record(self.root, accession)
            new_row, new = self.fixture.record(candidate, accession)
            self.assertEqual(new_row['bulk_source'], '2026Q3')
            self.assertNotEqual(old_row['shard'], new_row['shard'])
            for key in ('source_gzip','source_sha256','source_bytes','parsed_gzip','parsed_sha256','parsed_bytes','fetched_at'):
                self.assertEqual(old[key], new[key], key)
        db = readonly(candidate / 'inventory.sqlite3')
        self.assertEqual(db.execute('SELECT status,bulk_source FROM filings WHERE accession=?', (fixtures.PENDING,)).fetchone()[:], ('pending','2026Q2'))
        self.assertEqual(db.execute('SELECT count(*) FROM filings').fetchone()[0], 4); db.close()
        legacy, _ = self.fixture.record(self.root, fixtures.LEGACY)
        self.assertFalse((candidate / legacy['shard']).exists())
        self.assertEqual(self.fixture.rows(self.inventory), before)
        self.assertEqual(file_hash(self.inventory), original_hash)
        self.assertFalse(report['source_audit_performed'] or report['complete_backfill'])

    def test_unchanged_zip_does_not_reopen_originals_or_change_any_inventory_rows(self):
        root = self.seed_attached(); before = self.fixture.rows(root / 'inventory.sqlite3')
        directory, result, _ = self.prepare('unchanged', inventory=root / 'inventory.sqlite3')
        self.assertEqual(result['changed_source_quarters'], [])
        self.assertEqual(result['committed_documents_to_restore'], 0)
        output = self.base / 'unchanged-candidate'
        bulk.materialize(directory, result['plan_sha256'], root, output)
        self.assertEqual(self.fixture.rows(output / 'inventory.sqlite3'), before)
        self.assertFalse((output / 'shards').exists())

    def test_value_only_change_rechecks_all_originals_despite_equal_counts_and_metadata(self):
        root = self.seed_attached()
        directory, result, _ = self.prepare('values', inventory=root / 'inventory.sqlite3', bodies={'2026Q3': quarter_body(self.q3, price='999.123456')})
        self.assertEqual(result['committed_documents_to_restore'], 2)
        self.assertEqual(result['new_filings'], 0)
        before, after = readonly(root / 'inventory.sqlite3'), readonly(directory / 'inventory.sqlite3')
        self.assertEqual([tuple(row) for row in before.execute('SELECT * FROM filings ORDER BY accession')],
                         [tuple(row) for row in after.execute('SELECT * FROM filings ORDER BY accession')])
        before.close(); after.close()
        candidate = self.base / 'value-candidate'
        bulk.materialize(directory, result['plan_sha256'], root, candidate)
        with contextlib.redirect_stdout(io.StringIO()):
            result = audit(candidate, self.base / 'value-audit', workers=1,
                           inventory_snapshot=candidate / 'inventory.sqlite3', parent_inventory=root / 'inventory.sqlite3')
        self.assertEqual(result['selected_documents'], 2)
        self.assertEqual(result['counts'].get('original_xml_field_failure', 0), 0)
        self.assertEqual(result['counts'].get('document_failure', 0), 0)
        self.assertEqual(result['financial_table_comparisons']['review'], 2)
        self.assertGreater(result['counts']['original_xml_fields_checked'], 0)
        for accession in (fixtures.ACCESSION, fixtures.REMOVED):
            _, original = self.fixture.record(root, accession); _, updated = self.fixture.record(candidate, accession)
            self.assertEqual(original['source_gzip'], updated['source_gzip'])
            self.assertEqual(original['parsed_gzip'], updated['parsed_gzip'])

    def test_removed_association_retains_original_and_prior_bulk_observation(self):
        root = self.seed_attached()
        directory, result, _ = self.prepare('removed', inventory=root / 'inventory.sqlite3', bodies={'2026Q3': quarter_body(self.q3[:1])})
        self.assertEqual(result['committed_documents_to_restore'], 2)
        db = readonly(directory / 'inventory.sqlite3')
        removed = dict(db.execute('SELECT * FROM filings WHERE accession=?', (fixtures.REMOVED,)).fetchone())
        self.assertIsNone(removed['bulk_source']); self.assertIsNone(removed['bulk_metadata']); self.assertIsNone(removed['expected_counts'])
        self.assertIn('BULK_MEMBERSHIP_REMOVED:2026Q3', removed['discovery_issues'])
        history = [json.loads(row[0]) for row in db.execute("SELECT value FROM settings WHERE key LIKE 'bulk_observation_history:%'")]
        self.assertEqual(history[0]['observation']['accession'], fixtures.REMOVED)
        self.assertIsNotNone(history[0]['observation']['bulk_metadata'])
        self.assertEqual(db.execute('SELECT count(*) FROM filings').fetchone()[0], 4); db.close()
        candidate = self.base / 'removed-candidate'; bulk.materialize(directory, result['plan_sha256'], root, candidate)
        _, old = self.fixture.record(root, fixtures.REMOVED); _, new = self.fixture.record(candidate, fixtures.REMOVED)
        self.assertEqual(old['source_gzip'], new['source_gzip']); self.assertEqual(new['status'], 'review')

    def test_cross_quarter_movement_is_order_independent_and_overlaps_are_flagged(self):
        root = self.seed_attached()
        moved = {'2026Q3': quarter_body(self.q3[1:]), '2026Q2': quarter_body(self.q2 + self.q3[:1])}
        first, a, _ = self.prepare('move-one', inventory=root / 'inventory.sqlite3', bodies=moved, workers=1)
        second, b, _ = self.prepare('move-two', inventory=root / 'inventory.sqlite3', bodies=dict(reversed(list(moved.items()))), workers=2)
        self.assertEqual(a, b)
        self.assertEqual(self.fixture.rows(first / 'inventory.sqlite3'), self.fixture.rows(second / 'inventory.sqlite3'))
        db = readonly(first / 'inventory.sqlite3')
        row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (fixtures.ACCESSION,)).fetchone())
        self.assertEqual(row['bulk_source'], '2026Q2')
        self.assertIn('BULK_MEMBERSHIP_REMOVED:2026Q3', row['discovery_issues']); db.close()
        overlap, _, _ = self.prepare('overlap', inventory=root / 'inventory.sqlite3', bodies={'2026Q2': quarter_body(self.q2 + self.q3[:1])})
        db = readonly(overlap / 'inventory.sqlite3')
        row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (fixtures.ACCESSION,)).fetchone())
        self.assertEqual(row['bulk_source'], '2026Q3')
        self.assertIn('BULK_CROSS_QUARTER_CONFLICT:2026Q2', row['discovery_issues'])
        self.assertEqual(db.execute("SELECT count(*) FROM settings WHERE key LIKE 'bulk_competing_observation:%'").fetchone()[0], 1); db.close()
        self.assertEqual(json.loads((overlap / bulk.MANIFEST).read_text())['required_bulk_quarters'], ['2026Q3'])

    def test_new_bulk_only_filing_stays_pending_and_original_identity_is_preserved(self):
        rows = [(fixtures.ACCESSION, 999, '2026-09-01', '5'), self.q3[1], (fixtures.NEW, 64040, '2026-09-03', '4')]
        directory, result, _ = self.prepare(bodies={'2026Q3': quarter_body(rows)})
        self.assertEqual(result['new_filings'], 1)
        db = readonly(directory / 'inventory.sqlite3')
        changed = dict(db.execute('SELECT * FROM filings WHERE accession=?', (fixtures.ACCESSION,)).fetchone())
        self.assertEqual((changed['issuer_cik'], changed['form'], changed['filing_date']), (64040,'4','2026-09-03'))
        self.assertIn('BULK_IDENTITY_DISAGREEMENT', changed['discovery_issues'])
        new = dict(db.execute('SELECT * FROM filings WHERE accession=?', (fixtures.NEW,)).fetchone())
        self.assertEqual(new['status'], 'pending'); self.assertIsNone(new['source_url']); db.close()

    def test_serial_parallel_source_parsing_produces_identical_plan_assets_and_rows(self):
        first, one, _ = self.prepare('serial', workers=1)
        second, two, _ = self.prepare('parallel', workers=2)
        self.assertEqual(one, two)
        for path in first.rglob('*'):
            if path.is_file():
                self.assertEqual(path.read_bytes(), (second / path.relative_to(first)).read_bytes(), path.name)

    def test_missing_original_source_mutation_and_writer_lock_do_not_publish_candidate(self):
        directory, result, _ = self.prepare()
        row, _ = self.fixture.record(self.root, fixtures.ACCESSION)
        hidden = self.root / row['shard']; hidden.rename(hidden.with_suffix('.hidden'))
        with self.assertRaisesRegex(ValueError, 'must be restored'):
            bulk.materialize(directory, result['plan_sha256'], self.root, self.base / 'missing')
        self.assertFalse((self.base / 'missing').exists())
        hidden.with_suffix('.hidden').rename(hidden)
        with writer_lock(self.root), self.assertRaisesRegex(RuntimeError, 'Another backfill writer'):
            bulk.materialize(directory, result['plan_sha256'], self.root, self.base / 'busy')
        value = json.loads((directory / bulk.MANIFEST).read_text())
        (directory / value['source_files'][0]['file']).write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'checksum or bounds'):
            bulk.materialize(directory, result['plan_sha256'], self.root, self.base / 'corrupt')
        self.assertFalse((self.base / 'corrupt').exists())

    def test_repinned_omitted_original_and_arbitrary_inventory_change_fail_replay(self):
        root = self.seed_attached()
        directory, _, _ = self.prepare('value-plan', inventory=root / 'inventory.sqlite3', bodies={'2026Q3': quarter_body(self.q3, price='777')})
        selected = directory / 'committed-accessions.json'; selected.write_bytes(canonical([fixtures.ACCESSION]))
        pin = self.repin(directory, committed_documents=1, committed_accessions_sha256=file_hash(selected))
        with self.assertRaisesRegex(ValueError, 'omitted or added an affected'):
            bulk.materialize(directory, pin, root, self.base / 'omitted')
        directory, _, _ = self.prepare('changed-target', inventory=root / 'inventory.sqlite3', bodies={'2026Q3': quarter_body(self.q3, price='888')})
        db = sqlite3.connect(directory / 'inventory.sqlite3'); db.row_factory = sqlite3.Row
        db.execute('UPDATE filings SET attempts=99 WHERE accession=?', (fixtures.LEGACY,)); db.commit()
        target_state = state(db, schema(db)); db.close()
        pin = self.repin(directory, target_inventory_state=target_state, target_inventory_file_sha256=file_hash(directory / 'inventory.sqlite3'))
        with self.assertRaisesRegex(ValueError, 'omitted or added|independently replayed'):
            bulk.materialize(directory, pin, root, self.base / 'invented')
        self.assertFalse((self.base / 'invented').exists())

    def test_invalid_quarters_malformed_zip_and_bounded_sources_leave_parent_unchanged(self):
        before = file_hash(self.inventory)
        for values in ([], ['2026Q3','2026Q3'], ['2027Q1'], ['../path']):
            with self.assertRaises(ValueError):
                bulk.prepare(self.inventory, self.base / 'invalid', source_quarters=values, client=client_for(self.bodies))
        with self.assertRaises(zipfile.BadZipFile):
            self.prepare('not-zip', bodies={'2026Q3': b'not a zip'})
        with patch.object(bulk, 'MAX_DECODED_QUARTER_BYTES', 1), self.assertRaisesRegex(ValueError, 'oversized tables'):
            self.prepare('oversized')
        with patch.object(bulk, 'MAX_QUARTER_RECORDS', 1), self.assertRaisesRegex(ValueError, 'filing-record bound'):
            self.prepare('too-many-records', bodies={'2026Q3': self.bodies['2026Q3']})
        self.assertEqual(file_hash(self.inventory), before)
        self.assertFalse((self.base / 'not-zip').exists())

    def test_incomplete_index_parent_and_empty_changed_quarter_fail_closed(self):
        root = self.seed_attached()
        with self.assertRaisesRegex(ValueError, 'became empty'):
            self.prepare('empty', inventory=root / 'inventory.sqlite3', bodies={'2026Q3': quarter_body([])})
        db = sqlite3.connect(self.inventory); db.execute("DELETE FROM index_sources WHERE source_key='2026Q1'"); db.commit(); db.close()
        client = client_for(self.bodies)
        with self.assertRaisesRegex(ValueError, 'missing or out-of-scope'):
            bulk.prepare(self.inventory, self.base / 'incomplete', source_quarters=['2026Q3'], client=client, source_urls={'2026Q3':bulk.bulk_url('2026Q3')})
        self.assertEqual(client.calls, [])

    def test_withdrawn_interrupted_document_requires_recovery_before_refresh(self):
        root = self.seed_attached()
        db = sqlite3.connect(root / 'inventory.sqlite3')
        db.execute("UPDATE filings SET status='inflight' WHERE accession=?", (fixtures.REMOVED,)); db.commit(); db.close()
        before = file_hash(root / 'inventory.sqlite3')
        with self.assertRaisesRegex(ValueError, 'interrupted committed'):
            self.prepare('interrupted', inventory=root / 'inventory.sqlite3', bodies={'2026Q3': quarter_body(self.q3[:1])})
        self.assertEqual(file_hash(root / 'inventory.sqlite3'), before)

    def test_recorded_legacy_url_is_reused_and_url_changes_preserve_provenance(self):
        root = self.seed_attached()
        legacy_url = bulk.bulk_url('2026Q3').replace('datastandardsinnovation', 'structureddata')
        db = sqlite3.connect(root / 'inventory.sqlite3')
        db.execute("UPDATE sources SET url=? WHERE source_key='2026Q3'", (legacy_url,)); db.commit(); db.close()
        client = fixtures.SourceClient({legacy_url: self.bodies['2026Q3']})
        unchanged = bulk.prepare(root / 'inventory.sqlite3', self.base / 'legacy-url', source_quarters=['2026Q3'], client=client)
        self.assertEqual(client.calls, [(legacy_url, True)])
        self.assertEqual(unchanged['changed_source_quarters'], [])
        directory, changed, _ = self.prepare('new-url', inventory=root / 'inventory.sqlite3', bodies={'2026Q3':self.bodies['2026Q3']})
        self.assertEqual(changed['committed_documents_to_restore'], 2)
        bulk.materialize(directory, changed['plan_sha256'], root, self.base / 'new-url-candidate')
        db = readonly(self.base / 'new-url-candidate/inventory.sqlite3')
        history = [json.loads(row[0]) for row in db.execute("SELECT value FROM settings WHERE key LIKE 'bulk_source_history:%'")]
        self.assertEqual(history[0]['url'], legacy_url); db.close()

    def test_new_quarter_needs_explicit_exact_sec_url_before_any_request(self):
        client = client_for(self.bodies)
        with self.assertRaisesRegex(ValueError, 'explicit published SEC'):
            bulk.prepare(self.inventory, self.base / 'no-url', source_quarters=['2026Q3'], client=client)
        for url in ('https://example.com/2026q3_form345.zip', bulk.bulk_url('2026Q2')):
            with self.assertRaisesRegex(ValueError, 'exact quarter'):
                bulk.prepare(self.inventory, self.base / 'wrong-url', source_quarters=['2026Q3'], client=client,
                             source_urls={'2026Q3':url})
        self.assertEqual(client.calls, [])


class BulkRefreshChainTests(unittest.TestCase):
    def test_archive_session_bulk_refresh_increment_and_full_restore_preserve_every_document(self):
        from collections import Counter
        from insider_pipeline.github_session import open_inventory
        from insider_pipeline.increment import build as build_increment, restore as restore_increment
        from insider_pipeline.inventory_archive import capture_snapshot
        from test_github_session import GitHubSessionRefreshTests
        from test_github_documents import GitHubDocumentTests
        from test_increment import IncrementTests
        fixture = GitHubSessionRefreshTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        base = fixture.base
        rows = [(fixtures.ACCESSION, 64040, '2026-09-03', '4'), (fixtures.REMOVED, 64040, '2026-09-03', '4')]
        with fixture.github.patches():
            session = open_inventory(fixture.bucket, fixture.staged['transport_sha256'], base / 'bulk-cloud')
            prepared = bulk.prepare(session.root / 'inventory.sqlite3', base / 'bulk-plan', source_quarters=['2026Q3'],
                                    client=client_for({'2026Q3': quarter_body(rows)}), workers=1, source_urls={'2026Q3':bulk.bulk_url('2026Q3')})
            recovered = session.recover_bulk_refresh(base / 'bulk-plan', prepared['plan_sha256'],
                                                      document_index_pin=fixture.published['document_index_sha256'])
            self.assertEqual(recovered['selected_documents_verified'], 2)
            self.assertEqual(recovered['filing_selection_source'], 'local_bulk_refresh_plan')
            self.assertTrue(recovered['bulk_refresh_plan_parent_verified'])
            bulk.materialize(base / 'bulk-plan', prepared['plan_sha256'], session.root, base / 'bulk-candidate')
        calls = Counter(GitHubDocumentTests().downloads(fixture.github))
        self.assertTrue(all(count == 1 for count in calls.values()))
        self.assertFalse(any(kind == 'command' and args[:2] != ['release', 'download'] for kind, args in fixture.github.calls))
        target = base / 'bulk-target.sqlite3'; capture_snapshot(base / 'bulk-candidate', target)
        parent_manifest = base / 'increment/increment.json'
        with contextlib.redirect_stdout(io.StringIO()):
            built = build_increment(base / 'bulk-candidate', parent_manifest, file_hash(parent_manifest),
                                    fixture.target, target, base / 'bulk-increment', workers=1)
            restored = restore_increment(base / 'bulk-increment', parent_manifest, base / 'bulk-restored',
                                         built['increment_manifest_sha256'], ancestor_manifests=[base / 'baseline/baseline.json'])
            checked = audit(base / 'bulk-restored', base / 'bulk-full-audit', workers=1)
        self.assertEqual(restored['restored_documents'], 3)
        self.assertEqual(built['changed_committed_documents'], 2)
        IncrementTests().assert_inventory_equal(target, base / 'bulk-restored/inventory.sqlite3')
        for accession in (fixtures.ACCESSION, fixtures.REMOVED, fixtures.LEGACY):
            expected = fixture.root if accession == fixtures.LEGACY else base / 'bulk-candidate'
            self.assertEqual(fixture.fixture.record(expected, accession), fixture.fixture.record(base / 'bulk-restored', accession))
        self.assertEqual(checked['selected_documents'], 3)
        self.assertEqual(checked['counts'].get('original_xml_field_failure', 0), 0)
        self.assertEqual(checked['counts'].get('document_failure', 0), 0)
        self.assertFalse(checked['complete_backfill'])


if __name__ == '__main__':
    unittest.main()
