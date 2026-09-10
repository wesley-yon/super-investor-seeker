from contextlib import ExitStack
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from insider_pipeline.audit_batches import file_hash
from insider_pipeline.github_chain import (BUCKET_MARKER, Downloader, blob_name, declared_files, descriptor_name,
                                           read_chain, validate_bucket, validate_locator)
from insider_pipeline.github_increment_stage import check_capacity, stage_increment
from insider_pipeline.github_stage import REPOSITORY
from insider_pipeline.increment import MANIFEST, build
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import capture_snapshot
import test_increment as fixtures


class FakeGitHub:
    def __init__(self):
        self.releases, self.bodies, self.calls = {}, {}, []
        self.private, self.push = True, True
        self.corrupt_download = None

    def add(self, tag, files, draft=False, bucket=False):
        identity = len(self.releases) + 2
        release = {'id': identity, 'tag_name': tag, 'draft': draft, 'prerelease': bucket,
                   'published_at': None if draft else '2026-09-10', 'immutable': False,
                   'body': BUCKET_MARKER if bucket else 'Legacy baseline', 'html_url': 'https://github.com/' + REPOSITORY + '/releases/tag/' + tag}
        self.releases[tag] = release
        self.bodies[tag] = dict(files)
        return release

    def assets(self, tag):
        return [{'id': index, 'name': name, 'state': 'uploaded', 'size': len(body), 'digest': 'sha256:' + hashlib.sha256(body).hexdigest()}
                for index, (name, body) in enumerate(self.bodies[tag].items(), 1)]

    def api(self, path, **kwargs):
        self.calls.append(('api', path))
        if path == 'repos/' + REPOSITORY:
            return {'full_name': REPOSITORY, 'private': self.private, 'permissions': {'push': self.push}, 'default_branch': 'main'}
        if path.endswith('/releases/latest'):
            return {'id': 1, 'tag_name': 'dataset-existing'}
        if path.endswith('/commits/main'):
            return {'sha': 'c' * 40}
        if path.endswith('/releases?per_page=100'):
            return list(self.releases.values())
        if '/releases/tags/' in path:
            return self.releases[path.rsplit('/', 1)[1]]
        if '/assets?' in path:
            identity = int(path.split('/releases/')[1].split('/')[0])
            tag = next(tag for tag, value in self.releases.items() if value['id'] == identity)
            return self.assets(tag)
        raise AssertionError(path)

    def command(self, args, *ignored, **kwargs):
        self.calls.append(('command', list(args)))
        self_test = args[:2]
        tag = args[2]
        if self_test == ['release', 'create']:
            assert '--draft' in args and '--prerelease' in args and '--latest=false' in args
            assert tag not in self.releases
            return self.add(tag, {}, draft=True, bucket=True)['html_url']
        if self_test == ['release', 'upload']:
            assert '--clobber' not in args
            path = Path(args[3]); assert path.name not in self.bodies[tag]
            self.bodies[tag][path.name] = path.read_bytes()
            return ''
        if self_test == ['release', 'download']:
            name = args[args.index('--pattern') + 1]
            destination = Path(args[args.index('--dir') + 1]); assert not (destination / name).exists()
            body = self.bodies[tag][name]
            if name == self.corrupt_download:
                body = bytes([body[0] ^ 1]) + body[1:]
            (destination / name).write_bytes(body)
            return ''
        raise AssertionError(args)

    def patches(self):
        stack = ExitStack()
        for module in ('github_chain', 'github_increment_stage', 'github_stage'):
            stack.enter_context(patch('insider_pipeline.' + module + '.api', side_effect=self.api))
            stack.enter_context(patch('insider_pipeline.' + module + '.command', side_effect=self.command))
        return stack

    def publish(self, tag):
        self.releases[tag].update(draft=False, prerelease=True, published_at='2026-09-10')


class GitHubChainTests(unittest.TestCase):
    bucket = 'insider-archives-202609-001'

    def seed(self, base):
        fixture = fixtures.IncrementTests()
        root, parent, baseline, parent_pin = fixture.seed(base)
        target = base / 'target.sqlite3'; fixture.advance(root, target)
        directory = base / 'increment'
        value = build(root, baseline / 'baseline.json', parent_pin, parent, target, directory, workers=1)
        github = FakeGitHub()
        manifest = json.loads((baseline / 'baseline.json').read_text())
        names = [asset['file'] for asset in manifest['files']] + ['baseline.json']
        github.add('insider-baseline', {name: (baseline / name).read_bytes() for name in names})
        locator = {'layout': 'legacy_baseline', 'tag': 'insider-baseline', 'sha256': parent_pin}
        return root, target, directory, value['increment_manifest_sha256'], locator, github

    def test_stage_readback_resume_and_complete_cloud_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, target, bundle, pin, parent, github = self.seed(base)
            original = copy.deepcopy(github.bodies['insider-baseline'])
            with github.patches():
                result = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
                self.assertTrue(result['draft'])
                self.assertTrue(result['independently_downloaded_and_verified'])
                self.assertEqual(result['changed_documents'], 2)
                uploads = [args for kind, args in github.calls if kind == 'command' and args[:2] == ['release', 'upload']]
                self.assertEqual(Path(uploads[-1][3]).name, descriptor_name(result['transport_sha256']))
                # Reusing the exact draft uploads nothing, including its descriptor.
                again = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
                self.assertEqual(again['new_assets_uploaded'], 0)
                github.publish(self.bucket); github.push = False
                before_read = len(github.calls)
                report = read_chain(self.bucket, result['transport_sha256'], base / 'cloud', workers=1)
            self.assertEqual((report['checkpoints'], report['documents'], report['inventory_filings']), (2, 3, 4))
            self.assertTrue(report['full_restore_verified'])
            self.assertTrue(report['changed_source_audit_matches_archive'])
            self.assertTrue(report['latest_release_unchanged'])
            self.assertFalse(report['cloud_daily_maintenance_active'])
            self.assertEqual(github.bodies['insider-baseline'], original)
            for kind, args in github.calls[before_read:]:
                if kind == 'command':
                    self.assertEqual(args[:2], ['release', 'download'])
            fixtures.IncrementTests().assert_inventory_equal(target, base / 'cloud/restored/inventory.sqlite3')

    def test_second_increment_shares_bucket_and_resolves_its_ancestors(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, first_target, bundle, pin, parent, github = self.seed(base)
            with github.patches():
                first = stage_increment(bundle, base / 'stage1', pin, parent, self.bucket, 1)
                github.publish(self.bucket)
                original = copy.deepcopy(github.bodies[self.bucket])
                db = connect(root); db.execute("UPDATE filings SET attempts=3,status='retry',retry_after=99 WHERE status='pending'"); db.commit(); db.close()
                target = base / 'second.sqlite3'; capture_snapshot(root, target)
                second_dir = base / 'second'
                second = build(root, bundle / MANIFEST, pin, first_target, target, second_dir, workers=1)
                staged = stage_increment(second_dir, base / 'stage2', second['increment_manifest_sha256'], first['locator'], self.bucket, 1)
                self.assertFalse(staged['draft'])
                self.assertEqual(staged['changed_documents'], 0)
                self.assertEqual(staged['bucket_assets'], len(original) + staged['new_assets_uploaded'])
                self.assertFalse(any(asset['file'].startswith('filings-') for asset in second['files']))
                for name, body in original.items():
                    self.assertEqual(github.bodies[self.bucket][name], body)
                report = read_chain(self.bucket, staged['transport_sha256'], base / 'cloud', workers=1)
            self.assertEqual((report['checkpoints'], report['changed_documents'], report['inherited_documents']), (3, 0, 3))
            fixtures.IncrementTests().assert_inventory_equal(target, base / 'cloud/restored/inventory.sqlite3')
            self.assertEqual(len(github.releases), 2)

    def test_corrupt_readback_never_uploads_checkpoint_descriptor(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, pin, parent, github = self.seed(base)
            value = json.loads((bundle / MANIFEST).read_text())
            chunk = next(asset for asset in value['files'] if asset['file'].startswith('filings-'))
            github.corrupt_download = blob_name(chunk['sha256'])
            with github.patches(), self.assertRaisesRegex(ValueError, 'Downloaded checkpoint bytes'):
                stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
            self.assertTrue(github.releases[self.bucket]['draft'])
            self.assertFalse(any(name.startswith('checkpoint-') for name in github.bodies[self.bucket]))
            self.assertFalse((base / 'stage/increment-stage-report.json').exists())

    def test_reader_rejects_missing_ancestor_corruption_and_wrong_pin_before_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, pin, parent, github = self.seed(base)
            with github.patches():
                result = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
                github.publish(self.bucket)
                with self.assertRaises(ValueError):
                    read_chain(self.bucket, 'f' * 64, base / 'wrong', workers=1)
                name = next(name for name in github.bodies['insider-baseline'] if name.startswith('filings-'))
                body = github.bodies['insider-baseline'].pop(name)
                with self.assertRaisesRegex(ValueError, 'Remote checkpoint asset'):
                    read_chain(self.bucket, result['transport_sha256'], base / 'missing', workers=1)
                github.bodies['insider-baseline'][name] = body
                github.corrupt_download = name
                with self.assertRaisesRegex(ValueError, 'Downloaded checkpoint bytes'):
                    read_chain(self.bucket, result['transport_sha256'], base / 'corrupt', workers=1)
            for name in ('wrong', 'missing', 'corrupt'):
                self.assertFalse((base / name / 'restored').exists())

    def test_mismatched_parent_rejected_before_creating_bucket(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, pin, parent, github = self.seed(base)
            manifest = json.loads((bundle / MANIFEST).read_text())
            manifest['parent']['manifest_sha256'] = 'f' * 64
            (bundle / MANIFEST).write_bytes(canonical(manifest)); pin = file_hash(bundle / MANIFEST)
            with github.patches(), self.assertRaisesRegex(ValueError, 'remote parent differs'):
                stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
            self.assertNotIn(self.bucket, github.releases)

    def test_capacity_private_storage_and_existing_asset_identity_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'bucket is full'):
            check_capacity({str(index): {} for index in range(900)}, {'new': {}})
        for layout in ('file:///tmp/', '../unsafe'):
            with self.assertRaises(ValueError):
                validate_locator({'layout': layout, 'tag': 'insider-a', 'sha256': 'a' * 64})
        good = {'tag_name': self.bucket, 'body': BUCKET_MARKER, 'draft': False, 'prerelease': True, 'published_at': 'now'}
        for change in ({'prerelease': False}, {'body': 'unrelated'}, {'immutable': True}):
            with self.assertRaises(ValueError):
                validate_bucket({**good, **change}, self.bucket, writable=True)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, pin, parent, github = self.seed(base)
            with github.patches():
                github.private = False
                with self.assertRaisesRegex(ValueError, 'remain private'):
                    stage_increment(bundle, base / 'private', pin, parent, self.bucket, 1)
                github.private = True
                with self.assertRaisesRegex(ValueError, 'dataset pointer changed'):
                    stage_increment(bundle, base / 'latest', pin, parent, self.bucket, 99)
                github.add(self.bucket, {blob_name(pin): b'wrong'}, bucket=True)
                with self.assertRaisesRegex(ValueError, 'Remote checkpoint asset'):
                    stage_increment(bundle, base / 'conflict', pin, parent, self.bucket, 1)
            self.assertEqual(github.bodies[self.bucket], {blob_name(pin): b'wrong'})

    def test_pinned_transport_cannot_redirect_to_a_different_valid_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, pin, parent, github = self.seed(base)
            with github.patches():
                result = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
                github.publish(self.bucket)
                other = copy.deepcopy(github.bodies['insider-baseline'])
                baseline = json.loads(other['baseline.json']); baseline['documents'] += 1
                other['baseline.json'] = canonical(baseline)
                github.add('insider-other-baseline', other)
                transport = json.loads(github.bodies[self.bucket][descriptor_name(result['transport_sha256'])])
                transport['parent'] = {'layout': 'legacy_baseline', 'tag': 'insider-other-baseline',
                                       'sha256': hashlib.sha256(other['baseline.json']).hexdigest()}
                body = canonical(transport); changed_pin = hashlib.sha256(body).hexdigest()
                github.bodies[self.bucket][descriptor_name(changed_pin)] = body
                with self.assertRaisesRegex(ValueError, 'Transport parent differs'):
                    read_chain(self.bucket, changed_pin, base / 'wrong-parent', workers=1)
                with patch('insider_pipeline.github_chain.MAX_CHAIN', 1):
                    with self.assertRaisesRegex(ValueError, 'requires compaction'):
                        read_chain(self.bucket, result['transport_sha256'], base / 'too-deep', workers=1)
            self.assertFalse((base / 'wrong-parent/restored').exists())

    def test_full_chain_budget_is_checked_before_large_downloads(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, pin, parent, github = self.seed(base)
            with github.patches():
                result = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
                github.publish(self.bucket)
                permitted = {'baseline.json', descriptor_name(result['transport_sha256']), blob_name(pin)}
                metadata_bytes = sum(len(body) for files in github.bodies.values() for name, body in files.items() if name in permitted)
                before = len(github.calls)
                with patch('insider_pipeline.github_chain.MAX_DOWNLOAD_BYTES', metadata_bytes + 1):
                    with self.assertRaisesRegex(ValueError, 'chain exceeds'):
                        read_chain(self.bucket, result['transport_sha256'], base / 'too-large', workers=1)
                for kind, args in github.calls[before:]:
                    if kind == 'command':
                        self.assertIn(args[args.index('--pattern') + 1], permitted)
            self.assertFalse((base / 'too-large/restored').exists())

    def test_download_budget_is_combined_across_nodes_and_existing_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            downloader = Downloader(base / 'cache', budget=100)
            downloader.plan('insider-one', 'a', 60, 'a' * 64)
            with self.assertRaisesRegex(ValueError, 'chain exceeds'):
                downloader.plan('insider-two', 'b', 60, 'b' * 64)
            destination = base / 'existing'; destination.mkdir(); (destination / 'keep').write_text('unchanged')
            with self.assertRaisesRegex(ValueError, 'fresh directory'):
                read_chain(self.bucket, 'a' * 64, destination)
            self.assertEqual((destination / 'keep').read_text(), 'unchanged')
            row = {'file': 'one.zip', 'bytes': 10, 'sha256': 'a' * 64}
            for change in ({'file': '../escape'}, {'bytes': True}, {'sha256': 'bad'}):
                with self.assertRaises(ValueError):
                    declared_files({'files': [{**row, **change}]}, MANIFEST, 'b' * 64, 100)


if __name__ == '__main__':
    unittest.main()
