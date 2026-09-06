import copy
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from scripts import data_snapshot as snapshot
from scripts import restore_sec_candidate as candidate
from scripts.measure_runner_phase import measure
import sec_security_master as sec


class CandidateRunnerTests(unittest.TestCase):
    def metadata(self):
        repository = 'owner/private-data'
        tag = 'candidate-sec-test'
        info = {'full_name': repository, 'private': True, 'visibility': 'private'}
        release = {'tag_name': tag, 'draft': False, 'prerelease': True, 'assets': []}
        return repository, tag, info, release

    def test_private_candidate_metadata_is_required(self):
        repository, tag, info, release = self.metadata()
        candidate.validate_candidate(repository, tag, info, release)
        for owner_info, item in [
            ({**info, 'private': False}, release),
            ({**info, 'visibility': 'public'}, release),
            ({**info, 'full_name': 'other/private-data'}, release),
            (info, {**release, 'prerelease': False}),
            (info, {**release, 'draft': True}),
            (info, {**release, 'tag_name': 'dataset-live'}),
            (info, {**release, 'assets': [{'name': '../outside.manifest.json'}]}),
        ]:
            with self.subTest(info=owner_info, release=item):
                with self.assertRaises(snapshot.SnapshotError):
                    candidate.validate_candidate(repository, tag, owner_info, item)

    def test_test_workflow_has_no_production_publish_capability(self):
        workflow = (Path(__file__).resolve().parents[1] /
                    '.github/workflows/verify-sec-candidate.yml').read_text()
        self.assertIn('branches: [validation/sec-incremental-candidate-20260906]', workflow)
        self.assertIn('permission-contents: read', workflow)
        for forbidden in ('permission-contents: write', 'pages: write', 'id-token: write',
                          'publish_private_snapshot.sh', 'uses: ./.github/workflows/deploy-pages.yml'):
            self.assertNotIn(forbidden, workflow)

    def test_candidate_roundtrip_and_code_mismatch_fail_before_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source = base / 'source'
            (source / 'data/funds').mkdir(parents=True)
            (source / 'data/stocks').mkdir()
            (source / '.cache').mkdir()
            (source / 'data/funds/1.json').write_text('{"cik":1}\n')
            state = sec.empty_source_state()
            state['updated_at'] = '2026-08-05T16:00:00Z'
            sec.save_security_master_pair(sec.rebuild_security_master(state, []), state,
                master_path=source / snapshot.CACHE_FILES[0],
                source_state_path=source / snapshot.CACHE_FILES[1])
            packed = snapshot.pack_snapshot(root=source, output_dir=base / 'packed', source_sha='a' * 40)
            repo, tag, info, release = self.metadata()
            originals = [Path(packed['manifest_path']), Path(packed['archive_path'])]
            by_name = {p.name: p for p in originals}
            release['assets'] = [{'name': p.name, 'size': p.stat().st_size,
                                  'url': f'https://api.github.com/repos/{repo}/releases/assets/{i}'}
                                 for i, p in enumerate(originals)]
            def download(*, asset, destination, **kwargs):
                shutil.copyfile(by_name[asset['name']], destination)
            def github(url, _):
                return copy.deepcopy(release if '/releases/tags/' in url else info)
            with mock.patch.dict(os.environ, {snapshot.TOKEN_ENV: 'fake-private-token'}), \
                 mock.patch.object(snapshot, '_github_json', side_effect=github), \
                 mock.patch.object(snapshot, '_download_asset', side_effect=download):
                rejected = base / 'rejected'
                rejected.mkdir()
                with self.assertRaisesRegex(snapshot.SnapshotError, 'tested code SHA'):
                    candidate.restore_candidate(repository=repo, tag=tag, root=rejected,
                                                expected_source_sha='b' * 40)
                self.assertFalse((rejected / 'data').exists())
                target = base / 'target'
                target.mkdir()
                result = candidate.restore_candidate(repository=repo, tag=tag, root=target,
                                                     expected_source_sha='a' * 40)
                self.assertFalse(result['production_publication'])
                self.assertEqual((target / 'data/funds/1.json').read_bytes(),
                                 (source / 'data/funds/1.json').read_bytes())
                with self.assertRaisesRegex(snapshot.SnapshotError, 'without data or caches'):
                    candidate.restore_candidate(repository=repo, tag=tag, root=target,
                                                expected_source_sha='a' * 40)

    def test_phase_measurement_preserves_nonzero_status_and_no_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = measure([sys.executable, '-c', 'import sys;sys.exit(7)'],
                             name='failed', root=root, report_dir=root / 'metrics',
                             interval_seconds=0.01)
            self.assertEqual(result['exit_code'], 7)
            self.assertGreater(result['maximum_single_child_rss_bytes'], 0)
            self.assertGreater(result['minimum_free_bytes'], 0)
            saved = json.loads((root / 'metrics/failed.json').read_text())
            self.assertNotIn('environment', saved)
            self.assertEqual(saved, result)

    def test_cancelling_measurement_terminates_its_child_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / 'ready'
            helper = Path(__file__).resolve().parents[1] / 'scripts/measure_runner_phase.py'
            child = ('import pathlib,time;'
                     f'pathlib.Path({str(ready)!r}).write_text("ready");'
                     'time.sleep(20)')
            process = subprocess.Popen(
                [sys.executable, str(helper), '--name', 'cancelled', '--root', str(root),
                 '--report-dir', str(root / 'metrics'), '--', sys.executable, '-c', child],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(ready.exists())
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=5)
                self.assertNotEqual(process.returncode, 0)
                report = json.loads((root / 'metrics/cancelled.json').read_text())
                self.assertEqual(report['exit_code'], -signal.SIGTERM)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)

    def test_hard_kill_retains_last_complete_measurement_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / 'child.pid'
            helper = Path(__file__).resolve().parents[1] / 'scripts/measure_runner_phase.py'
            child = ('import os,pathlib,time;'
                     f'pathlib.Path({str(ready)!r}).write_text(str(os.getpid()));'
                     'time.sleep(20)')
            process = subprocess.Popen(
                [sys.executable, str(helper), '--name', 'hard_kill', '--root', str(root),
                 '--report-dir', str(root / 'metrics'), '--', sys.executable, '-c', child],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            child_pid = None
            report_path = root / 'metrics/hard_kill.json'
            try:
                deadline = time.monotonic() + 5
                while not (ready.exists() and report_path.exists()) and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(ready.exists() and report_path.exists())
                child_pid = int(ready.read_text())
                before = report_path.read_bytes()
                report = json.loads(before)
                self.assertFalse(report['completed'])
                self.assertIsNone(report['exit_code'])
                self.assertGreater(report['minimum_free_bytes'], 0)
                process.kill()
                os.killpg(child_pid, signal.SIGTERM)
                process.communicate(timeout=5)
                self.assertEqual(process.returncode, -signal.SIGKILL)
                retained = json.loads(report_path.read_bytes())
                self.assertFalse(retained['completed'])
                self.assertIsNone(retained['exit_code'])
                self.assertGreaterEqual(retained['elapsed_seconds'], report['elapsed_seconds'])
            finally:
                if child_pid is None and ready.exists():
                    child_pid = int(ready.read_text())
                if child_pid is not None:
                    try:
                        os.killpg(child_pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()
