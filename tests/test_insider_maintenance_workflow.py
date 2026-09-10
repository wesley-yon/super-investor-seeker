"""Execute maintenance workflow shell blocks with controlled input and no network."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / '.github/workflows/maintain-insider-checkpoint.yml'


def script(name):
    block = next(value for value in re.split(r'(?m)^      - name: ', WORKFLOW.read_text()) if value.startswith(name + '\n'))
    match = re.search(r'(?ms)^        run: \|\n(.*)', block)
    if match is None:
        raise AssertionError('Missing executable workflow step')
    return textwrap.dedent(match[1])


class InsiderMaintenanceWorkflowTests(unittest.TestCase):
    def run_block(self, name, variables, *, capture=True):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        root = Path(temporary.name); binary = root / 'bin'; binary.mkdir()
        captured = root / 'arguments.json'
        python = binary / 'python'
        if capture:
            python.write_text(f'#!{sys.executable}\nimport json, os, pathlib, sys\n'
                              'pathlib.Path(os.environ["CAPTURE_ARGUMENTS"]).write_text(json.dumps(sys.argv[1:]))\n')
            python.chmod(0o700)
        else:
            python.symlink_to(sys.executable)
        env = {**os.environ, 'PATH': str(binary) + ':/bin:/usr/bin', 'CAPTURE_ARGUMENTS': str(captured),
               'RUNNER_TEMP': str(root), **variables}
        completed = subprocess.run(['bash', '-c', script(name)], env=env, capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(captured.read_text()) if capture else completed.stdout

    def test_prepare_passes_quoted_input_as_arguments_without_shell_execution(self):
        values = {'INSIDER_RELEASE_TAG': 'insider-archives-202609-001', 'INSIDER_TRANSPORT_SHA256': 'a' * 64,
                  'INSIDER_DOCUMENT_INDEX_SHA256': 'b' * 64, 'INSIDER_THROUGH': '$(exit 9) " ; exit 7',
                  'INSIDER_COLLECTION_START': '`exit 8`', 'INSIDER_MAX_FILINGS': '50; exit 6'}
        args = self.run_block('Refresh sources, collect bounded originals, audit, and build checkpoint', values)
        self.assertEqual(args[:3], ['-m', 'insider_pipeline.cloud_maintenance', 'prepare'])
        for option, key in (('--tag', 'INSIDER_RELEASE_TAG'), ('--transport-sha256', 'INSIDER_TRANSPORT_SHA256'),
                            ('--document-index-sha256', 'INSIDER_DOCUMENT_INDEX_SHA256'), ('--through', 'INSIDER_THROUGH'),
                            ('--collection-start', 'INSIDER_COLLECTION_START'), ('--max-filings', 'INSIDER_MAX_FILINGS')):
            self.assertEqual(args[args.index(option) + 1], values[key])
        self.assertEqual(args[args.index('--seconds') + 1], '600')
        self.assertEqual(args[args.index('--workers') + 1], '2')

    def test_empty_optional_dates_are_omitted(self):
        args = self.run_block('Refresh sources, collect bounded originals, audit, and build checkpoint', {
            'INSIDER_RELEASE_TAG': 'insider-archives-202609-001', 'INSIDER_TRANSPORT_SHA256': 'a' * 64,
            'INSIDER_DOCUMENT_INDEX_SHA256': 'b' * 64, 'INSIDER_THROUGH': '', 'INSIDER_COLLECTION_START': '', 'INSIDER_MAX_FILINGS': '1'})
        self.assertNotIn('--through', args); self.assertNotIn('--collection-start', args); self.assertNotIn('', args)

    def test_publication_receives_only_prepared_pin_and_explicit_bucket(self):
        pin = '$(exit 9)'; bucket = '`exit 8`'
        args = self.run_block('Append and independently download the verified private checkpoint', {
            'INSIDER_PREPARED_SHA256': pin, 'INSIDER_BUCKET_TAG': bucket})
        self.assertEqual(args[:3], ['-m', 'insider_pipeline.cloud_maintenance', 'publish'])
        self.assertEqual(args[args.index('--prepared-sha256') + 1], pin)
        self.assertEqual(args[args.index('--bucket-tag') + 1], bucket)
        self.assertNotIn('--through', args)

    def test_summary_records_next_pins_and_incomplete_scope_without_raw_filing_values(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        root = Path(temporary.name); report = root / 'report.json'; summary = root / 'summary.md'
        report.write_text(json.dumps({'collection_completed_documents': 2, 'originals_rechecked': 3,
            'changed_documents_archived': 5, 'original_source_checks': 123, 'indexed_documents': 456,
            'locator': {'tag': 'insider-archives-202609-001', 'sha256': 'a' * 64}, 'document_index_sha256': 'b' * 64,
            'private_unused_filing': '0001234567-26-000001'}))
        self.run_block('Record maintenance result without raw private filings', {
            'INSIDER_REPORT': str(report), 'GITHUB_STEP_SUMMARY': str(summary)}, capture=False)
        text = summary.read_text()
        self.assertIn('a' * 64, text); self.assertIn('b' * 64, text)
        self.assertIn('does not activate a schedule', text)
        self.assertNotIn('0001234567-26-000001', text)


if __name__ == '__main__':
    unittest.main()
