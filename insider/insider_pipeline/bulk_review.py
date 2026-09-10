"""Reproduce and explain bulk-source differences without changing archived data.

This is a supplementary evidence ledger, not a replacement for the pinned source
audit. Row associations remain significant. A timezone-bearing xs:date can be
compared to a bulk calendar date only as an explicitly lossy projection, never
as equality of instants. Original values and their row associations are retained.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
import csv
from datetime import date
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
import zipfile

from .audit import FIELDS, compare, value
from .audit_batches import file_hash, readonly, validate_selection
from .http import atomic_write
from .increment import shard_path, source_path
from .inventory import canonical
from .locking import writer_lock
from .source_audit import compare_source

VERSION = '1.0.0'
TABLES = {'NONDERIV_TRANS': ('transactions', 'non_derivative'), 'DERIV_TRANS': ('transactions', 'derivative'),
          'NONDERIV_HOLDING': ('holdings', 'non_derivative'), 'DERIV_HOLDING': ('holdings', 'derivative')}
DATE_TIMEZONE = re.compile(r'(\d{4}-\d{2}-\d{2})(Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))\Z')


def calendar_projection(text):
    """Recognize positive four-digit xs:date lexemes; retain the calendar day.

    This intentionally does not convert to UTC or claim temporal equivalence.
    Other lexical forms remain unrecognized instead of being guessed.
    W3C: https://www.w3.org/TR/xmlschema-2/#date
    """
    match = DATE_TIMEZONE.fullmatch(str(text).strip())
    if match:
        try:
            return date.fromisoformat(match[1]).isoformat(), match[2]
        except ValueError:
            pass
    return None


def classification(xml_rows, bulk_rows, fields):
    def signature(rows, bulk=False, rounded=False, calendar=False, drop=None):
        result = Counter()
        for row in rows:
            values = []
            for field, (column, kind) in fields.items():
                if field == drop:
                    continue
                text = row.get(column if bulk else field, '')
                projected = calendar_projection(text) if calendar and kind == 'date' else None
                values.append(value(projected[0] if projected else text, kind, rounded))
            result[tuple(values)] += 1
        return result

    def marginals(rows):
        columns = [Counter() for _ in fields]
        for row, count in rows.items():
            for index, item in enumerate(row):
                columns[index][item] += count
        return columns

    bulk = signature(bulk_rows, bulk=True)
    modes = [(False, False, 'unchanged'), (True, False, 'rounding'),
             (False, True, 'date_timezone_loss'), (True, True, 'rounding_and_date_timezone_loss')]
    selected, associations, omitted = 'unexplained_source_difference', False, []
    if len(xml_rows) != len(bulk_rows):
        selected = 'row_count_difference'
        modes = []
    for rounded, calendar, label in modes:
        xml = signature(xml_rows, rounded=rounded, calendar=calendar)
        if xml == bulk:
            selected = 'exact' if label == 'unchanged' else label
            break
        if marginals(xml) == marginals(bulk):
            selected = 'row_association_difference' if label == 'unchanged' else label + '_and_row_association_difference'
            associations = True
            omitted = [field for field in fields
                       if signature(xml_rows, rounded=rounded, calendar=calendar, drop=field)
                       == signature(bulk_rows, bulk=True, drop=field)]
            break
    offsets = []
    for number, row in enumerate(xml_rows, 1):
        for field, (_, kind) in fields.items():
            projected = calendar_projection(row.get(field, '')) if kind == 'date' else None
            if projected:
                offsets.append({'row_number': number, 'field': field, 'original': row[field],
                                'calendar_date': projected[0], 'timezone': projected[1]})
    return {'category': selected, 'row_association_conflict': associations,
            'single_field_omissions_that_match': omitted, 'timezone_date_fields': offsets,
            'requires_source_review': associations or selected in ('unexplained_source_difference', 'row_count_difference'),
            'original_values_must_be_retained': True, 'bulk_values_applied': False}


def verify_audit(directory, pin):
    report = json.loads((directory / 'report.json').read_text())
    semantic = {key: report[key] for key in ('selection_sha256', 'audit_version', 'counts', 'financial_table_comparisons', 'groups')}
    if (not re.fullmatch('[0-9a-f]{64}', pin or '') or report['semantic_sha256'] != pin
            or hashlib.sha256(canonical(semantic)).hexdigest() != pin
            or any(report['counts'].get(key, 0) for key in ('document_failure', 'original_xml_field_failure'))):
        raise ValueError('Expected the pinned source audit with no original-source failures')
    db = readonly(directory / 'selection.sqlite3')
    try:
        metadata = json.loads(db.execute('SELECT value FROM metadata').fetchone()[0])
        validate_selection(db, metadata)
    finally:
        db.close()
    counts, comparisons, groups = Counter(), Counter(), set()
    for group in report['groups']:
        name = group['group']
        if (name in groups or not re.fullmatch(r'\d{4}Q[1-4](?:_index_only)?', name)
                or group['selection_sha256'] != report['selection_sha256'] or group['audit_version'] != report['audit_version']
                or group['details_file'] != name + '.jsonl.gz'
                or group['counts']['selected_documents'] != metadata['groups'].get(name)):
            raise ValueError('Source audit group identity differs from its selection')
        path = directory / group['details_file']
        if path.is_symlink() or path.stat().st_size != group['details_bytes'] or file_hash(path) != group['details_sha256']:
            raise ValueError('Source audit details differ from the pinned group')
        counts.update(group['counts']); comparisons.update(group['financial_table_comparisons']); groups.add(name)
    if (dict(counts) != report['counts'] or dict(comparisons) != report['financial_table_comparisons']
            or groups != set(metadata['groups']) or report['selection_sha256'] != metadata['selection_sha256']
            or report['selected_documents'] != metadata['selected_documents'] or report['scope'] != metadata['scope']):
        raise ValueError('Source audit coverage differs from its immutable selection')
    return report, metadata


def review_group(task):
    root, directory, output, group, metadata = task
    root, directory, output = Path(root), Path(directory), Path(output)
    findings = defaultdict(dict)
    with gzip.open(directory / group['details_file'], 'rt') as stream:
        for line in stream:
            item = json.loads(line)
            if item.get('kind') != 'bulk_difference':
                continue
            accession, table = item['accession'], item['table']
            if table not in TABLES or table in findings[accession] or item['status'] not in ('review', 'bulk_rounding'):
                raise ValueError('Duplicate or unsupported bulk finding')
            findings[accession][table] = item
    count = sum(len(tables) for tables in findings.values())
    if count != group['counts'].get('bulk_difference', 0):
        raise ValueError('Bulk finding membership differs from its pinned source audit')
    detail = output / (group['group'] + '.jsonl.gz')
    selection = readonly(directory / 'selection.sqlite3')
    databases, originals, identities = {}, {}, {}
    source_checks, documents = 0, 0
    try:
        for row in selection.execute('SELECT * FROM selected WHERE audit_group=? ORDER BY filing_date,accession', (group['group'],)):
            accession = row['accession']
            if accession not in findings:
                continue
            if row['shard'] not in databases:
                databases[row['shard']] = readonly(shard_path(root, row['shard']))
            stored = databases[row['shard']].execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone()
            if stored is None or row['bulk_source'] != group['group']:
                raise ValueError('Bulk finding does not point to a collected source document')
            bodies = {}
            for key in ('source', 'parsed'):
                body = gzip.decompress(stored[key + '_gzip'])
                if (hashlib.sha256(body).hexdigest() != row[key + '_sha256'] or row[key + '_sha256'] != stored[key + '_sha256']
                        or len(body) != stored[key + '_bytes']):
                    raise ValueError('Stored document differs from its pinned source provenance')
                bodies[key] = body
            parsed = json.loads(bodies['parsed'])
            checked, failures = compare_source(bodies['source'], parsed)
            if failures or parsed['filing']['source_url'] != stored['source_url']:
                raise ValueError('Original-source verification failed during bulk review')
            source_checks += sum(checked.values()); documents += 1
            originals[accession] = {table: [{name: row[name] for name in FIELDS} for row in parsed[category] if row['table'] == kind]
                                   for table, (category, kind) in TABLES.items() if table in findings[accession]}
            identities[accession] = {'source_url': stored['source_url'], 'source_sha256': stored['source_sha256'],
                                     'parsed_sha256': stored['parsed_sha256']}
        if set(originals) != set(findings):
            raise ValueError('Some bulk findings have no source document in the pinned selection')
        classifications, old_statuses, reproduced = Counter(), Counter(), 0
        with detail.open('wb') as raw, gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=6) as stream:
            if findings:
                source = metadata['sources'][group['group']]
                path = source_path(root, 'sources', source)
                if source['sha256'] != group['source_sha256'] or file_hash(path) != group['source_sha256']:
                    raise ValueError('Bulk source archive differs from the pinned source audit')
                with zipfile.ZipFile(path) as archive:
                    names = {Path(name).stem.upper(): name for name in archive.namelist()}
                    for table in TABLES:
                        targets = {accession for accession, tables in findings.items() if table in tables}
                        if not targets:
                            continue
                        bulk = defaultdict(list)
                        with archive.open(names[table]) as body:
                            reader = csv.DictReader(io.TextIOWrapper(body, encoding='utf-8-sig'), delimiter='\t')
                            headers = reader.fieldnames
                            for ordinal, row in enumerate(reader, 1):
                                if row['ACCESSION_NUMBER'] in targets:
                                    bulk[row['ACCESSION_NUMBER']].append({'record_ordinal': ordinal, 'fields': row})
                        fields = {field: item for field, item in FIELDS.items() if item[0] in headers}
                        if 'EXERCISE_DATE' in headers:
                            fields['exercise_date'] = ('EXERCISE_DATE', 'date')
                        for accession in sorted(targets):
                            expected = findings[accession][table]
                            xml_rows = originals[accession][table]
                            bulk_rows = [row['fields'] for row in bulk[accession]]
                            result = compare(xml_rows, bulk_rows, headers)
                            if result != {key: item for key, item in expected.items() if key not in ('kind', 'accession', 'table')}:
                                raise ValueError('A bulk discrepancy could not be reproduced from its original sources')
                            diagnosis = classification(xml_rows, bulk_rows, fields)
                            if diagnosis['category'] == 'exact':
                                raise ValueError('A reported discrepancy unexpectedly became exact')
                            record = {'accession': accession, 'table': table, 'audit_group': group['group'],
                                      'pinned_finding_sha256': hashlib.sha256(canonical(expected)).hexdigest(),
                                      'archived_status': expected['status'], **identities[accession],
                                      'bulk_source_sha256': group['source_sha256'],
                                      'original_rows': [{'row_number': i, 'fields': {name: row[name] for name in fields}}
                                                        for i, row in enumerate(xml_rows, 1)],
                                      'bulk_rows': bulk[accession], 'diagnosis': diagnosis,
                                      'original_source_fields_reverified': True}
                            stream.write(canonical(record) + b'\n')
                            classifications[diagnosis['category']] += 1; old_statuses[expected['status']] += 1; reproduced += 1
                if file_hash(path) != group['source_sha256']:
                    raise ValueError('Bulk source archive changed while its rows were reviewed')
        expected_statuses = {key: number for key, number in group['financial_table_comparisons'].items() if key != 'exact'}
        if reproduced != count or dict(old_statuses) != expected_statuses:
            raise ValueError('Supplementary ledger does not cover every non-exact comparison')
        return {'group': group['group'], 'findings_reproduced': reproduced, 'original_documents_rechecked': documents,
                'original_source_checks': source_checks, 'archived_statuses': dict(old_statuses),
                'classification_counts': dict(classifications), 'details_file': detail.name,
                'details_sha256': file_hash(detail), 'details_bytes': detail.stat().st_size}
    finally:
        selection.close()
        for db in databases.values():
            db.close()


def run(root, audit_directory, output, expected_audit_semantic_sha256, workers=4):
    root, audit_directory, output = Path(root).resolve(), Path(audit_directory).resolve(), Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Bulk review output must be a fresh directory')
    report, metadata = verify_audit(audit_directory, expected_audit_semantic_sha256)
    output.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(output.parent / ('.' + output.name + '.review-lock')):
        if output.exists() or output.is_symlink():
            raise ValueError('Bulk review output must be a fresh directory')
        temporary = Path(tempfile.mkdtemp(prefix='.' + output.name + '.reviewing-', dir=output.parent))
        started = time.monotonic()
        try:
            tasks = [(str(root), str(audit_directory), str(temporary), group, metadata) for group in report['groups']]
            if workers == 1:
                results = [review_group(task) for task in tasks]
            else:
                with ProcessPoolExecutor(max_workers=min(workers, max(1, len(tasks)))) as pool:
                    results = list(pool.map(review_group, tasks))
            results.sort(key=lambda item: item['group'])
            counts, statuses = Counter(), Counter()
            for result in results:
                counts.update(result['classification_counts']); statuses.update(result['archived_statuses'])
            if sum(counts.values()) != report['counts'].get('bulk_difference', 0):
                raise ValueError('The ledger is missing archived bulk discrepancies')
            semantic = {'review_version': VERSION, 'source_audit_semantic_sha256': expected_audit_semantic_sha256,
                        'selection_sha256': report['selection_sha256'], 'selected_documents': report['selected_documents'],
                        'findings_reproduced': sum(counts.values()), 'archived_statuses': dict(statuses),
                        'classification_counts': dict(counts), 'original_documents_rechecked': sum(r['original_documents_rechecked'] for r in results),
                        'original_source_checks': sum(r['original_source_checks'] for r in results), 'groups': results}
            final = {**semantic, 'semantic_sha256': hashlib.sha256(canonical(semantic)).hexdigest(),
                     'elapsed_seconds': round(time.monotonic() - started, 3), 'workers': workers,
                     'effective_workers': min(workers, len(tasks)), 'source_retention_verified': True,
                     'unexplained_differences': counts.get('unexplained_source_difference', 0),
                     'original_values_changed': False, 'existing_audit_changed': False, 'complete_backfill': False}
            atomic_write(temporary / 'report.json', canonical(final))
            verify_audit(audit_directory, expected_audit_semantic_sha256)
            if output.exists() or output.is_symlink():
                raise ValueError('Bulk review output was created by another process')
            os.rename(temporary, output)
            return final
        except BaseException as error:
            atomic_write(temporary / 'failure.json', canonical({'error_type': type(error).__name__, 'error': str(error), 'ledger_published': False}))
            raise
        finally:
            if temporary.exists() and not (temporary / 'failure.json').exists():
                shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--audit-semantic-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, choices=range(1, 5), default=4)
    args = parser.parse_args()
    print(json.dumps(run(args.root, args.audit, args.output, args.audit_semantic_sha256, args.workers), indent=2), flush=True)


if __name__ == '__main__':
    main()
