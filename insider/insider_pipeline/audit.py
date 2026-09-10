"""Read-only checksum and independent SEC bulk numerical reconciliation.

An audit is scoped to the committed accessions selected at its start. A growing
collection and its uncollected filings are never represented as fully audited.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext, ROUND_HALF_UP
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import xml.etree.ElementTree as ET
import zipfile

from .http import atomic_write
from .inventory import filed_date

FIELDS = {
    'security_title': ('SECURITY_TITLE', 'text'),
    'transaction_date': ('TRANS_DATE', 'date'),
    'deemed_execution_date': ('DEEMED_EXECUTION_DATE', 'date'),
    'transaction_code': ('TRANS_CODE', 'text'),
    'transaction_form_type': ('TRANS_FORM_TYPE', 'text'),
    'transaction_timeliness': ('TRANS_TIMELINESS', 'text'),
    'transaction_shares': ('TRANS_SHARES', 'number'),
    'transaction_price_per_share': ('TRANS_PRICEPERSHARE', 'number'),
    'reported_transaction_total_value': ('TRANS_TOTAL_VALUE', 'number'),
    'acquired_disposed': ('TRANS_ACQUIRED_DISP_CD', 'text'),
    'shares_owned_following': ('SHRS_OWND_FOLWNG_TRANS', 'number'),
    'value_owned_following': ('VALU_OWND_FOLWNG_TRANS', 'number'),
    'direct_or_indirect': ('DIRECT_INDIRECT_OWNERSHIP', 'text'),
    'nature_of_ownership': ('NATURE_OF_OWNERSHIP', 'text'),
    'conversion_or_exercise_price': ('CONV_EXERCISE_PRICE', 'number'),
    'exercise_date': ('EXCERCISE_DATE', 'date'),
    'expiration_date': ('EXPIRATION_DATE', 'date'),
    'underlying_security_title': ('UNDLYNG_SEC_TITLE', 'text'),
    'underlying_security_shares': ('UNDLYNG_SEC_SHARES', 'number'),
    'underlying_security_value': ('UNDLYNG_SEC_VALUE', 'number'),
}

SOURCE_PATHS = {
    'security_title': 'securityTitle', 'transaction_date': 'transactionDate',
    'deemed_execution_date': 'deemedExecutionDate', 'transaction_code': 'transactionCoding/transactionCode',
    'transaction_form_type': 'transactionCoding/transactionFormType', 'transaction_timeliness': 'transactionTimeliness',
    'transaction_shares': 'transactionAmounts/transactionShares',
    'transaction_price_per_share': 'transactionAmounts/transactionPricePerShare',
    'reported_transaction_total_value': 'transactionAmounts/transactionTotalValue',
    'acquired_disposed': 'transactionAmounts/transactionAcquiredDisposedCode',
    'shares_owned_following': 'postTransactionAmounts/sharesOwnedFollowingTransaction',
    'value_owned_following': 'postTransactionAmounts/valueOwnedFollowingTransaction',
    'direct_or_indirect': 'ownershipNature/directOrIndirectOwnership',
    'nature_of_ownership': 'ownershipNature/natureOfOwnership',
    'conversion_or_exercise_price': 'conversionOrExercisePrice',
    'exercise_date': 'exerciseDate', 'expiration_date': 'expirationDate',
    'underlying_security_title': 'underlyingSecurity/underlyingSecurityTitle',
    'underlying_security_shares': 'underlyingSecurity/underlyingSecurityShares',
    'underlying_security_value': 'underlyingSecurity/underlyingSecurityValue',
}


def compare_original(body, parsed):
    """Independently walk each source row, checking its field-to-row association."""
    candidates = re.findall(br'<XML>\s*([\s\S]*?)\s*</XML>', body, re.I)
    if not candidates:
        candidates = [body]
    roots = []
    for candidate in candidates:
        if not re.search(br'<(?:\w+:)?ownershipDocument\b', candidate):
            continue
        if re.search(br'<!\s*(?:DOCTYPE|ENTITY)\b', candidate.replace(b'\x00', b''), re.I):
            raise ValueError('Unsafe declaration in stored source')
        root = ET.fromstring(candidate)
        if root.tag.rsplit('}', 1)[-1] == 'ownershipDocument':
            roots.append(root)
    if len(roots) != 1:
        raise ValueError('Independent XML audit could not identify exactly one ownership document')
    root = roots[0]
    for element in root.iter():
        element.tag = element.tag.rsplit('}', 1)[-1]
    failures = []
    checked = 0
    for prefix, table in [('nonDerivative', 'non_derivative'), ('derivative', 'derivative')]:
        for suffix, category in [('Transaction', 'transactions'), ('Holding', 'holdings')]:
            source_rows = root.findall(prefix + 'Table/' + prefix + suffix)
            parsed_rows = [r for r in parsed[category] if r['table'] == table]
            if len(source_rows) != len(parsed_rows):
                failures.append({'table': table, 'category': category, 'error': 'Row count differs from original XML'})
                continue
            for number, (element, record) in enumerate(zip(source_rows, parsed_rows), 1):
                for field, path in SOURCE_PATHS.items():
                    node = element.find(path)
                    if node is None:
                        original = ''
                    else:
                        nested = node.find('value')
                        original = ''.join((nested if nested is not None else node).itertext()).strip()
                    checked += 1
                    if original != record[field]:
                        failures.append({'table': table, 'category': category, 'row_number': number,
                                         'field': field, 'original': original, 'parsed': record[field]})
    return checked, failures


def value(text, kind, rounded=False):
    text = str(text or '').strip()
    if not text:
        return ''
    if kind == 'number':
        try:
            number = Decimal(text)
            if not number.is_finite():
                return 'INVALID:' + text
            with localcontext() as context:
                context.prec = max(100, len(number.as_tuple().digits) + abs(number.as_tuple().exponent) + 10)
                if rounded:
                    number = number.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                return format(number.normalize(), 'f') if number else '0'
        except InvalidOperation:
            return 'INVALID:' + text
    if kind == 'date':
        try:
            return filed_date(text)
        except (ValueError, KeyError):
            return 'INVALID:' + text
    return ' '.join(text.split())


def compare(xml_rows, bulk_rows, headers):
    fields = {k: v for k, v in FIELDS.items() if v[0] in headers}
    if 'EXERCISE_DATE' in headers:
        fields['exercise_date'] = ('EXERCISE_DATE', 'date')

    def signatures(rows, bulk=False, rounded=False):
        return Counter(tuple(value(r.get(source if bulk else target, ''), kind, rounded)
                             for target, (source, kind) in fields.items()) for r in rows)

    xml, bulk = signatures(xml_rows), signatures(bulk_rows, True)
    if xml == bulk:
        return {'status': 'exact', 'rows': len(xml_rows), 'fields_checked': list(fields)}
    rounded = signatures(xml_rows, rounded=True)
    differences = []
    for target, (source, kind) in fields.items():
        a = Counter(value(r.get(target, ''), kind) for r in xml_rows)
        b = Counter(value(r.get(source, ''), kind) for r in bulk_rows)
        if a != b:
            differences.append({'field': target, 'xml_only': dict(a - b), 'bulk_only': dict(b - a)})
    residual = lambda counter: [{'values': dict(zip(fields, signature)), 'count': count}
                               for signature, count in counter.items()]
    return {'status': 'bulk_rounding' if rounded == bulk else 'review',
            'xml_rows': len(xml_rows), 'bulk_rows': len(bulk_rows), 'differences': differences,
            'unmatched_xml_rows': residual(xml - bulk), 'unmatched_bulk_rows': residual(bulk - xml),
            'fields_checked': list(fields)}


def audit(root, limit=0):
    root = Path(root)
    inventory = sqlite3.connect('file:' + str((root / 'inventory.sqlite3').resolve()) + '?mode=ro', uri=True)
    inventory.row_factory = sqlite3.Row
    sql = "SELECT * FROM filings WHERE status IN ('verified','review') ORDER BY filing_date,accession"
    if limit:
        sql += ' LIMIT ' + str(int(limit))
    selected = [dict(r) for r in inventory.execute(sql)]
    queue_counts = dict(inventory.execute('SELECT status,count(*) FROM filings GROUP BY status'))
    sources = {r['source_key']: dict(r) for r in inventory.execute('SELECT * FROM sources')}
    inventory.close()
    by_shard = defaultdict(list)
    for row in selected:
        by_shard[row['shard']].append(row)
    by_quarter = defaultdict(dict)
    failed, source_issues, source_field_failures = [], [], []
    source_fields_checked = 0
    checksum_documents = 0
    for shard, rows in by_shard.items():
        db = sqlite3.connect('file:' + str((root / shard).resolve()) + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        for row in rows:
            stored = db.execute('SELECT * FROM documents WHERE accession=?', (row['accession'],)).fetchone()
            try:
                if stored is None:
                    raise ValueError('Queue points to missing document')
                for key in ('source', 'parsed'):
                    data = gzip.decompress(stored[key + '_gzip'])
                    digest = hashlib.sha256(data).hexdigest()
                    if (digest != stored[key + '_sha256'] or digest != row[key + '_sha256']
                            or len(data) != stored[key + '_bytes']):
                        raise ValueError(key + ' checksum or length mismatch')
                parsed = json.loads(gzip.decompress(stored['parsed_gzip']))
                checksum_documents += 1
                if parsed:
                    checked, field_failures = compare_original(gzip.decompress(stored['source_gzip']), parsed)
                    source_fields_checked += checked
                    if field_failures:
                        source_field_failures.append({'accession': row['accession'], 'failures': field_failures})
                document_audit = json.loads(stored['audit_json'])
                if document_audit['issues']:
                    source_issues.append({'accession': row['accession'], 'issues': document_audit['issues']})
                if parsed and row['bulk_source']:
                    by_quarter[row['bulk_source']][row['accession']] = parsed
            except Exception as exc:
                failed.append({'accession': row['accession'], 'error': str(exc)})
        db.close()
    comparison_counts = Counter()
    comparisons = []
    tables = {'NONDERIV_TRANS': ('transactions', 'non_derivative'), 'DERIV_TRANS': ('transactions', 'derivative'),
              'NONDERIV_HOLDING': ('holdings', 'non_derivative'), 'DERIV_HOLDING': ('holdings', 'derivative')}
    for quarter, parsed in by_quarter.items():
        source = sources[quarter]
        path = root / source['path']
        if hashlib.sha256(path.read_bytes()).hexdigest() != source['sha256']:
            raise ValueError('Quarterly source checksum mismatch: ' + quarter)
        with zipfile.ZipFile(path) as archive:
            names = {Path(n).stem.upper(): n for n in archive.namelist()}
            for table, (category, kind) in tables.items():
                bulk = defaultdict(list)
                with archive.open(names[table]) as stream:
                    reader = csv.DictReader(io.TextIOWrapper(stream, encoding='utf-8-sig'), delimiter='\t')
                    headers = reader.fieldnames
                    for record in reader:
                        if record['ACCESSION_NUMBER'] in parsed:
                            bulk[record['ACCESSION_NUMBER']].append(record)
                for accession, document in parsed.items():
                    xml_rows = [r for r in document[category] if r['table'] == kind]
                    result = compare(xml_rows, bulk[accession], headers)
                    comparison_counts[result['status']] += 1
                    if result['status'] != 'exact':
                        comparisons.append({'accession': accession, 'table': table, **result})
    return {'created_at': datetime.now(timezone.utc).isoformat(), 'selected_committed_documents': len(selected),
            'queue_counts_at_audit': queue_counts, 'checksum_documents_passed': checksum_documents,
            'checksum_failures': failed, 'source_review_issues': source_issues,
            'original_xml_financial_fields_checked': source_fields_checked,
            'original_xml_field_failures': source_field_failures,
            'financial_table_comparisons': dict(comparison_counts), 'differences': comparisons,
            'scope': 'Committed documents only. Compares available mapped fields in all four financial tables to the independent SEC bulk archives. Numeric rounding is a diagnostic only; originals and normalized data are never altered. Footnotes and owners receive count checks in collection but are not independently field-compared here.',
            'complete_backfill': False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.root, args.limit)
    atomic_write(args.output, json.dumps(report, indent=2).encode())
    print(json.dumps({k: v for k, v in report.items() if k not in {'differences', 'scope'}}), flush=True)
    if report['checksum_failures'] or report['original_xml_field_failures']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
