"""Validate normalized ownership entities and raw-field retention against XML."""
from collections import Counter
from datetime import datetime
from decimal import Decimal, localcontext
import json
import re
import xml.etree.ElementTree as ET

from .audit import SOURCE_PATHS, compare_original

FILING_PATHS = {
    'schema_version': 'schemaVersion', 'document_type': 'documentType',
    'period_of_report': 'periodOfReport', 'date_of_original_submission': 'dateOfOriginalSubmission',
    'issuer_cik': 'issuer/issuerCik', 'issuer_name': 'issuer/issuerName',
    'issuer_trading_symbol': 'issuer/issuerTradingSymbol',
    'no_securities_owned': 'noSecuritiesOwned', 'not_subject_to_section16': 'notSubjectToSection16',
    'form3_holdings_reported': 'form3HoldingsReported', 'form4_transactions_reported': 'form4TransactionsReported',
    'form5_holdings_reported': 'form5HoldingsReported', 'form5_transactions_reported': 'form5TransactionsReported',
    'aff10b5_one': 'aff10b5One', 'remarks': 'remarks',
}
OWNER_PATHS = {
    'owner_cik': 'reportingOwnerId/rptOwnerCik', 'owner_name': 'reportingOwnerId/rptOwnerName',
    'street1': 'reportingOwnerAddress/rptOwnerStreet1', 'street2': 'reportingOwnerAddress/rptOwnerStreet2',
    'city': 'reportingOwnerAddress/rptOwnerCity', 'state': 'reportingOwnerAddress/rptOwnerState',
    'zip_code': 'reportingOwnerAddress/rptOwnerZipCode', 'state_description': 'reportingOwnerAddress/rptOwnerStateDescription',
    'is_director': 'reportingOwnerRelationship/isDirector', 'is_officer': 'reportingOwnerRelationship/isOfficer',
    'is_ten_percent_owner': 'reportingOwnerRelationship/isTenPercentOwner', 'is_other': 'reportingOwnerRelationship/isOther',
    'officer_title': 'reportingOwnerRelationship/officerTitle', 'other_text': 'reportingOwnerRelationship/otherText',
}


def original_tree(body):
    candidates = re.findall(br'<XML>\s*([\s\S]*?)\s*</XML>', body, re.I) or [body]
    roots = []
    for candidate in candidates:
        if not re.search(br'<(?:\w+:)?ownershipDocument\b', candidate):
            continue
        if re.search(br'<!\s*(?:DOCTYPE|ENTITY)\b', candidate.replace(b'\x00', b''), re.I):
            raise ValueError('Unsafe declaration in stored ownership XML')
        root = ET.fromstring(candidate)
        if root.tag.rsplit('}', 1)[-1] == 'ownershipDocument':
            roots.append(root)
    if len(roots) != 1:
        raise ValueError('Expected one original ownership document')
    root = roots[0]
    for element in root.iter():
        element.tag = element.tag.rsplit('}', 1)[-1]
    return root


def text_value(element, path):
    node = element.find(path) if path else element
    if node is None:
        return ''
    nested = node.find('value')
    return ''.join((node if nested is None else nested).itertext()).strip()


def raw_fields(element):
    """Derive the documented path representation without invoking the parser."""
    collected = {}

    def add(path, value):
        collected.setdefault(path, []).append(value)

    stack = [(element, '')]
    while stack:
        node, path = stack.pop()
        for name, value in node.attrib.items():
            add((path + '/' if path else '') + '@' + name.rsplit('}', 1)[-1], value)
        children = list(node)
        if not children:
            add(path or '.', node.text or '')
            continue
        if node.text and node.text.strip():
            add((path + '/' if path else '') + 'text()', node.text)
        totals = Counter(child.tag for child in children)
        seen = Counter()
        pending = []
        for child in children:
            seen[child.tag] += 1
            label = child.tag + (f'[{seen[child.tag]}]' if totals[child.tag] > 1 else '')
            child_path = (path + '/' if path else '') + label
            pending.append((child, child_path))
            if child.tail and child.tail.strip():
                add(child_path + '/tail()', child.tail)
        stack.extend(reversed(pending))
    return {path: values[0] if len(values) == 1 else values for path, values in collected.items()}


def compare_source(body, parsed):
    financial_checked, failures = compare_original(body, parsed)
    counts = Counter(financial_fields=financial_checked)
    root = original_tree(body)

    def check(category, locator, field, original, normalized):
        counts[category] += 1
        if original != normalized:
            failures.append({'category': category, 'locator': locator, 'field': field,
                             'original': original, 'parsed': normalized})

    def refs(element, paths):
        result = {}
        for field, path in paths.items():
            node = element.find(path)
            if node is not None:
                ids = list(dict.fromkeys(child.attrib['id'] for child in node.iter('footnoteId') if 'id' in child.attrib))
                if ids:
                    result[field] = ids
        return result

    def fields(category, locator, element, record, paths):
        for field, path in paths.items():
            check(category, locator, field, text_value(element, path), record.get(field))
        if 'normalized_footnote_refs' in record:
            check('footnote_links', locator, 'normalized_footnote_refs', refs(element, paths), record['normalized_footnote_refs'])

    filing = parsed['filing']
    header = body.split(b'</SEC-HEADER>', 1)[0] if b'</SEC-HEADER>' in body else body[:20000]
    for pattern, field in [
        (br'ACCESSION NUMBER:\s*([0-9]{10}-[0-9]{2}-[0-9]{6})', 'accession'),
        (br'FILED AS OF DATE:\s*([0-9]{8})', 'filing_date'),
        (br'CONFORMED SUBMISSION TYPE:\s*([^\r\n]+)', 'document_type'),
    ]:
        match = re.search(pattern, header)
        if match:
            original = match.group(1).decode('ascii').strip()
            if field == 'filing_date':
                original = datetime.strptime(original, '%Y%m%d').date().isoformat()
            normalized = filing[field]
            if field == 'document_type':
                original, normalized = original.replace('/A', 'A'), normalized.replace('/A', 'A')
            check('submission_header_fields', 'filing', field, original, normalized)
    fields('filing_fields', 'filing', root, filing, FILING_PATHS)
    expected_raw = raw_fields(root)
    actual_raw = filing['raw_fields']
    for path in sorted(set(expected_raw) | set(actual_raw)):
        check('raw_paths', 'filing', path, expected_raw.get(path), actual_raw.get(path))
    owners = root.findall('reportingOwner')
    check('entity_counts', 'filing', 'reporting_owner_count', len(owners), filing['reporting_owner_count'])
    check('entity_counts', 'owners', 'rows', len(owners), len(parsed['owners']))
    for number, (element, owner) in enumerate(zip(owners, parsed['owners']), 1):
        fields('owner_fields', f'owner:{number}', element, owner, OWNER_PATHS)
    for category, path, field_paths in [
        ('footnotes', 'footnotes/footnote', {'text': ''}),
        ('signatures', 'ownerSignature', {'signature_name': 'signatureName', 'signature_date': 'signatureDate'}),
    ]:
        elements = root.findall(path)
        check('entity_counts', category, 'rows', len(elements), len(parsed[category]))
        for number, (element, record) in enumerate(zip(elements, parsed[category]), 1):
            fields(category + '_fields', f'{category}:{number}', element, record, field_paths)
            if category == 'footnotes':
                check('footnotes_fields', f'footnotes:{number}', 'footnote_id', element.attrib.get('id', ''), record['footnote_id'])
    for prefix, table in [('nonDerivative', 'non_derivative'), ('derivative', 'derivative')]:
        for suffix, category in [('Transaction', 'transactions'), ('Holding', 'holdings')]:
            source_rows = root.findall(prefix + 'Table/' + prefix + suffix)
            records = [r for r in parsed[category] if r['table'] == table]
            for number, (element, record) in enumerate(zip(source_rows, records), 1):
                locator = f'{table}:{category}:{number}'
                paths = {**SOURCE_PATHS, 'equity_swap_involved': 'transactionCoding/equitySwapInvolved'}
                check('footnote_links', locator, 'normalized_footnote_refs', refs(element, paths), record['normalized_footnote_refs'])
                check('financial_extra_fields', locator, 'equity_swap_involved', text_value(element, paths['equity_swap_involved']), record['equity_swap_involved'])
                if category == 'transactions':
                    shares = text_value(element, 'transactionAmounts/transactionShares')
                    price = text_value(element, 'transactionAmounts/transactionPricePerShare')
                    product = ''
                    if all(re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)', item) for item in [shares, price]):
                        a, b = Decimal(shares), Decimal(price)
                        if a >= 0 and b >= 0:
                            with localcontext() as context:
                                context.prec = max(28, len(a.as_tuple().digits) + len(b.as_tuple().digits) + 2)
                                product = format(a * b, 'f')
                    check('calculated_values', locator, 'transaction_value', product, record['transaction_value'])
                    check('calculated_values', locator, 'transaction_value_basis',
                          'reported_shares_times_reported_price' if product else '', record['transaction_value_basis'])
    names = ' | '.join(text_value(element, 'reportingOwnerId/rptOwnerName') for element in owners)
    ciks = ' | '.join(text_value(element, 'reportingOwnerId/rptOwnerCik') for element in owners)
    for record in [filing] + parsed['transactions'] + parsed['holdings']:
        locator = record.get('row_id', 'filing')
        check('owner_joins', locator, 'owner_names', names, record['owner_names'])
        check('owner_joins', locator, 'owner_ciks', ciks, record['owner_ciks'])
        check('owner_joins', locator, 'owners_json', parsed['owners'], json.loads(record['owners_json']))
    for category in ['owners', 'transactions', 'holdings', 'footnotes', 'signatures']:
        for number, record in enumerate(parsed[category], 1):
            for field in ['accession', 'filing_date', 'source_url']:
                check('filing_joins', f'{category}:{number}', field, filing[field], record[field])
    return dict(counts), failures
