"""Retain archived prices while upgrading their storage receipt format.

This is an offline format migration, with no acquisition or lookup capability.
Original receipts stay private and hashed; numerical data is never recomputed.
"""
from copy import deepcopy
import json
from pathlib import Path

import quantity_estimation as q


NORMAL_METHODS = {'sec_same_quarter_median', 'saved_quarter_close'}


def normalize_archived_close(original: dict) -> dict:
    """Convert the archived v1 listing envelope into the saved-close schema."""
    listing = original.get('provider_listing', {})
    identifiers = [value for key, value in listing.items()
                   if key.lower().startswith('listing')
                   and key.lower().endswith(('identifier', 'id'))] if isinstance(listing, dict) else []
    if (not original.get('listing_id') or identifiers != [original['listing_id']]
            or not isinstance(original.get('method'), str) or not original['method']):
        raise q.QuantityEstimationError('archived receipt lacks its exact listing identifier')
    ref = deepcopy(original)
    ref['method'] = 'saved_quarter_close'
    ref['source_listing'] = {
        'ticker': listing.get('ticker'), 'mic': listing.get('operatingMic'),
        'currency': listing.get('tradingCurrency'), 'exchange': listing.get('exchangeCode'),
        'listing_id': original['listing_id'],
    }
    ref['source_response_sha256'] = ref.pop('provider_response_sha256', None)
    ref.pop('provider_listing', None)
    ref['original_receipt'] = deepcopy(original)
    ref['original_receipt_sha256'] = q.canonical_json_hash(original)
    errors = q.validate_reference(ref)
    if errors:
        raise q.QuantityEstimationError('invalid archived close: ' + '; '.join(errors))
    return ref


def migrate_saved_prices(root: Path) -> dict:
    """Preflight all changes; resume safely after interrupted individual writes.

    Both receipt generations remain in the ledger until every dependent file
    is rewritten. Callers must finish this migration before validation/publishing.
    Snapshot restore invokes it in staging, inside the existing rollback boundary.
    """
    root = Path(root)
    paths = [root / '.cache' / name for name in (
        'quarter_close_prices.json', 'quantity_estimation_evidence.json')]
    books, replacements, input_bytes = {}, {}, {}
    for path in paths:
        if not path.exists():
            continue
        if path.is_symlink():
            raise q.QuantityEstimationError('price evidence must be a regular file')
        input_bytes[path] = path.read_bytes()
        book = q.load_book(path)
        if json.loads(input_bytes[path]) != book:
            raise q.QuantityEstimationError('price evidence changed during migration read')
        for old_id, old in book.get('references', {}).items():
            if q.canonical_json_hash(old) != old_id:
                raise q.QuantityEstimationError('changed archived price reference')
            if old.get('method') in NORMAL_METHODS:
                if q.validate_reference(old):
                    raise q.QuantityEstimationError('invalid saved quantity reference')
                continue
            new = normalize_archived_close(old)
            replacements[old_id] = new
        books[path] = book
    if not replacements:
        return {'references': 0, 'annotations': 0, 'files': 0}

    changes, annotations = [], 0
    def rewrite(node):
        nonlocal annotations
        if isinstance(node, dict):
            annotation = node.get('quantity_estimate')
            if isinstance(annotation, dict) and annotation.get('reference_id') in replacements:
                new = replacements[annotation['reference_id']]
                old = new['original_receipt']
                expected = {'policy_version': q.POLICY_VERSION,
                            'reference_id': q.canonical_json_hash(old),
                            'method': old['method'], 'unit': old['unit']}
                if annotation != expected:
                    raise q.QuantityEstimationError('annotation disagrees with archived receipt')
                node['quantity_estimate'] = {**annotation, 'reference_id': q.canonical_json_hash(new),
                                             'method': new['method']}
                annotations += 1
            for value in node.values():
                rewrite(value)
        elif isinstance(node, list):
            for value in node:
                rewrite(value)
    for directory in (root / 'data/funds', root / 'data/stocks'):
        if directory.is_symlink():
            raise q.QuantityEstimationError('data directory must not be a symlink')
        for path in sorted(directory.glob('*.json')):
            if path.is_symlink():
                raise q.QuantityEstimationError('data file must not be a symlink')
            before = path.read_bytes()
            # Avoid parsing unaffected large files.
            if b'quantity_estimate' not in before:
                continue
            data = json.loads(before)
            count = annotations
            rewrite(data)
            if annotations != count:
                changes.append((path, before, data))
    for path, before, _ in changes:
        if path.read_bytes() != before:
            raise q.QuantityEstimationError('data changed during receipt migration preflight')
    for path, before in input_bytes.items():
        if path.read_bytes() != before:
            raise q.QuantityEstimationError('price evidence changed during migration preflight')
    for path, book in books.items():
        staged = deepcopy(book)
        for old_id, new in replacements.items():
            if old_id in staged['references']:
                staged['references'][q.canonical_json_hash(new)] = new
        q.atomic_json(path, staged)
    for path, _, data in changes:
        q.atomic_json(path, data)
    for path, book in books.items():
        book['references'] = {
            q.canonical_json_hash(replacements[key]) if key in replacements else key:
            replacements.get(key, value) for key, value in book['references'].items()}
        q.atomic_json(path, book)
    return {'references': len(replacements), 'annotations': annotations, 'files': len(changes)}
