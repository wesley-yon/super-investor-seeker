"""Pinned private review layer for display identities; never rewrites SEC evidence.

Only the reviewed bytes selected in code can extend the resolver. SEC conflicts
stop publication instead of silently changing a reviewed identity. Historical
identity retention is independent of permission to request a current quote.
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re

from sec_security_master import SecurityMasterError

REVIEW_COMMIT = '24a837e856d2e0cb5cccee19e307bd3217432deb'
REVIEW_PATH = 'reviewed-maps/2026-09-07/candidate-map.json'
REVIEW_BLOB = '9023e68b4a7e953bab7c5e8765b30e7bad725110'
REVIEW_SHA256 = '1777f853a18fff01292647584d11350f646dbf04e2f56a71cdf1dbf5445d03b2'
CACHE_RELATIVE_PATH = Path('.cache/reviewed_ticker_map.json')
REVIEW_SOURCE = 'reviewed_primary_identity'
REVIEW_MAX_AGE_DAYS = 90


def validate_review_bytes(raw: bytes, *, as_of: date | None = None) -> dict:
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise SecurityMasterError('reviewed map checksum differs from approved revision')
    document = json.loads(raw)
    age = ((as_of or date.today()) - date.fromisoformat(document['as_of'])).days
    if not 0 <= age <= REVIEW_MAX_AGE_DAYS:
        raise SecurityMasterError('reviewed map requires source revalidation before publication')
    mappings = document['mappings']
    if len(mappings) != 10532:
        raise SecurityMasterError('reviewed map population differs from approved revision')
    for key, entry in mappings.items():
        if (not re.fullmatch(r'[A-Z0-9]{9}\|(EQUITY|PREF|WARRANT)', key)
                or entry.get('baseline_record') != key
                or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-^/]{0,19}', entry['ticker'])):
            raise SecurityMasterError(f'invalid reviewed identity: {key}')
    return document


@lru_cache(maxsize=4)
def _read_review(path: str, mtime_ns: int, size: int, today: date) -> dict:
    return validate_review_bytes(Path(path).read_bytes(), as_of=today)


def load_review(root: Path, *, required: bool = False) -> dict | None:
    path = Path(root) / CACHE_RELATIVE_PATH
    if path.is_symlink():
        raise SecurityMasterError('reviewed map must not be a symlink')
    if not path.exists():
        if required:
            raise SecurityMasterError('approved private reviewed map is missing')
        return None
    stat = path.stat()
    return _read_review(str(path.resolve()), stat.st_mtime_ns, stat.st_size, date.today())


def apply_review(master: dict, root: Path, *, document: dict | None = None) -> dict:
    """Return a display-only projection, retaining the original SEC document."""
    review = document if document is not None else load_review(root)
    if review is None or not isinstance(master.get('records'), dict):
        return master
    records = dict(master['records'])
    conflicts = []
    for key, accepted in review['mappings'].items():
        original = records.get(key, {})
        ticker = accepted['ticker']
        if original.get('mapping_status') == 'resolved' and original.get('ticker') != ticker:
            conflicts.append(key)
            continue
        cusip, kind = key.split('|')
        record = dict(original)
        record["reviewed_identity"] = True
        if original.get('mapping_status') != 'resolved':
            record.update(cusip=cusip, instrument_type=kind,
                          mapping_status='resolved', ticker=ticker,
                          ticker_source=REVIEW_SOURCE,
                          ticker_as_of=accepted['ticker_as_of'])
        if accepted.get('price_lookup_allowed') is False:
            record.update(price_lookup_allowed=False,
                          trading_status='historical_retired_identity')
        records[key] = record
    if conflicts:
        raise SecurityMasterError('SEC/reviewed ticker conflict requires review: '
                                  + ', '.join(sorted(conflicts)[:20]))
    return {**master, 'records': records}


def public_instrument_mappings(cusip: str, primary_type: str, records: dict) -> dict:
    """Retain exact reviewed row types when a CUSIP's aggregate type differs.

    Some old filings label a fund by its underlying assets (bond/preferred) or
    report a warrant as EQUITY. Do not alter a retained row's classified identity
    to fit the single aggregate registry type, or attach an issuer's common
    symbol to it. Each secondary mapping must have its own approved exact key.
    """
    result = {}
    for kind in ('EQUITY', 'PREF', 'WARRANT'):
        record = records.get(f'{cusip}|{kind}', {})
        if (kind == primary_type or not record.get('reviewed_identity')
                or record.get('mapping_status') != 'resolved'):
            continue
        result[kind] = {field: record[field] for field in (
            'ticker', 'ticker_source', 'ticker_as_of',
            'price_lookup_allowed', 'trading_status',
        ) if field in record}
    return result
