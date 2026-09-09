"""Checksummed exact preferred-interest review; never infer from issuer names."""
from __future__ import annotations
from functools import lru_cache
import hashlib
import json
from pathlib import Path

REVIEW_SHA256 = '70b2aacf17b931199dbc1790e871355d238d8f4984421385bbe7d5072aa908ef'
REVIEW_SOURCE = 'reviewed_primary_classification'

@lru_cache(maxsize=1)
def classification_review() -> dict:
    raw = Path(__file__).with_name('reviewed_preferred_classifications.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError('preferred-classification review checksum mismatch')
    return json.loads(raw)['securities']


def reviewed_preferred_type(holding: dict, parsed_type: str) -> str | None:
    if parsed_type not in {'EQUITY', 'PREF', 'NOTE'}:
        return None
    if str(holding.get('put_call') or '').strip().upper() in {'CALL', 'PUT'}:
        return None
    if str(holding.get('holding_type') or holding.get('option_type') or '').upper() in {'CALL', 'PUT', 'OPT'}:
        return None
    for field in ('class', 'reported_class'):
        text = str(holding.get(field) or '').strip().upper()
        if text in {'CALL', 'PUT', 'OPT', 'OPTION', 'OPTIONS', 'EQUITY OPTION', 'ETF OPTION'}:
            return None
    cusip = str(holding.get('cusip') or '').strip().upper()
    reported = str(holding.get('reported_cusip') or cusip).strip().upper()
    entry = classification_review().get(cusip)
    if reported != cusip or not entry or parsed_type not in entry['from_types']:
        return None
    return entry['to_type'] if entry['to_type'] != parsed_type else None


def public_preferred_metadata(registry: dict) -> dict:
    """Only corrected corpus identities can publish names and old-link routes."""
    accepted = {c: r for c, r in classification_review().items()
                if registry.get(c, {}).get('type') == r['to_type']}
    return {
        'preferred_type_corrections': {
            f'{c}|{t}': r['to_type'] for c, r in accepted.items()
            for t in r['from_types'] if t != r['to_type']
        },
        'instrument_names': {c: r['name'] for c, r in accepted.items()},
    }
