"""Reviewed, dated display continuity; never changes a position's identity.

The graph joins search results and documents corporate actions. It grants no
permission to combine holdings, convert quantities, or enable quote lookups.
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

REVIEW_SHA256 = 'dfb38958f5e0890b8ada42aab0a1674d718a56a19ec9a6ec59ca2adc71881c20'
SCHEMA_VERSION = 1


def validate_history(document: dict) -> dict:
    if not isinstance(document, dict) or document.get('schema_version') != SCHEMA_VERSION:
        raise ValueError('unsupported security-history schema')
    raw_date = document.get('reviewed_as_of')
    if not isinstance(raw_date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', raw_date):
        raise ValueError('invalid security-history review date')
    reviewed = date.fromisoformat(raw_date)
    groups = document.get('groups')
    if not isinstance(groups, dict) or not groups:
        raise ValueError('security history must contain reviewed groups')
    seen = set()
    for key, group in groups.items():
        if not isinstance(group, dict):
            raise ValueError('invalid security-history group')
        kind = group.get('instrument_type')
        cusips = group.get('cusips', [])
        events = group.get('events', [])
        if (not isinstance(cusips, list) or not isinstance(events, list)
                or kind not in {'EQUITY', 'PREF', 'WARRANT'}
                or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-/]{0,19}', group.get('ticker', ''))
                or not isinstance(group.get('name'), str) or not group['name'].strip()
                or group.get('kind') not in {'COMMON', 'ETF', 'PREFERRED', 'WARRANT'}
                or len(cusips) < 2 or len(events) != len(cusips) - 1
                or key != f'{cusips[0]}|{kind}'):
            raise ValueError(f'invalid security-history group: {key}')
        for cusip in cusips:
            identity = f'{cusip}|{kind}'
            if not isinstance(cusip, str) or not re.fullmatch(r'[A-Z0-9]{9}', cusip) or identity in seen:
                raise ValueError(f'duplicate or invalid history identity: {identity}')
            seen.add(identity)
        previous = date.min
        for index, event in enumerate(events):
            if (not isinstance(event, dict)
                    or not isinstance(event.get('effective_date'), str)
                    or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', event['effective_date'])):
                raise ValueError('invalid security-history event date')
            effective = date.fromisoformat(event['effective_date'])
            if (event.get('from_cusip') != cusips[index]
                    or event.get('to_cusip') != cusips[index + 1]
                    or not previous < effective <= reviewed
                    or not isinstance(event.get('description'), str)
                    or not event['description'].strip()
                    or not isinstance(event.get('sources'), list)
                    or not event['sources']):
                raise ValueError(f'invalid security-history transition: {key}')
            previous = effective
            for source in event['sources']:
                if not isinstance(source, str) or re.search(r'[\s<>"\']', source):
                    raise ValueError(f'unsafe security-history source: {key}')
                url = urlsplit(source)
                if url.scheme != 'https' or not url.hostname or url.username or url.password:
                    raise ValueError(f'unsafe security-history source: {key}')
    return document


@lru_cache(maxsize=1)
def history_review() -> dict:
    raw = Path(__file__).with_name('reviewed_security_history.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError('security-history review checksum mismatch')
    return validate_history(json.loads(raw))


def public_identity_history() -> dict:
    review = history_review()
    return {key: review[key] for key in ('schema_version', 'reviewed_as_of', 'groups')}


def historical_security_keys(review: dict | None = None) -> set[str]:
    """Retired instruments, not option contracts referring to an underlying."""
    keys = {
        f"{cusip}|{group['instrument_type']}"
        for group in history_review()['groups'].values()
        for cusip in group['cusips'][:-1]
    }
    for key, entry in (review or {}).get('display_mappings', {}).items():
        if (entry.get('ticker_temporality') == 'historical_only'
                and entry.get('match_kind') == 'exact_cusip'
                and key.endswith(('|EQUITY', '|PREF', '|WARRANT', '|NOTE'))):
            keys.add(key)
    return keys
