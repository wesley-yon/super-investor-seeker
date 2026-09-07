#!/usr/bin/env python3
"""Capture a pre-ingest baseline, then regenerate its complete dependency set.

Routine filing updates reuse the verified SEC master. A resolution-rule upgrade
replays saved SEC evidence once; new identities remain tickerless until that
evidence or scheduled SEC maintenance proves an exact mapping.
All publication gates remain mandatory; this command is not a publisher.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pipeline as p  # noqa: E402
from incremental_validation import checker_fingerprint  # noqa: E402
from sec_security_master import (  # noqa: E402
    DEFAULT_MAX_EVIDENCE_AGE_DAYS,
    DEFAULT_MIN_CONFIRMATION_DATES,
    DEFAULT_RECENT_WINDOW_DAYS,
    TICKER_RESOLUTION_RULES_VERSION,
    _retain_prior_mappings_with_unresolved_extensions,
)

BASELINE = ROOT / '.cache/incremental_update_baseline.json'
STAT_FIELDS = {'total_value', 'holder_count', 'first_seen', 'last_seen'}


def registry_identity(registry):
    return {key: {k: v for k, v in row.items() if k not in STAT_FIELDS}
            for key, row in registry.items()}


def inventory(registry):
    result = {}
    for path in sorted(p.FUNDS_DIR.glob('*.json')):
        raw = path.read_bytes()
        fund = json.loads(raw)
        cusips, ids = set(), set()
        for quarter in fund.get('quarters', []):
            for h in quarter.get('holdings', []):
                cusip = str(h.get('cusip') or '').strip().upper()
                if cusip:
                    cusips.add(cusip)
                    ids.add(p.stock_lookup_id(cusip, p.published_holding_instrument_type(h, registry.get(cusip))))
        result[path.name] = {'sha256': hashlib.sha256(raw).hexdigest(),
                             'cusips': sorted(cusips), 'ids': sorted(ids)}
    return result


def public_state(state):
    # These flags change current holder eligibility even when fund bytes do not.
    return {'withheld': {str(cik): {'report_date': row['report_date'], 'reasons': sorted(row['reasons'])}
                         for cik, row in p._active_withheld_targets_by_cik(state).items()},
            'unverified': {str(cik): {date: sorted(reasons) for date, reasons in dates.items()}
                           for cik, dates in p._active_unverified_targets_by_cik(state).items()}}


@p._serialize_pipeline_maintenance
def capture(path=BASELINE):
    registry = p.load_cusip_registry()
    p._atomic_write_json(path, {'version': 1, 'code': checker_fingerprint(ROOT),
                               'funds': inventory(registry),
                               'registry': registry_identity(registry),
                               'state': public_state(p.load_state())})


def affected(before, after, changed_cusips, changed_ciks=()):
    names = {name for name in set(before) | set(after)
             if before.get(name) != after.get(name)
             or Path(name).stem in changed_ciks
             or changed_cusips.intersection(before.get(name, {}).get('cusips', []))
             or changed_cusips.intersection(after.get(name, {}).get('cusips', []))}
    stock_ids = {stock_id for name in names for corpus in (before, after)
                 for stock_id in corpus.get(name, {}).get('ids', [])}
    return names, stock_ids


def upgrade_master_resolution_rules():
    """Replay saved proof once per rule version, under every production gate."""
    master = p.load_security_master(p.SEC_SECURITY_MASTER_PATH)
    version = master.get('policy', {}).get('resolution_rules_version', 0)
    if version > TICKER_RESOLUTION_RULES_VERSION:
        raise p.FundDataError(
            f'SEC ticker resolution rules version {version} is newer than '
            f'this code ({TICKER_RESOLUTION_RULES_VERSION}); refusing downgrade')
    if version == TICKER_RESOLUTION_RULES_VERSION:
        return False

    master, source = p.load_security_master_pair(
        master_path=p.SEC_SECURITY_MASTER_PATH,
        source_state_path=p.SEC_SOURCE_STATE_PATH)
    # Recheck the bound pair in case recovery replaced the initially read
    # master. Do not stamp a current marker onto evidence from newer rules.
    version = master.get('policy', {}).get('resolution_rules_version', 0)
    if version > TICKER_RESOLUTION_RULES_VERSION:
        raise p.FundDataError(
            f'SEC ticker resolution rules version {version} is newer than '
            f'this code ({TICKER_RESOLUTION_RULES_VERSION}); refusing downgrade')
    if version == TICKER_RESOLUTION_RULES_VERSION:
        return False
    universe = p.collect_security_master_universe()
    # Keep saved identity types and historical as-filed witnesses, including
    # keys no longer present in the current official list or fund corpus.
    # Display labels and previous ticker decisions are never replay inputs.
    for record in master.get('records', {}).values():
        base = {'cusip': record['cusip'], 'instrument_type': record['instrument_type']}
        identities = record.get('reported_identities', [])
        if not identities:
            universe.append({**base, **{field: record[field]
                                       for field in ('reported_issuer', 'reported_class')
                                       if field in record}})
        for identity in identities:
            evidence = [item for item in record.get('reported_identity_evidence', [])
                        if all(item.get(field) == value for field, value in identity.items())]
            universe.append({**base,
                             'reported_issuer': identity['reported_issuer'],
                             'reported_class': identity['reported_class'],
                             'reported_identity_evidence': evidence})
    policy = master.get('policy', {})
    rebuilt = p.rebuild_sec_security_master(
        source, universe,
        recent_window_days=policy.get('recent_window_days', DEFAULT_RECENT_WINDOW_DAYS),
        max_evidence_age_days=policy.get('max_evidence_age_days', DEFAULT_MAX_EVIDENCE_AGE_DAYS),
        min_confirmation_dates=policy.get('min_confirmation_dates', DEFAULT_MIN_CONFIRMATION_DATES))
    acceptance = p.audit_security_master(
        rebuilt, prior_master=master, as_of=datetime.now(timezone.utc))
    if not acceptance['ok']:
        raise p.FundDataError(
            'SEC ticker resolution rule upgrade failed the publication gate: '
            + '; '.join(acceptance['issues']))
    p.save_security_master_pair(
        rebuilt, source, master_path=p.SEC_SECURITY_MASTER_PATH,
        source_state_path=p.SEC_SOURCE_STATE_PATH)
    p.log.info('Replayed saved SEC evidence for ticker rules %s -> %s; resolved mappings %s -> %s',
               version, TICKER_RESOLUTION_RULES_VERSION,
               master.get('summary', {}).get('resolved', 0),
               rebuilt.get('summary', {}).get('resolved', 0))
    return True


def extend_master_for_changed_funds(paths):
    if not paths:
        return
    # The restored master supplies the existing identity keys. Avoid loading
    # the much larger source state unless an extension is actually required;
    # the complete source audit remains a mandatory publication gate.
    master = p.load_security_master(p.SEC_SECURITY_MASTER_PATH)
    records = master.get('records', {})
    additions = []
    for path in paths:
        fund = json.loads(path.read_bytes())
        for quarter in fund.get('quarters', []):
            for identity in p._security_universe_from_holdings(quarter.get('holdings', []), quarter.get('reported_identity_sources', [])):
                if p.security_key(identity['cusip'], identity['instrument_type']) not in records:
                    additions.append(identity)
    if not additions:
        return
    master, source = p.load_security_master_pair(master_path=p.SEC_SECURITY_MASTER_PATH,
                                                source_state_path=p.SEC_SOURCE_STATE_PATH)
    master = _retain_prior_mappings_with_unresolved_extensions(
        master, source, additions,
        new_identity_reason='sec_evidence_refresh_pending_new_identity')
    p.save_security_master_pair(master, source, master_path=p.SEC_SECURITY_MASTER_PATH,
                                source_state_path=p.SEC_SOURCE_STATE_PATH)
    p.log.info('Retained existing mappings; deferred ticker proof for %s new reported identity row(s)', len(additions))


@p._serialize_pipeline_maintenance
def regenerate(path=BASELINE):
    baseline = json.loads(path.read_bytes())
    if baseline.get('version') != 1 or baseline.get('code') != checker_fingerprint(ROOT):
        raise p.FundDataError('incremental baseline is incompatible; capture before ingestion with the current code')
    state = p.load_state()
    p.enforce_published_quarter_health(state)
    p.save_state(state)
    prior_registry = p.load_cusip_registry()
    current = inventory(prior_registry)
    changed, _ = affected(baseline['funds'], current, set())
    if not upgrade_master_resolution_rules():
        extend_master_for_changed_funds([p.FUNDS_DIR / name for name in sorted(changed) if name in current])
    registry = p.build_cusip_registry()
    p.write_security_labels(registry)
    issues = p.validate_cusip_registry(current_cusips=registry.observed_cusips)
    if issues:
        raise p.FundDataError('SEC registry publication gate failed: ' + '; '.join(issues))
    identity = registry_identity(registry)
    changed_cusips = {key for key in set(baseline['registry']) | set(identity)
                      if baseline['registry'].get(key) != identity.get(key)}
    names, _ = affected(baseline['funds'], current, changed_cusips)
    p.canonicalize_fund_files(preserve_position_identity=True,
                             fund_paths=[p.FUNDS_DIR / name for name in sorted(names) if name in current])
    # Quantity dependencies can cross funds. Keep this global evidence pass;
    # detect every resulting byte change before selecting derived stock files.
    p.repair_zero_share_holdings_in_place()
    p.upgrade_composition_hashes_in_place()
    final = inventory(registry)
    old_state, new_state = baseline['state'], public_state(state)
    changed_ciks = {str(cik) for kind in new_state
                    for cik in set(old_state[kind]) | set(new_state[kind])
                    if old_state[kind].get(cik) != new_state[kind].get(cik)}
    names, stock_ids = affected(baseline['funds'], final, changed_cusips, changed_ciks)
    p.regenerate_stock_files_and_index(state=state, stock_ids=stock_ids)
    p.write_ticker_health_report()
    summary = {'changed_funds': len(names), 'rebuilt_stock_ids': len(stock_ids),
               'registry_identity_changes': len(changed_cusips)}
    p.log.info('Incremental regeneration: %s', summary)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['capture', 'regenerate'])
    parser.add_argument('--baseline', type=Path, default=BASELINE)
    args = parser.parse_args()
    (capture if args.action == 'capture' else regenerate)(args.baseline)
