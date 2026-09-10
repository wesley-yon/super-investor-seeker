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
from selective_rebuild import (  # noqa: E402
    RebuildCache, code_fingerprint, dependency_paths, digest, dirty_funds,
    evidence_cusips, evidence_order, fund_hashes,
)
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


def inventory(registry, previous=None):
    result = {}
    previous = previous or {}
    registry_key = digest(registry_identity(registry))
    for path in sorted(p.FUNDS_DIR.glob('*.json')):
        if path.is_symlink():
            raise p.FundDataError(f'fund inventory must not follow a symlink: {path}')
        raw = path.read_bytes()
        sha256 = hashlib.sha256(raw).hexdigest()
        cached = previous.get(path.name, {})
        if (cached.get('sha256') == sha256 and cached.get('registry_key') == registry_key
                and isinstance(cached.get('evidence_order'), list)):
            result[path.name] = cached
            continue
        fund = json.loads(raw)
        cusips, ids, reported = set(), set(), {}
        for quarter in fund.get('quarters', []):
            for h in quarter.get('holdings', []):
                reported_cusip = p.normalize_security_identifier(h.get('reported_cusip') or h.get('cusip'))
                if reported_cusip:
                    reported[reported_cusip] = None
                cusip = str(h.get('cusip') or '').strip().upper()
                if cusip:
                    cusips.add(cusip)
                    ids.add(p.stock_lookup_id(cusip, p.published_holding_instrument_type(h, registry.get(cusip))))
        result[path.name] = {'sha256': sha256, 'registry_key': registry_key,
                             'cusips': sorted(cusips | set(reported)), 'ids': sorted(ids),
                             'evidence_order': list(reported)}
    return result


def public_state(state):
    # These flags change current holder eligibility even when fund bytes do not.
    return {'withheld': {str(cik): {'report_date': row['report_date'], 'reasons': sorted(row['reasons'])}
                         for cik, row in p._active_withheld_targets_by_cik(state).items()},
            'unverified': {str(cik): {date: sorted(reasons) for date, reasons in dates.items()}
                           for cik, dates in p._active_unverified_targets_by_cik(state).items()}}


@p._serialize_pipeline_maintenance
def capture(path=BASELINE):
    cache = RebuildCache(ROOT)
    registry = p.load_cusip_registry()
    p._atomic_write_json(path, {'version': 2, 'code': cache.code,
                               'funds': inventory(registry, cache.data.get('funds')),
                               'registry': registry_identity(registry),
                               'state': public_state(p.load_state())})


def affected(before, after, changed_cusips, changed_ciks=()):
    names = {name for name in set(before) | set(after)
             if before.get(name, {}).get('sha256') != after.get(name, {}).get('sha256')
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


def _identity_inputs(cache):
    return cache.relative_hashes([
        p.SEC_SECURITY_MASTER_PATH, p.SEC_SOURCE_STATE_PATH,
        ROOT / '.cache/reviewed_ticker_map.json',
    ])


def _quantity_inputs(cache):
    # Include the saved-price migration's inputs as well as the current books.
    return {'funds': fund_hashes(p.FUNDS_DIR), 'evidence': cache.relative_hashes([
        ROOT / '.cache/quantity_estimation_evidence.json',
        ROOT / '.cache/quarter_close_prices.json',
    ])}


def _stock_outputs():
    return [p.INDEX_PATH, p.FUNDS_INDEX_PATH, *p.STOCKS_DIR.glob('*.json')]


@p._serialize_pipeline_maintenance
def regenerate(path=BASELINE, *, full_rebuild=False):
    p._recover_interrupted_derived_publishes()
    try:
        baseline = json.loads(path.read_bytes())
        compatible = baseline.get('version') == 2 and baseline.get('code') == code_fingerprint(ROOT)
    except (OSError, ValueError, AttributeError):
        compatible = False
    cache = RebuildCache(ROOT, enabled=not full_rebuild and compatible)
    old = cache.data
    if not cache.usable:
        p.log.info('Complete regeneration fallback: %s', cache.reason)
    reused = []
    prior_registry = p.load_cusip_registry()
    current = inventory(prior_registry, old.get('funds'))
    old_funds = old.get('funds', {})
    state = p.load_state()

    # Peer health can change across funds. Reuse only a completely clean
    # inventory bound to every fund's bytes; otherwise run the full peer pass.
    health_inputs = {name: entry['sha256'] for name, entry in current.items()}
    if old.get('health_inputs') == health_inputs and old.get('healthy_keys') is not None:
        health_inventory = ({}, {tuple(key) for key in old['healthy_keys']})
        reused.append('quarter_health')
    else:
        health_inventory = p.inventory_published_quarter_health_issues()
    p.enforce_published_quarter_health(state, health_inventory=health_inventory)
    current = inventory(prior_registry, current)
    changed = dirty_funds(old_funds, current)
    identity_inputs = _identity_inputs(cache)
    source_unchanged = cache.usable and old.get('identity_inputs') == identity_inputs
    if source_unchanged:
        upgraded = False
        reused.append('resolution_rules')
    else:
        upgraded = upgrade_master_resolution_rules()
    if not upgraded:
        extend_master_for_changed_funds([p.FUNDS_DIR / name for name in sorted(changed) if name in current])
    identity_inputs = _identity_inputs(cache)
    source_unchanged = cache.usable and old.get('identity_inputs') == identity_inputs

    registry_paths = [p.CUSIP_REGISTRY_PATH, p.LEGACY_CUSIP_REGISTRY_PATH]
    registry_reusable = source_unchanged and cache.output_matches('registry', registry_paths)
    registry_cusips = (evidence_cusips(old_funds, changed) | evidence_cusips(current, changed)
                      if registry_reusable else None)
    if registry_cusips == set():
        registry = p.CusipRegistry(prior_registry, observed_cusips=evidence_cusips(current))
        reused.append('registry')
    else:
        registry = p.build_cusip_registry(
            affected_cusips=registry_cusips,
            fund_paths=dependency_paths(p.FUNDS_DIR, current, registry_cusips)
            if registry_cusips is not None else None,
        )
    if 'registry' not in reused or not cache.output_matches('labels', [p.SECURITY_LABELS_PATH]):
        p.write_security_labels(registry)
    else:
        reused.append('labels')
    if 'registry' not in reused:
        issues = p.validate_cusip_registry(current_cusips=evidence_cusips(current))
        if issues:
            raise p.FundDataError('SEC registry publication gate failed: ' + '; '.join(issues))

    registry_keys = {key: digest(value) for key, value in registry_identity(registry).items()}
    old_registry_keys = old.get('registry_keys', {})
    changed_cusips = {key for key in old_registry_keys.keys() | registry_keys.keys()
                     if old_registry_keys.get(key) != registry_keys.get(key)}
    names, _ = affected(old_funds, current, changed_cusips)
    p.canonicalize_fund_files(preserve_position_identity=True,
                             fund_paths=[p.FUNDS_DIR / name for name in sorted(names) if name in current])

    # A receipt is reusable only when the previous calculation made no further
    # changes to these exact inputs. Changed peer/evidence inputs rerun globally.
    quantity_inputs = _quantity_inputs(cache)
    if old.get('quantity_inputs') == quantity_inputs and old.get('quantity_outputs') == quantity_inputs:
        reused.append('quantity')
    else:
        p.repair_zero_share_holdings_in_place()
    quantity_outputs = _quantity_inputs(cache)
    current = inventory(registry, current)
    hash_names = dirty_funds(old_funds, current)
    p.upgrade_composition_hashes_in_place(
        fund_paths=[p.FUNDS_DIR / name for name in sorted(hash_names) if name in current])
    final = inventory(registry, current)
    old_state, new_state = old.get('state', {'withheld': {}, 'unverified': {}}), public_state(state)
    changed_ciks = {str(cik) for kind in new_state
                    for cik in set(old_state[kind]) | set(new_state[kind])
                    if old_state[kind].get(cik) != new_state[kind].get(cik)}
    names, stock_ids = affected(old_funds, final, changed_cusips, changed_ciks)
    stock_outputs_match = cache.output_matches('stocks', _stock_outputs())
    rebuild_all_stocks = not cache.usable or not stock_outputs_match
    rebuilt_stock_count = 0
    if not names and not changed_cusips and not changed_ciks and stock_outputs_match:
        reused.append('stocks_and_indexes')
    else:
        rebuilt_stock_count = p.regenerate_stock_files_and_index(
            state=state, stock_ids=None if rebuild_all_stocks else stock_ids)

    # Cached aggregates contain all identifiers, including currently clean
    # ones. Recompute touched identifiers from every contributing fund, then
    # restore first-observation order so ties and report bytes match a full scan.
    report_names = dirty_funds(old_funds, final)
    report_cusips = evidence_cusips(old_funds, report_names) | evidence_cusips(final, report_names) | changed_cusips
    if cache.usable and isinstance(old.get('health_records'), dict):
        records = {key: value for key, value in old['health_records'].items() if key not in report_cusips}
        if report_cusips:
            records.update(p.aggregate_ticker_health(
                registry, fund_paths=dependency_paths(p.FUNDS_DIR, final, report_cusips), cusips=report_cusips))
        else:
            reused.append('ticker_health_aggregation')
        order = evidence_order(final)
        if set(records) != set(order):
            records = p.aggregate_ticker_health(registry)
        else:
            records = {key: records[key] for key in order}
    else:
        records = p.aggregate_ticker_health(registry)
    p.write_ticker_health_report(records=records)
    cache.save({
        'funds': final, 'registry_keys': registry_keys, 'state': new_state,
        'identity_inputs': identity_inputs, 'health_records': records,
        'health_inputs': health_inputs,
        'healthy_keys': sorted(health_inventory[1]) if not health_inventory[0] else None,
        'quantity_inputs': quantity_inputs, 'quantity_outputs': quantity_outputs,
        'outputs': {
            'registry': cache.relative_hashes(registry_paths),
            'labels': cache.relative_hashes([p.SECURITY_LABELS_PATH]),
            'stocks': cache.relative_hashes(_stock_outputs()),
        },
    })
    summary = {'changed_funds': len(names),
               'rebuilt_stock_ids': rebuilt_stock_count,
               'registry_identity_changes': len(changed_cusips),
               'registry_cusips_rebuilt': len(registry) if registry_cusips is None else len(registry_cusips),
               'reused_phases': reused, 'full_rebuild': not cache.usable}
    p.log.info('Selective regeneration: %s', summary)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['capture', 'regenerate'])
    parser.add_argument('--baseline', type=Path, default=BASELINE)
    parser.add_argument('--full-rebuild', action='store_true', help='Ignore acceleration state and rebuild all derived outputs')
    args = parser.parse_args()
    if args.action == 'capture':
        capture(args.baseline)
    else:
        regenerate(args.baseline, full_rebuild=args.full_rebuild)
