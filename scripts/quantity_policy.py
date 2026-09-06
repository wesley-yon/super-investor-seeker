#!/usr/bin/env python3
"""Prepare, audit, and apply local quantity estimates and Fiscal.ai exports."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import quantity_estimation as q  # noqa: E402


def prepare_requests(requests: list, catalog: list, master: dict, company_tickers: dict) -> dict:
    """Bind a USD listing to a dated SEC identity; never resolve a ticker here."""
    by_ticker = defaultdict(list)
    for company in catalog:
        if company.get('tradingCurrency') == 'USD' and company.get('micCode') in {'XNAS', 'XNYS', 'XASE', 'ARCX', 'BATS', 'IEXG'} and 'stock_prices' in company.get('availableDatasets', []):
            by_ticker[company['ticker']].append(company)
    sec_ciks = defaultdict(set)
    for row in company_tickers.values():
        sec_ciks[row['ticker']].add(str(int(row['cik_str'])))
    company_sha = q.canonical_json_hash(company_tickers)
    groups, skips, seen = {}, Counter(), set()
    def normalize(value):
        return re.sub(r'[^A-Z0-9]', '', str(value).upper())
    for request in requests:
        cusip, day = request['cusip'], request['report_date']
        if (cusip, day) in seen:
            continue
        seen.add((cusip, day))
        rec = master['records'].get(cusip + '|EQUITY', {})
        ticker = rec.get('ticker')
        if rec.get('mapping_status') != 'resolved' or rec.get('ticker_source') != 'sec_ftd':
            skips['no_exact_SEC_equity_mapping'] += 1
            continue
        choices = {c['companyKey']: c for c in by_ticker.get(ticker, [])}
        if len(choices) != 1:
            skips['no_unique_US_Fiscal_listing'] += 1
            continue
        company = next(iter(choices.values()))
        cik = str(company.get('cik') or '').lstrip('0')
        same_cik = bool(cik and cik in sec_ciks.get(ticker, set()))
        same_title = normalize(company['name']) in {normalize(t) for t in rec.get('symbol_validation_titles', [])}
        if not same_cik and not same_title:
            skips['Fiscal_issuer_does_not_match_SEC'] += 1
            continue
        intervals = [i for i in rec.get('symbol_intervals', []) if i.get('first_seen', '9999') <= day <= i.get('last_seen', '')]
        if len(intervals) != 1 or intervals[0].get('symbol') != ticker:
            skips['no_unambiguous_historical_SEC_symbol'] += 1
            continue
        identity = {'cusip': cusip, 'instrument_type': 'EQUITY', 'ticker': ticker, 'ticker_source': 'sec_ftd', 'issuer_cik': cik or None, 'issuer_match': 'sec_company_cik' if same_cik else 'sec_company_title', 'company_title': company['name'], 'sec_company_tickers_sha256': company_sha, 'interval': {k: intervals[0][k] for k in ['symbol', 'first_seen', 'last_seen', 'sources'] if k in intervals[0]}}
        group = groups.setdefault(company['companyKey'], {'company': company, 'requests': []})
        group['requests'].append({'cusip': cusip, 'report_date': day, 'price_date': q.quarter_close_date(day), 'sec_identity': identity})
    return {'schema_version': 1, 'groups': [groups[k] for k in sorted(groups)], 'skipped': dict(skips)}


def import_exports(groups: list, exports: list, market_path: Path) -> dict:
    by_key = {}
    for export in exports:
        key = export['companyKey']
        if key in by_key and by_key[key] != export:
            raise q.QuantityEstimationError(f'conflicting exports for {key}')
        by_key[key] = export
    book = q.load_book(market_path)
    rejected, accepted = [], 0
    for group in groups:
        company = group['company']
        for request in group['requests']:
            try:
                export = by_key.get(company['companyKey'])
                if export is None:
                    raise q.QuantityEstimationError('missing provider export')
                reference = q.fiscal_reference(export, request, company)
                # A refresh replaces this market observation, while previously
                # applied estimates keep their immutable evidence in the ledger.
                book['references'] = {key: row for key, row in book['references'].items() if (row['cusip'], row['report_date'], row['instrument_type']) != (reference['cusip'], reference['report_date'], reference['instrument_type'])}
                book['references'][q.canonical_json_hash(reference)] = reference
                accepted += 1
            except (q.QuantityEstimationError, TypeError, ValueError, KeyError, AttributeError) as exc:
                rejected.append({'company_key': company['companyKey'], 'cusip': request['cusip'], 'report_date': request['report_date'], 'reason': str(exc)})
    q.atomic_json(market_path, book)
    return {'accepted': accepted, 'rejected': rejected}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    sub = parser.add_subparsers(dest='command', required=True)
    plan = sub.add_parser('plan'); plan.add_argument('--output', type=Path, required=True)
    apply = sub.add_parser('apply'); apply.add_argument('--plan', type=Path, required=True)
    prepare = sub.add_parser('prepare-prices'); prepare.add_argument('--catalog', type=Path, nargs='+', required=True); prepare.add_argument('--output', type=Path, required=True)
    ingest = sub.add_parser('import-prices'); ingest.add_argument('--requests', type=Path, required=True); ingest.add_argument('--exports', type=Path, nargs='+', required=True); ingest.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    cache = args.root / '.cache'
    evidence, market, requests = (cache / name for name in ['quantity_estimation_evidence.json', 'quarter_close_prices.json', 'quarter_close_price_requests.json'])
    read = lambda path: json.loads(path.read_text())
    if args.command == 'plan':
        result = q.build_plan(args.root / 'data/funds', evidence_path=evidence, market_path=market)
        q.atomic_json(args.output, result)
        q.atomic_json(requests, {'schema_version': 1, 'requests': result['price_requests']})
        print(json.dumps({'targets': len(result['targets']), 'methods': dict(Counter(row['revised'].get('quantity_estimate', {}).get('method', 'unknown') for row in result['targets'])), 'price_requests': len(result['price_requests'])}))
    elif args.command == 'apply':
        print(json.dumps(q.apply_plan(read(args.plan), args.root / 'data/funds', evidence_path=evidence, request_path=requests)))
    elif args.command == 'prepare-prices':
        catalog = []
        for path in args.catalog:
            data = read(path)
            for page in data if isinstance(data, list) else [data]:
                catalog.extend(page['companies'])
        result = prepare_requests(read(requests)['requests'], catalog, read(cache / 'sec_security_master.json'), read(args.root / 'data/company_tickers.json'))
        q.atomic_json(args.output, result)
        print(json.dumps({'companies': len(result['groups']), 'skipped': result['skipped']}))
    else:
        manifest = read(args.requests)
        groups = manifest if isinstance(manifest, list) else manifest['groups']
        result = import_exports(groups, [row for path in args.exports for row in read(path)], market)
        q.atomic_json(args.report, result)
        print(json.dumps({'accepted': result['accepted'], 'rejections': dict(Counter(row['reason'] for row in result['rejected']))}))


if __name__ == '__main__':
    main()
