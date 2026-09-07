#!/usr/bin/env python3
"""Fatal publication gate for reviewed mappings and measured holding-row coverage."""
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import date
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from reviewed_ticker_map import load_review, REVIEW_COMMIT  # noqa: E402


def count_fund(path):
    counts = Counter()
    identities = {}
    for quarter in json.loads(path.read_bytes()).get('quarters', []):
        period = quarter.get('report_date', '')
        for holding in quarter.get('holdings', []):
            kind = holding.get('holding_type')
            cusip = str(holding.get('reported_cusip') or holding.get('cusip') or '').strip().upper()
            if kind != 'EQUITY' or not cusip:
                continue
            key = cusip + '|EQUITY'
            ticker = holding.get('ticker')
            counts[(period, 'rows')] += 1
            if ticker:
                counts[(period, 'resolved')] += 1
            identities.setdefault(key, set()).add(ticker)
    return counts, identities


def check_coverage(totals, target, latest):
    for period in sorted({target, latest}):
        rows, resolved = totals[(period, 'rows')], totals[(period, 'resolved')]
        if not rows or resolved * 100 < rows * 98:
            raise ValueError(f'{period}: EQUITY coverage {resolved}/{rows} is below 98%; retain last published snapshot')


def main():
    review = load_review(ROOT, required=True)
    registry = json.loads((ROOT / 'data/cusip_registry.json').read_bytes())
    totals, identities = Counter(), {}
    with ProcessPoolExecutor(max_workers=min(4, os.cpu_count() or 1)) as pool:
        for counts, seen in pool.map(count_fund, sorted((ROOT / 'data/funds').glob('*.json')), chunksize=20):
            totals.update(counts)
            for key, tickers in seen.items():
                identities.setdefault(key, set()).update(tickers)
    problems = []
    for key, entry in review['mappings'].items():
        cusip, kind = key.split('|')
        if key in identities and identities[key] != {entry['ticker']}:
            problems.append(f'{key}: holdings differ from reviewed ticker')
        public = registry.get(cusip)
        if public and public.get('type') != kind:
            typed = public.get('instrument_mappings', {}).get(kind)
            if typed is None or typed.get('ticker') != entry['ticker']:
                problems.append(f'{key}: missing exact instrument mapping')
        if public and public.get('type') == kind:
            if public.get('ticker') != entry['ticker']:
                problems.append(f'{key}: registry differs from reviewed ticker')
            if entry.get('price_lookup_allowed') is False:
                if public.get('price_lookup_allowed') is not False:
                    problems.append(f'{key}: missing historical quote exclusion')
                stock = ROOT / 'data/stocks' / (cusip + '.json')
                if stock.exists() and json.loads(stock.read_bytes()).get('price_lookup_allowed') is not False:
                    problems.append(f'{key}: stock missing historical quote exclusion')
    if problems:
        raise ValueError('; '.join(problems[:20]))
    periods = sorted(period for period, field in totals if field == 'rows' and period <= date.today().isoformat())
    if not periods:
        raise ValueError('no EQUITY holding population')
    check_coverage(totals, review['target']['quarter'], periods[-1])
    report = {'ok': True, 'review_commit': REVIEW_COMMIT, 'review_as_of': review['as_of'],
              'quarter_coverage': {period: {'rows': totals[(period, 'rows')],
                 'resolved': totals[(period, 'resolved')],
                 'percent': 100 * totals[(period, 'resolved')] / totals[(period, 'rows')]}
                 for period in periods}}
    output = ROOT / '.cache/reviewed_coverage_report.json'
    output.write_text(json.dumps(report, indent=2) + '\n')
    age = (date.today() - date.fromisoformat(review['as_of'])).days
    if age >= 30:
        print('::warning::Reviewed identity sources are due for monthly revalidation; publication blocks after 90 days')
    print(json.dumps({'ok': True, 'latest': periods[-1], **report['quarter_coverage'][periods[-1]]}))


if __name__ == '__main__':
    main()
