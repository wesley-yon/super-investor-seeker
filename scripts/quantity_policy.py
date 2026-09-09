#!/usr/bin/env python3
"""Plan and apply quantity estimates from saved prices and SEC filings."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import quantity_estimation as q  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    sub = parser.add_subparsers(dest='command', required=True)
    plan = sub.add_parser('plan'); plan.add_argument('--output', type=Path, required=True)
    apply = sub.add_parser('apply'); apply.add_argument('--plan', type=Path, required=True)
    sub.add_parser('migrate-receipts', help='Upgrade archived receipt storage without changing prices or quantities')
    args = parser.parse_args()
    cache = args.root / '.cache'
    evidence, market = (cache / name for name in ['quantity_estimation_evidence.json', 'quarter_close_prices.json'])
    read = lambda path: json.loads(path.read_text())
    if args.command == 'migrate-receipts':
        from saved_price_migration import migrate_saved_prices
        print(json.dumps(migrate_saved_prices(args.root)))
    elif args.command == 'plan':
        result = q.build_plan(args.root / 'data/funds', evidence_path=evidence, market_path=market)
        q.atomic_json(args.output, result)
        print(json.dumps({'targets': len(result['targets']), 'methods': dict(Counter(row['revised'].get('quantity_estimate', {}).get('method', 'unknown') for row in result['targets']))}))
    elif args.command == 'apply':
        print(json.dumps(q.apply_plan(read(args.plan), args.root / 'data/funds', evidence_path=evidence)))


if __name__ == '__main__':
    main()
