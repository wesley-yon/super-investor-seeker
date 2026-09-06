"""Content-bound reuse of successful local checks, with full global reconciliation.

The optional SQLite file is an acceleration cache in the verified private
snapshot. It never supplies source data or a ticker decision. Every reuse binds
actual file bytes, checker code, and the relevant registry/calendar/quantity
inputs. Missing, incompatible, or damaged entries run the original checker.
Transactions are committed only after every publication gate succeeds.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import statistics
import sys
import zlib
from collections import Counter, defaultdict
from pathlib import Path

CACHE_RELATIVE_PATH = Path('.cache/validation_cache.sqlite3')
_SCHEMA = 1
_NON_IDENTITY_REGISTRY_FIELDS = {'total_value', 'holder_count', 'first_seen', 'last_seen'}


def _encode(value):
    if isinstance(value, frozenset):
        return {'$frozenset': [_encode(v) for v in sorted(value, key=repr)]}
    if isinstance(value, set):
        return {'$set': [_encode(v) for v in sorted(value, key=repr)]}
    if isinstance(value, tuple):
        return {'$tuple': [_encode(v) for v in value]}
    if isinstance(value, dict):
        if any(not isinstance(k, str) for k in value):
            return {'$mapping': [[_encode(k), _encode(v)] for k, v in value.items()]}
        return {k: _encode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_encode(v) for v in value]
    return value


def _decode(value):
    if isinstance(value, list):
        return [_decode(v) for v in value]
    if isinstance(value, dict):
        if set(value) == {'$set'}:
            return set(_decode(value['$set']))
        if set(value) == {'$frozenset'}:
            return frozenset(_decode(value['$frozenset']))
        if set(value) == {'$tuple'}:
            return tuple(_decode(value['$tuple']))
        if set(value) == {'$mapping'}:
            return {_decode(k): _decode(v) for k, v in value['$mapping']}
        return {k: _decode(v) for k, v in value.items()}
    return value


def _bytes(value):
    return json.dumps(_encode(value), sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _digest(value):
    return hashlib.sha256(_bytes(value)).hexdigest()


def _peer_digest(code, compiled):
    # The global peer corpus is large. Hash one canonical entry at a time
    # instead of materializing a second full JSON representation in memory.
    digest = hashlib.sha256(code.encode())
    for key in sorted(compiled):
        digest.update(_bytes([key, compiled[key]]))
        digest.update(b'\n')
    return digest.hexdigest()


def checker_fingerprint(root: Path) -> str:
    digest = hashlib.sha256(f'{_SCHEMA}:{sys.version_info[:2]}'.encode())
    for path in sorted(root.glob('*.py')):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


class ValidationCache:
    def __init__(self, validator, path: Path | None = None, *, reuse: bool = True):
        self.v = validator
        self.reuse = reuse
        self.path = Path(path or validator.ROOT / CACHE_RELATIVE_PATH)
        if self.path.is_symlink():
            raise ValueError('validation cache must not be a symlink')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.code = checker_fingerprint(validator.ROOT)
        self.counts = Counter()
        self.connection = sqlite3.connect(self.path)
        self.connection.execute('PRAGMA trusted_schema=OFF')
        try:
            if self.connection.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise sqlite3.DatabaseError('damaged acceleration cache')
            self.connection.execute('CREATE TABLE IF NOT EXISTS checks (kind TEXT, name TEXT, key TEXT NOT NULL, metadata TEXT NOT NULL, payload BLOB NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(kind,name)) WITHOUT ROWID')
            if [row[1] for row in self.connection.execute('PRAGMA table_info(checks)')] != ['kind', 'name', 'key', 'metadata', 'payload', 'digest']:
                raise sqlite3.DatabaseError('incompatible acceleration cache')
            self.connection.commit()
        except sqlite3.DatabaseError:
            # A damaged optional cache cannot certify anything. Recreate only
            # this generated file and fall back to the complete checks.
            self.connection.close()
            self.path.unlink()
            self.connection = sqlite3.connect(self.path)
            self.connection.execute('PRAGMA trusted_schema=OFF')
            self.connection.execute('CREATE TABLE checks (kind TEXT, name TEXT, key TEXT NOT NULL, metadata TEXT NOT NULL, payload BLOB NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(kind,name)) WITHOUT ROWID')
            self.connection.commit()
        self.connection.execute('BEGIN')
        self.registry_hashes = {}
        self.fund_hashes = {}

    def metadata(self, kind, name):
        row = self.connection.execute('SELECT metadata FROM checks WHERE kind=? AND name=?', (kind, name)).fetchone()
        if row:
            try:
                value = json.loads(row[0])
                return value if isinstance(value, dict) else {}
            except (ValueError, TypeError):
                pass
        return {}

    def read(self, kind, name, key):
        row = self.connection.execute('SELECT key,payload,digest FROM checks WHERE kind=? AND name=?', (kind, name)).fetchone()
        if self.reuse and row and row[0] == key:
            try:
                raw = zlib.decompress(row[1])
                if hashlib.sha256(raw).hexdigest() == row[2]:
                    result = _decode(json.loads(raw))
                    self.counts[f'{kind}_reused'] += 1
                    return result
            except (ValueError, TypeError, zlib.error):
                pass
        self.counts[f'{kind}_checked'] += 1
        return None

    def write(self, kind, name, key, metadata, result):
        raw = _bytes(result)
        self.connection.execute('INSERT OR REPLACE INTO checks VALUES (?,?,?,?,?,?)',
                                (kind, name, key, json.dumps(metadata, sort_keys=True), zlib.compress(raw, 1), hashlib.sha256(raw).hexdigest()))

    def registry_key(self, cusips):
        return [(cusip, self.registry_hashes.get(cusip)) for cusip in sorted(cusips)]

    def validate_funds(self, errors, registry, quality_summary):
        v = self.v
        initial_error_count = len(errors)
        self.registry_hashes = {
            key: _digest({k: value for k, value in entry.items() if k not in _NON_IDENTITY_REGISTRY_FIELDS})
            for key, entry in registry.items() if isinstance(entry, dict)
        }
        quantity = v.load_quantity_evidence(v.cache_dir_for_funds(v.FUNDS_DIR) / 'quantity_estimation_evidence.json')
        quantity_key = _digest(quantity)
        paths = sorted(v.FUNDS_DIR.glob('*.json'))
        # A quiet polling run need not decode every per-fund contribution.
        # Bind the aggregate to actual bytes of EVERY fund plus all registry
        # identities and quantity evidence before reusing its reconciled sums.
        self.fund_hashes = {fp.name: hashlib.sha256(fp.read_bytes()).hexdigest() for fp in paths}
        corpus_key = _digest([self.code, self.fund_hashes, self.registry_hashes, quantity_key])
        aggregate = self.read('fund_corpus', 'all', corpus_key)
        if aggregate is not None:
            self.counts['fund_reused'] += len(paths)
            self.counts['peer_reused'] += len(paths)
            for field, value in aggregate['quality'].items():
                quality_summary[field] = quality_summary.get(field, 0) + value
            return ({fp.stem: fp for fp in paths}, aggregate['groups'], aggregate['cusips'],
                    aggregate['calendars'], aggregate['stats'])
        initial_quality = dict(quality_summary)
        files, groups, cusips, calendars, stats = {}, defaultdict(lambda: {'cusips': set(), 'issuers': set()}), set(), {}, defaultdict(v._empty_current_stats)
        peers = defaultdict(lambda: defaultdict(list))
        for fp in paths:
            file_hash = self.fund_hashes[fp.name]
            old = self.metadata('fund', fp.name)
            key = _digest([self.code, file_hash, self.registry_key(old.get('cusips', [])), quantity_key])
            result = self.read('fund', fp.name, key)
            if result is None:
                local_errors, quality, observations = [], {}, defaultdict(lambda: defaultdict(list))
                output = v.validate_funds(local_errors, registry, quality, paths=[fp], check_peers=False,
                                          peer_observations=observations, quantity_evidence=quantity)
                errors.extend(local_errors)
                result = {'groups': dict(output[1]), 'cusips': output[2], 'calendars': output[3],
                          'stats': output[4], 'quality': quality, 'peers': dict(observations)}
                key = _digest([self.code, file_hash, self.registry_key(result['cusips']), quantity_key])
                if not local_errors:
                    self.write('fund', fp.name, key, {'cusips': sorted(result['cusips'])}, result)
            files[fp.stem] = fp
            cusips.update(result['cusips'])
            calendars.update(result['calendars'])
            for stock_id, values in result['groups'].items():
                for field, value in values.items():
                    groups[stock_id][field].update(value)
            for stock_id, values in result['stats'].items():
                target = stats[stock_id]
                for field, value in values.items():
                    if field == 'largest_value':
                        if value is not None and (target[field] is None or value > target[field]):
                            target[field] = value
                    else:
                        target[field] += value
                        if field.endswith('_digest'):
                            target[field] %= v._POSITION_DIGEST_MODULUS
            for field, value in result['quality'].items():
                quality_summary[field] = quality_summary.get(field, 0) + value
            for peer_key, by_filer in result['peers'].items():
                for filer, observations in by_filer.items():
                    peers[peer_key][filer].extend(observations)
        compiled = v.compile_peer_price_index(peers, consume=True)
        refs = {(report_date, cusip): (statistics.median(price for price, _ in observations), len(observations))
                for (report_date, cusip, kind), observations in compiled.items()
                if kind == 'EQUITY' and len(observations) >= 4}
        peer_key = _peer_digest(self.code, compiled)
        # Cross-fund references are still checked over the complete corpus.
        # Any changed peer evidence invalidates these checks, including checks
        # of unchanged funds that depend on a changed fund's observations.
        for fp in files.values():
            key = _digest([peer_key, self.fund_hashes[fp.name]])
            if self.read('peer', fp.name, key) is not None:
                continue
            local_errors = []
            v.validate_value_unit_peer_consistency(refs, local_errors, compiled, paths=[fp])
            errors.extend(local_errors)
            if not local_errors:
                self.write('peer', fp.name, key, {}, True)
        if len(errors) == initial_error_count:
            self.write('fund_corpus', 'all', corpus_key, {}, {
                'groups': dict(groups), 'cusips': cusips, 'calendars': calendars,
                'stats': dict(stats), 'quality': {field: value - initial_quality.get(field, 0)
                                                for field, value in quality_summary.items()
                                                if isinstance(value, (int, float))}})
        return files, groups, cusips, calendars, dict(stats)

    def validate_stocks(self, errors, fund_calendars, expected_current_stats, expected_split_adjustments, *, registry):
        v = self.v
        files, seen = {}, set()
        expected_split_adjustments.clear()
        for fp in sorted(v.STOCKS_DIR.glob('*.json')):
            raw = fp.read_bytes()
            file_hash = hashlib.sha256(raw).hexdigest()
            metadata = self.metadata('stock', fp.name)
            def context(meta):
                return _digest([self.code, file_hash, self.registry_key([meta.get('cusip', '')]),
                                expected_current_stats.get(meta.get('stock_id')),
                                [(cik, fund_calendars.get(cik)) for cik in meta.get('ciks', [])]])
            key = context(metadata)
            result = self.read('stock', fp.name, key)
            if result is None:
                local_errors, splits = [], {}
                try:
                    stock = json.loads(raw)
                    metadata = {'stock_id': stock.get('stock_id'), 'cusip': stock.get('cusip'),
                                'ciks': sorted({str(h.get('cik')) for h in stock.get('holders', []) if isinstance(h, dict)})}
                except (ValueError, TypeError, AttributeError):
                    metadata = {}
                stock_id = metadata.get('stock_id')
                selected_stats = ({stock_id: expected_current_stats[stock_id]} if stock_id in expected_current_stats else {})
                v.validate_stocks(local_errors, fund_calendars, selected_stats, splits, registry=registry, paths=[fp])
                errors.extend(local_errors)
                result = {'stock_id': stock_id, 'splits': splits}
                if not local_errors:
                    self.write('stock', fp.name, context(metadata), metadata, result)
            stock_id = result['stock_id']
            if stock_id in seen:
                errors.append(f'multiple stock files publish duplicate stock_id {stock_id}')
            seen.add(stock_id)
            files[fp.stem] = fp
            expected_split_adjustments.update(result['splits'])
        missing = sorted(key for key, value in expected_current_stats.items()
                         if (value.get('holder_count') or value.get('transition_count') or value.get('history_count')) and key not in seen)
        if missing:
            errors.append(f'{len(missing)} retained stock identities have no generated stock file; samples: ' + ', '.join(missing[:10]))
        return files

    def finish(self, *, success):
        if success:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()
        print('Incremental validation checks: ' + json.dumps(dict(self.counts), sort_keys=True))
