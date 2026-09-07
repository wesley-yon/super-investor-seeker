"""Bounded, read-only worker processes for independent corpus checks.

Workers receive checker inputs once per phase. They never open the validation
cache or publish data. The parent combines results in file order and owns every
SQLite write and cross-file reconciliation.
"""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
import json
import multiprocessing
import os
from pathlib import Path
import time


def worker_limit(requested: int | None = None) -> int:
    if requested is not None and (type(requested) is not int or requested < 1):
        raise ValueError('audit workers must be a positive integer')
    cores = os.cpu_count() or 1
    if hasattr(os, 'sched_getaffinity'):
        cores = min(cores, len(os.sched_getaffinity(0)))
    try:
        quota, period = Path('/sys/fs/cgroup/cpu.max').read_text().split()
        if quota != 'max':
            cores = min(cores, max(1, int(quota) // int(period)))
    except (OSError, ValueError, ZeroDivisionError):
        pass
    # Full-corpus peer inputs measured close to 2 GiB per interpreter.
    # Reserve that headroom for each worker and recheck at each phase.
    # Account for hosted-runner/container limits, rather than host RAM alone.
    available = None
    try:
        available = os.sysconf('SC_AVPHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')
    except (ValueError, OSError):
        try:
            available = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE') // 4
        except (ValueError, OSError):
            pass
    try:
        for line in Path('/proc/meminfo').read_text().splitlines():
            if line.startswith('MemAvailable:'):
                available = int(line.split()[1]) * 1024
                break
    except (OSError, ValueError, IndexError):
        pass
    try:
        limit = Path('/sys/fs/cgroup/memory.max').read_text().strip()
        if limit != 'max':
            remaining = max(0, int(limit) - int(Path('/sys/fs/cgroup/memory.current').read_text()))
            available = remaining if available is None else min(available, remaining)
    except (OSError, ValueError):
        pass
    memory_workers = max(1, available // (2 * 1024 ** 3)) if available is not None else 1
    return max(1, min(requested or 4, 4, cores, memory_workers))


def batches(paths, size):
    for start in range(0, len(paths), size):
        yield paths[start:start + size]


@contextmanager
def audit_phase(name):
    started = time.perf_counter()
    print(f'Audit phase started: {name}', flush=True)
    try:
        yield
    finally:
        print(f'Audit phase finished: {name} ({time.perf_counter() - started:.2f}s)', flush=True)


def _validate_one(kind, path, validator, inputs):
    errors = []
    if kind == 'fund':
        quality, observations = {}, defaultdict(lambda: defaultdict(list))
        output = validator.validate_funds(
            errors, inputs['registry'], quality, paths=[path], check_peers=False,
            peer_observations=observations, quantity_evidence=inputs['quantity'])
        result = {'groups': dict(output[1]), 'cusips': output[2],
                        'calendars': output[3], 'stats': dict(output[4]),
                        'quality': quality, 'peers': {key: dict(value) for key, value in observations.items()}}
        # Serialization/compression is CPU work too; workers prepare bytes,
        # while the parent remains the only process that writes SQLite.
        from incremental_validation import prepare_cache_payload
        return errors, result, None if errors else prepare_cache_payload(result)
    if kind == 'peer':
        validator.validate_value_unit_peer_consistency(
            inputs['refs'], errors, inputs['compiled'], paths=[path])
        return errors
    if kind == 'stock':
        try:
            stock = json.loads(path.read_bytes())
            metadata = {'stock_id': stock.get('stock_id'), 'cusip': stock.get('cusip'),
                        'ciks': sorted({str(h.get('cik')) for h in stock.get('holders', []) if isinstance(h, dict)})}
        except (ValueError, TypeError, AttributeError):
            metadata = {}
        stock_id = metadata.get('stock_id')
        stats = inputs['stats']
        selected = {stock_id: stats[stock_id]} if stock_id in stats else {}
        splits = {}
        validator.validate_stocks(errors, inputs['calendars'], selected, splits,
                                  registry=inputs['registry'], paths=[path])
        return errors, {'stock_id': stock_id, 'splits': splits}, metadata
    raise ValueError(f'unknown audit phase: {kind}')


def _initialize_worker(kind, locations, inputs):
    import validate_data
    global _worker_kind, _worker_validator, _worker_inputs
    for field, path in locations.items():
        setattr(validate_data, field, Path(path))
    _worker_kind, _worker_validator, _worker_inputs = kind, validate_data, inputs


def _check_worker(path):
    return _validate_one(_worker_kind, path, _worker_validator, _worker_inputs)


class CheckPool:
    def __init__(self, validator, workers, kind, inputs):
        self.validator, self.workers, self.kind, self.inputs = validator, worker_limit(workers), kind, inputs
        self.executor = None

    def __enter__(self):
        return self

    def map(self, paths):
        if not paths:
            return iter(())
        if self.workers == 1 or (self.executor is None and len(paths) == 1):
            return (_validate_one(self.kind, path, self.validator, self.inputs) for path in paths)
        if self.executor is None:
            locations = {field: str(getattr(self.validator, field))
                         for field in ('ROOT', 'DATA_DIR', 'FUNDS_DIR', 'STOCKS_DIR',
                                       'INDEX_PATH', 'FUNDS_INDEX_PATH', 'CUSIP_REGISTRY_PATH',
                                       'SEC_SECURITY_MASTER_PATH', 'SEC_SOURCE_STATE_PATH')}
            self.executor = ProcessPoolExecutor(
                max_workers=self.workers, mp_context=multiprocessing.get_context('spawn'),
                initializer=_initialize_worker, initargs=(self.kind, locations, self.inputs))
            print(f'Audit {self.kind} checks: using {self.workers} worker processes', flush=True)
        # Callers submit bounded batches. Ordered results preserve diagnostics
        # and floating-point accumulation order from the serial checker.
        return self.executor.map(_check_worker, paths, chunksize=1)

    def __exit__(self, *_):
        if self.executor is not None:
            self.executor.shutdown(wait=True, cancel_futures=True)
