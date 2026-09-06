#!/usr/bin/env python3
"""Measure one hosted test phase without retaining its environment or secrets."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import resource
import signal
import shutil
import subprocess
import sys
import time


def cgroup_peak_path() -> Path | None:
    try:
        for line in Path('/proc/self/cgroup').read_text().splitlines():
            if line.startswith('0::'):
                relative = Path(line[3:].lstrip('/'))
                if '..' not in relative.parts:
                    path = Path('/sys/fs/cgroup') / relative / 'memory.peak'
                    if path.is_file():
                        return path
    except OSError:
        pass
    return None


def read_number(path: Path | None) -> int | None:
    try:
        return int(path.read_text()) if path else None
    except (OSError, ValueError):
        return None


def kernel_oom_kills() -> int | None:
    """Read the Linux VM counter without requiring privileged kernel logs."""
    try:
        for line in Path('/proc/vmstat').read_text().splitlines():
            key, value = line.split()
            if key == 'oom_kill':
                return int(value)
    except (OSError, ValueError):
        pass
    return None


def process_group_memory(group: int) -> tuple[int, int | None]:
    """RSS is an upper bound; Linux PSS apportions pages shared by workers."""
    rss = 0
    pss = 0
    pss_available = False
    output = subprocess.check_output(['ps', '-eo', 'pid=,pgid=,rss='], text=True)
    for line in output.splitlines():
        fields = line.split()
        if len(fields) != 3 or int(fields[1]) != group:
            continue
        pid, _, resident = map(int, fields)
        rss += resident * 1024
        try:
            for row in Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines():
                if row.startswith('Pss:'):
                    pss += int(row.split()[1]) * 1024
                    pss_available = True
                    break
        except (OSError, ValueError):
            pass
    return rss, pss if pss_available else None


def measure(command: list[str], *, name: str, root: Path, report_dir: Path,
            interval_seconds: float = 2.0) -> dict:
    root = root.resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    before = shutil.disk_usage(root)
    minimum_free = before.free
    peak_rss = 0
    peak_pss = None
    peak_path = cgroup_peak_path()
    cgroup_before = read_number(peak_path)
    oom_before = kernel_oom_kills()
    started = time.monotonic()
    process = subprocess.Popen(command, start_new_session=True)
    def forward_signal(number, _frame):
        if process.poll() is None:
            try:
                os.killpg(process.pid, number)
            except ProcessLookupError:
                pass
    previous_handlers = {number: signal.signal(number, forward_signal)
                         for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        while True:
            minimum_free = min(minimum_free, shutil.disk_usage(root).free)
            rss, pss = process_group_memory(process.pid)
            peak_rss = max(peak_rss, rss)
            if pss is not None:
                peak_pss = max(peak_pss or 0, pss)
            status = process.poll()
            if status is not None:
                break
            time.sleep(interval_seconds)
    finally:
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)
    ending = shutil.disk_usage(root)
    oom_after = kernel_oom_kills()
    minimum_free = min(minimum_free, ending.free)
    maximum_child_rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    if sys.platform != 'darwin':
        maximum_child_rss *= 1024
    report = {
        'phase': name, 'exit_code': status,
        'elapsed_seconds': round(time.monotonic() - started, 3),
        'sampling_interval_seconds': interval_seconds,
        'peak_sampled_process_group_rss_bytes': peak_rss,
        'peak_sampled_process_group_pss_bytes': peak_pss,
        'maximum_single_child_rss_bytes': maximum_child_rss,
        'cgroup_memory_peak_before_bytes': cgroup_before,
        'cgroup_memory_peak_after_bytes': read_number(peak_path),
        'kernel_oom_kills_before': oom_before,
        'kernel_oom_kills_after': oom_after,
        'kernel_oom_kills_during_phase': (
            max(0, oom_after - oom_before)
            if oom_before is not None and oom_after is not None else None
        ),
        'filesystem_total_bytes': before.total,
        'starting_free_bytes': before.free, 'minimum_free_bytes': minimum_free,
        'ending_free_bytes': ending.free,
        'peak_additional_disk_bytes': max(0, before.free - minimum_free),
        'logical_cpu_count': os.cpu_count(),
    }
    (report_dir / f'{name}.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, sort_keys=True), flush=True)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name', required=True)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--report-dir', type=Path, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.name or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789_-' for c in args.name):
        parser.error('Phase name must be a simple lowercase identifier')
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('A command is required')
    report = measure(command, name=args.name, root=args.root, report_dir=args.report_dir)
    raise SystemExit(report['exit_code'])


if __name__ == '__main__':
    main()
