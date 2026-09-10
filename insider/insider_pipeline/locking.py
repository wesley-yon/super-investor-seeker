"""An advisory process lock shared by all inventory writers."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path


@contextmanager
def writer_lock(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'writer.lock').open('a+') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('Another backfill writer is active; do not start a duplicate') from exc
        stream.seek(0)
        stream.truncate()
        stream.write(str(os.getpid()) + '\n')
        stream.flush()
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
