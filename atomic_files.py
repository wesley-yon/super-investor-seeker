"""Sibling-file replacement mechanics shared by the durable JSON writers.

Serialization, target validation, directory durability, and cleanup policy stay
with each caller. This module never buffers a complete JSON document in memory.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


def fsync_directory(path: Path) -> None:
    """Persist directory-entry changes used by atomic file transactions."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _remove_temporary(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def discard_temporary(
    path: Path, *, sync_parent: Callable[[Path], None] | None = None,
) -> None:
    """Best-effort cleanup that preserves an active write/interruption error."""
    try:
        path.unlink()
        if sync_parent is not None:
            sync_parent(path.parent)
    except BaseException:
        pass


@contextmanager
def atomic_text_output(
    path: Path,
    *,
    sync_parent: Callable[[Path], None] | None = None,
    cleanup: Callable[[Path], None] = _remove_temporary,
    prefix: str | None = None,
    private_mode: int | None = None,
    newline: str | None = "\n",
) -> Iterator[TextIO]:
    """Render, sync, and replace through an exclusive sibling temporary file.

    Callbacks retain each writer's existing durability and error policy. A
    descriptor that could not be transferred to a stream is closed explicitly.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=prefix if prefix is not None else f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    descriptor_open = True
    try:
        if private_mode is not None:
            os.fchmod(descriptor, private_mode)
        output = os.fdopen(descriptor, "w", encoding="utf-8", newline=newline)
        descriptor_open = False
        with output:
            yield output
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
        if sync_parent is not None:
            sync_parent(path.parent)
    except BaseException:
        if descriptor_open:
            try:
                os.close(descriptor)
            except BaseException:
                pass
        cleanup(temporary_path)
        raise
