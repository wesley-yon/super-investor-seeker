#!/usr/bin/env python3
"""Retry explicitly safe GitHub CLI reads and classify uncertain mutations."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from typing import TextIO

RETRY_DELAYS_SECONDS = (1, 3)
TRANSIENT_MUTATION_EXIT_CODE = 75
_TRANSIENT_HTTP_RE = re.compile(
    r"\bHTTP(?:\s+(?:error|status))?[: ]+(429|500|502|503|504)\b", re.IGNORECASE
)
_HTTP_403_RE = re.compile(r"\bHTTP(?:\s+(?:error|status))?[: ]+403\b", re.IGNORECASE)
_RATE_LIMIT_RE = re.compile(
    r"\b(?:secondary\s+)?rate\s+limit\b|\babuse detection\b", re.IGNORECASE
)
_TRANSIENT_TRANSPORT_RE = re.compile(
    r"connection reset|TLS handshake timeout|i/o timeout|timed out|"
    r"temporarily unavailable|unexpected EOF",
    re.IGNORECASE,
)
_RELEASE_NOT_FOUND_RE = re.compile(
    r"\brelease not found\b|\bHTTP(?:\s+(?:error|status))?[: ]+404\b",
    re.IGNORECASE,
)
_API_MUTATION_FLAGS = frozenset(
    {"--field", "--input", "--method", "--raw-field", "-F", "-X", "-f"}
)
_API_MUTATION_PREFIXES = (
    "--field=",
    "--input=",
    "--method=",
    "--raw-field=",
    "-F",
    "-X",
    "-f",
)


def _run_command(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        capture_output=True,
        check=False,
        text=True,
    )


def _is_read_only(arguments: Sequence[str]) -> bool:
    if not arguments:
        return False
    if arguments[0] == "api":
        for argument in arguments[1:]:
            if argument in _API_MUTATION_FLAGS or argument.startswith(
                _API_MUTATION_PREFIXES
            ):
                return False
        return True
    if list(arguments[:2]) == ["release", "view"]:
        return True
    if list(arguments[:2]) == ["release", "download"]:
        # A failed download can leave a partial local file. Replaying it is safe
        # only when the caller explicitly requested overwrite semantics.
        return "--clobber" in arguments[2:]
    return False


def _is_transient(output: str, *, retry_forbidden_read: bool = False) -> bool:
    if _TRANSIENT_HTTP_RE.search(output) or _TRANSIENT_TRANSPORT_RE.search(output):
        return True
    return bool(
        _HTTP_403_RE.search(output)
        and (retry_forbidden_read or _RATE_LIMIT_RE.search(output))
    )


def run(
    arguments: Sequence[str],
    *,
    allow_release_not_found: bool = False,
    retry_forbidden_read: bool = False,
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] = _run_command,
    sleeper: Callable[[float], None] = time.sleep,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run ``gh`` with bounded read retries and no blind mutation replay."""
    if not arguments:
        raise ValueError("at least one GitHub CLI argument is required")
    read_only = _is_read_only(arguments)
    if retry_forbidden_read and not read_only:
        raise ValueError("--retry-forbidden-read requires a read-only command")

    output_stream: TextIO = sys.stdout if stdout is None else stdout
    error_stream: TextIO = sys.stderr if stderr is None else stderr
    command = ["gh", *arguments]
    release_delete = list(arguments[:2]) == ["release", "delete"]

    for attempt in range(len(RETRY_DELAYS_SECONDS) + 1):
        completed = runner(command)
        if completed.returncode == 0:
            output_stream.write(completed.stdout)
            error_stream.write(completed.stderr)
            return 0

        diagnostic = f"{completed.stdout}\n{completed.stderr}"
        if (
            allow_release_not_found
            and release_delete
            and _RELEASE_NOT_FOUND_RE.search(diagnostic)
        ):
            error_stream.write(
                "::notice::Release was already absent; stale cleanup is complete\n"
            )
            return 0

        transient = _is_transient(
            diagnostic,
            retry_forbidden_read=retry_forbidden_read,
        )
        if transient and read_only and attempt < len(RETRY_DELAYS_SECONDS):
            error_stream.write(completed.stdout)
            error_stream.write(completed.stderr)
            delay = RETRY_DELAYS_SECONDS[attempt]
            unit = "second" if delay == 1 else "seconds"
            error_stream.write(
                f"::warning::Transient GitHub CLI read failure; retrying in {delay} {unit}\n"
            )
            sleeper(delay)
            continue

        error_stream.write(completed.stdout)
        error_stream.write(completed.stderr)
        if transient and not read_only:
            error_stream.write(
                "::warning::GitHub mutation outcome is uncertain; reconcile before replay\n"
            )
            return TRANSIENT_MUTATION_EXIT_CODE
        return completed.returncode

    raise AssertionError("bounded GitHub CLI retry loop fell through")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-release-not-found", action="store_true")
    parser.add_argument("--retry-forbidden-read", action="store_true")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    options = parser.parse_args(argv)
    arguments = list(options.arguments)
    if arguments[:1] == ["--"]:
        arguments = arguments[1:]
    if not arguments:
        parser.error("GitHub CLI arguments are required after --")
    try:
        return run(
            arguments,
            allow_release_not_found=options.allow_release_not_found,
            retry_forbidden_read=options.retry_forbidden_read,
        )
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
