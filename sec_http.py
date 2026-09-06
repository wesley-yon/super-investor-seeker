"""Shared HTTP mechanics for the source-specific SEC fetchers.

Callers retain URL admission, retry policy, exception types, and pacing scope.
Every redirect destination is checked before a contact-bearing request is sent.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests


_REDIRECT_STATUS_CODES = frozenset({301, 302, 303, 307, 308})


@dataclass(frozen=True)
class RedirectPolicy:
    normalize_url: Callable[[str], str]
    error_type: type[ValueError]
    limit_message: str
    missing_location_message: str
    missing_location_error_type: type[ValueError] | None = None
    changed_response_message: str | None = None
    unsupported_status_message: str | None = None
    check_redirect: Callable[[str], None] | None = None


def get_sec_response(
    http: requests.Session,
    canonical_url: str,
    *,
    headers: Mapping[str, str],
    timeout: float,
    max_redirects: int,
    policy: RedirectPolicy,
    pace: Callable[[], None] | None = None,
) -> requests.Response:
    """Make one attempt, following only caller-approved redirect targets.

    The caller admits the initial URL. HTTP status handling and retries remain
    outside this loop so each source retains its existing failure semantics.
    """
    request_url = canonical_url
    for redirect_count in range(max_redirects + 1):
        if pace is not None:
            pace()
        response = http.get(
            request_url, headers=dict(headers), timeout=timeout, allow_redirects=False,
        )
        response_url = policy.normalize_url(str(response.url or request_url))
        if policy.changed_response_message and response_url != request_url:
            raise policy.error_type(policy.changed_response_message)
        if response.status_code not in _REDIRECT_STATUS_CODES:
            if policy.unsupported_status_message and 300 <= response.status_code < 400:
                raise policy.error_type(policy.unsupported_status_message)
            return response
        if redirect_count >= max_redirects:
            raise policy.error_type(policy.limit_message)
        location = str(response.headers.get("Location") or "").strip()
        if not location:
            error_type = policy.missing_location_error_type or policy.error_type
            raise error_type(policy.missing_location_message)
        request_url = policy.normalize_url(urljoin(response_url, location))
        if policy.check_redirect is not None:
            policy.check_redirect(request_url)
    raise AssertionError("bounded SEC redirect loop exhausted")


def make_rate_pacer(
    requests_per_second: float, *, clock: Any, lock: Any,
) -> Callable[[], None]:
    """Reserve per-instance request slots, sleeping outside the supplied lock.

    Keeping the clock object live preserves callers' clock/sleep test seams.
    The security-master fetcher uses its separate process-wide pacing lock.
    """
    next_request_at = 0.0
    interval = 1.0 / float(requests_per_second)

    def pace() -> None:
        nonlocal next_request_at
        with lock:
            now = clock.monotonic()
            scheduled = max(now, next_request_at)
            delay = scheduled - now
            next_request_at = scheduled + interval
        if delay > 0:
            clock.sleep(delay)

    return pace
