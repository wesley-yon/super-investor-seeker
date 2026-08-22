"""Characterization and hardening tests for the shared SEC HTTP client."""

from __future__ import annotations

import asyncio
import concurrent.futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import requests

import pipeline


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        *,
        url: str = "https://www.sec.gov/Archives/edgar/data/1/index.json",
        headers: dict[str, str] | None = None,
        content: bytes = b"payload",
        text: str = "payload",
        json_value: object | None = None,
    ) -> None:
        self.status_code = status_code
        self.url = url
        self.headers = headers or {}
        self.content = content
        self.text = text
        self._json_value = {} if json_value is None else json_value
        self.closed = False

    def json(self) -> object:
        return self._json_value

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def close(self) -> None:
        self.closed = True

    def iter_content(self, chunk_size: int = 8192):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset:offset + chunk_size]


class _HostileResponseFailure(BaseException):
    """Synthetic non-control-flow BaseException from a response object."""


class CharacterizationTests(unittest.TestCase):
    def test_global_http_is_a_singleton_patch_seam(self) -> None:
        self.assertIsInstance(pipeline.HTTP, pipeline.RateLimitedSession)
        self.assertIs(pipeline.HTTP, pipeline.HTTP)
        with mock.patch.object(pipeline.HTTP, "get", return_value="patched") as get:
            self.assertEqual("patched", pipeline.HTTP.get("https://example.invalid"))
        get.assert_called_once_with("https://example.invalid")

    def test_zero_arg_client_sets_default_sec_headers(self) -> None:
        with mock.patch.object(pipeline, "USER_AGENT", pipeline.DEFAULT_USER_AGENT):
            client = pipeline.RateLimitedSession()
        self.assertEqual(pipeline.DEFAULT_USER_AGENT, client.session.headers["User-Agent"])
        self.assertEqual("gzip, deflate", client.session.headers["Accept-Encoding"])
        self.assertEqual("*/*", client.session.headers["Accept"])

    def test_zero_arg_client_uses_default_timeout(self) -> None:
        client = pipeline.RateLimitedSession()
        client.session = mock.Mock()
        client.session.get.return_value = FakeResponse()
        with mock.patch.object(client, "_claim_slot"):
            client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
        self.assertEqual(pipeline.HTTP_TIMEOUT, client.session.get.call_args.kwargs["timeout"])

    def test_caller_timeout_overrides_default_without_duplicate_keyword(self) -> None:
        client = pipeline.RateLimitedSession()
        client.session = mock.Mock()
        client.session.get.return_value = FakeResponse()
        with mock.patch.object(client, "_claim_slot", return_value=0.0):
            client.get("https://www.sec.gov/Archives/edgar/data/1/index.json", timeout=7)
        self.assertEqual(7, client.session.get.call_args.kwargs["timeout"])

    def test_request_deadline_clamps_timeout_and_is_not_forwarded(self) -> None:
        session = mock.Mock()
        response = FakeResponse()
        session.get.return_value = response
        client = pipeline.RateLimitedSession(
            session=session,
            monotonic=lambda: 10.0,
            rate=8,
        )

        with mock.patch.object(client, "_claim_slot", return_value=0.0):
            returned = client.get(
                "https://www.sec.gov/Archives/edgar/data/1/index.json",
                timeout=30,
                deadline_monotonic=12.0,
            )

        self.assertIs(response, returned)
        self.assertEqual(2.0, session.get.call_args.kwargs["timeout"])
        self.assertNotIn("deadline_monotonic", session.get.call_args.kwargs)

    def test_request_deadline_closes_response_that_returns_after_expiry(self) -> None:
        clock = [0.0]
        response = FakeResponse()
        session = mock.Mock()

        def return_late(*_args, **_kwargs):
            clock[0] = 61.0
            return response

        session.get.side_effect = return_late
        client = pipeline.RateLimitedSession(
            session=session,
            monotonic=lambda: clock[0],
            rate=8,
        )

        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"),
        ):
            client.get(
                "https://www.sec.gov/Archives/edgar/data/1/index.json",
                deadline_monotonic=60.0,
            )

        self.assertTrue(response.closed)

    def test_request_deadline_does_not_wait_for_a_blocking_close(self) -> None:
        clock = [0.0]

        class BlockingCloseResponse(FakeResponse):
            def __init__(self) -> None:
                super().__init__()
                self.close_entered = threading.Event()
                self.close_released = threading.Event()
                self.close_calls = 0

            def close(self) -> None:
                self.close_calls += 1
                self.close_entered.set()
                self.close_released.wait(timeout=1.0)
                self.closed = True

        response = BlockingCloseResponse()
        session = mock.Mock()

        def return_late(*_args, **_kwargs):
            clock[0] = 61.0
            return response

        session.get.side_effect = return_late
        client = pipeline.RateLimitedSession(
            session=session,
            monotonic=lambda: clock[0],
            rate=8,
        )
        started = time.monotonic()
        try:
            with (
                mock.patch.object(client, "_claim_slot", return_value=0.0),
                self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"),
            ):
                client.get(
                    "https://www.sec.gov/Archives/edgar/data/1/index.json",
                    deadline_monotonic=60.0,
                )
            self.assertLess(time.monotonic() - started, 0.3)
            self.assertTrue(response.close_entered.wait(timeout=0.2))
            self.assertFalse(response.closed)
        finally:
            response.close_released.set()

        close_deadline = time.monotonic() + 0.2
        while not response.closed and time.monotonic() < close_deadline:
            time.sleep(0.005)
        self.assertTrue(response.closed)
        self.assertEqual(1, response.close_calls)

    def test_request_deadline_refuses_limiter_wait_without_issuing(self) -> None:
        sleeps: list[float] = []
        session = mock.Mock()
        client = pipeline.RateLimitedSession(
            session=session,
            sleep=sleeps.append,
            monotonic=lambda: 10.0,
            rate=1,
        )
        client._last_request = 10.0

        with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
            client.get(
                "https://www.sec.gov/Archives/edgar/data/1/index.json",
                deadline_monotonic=10.5,
            )

        self.assertEqual([], sleeps)
        session.get.assert_not_called()

    def test_request_deadline_stops_retry_when_backoff_does_not_fit(self) -> None:
        retry = FakeResponse(503)
        events: list[dict[str, object]] = []
        sleeps: list[float] = []
        session = mock.Mock(get=mock.Mock(return_value=retry))
        client = pipeline.RateLimitedSession(
            session=session,
            sleep=sleeps.append,
            monotonic=lambda: 0.0,
            event_sink=events.append,
            rate=8,
        )

        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            mock.patch.object(pipeline, "MAX_RETRIES", 2),
            self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"),
        ):
            client.get(
                "https://www.sec.gov/Archives/edgar/data/1/index.json",
                deadline_monotonic=1.0,
            )

        self.assertTrue(retry.closed)
        self.assertEqual(1, session.get.call_count)
        self.assertEqual([], sleeps)
        self.assertEqual(1, len(events))
        self.assertEqual(0.0, events[0]["sleep"])
        self.assertEqual(
            {"attempt", "status", "latency", "sleep", "limiter_wait"},
            set(events[0]),
        )

    def test_caller_streaming_mode_reaches_underlying_session(self) -> None:
        client = pipeline.RateLimitedSession()
        client.session = mock.Mock()
        response = FakeResponse()
        client.session.get.return_value = response
        with mock.patch.object(client, "_claim_slot", return_value=0.0):
            returned = client.get(
                "https://www.sec.gov/Archives/edgar/data/1/index.json",
                stream=True,
            )

        self.assertIs(response, returned)
        self.assertIs(True, client.session.get.call_args.kwargs["stream"])

    def test_requests_are_spaced_at_eight_per_second(self) -> None:
        client = pipeline.RateLimitedSession()
        client._last_request = 1.0
        client.session = mock.Mock()
        client.session.get.return_value = FakeResponse()
        with (
            mock.patch.object(pipeline.time, "monotonic", side_effect=[1.0, 1.125, 1.125, 1.125]),
            mock.patch.object(pipeline.time, "sleep") as sleep,
        ):
            client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
        sleep.assert_called_once_with(1.0 / 8.0)

    def test_issue_lock_allows_concurrent_network_calls_after_slot_claim(self) -> None:
        client = pipeline.RateLimitedSession()
        active = 0
        peak_active = 0
        active_lock = threading.Lock()
        entered = threading.Barrier(2)

        def get(*_args, **_kwargs):
            nonlocal active, peak_active
            with active_lock:
                active += 1
                peak_active = max(peak_active, active)
            entered.wait(timeout=2)
            with active_lock:
                active -= 1
            return FakeResponse()

        client.session = mock.Mock()
        client.session.get.side_effect = get
        with mock.patch.object(pipeline, "MIN_REQUEST_INTERVAL", 0.0):
            threads = [threading.Thread(target=client.get, args=("https://www.sec.gov/Archives/",)) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(2, peak_active)

    def test_legacy_retry_statuses_retry_then_return_response(self) -> None:
        client = pipeline.RateLimitedSession()
        client.session = mock.Mock()
        expected = FakeResponse()
        client.session.get.side_effect = [
            FakeResponse(403), FakeResponse(429), FakeResponse(503), expected,
        ]
        with (
            mock.patch.object(client, "_claim_slot"),
            mock.patch.object(pipeline.time, "sleep") as sleep,
        ):
            actual = client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
        self.assertIs(expected, actual)
        self.assertEqual(4, client.session.get.call_count)
        self.assertEqual([mock.call(2.0), mock.call(4.0), mock.call(8.0)], sleep.call_args_list)

    def test_retries_required_statuses_and_transport_errors_for_six_total_attempts(self) -> None:
        client = pipeline.RateLimitedSession()
        client.session = mock.Mock()
        expected = FakeResponse()
        client.session.get.side_effect = [
            FakeResponse(500),
            requests.Timeout("slow"),
            FakeResponse(502),
            requests.ConnectionError("offline"),
            FakeResponse(504),
            expected,
        ]
        with (
            mock.patch.object(client, "_claim_slot"),
            mock.patch.object(pipeline.time, "sleep"),
        ):
            response = client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
        self.assertIs(expected, response)
        self.assertEqual(6, client.session.get.call_count)

    def test_exhausted_retries_do_not_expose_query_or_exception_text(self) -> None:
        secret = "TASK5_SYNTHETIC_SECRET"
        url = f"https://www.sec.gov/Archives/a?token={secret}"
        cases = {
            "transport": requests.Timeout(f"exception-{secret}"),
            "status": FakeResponse(503, url=url),
        }
        for label, response_or_error in cases.items():
            with self.subTest(case=label):
                session = mock.Mock()
                if isinstance(response_or_error, BaseException):
                    session.get.side_effect = response_or_error
                else:
                    session.get.return_value = response_or_error
                client = pipeline.RateLimitedSession(
                    session=session,
                    sleep=lambda _delay: None,
                    rate=8,
                )
                with (
                    mock.patch.object(client, "_claim_slot", return_value=0.0),
                    mock.patch.object(pipeline, "MAX_RETRIES", 1),
                    self.assertLogs(pipeline.log, level="WARNING") as logs,
                    self.assertRaises(RuntimeError) as raised,
                ):
                    client.get(url)

                rendered_logs = "\n".join(logs.output)
                self.assertNotIn(secret, rendered_logs)
                self.assertNotIn(secret, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)

    def test_nonretryable_http_error_does_not_expose_query_text(self) -> None:
        class QueryLeakingResponse(FakeResponse):
            def raise_for_status(self) -> None:
                error = requests.HTTPError(f"HTTP 404 for URL {self.url}")
                error.response = self
                raise error

        secret = "TASK5_SYNTHETIC_SECRET"
        url = f"https://www.sec.gov/Archives/a?token={secret}"
        response = QueryLeakingResponse(404, url=url)
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=response)),
            rate=8,
        )
        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            self.assertRaises(requests.HTTPError) as raised,
        ):
            client.get(url)

        self.assertNotIn(secret, str(raised.exception))
        self.assertIs(response, raised.exception.response)
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(response.closed)

    def test_unexpected_request_exception_is_normalized_without_secret(self) -> None:
        secret = "TASK5_SYNTHETIC_SECRET"
        response = mock.Mock(spec=requests.Response)
        error = requests.RequestException(secret, response=response)
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(side_effect=error)),
            rate=8,
        )
        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            self.assertRaises(RuntimeError) as raised,
        ):
            client.get("https://www.sec.gov/Archives/a")

        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        response.close.assert_called_once_with()

    def test_session_method_hostile_base_exception_is_sanitized(self) -> None:
        secret = "TASK5_SESSION_BASE_EXCEPTION_SECRET"
        for error_type in (RuntimeError, _HostileResponseFailure):
            with self.subTest(error_type=error_type.__name__):
                response = mock.Mock(spec=requests.Response)
                error = error_type(secret)
                setattr(error, "response", response)
                client = pipeline.RateLimitedSession(
                    session=mock.Mock(get=mock.Mock(side_effect=error)),
                    rate=8,
                )
                with (
                    mock.patch.object(client, "_claim_slot", return_value=0.0),
                    self.assertRaises(RuntimeError) as raised,
                ):
                    client.get("https://www.sec.gov/Archives/a")

                self.assertEqual("SEC request failed", str(raised.exception))
                self.assertNotIn(secret, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertIsNone(raised.exception.__context__)
                response.close.assert_called_once_with()

    def test_exception_response_cleanup_survives_an_unwritable_error_marker(self) -> None:
        class MarkerRejectingError(BaseException):
            def __setattr__(self, name: str, value: object) -> None:
                if name == "_sec_response_cleanup":
                    raise AttributeError("synthetic marker rejection")
                super().__setattr__(name, value)

        response = mock.Mock(spec=requests.Response)
        error = MarkerRejectingError("synthetic transport detail")
        error.response = response

        pipeline.close_sec_exception_response(error)
        pipeline.close_sec_exception_response(error)

        response.close.assert_called_once_with()

    def test_exception_response_cleanup_marks_before_reentrant_close(self) -> None:
        error = requests.RequestException("synthetic transport detail")
        close_calls = 0

        class ReentrantResponse:
            def close(self) -> None:
                nonlocal close_calls
                close_calls += 1
                pipeline.close_sec_exception_response(error)

        setattr(error, "response", ReentrantResponse())
        pipeline.close_sec_exception_response(error)

        self.assertEqual(1, close_calls)

    def test_exception_response_cleanup_survives_two_unwritable_markers(self) -> None:
        class MarkerRejectingResponse:
            def __init__(self) -> None:
                self.close_calls = 0

            def __setattr__(self, name: str, value: object) -> None:
                if name == "_sec_response_cleanup":
                    raise AttributeError("synthetic response marker rejection")
                super().__setattr__(name, value)

            def close(self) -> None:
                self.close_calls += 1

        class MarkerRejectingError(BaseException):
            def __setattr__(self, name: str, value: object) -> None:
                if name == "_sec_response_cleanup":
                    raise AttributeError("synthetic error marker rejection")
                super().__setattr__(name, value)

        response = MarkerRejectingResponse()
        error = MarkerRejectingError("synthetic transport detail")
        error.response = response

        pipeline.close_sec_exception_response(error)
        pipeline.close_sec_exception_response(error)

        self.assertEqual(1, response.close_calls)

    def test_exception_response_cleanup_does_not_leak_at_fallback_capacity(self) -> None:
        class MarkerRejectingResponse:
            def __init__(self) -> None:
                self.close_calls = 0

            @property
            def _sec_response_cleanup(self) -> None:
                return None

            def close(self) -> None:
                self.close_calls += 1

        class MarkerRejectingError(BaseException):
            @property
            def _sec_response_cleanup(self) -> None:
                return None

        with pipeline._SEC_RESPONSE_CLEANUP_LOCK:
            prior_errors = dict(pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK)
            prior_responses = dict(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK)
            pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.clear()
            pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.clear()
        errors: list[MarkerRejectingError] = []
        responses: list[MarkerRejectingResponse] = []
        try:
            for _ in range(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT + 1):
                response = MarkerRejectingResponse()
                error = MarkerRejectingError("synthetic transport detail")
                setattr(error, "response", response)
                errors.append(error)
                responses.append(response)
                pipeline.close_sec_exception_response(error)

            for error in errors:
                pipeline.close_sec_exception_response(error)

            self.assertTrue(responses)
            self.assertTrue(all(response.close_calls == 1 for response in responses))
            self.assertLessEqual(
                len(pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK),
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT,
            )
            self.assertLessEqual(
                len(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK),
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT,
            )
        finally:
            with pipeline._SEC_RESPONSE_CLEANUP_LOCK:
                pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.clear()
                pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.update(prior_errors)
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.clear()
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.update(prior_responses)

    def test_exception_response_cleanup_closes_once_when_fallback_is_full(self) -> None:
        class OpaqueResponse:
            __slots__ = ("close_calls",)

            def __init__(self) -> None:
                self.close_calls = 0

            def close(self) -> None:
                self.close_calls += 1

        fillers = [object() for _ in range(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT)]
        with pipeline._SEC_RESPONSE_CLEANUP_LOCK:
            prior_errors = dict(pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK)
            prior_responses = dict(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK)
            pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.clear()
            pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.update(
                (id(value), value) for value in fillers
            )
            pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.clear()
            pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.update(
                (id(value), value) for value in fillers
            )
        try:
            response = OpaqueResponse()
            error = requests.RequestException("synthetic transport detail")
            setattr(error, "response", response)

            pipeline.close_sec_exception_response(error)
            pipeline.close_sec_exception_response(error)

            self.assertEqual(1, response.close_calls)
            self.assertEqual(
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT,
                len(pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK),
            )
            self.assertEqual(
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT,
                len(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK),
            )
        finally:
            with pipeline._SEC_RESPONSE_CLEANUP_LOCK:
                pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.clear()
                pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.update(prior_errors)
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.clear()
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.update(prior_responses)

    def test_exception_response_cleanup_at_capacity_deduplicates_across_wrappers(
        self,
    ) -> None:
        class OpaqueResponse:
            __slots__ = ("close_calls",)

            def __init__(self) -> None:
                self.close_calls = 0

            @property
            def _sec_response_cleanup(self) -> None:
                return None

            def close(self) -> None:
                self.close_calls += 1

        fillers = [object() for _ in range(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT)]
        with pipeline._SEC_RESPONSE_CLEANUP_LOCK:
            prior_errors = dict(pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK)
            prior_responses = dict(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK)
            pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.clear()
            pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.update(
                (id(value), value) for value in fillers
            )
            pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.clear()
            pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.update(
                (id(value), value) for value in fillers
            )
        try:
            response = OpaqueResponse()
            errors = [RuntimeError("synthetic transport detail") for _ in range(2)]
            for error in errors:
                setattr(error, "response", response)

            threads = [
                threading.Thread(
                    target=pipeline.close_sec_exception_response,
                    args=(error,),
                )
                for error in errors
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(1, response.close_calls)
            self.assertLessEqual(
                len(pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK),
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT,
            )
            self.assertLessEqual(
                len(pipeline._SEC_RESPONSE_CLEANUP_FALLBACK),
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK_LIMIT,
            )
        finally:
            with pipeline._SEC_RESPONSE_CLEANUP_LOCK:
                pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.clear()
                pipeline._SEC_EXCEPTION_CLEANUP_FALLBACK.update(prior_errors)
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.clear()
                pipeline._SEC_RESPONSE_CLEANUP_FALLBACK.update(prior_responses)

    def test_exception_response_cleanup_ignores_spoofed_marker_properties(self) -> None:
        class SpoofedResponse:
            __slots__ = ("close_calls",)

            def __init__(self) -> None:
                self.close_calls = 0

            @property
            def _sec_response_cleanup(self) -> object:
                return pipeline._CLOSED_SEC_EXCEPTION_RESPONSE

            def close(self) -> None:
                self.close_calls += 1

        class SpoofedError(BaseException):
            __slots__ = ("response",)

            @property
            def _sec_response_cleanup(self) -> object:
                return pipeline._CLOSED_SEC_EXCEPTION_RESPONSE

        response = SpoofedResponse()
        error = SpoofedError("synthetic transport detail")
        error.response = response

        pipeline.close_sec_exception_response(error)
        pipeline.close_sec_exception_response(error)

        self.assertEqual(1, response.close_calls)

    def test_transport_exceptions_close_attached_responses_before_retry(self) -> None:
        responses = [mock.Mock(spec=requests.Response) for _ in range(pipeline.MAX_RETRIES)]
        errors = [
            requests.Timeout("synthetic transport detail", response=response)
            for response in responses
        ]
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(side_effect=errors)),
            sleep=lambda _seconds: None,
            jitter=lambda _delay: 0.0,
            rate=8,
        )
        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            self.assertRaises(RuntimeError) as raised,
        ):
            client.get("https://www.sec.gov/Archives/a")

        for response in responses:
            response.close.assert_called_once_with()
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_non_retryable_4xx_is_not_reissued(self) -> None:
        client = pipeline.RateLimitedSession()
        client.session = mock.Mock()
        response = FakeResponse(404)
        client.session.get.return_value = response
        with (
            mock.patch.object(client, "_claim_slot"),
            mock.patch.object(pipeline.time, "sleep") as sleep,
            self.assertRaises(requests.HTTPError) as raised,
        ):
            client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
        self.assertIs(response, raised.exception.response)
        self.assertTrue(response.closed)
        self.assertEqual(1, client.session.get.call_count)
        sleep.assert_not_called()

    def test_backoff_uses_injected_jitter_and_caps_sleep(self) -> None:
        sleeps: list[float] = []
        session = mock.Mock()
        session.get.side_effect = [FakeResponse(429), FakeResponse()]
        client = pipeline.RateLimitedSession(
            session=session,
            sleep=sleeps.append,
            jitter=lambda _delay: 999.0,
            rate=8,
        )
        with mock.patch.object(client, "_claim_slot"):
            client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
        self.assertEqual([pipeline.RETRY_MAX], sleeps)

    def test_retry_after_uses_numeric_and_http_date_with_cap(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for retry_after, expected in (
            ("4", 4.0),
            ((now + timedelta(seconds=7)).strftime("%a, %d %b %Y %H:%M:%S GMT"), 7.0),
            ("999", pipeline.RETRY_MAX),
            ("not-a-date", pipeline.RETRY_BASE),
            ((now - timedelta(seconds=1)).strftime("%a, %d %b %Y %H:%M:%S GMT"), pipeline.RETRY_BASE),
        ):
            with self.subTest(retry_after=retry_after):
                sleeps: list[float] = []
                session = mock.Mock()
                session.get.side_effect = [
                    FakeResponse(429, headers={"Retry-After": retry_after}),
                    FakeResponse(),
                ]
                client = pipeline.RateLimitedSession(
                    session=session,
                    sleep=sleeps.append,
                    wall_now=lambda: now,
                    rate=8,
                )
                with mock.patch.object(client, "_claim_slot"):
                    client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
                self.assertEqual([expected], sleeps)

    def test_event_sink_receives_only_allowlisted_metadata(self) -> None:
        events: list[dict[str, object]] = []
        session = mock.Mock()
        response = FakeResponse(content=b"secret", text="private")
        session.get.return_value = response
        client = pipeline.RateLimitedSession(
            session=session,
            event_sink=events.append,
            monotonic=iter([1.0, 1.0, 1.25, 1.5]).__next__,
            rate=8,
        )
        client.get("https://www.sec.gov/Archives/edgar/data/1/index.json")
        self.assertEqual(1, len(events))
        self.assertEqual(
            {"attempt", "status", "latency", "sleep", "limiter_wait"},
            set(events[0]),
        )
        self.assertEqual(1, events[0]["attempt"])
        self.assertEqual(200, events[0]["status"])
        self.assertNotIn("body", events[0])
        self.assertNotIn("text", events[0])
        self.assertNotIn("url", events[0])

    def test_scoped_request_event_observer_is_safe_and_unsubscribes(self) -> None:
        constructor_events: list[dict[str, object]] = []
        scoped_events: list[dict[str, object]] = []
        session = mock.Mock()
        session.get.side_effect = [
            FakeResponse(content=b"first-secret", text="first-private"),
            FakeResponse(content=b"second-secret", text="second-private"),
        ]
        client = pipeline.RateLimitedSession(
            session=session,
            event_sink=constructor_events.append,
            rate=8,
        )

        with mock.patch.object(client, "_claim_slot", return_value=0.0):
            with pipeline.observe_sec_request_events(scoped_events.append):
                client.get("https://www.sec.gov/Archives/a")
            client.get("https://www.sec.gov/Archives/b")

        self.assertEqual(2, len(constructor_events))
        self.assertEqual(1, len(scoped_events))
        self.assertEqual(
            {"attempt", "status", "latency", "sleep", "limiter_wait"},
            set(scoped_events[0]),
        )
        rendered = repr(scoped_events)
        self.assertNotIn("secret", rendered)
        self.assertNotIn("private", rendered)
        self.assertNotIn("Archives", rendered)

    def test_event_sink_exception_does_not_change_successful_fetch(self) -> None:
        response = FakeResponse(content=b"secret", text="private")
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=response)),
            event_sink=mock.Mock(side_effect=RuntimeError("telemetry unavailable")),
            rate=8,
        )
        with mock.patch.object(client, "_claim_slot", return_value=0.0):
            self.assertIs(response, client.get("https://www.sec.gov/Archives/a"))
        self.assertFalse(response.closed)

    def test_nonretryable_http_failure_emits_safe_event_after_cleanup(self) -> None:
        response = FakeResponse(404, content=b"secret", text="private")
        events: list[dict[str, object]] = []
        observed_closed: list[bool] = []

        def observe(event: dict[str, object]) -> None:
            observed_closed.append(response.closed)
            events.append(event)

        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=response)),
            event_sink=observe,
            rate=8,
        )
        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            self.assertRaises(requests.HTTPError),
        ):
            client.get("https://www.sec.gov/Archives/a")

        self.assertEqual([True], observed_closed)
        self.assertEqual(1, len(events))
        self.assertEqual(1, events[0]["attempt"])
        self.assertEqual(404, events[0]["status"])
        self.assertEqual(
            {"attempt", "status", "latency", "sleep", "limiter_wait"},
            set(events[0]),
        )
        self.assertNotIn("secret", repr(events))
        self.assertTrue(response.closed)

    def test_terminal_request_exception_emits_safe_attempt_event(self) -> None:
        response = mock.Mock(spec=requests.Response)
        error = requests.RequestException("private transport detail", response=response)
        events: list[dict[str, object]] = []
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(side_effect=error)),
            event_sink=events.append,
            rate=8,
        )
        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            self.assertRaises(RuntimeError),
        ):
            client.get("https://www.sec.gov/Archives/a")

        self.assertEqual(1, len(events))
        self.assertEqual(1, events[0]["attempt"])
        self.assertIsNone(events[0]["status"])
        self.assertNotIn("private", repr(events))
        response.close.assert_called_once_with()

    def test_discarded_retry_response_is_closed_before_telemetry(self) -> None:
        retry = FakeResponse(503)
        success = FakeResponse()
        observed_closed: list[bool] = []
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(side_effect=[retry, success])),
            sleep=lambda _delay: None,
            event_sink=lambda _event: observed_closed.append(retry.closed),
            rate=8,
        )
        with mock.patch.object(client, "_claim_slot", return_value=0.0):
            self.assertIs(success, client.get("https://www.sec.gov/Archives/a"))
        self.assertTrue(observed_closed[0])

    def test_exhausted_retry_response_closes_before_base_exception_from_telemetry(self) -> None:
        response = FakeResponse(503)
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=response)),
            event_sink=mock.Mock(side_effect=SystemExit()),
            rate=8,
        )
        with (
            mock.patch.object(client, "_claim_slot", return_value=0.0),
            mock.patch.object(pipeline, "MAX_RETRIES", 1),
            self.assertRaises(SystemExit),
        ):
            client.get("https://www.sec.gov/Archives/a")
        self.assertTrue(response.closed)

    def test_base_exception_from_active_response_closes_before_propagating(self) -> None:
        class InterruptingResponse:
            url = "https://www.sec.gov/Archives/a"
            headers: dict[str, str] = {}
            closed = False

            @property
            def status_code(self) -> int:
                raise KeyboardInterrupt()

            def close(self) -> None:
                self.closed = True

        response = InterruptingResponse()
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=response)), rate=8
        )
        with mock.patch.object(client, "_claim_slot", return_value=0.0), self.assertRaises(KeyboardInterrupt):
            client.get("https://www.sec.gov/Archives/a")
        self.assertTrue(response.closed)

    def test_event_sink_keyboard_interrupt_closes_active_response_and_propagates(self) -> None:
        response = FakeResponse()
        client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=response)),
            event_sink=mock.Mock(side_effect=KeyboardInterrupt()),
            rate=8,
        )
        with mock.patch.object(client, "_claim_slot", return_value=0.0), self.assertRaises(KeyboardInterrupt):
            client.get("https://www.sec.gov/Archives/a")
        self.assertTrue(response.closed)

    def test_sec_request_rate_defaults_validates_and_fails_closed(self) -> None:
        with mock.patch.dict(pipeline.os.environ, {}, clear=True):
            self.assertEqual(8.0, pipeline.sec_max_requests_per_second())
        with mock.patch.dict(pipeline.os.environ, {"SEC_MAX_REQUESTS_PER_SECOND": "5"}, clear=True):
            self.assertEqual(5.0, pipeline.sec_max_requests_per_second())
        for value in ("0", "-1", "nan", "inf", "9", "wat"):
            with self.subTest(value=value), mock.patch.dict(
                pipeline.os.environ,
                {"SEC_MAX_REQUESTS_PER_SECOND": value},
                clear=True,
            ):
                with self.assertRaises(ValueError):
                    pipeline.sec_max_requests_per_second()

    def test_injected_rate_uses_same_safe_policy_as_environment(self) -> None:
        for rate in (True, "5", float("nan"), float("inf"), 0, -1, 9):
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                pipeline.RateLimitedSession(rate=rate)
        for rate in (0.5, 8):
            with self.subTest(rate=rate):
                self.assertIsInstance(pipeline.RateLimitedSession(rate=rate), pipeline.RateLimitedSession)

    def test_declared_sec_user_agent_validation_is_opt_in(self) -> None:
        for user_agent in ("", "  ", pipeline.DEFAULT_USER_AGENT, "Product/1.0"):
            with self.subTest(user_agent=user_agent):
                with self.assertRaises(ValueError):
                    pipeline.require_declared_sec_user_agent(user_agent)
        self.assertEqual(
            "InvestorResearch/1.0 ops@example.org",
            pipeline.require_declared_sec_user_agent("InvestorResearch/1.0 ops@example.org"),
        )
        self.assertIsInstance(pipeline.RateLimitedSession(), pipeline.RateLimitedSession)

    def test_sec_url_validation_allows_supported_paths_and_rejects_lookalikes(self) -> None:
        valid = (
            "https://www.sec.gov/Archives/edgar/data/1/index.json?x=1",
            "https://www.sec.gov/files/company_tickers.json",
            "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
            "https://www.sec.gov/data/submissions/CIK0000000001.json",
            "https://data.sec.gov/submissions/CIK0000000001.json",
            "https://www.sec.gov/edgar/browse/?CIK=1&owner=exclude",
            "https://www.sec.gov/cgi-bin/browse-edgar?CIK=1&owner=exclude",
        )
        for url in valid:
            with self.subTest(url=url):
                self.assertEqual(url, pipeline.validate_sec_url(url))
        for url in (
            "http://www.sec.gov/Archives/a",
            "https://sec.gov/Archives/a",
            "https://www.sec.gov.evil.example/Archives/a",
            "https://user@www.sec.gov/Archives/a",
            "https://www.sec.gov:444/Archives/a",
            "https://www.sec.gov/Archives/a#fragment",
            "https://www.sec.gov/not-sec/a",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                pipeline.validate_sec_url(url)

    def test_manual_redirects_validate_each_hop_and_close_intermediate(self) -> None:
        session = mock.Mock()
        redirect = FakeResponse(302, headers={"Location": "/Archives/next"})
        final = FakeResponse(url="https://www.sec.gov/Archives/next")
        session.get.side_effect = [redirect, final]
        client = pipeline.RateLimitedSession(session=session, rate=8)
        with mock.patch.object(client, "_claim_slot", return_value=0.0) as claim_slot:
            self.assertIs(final, client.get("https://www.sec.gov/Archives/start"))
        self.assertEqual(2, claim_slot.call_count)
        self.assertTrue(redirect.closed)
        self.assertFalse(final.closed)
        self.assertEqual(False, session.get.call_args_list[0].kwargs["allow_redirects"])
        self.assertEqual("https://www.sec.gov/Archives/next", session.get.call_args_list[1].args[0])
        with self.assertRaises(ValueError):
            client.get("https://www.sec.gov/Archives/start", allow_redirects=True)

    def test_invalid_response_url_closes_response_before_raising(self) -> None:
        session = mock.Mock()
        response = FakeResponse(url="https://example.invalid/Archives/a")
        session.get.return_value = response
        client = pipeline.RateLimitedSession(session=session, rate=8)
        with mock.patch.object(client, "_claim_slot", return_value=0.0), self.assertRaises(ValueError):
            client.get("https://www.sec.gov/Archives/a")
        self.assertTrue(response.closed)

    def test_bounded_response_reader_rejects_invalid_max_bytes_before_reading(self) -> None:
        for max_bytes in (True, "4", 1.5, float("nan"), float("inf"), -1):
            with self.subTest(max_bytes=max_bytes):
                response = FakeResponse()
                response.iter_content = mock.Mock()
                with self.assertRaises(ValueError):
                    pipeline.read_bounded_sec_response(response, max_bytes=max_bytes)
                response.iter_content.assert_not_called()
                self.assertTrue(response.closed)

    def test_bounded_response_reader_allows_zero_only_for_empty_payload(self) -> None:
        empty = FakeResponse(content=b"")
        self.assertEqual(b"", pipeline.read_bounded_sec_response(empty, max_bytes=0))
        self.assertTrue(empty.closed)
        nonempty = FakeResponse(content=b"x")
        with self.assertRaises(ValueError):
            pipeline.read_bounded_sec_response(nonempty, max_bytes=0)
        self.assertTrue(nonempty.closed)

    def test_bounded_response_reader_enforces_deadline_around_each_chunk(self) -> None:
        clock = [0.0]

        class LateChunkResponse(FakeResponse):
            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                clock[0] = 2.0
                yield b"x"

        response = LateChunkResponse()
        with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
            pipeline.read_bounded_sec_response(
                response,
                max_bytes=1,
                deadline_monotonic=1.0,
                monotonic=lambda: clock[0],
            )

        self.assertTrue(response.closed)

    def test_bounded_response_reader_preserves_deadline_during_iterator_setup(
        self,
    ) -> None:
        readings = iter((0.0, 1.0))
        response = FakeResponse()
        response.iter_content = mock.Mock()

        with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
            pipeline.read_bounded_sec_response(
                response,
                max_bytes=1,
                deadline_monotonic=1.0,
                monotonic=lambda: next(readings),
            )

        response.iter_content.assert_not_called()
        self.assertTrue(response.closed)

    def test_reader_expiry_before_pump_does_not_wait_for_blocking_close(self) -> None:
        readings = iter((0.0, 1.0))

        class BlockingCloseResponse(FakeResponse):
            def __init__(self) -> None:
                super().__init__(content=b"")
                self.close_entered = threading.Event()
                self.close_released = threading.Event()
                self.close_calls = 0

            def close(self) -> None:
                self.close_calls += 1
                self.close_entered.set()
                self.close_released.wait(timeout=1.0)
                self.closed = True

        response = BlockingCloseResponse()
        response.iter_content = mock.Mock()
        started = time.monotonic()
        try:
            with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
                pipeline.read_bounded_sec_response(
                    response,
                    max_bytes=1,
                    deadline_monotonic=1.0,
                    monotonic=lambda: next(readings),
                )
            self.assertLess(time.monotonic() - started, 0.3)
            self.assertTrue(response.close_entered.wait(timeout=0.2))
            self.assertFalse(response.closed)
        finally:
            response.close_released.set()

        close_deadline = time.monotonic() + 0.2
        while not response.closed and time.monotonic() < close_deadline:
            time.sleep(0.005)
        response.iter_content.assert_not_called()
        self.assertTrue(response.closed)
        self.assertEqual(1, response.close_calls)

    def test_bounded_response_reader_interrupts_a_blocking_chunk_at_deadline(
        self,
    ) -> None:
        class BlockingResponse(FakeResponse):
            def __init__(self) -> None:
                super().__init__(content=b"")
                self.released = threading.Event()

            def close(self) -> None:
                super().close()
                self.released.set()

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                self.released.wait(timeout=1.0)
                if not self.closed:
                    yield b"x"

        response = BlockingResponse()
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
            pipeline.read_bounded_sec_response(
                response,
                max_bytes=1,
                deadline_monotonic=started + 0.05,
                monotonic=time.monotonic,
            )
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.5)
        self.assertTrue(response.closed)
        self.assertTrue(response.released.is_set())

    def test_deadline_does_not_wait_for_a_blocking_response_close(self) -> None:
        class BlockingCloseResponse(FakeResponse):
            def __init__(self) -> None:
                super().__init__(content=b"")
                self.read_entered = threading.Event()
                self.read_released = threading.Event()
                self.close_entered = threading.Event()
                self.close_released = threading.Event()
                self.close_calls = 0

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                self.read_entered.set()
                self.read_released.wait(timeout=1.0)
                yield b"x"

            def close(self) -> None:
                self.close_calls += 1
                self.close_entered.set()
                self.read_released.set()
                self.close_released.wait(timeout=1.0)
                self.closed = True

        response = BlockingCloseResponse()
        started = time.monotonic()
        try:
            with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
                pipeline.read_bounded_sec_response(
                    response,
                    max_bytes=1,
                    deadline_monotonic=started + 0.05,
                    monotonic=time.monotonic,
                )
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 0.3)
            self.assertTrue(response.read_entered.is_set())
            self.assertTrue(response.close_entered.wait(timeout=0.2))
            self.assertFalse(response.closed)
        finally:
            response.read_released.set()
            response.close_released.set()

        close_deadline = time.monotonic() + 0.2
        while not response.closed and time.monotonic() < close_deadline:
            time.sleep(0.005)
        self.assertTrue(response.closed)
        self.assertEqual(1, response.close_calls)

    def test_deadline_stream_pump_capacity_is_bounded(self) -> None:
        class FirstResponse(FakeResponse):
            def __init__(self) -> None:
                super().__init__(content=b"")
                self.entered = threading.Event()
                self.released = threading.Event()

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                self.entered.set()
                self.released.wait(timeout=1.0)
                yield b"x"

            def close(self) -> None:
                self.released.set()
                super().close()

        class SecondResponse(FakeResponse):
            def __init__(self) -> None:
                super().__init__(content=b"")
                self.iterator_entered = False

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                self.iterator_entered = True
                yield b"y"

        first = FirstResponse()
        second = SecondResponse()
        first_results: list[bytes] = []
        first_errors: list[BaseException] = []

        def read_first() -> None:
            try:
                first_results.append(
                    pipeline.read_bounded_sec_response(
                        first,
                        max_bytes=1,
                        deadline_monotonic=time.monotonic() + 1.0,
                    )
                )
            except BaseException as error:
                first_errors.append(error)

        with mock.patch.object(
            pipeline,
            "_SEC_STREAM_PUMP_SLOTS",
            threading.BoundedSemaphore(1),
        ):
            first_thread = threading.Thread(target=read_first, daemon=True)
            first_thread.start()
            self.assertTrue(first.entered.wait(timeout=0.2))
            started = time.monotonic()
            try:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "^SEC request deadline reached$",
                ):
                    pipeline.read_bounded_sec_response(
                        second,
                        max_bytes=1,
                        deadline_monotonic=started + 0.05,
                    )
                self.assertLess(time.monotonic() - started, 0.3)
                self.assertFalse(second.iterator_entered)
            finally:
                first.released.set()
                first_thread.join(timeout=0.5)

        close_deadline = time.monotonic() + 0.2
        while not second.closed and time.monotonic() < close_deadline:
            time.sleep(0.005)
        self.assertFalse(first_thread.is_alive())
        self.assertEqual([b"x"], first_results)
        self.assertEqual([], first_errors)
        self.assertTrue(first.closed)
        self.assertTrue(second.closed)

    def test_bounded_reader_interrupts_a_real_delayed_socket_read(self) -> None:
        class DelayedHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"x")
                self.wfile.flush()
                time.sleep(0.5)
                try:
                    self.wfile.write(b"y")
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(  # noqa: A002 - stdlib override name
                self,
                format: str,
                *args: object,
            ) -> None:
                del format, args

        class LocalServer(ThreadingHTTPServer):
            daemon_threads = True

        existing_workers = {
            worker.ident
            for worker in threading.enumerate()
            if worker.name == "sec-response-stream"
        }
        server = LocalServer(("127.0.0.1", 0), DelayedHandler)
        server_thread = threading.Thread(
            target=lambda: server.serve_forever(poll_interval=0.01),
            daemon=True,
        )
        server_thread.start()
        try:
            response = requests.get(
                f"http://127.0.0.1:{server.server_port}/delayed",
                stream=True,
                timeout=1.0,
            )
            started = time.monotonic()
            with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
                pipeline.read_bounded_sec_response(
                    response,
                    max_bytes=2,
                    deadline_monotonic=started + 0.05,
                    monotonic=time.monotonic,
                )
            elapsed = time.monotonic() - started
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=1.0)

        worker_deadline = time.monotonic() + 0.2
        new_workers: list[threading.Thread] = []
        while time.monotonic() < worker_deadline:
            new_workers = [
                worker
                for worker in threading.enumerate()
                if worker.name == "sec-response-stream"
                and worker.ident not in existing_workers
                and worker.is_alive()
            ]
            if not new_workers:
                break
            time.sleep(0.005)

        self.assertLess(elapsed, 0.3)
        self.assertTrue(response.raw.closed)
        self.assertFalse(new_workers)

    def test_bounded_response_reader_checks_length_and_observed_overflow(self) -> None:
        too_large = FakeResponse(headers={"Content-Length": "5"}, content=b"12345")
        with self.assertRaises(ValueError):
            pipeline.read_bounded_sec_response(too_large, max_bytes=4)
        self.assertTrue(too_large.closed)
        overflow = FakeResponse(content=b"12345")
        with self.assertRaises(ValueError):
            pipeline.read_bounded_sec_response(overflow, max_bytes=4)
        self.assertTrue(overflow.closed)
        response = FakeResponse(content=b"1234")
        self.assertEqual(b"1234", pipeline.read_bounded_sec_response(response, max_bytes=4))
        self.assertTrue(response.closed)

    def test_bounded_response_reader_rejects_noncanonical_content_length(self) -> None:
        for content_length in (
            "-1",
            "+1",
            " 1",
            "1 ",
            "01",
            "1.0",
            "0x1",
            "",
            "9" * 21,
        ):
            with self.subTest(content_length=content_length):
                response = FakeResponse(
                    headers={"Content-Length": content_length},
                    content=b"x",
                )
                response.iter_content = mock.Mock()
                with self.assertRaises(ValueError):
                    pipeline.read_bounded_sec_response(response, max_bytes=4)
                response.iter_content.assert_not_called()
                self.assertTrue(response.closed)

    def test_bounded_reader_cleanup_failure_never_replaces_primary_or_success(self) -> None:
        secret = "TASK5_CLOSE_SECRET"

        class CloseFailingResponse(FakeResponse):
            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                self.close_calls = 0

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError(secret)

        invalid = CloseFailingResponse(
            headers={"Content-Length": "bad"},
            content=b"x",
        )
        with self.assertRaises(ValueError) as raised:
            pipeline.read_bounded_sec_response(invalid, max_bytes=4)
        self.assertEqual("SEC response Content-Length is invalid", str(raised.exception))
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertEqual(1, invalid.close_calls)

        success = CloseFailingResponse(content=b"ok")
        self.assertEqual(
            b"ok",
            pipeline.read_bounded_sec_response(success, max_bytes=2),
        )
        self.assertEqual(1, success.close_calls)

    def test_bounded_reader_stream_failure_is_sanitized_and_cleanup_is_best_effort(
        self,
    ) -> None:
        stream_secret = "TASK5_STREAM_SECRET"
        close_secret = "TASK5_CLOSE_SECRET"

        class StreamFailingResponse(FakeResponse):
            def __init__(self) -> None:
                super().__init__()
                self.close_calls = 0

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                raise requests.exceptions.ChunkedEncodingError(stream_secret)
                yield b""  # pragma: no cover - preserve generator shape

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError(close_secret)

        response = StreamFailingResponse()
        with self.assertRaises(RuntimeError) as raised:
            pipeline.read_bounded_sec_response(response, max_bytes=4)
        self.assertEqual("SEC response stream failed", str(raised.exception))
        self.assertNotIn(stream_secret, str(raised.exception))
        self.assertNotIn(close_secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertEqual(1, response.close_calls)

    def test_bounded_reader_header_access_failure_is_sanitized_and_closed_once(
        self,
    ) -> None:
        secret = "TASK5_HEADER_SECRET"

        class ExplodingHeaders:
            def __init__(self, error: BaseException) -> None:
                self.error = error

            def get(self, _name: str):
                raise self.error

        for error in (RuntimeError(secret), _HostileResponseFailure(secret)):
            with self.subTest(error_type=type(error).__name__):
                response = FakeResponse(content=b"ok")
                response.headers = ExplodingHeaders(error)  # type: ignore[assignment]
                response.close = mock.Mock()
                with self.assertRaises(ValueError) as raised:
                    pipeline.read_bounded_sec_response(response, max_bytes=2)

                self.assertEqual(
                    "SEC response headers are invalid",
                    str(raised.exception),
                )
                self.assertNotIn(secret, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertIsNone(raised.exception.__context__)
                response.close.assert_called_once_with()

    def test_bounded_reader_hostile_base_exception_is_sanitized_and_closed_once(
        self,
    ) -> None:
        secret = "TASK5_STREAM_BASE_EXCEPTION_SECRET"

        class HostileStreamResponse(FakeResponse):
            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                raise _HostileResponseFailure(secret)
                yield b""  # pragma: no cover - preserve generator shape

        response = HostileStreamResponse()
        response.close = mock.Mock()
        with self.assertRaises(RuntimeError) as raised:
            pipeline.read_bounded_sec_response(response, max_bytes=4)

        self.assertEqual("SEC response stream failed", str(raised.exception))
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        response.close.assert_called_once_with()

    def test_shared_client_response_metadata_failures_are_sanitized_and_closed_once(
        self,
    ) -> None:
        secret = "TASK5_RESPONSE_METADATA_SECRET"

        class ExplodingHeaders:
            def get(self, _name: str):
                raise RuntimeError(secret)

        class ExplodingURLResponse(FakeResponse):
            @property
            def url(self) -> str:
                raise RuntimeError(secret)

            @url.setter
            def url(self, value: str) -> None:
                self._url = value

        class ExplodingStatusResponse(FakeResponse):
            @property
            def status_code(self) -> int:
                raise RuntimeError(secret)

            @status_code.setter
            def status_code(self, value: int) -> None:
                self._status_code = value

        cases = (
            (ExplodingURLResponse(), "SEC response URL is invalid"),
            (ExplodingStatusResponse(), "SEC response status is invalid"),
            (
                FakeResponse(302, headers={"Location": "/Archives/next"}),
                "SEC response headers are invalid",
            ),
        )
        cases[2][0].headers = ExplodingHeaders()  # type: ignore[union-attr,assignment]
        for response, expected in cases:
            with self.subTest(expected=expected):
                response.close = mock.Mock()  # type: ignore[method-assign]
                client = pipeline.RateLimitedSession(
                    session=mock.Mock(get=mock.Mock(return_value=response)),
                    rate=8,
                )
                with (
                    mock.patch.object(client, "_claim_slot", return_value=0.0),
                    self.assertRaises(ValueError) as raised,
                ):
                    client.get("https://www.sec.gov/Archives/a")

                self.assertEqual(expected, str(raised.exception))
                self.assertNotIn(secret, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertIsNone(raised.exception.__context__)
                response.close.assert_called_once_with()  # type: ignore[attr-defined]

    def test_shared_client_hostile_base_exception_metadata_is_sanitized(
        self,
    ) -> None:
        secret = "TASK5_RESPONSE_BASE_EXCEPTION_SECRET"

        class ExplodingHeaders:
            def get(self, _name: str):
                raise _HostileResponseFailure(secret)

        class ExplodingURLResponse(FakeResponse):
            @property
            def url(self) -> str:
                raise _HostileResponseFailure(secret)

            @url.setter
            def url(self, value: str) -> None:
                self._url = value

        class ExplodingStatusResponse(FakeResponse):
            @property
            def status_code(self) -> int:
                raise _HostileResponseFailure(secret)

            @status_code.setter
            def status_code(self, value: int) -> None:
                self._status_code = value

        class ExplodingHeadersResponse(FakeResponse):
            @property
            def headers(self):
                raise _HostileResponseFailure(secret)

            @headers.setter
            def headers(self, value) -> None:
                self._headers = value

        location = FakeResponse(302, headers={"Location": "/Archives/next"})
        location.headers = ExplodingHeaders()  # type: ignore[assignment]
        cases = (
            (ExplodingURLResponse(), "SEC response URL is invalid"),
            (ExplodingStatusResponse(), "SEC response status is invalid"),
            (ExplodingHeadersResponse(302), "SEC response headers are invalid"),
            (location, "SEC response headers are invalid"),
        )
        for response, expected in cases:
            with self.subTest(expected=expected, response=type(response).__name__):
                response.close = mock.Mock()  # type: ignore[method-assign]
                client = pipeline.RateLimitedSession(
                    session=mock.Mock(get=mock.Mock(return_value=response)),
                    rate=8,
                )
                with (
                    mock.patch.object(client, "_claim_slot", return_value=0.0),
                    self.assertRaises(ValueError) as raised,
                ):
                    client.get("https://www.sec.gov/Archives/a")

                self.assertEqual(expected, str(raised.exception))
                self.assertNotIn(secret, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertIsNone(raised.exception.__context__)
                response.close.assert_called_once_with()  # type: ignore[attr-defined]

    def test_retry_after_header_access_failure_falls_back_without_leaking(self) -> None:
        secret = "TASK5_RETRY_HEADER_SECRET"

        class ExplodingHeaders:
            def __init__(self, error: BaseException) -> None:
                self.error = error

            def get(self, _name: str):
                raise self.error

        for error in (RuntimeError(secret), _HostileResponseFailure(secret)):
            with self.subTest(error_type=type(error).__name__):
                retry = FakeResponse(503)
                retry.headers = ExplodingHeaders(error)  # type: ignore[assignment]
                retry.close = mock.Mock()
                success = FakeResponse()
                sleeps: list[float] = []
                client = pipeline.RateLimitedSession(
                    session=mock.Mock(get=mock.Mock(side_effect=[retry, success])),
                    sleep=sleeps.append,
                    rate=8,
                )
                with (
                    mock.patch.object(client, "_claim_slot", return_value=0.0),
                    self.assertLogs(pipeline.log, level="WARNING") as logs,
                ):
                    self.assertIs(
                        success,
                        client.get("https://www.sec.gov/Archives/a"),
                    )

                self.assertEqual([pipeline.RETRY_BASE], sleeps)
                self.assertNotIn(secret, "\n".join(logs.output))
                retry.close.assert_called_once_with()

    def test_response_control_flow_exceptions_propagate_and_close_once(self) -> None:
        controls = (
            KeyboardInterrupt(),
            SystemExit(),
            GeneratorExit(),
            asyncio.CancelledError(),
            concurrent.futures.CancelledError(),
        )
        for control in controls:
            with self.subTest(control=type(control).__name__):
                class InterruptingResponse(FakeResponse):
                    @property
                    def status_code(self) -> int:
                        raise control

                    @status_code.setter
                    def status_code(self, value: int) -> None:
                        self._status_code = value

                response = InterruptingResponse()
                response.close = mock.Mock()
                client = pipeline.RateLimitedSession(
                    session=mock.Mock(get=mock.Mock(return_value=response)),
                    rate=8,
                )
                with (
                    mock.patch.object(client, "_claim_slot", return_value=0.0),
                    self.assertRaises(type(control)),
                ):
                    client.get("https://www.sec.gov/Archives/a")
                response.close.assert_called_once_with()

    def test_stream_control_flow_exceptions_propagate_and_close_once(self) -> None:
        controls = (
            KeyboardInterrupt(),
            SystemExit(),
            GeneratorExit(),
            asyncio.CancelledError(),
            concurrent.futures.CancelledError(),
        )
        for control in controls:
            with self.subTest(control=type(control).__name__):
                class InterruptingResponse(FakeResponse):
                    def iter_content(self, chunk_size: int = 8192):
                        del chunk_size
                        raise control
                        yield b""  # pragma: no cover - preserve generator shape

                response = InterruptingResponse()
                response.close = mock.Mock()
                with self.assertRaises(type(control)):
                    pipeline.read_bounded_sec_response(
                        response,
                        max_bytes=4,
                        deadline_monotonic=time.monotonic() + 1.0,
                        monotonic=time.monotonic,
                    )
                response.close.assert_called_once_with()

    def test_shared_client_cleanup_failure_never_replaces_public_outcome(self) -> None:
        secret = "TASK5_RETRY_CLOSE_SECRET"

        class CloseFailingResponse(FakeResponse):
            def __init__(self, status_code: int = 200, **kwargs) -> None:
                super().__init__(status_code, **kwargs)
                self.close_calls = 0

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError(secret)

        redirect = CloseFailingResponse(
            302,
            headers={"Location": "/Archives/next"},
        )
        final = FakeResponse(url="https://www.sec.gov/Archives/next")
        redirect_client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(side_effect=[redirect, final])),
            rate=8,
        )
        with mock.patch.object(redirect_client, "_claim_slot", return_value=0.0):
            self.assertIs(
                final,
                redirect_client.get("https://www.sec.gov/Archives/start"),
            )
        self.assertEqual(1, redirect.close_calls)

        invalid = CloseFailingResponse(
            url="https://example.invalid/Archives/a",
        )
        invalid_client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=invalid)),
            rate=8,
        )
        with (
            mock.patch.object(invalid_client, "_claim_slot", return_value=0.0),
            self.assertRaises(ValueError) as invalid_error,
        ):
            invalid_client.get("https://www.sec.gov/Archives/a")
        self.assertNotIn(secret, str(invalid_error.exception))
        self.assertIsNone(invalid_error.exception.__cause__)
        self.assertIsNone(invalid_error.exception.__context__)
        self.assertEqual(1, invalid.close_calls)

        nonretryable = CloseFailingResponse(404)
        nonretryable_client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=nonretryable)),
            rate=8,
        )
        with (
            mock.patch.object(nonretryable_client, "_claim_slot", return_value=0.0),
            self.assertRaises(requests.HTTPError) as http_error,
        ):
            nonretryable_client.get("https://www.sec.gov/Archives/a")
        self.assertEqual("SEC HTTP 404", str(http_error.exception))
        self.assertNotIn(secret, str(http_error.exception))
        self.assertIsNone(http_error.exception.__cause__)
        self.assertIsNone(http_error.exception.__context__)
        self.assertIs(nonretryable, http_error.exception.response)
        self.assertEqual(1, nonretryable.close_calls)

        exhausted = CloseFailingResponse(503)
        exhausted_client = pipeline.RateLimitedSession(
            session=mock.Mock(get=mock.Mock(return_value=exhausted)),
            sleep=lambda _delay: None,
            rate=8,
        )
        with (
            mock.patch.object(exhausted_client, "_claim_slot", return_value=0.0),
            mock.patch.object(pipeline, "MAX_RETRIES", 1),
            self.assertRaises(RuntimeError) as retry_error,
        ):
            exhausted_client.get("https://www.sec.gov/Archives/a")
        self.assertEqual("SEC GET failed after 1 retries", str(retry_error.exception))
        self.assertNotIn(secret, str(retry_error.exception))
        self.assertIsNone(retry_error.exception.__cause__)
        self.assertIsNone(retry_error.exception.__context__)
        self.assertEqual(1, exhausted.close_calls)

    def test_retryable_and_exhausted_responses_close_but_success_stays_open(self) -> None:
        session = mock.Mock()
        retry = FakeResponse(503)
        success = FakeResponse()
        session.get.side_effect = [retry, success]
        client = pipeline.RateLimitedSession(session=session, sleep=lambda _value: None, rate=8)
        with mock.patch.object(client, "_claim_slot", return_value=0.0):
            self.assertIs(success, client.get("https://www.sec.gov/Archives/a"))
        self.assertTrue(retry.closed)
        self.assertFalse(success.closed)
        exhausted = FakeResponse(503)
        session.get.side_effect = [exhausted] * pipeline.MAX_RETRIES
        with mock.patch.object(client, "_claim_slot", return_value=0.0), self.assertRaises(RuntimeError):
            client.get("https://www.sec.gov/Archives/a")
        self.assertTrue(exhausted.closed)

    def test_response_contract_keeps_json_content_and_text_consumers_working(self) -> None:
        client = pipeline.RateLimitedSession()
        json_response = FakeResponse(json_value={"filings": ["x"]})
        content_response = FakeResponse(content=b"<xml/>")
        text_response = FakeResponse(text="listing")
        client.session = mock.Mock()
        client.session.get.side_effect = [json_response, content_response, text_response]
        with mock.patch.object(client, "_claim_slot"):
            self.assertEqual({"filings": ["x"]}, client.get("https://www.sec.gov/Archives/a").json())
            self.assertEqual(b"<xml/>", client.get("https://www.sec.gov/Archives/b").content)
            self.assertEqual("listing", client.get("https://www.sec.gov/Archives/c").text)


if __name__ == "__main__":
    unittest.main()
