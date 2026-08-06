"""Coverage for resolve_cusips_via_openfigi's response-parsing defenses.

These are the paths that the weekly full CUSIP refresh leans on: if any of
them raise or silently mis-parse, tickers go missing from the published data.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline


def _fake_response(status=200, payload=None, raise_on_json=False):
    resp = mock.MagicMock()
    resp.status_code = status
    resp.text = "<mock response body>"
    if raise_on_json:
        resp.json.side_effect = ValueError("no JSON")
    else:
        resp.json.return_value = payload
    return resp


class OpenFIGIParseTests(unittest.TestCase):
    def setUp(self):
        # One CUSIP per test unless overridden — batch size is irrelevant
        # here because we mock the HTTP layer.
        pipeline._OPENFIGI_RUN_CACHE.clear()
        self.cusips = ["037833100"]
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.details_path = Path(self.tempdir.name) / "openfigi_details.json"
        self.details_patch = mock.patch.object(
            pipeline,
            "OPENFIGI_DETAILS_PATH",
            self.details_path,
        )
        self.details_patch.start()
        self.addCleanup(self.details_patch.stop)

    def _run(self, resp):
        with mock.patch.object(pipeline, "_openfigi_post", return_value=resp):
            return pipeline.resolve_cusips_via_openfigi(list(self.cusips))

    def _details(self):
        if not self.details_path.exists():
            return {}
        return json.loads(self.details_path.read_text())

    def test_none_response_returns_empty(self):
        self.assertEqual(self._run(None), {})

    def test_non_200_returns_empty(self):
        self.assertEqual(self._run(_fake_response(status=500, payload=[])), {})

    def test_post_retries_transient_503_then_returns_success(self):
        unavailable = _fake_response(status=503, payload=[])
        success = _fake_response(
            payload=[{"data": [{"ticker": "AAPL", "exchCode": "US"}]}]
        )
        with (
            mock.patch.object(
                pipeline.requests,
                "post",
                side_effect=[unavailable, success],
            ) as post,
            mock.patch.object(pipeline.time, "sleep") as sleep,
        ):
            response = pipeline._openfigi_post([
                {"idType": "ID_CUSIP", "idValue": "037833100"},
            ])

        self.assertIs(response, success)
        self.assertEqual(2, post.call_count)
        sleep.assert_called_once_with(1)

    def test_post_retries_transient_429_then_returns_success(self):
        rate_limited = _fake_response(status=429, payload=[])
        rate_limited.headers = {"ratelimit-reset": "0"}
        success = _fake_response(
            payload=[{"data": [{"ticker": "AAPL", "exchCode": "US"}]}]
        )
        with (
            mock.patch.object(
                pipeline.requests,
                "post",
                side_effect=[rate_limited, success],
            ) as post,
            mock.patch.object(pipeline.time, "sleep") as sleep,
        ):
            response = pipeline._openfigi_post([
                {"idType": "ID_CUSIP", "idValue": "037833100"},
            ])

        self.assertIs(response, success)
        self.assertEqual(2, post.call_count)
        sleep.assert_called_once_with(30)

    def test_malformed_json_does_not_raise(self):
        self.assertEqual(self._run(_fake_response(raise_on_json=True)), {})

    def test_non_list_payload_does_not_raise(self):
        # OpenFIGI error shape is sometimes {"error": "..."} rather than a list
        self.assertEqual(self._run(_fake_response(payload={"error": "oops"})), {})

    def test_entry_without_data_key_is_skipped(self):
        # OpenFIGI returns {"warning": "..."} for unresolved CUSIPs
        self.assertEqual(
            self._run(_fake_response(payload=[{"warning": "no identifier found"}])),
            {},
        )
        self.assertEqual(
            {"037833100": {"status": "no_match"}},
            self._details(),
        )

    def test_non_dict_entry_is_skipped(self):
        self.assertEqual(self._run(_fake_response(payload=[None])), {})
        self.assertEqual(self._run(_fake_response(payload=["unexpected"])), {})

    def test_non_list_inner_data_is_skipped(self):
        self.assertEqual(
            self._run(_fake_response(payload=[{"data": "not-a-list"}])),
            {},
        )

    def test_non_dict_inside_inner_list_is_skipped(self):
        self.assertEqual(
            self._run(_fake_response(payload=[{"data": [None, "string"]}])),
            {},
        )

    def test_us_exchange_is_preferred(self):
        payload = [{
            "data": [
                {
                    "ticker": "AAPL-FOREIGN",
                    "name": "APPLE INC",
                    "exchCode": "DE",
                },
                {
                    "ticker": "aapl",
                    "name": "APPLE INC",
                    "securityDescription": "AAPL",
                    "marketSector": "Equity",
                    "securityType": "Common Stock",
                    "securityType2": "Common Stock",
                    "exchCode": "US",
                },
                {"ticker": "AAPL-OTHER", "exchCode": "GB"},
            ]
        }]
        self.assertEqual(
            self._run(_fake_response(payload=payload)),
            {"037833100": "AAPL"},
        )
        self.assertEqual(
            {
                "status": "matched",
                "ticker": "AAPL",
                "name": "APPLE INC",
                "securityDescription": "AAPL",
                "marketSector": "Equity",
                "securityType": "Common Stock",
                "securityType2": "Common Stock",
                "exchCode": "US",
            },
            self._details()["037833100"],
        )

    def test_named_us_exchange_is_preferred_over_foreign_venue(self):
        payload = [{
            "data": [
                {
                    "ticker": "81K0",
                    "name": "ARK 21SHRS ACTBITC FUT STRGY",
                    "exchCode": "GR",
                },
                {
                    "ticker": "ARKA",
                    "name": "ARK 21SHRS ACTBITC FUT STRGY",
                    "exchCode": "NEW YORK",
                },
            ]
        }]
        self.assertEqual(
            self._run(_fake_response(payload=payload)),
            {"037833100": "ARKA"},
        )

    def test_falls_back_to_first_ticker_when_no_us(self):
        payload = [{
            "data": [
                {"ticker": "AAPL-DE", "exchCode": "DE"},
                {"ticker": "AAPL-GB", "exchCode": "GB"},
            ]
        }]
        # No US match: first non-empty ticker wins.
        self.assertEqual(
            self._run(_fake_response(payload=payload)),
            {"037833100": "AAPL-DE"},
        )

    def test_ticker_candidate_wins_over_description_only_candidate(self):
        payload = [{
            "data": [
                {
                    "name": "DESCRIPTION ONLY",
                    "securityDescription": "DESCRIPTION ONLY",
                    "marketSector": "Corp",
                },
                {
                    "ticker": "RIVN 3.625 10/15/30",
                    "name": "RIVIAN AUTO INC",
                    "securityDescription": "RIVN 3 5/8 10/15/30",
                    "marketSector": "Corp",
                    "securityType": "US DOMESTIC",
                    "securityType2": "Corp",
                    "exchCode": "TRACE",
                },
            ]
        }]
        self.assertEqual(
            self._run(_fake_response(payload=payload)),
            {"037833100": "RIVN 3.625 10/15/30"},
        )
        detail = self._details()["037833100"]
        self.assertEqual("RIVN 3.625 10/15/30", detail["ticker"])
        self.assertEqual(
            "RIVN 3.625 10/15/30",
            pipeline._openfigi_security_label(detail, "037833100"),
        )

    def test_structured_note_description_outranks_plain_us_ticker(self):
        detail = {
            "status": "matched",
            "ticker": "RIVN",
            "name": "RIVIAN AUTOMOTIVE INC",
            "securityDescription": "RIVN 3.625 10/15/30",
            "marketSector": "Corp",
            "securityType": "Corp",
            "securityType2": "Corp",
            "exchCode": "US",
        }
        self.assertEqual(
            "RIVN 3.625 10/15/30",
            pipeline._openfigi_security_label(detail, "76954AAD5"),
        )
        label, source = pipeline._registry_security_label(
            identifier="76954AAD5",
            entry={
                "ticker": None,
                "dominant_issuer": "RIVIAN AUTOMOTIVE INC",
                "dominant_class": "NOTE 3.625% 10/15/30",
                "type": "NOTE",
            },
            openfigi_detail=detail,
            prior_entry=None,
            legacy_openfigi_label=None,
        )
        self.assertEqual("RIVN 3.625 10/15/30", label)
        self.assertEqual("openfigi", source)
        company_detail = {
            **detail,
            "ticker": "ATEN",
            "name": "A10 NETWORKS INC",
            "securityDescription": "A10 NETWORKS INC",
            "marketSector": "Equity",
            "securityType": "Common Stock",
            "securityType2": "Common Stock",
        }
        self.assertEqual(
            "ATEN",
            pipeline._openfigi_security_label(
                company_detail,
                "002121101",
            ),
        )

    def test_start_digit_venue_description_falls_through_to_name(self):
        detail = {
            "status": "matched",
            "ticker": "81K0",
            "securityDescription": "81K0 GR",
            "name": "ARK 21SHARES ACTIVE BITCOIN FUTURES STRATEGY ETF",
            "exchCode": "GR",
        }
        self.assertEqual(
            "ARK 21SHARES ACTIVE BITCOIN FUTURES STRATEGY ETF",
            pipeline._openfigi_security_label(detail, "02072L359"),
        )

    def test_us_warrant_suffix_is_a_display_label_but_not_canonical_ticker(
        self,
    ):
        detail = {
            "status": "matched",
            "ticker": "FLYX/WS",
            "securityDescription": "FLYX/WS",
            "name": "FLYEXCLUSIVE INC",
            "marketSector": "Equity",
            "securityType": "Equity WRT",
            "securityType2": "Warrant",
            "exchCode": "US",
        }
        self.assertEqual(
            "FLYX/WS",
            pipeline._openfigi_security_label(detail, "343928115"),
        )
        self.assertIsNone(
            pipeline._openfigi_canonical_ticker(
                detail,
                identifier="343928115",
                instrument_type="WARRANT",
            )
        )
        nasdaq_capital_detail = {
            **detail,
            "ticker": "QBTS/WS",
            "securityDescription": "QBTS/WS",
            "exchCode": "UC",
        }
        self.assertEqual(
            "QBTS/WS",
            pipeline._openfigi_security_label(
                nasdaq_capital_detail,
                "26740W117",
            ),
        )

    def test_right_symbol_is_display_only_even_when_filed_as_equity(self):
        detail = {
            "status": "matched",
            "ticker": "BKT-R",
            "securityDescription": "BKT-R",
            "name": "BLACKROCK INCOME TRUST INC",
            "marketSector": "Equity",
            "securityType": "Common Stock",
            "securityType2": "Common Stock",
            "exchCode": "US",
        }
        self.assertEqual(
            "BKT-R",
            pipeline._openfigi_security_label(detail, "09247F118"),
        )
        self.assertIsNone(
            pipeline._openfigi_canonical_ticker(
                detail,
                identifier="09247F118",
                instrument_type="EQUITY",
                dominant_class="RIGHT 10/20/2025",
            )
        )

    def test_empty_cusip_list_short_circuits(self):
        # Should not even attempt an HTTP call.
        with mock.patch.object(pipeline, "_openfigi_post") as post:
            out = pipeline.resolve_cusips_via_openfigi([])
        self.assertEqual(out, {})
        post.assert_not_called()

    def test_letter_leading_identifiers_use_cins_and_numeric_use_cusip(self):
        self.cusips = ["037833100", "G9001E110"]
        response = _fake_response(payload=[
            {"warning": "not found"},
            {"warning": "not found"},
        ])
        with mock.patch.object(
            pipeline,
            "_openfigi_post",
            return_value=response,
        ) as post:
            self.assertEqual(
                pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                {},
            )

        self.assertEqual(
            [
                {"idType": "ID_CUSIP", "idValue": "037833100"},
                {"idType": "ID_CINS", "idValue": "G9001E110"},
            ],
            post.call_args.args[0],
        )

    def test_mixed_batch_partial_resolution(self):
        self.cusips = ["037833100", "BADCUSIP0"]
        payload = [
            {"data": [{"ticker": "AAPL", "exchCode": "US"}]},
            {"warning": "not found"},
        ]
        self.assertEqual(
            self._run(_fake_response(payload=payload)),
            {"037833100": "AAPL"},
        )

    def test_ticker_is_uppercased(self):
        payload = [{"data": [{"ticker": "aapl", "exchCode": "US"}]}]
        self.assertEqual(
            self._run(_fake_response(payload=payload)),
            {"037833100": "AAPL"},
        )

    def test_public_mode_resolves_without_api_key(self):
        response = _fake_response(
            payload=[{"data": [{"ticker": "AAPL", "exchCode": "US"}]}]
        )
        with mock.patch.object(
            pipeline,
            "get_openfigi_api_key",
            return_value="",
        ):
            with mock.patch.object(
                pipeline,
                "_openfigi_post",
                return_value=response,
            ) as post:
                self.assertEqual(
                    pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                    {"037833100": "AAPL"},
                )
                self.assertEqual(
                    pipeline.OPENFIGI_FREE_BATCH,
                    pipeline.openfigi_batch_size(),
                )

        post.assert_called_once()

    def test_durable_matched_and_no_match_details_short_circuit_http(self):
        self.cusips = ["037833100", "594918104"]
        pipeline.save_openfigi_details({
            "037833100": {
                "status": "matched",
                "ticker": "aapl",
            },
            "594918104": {
                "status": "no_match",
            },
        })

        with mock.patch.object(pipeline, "_openfigi_post") as post:
            result = pipeline.resolve_cusips_via_openfigi(list(self.cusips))

        self.assertEqual({"037833100": "AAPL"}, result)
        post.assert_not_called()
        self.assertEqual("AAPL", pipeline._OPENFIGI_RUN_CACHE["037833100"])
        self.assertIsNone(pipeline._OPENFIGI_RUN_CACHE["594918104"])

    def test_force_refresh_bypasses_durable_and_run_cache(self):
        self.cusips = ["037833100", "594918104"]
        pipeline.save_openfigi_details({
            "037833100": {
                "status": "matched",
                "ticker": "STALE",
            },
            "594918104": {
                "status": "no_match",
            },
        })
        pipeline._OPENFIGI_RUN_CACHE.update({
            "037833100": "RUN-STALE",
            "594918104": None,
        })
        response = _fake_response(payload=[
            {
                "data": [{
                    "figi": "BBG000QW7VC1",
                    "ticker": "RIVN",
                    "exchCode": "US",
                }],
            },
            {
                "data": [{
                    "figi": "BBG00L2H7F55",
                    "ticker": "BILL",
                    "exchCode": "US",
                }],
            },
        ])

        with mock.patch.object(
            pipeline,
            "_openfigi_post",
            return_value=response,
        ) as post:
            result = pipeline.resolve_cusips_via_openfigi(
                list(self.cusips),
                force_refresh=True,
            )

        self.assertEqual(
            {
                "037833100": "RIVN",
                "594918104": "BILL",
            },
            result,
        )
        post.assert_called_once()
        self.assertEqual(
            [
                {"idType": "ID_CUSIP", "idValue": "037833100"},
                {"idType": "ID_CUSIP", "idValue": "594918104"},
            ],
            post.call_args.args[0],
        )

    def test_full_refresh_rejects_invalid_configured_api_key(self):
        unauthorized = _fake_response(status=401, payload={"error": "Invalid key"})
        with (
            mock.patch.dict(
                pipeline.os.environ,
                {"OPENFIGI_API_KEY": "configured-but-invalid"},
            ),
            mock.patch.object(
                pipeline.requests,
                "post",
                return_value=unauthorized,
            ) as post,
        ):
            with self.assertRaisesRegex(
                pipeline.OpenFIGIFullRefreshError,
                r"HTTP 401",
            ):
                pipeline.resolve_cusips_via_openfigi(
                    list(self.cusips),
                    force_refresh=True,
                )

        self.assertEqual(
            "configured-but-invalid",
            post.call_args.kwargs["headers"]["X-OPENFIGI-APIKEY"],
        )

    def test_full_refresh_rejects_forbidden_response(self):
        with mock.patch.object(
            pipeline,
            "_openfigi_post",
            return_value=_fake_response(status=403, payload={"error": "Forbidden"}),
        ):
            with self.assertRaisesRegex(
                pipeline.OpenFIGIFullRefreshError,
                r"HTTP 403",
            ):
                pipeline.resolve_cusips_via_openfigi(
                    list(self.cusips),
                    force_refresh=True,
                )

    def test_full_refresh_rejects_exhausted_transport_failure(self):
        with mock.patch.object(pipeline, "_openfigi_post", return_value=None):
            with self.assertRaisesRegex(
                pipeline.OpenFIGIFullRefreshError,
                r"no response",
            ):
                pipeline.resolve_cusips_via_openfigi(
                    list(self.cusips),
                    force_refresh=True,
                )

    def test_full_refresh_rejects_exhausted_rate_limit_or_server_error(self):
        for status in (429, 500, 503):
            with self.subTest(status=status), mock.patch.object(
                pipeline,
                "_openfigi_post",
                return_value=_fake_response(status=status, payload=[]),
            ):
                with self.assertRaisesRegex(
                    pipeline.OpenFIGIFullRefreshError,
                    rf"HTTP {status}",
                ):
                    pipeline.resolve_cusips_via_openfigi(
                        list(self.cusips),
                        force_refresh=True,
                    )

    def test_full_refresh_rejects_malformed_or_incomplete_batches(self):
        cases = {
            "invalid JSON": _fake_response(raise_on_json=True),
            "not a list": _fake_response(payload={"error": "oops"}),
            r"result\(s\) for": _fake_response(payload=[]),
            "non-object": _fake_response(payload=[None]),
            "error result": _fake_response(
                payload=[{"error": "unexpected processing error"}]
            ),
            "non-exclusive result shape": _fake_response(payload=[{
                "data": [{"figi": "BBG000BLNNH6"}],
                "warning": "no identifier found",
            }]),
            "empty warning": _fake_response(payload=[{"warning": "  "}]),
            "malformed mapping data": _fake_response(
                payload=[{"data": "not-a-list"}]
            ),
            "empty mapping data": _fake_response(payload=[{"data": []}]),
            "without a non-empty FIGI": _fake_response(
                payload=[{"data": [{"ticker": "AAPL"}]}]
            ),
        }
        for expected, response in cases.items():
            with self.subTest(expected=expected), mock.patch.object(
                pipeline,
                "_openfigi_post",
                return_value=response,
            ):
                with self.assertRaisesRegex(
                    pipeline.OpenFIGIFullRefreshError,
                    expected,
                ):
                    pipeline.resolve_cusips_via_openfigi(
                        list(self.cusips),
                        force_refresh=True,
                    )

    def test_full_refresh_accepts_schema_valid_match_without_ticker(self):
        response = _fake_response(payload=[{
            "data": [{
                "figi": "BBG000BLNNH6",
                "ticker": None,
                "name": "Example instrument",
            }],
        }])
        with mock.patch.object(
            pipeline,
            "_openfigi_post",
            return_value=response,
        ):
            result = pipeline.resolve_cusips_via_openfigi(
                list(self.cusips),
                force_refresh=True,
            )

        self.assertEqual({}, result)
        self.assertEqual("matched", self._details()["037833100"]["status"])
        self.assertIsNone(self._details()["037833100"]["ticker"])

    def test_full_refresh_accepts_schema_valid_no_match_warning(self):
        response = _fake_response(payload=[{
            "warning": "No identifier found.",
        }])
        with mock.patch.object(
            pipeline,
            "_openfigi_post",
            return_value=response,
        ):
            result = pipeline.resolve_cusips_via_openfigi(
                list(self.cusips),
                force_refresh=True,
            )

        self.assertEqual({}, result)
        self.assertEqual(
            {"037833100": {"status": "no_match"}},
            self._details(),
        )

    def test_full_refresh_failure_cannot_succeed_from_stale_cache(self):
        pipeline.save_openfigi_details({
            "037833100": {
                "status": "matched",
                "ticker": "STALE",
            },
        })
        pipeline._OPENFIGI_RUN_CACHE["037833100"] = "RUN-STALE"

        with mock.patch.object(pipeline, "_openfigi_post", return_value=None):
            with self.assertRaises(pipeline.OpenFIGIFullRefreshError):
                pipeline.resolve_cusips_via_openfigi(
                    list(self.cusips),
                    force_refresh=True,
                )

        self.assertEqual(
            "STALE",
            self._details()["037833100"]["ticker"],
        )

    def test_definitive_no_match_is_cached_for_this_run(self):
        response = _fake_response(
            payload=[{"warning": "no identifier found"}]
        )
        with mock.patch.object(
            pipeline, "_openfigi_post", return_value=response
        ) as post:
            self.assertEqual(
                pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                {},
            )
            self.assertEqual(
                pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                {},
            )

        post.assert_called_once()
        self.assertEqual(
            {"037833100": {"status": "no_match"}},
            self._details(),
        )

    def test_definitive_no_match_persists_but_transient_error_does_not(self):
        self.cusips = ["037833100", "594918104"]
        response = _fake_response(payload=[
            {"error": "No identifier found."},
            {"error": "unexpected processing error"},
        ])

        self.assertEqual(self._run(response), {})
        self.assertEqual(
            {"037833100": {"status": "no_match"}},
            self._details(),
        )
        self.assertIn("037833100", pipeline._OPENFIGI_RUN_CACHE)
        self.assertNotIn("594918104", pipeline._OPENFIGI_RUN_CACHE)

    def test_positive_match_is_cached_for_this_run(self):
        response = _fake_response(
            payload=[{"data": [{"ticker": "AAPL", "exchCode": "US"}]}]
        )
        with mock.patch.object(
            pipeline, "_openfigi_post", return_value=response
        ) as post:
            first = pipeline.resolve_cusips_via_openfigi(list(self.cusips))
            second = pipeline.resolve_cusips_via_openfigi(list(self.cusips))

        self.assertEqual(first, {"037833100": "AAPL"})
        self.assertEqual(second, first)
        post.assert_called_once()

    def test_transient_failure_is_not_cached(self):
        success = _fake_response(
            payload=[{"data": [{"ticker": "AAPL", "exchCode": "US"}]}]
        )
        with mock.patch.object(
            pipeline, "_openfigi_post", side_effect=[None, success]
        ) as post:
            self.assertEqual(
                pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                {},
            )
            self.assertEqual({}, self._details())
            self.assertEqual(
                pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                {"037833100": "AAPL"},
            )

        self.assertEqual(post.call_count, 2)

    def test_per_item_error_is_not_cached(self):
        processing_error = _fake_response(
            payload=[{"error": "unexpected processing error"}]
        )
        success = _fake_response(
            payload=[{"data": [{"ticker": "AAPL", "exchCode": "US"}]}]
        )
        with mock.patch.object(
            pipeline,
            "_openfigi_post",
            side_effect=[processing_error, success],
        ) as post:
            self.assertEqual(
                pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                {},
            )
            self.assertEqual({}, self._details())
            self.assertEqual(
                pipeline.resolve_cusips_via_openfigi(list(self.cusips)),
                {"037833100": "AAPL"},
            )

        self.assertEqual(post.call_count, 2)

    def test_mixed_batch_is_fully_served_from_run_cache(self):
        self.cusips = ["037833100", "BADCUSIP0"]
        response = _fake_response(payload=[
            {"data": [{"ticker": "AAPL", "exchCode": "US"}]},
            {"warning": "not found"},
        ])
        with mock.patch.object(
            pipeline, "_openfigi_post", return_value=response
        ) as post:
            first = pipeline.resolve_cusips_via_openfigi(list(self.cusips))
            second = pipeline.resolve_cusips_via_openfigi(list(self.cusips))

        self.assertEqual(first, {"037833100": "AAPL"})
        self.assertEqual(second, first)
        post.assert_called_once()


if __name__ == "__main__":
    unittest.main()
