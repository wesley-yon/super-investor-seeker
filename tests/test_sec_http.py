"""Preserve the different public fetcher contracts across shared transport."""

import time
import unittest
from unittest import mock

import requests

import sec_13f_accession_discovery as submissions
import sec_edgar_evidence as edgar
import sec_security_master as master


FILING_URL = (
    "https://www.sec.gov/Archives/edgar/data/1652044/"
    "000095012326008000/primary_doc.xml"
)
FACTORIES = {
    "master": (master.make_sec_fetcher, master.SEC_COMPANY_TICKERS_URL),
    "filing": (edgar.make_sec_filing_fetcher, FILING_URL),
    "discovery": (edgar.make_sec_discovery_fetcher, FILING_URL),
    "submissions": (
        submissions.make_sec_submissions_fetcher,
        "https://data.sec.gov/submissions/CIK0001652044.json",
    ),
}


class SecHttpContractTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(mock.patch.object(master, "_SEC_NEXT_REQUEST_AT", 0.0))
        self.enterContext(mock.patch.object(time, "monotonic", return_value=1000.0))
        self.sleep = self.enterContext(mock.patch.object(time, "sleep"))

    def fetcher(self, kind, statuses, **options):
        factory, url = FACTORIES[kind]
        responses = []
        for status in statuses:
            response = requests.Response()
            response.url = url
            response.status_code = status
            response._content = b"fixture"
            responses.append(response)
        session = mock.Mock()
        session.get.side_effect = responses
        fetch = factory("Test test@example.test", session=session, **options)
        return fetch, session, url

    def test_source_specific_retry_statuses_remain_distinct(self):
        for kind in FACTORIES:
            for status in (401, 403, 429):
                with self.subTest(kind=kind, status=status):
                    fetch, session, url = self.fetcher(kind, [status, 200])
                    retries = status == 429 and kind != "filing" or (
                        status == 403 and kind in {"master", "submissions"}
                    )
                    if retries:
                        self.assertEqual(b"fixture", fetch(url))
                    else:
                        with self.assertRaises(requests.HTTPError):
                            fetch(url)
                    self.assertEqual(2 if retries else 1, session.get.call_count)

    def test_missing_redirect_location_keeps_source_errors(self):
        expected = {
            "master": (master.SourceParseError, "SEC redirect response has no Location header"),
            "filing": (edgar.NonSECFilingURL, "SEC filing redirect response has no Location header"),
            "discovery": (edgar.NonSECFilingURL, "SEC redirect response has no Location header"),
            "submissions": (submissions.NonSECSubmissionsURL, "SEC submissions redirects are not allowed"),
        }
        for kind, (error_type, message) in expected.items():
            with self.subTest(kind=kind):
                fetch, session, url = self.fetcher(kind, [302, 200])
                with self.assertRaises(error_type) as caught:
                    fetch(url)
                self.assertIs(type(caught.exception), error_type)
                self.assertEqual(message, str(caught.exception))
                self.assertEqual(1, session.get.call_count)

    def test_unrecognized_redirect_status_keeps_source_policy(self):
        for kind in FACTORIES:
            for status in (300, 304, 399):
                with self.subTest(kind=kind, status=status):
                    fetch, session, url = self.fetcher(kind, [status])
                    if kind in {"filing", "discovery"}:
                        with self.assertRaisesRegex(
                            edgar.NonSECFilingURL, "unsupported SEC .* redirect response",
                        ):
                            fetch(url)
                    else:
                        self.assertEqual(b"fixture", fetch(url))
                    self.assertEqual(1, session.get.call_count)

    def test_only_master_pacing_is_shared_between_fetcher_instances(self):
        for kind in ("master", "discovery", "submissions"):
            with self.subTest(kind=kind):
                master._SEC_NEXT_REQUEST_AT = 0.0
                first, _, url = self.fetcher(kind, [200, 200])
                second, _, _ = self.fetcher(kind, [200])
                self.sleep.reset_mock()
                first(url)
                second(url)
                if kind == "master":
                    self.sleep.assert_called_once_with(0.125)
                else:
                    self.sleep.assert_not_called()
                    first(url)
                    self.sleep.assert_called_once_with(0.125)


if __name__ == "__main__":
    unittest.main()
