import unittest

from scripts.pages_deployment_identity import latest_successful_deployment, verify_run_identity


class PagesDeploymentIdentityTests(unittest.TestCase):
    def receipt(self, states):
        def read_api(endpoint):
            if "/statuses?" not in endpoint:
                return [{"id": index} for index in range(len(states), 0, -1)]
            index = int(endpoint.split("/deployments/")[1].split("/")[0])
            return [{"state": states[index - 1], "created_at": "2026-09-09T20:56:02Z",
                     "log_url": "https://github.com/owner/site/actions/runs/123/job/456"}]
        return latest_successful_deployment("owner/site", read_api)

    def test_latest_success_is_selected_after_later_failed_attempt(self):
        receipt = self.receipt(["success", "failure"])
        self.assertEqual(1, receipt["deployment_id"])
        verify_run_identity(receipt, "owner/site", "123")

    def test_newer_success_supersedes_prior_receipt(self):
        self.assertEqual(2, self.receipt(["success", "success"])["deployment_id"])

    def test_pending_deployment_prevents_marker_or_retention_decision(self):
        for state in ("queued", "in_progress", "pending", "waiting", "unexpected"):
            with self.subTest(state=state), self.assertRaisesRegex(ValueError, "not terminal"):
                self.receipt(["success", state])

    def test_fresh_receipt_must_match_run_not_only_commit(self):
        receipt = self.receipt(["success"])
        with self.assertRaisesRegex(ValueError, "does not belong"):
            verify_run_identity(receipt, "owner/site", "124")

    def test_invalid_and_missing_api_evidence_fails_closed(self):
        for data in ({}, [], [{"id": "1"}], [{"id": True}]):
            with self.subTest(data=data), self.assertRaises(ValueError):
                latest_successful_deployment("owner/site", lambda _: data)
