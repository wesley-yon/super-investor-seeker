from __future__ import annotations

import io
import subprocess
import unittest
from unittest import mock

from scripts import github_cli_retry


class GitHubCliRetryTests(unittest.TestCase):
    def test_retries_transient_http_error_and_preserves_success_stdout(self) -> None:
        runner = mock.Mock(
            side_effect=[
                subprocess.CompletedProcess(
                    args=["gh", "api", "/rate-limited"],
                    returncode=1,
                    stdout="",
                    stderr="gh: HTTP 503: Service Unavailable\n",
                ),
                subprocess.CompletedProcess(
                    args=["gh", "api", "/rate-limited"],
                    returncode=0,
                    stdout='{"ok":true}\n',
                    stderr="",
                ),
            ]
        )
        sleeper = mock.Mock()
        stdout = io.StringIO()
        stderr = io.StringIO()

        status = github_cli_retry.run(
            ["api", "/rate-limited"],
            runner=runner,
            sleeper=sleeper,
            stdout=stdout,
            stderr=stderr,
        )

        self.assertEqual(0, status)
        self.assertEqual('{"ok":true}\n', stdout.getvalue())
        self.assertEqual(2, runner.call_count)
        sleeper.assert_called_once_with(1)
        self.assertIn("retrying in 1 second", stderr.getvalue())

    def test_missing_release_is_success_only_when_explicitly_allowed(self) -> None:
        missing = subprocess.CompletedProcess(
            args=["gh", "release", "delete", "dataset-stale"],
            returncode=1,
            stdout="",
            stderr="release not found\n",
        )
        allowed_stderr = io.StringIO()

        allowed_status = github_cli_retry.run(
            ["release", "delete", "dataset-stale"],
            allow_release_not_found=True,
            runner=mock.Mock(return_value=missing),
            sleeper=mock.Mock(),
            stdout=io.StringIO(),
            stderr=allowed_stderr,
        )
        denied_status = github_cli_retry.run(
            ["release", "delete", "dataset-stale"],
            runner=mock.Mock(return_value=missing),
            sleeper=mock.Mock(),
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(0, allowed_status)
        self.assertIn("already absent", allowed_stderr.getvalue())
        self.assertEqual(1, denied_status)

    def test_missing_release_override_cannot_mask_an_unrelated_api_404(self) -> None:
        missing = subprocess.CompletedProcess(
            args=["gh", "api", "/repos/owner/missing"],
            returncode=1,
            stdout="HTTP 404: Not Found\n",
            stderr="",
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        status = github_cli_retry.run(
            ["api", "/repos/owner/missing"],
            allow_release_not_found=True,
            runner=mock.Mock(return_value=missing),
            sleeper=mock.Mock(),
            stdout=stdout,
            stderr=stderr,
        )

        self.assertEqual(1, status)
        self.assertEqual("", stdout.getvalue())
        self.assertIn("HTTP 404", stderr.getvalue())

    def test_plain_http_403_fails_without_retry(self) -> None:
        forbidden = subprocess.CompletedProcess(
            args=["gh", "api", "/forbidden"],
            returncode=1,
            stdout="",
            stderr="gh: HTTP 403: Forbidden\n",
        )
        runner = mock.Mock(return_value=forbidden)
        sleeper = mock.Mock()

        status = github_cli_retry.run(
            ["api", "/forbidden"],
            runner=runner,
            sleeper=sleeper,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(1, status)
        runner.assert_called_once()
        sleeper.assert_not_called()

    def test_retries_are_bounded_when_transient_failure_persists(self) -> None:
        unavailable = subprocess.CompletedProcess(
            args=["gh", "api", "/unavailable"],
            returncode=1,
            stdout="",
            stderr="HTTP 503: Service Unavailable\n",
        )
        runner = mock.Mock(return_value=unavailable)
        sleeper = mock.Mock()

        status = github_cli_retry.run(
            ["api", "/unavailable"],
            runner=runner,
            sleeper=sleeper,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(1, status)
        self.assertEqual(3, runner.call_count)
        self.assertEqual([mock.call(1), mock.call(3)], sleeper.call_args_list)

    def test_explicit_rate_limited_403_is_retryable(self) -> None:
        rate_limited = subprocess.CompletedProcess(
            args=["gh", "api", "/rate-limited"],
            returncode=1,
            stdout="",
            stderr="HTTP 403: secondary rate limit exceeded\n",
        )
        success = subprocess.CompletedProcess(
            args=["gh", "api", "/rate-limited"],
            returncode=0,
            stdout="ok\n",
            stderr="",
        )
        runner = mock.Mock(side_effect=[rate_limited, success])
        sleeper = mock.Mock()

        status = github_cli_retry.run(
            ["api", "/rate-limited"],
            runner=runner,
            sleeper=sleeper,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(0, status)
        self.assertEqual(2, runner.call_count)
        sleeper.assert_called_once_with(1)

    def test_fresh_token_mode_retries_plain_403_for_read_only_command(self) -> None:
        forbidden = subprocess.CompletedProcess(
            args=["gh", "release", "view", "dataset-current"],
            returncode=1,
            stdout="",
            stderr="gh: HTTP 403: Forbidden\n",
        )
        success = subprocess.CompletedProcess(
            args=["gh", "release", "view", "dataset-current"],
            returncode=0,
            stdout="false\n",
            stderr="",
        )
        runner = mock.Mock(side_effect=[forbidden, success])
        sleeper = mock.Mock()

        status = github_cli_retry.run(
            ["release", "view", "dataset-current"],
            retry_forbidden_read=True,
            runner=runner,
            sleeper=sleeper,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(0, status)
        self.assertEqual(2, runner.call_count)
        sleeper.assert_called_once_with(1)

    def test_transient_mutation_is_reported_without_automatic_replay(self) -> None:
        unavailable = subprocess.CompletedProcess(
            args=["gh", "release", "edit", "dataset-current", "--latest"],
            returncode=1,
            stdout="",
            stderr="HTTP 503: Service Unavailable\n",
        )
        runner = mock.Mock(return_value=unavailable)
        sleeper = mock.Mock()

        status = github_cli_retry.run(
            ["release", "edit", "dataset-current", "--latest"],
            runner=runner,
            sleeper=sleeper,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(github_cli_retry.TRANSIENT_MUTATION_EXIT_CODE, status)
        runner.assert_called_once()
        sleeper.assert_not_called()

    def test_api_delete_is_reported_without_automatic_replay(self) -> None:
        unavailable = subprocess.CompletedProcess(
            args=["gh", "api", "--method", "DELETE", "/git/refs/tags/stale"],
            returncode=1,
            stdout="",
            stderr="HTTP 503: Service Unavailable\n",
        )
        runner = mock.Mock(return_value=unavailable)
        sleeper = mock.Mock()

        status = github_cli_retry.run(
            ["api", "--method", "DELETE", "/git/refs/tags/stale"],
            runner=runner,
            sleeper=sleeper,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(github_cli_retry.TRANSIENT_MUTATION_EXIT_CODE, status)
        runner.assert_called_once()
        sleeper.assert_not_called()

    def test_fresh_token_mode_rejects_mutating_command(self) -> None:
        runner = mock.Mock()

        with self.assertRaisesRegex(ValueError, "read-only"):
            github_cli_retry.run(
                ["release", "edit", "dataset-current", "--latest"],
                retry_forbidden_read=True,
                runner=runner,
                sleeper=mock.Mock(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        runner.assert_not_called()

    def test_release_download_retries_only_with_clobber(self) -> None:
        unavailable = subprocess.CompletedProcess(
            args=["gh", "release", "download", "dataset-current"],
            returncode=1,
            stdout="",
            stderr="HTTP 503: Service Unavailable\n",
        )
        without_clobber = mock.Mock(return_value=unavailable)
        with_clobber = mock.Mock(
            side_effect=[
                unavailable,
                subprocess.CompletedProcess(
                    args=[
                        "gh",
                        "release",
                        "download",
                        "dataset-current",
                        "--clobber",
                    ],
                    returncode=0,
                    stdout="",
                    stderr="",
                ),
            ]
        )

        unsafe_status = github_cli_retry.run(
            ["release", "download", "dataset-current"],
            runner=without_clobber,
            sleeper=mock.Mock(),
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )
        safe_status = github_cli_retry.run(
            ["release", "download", "dataset-current", "--clobber"],
            runner=with_clobber,
            sleeper=mock.Mock(),
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        self.assertEqual(github_cli_retry.TRANSIENT_MUTATION_EXIT_CODE, unsafe_status)
        without_clobber.assert_called_once()
        self.assertEqual(0, safe_status)
        self.assertEqual(2, with_clobber.call_count)


if __name__ == "__main__":
    unittest.main()
