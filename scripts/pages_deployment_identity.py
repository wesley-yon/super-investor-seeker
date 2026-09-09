#!/usr/bin/env python3
"""Identify the successful Pages deployment while the Pages lock is held."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def latest_successful_deployment(repository: str, read_api) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("invalid public repository identity")
    deployments = read_api(f"/repos/{repository}/deployments?environment=github-pages&per_page=20")
    if not isinstance(deployments, list):
        raise ValueError("invalid Pages deployment list")
    for deployment in deployments:
        deployment_id = deployment.get("id")
        if type(deployment_id) is not int or deployment_id < 1:
            raise ValueError("invalid Pages deployment ID")
        statuses = read_api(f"/repos/{repository}/deployments/{deployment_id}/statuses?per_page=1")
        if not isinstance(statuses, list) or not statuses or not isinstance(statuses[0], dict):
            raise ValueError("Pages deployment has no verifiable status")
        status = statuses[0]
        state = status.get("state")
        if state == "success":
            timestamp = status.get("created_at")
            if not isinstance(timestamp, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timestamp):
                raise ValueError("successful Pages deployment has no valid timestamp")
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return {"deployment_id": deployment_id, "deployed_at": timestamp,
                    "log_url": status.get("log_url", "")}
        if state not in {"failure", "error", "inactive"}:
            raise ValueError("a newer Pages deployment is not terminal; refusing finalization")
    raise ValueError("no successful Pages deployment")


def verify_run_identity(receipt: dict, repository: str, run_id: str) -> None:
    if not run_id.isdecimal() or not re.fullmatch(
        re.escape(f"https://github.com/{repository}/actions/runs/{run_id}/job/") + r"[0-9]+",
        receipt["log_url"],
    ):
        raise ValueError("successful Pages deployment does not belong to this workflow run")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", help="Require a fresh deployment from this run; omit for an already verified no-op")
    parser.add_argument("--github-output", action="store_true")
    args = parser.parse_args()
    repository = os.environ["GITHUB_REPOSITORY"]

    def read_api(endpoint):
        return json.loads(subprocess.check_output([
            sys.executable, str(Path(__file__).with_name("github_cli_retry.py")),
            "--retry-forbidden-read", "--", "api", endpoint,
        ], text=True))

    receipt = latest_successful_deployment(repository, read_api)
    if args.run_id is not None:
        verify_run_identity(receipt, repository, args.run_id)
    if args.github_output:
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write(f"deployment_id={receipt['deployment_id']}\n")
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
