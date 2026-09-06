#!/usr/bin/env python3
"""Capture SEC mapping responses once, then rebuild independently offline.

The reported holding universe is an input. This does not repeat the separate
Form 13F corpus reconstruction or modify the published data/master pair.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import socket
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class FrozenInputError(BaseException):
    """A missing/corrupt replay input must bypass last-good fallback handlers."""


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def write_json(path: Path, obj: object) -> None:
    raw = (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(raw)
    temporary.replace(path)


class FrozenResponses:
    def __init__(self, root: Path, *, capture: bool):
        self.root = root
        self.capture = capture
        self.used: set[str] = set()
        self.path = root / "responses.json"
        self.entries = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.sealed = (root / "sealed.json").exists()
        if capture and self.sealed:
            raise FrozenInputError("A sealed input bundle cannot capture new responses")
        if not capture and not self.sealed:
            raise FrozenInputError("Replay requires a sealed input bundle")
        (root / "blobs").mkdir(exist_ok=True)

    def fetch(self, url: str, live=None) -> bytes:
        self.used.add(url)
        entry = self.entries.get(url)
        if entry is None:
            if not self.capture or live is None:
                raise FrozenInputError(f"Unrecorded SEC replay URL: {url}")
            try:
                raw = bytes(live(url))
            except Exception as exc:
                # Retain a stable failure outcome without persisting headers,
                # credentials, contact identity, or arbitrary exception text.
                entry = {"error_type": type(exc).__name__}
            else:
                sha = digest(raw)
                (self.root / "blobs" / sha).write_bytes(raw)
                entry = {"sha256": sha, "size_bytes": len(raw)}
            self.entries[url] = entry
            write_json(self.path, self.entries)
            if len(self.entries) % 25 == 0:
                print(f"Captured {len(self.entries)} distinct SEC responses", flush=True)
        if "error_type" in entry:
            raise RuntimeError(f"Frozen SEC request failure: {entry['error_type']}")
        sha = entry.get("sha256", "")
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise FrozenInputError("Invalid frozen response checksum")
        try:
            raw = (self.root / "blobs" / sha).read_bytes()
        except OSError as exc:
            raise FrozenInputError("Missing frozen SEC response") from exc
        if len(raw) != entry["size_bytes"] or digest(raw) != sha:
            raise FrozenInputError("Frozen SEC response checksum mismatch")
        return raw


def disable_network() -> None:
    def denied(*args, **kwargs):
        raise FrozenInputError("Network access attempted during offline SEC replay")
    socket.socket.connect = denied
    socket.socket.connect_ex = denied
    socket.create_connection = denied
    socket.getaddrinfo = denied


def code_hashes() -> dict[str, str]:
    names = ["pipeline.py", "sec_security_master.py", "sec_edgar_evidence.py",
             "sec_http.py", "atomic_files.py",
             "security_identity.py", "scripts/frozen_sec_rebuild.py"]
    return {name: digest((ROOT / name).read_bytes()) for name in names}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture", "replay"))
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture = args.mode == "capture"
    # Separate empty output directories are mandatory. Never seed from a saved
    # source state, master, EDGAR journal, or production output.
    args.output.mkdir(parents=True, exist_ok=False)
    args.bundle.mkdir(parents=True, exist_ok=True)
    if not capture:
        disable_network()
    import pipeline
    import sec_security_master as sec
    from sec_edgar_evidence import make_sec_discovery_fetcher

    started = time.monotonic()
    metadata_path = args.bundle / "inputs.json"
    universe_path = args.bundle / "universe.json"
    if capture and not metadata_path.exists():
        universe = pipeline.collect_security_master_universe()
        write_json(universe_path, universe)
        write_json(metadata_path, {
            "schema_version": 1,
            "as_of": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "universe_sha256": digest(universe_path.read_bytes()),
            "code_sha256": code_hashes(),
            "scope": "SEC mapping sources; independently verified reported holdings are inputs",
        })
    metadata = json.loads(metadata_path.read_text())
    if metadata["code_sha256"] != code_hashes():
        raise FrozenInputError("Builder code differs from the frozen input contract")
    raw_universe = universe_path.read_bytes()
    if digest(raw_universe) != metadata["universe_sha256"]:
        raise FrozenInputError("Frozen holding universe checksum mismatch")
    universe = json.loads(raw_universe)
    current = datetime.fromisoformat(metadata["as_of"])
    responses = FrozenResponses(args.bundle, capture=capture)
    if not capture:
        seal = json.loads((args.bundle / "sealed.json").read_text())
        if seal["responses_sha256"] != digest(responses.path.read_bytes()):
            raise FrozenInputError("Frozen response manifest changed after sealing")
        if seal["inputs_sha256"] != digest(metadata_path.read_bytes()):
            raise FrozenInputError("Frozen build metadata changed after sealing")
    live_core = sec.make_sec_fetcher() if capture else None
    live_edgar = make_sec_discovery_fetcher() if capture else None
    paths = {"master_path": args.output / "sec_security_master.json",
             "source_state_path": args.output / "sec_source_state.json"}
    print(f"Starting {args.mode} from empty state with {len(universe)} identities", flush=True)
    result = sec.refresh_security_master(
        universe, **paths, now=current, lookback_months=None,
        fetcher=lambda url: responses.fetch(url, live_core),
        minimum_current_symbol_population_by_kind=pipeline.PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND,
        minimum_current_symbol_title_ratio=pipeline.PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO,
        minimum_active_official_cusip_count=pipeline.PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT,
        enforce_latest_completed_official_period=True,
        enforce_reported_identity_evidence=True,
    )
    if result.errors:
        raise FrozenInputError(f"Core SEC build failed: {result.errors}")
    print("Core archive parsing complete; rebuilding supplemental evidence", flush=True)
    result = pipeline._refresh_sec_fund_series_evidence(
        result, universe, **paths, refreshed_at=current,
        fetcher=lambda url: responses.fetch(url, live_core),
    )
    result = pipeline._refresh_sec_edgar_exceptions(
        result, universe, **paths, refreshed_at=current,
        fetcher=lambda url: responses.fetch(url, live_edgar),
        checkpoint_batches=True, checkpoint_root=args.output,
    )
    acceptance = sec.audit_security_master(result.master, as_of=current)
    if result.errors or not acceptance["ok"]:
        raise FrozenInputError(f"Frozen build acceptance failed: {result.errors}, {acceptance}")
    if responses.used != set(responses.entries):
        raise FrozenInputError("Build did not consume exactly the frozen response inventory")
    seal = {"responses_sha256": digest(responses.path.read_bytes()),
            "inputs_sha256": digest(metadata_path.read_bytes())}
    if capture:
        write_json(args.bundle / "sealed.json", seal)
    report = {
        "mode": args.mode, "started_with_empty_state": True,
        "network_disabled": not capture, "elapsed_seconds": time.monotonic() - started,
        "response_count": len(responses.used), "input_seal": seal,
        "record_count": len(result.master["records"]), "acceptance": acceptance,
        "outputs": {name: digest(path.read_bytes()) for name, path in paths.items()},
    }
    write_json(args.output / "build-report.json", report)
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
