#!/usr/bin/env python3
"""Audit or apply the exact reviewed NOTE repairs, then rebuild site outputs.

Default: dry run. --apply stages every changed fund, verifies the whole batch,
and replaces files under the pipeline maintenance lock. --rebuild regenerates
the registry, stock pages, index and labels without changing quantity policy.
Original filing text, row amounts and source hashes are never changed.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from composition_integrity import calculate_quarter_composition_hash  # noqa: E402
from note_classification import (  # noqa: E402
    REVIEW_SHA256, classification_review, reviewed_note_type,
)
from security_identity import holding_instrument_type  # noqa: E402


def quarter_hash(quarter: dict) -> str:
    return calculate_quarter_composition_hash(quarter, current_hash_version=3)


def repair_fund(fund: dict, *, review: str = "note") -> tuple[dict, dict]:
    """Return a copy with only the selected review types and hashes changed."""
    if review == "preferred":
        from preferred_classification import REVIEW_SHA256 as digest, reviewed_preferred_type as correct
    elif review == "note":
        digest, correct = REVIEW_SHA256, reviewed_note_type
    else:
        raise ValueError("unknown classification review")
    repair_key = f"{review}_classification_repair"
    candidate = deepcopy(fund)
    counts: Counter[str] = Counter()
    changed_quarters = 0
    for before, quarter in zip(fund.get("quarters", []), candidate.get("quarters", [])):
        changes = []
        for index, holding in enumerate(quarter.get("holdings", [])):
            kind = holding_instrument_type(holding)
            corrected = correct(holding, kind)
            if not corrected:
                continue
            # Explicit option class text also remains an option, even if a
            # legacy stored type was NOTE. That separate ambiguity is not in
            # this repair's authorization or proof set.
            cls = str(holding.get("reported_class") or holding.get("class") or "").upper().strip()
            if cls in {"CALL", "PUT", "OPT", "OPTION", "OPTIONS", "EQUITY OPTION", "ETF OPTION"}:
                continue
            cusip = str(holding.get("cusip") or "").strip().upper()
            holding["holding_type"] = corrected
            changes.append({"row": index, "cusip": cusip, "from": kind, "to": corrected})
            counts[cusip] += 1
        if not changes:
            continue
        prior_hash = before.get("composition_hash")
        if prior_hash is not None:
            if quarter_hash(before) != prior_hash:
                raise ValueError(f"pre-repair composition hash mismatch: {fund.get('cik')}/{quarter.get('report_date')}")
            quarter["composition_hash"] = quarter_hash(quarter)
        elif before.get("composition_version") is not None:
            raise ValueError("composed quarter is missing its source hash")

        # This assertion covers every field, not just selected financial
        # totals: undo the authorized type/hash changes and require equality.
        restored = deepcopy(quarter)
        for change in changes:
            old_row = before["holdings"][change["row"]]
            if "holding_type" in old_row:
                restored["holdings"][change["row"]]["holding_type"] = old_row["holding_type"]
            else:
                restored["holdings"][change["row"]].pop("holding_type", None)
        if prior_hash is not None:
            restored["composition_hash"] = prior_hash
        if restored != before:
            raise ValueError("classification repair changed unrelated fields")
        if repair_key in quarter:
            raise ValueError(f"repaired quarter acquired a new {review} classification; review its provenance")
        quarter[repair_key] = {
            "review_sha256": digest,
            "prior_composition_hash": prior_hash,
            "prior_composition_hash_version": before.get("composition_hash_version", 1),
            "changes": changes,
        }
        changed_quarters += 1
    return candidate, {"quarters": changed_quarters, "rows": sum(counts.values()), "by_cusip": dict(counts)}


def stage_fund(args: tuple[str, str, str]) -> dict:
    source_path, staging = map(Path, args[:2])
    review = args[2] if len(args) > 2 else "note"
    if source_path.is_symlink():
        raise ValueError(f"fund must not be a symlink: {source_path.name}")
    raw = source_path.read_bytes()
    candidate, result = repair_fund(json.loads(raw), review=review)
    result.update(name=source_path.name, before_sha256=hashlib.sha256(raw).hexdigest())
    if result["rows"]:
        (staging / source_path.name).write_text(json.dumps(candidate, separators=(",", ":")) + "\n")
    return result


def repair_directory(funds_dir: Path, *, apply: bool, workers: int = 2, review: str = "note") -> dict:
    """Stage and verify all files before mutation; retain originals on rollback."""
    if review == "preferred":
        from preferred_classification import classification_review as load, REVIEW_SHA256 as digest
    elif review == "note":
        load, digest = classification_review, REVIEW_SHA256
    else:
        raise ValueError("unknown classification review")
    load()  # Fail before any writes if review bytes changed.
    funds_dir = funds_dir.resolve()
    if not funds_dir.is_dir():
        raise ValueError("funds directory is missing")
    paths = sorted(funds_dir.glob("*.json"))
    if not paths:
        raise ValueError("funds directory is empty")
    with tempfile.TemporaryDirectory(prefix=".note-repair-", dir=funds_dir.parent) as temporary:
        staging = Path(temporary)
        inputs = [(str(p), str(staging), review) for p in paths]
        if workers == 1:
            results = list(map(stage_fund, inputs))
        else:
            with ProcessPoolExecutor(max_workers=min(workers, len(paths))) as pool:
                results = list(pool.map(stage_fund, inputs, chunksize=16))
        changed = [r for r in results if r["rows"]]
        counts: Counter[str] = Counter()
        for row in changed:
            counts.update(row["by_cusip"])
        report = {
            "review_sha256": digest, "mode": "apply" if apply else "dry_run",
            "funds": len(changed), "quarters": sum(r["quarters"] for r in changed),
            "rows": sum(counts.values()), "by_cusip": dict(sorted(counts.items())),
            "original_fields_preserved": True,
        }
        if apply:
            # Detect concurrent edits before committing any staged replacement.
            for row in changed:
                if hashlib.sha256((funds_dir / row["name"]).read_bytes()).hexdigest() != row["before_sha256"]:
                    raise ValueError("fund changed after classification repair was staged")
            originals = staging / "originals"
            originals.mkdir()
            replaced = []
            try:
                for row in changed:
                    name = row["name"]
                    os.link(funds_dir / name, originals / name)
                    os.replace(staging / name, funds_dir / name)
                    replaced.append(name)
            except BaseException:
                for name in reversed(replaced):
                    os.replace(originals / name, funds_dir / name)
                raise
        return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--pending-only", action="store_true",
                        help="skip a completed review recorded after a successful rebuild")
    parser.add_argument("--workers", type=int, default=2, choices=range(1, 9))
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.rebuild and not args.apply:
        parser.error("--rebuild requires --apply")
    if args.pending_only and not (args.apply and args.rebuild):
        parser.error("--pending-only requires --apply --rebuild")
    import pipeline

    @pipeline._serialize_pipeline_maintenance
    def run() -> dict:
        state = pipeline.load_state()
        complete = state.get("note_classification_review_sha256") == REVIEW_SHA256
        if args.pending_only and complete:
            classification_review()
            return {"review_sha256": REVIEW_SHA256, "already_applied": True, "rows": 0}
        report = repair_directory(pipeline.FUNDS_DIR, apply=args.apply, workers=args.workers)
        if args.rebuild and (report["rows"] or not complete):
            pipeline.rebuild_registry_backed_outputs(preserve_position_economics=True)
            # Record completion only after all derived artifacts succeeded.
            state = pipeline.load_state()
            state["note_classification_review_sha256"] = REVIEW_SHA256
            pipeline.save_state(state)
            report["outputs_rebuilt"] = True
        return report

    report = run()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "by_cusip"}))


if __name__ == "__main__":
    main()
