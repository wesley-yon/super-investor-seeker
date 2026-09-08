#!/usr/bin/env python3
"""Dry-run or apply the exact preferred-interest review and rebuild site data."""
import argparse
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from preferred_classification import REVIEW_SHA256, classification_review  # noqa: E402
from scripts.repair_note_classifications import repair_directory  # noqa: E402


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
        complete = state.get("preferred_classification_review_sha256") == REVIEW_SHA256
        if args.pending_only and complete:
            classification_review()
            return {"review_sha256": REVIEW_SHA256, "already_applied": True, "rows": 0}
        report = repair_directory(pipeline.FUNDS_DIR, apply=args.apply, workers=args.workers, review="preferred")
        if args.rebuild and (report["rows"] or not complete):
            pipeline.rebuild_registry_backed_outputs(preserve_position_economics=True)
            # Record completion only after all derived artifacts succeeded.
            state = pipeline.load_state()
            state["preferred_classification_review_sha256"] = REVIEW_SHA256
            pipeline.save_state(state)
            report["outputs_rebuilt"] = True
        return report

    report = run()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "by_cusip"}))


if __name__ == "__main__":
    main()
