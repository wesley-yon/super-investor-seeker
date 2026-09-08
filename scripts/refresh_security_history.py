#!/usr/bin/env python3
"""Refresh reviewed identifier metadata without combining filing positions."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from security_history import REVIEW_SHA256, history_review


def refresh(pipeline, *, pending_only=False):
    history_review()
    state = pipeline.load_state()
    if pending_only and state.get("security_history_review_sha256") == REVIEW_SHA256:
        return {"already_applied": True, "review_sha256": REVIEW_SHA256}
    pipeline.rebuild_registry_backed_outputs(preserve_position_economics=True)
    state = pipeline.load_state()
    state["security_history_review_sha256"] = REVIEW_SHA256
    pipeline.save_state(state)
    return {"outputs_rebuilt": True, "review_sha256": REVIEW_SHA256}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pending-only", action="store_true")
    args = parser.parse_args()
    import pipeline
    run = pipeline._serialize_pipeline_maintenance(refresh)
    print(json.dumps(run(pipeline, pending_only=args.pending_only)))


if __name__ == "__main__":
    main()
