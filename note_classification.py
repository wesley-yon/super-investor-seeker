"""Narrow, checksummed corrections for 59 securities formerly parsed as NOTE.

The original SEC text is evidence, not a reliable security-type vocabulary:
IBONDS names contain BOND, and preferred-share classes can contain a coupon.
Only the reviewed exact identifiers can override those parser heuristics.
"""
from __future__ import annotations

from functools import lru_cache
import hashlib
import json
from pathlib import Path

REVIEW_SHA256 = "81f865a122d23dea7845077bb4a5347ec2b9b858eb8a9b3e742480ba4a869a70"
REVIEW_PATH = Path(__file__).with_name("reviewed_note_classifications.json")
REVIEW_SOURCE = "reviewed_primary_classification"


@lru_cache(maxsize=1)
def classification_review() -> dict:
    raw = REVIEW_PATH.read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("note-classification review checksum mismatch")
    return json.loads(raw)["securities"]


def reviewed_note_type(holding: dict, parsed_type: str) -> str | None:
    """Correct only an exact NOTE; explicit or legacy options never qualify."""
    if parsed_type != "NOTE":
        return None
    if str(holding.get("put_call") or "").strip().upper() in {"CALL", "PUT"}:
        return None
    cusip = str(holding.get("cusip") or "").strip().upper()
    reported = str(holding.get("reported_cusip") or cusip).strip().upper()
    if reported != cusip:
        return None
    entry = classification_review().get(cusip)
    return entry["to_type"] if entry else None


def public_note_type_corrections(registry: dict) -> dict[str, str]:
    """Publish redirects only after the corresponding corpus type is repaired."""
    return {
        cusip: spec["to_type"]
        for cusip, spec in classification_review().items()
        if registry.get(cusip, {}).get("type") == spec["to_type"]
    }
