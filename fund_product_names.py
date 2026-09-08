"""Exact-CUSIP descriptive names for gaps in the SEC fund-symbol directory.

This reviewed issuer evidence can only name an already resolved ETF. It cannot
resolve a symbol, classify an instrument, or change a position's identity.
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse

from security_identity import normalize_security_label

REVIEW_PATH = Path(__file__).with_name("reviewed_fund_product_names.json")
REVIEW_SHA256 = "7f7b5fde09361720e6fcad6dab2b1cb5411c43f9e4bcbbdf336d3731d4a9fdfd"
PRODUCT_NAME_SOURCE = "reviewed_primary_product_name"
ISSUER_HOSTS = frozenset({
    "www.innovatoretfs.com", "www.blackrock.com", "www.invesco.com",
})


def validate_review_bytes(raw: bytes, *, as_of: date | None = None) -> dict:
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("fund product-name review checksum mismatch")
    review = json.loads(raw)
    reviewed_at = date.fromisoformat(review["as_of"])
    age = ((as_of or date.today()) - reviewed_at).days
    if not 0 <= age <= 90:
        raise ValueError("fund product-name review requires primary-source revalidation")
    entries = review.get("securities")
    if review.get("schema_version") != 1 or not isinstance(entries, dict) or not entries:
        raise ValueError("invalid fund product-name review")
    for cusip, entry in entries.items():
        url = urlparse(entry.get("url", ""))
        name = entry.get("name")
        if (
            not re.fullmatch(r"[A-Z0-9]{9}", cusip)
            or entry.get("instrument_type") != "EQUITY"
            or entry.get("security_kind") != "ETF"
            or not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,15}", entry.get("ticker", ""))
            or not name
            or normalize_security_label(name, identifier=cusip) != name
            or url.scheme != "https"
            or url.hostname not in ISSUER_HOSTS
            or url.username or url.password
            or not re.fullmatch(r"[a-f0-9]{64}", entry.get("sha256", ""))
            or not entry.get("locator")
            or not str(entry.get("retrieved_at", "")).startswith(review["as_of"] + "T")
        ):
            raise ValueError(f"invalid exact issuer product-name proof: {cusip}")
    return entries


@lru_cache(maxsize=1)
def load_review() -> dict:
    return validate_review_bytes(REVIEW_PATH.read_bytes())


def reviewed_product_name(cusip: str, entry: dict) -> str | None:
    """Return a description only when the independently resolved identity agrees."""
    if (
        entry.get("type") != "EQUITY"
        or entry.get("security_kind") != "ETF"
        or entry.get("mapping_status") != "resolved"
    ):
        return None
    proof = load_review().get(cusip)
    if not proof or entry.get("ticker") != proof["ticker"]:
        return None
    return proof["name"]


def valid_reviewed_product_name(cusip: str, entry: dict) -> bool:
    """Do not accept a copied name/source tag as proof for another instrument."""
    return bool(entry.get("product_name")) and (
        entry.get("product_name") == reviewed_product_name(cusip, entry)
    )
