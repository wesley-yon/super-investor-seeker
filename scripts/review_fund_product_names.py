#!/usr/bin/env python3
"""Re-fetch approved issuer URLs into a review candidate; never change live data."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlparse

from lxml import html
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from fund_product_names import ISSUER_HOSTS, REVIEW_PATH  # noqa: E402


def parse_page(raw: bytes, *, url: str, cusip: str, ticker: str) -> dict:
    host = urlparse(url).hostname
    if host not in ISSUER_HOSTS or urlparse(url).scheme != "https":
        raise ValueError("unapproved issuer host")
    doc = html.fromstring(raw)

    def one(xpath: str, **kwargs) -> str:
        values = doc.xpath(xpath, **kwargs)
        if len(values) != 1:
            raise ValueError("missing or ambiguous issuer identity field")
        value = values[0]
        return " ".join((value.text_content() if hasattr(value, "text_content") else value).split())

    series = ""
    if host == "www.innovatoretfs.com":
        facts = {}
        for label in doc.xpath('//*[contains(concat(" ",normalize-space(@class)," ")," al_fund ")]'):
            key = " ".join(label.text_content().split())
            if key not in {"CUSIP", "Ticker", "Series"}:
                continue
            values = label.getparent().xpath('./*[contains(concat(" ",normalize-space(@class)," ")," ar_fund ")]')
            if len(values) != 1:
                raise ValueError("ambiguous issuer fund fact")
            value = " ".join(values[0].text_content().split())
            if key in facts and facts[key] != value:
                raise ValueError("conflicting issuer fund fact")
            facts[key] = value
        source_cusip, source_ticker = facts.get("CUSIP"), facts.get("Ticker")
        title = one('//title')
        prefix = ticker + " - "
        if not title.startswith(prefix):
            raise ValueError("issuer title does not match requested symbol")
        name = title[len(prefix):].replace("™", "").replace("®", "").strip()
        series = facts.get("Series", "")
        if series and series not in {"-", "--", "N/A"} and series.casefold() not in name.casefold():
            name += " — " + series
    elif host == "www.blackrock.com":
        name = one('//h1')
        source_cusip = one('//*[@data-id="keyFundFacts-cusip-data"]')
        title = one('//title')
        source_ticker = title.rsplit("|", 1)[-1].strip()
    else:
        source_cusip = one('//meta[@name="cusip"]/@content')
        source_ticker = one('//meta[@name="ticker"]/@content')
        name = one('//meta[@name="shareClassFullName"]/@content')
    if source_cusip != cusip or source_ticker != ticker or "ETF" not in name:
        raise ValueError("issuer page does not prove the exact ETF CUSIP and ticker")
    return {"name": name, "series": series}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New candidate JSON; must not exist")
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("candidate output already exists")
    # Expiration is why this command may be needed, so read the old review as
    # an input URL list. The production loader still requires approved hashes.
    current = json.loads(REVIEW_PATH.read_bytes())
    errors = []

    def fetch(item):
        cusip, entry = item
        try:
            response = requests.get(entry["url"], timeout=30)
            response.raise_for_status()
            parsed = parse_page(response.content, url=response.url, cusip=cusip, ticker=entry["ticker"])
            result = {**entry, **parsed, "url": response.url,
                      "sha256": hashlib.sha256(response.content).hexdigest(),
                      "retrieved_at": datetime.now(timezone.utc).isoformat()}
            return cusip, result, None
        except Exception as exc:
            return cusip, None, str(exc)
        finally:
            time.sleep(1)

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(fetch, sorted(current["securities"].items())))
    entries = {}
    for cusip, entry, error in results:
        if error:
            errors.append({"cusip": cusip, "error": error})
        else:
            entries[cusip] = entry
    if errors:
        raise ValueError("incomplete issuer review: " + json.dumps(errors))
    candidate = {"schema_version": 1, "as_of": datetime.now(timezone.utc).date().isoformat(),
                 "securities": entries}
    raw = (json.dumps(candidate, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode()
    with args.output.open("xb") as handle:
        handle.write(raw)
    print(json.dumps({"candidate": str(args.output), "securities": len(entries),
                      "sha256": hashlib.sha256(raw).hexdigest()}))


if __name__ == "__main__":
    main()
