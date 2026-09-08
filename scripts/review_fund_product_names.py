#!/usr/bin/env python3
"""Re-fetch approved issuer URLs into a review candidate; never change live data."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
from io import BytesIO
import json
from pathlib import Path
import sys
import re
import time
from urllib.parse import urlparse

from lxml import html
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from fund_product_names import ISSUER_HOSTS, REVIEW_PATH  # noqa: E402


def parse_dws_pdf_text(text: str, *, cusip: str, ticker: str) -> dict:
    """Use the fund header and ETF details, excluding index/NAV tickers."""
    text = " ".join(text.split())
    header = re.match(r"^(.*?) Q[1-4] \| \d{1,2}\.\d{1,2}\.\d{2} Ticker: ([A-Z0-9.-]+)\b", text)
    details = re.findall(r"ETF details (.*?) Index details", text)
    if not header or len(details) != 1:
        raise ValueError("missing or ambiguous issuer ETF factsheet fields")
    symbols = re.findall(r"NYSE ticker ([A-Z0-9.-]+) NAV ticker", details[0])
    cusips = re.findall(r"\bCUSIP ([A-Z0-9]{9})\b", details[0])
    name, source_ticker = header.groups()
    if source_ticker != ticker or symbols != [ticker] or cusips != [cusip] or not name.endswith(" ETF"):
        raise ValueError("issuer PDF does not prove the exact ETF CUSIP and ticker")
    return {"name": name, "series": ""}


def parse_page(raw: bytes, *, url: str, cusip: str, ticker: str) -> dict:
    host = urlparse(url).hostname
    if host not in ISSUER_HOSTS or urlparse(url).scheme != "https":
        raise ValueError("unapproved issuer host")
    if host == "etf.dws.com":
        if not raw.startswith(b"%PDF-"):
            raise ValueError("issuer factsheet must be a PDF")
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise ValueError("PDF source revalidation requires the optional pypdf package") from exc
        return parse_dws_pdf_text(PdfReader(BytesIO(raw)).pages[0].extract_text(), cusip=cusip, ticker=ticker)
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
    elif host == "leverageshares.com":
        source_cusip = one('//span[normalize-space(text())="CUSIP"]/following-sibling::span')
        source_ticker = one('//span[normalize-space(text())="Ticker"]/following-sibling::span')
        name = one('//h1')
        products = [json.loads(value) for value in doc.xpath('//script[@type="application/ld+json"]/text()')]
        products = [value for value in products if isinstance(value, dict) and value.get("@type") == "FinancialProduct"]
        if len(products) != 1:
            raise ValueError("missing or ambiguous issuer product identity")
        product = products[0]
        if (product.get("alternateName") != ticker
                or product.get("provider", {}).get("name") != "Leverage Shares"
                or " ".join(str(product.get("name", "")).split()).casefold() != name.casefold()):
            raise ValueError("conflicting issuer product identity")
        name = "Leverage Shares " + name
    elif host == "bondbloxxetf.com":
        # Scope to Fund Details, excluding the benchmark's own Ticker row.
        tables = doc.xpath('//table[.//tr[1]/td[1][normalize-space(.)="Product Name"]]')
        if len(tables) != 1:
            raise ValueError("missing or ambiguous issuer fund-details table")
        facts = {}
        for row in tables[0].xpath('.//tr'):
            cells = row.xpath('./td')
            if len(cells) != 2:
                continue
            key, value = (" ".join(cell.text_content().split()) for cell in cells)
            if key not in {"Product Name", "Ticker", "CUSIP"}:
                continue
            if key in facts:
                raise ValueError("ambiguous issuer fund fact")
            facts[key] = value
        source_cusip, source_ticker = facts.get("CUSIP"), facts.get("Ticker")
        name = facts.get("Product Name", "")
    elif host == "www.allianzim.com":
        def fact(label):
            return one('//li[.//span[@class="page-sidebar__label"][normalize-space(.)=$label]]'
                       '/div[@class="page-sidebar__value"]', label=label)
        source_cusip, source_ticker = fact("CUSIP"), fact("Ticker")
        name = fact("Fund Name")
    elif host == "bluemontefunds.com":
        source_cusip = one('//tr[td[1][normalize-space(.)="Cusip"]]/td[2]')
        source_ticker = one('//tr[td[1][normalize-space(.)="Ticker"]]/td[2]')
        if one('//h1') != source_ticker:
            raise ValueError("conflicting issuer heading and fund symbol")
        name = one('//h2')
    elif host == "coinshares.com":
        def fact(label):
            return one('//li[span[@class="table-name"][normalize-space(.)=$label]]/button/span', label=label)
        source_cusip, source_ticker = fact("CUSIP"), fact("Ticker")
        name = fact("Product name")
    elif host == "www.rexshares.com":
        def fact(label):
            return one('//div[@class="t-row"][div[normalize-space(.)=$label]]'
                       '/div[contains(concat(" ",normalize-space(@class)," ")," t-data ")]', label=label)
        source_cusip, source_ticker = fact("CUSIP"), fact("Ticker")
        name = one('//h2[starts-with(normalize-space(.),"T-REX ") and substring(normalize-space(.),'
                   'string-length(normalize-space(.))-3)=" ETF"]')
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
    parser.add_argument("--source-directory", type=Path,
                        help="Use freshly captured TICKER.html/.pdf evidence instead of HTTP requests")
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
            if args.source_directory:
                suffix = "pdf" if entry.get("source_format") == "pdf" else "html"
                source = args.source_directory / f'{entry["ticker"]}.{suffix}'
                captured_at = datetime.fromtimestamp(source.stat().st_mtime, timezone.utc)
                if captured_at.date() != datetime.now(timezone.utc).date():
                    raise ValueError("local evidence must be freshly captured today")
                raw, url = source.read_bytes(), entry["url"]
                source_format = entry.get("source_format", "html")
            else:
                response = requests.get(entry["url"], timeout=30)
                response.raise_for_status()
                raw, url = response.content, response.url
                captured_at = datetime.now(timezone.utc)
                source_format = "pdf" if raw.startswith(b"%PDF-") else "html"
            parsed = parse_page(raw, url=url, cusip=cusip, ticker=entry["ticker"])
            result = {**entry, **parsed, "url": url,
                      "sha256": hashlib.sha256(raw).hexdigest(),
                      "retrieved_at": captured_at.isoformat(), "source_format": source_format}
            return cusip, result, None
        except Exception as exc:
            return cusip, None, str(exc)
        finally:
            if not args.source_directory:
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
