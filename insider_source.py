"""Offline, fail-closed SEC filing-index metadata parser."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

from lxml import etree

from insider_contract import MAX_RAW_XML_BYTES
from pipeline import validate_sec_url
from security_identity import normalize_sec_cik


MAX_INDEX_HTML_BYTES = 1_000_000
MAX_INDEX_HTML_ELEMENTS = 100_000
MAX_INDEX_TABLE_ROWS = 10_000
MAX_INDEX_FIELD_CHARS = 4_096
_FORM_TYPES = frozenset({"3", "3/A", "4", "4/A", "5", "5/A"})
_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_FILENAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\.xml")
_ARCHIVE_PATH_SEGMENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}")
_ACCEPTED_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")
_SEC_LEGACY_HTML_DOCTYPE = (
    b'<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" '
    b'"http://www.w3.org/TR/html4/loose.dtd">'
)


class InsiderIndexParseError(ValueError):
    """Raised when a filing-index page cannot be safely bound to a filing."""


def _text(element: etree._Element) -> str:
    text = " ".join(fragment.strip() for fragment in element.itertext() if fragment.strip())
    if len(text) > MAX_INDEX_FIELD_CHARS:
        raise InsiderIndexParseError("filing-index field exceeds size limit")
    return text


def _canonical_cik(value: object, label: str) -> str:
    if type(value) is not str:
        raise InsiderIndexParseError(f"{label} CIK is invalid")
    try:
        normalized = normalize_sec_cik(value)
    except ValueError as error:
        raise InsiderIndexParseError(f"{label} CIK is invalid") from error
    if normalized == "0000000000":
        raise InsiderIndexParseError(f"{label} CIK is invalid")
    return normalized


def _canonical_accession(value: object) -> str:
    if type(value) is not str or not _ACCESSION_RE.fullmatch(value):
        raise InsiderIndexParseError("accession number is invalid")
    return value


def _canonical_filing_date(value: object, label: str) -> str:
    if type(value) is not str or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise InsiderIndexParseError(f"{label} is invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise InsiderIndexParseError(f"{label} is invalid") from error
    if parsed.isoformat() != value:
        raise InsiderIndexParseError(f"{label} is invalid")
    return value


def _strict_archive_url(url: object, accession: str, *, index: bool) -> tuple[str, str]:
    if type(url) is not str:
        raise InsiderIndexParseError("filing-index URL is invalid")
    try:
        validate_sec_url(url)
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise InsiderIndexParseError("filing-index URL is invalid") from error
    parts = parsed.path.split("/")
    compact = accession.replace("-", "")
    expected_last = (
        {f"{accession}-index.htm", f"{accession}-index.html"}
        if index
        else None
    )
    document_tail = parts[6:] if len(parts) >= 7 else []
    if (
        parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or (len(parts) != 7 if index else len(parts) < 7)
        or parts[1:4] != ["Archives", "edgar", "data"]
        or not parts[4].isdigit()
        or parts[5] != compact
        or (not index and not document_tail)
        or (not index and any(not _ARCHIVE_PATH_SEGMENT_RE.fullmatch(part) for part in document_tail))
        or (expected_last is not None and parts[6] not in expected_last)
    ):
        raise InsiderIndexParseError("filing-index URL is invalid")
    return url, _canonical_cik(parts[4], "archive")


def _bounded_html_scan(index_html_bytes: bytes) -> None:
    """Bound all parser-relevant structure and text before building a DOM."""

    parser = etree.HTMLPullParser(
        events=("start", "end"), recover=False, no_network=True, huge_tree=False
    )
    element_count = 0
    official_table_depths: list[int] = []
    field_depths: list[int] = []
    depth = 0
    row_count = 0

    def clear(element: etree._Element) -> None:
        parent = element.getparent()
        element.clear()
        if parent is not None:
            while element.getprevious() is not None:
                del parent[0]

    def process(event: str, element: etree._Element) -> None:
        nonlocal depth, element_count, row_count
        tag = element.tag if isinstance(element.tag, str) else ""
        if event == "start":
            depth += 1
            element_count += 1
            if element_count > MAX_INDEX_HTML_ELEMENTS:
                raise InsiderIndexParseError("filing-index HTML contains too many elements")
            classes = set((element.get("class") or "").split())
            if tag == "table" and _official_document_table(element):
                official_table_depths.append(depth)
            if tag == "tr" and official_table_depths:
                row_count += 1
                if row_count > MAX_INDEX_TABLE_ROWS + 1:  # header plus data rows
                    raise InsiderIndexParseError("filing-index table exceeds row limit")
            if (
                element.get("id") == "formName"
                or bool({"infoHead", "info"} & classes)
                or tag in {"strong", "b"}
                or (official_table_depths and tag in {"th", "td"})
            ):
                field_depths.append(depth)
            return

        if field_depths and field_depths[-1] == depth:
            if len(" ".join(fragment.strip() for fragment in element.itertext() if fragment.strip())) > MAX_INDEX_FIELD_CHARS:
                raise InsiderIndexParseError("filing-index field exceeds size limit")
            field_depths.pop()
        if tag == "table" and official_table_depths and official_table_depths[-1] == depth:
            official_table_depths.pop()
        if not field_depths:
            clear(element)
        depth -= 1

    try:
        for offset in range(0, len(index_html_bytes), 8192):
            parser.feed(index_html_bytes[offset : offset + 8192])
            for event, element in parser.read_events():
                process(event, element)
        parser.close()
        for event, element in parser.read_events():
            process(event, element)
    except etree.XMLSyntaxError as error:
        raise InsiderIndexParseError("filing-index HTML is malformed") from error


def _reject_unsafe_declarations(index_html_bytes: bytes) -> None:
    """Allow only one plain HTML5 doctype outside well-formed comments."""

    position = 0
    doctype_count = 0
    while (marker := index_html_bytes.find(b"<!", position)) >= 0:
        if index_html_bytes.startswith(b"<!--", marker):
            comment_end = index_html_bytes.find(b"-->", marker + 4)
            if comment_end < 0:
                raise InsiderIndexParseError("filing-index DTDs and entities are disabled")
            position = comment_end + 3
            continue
        doctype = re.match(br"<!DOCTYPE[ \t\r\n]+html[ \t\r\n]*>", index_html_bytes[marker:], re.I)
        if index_html_bytes.startswith(_SEC_LEGACY_HTML_DOCTYPE, marker):
            doctype_end = marker + len(_SEC_LEGACY_HTML_DOCTYPE)
        elif doctype is not None:
            doctype_end = marker + doctype.end()
        else:
            raise InsiderIndexParseError("filing-index DTDs and entities are disabled")
        doctype_count += 1
        if doctype_count > 1:
            raise InsiderIndexParseError("filing-index DTDs and entities are disabled")
        position = doctype_end


def _parse_html(index_html_bytes: bytes) -> etree._Element:
    if type(index_html_bytes) is not bytes:
        raise TypeError("filing-index HTML must be bytes")
    if not index_html_bytes or len(index_html_bytes) > MAX_INDEX_HTML_BYTES:
        raise InsiderIndexParseError("filing-index HTML exceeds size limit")
    _reject_unsafe_declarations(index_html_bytes)
    _bounded_html_scan(index_html_bytes)
    parser = etree.HTMLParser(
        recover=False,
        no_network=True,
        remove_blank_text=False,
        huge_tree=False,
        remove_comments=False,
    )
    try:
        root = etree.fromstring(index_html_bytes, parser)
    except etree.XMLSyntaxError as error:
        raise InsiderIndexParseError("filing-index HTML is malformed") from error
    if root is None:
        raise InsiderIndexParseError("filing-index HTML is malformed")
    return root


def _one_labeled_value(root: etree._Element, label: str) -> str:
    matches: list[str] = []
    for node in root.xpath("//*[@class='infoHead'] | //*[(text() = 'Filing Date' or text() = 'Accepted')]"):
        direct_text = (node.text or "").strip().rstrip(":")
        if direct_text != label:
            continue
        classes = set((node.get("class") or "").split())
        if "infoHead" in classes:
            sibling = node.getnext()
            if sibling is None or "info" not in set((sibling.get("class") or "").split()):
                raise InsiderIndexParseError(f"filing-index {label} is malformed")
            value = _text(sibling)
        elif (node.text or "").strip().rstrip(":") == label:
            values = node.xpath("./strong|./b")
            if len(values) != 1:
                raise InsiderIndexParseError(f"filing-index {label} is malformed")
            value = _text(values[0])
        else:
            continue
        if value:
            matches.append(value)
    if len(matches) != 1:
        raise InsiderIndexParseError(f"filing-index {label} is missing or ambiguous")
    return matches[0]


def _form_type(root: etree._Element) -> str:
    form_nodes = root.xpath("//*[@id='formName']")
    if len(form_nodes) != 1:
        raise InsiderIndexParseError("filing-index form type is missing or ambiguous")
    form_name = _text(form_nodes[0])
    leading_declaration = re.match(
        r"^Form\s+([0-9][A-Z0-9/-]*)\b",
        form_name,
        re.I,
    )
    declarations = re.findall(
        r"\bForm\s+([0-9][A-Z0-9/-]*)\b",
        form_name,
        re.I,
    )
    if (
        leading_declaration is None
        or len(declarations) != 1
        or leading_declaration.group(1).upper() not in _FORM_TYPES
    ):
        raise InsiderIndexParseError("filing-index form type is missing or ambiguous")
    return leading_declaration.group(1).upper()


def _accepted_at(value: str) -> str:
    if not _ACCEPTED_RE.fullmatch(value):
        raise InsiderIndexParseError("filing-index Accepted timestamp is invalid")
    try:
        naive = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        zone = ZoneInfo("America/New_York")
        first = naive.replace(tzinfo=zone, fold=0)
        second = naive.replace(tzinfo=zone, fold=1)
        if (
            first.utcoffset() != second.utcoffset()
            or first.astimezone(ZoneInfo("UTC")).astimezone(zone).replace(tzinfo=None)
            != naive
        ):
            raise ValueError
        eastern = first
    except ValueError as error:
        raise InsiderIndexParseError("filing-index Accepted timestamp is invalid") from error
    return eastern.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def _official_document_table(table: etree._Element) -> bool:
    classes = set((table.get("class") or "").split())
    summary = " ".join((table.get("summary") or "").split()).casefold()
    return "tableFile" in classes and summary == "document format files"


def _ownership_row(
    root: etree._Element,
    form_type: str,
    *,
    accession: str,
    related_archive_ciks: frozenset[str],
) -> tuple[str, str, str, str]:
    candidates: list[tuple[str, str, str, str]] = []
    row_count = 0
    official_tables = [table for table in root.xpath("//table") if _official_document_table(table)]
    if len(official_tables) != 1:
        raise InsiderIndexParseError("filing-index requires exactly one official Document Format Files table")
    for table in official_tables:
        header_cells = table.xpath("./tr[1]/*")
        if len(header_cells) != 5 or any(cell.tag != "th" for cell in header_cells):
            raise InsiderIndexParseError("filing-index document table headers are invalid")
        headers = [_text(cell).casefold() for cell in header_cells]
        if headers != ["seq", "description", "document", "type", "size"]:
            raise InsiderIndexParseError("filing-index document table headers are invalid")
        for row in table.xpath("./tr[position()>1]"):
            row_count += 1
            if row_count > MAX_INDEX_TABLE_ROWS:
                raise InsiderIndexParseError("filing-index table exceeds row limit")
            cells = row.xpath("./td")
            if len(cells) != len(headers):
                raise InsiderIndexParseError("filing-index document row is malformed")
            values = dict(zip(headers, (_text(cell) for cell in cells), strict=True))
            sequence = values["seq"]
            document_type = values["type"]
            links = cells[headers.index("document")].xpath(".//a[@href]")
            if document_type != form_type:
                continue
            if (
                not re.fullmatch(r"[1-9][0-9]*", sequence)
                or sequence != "1"
                or len(links) != 1
            ):
                raise InsiderIndexParseError("filing-index ownership row is invalid")
            href = links[0].get("href")
            link_filename = _text(links[0])
            if href is None:
                raise InsiderIndexParseError("filing-index document href is unsafe")
            try:
                parsed_href = urlsplit(href)
            except ValueError as error:
                raise InsiderIndexParseError("filing-index document href is unsafe") from error
            filename = parsed_href.path.rsplit("/", 1)[-1]
            root_relative_archive = (
                not parsed_href.scheme
                and not parsed_href.netloc
                and parsed_href.path.startswith("/Archives/edgar/data/")
            )
            absolute_archive = (
                parsed_href.scheme == "https"
                and parsed_href.hostname in {"www.sec.gov", "sec.gov"}
                and parsed_href.path.startswith("/Archives/edgar/data/")
            )
            relative_archive = (
                not parsed_href.scheme
                and not parsed_href.netloc
                and not parsed_href.path.startswith("/")
            )
            path_parts = parsed_href.path.split("/")
            if parsed_href.path.startswith("/"):
                path_parts = path_parts[1:]
            rendered_archive_cik: str | None = None
            if (
                root_relative_archive
                and len(path_parts) == 7
                and path_parts[:3] == ["Archives", "edgar", "data"]
                and path_parts[4] == accession.replace("-", "")
                and re.fullmatch(r"xslF345X[0-9]{2}", path_parts[5])
                and path_parts[6] == filename
            ):
                try:
                    rendered_archive_cik = _canonical_cik(
                        path_parts[3], "rendered document archive"
                    )
                except InsiderIndexParseError:
                    rendered_archive_cik = None
            if (
                rendered_archive_cik in related_archive_ciks
                and sequence == "1"
                and values["description"] in {form_type, "PRIMARY DOCUMENT"}
                and values["size"] == ""
                and _FILENAME_RE.fullmatch(filename)
                and link_filename == f"{filename.removesuffix('.xml')}.html"
                and not parsed_href.query
                and not parsed_href.fragment
            ):
                continue
            if (
                not _FILENAME_RE.fullmatch(filename)
                or link_filename != filename
                or parsed_href.query
                or parsed_href.fragment
                or parsed_href.username is not None
                or parsed_href.password is not None
                or not (relative_archive or root_relative_archive or absolute_archive)
                or not path_parts
                or any(not _ARCHIVE_PATH_SEGMENT_RE.fullmatch(part) for part in path_parts)
            ):
                raise InsiderIndexParseError("filing-index document href is unsafe")
            candidates.append((sequence, document_type, href, filename))
    if len(candidates) != 1:
        raise InsiderIndexParseError("filing-index ownership document is missing or ambiguous")
    return candidates[0]


def parse_insider_filing_index(
    index_html_bytes: bytes,
    *,
    index_url: str,
    accession_number: str,
    issuer_cik: str,
    reporting_owner_ciks: object,
) -> dict[str, object]:
    """Parse one offline SEC index page into deterministic source metadata fields."""

    accession = _canonical_accession(accession_number)
    issuer = _canonical_cik(issuer_cik, "issuer")
    if type(reporting_owner_ciks) not in (list, tuple):
        raise InsiderIndexParseError("reporting owner CIKs are invalid")
    owners = [_canonical_cik(value, "reporting owner") for value in reporting_owner_ciks]
    if len(set(owners)) != len(owners):
        raise InsiderIndexParseError("reporting owner CIKs are ambiguous")
    canonical_index_url, index_archive_cik = _strict_archive_url(index_url, accession, index=True)
    if index_archive_cik != issuer:
        raise InsiderIndexParseError("filing-index archive CIK does not match issuer")
    root = _parse_html(index_html_bytes)
    form_type = _form_type(root)
    filing_date = _canonical_filing_date(_one_labeled_value(root, "Filing Date"), "filing-index Filing Date")
    accepted = _accepted_at(_one_labeled_value(root, "Accepted"))
    sequence, document_type, href, filename = _ownership_row(
        root,
        form_type,
        accession=accession,
        related_archive_ciks=frozenset({issuer, *owners}),
    )
    document_url = urljoin(canonical_index_url, href)
    canonical_document_url, document_archive_cik = _strict_archive_url(
        document_url, accession, index=False
    )
    if document_archive_cik == issuer:
        role = "issuer"
    elif document_archive_cik in owners:
        role = "reporting_owner"
    else:
        raise InsiderIndexParseError("document archive CIK is unrelated")
    return {
        "accession_number": accession,
        "form_type": form_type,
        "filing_date": filing_date,
        "accepted_at": accepted,
        "issuer_cik": issuer,
        "reporting_owner_ciks": owners,
        "index_url": canonical_index_url,
        "index_archive_cik": index_archive_cik,
        "document_url": canonical_document_url,
        "document_archive_cik": document_archive_cik,
        "document_archive_cik_role": role,
        "document_sequence": sequence,
        "document_type": document_type,
        "document_filename": filename,
    }


INSIDER_SOURCE_METADATA_VERSION = 1
_SOURCE_ROOT_KEYS = frozenset({
    "contract_version", "accession_number", "form_type", "filing_date", "accepted_at",
    "issuer_cik", "reporting_owner_ciks", "index", "document",
})
_INDEX_KEYS = frozenset({"url", "archive_cik", "sha256", "byte_count"})
_DOCUMENT_KEYS = frozenset({
    "url", "archive_cik", "archive_cik_role", "sequence", "document_type",
    "filename", "sha256", "byte_count",
})
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def validate_insider_source_metadata(metadata: object) -> dict[str, object]:
    """Validate the exact deterministic source-evidence v1 shape."""

    if not isinstance(metadata, dict) or set(metadata) != _SOURCE_ROOT_KEYS:
        raise InsiderIndexParseError("source metadata root keys are invalid")
    if (
        type(metadata.get("contract_version")) is not int
        or metadata.get("contract_version") != INSIDER_SOURCE_METADATA_VERSION
    ):
        raise InsiderIndexParseError("source metadata contract version is invalid")
    accession = _canonical_accession(metadata.get("accession_number"))
    form_type = metadata.get("form_type")
    if type(form_type) is not str or form_type not in _FORM_TYPES:
        raise InsiderIndexParseError("source metadata form type is invalid")
    filing_date = _canonical_filing_date(metadata.get("filing_date"), "source metadata filing date")
    accepted_at = metadata.get("accepted_at")
    if type(accepted_at) is not str or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", accepted_at
    ):
        raise InsiderIndexParseError("source metadata accepted timestamp is invalid")
    try:
        datetime.strptime(accepted_at, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise InsiderIndexParseError("source metadata accepted timestamp is invalid") from error
    issuer = _canonical_cik(metadata.get("issuer_cik"), "issuer")
    owners = metadata.get("reporting_owner_ciks")
    if type(owners) is not list:
        raise InsiderIndexParseError("source metadata owner CIKs are invalid")
    canonical_owners = [_canonical_cik(value, "reporting owner") for value in owners]
    if canonical_owners != owners or len(set(canonical_owners)) != len(canonical_owners):
        raise InsiderIndexParseError("source metadata owner CIKs are invalid")
    index = metadata.get("index")
    document = metadata.get("document")
    if not isinstance(index, dict) or set(index) != _INDEX_KEYS:
        raise InsiderIndexParseError("source metadata index keys are invalid")
    if not isinstance(document, dict) or set(document) != _DOCUMENT_KEYS:
        raise InsiderIndexParseError("source metadata document keys are invalid")
    index_url, index_cik = _strict_archive_url(index.get("url"), accession, index=True)
    if index_cik != issuer or index.get("archive_cik") != index_cik:
        raise InsiderIndexParseError("source metadata index archive CIK is invalid")
    document_url, document_cik = _strict_archive_url(document.get("url"), accession, index=False)
    if document.get("archive_cik") != document_cik:
        raise InsiderIndexParseError("source metadata document archive CIK is invalid")
    role = document.get("archive_cik_role")
    expected_role = "issuer" if document_cik == issuer else "reporting_owner"
    if role != expected_role or (role == "reporting_owner" and document_cik not in canonical_owners):
        raise InsiderIndexParseError("source metadata document archive CIK role is invalid")
    if document.get("sequence") != "1":
        raise InsiderIndexParseError("source metadata document sequence is invalid")
    if document.get("document_type") != form_type:
        raise InsiderIndexParseError("source metadata document type is invalid")
    if type(document.get("filename")) is not str or not _FILENAME_RE.fullmatch(document["filename"]):
        raise InsiderIndexParseError("source metadata document filename is invalid")
    if urlsplit(document_url).path.rsplit("/", 1)[-1] != document["filename"]:
        raise InsiderIndexParseError("source metadata document filename is invalid")
    for artifact, maximum in ((index, MAX_INDEX_HTML_BYTES), (document, MAX_RAW_XML_BYTES)):
        sha256 = artifact.get("sha256")
        byte_count = artifact.get("byte_count")
        if type(sha256) is not str or not _SHA256_RE.fullmatch(sha256):
            raise InsiderIndexParseError("source metadata SHA-256 is invalid")
        if type(byte_count) is not int or not 1 <= byte_count <= maximum:
            raise InsiderIndexParseError("source metadata byte count is invalid")
    return {
        "contract_version": INSIDER_SOURCE_METADATA_VERSION,
        "accession_number": accession,
        "form_type": form_type,
        "filing_date": filing_date,
        "accepted_at": accepted_at,
        "issuer_cik": issuer,
        "reporting_owner_ciks": canonical_owners,
        "index": dict(index, url=index_url, archive_cik=index_cik),
        "document": dict(document, url=document_url, archive_cik=document_cik),
    }


def build_insider_source_metadata(
    index_metadata: object, index_html_bytes: bytes, raw_xml_bytes: bytes
) -> dict[str, object]:
    """Build deterministic v1 source metadata after independent parsing."""

    if type(index_metadata) is not dict or type(index_html_bytes) is not bytes or type(raw_xml_bytes) is not bytes:
        raise TypeError("source metadata inputs must be bytes and parsed metadata")
    required = {
        "accession_number", "form_type", "filing_date", "accepted_at", "issuer_cik",
        "reporting_owner_ciks", "index_url", "index_archive_cik", "document_url",
        "document_archive_cik", "document_archive_cik_role", "document_sequence",
        "document_type", "document_filename",
    }
    if set(index_metadata) != required:
        raise InsiderIndexParseError("filing-index metadata shape is invalid")
    return validate_insider_source_metadata({
        "contract_version": INSIDER_SOURCE_METADATA_VERSION,
        "accession_number": index_metadata["accession_number"],
        "form_type": index_metadata["form_type"],
        "filing_date": index_metadata["filing_date"],
        "accepted_at": index_metadata["accepted_at"],
        "issuer_cik": index_metadata["issuer_cik"],
        "reporting_owner_ciks": index_metadata["reporting_owner_ciks"],
        "index": {
            "url": index_metadata["index_url"], "archive_cik": index_metadata["index_archive_cik"],
            "sha256": hashlib.sha256(index_html_bytes).hexdigest(), "byte_count": len(index_html_bytes),
        },
        "document": {
            "url": index_metadata["document_url"], "archive_cik": index_metadata["document_archive_cik"],
            "archive_cik_role": index_metadata["document_archive_cik_role"], "sequence": index_metadata["document_sequence"],
            "document_type": index_metadata["document_type"], "filename": index_metadata["document_filename"],
            "sha256": hashlib.sha256(raw_xml_bytes).hexdigest(), "byte_count": len(raw_xml_bytes),
        },
    })


def canonical_source_metadata_json_bytes(metadata: object) -> bytes:
    validated = validate_insider_source_metadata(metadata)
    return (json.dumps(validated, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")

__all__ = [
    "INSIDER_SOURCE_METADATA_VERSION",
    "MAX_INDEX_HTML_BYTES",
    "MAX_INDEX_HTML_ELEMENTS",
    "MAX_INDEX_TABLE_ROWS",
    "MAX_INDEX_FIELD_CHARS",
    "InsiderIndexParseError",
    "build_insider_source_metadata",
    "canonical_source_metadata_json_bytes",
    "parse_insider_filing_index",
    "validate_insider_source_metadata",
]
