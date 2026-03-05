r"""
Download Louisiana Supreme Court opinions from the official Louisiana Supreme Court
website and save local PDFs plus extracted text/metadata for indexing.

Source archive: https://www.lasc.org/CourtActions/<year>

Examples:
  python scripts\download_louisiana_case_law.py
  python scripts\download_louisiana_case_law.py --years 2024-2026
  python scripts\download_louisiana_case_law.py --years 2025 --workers 8
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import json
import os
import re
import sys
import threading
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag


BASE = "https://www.lasc.org"
CATEGORY_KEY = "louisiana-supreme-court-decisions"
CATEGORY_NAME = "Louisiana Supreme Court Decisions"
MIN_ARCHIVE_YEAR = 2000
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

_INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
_WS = re.compile(r"\s+")
_LEAD_CITATION_RE = re.compile(
    r"^(?P<citation>\d{4}\s*-\s*[A-Z/]{1,6}\s*-\s*[A-Z0-9 ]*\d{1,5})\s+(?P<title>.+)$",
    re.IGNORECASE,
)
_PDF_CHAR_REPLACEMENTS = {
    "\u00a0": " ",
    "\u00ad": "",
    "\u2010": "-",
    "\u2011": "-",
    "\u2012": "-",
    "\u2013": "-",
    "\u2014": "-",
    "\u2015": "-",
    "\u2018": "'",
    "\u2019": "'",
    "\u201a": "'",
    "\u201b": "'",
    "\u201c": '"',
    "\u201d": '"',
    "\u201e": '"',
    "\u201f": '"',
    "\u2022": "*",
    "\u2023": "*",
    "\u2026": "...",
    "\u2212": "-",
    "\u25aa": "*",
    "\u25ab": "*",
    "\u25a0": "*",
    "\uf0b7": "*",
    "\uf02a": "*",
}
_CAPTION_PARTY_TOKENS = (
    "APPELLANT",
    "APPELLEE",
    "CLAIMANT",
    "C/W",
    "DEFENDANT",
    "IN RE",
    "JANE DOE",
    "JOHN DOE",
    "PETITIONER",
    "PLAINTIFF",
    "RELATOR",
    "RESPONDENT",
    "STATE OF",
    "VERSUS",
    "VS.",
)
_CAPTION_CONNECTOR_LINES = {"AND", "OR", "VERSUS", "VS", "VS."}


@dataclass(frozen=True)
class OpinionEntry:
    order: int
    doc_id: str
    citation: str
    title: str
    url: str
    release_url: str
    pdf_url: str
    release_code: str
    release_date: str
    author: str
    disposition: str
    parish: str
    notes: str


@dataclass(frozen=True)
class Bundle:
    category_key: str
    category_name: str
    bundle_name: str
    source_toc_url: str
    entries: list[OpinionEntry]


class _Logger:
    def __init__(self, *, verbose: bool) -> None:
        self._verbose = verbose
        self._lock = threading.Lock()

    def _log(self, label: str, msg: str, *, err: bool = False) -> None:
        stream = sys.stderr if err else sys.stdout
        with self._lock:
            print(f"[{time.strftime('%H:%M:%S')}] {label}{msg}", file=stream, flush=True)

    def info(self, msg: str) -> None:
        self._log("", msg)

    def debug(self, msg: str) -> None:
        if self._verbose:
            self._log("[debug] ", msg)

    def warn(self, msg: str) -> None:
        self._log("[warn] ", msg, err=True)


def safe_name(value: str, *, max_len: int = 140) -> str:
    value = _WS.sub(" ", value.strip())
    value = _INVALID_PATH_CHARS.sub("_", value)
    value = value.rstrip(". ")
    if len(value) > max_len:
        value = value[: max_len - 1].rstrip()
    return value or "_"


def _atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding=encoding, newline="\n")
    os.replace(tmp, path)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)


def _write_json(path: Path, payload: object) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _normalize_ws(value: str) -> str:
    return _WS.sub(" ", (value or "").replace("\xa0", " ")).strip()


def _new_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_REQUEST_HEADERS)
    return session


def _year_release_url(year: int) -> str:
    return f"{BASE}/CourtActions/{year}"


def _parse_years(raw: str) -> list[int]:
    text = (raw or "").strip().lower()
    current_year = datetime.now().year
    if text in {"", "all"}:
        return list(range(MIN_ARCHIVE_YEAR, current_year + 1))

    out: set[int] = set()
    for part in text.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            lhs, rhs = token.split("-", 1)
            start = int(lhs)
            end = int(rhs)
            if start > end:
                start, end = end, start
            out.update(range(start, end + 1))
        else:
            out.add(int(token))

    years = sorted(y for y in out if MIN_ARCHIVE_YEAR <= y <= current_year)
    if not years:
        raise SystemExit(
            f"No valid years requested. Official archive coverage starts at {MIN_ARCHIVE_YEAR} and ends at {current_year}."
        )
    return years


def _pubdate_to_iso(raw: str) -> str:
    value = _normalize_ws(raw)
    if not value:
        return ""
    for fmt in ("%m-%d-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return value


def _citation_from_anchor_text(raw_text: str) -> tuple[str, str]:
    raw_text = _normalize_ws(raw_text)
    if not raw_text:
        return "", ""
    match = _LEAD_CITATION_RE.match(raw_text)
    if not match:
        return "", raw_text
    citation = _normalize_ws(match.group("citation"))
    citation = re.sub(r"\s*-\s*", "-", citation)
    citation = re.sub(r"(?<=-)\s+(?=\d)", "", citation)
    return citation.upper(), _normalize_ws(match.group("title"))


def _opinion_doc_id(release_code: str, pdf_url: str) -> str:
    stem = Path(urlparse(pdf_url).path).stem
    return safe_name(f"{release_code}__{stem}", max_len=180)


def _normalize_pdf_href(href: str) -> str:
    value = _normalize_ws(href)
    if not value:
        return ""
    value = re.sub(r"^htto://", "https://", value, flags=re.IGNORECASE)
    value = re.sub(r"^http://", "https://", value, flags=re.IGNORECASE)
    return urljoin(BASE, value)


def _paragraph_text(tag: Tag) -> str:
    return _normalize_ws(tag.get_text(" ", strip=True))


def _author_heading_text(text: str) -> str:
    normalized = _normalize_ws(text)
    if not normalized:
        return ""
    if re.match(r"^(BY\s+.+|PER CURIAM):$", normalized, re.IGNORECASE):
        return normalized.rstrip(":")
    return ""


def _parse_release_entries(
    *,
    release_url: str,
    release_code: str,
    release_date: str,
    inner_html: str,
) -> list[dict[str, str]]:
    outer = BeautifulSoup(inner_html, "html.parser")
    main = outer.find(id="mainbodycontent") or outer
    nrbody = main.find(class_="nrbody") or main

    entries: list[dict[str, str]] = []
    current_author = ""
    last_entry: Optional[dict[str, str]] = None

    for child in nrbody.find_all(["h1", "h2", "h3", "p"]):
        text = _paragraph_text(child)
        if not text:
            continue

        author_heading = _author_heading_text(text)
        if author_heading:
            current_author = author_heading
            continue

        candidate_link: Optional[Tag] = None
        candidate_citation = ""
        candidate_title = ""
        for pdf_link in child.find_all("a", href=True):
            href = pdf_link["href"]
            if not href.lower().endswith(".pdf"):
                continue
            anchor_text = _normalize_ws(pdf_link.get_text(" ", strip=True))
            citation, title = _citation_from_anchor_text(anchor_text)
            if citation:
                candidate_link = pdf_link
                candidate_citation = citation
                candidate_title = title
                break

        if candidate_link is None:
            if last_entry is not None:
                existing = last_entry.get("notes", "")
                last_entry["notes"] = f"{existing}\n{text}".strip() if existing else text
            continue

        anchor_text = _normalize_ws(candidate_link.get_text(" ", strip=True))
        citation = candidate_citation
        title = candidate_title
        remainder = text
        if anchor_text and remainder.startswith(anchor_text):
            remainder = remainder[len(anchor_text) :].strip()
        parish = ""
        disposition = remainder
        parish_match = re.match(r"^\((?P<parish>[^)]*?)\)\s*(?P<rest>.*)$", remainder)
        if parish_match:
            parish = _normalize_ws(parish_match.group("parish"))
            disposition = _normalize_ws(parish_match.group("rest"))

        pdf_url = _normalize_pdf_href(candidate_link["href"])
        last_entry = {
            "doc_id": _opinion_doc_id(release_code, pdf_url),
            "citation": citation,
            "title": title,
            "release_url": release_url,
            "pdf_url": pdf_url,
            "release_code": release_code,
            "release_date": release_date,
            "author": current_author,
            "disposition": disposition,
            "parish": parish,
            "notes": "",
        }
        entries.append(last_entry)

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        key = entry["pdf_url"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def _crawl_year(
    session: requests.Session,
    *,
    year: int,
    timeout_s: float,
    logger: _Logger,
    max_cases: Optional[int],
) -> Bundle:
    court_actions_url = _year_release_url(year)
    logger.info(f"[crawl] {year}: {court_actions_url}")
    html_text = session.get(court_actions_url, timeout=timeout_s)
    html_text.raise_for_status()
    soup = BeautifulSoup(html_text.text, "html.parser")

    release_urls: list[str] = []
    seen_release_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("/Opinions?p="):
            continue
        url = urljoin(BASE, href)
        if url in seen_release_urls:
            continue
        seen_release_urls.add(url)
        release_urls.append(url)

    logger.info(f"[crawl] {year}: releases={len(release_urls)}")
    raw_entries: list[dict[str, str]] = []
    for release_url in release_urls:
        release_res = session.get(release_url, timeout=timeout_s)
        release_res.raise_for_status()
        release_soup = BeautifulSoup(release_res.text, "html.parser")
        pubdate_meta = release_soup.find("meta", attrs={"name": "pubdate"})
        release_date = _pubdate_to_iso(pubdate_meta.get("content", "") if pubdate_meta else "")
        textarea = release_soup.find("textarea", id="PostContent")
        if textarea is not None:
            inner_html = html.unescape(textarea.text)
        else:
            mainbody = release_soup.find(id="mainbodycontent")
            if mainbody is None:
                logger.warn(f"[crawl] missing PostContent/mainbodycontent on {release_url}")
                continue
            inner_html = mainbody.decode()
        release_code = (parse_qs(urlparse(release_url).query).get("p") or [""])[0]
        parsed = _parse_release_entries(
            release_url=release_url,
            release_code=release_code,
            release_date=release_date,
            inner_html=inner_html,
        )
        raw_entries.extend(parsed)
        if max_cases is not None and len(raw_entries) >= max_cases:
            raw_entries = raw_entries[:max_cases]
            break

    entries = [
        OpinionEntry(
            order=index,
            doc_id=entry["doc_id"],
            citation=entry["citation"],
            title=entry["title"],
            url=entry["release_url"],
            release_url=entry["release_url"],
            pdf_url=entry["pdf_url"],
            release_code=entry["release_code"],
            release_date=entry["release_date"],
            author=entry["author"],
            disposition=entry["disposition"],
            parish=entry["parish"],
            notes=entry["notes"],
        )
        for index, entry in enumerate(raw_entries, start=1)
    ]
    logger.info(f"[crawl] {year}: cases={len(entries)}")
    return Bundle(
        category_key=CATEGORY_KEY,
        category_name=CATEGORY_NAME,
        bundle_name=f"{year} Decisions",
        source_toc_url=court_actions_url,
        entries=entries,
    )


def _extract_text_pymupdf(pdf_path: Path) -> str:
    import fitz  # PyMuPDF

    with fitz.open(str(pdf_path)) as pdf_doc:
        pages = [page.get_text("text") for page in pdf_doc]
    return _normalize_extracted_pages(pages)


def _extract_text_pypdf(pdf_path: Path) -> str:
    from pypdf import PdfReader

    reader = PdfReader(str(pdf_path), strict=False)
    pages = [(page.extract_text() or "") for page in reader.pages]
    return _normalize_extracted_pages(pages)


def _extract_pdf_text(pdf_path: Path, *, backend: str, logger: _Logger) -> tuple[str, str]:
    if backend not in {"auto", "pymupdf", "pypdf"}:
        raise ValueError(f"Unsupported backend: {backend}")

    if backend in {"auto", "pymupdf"}:
        try:
            return _extract_text_pymupdf(pdf_path), "pymupdf"
        except Exception as exc:
            if backend == "pymupdf":
                raise
            logger.debug(f"PyMuPDF extraction unavailable for {pdf_path.name}: {exc}")

    return _extract_text_pypdf(pdf_path), "pypdf"


def _sanitize_pdf_characters(text: str) -> str:
    text = unicodedata.normalize("NFKC", text.replace("\r\n", "\n").replace("\r", "\n"))
    out: list[str] = []
    for ch in text:
        if ch in _PDF_CHAR_REPLACEMENTS:
            out.append(_PDF_CHAR_REPLACEMENTS[ch])
            continue
        category = unicodedata.category(ch)
        if category in {"Cf", "Cs"}:
            continue
        if category == "Co":
            out.append(" ")
            continue
        out.append(ch)
    return "".join(out)


def _line_is_caption_fragment(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    upper = stripped.upper()
    if upper in _CAPTION_CONNECTOR_LINES:
        return True
    if stripped != upper:
        return False
    if any(token in upper for token in _CAPTION_PARTY_TOKENS):
        return True
    return bool(
        re.search(
            r"\b(APPELLANT|APPELLEE|CLAIMANT|DEFENDANT|PETITIONER|PLAINTIFF|RELATOR|RESPONDENT)\s*$",
            upper,
        )
    )


def _line_is_caption_party_number(line: str, *, prev_line: str, next_line: str) -> bool:
    stripped = line.strip()
    if not stripped.isdigit():
        return False
    if len(stripped) > 2:
        return False

    prev = prev_line.strip()
    nxt = next_line.strip()
    if not prev or not nxt:
        return False
    if prev != prev.upper() or nxt != nxt.upper():
        return False
    if _line_is_caption_fragment(prev) or _line_is_caption_fragment(nxt):
        return True
    return nxt in _CAPTION_CONNECTOR_LINES and bool(
        re.search(
            r"\b(APPELLANT|APPELLEE|CLAIMANT|DEFENDANT|PETITIONER|PLAINTIFF|RELATOR|RESPONDENT)\s*$",
            prev,
        )
    )


def _line_is_probable_page_number(
    line: str,
    *,
    prev_line: str,
    next_line: str,
    rank: int,
    total_nonempty: int,
) -> bool:
    stripped = line.strip()
    if not stripped.isdigit():
        return False
    if len(stripped) > 3:
        return False
    if _line_is_caption_party_number(line, prev_line=prev_line, next_line=next_line):
        return False
    return True


def _line_is_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped in {"SUPREME COURT OF LOUISIANA"}:
        return True
    if re.match(r"^No\.\s", stripped, re.IGNORECASE):
        return True
    if re.match(r"^(facts and procedural history|discussion|conclusion|decree|analysis)\b", stripped, re.IGNORECASE):
        return True
    if _line_is_caption_fragment(stripped):
        return False
    if stripped.isupper() and len(stripped) <= 140 and len(stripped.split()) <= 14:
        return True
    return False


def _line_is_footnote_start(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith("* "):
        return True
    return bool(re.match(r"^\*?[0-9]+\s{1,3}[A-Z(]", stripped))


def _paragraph_is_footnote(paragraph: str) -> bool:
    return _line_is_footnote_start(paragraph)


def _merge_wrapped_line(current: str, line: str) -> str:
    if not current:
        return line
    if current.endswith("-") and line and line[0].isalnum():
        return current + line
    if line and line[0] in ",.;:?!)]}":
        return current + line
    return f"{current} {line}"


def _normalize_page_to_paragraphs(page_text: str) -> list[str]:
    lines = [(_normalize_ws(line) if line.strip() else "") for line in _sanitize_pdf_characters(page_text).split("\n")]
    nonempty_indexes = [idx for idx, line in enumerate(lines) if line]
    rank_by_index = {idx: rank for rank, idx in enumerate(nonempty_indexes)}
    total_nonempty = len(nonempty_indexes)
    prev_nonempty: list[str] = []
    last_line = ""
    for line in lines:
        prev_nonempty.append(last_line)
        if line:
            last_line = line
    next_nonempty = [""] * len(lines)
    next_line = ""
    for idx in range(len(lines) - 1, -1, -1):
        next_nonempty[idx] = next_line
        if lines[idx]:
            next_line = lines[idx]

    paragraphs: list[str] = []
    current = ""
    current_kind = ""

    def flush_current() -> None:
        nonlocal current, current_kind
        if current:
            paragraphs.append(current)
            current = ""
            current_kind = ""

    for idx, line in enumerate(lines):
        if not line:
            flush_current()
            continue
        rank = rank_by_index.get(idx, 0)
        if _line_is_caption_party_number(line, prev_line=prev_nonempty[idx], next_line=next_nonempty[idx]):
            if current_kind == "caption":
                current = _merge_wrapped_line(current, line)
                continue
            if not current and paragraphs and _line_is_caption_fragment(paragraphs[-1]):
                current = paragraphs.pop()
            current = _merge_wrapped_line(current, line)
            current_kind = "caption"
            continue
        if _line_is_probable_page_number(
            line,
            prev_line=prev_nonempty[idx],
            next_line=next_nonempty[idx],
            rank=rank,
            total_nonempty=total_nonempty,
        ):
            continue
        if _line_is_caption_fragment(line):
            if current_kind == "caption" or _line_is_caption_fragment(current):
                current = _merge_wrapped_line(current, line)
                current_kind = "caption"
                continue
            if not current and paragraphs and _line_is_caption_fragment(paragraphs[-1]):
                current = paragraphs.pop()
                current = _merge_wrapped_line(current, line)
                current_kind = "caption"
                continue
        if _line_is_heading(line):
            flush_current()
            paragraphs.append(line)
            continue
        if _line_is_footnote_start(line):
            flush_current()
            current = line
            current_kind = "footnote"
            continue
        if current_kind == "footnote":
            current = _merge_wrapped_line(current, line)
            continue
        current = _merge_wrapped_line(current, line)

    flush_current()
    return [paragraph for paragraph in paragraphs if paragraph]


def _should_merge_page_break(prev_paragraph: str, next_paragraph: str) -> bool:
    prev = prev_paragraph.strip()
    nxt = next_paragraph.strip()
    if not prev or not nxt:
        return False
    if _line_is_heading(prev) or _line_is_heading(nxt):
        return False
    if prev.endswith("-"):
        return True
    if nxt[0].islower():
        return True
    if prev and prev[-1] not in ".!?;:" and nxt[0].isalnum():
        return True
    return False


def _normalize_extracted_pages(pages: list[str]) -> str:
    merged_paragraphs: list[str] = []
    for page_text in pages:
        paragraphs = _normalize_page_to_paragraphs(page_text)
        if not paragraphs:
            continue
        if merged_paragraphs:
            merge_idx = len(merged_paragraphs) - 1
            while merge_idx >= 0 and _paragraph_is_footnote(merged_paragraphs[merge_idx]):
                merge_idx -= 1
            if merge_idx >= 0 and _should_merge_page_break(merged_paragraphs[merge_idx], paragraphs[0]):
                merged_paragraphs[merge_idx] = _merge_wrapped_line(merged_paragraphs[merge_idx], paragraphs[0])
                paragraphs = paragraphs[1:]
        merged_paragraphs.extend(paragraphs)

    body = "\n\n".join(paragraph.strip() for paragraph in merged_paragraphs if paragraph.strip())
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def _text_to_html(text: str) -> str:
    blocks = [block.strip() for block in text.split("\n\n") if block.strip()]
    out: list[str] = []
    for block in blocks:
        escaped = html.escape(block).replace("\n", "<br />\n")
        out.append(f"<p>{escaped}</p>")
    return "\n".join(out)


def _build_doc_text(entry: OpinionEntry, *, pdf_text: str, notes: str) -> str:
    header_lines = [
        entry.citation,
        entry.title,
        "Court: Louisiana Supreme Court",
    ]
    if entry.release_date:
        header_lines.append(f"Release date: {entry.release_date}")
    if entry.release_code:
        header_lines.append(f"Release: {entry.release_code}")
    if entry.author:
        header_lines.append(f"Author: {entry.author}")
    if entry.parish:
        header_lines.append(f"Parish: {entry.parish}")
    if entry.disposition:
        header_lines.append(f"Disposition: {entry.disposition}")
    if notes:
        header_lines.append(f"Release notes: {notes}")
    header_lines.append(f"Official release page: {entry.release_url}")
    header_lines.append(f"Official PDF: {entry.pdf_url}")
    body = "\n".join(line for line in header_lines if line.strip())
    return f"{body}\n\n{pdf_text.strip()}".strip()


def _download_case(
    *,
    out_dir: Path,
    bundle_dir: Path,
    entry: OpinionEntry,
    timeout_s: float,
    backend: str,
    resume: bool,
    logger: _Logger,
) -> tuple[str, str]:
    sections_dir = bundle_dir / "sections"
    pdf_path = sections_dir / f"{entry.doc_id}.pdf"
    json_path = sections_dir / f"{entry.doc_id}.json"
    txt_path = sections_dir / f"{entry.doc_id}.txt"

    if resume and pdf_path.exists() and json_path.exists() and txt_path.exists():
        return "skipped", entry.doc_id

    if not pdf_path.exists() or pdf_path.stat().st_size <= 0:
        res = requests.get(entry.pdf_url, timeout=timeout_s, headers=_REQUEST_HEADERS)
        res.raise_for_status()
        _atomic_write_bytes(pdf_path, res.content)

    pdf_text, extraction_backend = _extract_pdf_text(pdf_path, backend=backend, logger=logger)
    notes = _normalize_ws(entry.notes)
    doc_text = _build_doc_text(entry, pdf_text=pdf_text, notes=notes)
    doc_html = _text_to_html(doc_text)

    local_file_rel = pdf_path.relative_to(out_dir).as_posix()
    payload = {
        "doc_id": entry.doc_id,
        "url": entry.url,
        "citation": entry.citation,
        "title": entry.title,
        "downloaded_at_epoch": int(time.time()),
        "court": "Louisiana Supreme Court",
        "release_url": entry.release_url,
        "pdf_url": entry.pdf_url,
        "release_code": entry.release_code,
        "release_date": entry.release_date,
        "author": entry.author,
        "parish": entry.parish,
        "disposition": entry.disposition,
        "notes": notes,
        "local_file": local_file_rel,
        "pdf_extract_backend": extraction_backend,
        "doc_html": doc_html,
        "doc_text": doc_text,
    }
    _write_json(json_path, payload)
    wrapper = f"{entry.citation}\n{entry.title}\n{entry.url}\n\n{doc_text}\n"
    _atomic_write_text(txt_path, wrapper)
    return "ok", entry.doc_id


def _download_bundle_sections(
    *,
    out_dir: Path,
    bundle: Bundle,
    bundle_dir: Path,
    timeout_s: float,
    backend: str,
    resume: bool,
    workers: int,
    logger: _Logger,
) -> dict[str, int]:
    stats = {"expected": len(bundle.entries), "ok": 0, "skipped": 0, "err": 0}
    if not bundle.entries:
        return stats

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [
            pool.submit(
                _download_case,
                out_dir=out_dir,
                bundle_dir=bundle_dir,
                entry=entry,
                timeout_s=timeout_s,
                backend=backend,
                resume=resume,
                logger=logger,
            )
            for entry in bundle.entries
        ]

        for future in concurrent.futures.as_completed(futures):
            try:
                status, doc_id = future.result()
                stats[status] += 1
                logger.debug(f"[download] {status} {doc_id}")
            except Exception as exc:
                stats["err"] += 1
                logger.warn(f"[download] failed: {exc}")

    return stats


def _prune_stale_section_files(*, bundle: Bundle, bundle_dir: Path, logger: _Logger) -> int:
    sections_dir = bundle_dir / "sections"
    if not sections_dir.exists():
        return 0

    valid_doc_ids = {entry.doc_id for entry in bundle.entries}
    removed = 0
    for path in sections_dir.iterdir():
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".json", ".txt", ".pdf"}:
            continue
        if path.stem in valid_doc_ids:
            continue
        try:
            path.unlink()
            removed += 1
        except OSError as exc:
            logger.warn(f"[prune] failed to remove stale file {path}: {exc}")
    if removed:
        logger.info(f"[prune] {bundle.bundle_name}: removed {removed} stale section file(s)")
    return removed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="out", help="Output directory (default: out)")
    parser.add_argument(
        "--years",
        default="all",
        help="Years or ranges to crawl (default: all official years, 2000-current). Example: 2024-2026,2020",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip cases that already have pdf/json/txt output (default: true)",
    )
    parser.add_argument("--workers", type=int, default=6, help="Concurrent PDF downloads/extractions (default: 6)")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds (default: 30)")
    parser.add_argument(
        "--pdf-backend",
        choices=["auto", "pymupdf", "pypdf"],
        default="auto",
        help="PDF text extraction backend (default: auto)",
    )
    parser.add_argument(
        "--max-cases-per-year",
        type=int,
        default=None,
        help="Limit cases per year for testing/debugging",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args(argv)

    logger = _Logger(verbose=bool(args.verbose))
    years = _parse_years(str(args.years))
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    category_dir = out_dir / safe_name(CATEGORY_NAME)
    category_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "config: "
        f"years={years[0]}-{years[-1]} count={len(years)} "
        f"resume={bool(args.resume)} workers={int(args.workers)} "
        f"timeout={float(args.timeout)}s backend={args.pdf_backend}"
    )

    session = _new_session()
    total_cases = 0
    total_ok = 0
    total_skipped = 0
    total_err = 0

    for year in years:
        bundle = _crawl_year(
            session,
            year=year,
            timeout_s=float(args.timeout),
            logger=logger,
            max_cases=args.max_cases_per_year,
        )
        bundle_dir = category_dir / safe_name(bundle.bundle_name)
        bundle_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            bundle_dir / "bundle.json",
            {
                "category_key": bundle.category_key,
                "category_name": bundle.category_name,
                "bundle_name": bundle.bundle_name,
                "source_toc_url": bundle.source_toc_url,
                "entries": [asdict(entry) for entry in bundle.entries],
            },
        )
        _prune_stale_section_files(bundle=bundle, bundle_dir=bundle_dir, logger=logger)

        logger.info(f"[download] {bundle.bundle_name}: docs={len(bundle.entries)}")
        stats = _download_bundle_sections(
            out_dir=out_dir,
            bundle=bundle,
            bundle_dir=bundle_dir,
            timeout_s=float(args.timeout),
            backend=str(args.pdf_backend),
            resume=bool(args.resume),
            workers=int(args.workers),
            logger=logger,
        )
        _write_json(
            bundle_dir / "bundle.downloaded.json",
            {
                "bundle_name": bundle.bundle_name,
                "docs": stats["expected"],
                "ok": stats["ok"],
                "skipped": stats["skipped"],
                "err": stats["err"],
                "completed_at_epoch": int(time.time()),
            },
        )

        total_cases += stats["expected"]
        total_ok += stats["ok"]
        total_skipped += stats["skipped"]
        total_err += stats["err"]
        logger.info(
            f"[download] {bundle.bundle_name}: ok={stats['ok']} skipped={stats['skipped']} err={stats['err']}"
        )

    logger.info(
        f"[done] cases={total_cases} ok={total_ok} skipped={total_skipped} err={total_err}"
    )
    return 0 if total_err == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
