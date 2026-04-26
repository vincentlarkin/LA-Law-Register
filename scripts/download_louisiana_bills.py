r"""
Download Louisiana legislative bill metadata into the local searchable library.

The official Louisiana Legislature session pages expose final disposition tables
for closed sessions. This script turns those records plus each bill's printable
Bill.aspx page into normal BayouLex bundle/section files.

Run examples:
  python scripts\download_louisiana_bills.py --session 25RS
  python scripts\download_louisiana_bills.py --session all
  python scripts\download_louisiana_bills.py --session 25RS --max-bills 25
"""

from __future__ import annotations

import argparse
import concurrent.futures
from dataclasses import dataclass
import datetime as _dt
from html import unescape
from html.parser import HTMLParser
import io
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


BASE = "https://www.legis.la.gov/legis/"
BILLS_CATEGORY = "Louisiana Legislative Bills"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

_INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
_WS = re.compile(r"\s+")
_FINAL_ENTRY_RE = re.compile(
    r'<a\b[^>]*href="(?P<href>BillInfo\.aspx\?i=(?P<id>\d+))"[^>]*>\s*(?P<number>\d+)\s*</a>\s*'
    r'<span\b[^>]*>\s*(?P<disposition>.*?)\s*</span>',
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class BillStub:
    session_id: str
    session_name: str
    session_closed: bool
    chamber: str
    bill_id: str
    bill_number: str
    final_disposition: str
    bill_info_url: str
    bill_print_url: str


@dataclass(frozen=True)
class BillDoc:
    order: int
    session_id: str
    session_name: str
    chamber: str
    bill_id: str
    bill_number: str
    author: str
    title: str
    current_status: str
    final_disposition: str
    status_group: str
    status_label: str
    bill_info_url: str
    bill_print_url: str
    pdf_url: str
    pdf_label: str
    doc_text: str


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


def _fetch(url: str, *, retries: int = 3) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.6 * attempt)
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def _fetch_bytes(url: str, *, retries: int = 3) -> bytes:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=45) as resp:
                return resp.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.6 * attempt)
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def _html_to_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    return _WS.sub(" ", unescape(value).replace("\xa0", " ")).strip()


class _PrintableTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.links: list[tuple[str, str]] = []
        self._skip_depth = 0
        self._in_link = False
        self._link_text: list[str] = []
        self._link_href = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in {"script", "style"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag == "a":
            attr_map = {k.lower(): v or "" for k, v in attrs}
            self._in_link = True
            self._link_text = []
            self._link_href = attr_map.get("href", "")
        if tag in {"br", "p", "div", "tr", "td", "li", "h1", "h2", "h3", "span"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        self.parts.append(data)
        if self._in_link:
            self._link_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag == "a" and self._in_link:
            text = _WS.sub(" ", "".join(self._link_text)).strip()
            if text or self._link_href:
                self.links.append((text, self._link_href))
            self._in_link = False
            self._link_text = []
            self._link_href = ""
        if tag in {"p", "div", "tr", "li", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def text(self) -> str:
        lines: list[str] = []
        for raw in "".join(self.parts).replace("\r", "\n").split("\n"):
            line = _WS.sub(" ", raw.replace("\xa0", " ")).strip()
            if line:
                lines.append(line)
        return "\n".join(lines)


class _SessionOptionParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.options: list[tuple[str, str]] = []
        self._in_option = False
        self._value = ""
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "option":
            return
        attr_map = {k.lower(): v or "" for k, v in attrs}
        self._in_option = True
        self._value = attr_map.get("value", "")
        self._text = []

    def handle_data(self, data: str) -> None:
        if self._in_option:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "option" or not self._in_option:
            return
        text = _WS.sub(" ", "".join(self._text).strip())
        if self._value and text:
            self.options.append((self._value, text))
        self._in_option = False
        self._value = ""
        self._text = []


def _available_sessions() -> list[tuple[str, str]]:
    html = _fetch(urljoin(BASE, "BillSearch.aspx"))
    parser = _SessionOptionParser()
    parser.feed(html)
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for session_id, session_name in parser.options:
        if session_id in seen:
            continue
        seen.add(session_id)
        out.append((session_id, session_name))
    return out


def _session_year_from_id(session_id: str) -> int:
    match = re.match(r"^(\d{2})", session_id)
    if not match:
        return 0
    yy = int(match.group(1))
    return 1900 + yy if yy >= 90 else 2000 + yy


def _resolve_session_args(raw_session: str) -> list[tuple[str, str]]:
    raw_session = raw_session.strip()
    available = _available_sessions()
    by_id = {session_id.upper(): (session_id, name) for session_id, name in available}
    if raw_session.casefold() == "all":
        return available

    out: list[tuple[str, str]] = []
    for part in re.split(r"[,;]\s*", raw_session):
        session_id = part.strip()
        if not session_id:
            continue
        out.append(by_id.get(session_id.upper(), (session_id, session_id)))
    return out


def _session_is_closed(session_id: str) -> bool:
    try:
        html = _fetch(urljoin(BASE, f"SessionInfo/SessionInfo_{session_id}.aspx"))
    except Exception:
        return True
    text = _html_to_text(html)
    session_year = _session_year_from_id(session_id)
    if "Final Adjournment no later than" in text:
        year_match = re.search(r"\b(19|20)\d{2}\b", text)
        if year_match and int(year_match.group(0)) < _dt.date.today().year:
            return True
        return False
    if "Final Adjournment" in text:
        return True
    return bool(session_year and session_year < _dt.date.today().year)


def _extract_final_disposition(
    session_id: str,
    chamber: str,
    *,
    session_name: str,
    session_closed: bool,
) -> list[BillStub]:
    url = urljoin(BASE, f"FinalDisposition.aspx?c={chamber}&sid={session_id}")
    html = _fetch(url)
    prefix = "HB" if chamber == "H" else "SB"
    out: list[BillStub] = []
    for match in _FINAL_ENTRY_RE.finditer(html):
        number = match.group("number")
        bill_id = match.group("id")
        bill_number = f"{prefix}{number}"
        info_url = urljoin(BASE, match.group("href"))
        out.append(
            BillStub(
                session_id=session_id,
                session_name=session_name,
                session_closed=session_closed,
                chamber=chamber,
                bill_id=bill_id,
                bill_number=bill_number,
                final_disposition=_html_to_text(match.group("disposition")),
                bill_info_url=info_url,
                bill_print_url=urljoin(BASE, f"Bill.aspx?i={bill_id}"),
            )
        )
    return out


def _classify_status(final_disposition: str, current_status: str, *, session_closed: bool) -> tuple[str, str]:
    disposition = final_disposition.strip().upper()
    current = current_status.strip().upper()
    if disposition.startswith("ACT ") or " - ACT " in current or current.startswith("SIGNED BY THE GOVERNOR - ACT"):
        return "law", "Passed into Law"
    if "VETOED" in disposition or "VETOED" in current:
        return "vetoed", "Vetoed"
    if not session_closed and "WITHDRAWN" not in disposition:
        return "pending", "Still in Process"
    if not disposition:
        return "pending", "Still in Process"
    return "failed", "Failed or Other Final Disposition"


def _line_after(lines: list[str], marker: str) -> str:
    marker_folded = marker.casefold()
    for idx, line in enumerate(lines):
        if line.casefold() != marker_folded:
            continue
        for value in lines[idx + 1 : idx + 8]:
            if value.startswith("(as of ") or value == ":":
                continue
            return value
    return ""


def _current_status_from_lines(lines: list[str], fallback: str) -> str:
    for line in lines:
        if not line.casefold().startswith("current status"):
            continue
        match = re.search(r"^Current Status\b.*:\s*(.+)$", line, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return _line_after(lines, "Current Status") or fallback


def _document_link_for_bill(
    links: list[tuple[str, str]],
    bill_number: str,
    *,
    status_group: str,
) -> tuple[str, str]:
    document_links: list[tuple[str, str]] = []
    for link_text, href in links:
        if not href or "ViewDocument.aspx?d=" not in href:
            continue
        document_links.append((_WS.sub(" ", link_text).strip(), urljoin(BASE, href)))

    bill_compact = re.sub(r"\s+", "", bill_number).casefold()
    if status_group == "law":
        act_re = re.compile(rf"^{re.escape(bill_number)}\s+Act\b", re.IGNORECASE)
        for link_text, href in document_links:
            label_compact = re.sub(r"\s+", "", link_text).casefold()
            if act_re.search(link_text) or ("act" in link_text.casefold() and bill_compact in label_compact):
                return link_text, href
        for link_text, href in document_links:
            label_compact = re.sub(r"\s+", "", link_text).casefold()
            label_folded = link_text.casefold()
            if bill_compact not in label_compact:
                continue
            if any(word in label_folded for word in ("digest", "fiscal", "vote", "amendment", "summary")):
                continue
            if re.search(r"\b(en|enrolled)\b", link_text, re.IGNORECASE) or label_compact.endswith("en"):
                return link_text, href

    wanted = (
        f"{bill_number} Enrolled",
        f"{bill_number} Reengrossed",
        f"{bill_number} Engrossed",
        f"{bill_number} Original",
    )
    for prefix in wanted:
        for link_text, href in document_links:
            label_compact = re.sub(r"\s+", "", link_text).casefold()
            prefix_compact = re.sub(r"\s+", "", prefix).casefold()
            if link_text.casefold().startswith(prefix.casefold()) or label_compact.startswith(prefix_compact):
                return link_text, href
    return "", ""


def _document_links_from_lines(lines: list[str], bill_number: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    url_re = re.compile(r"^https?://\S*ViewDocument\.aspx\?d=\d+\S*$", re.IGNORECASE)
    for idx, line in enumerate(lines[:-1]):
        next_line = lines[idx + 1].strip()
        if url_re.match(next_line):
            out.append((_WS.sub(" ", line).strip(), next_line))
    return out


def _extract_pdf_text_from_bytes(data: bytes) -> str:
    if not data:
        return ""
    try:
        import fitz  # type: ignore[import-not-found]

        chunks: list[str] = []
        with fitz.open(stream=data, filetype="pdf") as doc:
            for page in doc:
                chunks.append(page.get_text("text"))
        text = "\n".join(chunks)
    except Exception:
        try:
            from pypdf import PdfReader  # type: ignore[import-not-found]

            reader = PdfReader(io.BytesIO(data))
            text = "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception:
            return ""

    lines = []
    for raw in text.replace("\r", "\n").split("\n"):
        line = _WS.sub(" ", raw.replace("\xa0", " ")).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def _fetch_document_text(url: str) -> str:
    if not url:
        return ""
    return _extract_pdf_text_from_bytes(_fetch_bytes(url))


def _parse_printable_bill(
    stub: BillStub,
    html: str,
    *,
    order: int,
    include_document_text: bool,
    document_statuses: set[str],
) -> BillDoc:
    parser = _PrintableTextParser()
    parser.feed(html)
    text = parser.text()
    lines = text.splitlines()

    session_name = stub.session_name or stub.session_id
    for idx, line in enumerate(lines):
        if line == f"Bill Info - {stub.bill_number}" and idx + 1 < len(lines):
            session_name = lines[idx + 1].strip() or stub.session_id
            break

    author = ""
    title = ""
    for idx, line in enumerate(lines):
        if line == stub.bill_number:
            if idx + 1 < len(lines) and lines[idx + 1].casefold().startswith("by "):
                author = lines[idx + 1][3:].strip()
                if idx + 2 < len(lines):
                    title = lines[idx + 2].strip()
            break

    current_status = _current_status_from_lines(lines, stub.final_disposition)

    status_group, status_label = _classify_status(
        stub.final_disposition,
        current_status,
        session_closed=stub.session_closed,
    )
    document_links = parser.links + _document_links_from_lines(lines, stub.bill_number)
    pdf_label, pdf_url = _document_link_for_bill(document_links, stub.bill_number, status_group=status_group)
    document_text = ""
    if include_document_text and status_group in document_statuses and pdf_url:
        document_text = _fetch_document_text(pdf_url)

    header = [
        stub.bill_number,
        title,
        f"Session: {stub.session_id}",
        f"Author: {author}",
        f"Current Status: {current_status}",
        f"Final Disposition: {stub.final_disposition}",
        f"Status Group: {status_label}",
        f"Official Bill Page: {stub.bill_info_url}",
        f"Official Document: {pdf_label}" if pdf_label else "",
        f"Official Document URL: {pdf_url}" if pdf_url else "",
        "",
    ]
    doc_text = "\n".join(line for line in header if line) + "\n\n" + text
    if document_text:
        doc_text = (
            f"{doc_text}\n\n"
            "===== OFFICIAL ACT TEXT =====\n"
            f"Source: {pdf_label or pdf_url}\n"
            f"{pdf_url}\n\n"
            f"{document_text}"
        )

    return BillDoc(
        order=order,
        session_id=stub.session_id,
        session_name=session_name,
        chamber=stub.chamber,
        bill_id=stub.bill_id,
        bill_number=stub.bill_number,
        author=author,
        title=title,
        current_status=current_status,
        final_disposition=stub.final_disposition,
        status_group=status_group,
        status_label=status_label,
        bill_info_url=stub.bill_info_url,
        bill_print_url=stub.bill_print_url,
        pdf_url=pdf_url,
        pdf_label=pdf_label,
        doc_text=doc_text,
    )


def _download_bill(
    stub: BillStub,
    *,
    order: int,
    include_document_text: bool,
    document_statuses: set[str],
) -> BillDoc:
    html = _fetch(stub.bill_print_url)
    return _parse_printable_bill(
        stub,
        html,
        order=order,
        include_document_text=include_document_text,
        document_statuses=document_statuses,
    )


def _chamber_label(chamber: str) -> str:
    return "House Bills" if chamber == "H" else "Senate Bills" if chamber == "S" else "Bills"


def _bundle_name(session_name: str, chamber: str) -> str:
    return f"{session_name} - {_chamber_label(chamber)}"


def _entry_for(doc: BillDoc) -> dict[str, object]:
    return {
        "order": doc.order,
        "doc_id": doc.bill_id,
        "citation": doc.bill_number,
        "title": doc.title,
        "url": doc.bill_info_url,
    }


def _write_bill_bundle(out_dir: Path, session_id: str, docs: list[BillDoc]) -> None:
    by_chamber: dict[str, list[BillDoc]] = {}
    for doc in docs:
        by_chamber.setdefault(doc.chamber, []).append(doc)

    category_dir = out_dir / safe_name(BILLS_CATEGORY)
    if category_dir.exists():
        session_names = {safe_name(doc.session_name) for doc in docs if doc.session_name}
        stale_prefixes = {f"{session_id} - ", *{f"{name} - " for name in session_names}}
        for child in category_dir.iterdir():
            if child.is_dir() and any(child.name.startswith(prefix) for prefix in stale_prefixes):
                shutil.rmtree(child)

    for chamber, chamber_docs in sorted(by_chamber.items()):
        session_name = next((doc.session_name for doc in chamber_docs if doc.session_name), session_id)
        bundle_name = _bundle_name(session_name, chamber)
        bundle_dir = category_dir / safe_name(bundle_name)
        sections_dir = bundle_dir / "sections"
        sections_dir.mkdir(parents=True, exist_ok=True)

        entries = []
        for doc in sorted(chamber_docs, key=lambda item: int(re.sub(r"\D", "", item.bill_number) or 0)):
            txt_rel = str((sections_dir / f"{doc.bill_id}.txt").relative_to(out_dir)).replace("\\", "/")
            meta = {
                "doc_id": doc.bill_id,
                "citation": doc.bill_number,
                "title": doc.title,
                "session_id": doc.session_id,
                "session_name": doc.session_name,
                "chamber": doc.chamber,
                "chamber_label": _chamber_label(doc.chamber),
                "author": doc.author,
                "current_status": doc.current_status,
                "final_disposition": doc.final_disposition,
                "bill_status_group": doc.status_group,
                "bill_status_label": doc.status_label,
                "url": doc.bill_info_url,
                "bill_print_url": doc.bill_print_url,
                "pdf_url": doc.pdf_url,
                "pdf_label": doc.pdf_label,
                "local_file": txt_rel,
            }
            _atomic_write_text(sections_dir / f"{doc.bill_id}.json", json.dumps(meta, ensure_ascii=False, indent=2))
            _atomic_write_text(sections_dir / f"{doc.bill_id}.txt", doc.doc_text)
            entries.append(_entry_for(doc))

        bundle = {
            "category_key": "legislative-bills",
            "category_name": BILLS_CATEGORY,
            "bundle_name": bundle_name,
            "source_toc_url": urljoin(BASE, f"FinalDisposition.aspx?c={chamber}&sid={session_id}"),
            "entries": entries,
        }
        _atomic_write_text(bundle_dir / "bundle.json", json.dumps(bundle, ensure_ascii=False, indent=2))


def _download_session(
    *,
    out_dir: Path,
    session_id: str,
    session_name: str,
    workers: int,
    max_bills: int,
    include_document_text: bool,
    document_statuses: set[str],
) -> tuple[int, dict[str, int], int]:
    print(f"[info] Reading final disposition tables for {session_id}...", flush=True)
    session_closed = _session_is_closed(session_id)
    if not session_closed:
        print(f"[info]   {session_id} appears to be open; non-final bills will be marked Still in Process.")
    stubs = _extract_final_disposition(
        session_id,
        "H",
        session_name=session_name,
        session_closed=session_closed,
    ) + _extract_final_disposition(
        session_id,
        "S",
        session_name=session_name,
        session_closed=session_closed,
    )
    stubs = sorted(stubs, key=_natural_bill_order)
    if max_bills > 0:
        stubs = stubs[:max_bills]
    if not stubs:
        print(f"[warn] No bills found for session {session_id}", file=sys.stderr, flush=True)
        return 0, {}, 0

    print(f"[info] Downloading {len(stubs):,} bill page(s) for {session_id}...", flush=True)
    docs: list[BillDoc] = []
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(
                _download_bill,
                stub,
                order=idx + 1,
                include_document_text=include_document_text,
                document_statuses=document_statuses,
            ): stub
            for idx, stub in enumerate(stubs)
        }
        for done_count, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            stub = futures[future]
            try:
                docs.append(future.result())
            except Exception as exc:
                errors.append(f"{stub.bill_number} ({stub.bill_id}): {exc}")
            if done_count % 100 == 0 or done_count == len(stubs):
                print(
                    f"[info]   {session_id}: {done_count:,}/{len(stubs):,} bill pages processed",
                    flush=True,
                )

    if errors:
        err_path = out_dir / safe_name(BILLS_CATEGORY) / f"{session_id}_download_errors.jsonl"
        _atomic_write_text(err_path, "\n".join(json.dumps({"error": err}, ensure_ascii=False) for err in errors) + "\n")
        print(f"[warn] {len(errors):,} bill(s) failed for {session_id}; wrote {err_path}", file=sys.stderr)

    if not docs:
        print(f"[error] No bill pages downloaded successfully for {session_id}.", file=sys.stderr)
        return 0, {}, len(errors)

    _write_bill_bundle(out_dir, session_id, docs)
    counts: dict[str, int] = {}
    for doc in docs:
        counts[doc.status_label] = counts.get(doc.status_label, 0) + 1
    count_msg = ", ".join(f"{label}: {count:,}" for label, count in sorted(counts.items()))
    print(f"[ok] Wrote {len(docs):,} bills for {session_id} ({count_msg}) -> {out_dir / safe_name(BILLS_CATEGORY)}")
    return len(docs), counts, len(errors)


def _natural_bill_order(stub: BillStub) -> tuple[int, int]:
    chamber_rank = 0 if stub.chamber == "H" else 1
    return chamber_rank, int(re.sub(r"\D", "", stub.bill_number) or 0)


def _parse_document_statuses(raw: str) -> set[str]:
    values = {part.strip().casefold() for part in re.split(r"[,;]\s*", raw) if part.strip()}
    if not values or "law" in values:
        values.add("law")
    if "all" in values:
        return {"law", "vetoed", "pending", "failed"}
    allowed = {"law", "vetoed", "pending", "failed"}
    unknown = values - allowed
    if unknown:
        raise ValueError(f"unknown document status value(s): {', '.join(sorted(unknown))}")
    return values


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", default="25RS", help="Legislative session id, comma list, or all (default: 25RS)")
    parser.add_argument("--out", default="out", help="Output root (default: out)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel bill page fetches (default: 8)")
    parser.add_argument("--max-bills", type=int, default=0, help="Development limit; 0 downloads all")
    parser.add_argument("--max-sessions", type=int, default=0, help="Development limit when --session all is used")
    parser.add_argument(
        "--include-document-text",
        action="store_true",
        help="Fetch official document PDFs in memory and extract their text into generated .txt files",
    )
    parser.add_argument(
        "--document-statuses",
        default="law",
        help="Statuses to extract document text for: law, vetoed, pending, failed, all (default: law)",
    )
    args = parser.parse_args(argv)

    try:
        document_statuses = _parse_document_statuses(args.document_statuses)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    sessions = _resolve_session_args(args.session)
    if args.max_sessions > 0:
        sessions = sessions[: args.max_sessions]
    if not sessions:
        print("[error] No sessions resolved.", file=sys.stderr)
        return 1

    print(f"[info] Resolved {len(sessions):,} session(s): {', '.join(session_id for session_id, _ in sessions)}")
    grand_total = 0
    grand_errors = 0
    grand_counts: dict[str, int] = {}
    for session_idx, (session_id, session_name) in enumerate(sessions, start=1):
        print(f"[info] Session {session_idx:,}/{len(sessions):,}: {session_id} ({session_name})", flush=True)
        total, counts, errors = _download_session(
            out_dir=out_dir,
            session_id=session_id,
            session_name=session_name,
            workers=args.workers,
            max_bills=args.max_bills,
            include_document_text=args.include_document_text,
            document_statuses=document_statuses,
        )
        grand_total += total
        grand_errors += errors
        for label, count in counts.items():
            grand_counts[label] = grand_counts.get(label, 0) + count

    count_msg = ", ".join(f"{label}: {count:,}" for label, count in sorted(grand_counts.items()))
    print(f"[ok] Wrote {grand_total:,} total bill records ({count_msg})")
    return 0 if grand_total > 0 and grand_errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
