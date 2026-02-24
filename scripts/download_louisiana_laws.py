"""
Download Louisiana laws from https://www.legis.la.gov/legis/LawsContents.aspx
and save them as local text + static PDFs.

Why Playwright?
  The TOC pages are ASP.NET WebForms; many nodes expand via postback and do not
  have stable URLs. We use Playwright (Edge channel) to click nodes and extract
  the real Law document links: Law.aspx?d=<id>.

Run examples:
  python scripts/download_louisiana_laws.py --categories revised-statutes
  python scripts/download_louisiana_laws.py --categories all
  python scripts/download_louisiana_laws.py --categories revised-statutes --max-bundles 1 --max-sections 10
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable, Literal, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import Browser, Page, sync_playwright


BASE = "https://www.legis.la.gov/legis/"
SCRIPT_VERSION = 3  # bump when cache schema changes
_TOC_CACHE_SCHEMA_VERSION = 1


CategoryKey = Literal[
    "revised-statutes",
    "louisiana-constitution",
    "constitution-ancillaries",
    "childrens-code",
    "civil-code",
    "code-of-civil-procedure",
    "code-of-criminal-procedure",
    "code-of-evidence",
    "house-rules",
    "senate-rules",
    "joint-rules",
]


@dataclass(frozen=True)
class CategoryConfig:
    key: CategoryKey
    display_name: str
    folder_id: int


@dataclass(frozen=True)
class RootNode:
    number_text: str
    name_text: str
    click_id: str  # DOM id to click (anchor id)


@dataclass(frozen=True)
class TocEntry:
    order: int
    citation: str
    title: str
    url: str  # absolute URL

    @property
    def doc_id(self) -> str:
        qs = parse_qs(urlparse(self.url).query)
        d = (qs.get("d") or [""])[0]
        return d


@dataclass(frozen=True)
class Bundle:
    category_key: str
    category_name: str
    bundle_name: str
    source_toc_url: str
    entries: list[TocEntry]


def _new_requests_session(*, trust_env: bool) -> requests.Session:
    s = requests.Session()
    s.trust_env = trust_env
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
    )
    return s


# Windows-invalid path characters + ASCII control characters.
# NOTE: must be `\x00-\x1F` (not `\\x00-\\x1F`) or the regex range accidentally
# eats lots of normal characters (e.g. uppercase letters).
_INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
_WS = re.compile(r"\s+")


def safe_name(s: str, *, max_len: int = 140) -> str:
    s = s.strip()
    s = _WS.sub(" ", s)
    s = _INVALID_PATH_CHARS.sub("_", s)
    s = s.rstrip(". ")  # Windows doesn't like trailing dot/space
    if len(s) > max_len:
        s = s[: max_len - 1].rstrip()
    return s or "_"


def _ts() -> str:
    return time.strftime("%H:%M:%S")


class _Logger:
    def __init__(self, *, verbose: bool) -> None:
        self._verbose = verbose
        self._lock = threading.Lock()

    def info(self, msg: str) -> None:
        with self._lock:
            print(f"[{_ts()}] {msg}", flush=True)

    def debug(self, msg: str) -> None:
        if not self._verbose:
            return
        with self._lock:
            print(f"[{_ts()}] [debug] {msg}", flush=True)

    def warn(self, msg: str) -> None:
        with self._lock:
            print(f"[{_ts()}] [warn] {msg}", file=sys.stderr, flush=True)


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


def _append_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding=encoding, newline="\n") as f:
        f.write(content)


def _fmt_rate(count: int, seconds: float) -> str:
    if seconds <= 0:
        return "?"
    return f"{count / seconds:.1f}/s"


def _fmt_dur(seconds: float) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:d}:{s:02d}"


def _run_with_heartbeat(
    *,
    logger: Optional["_Logger"],
    label: str,
    interval_s: float,
    fn: Callable[[], None],
) -> None:
    """Run a blocking step while periodically logging liveness."""
    if logger is None:
        fn()
        return

    interval = max(1.0, float(interval_s))
    start = time.monotonic()
    stop = threading.Event()

    def _beat() -> None:
        while not stop.wait(interval):
            logger.info(f"    [pdf] {label}: still working elapsed={_fmt_dur(time.monotonic() - start)}")

    t = threading.Thread(target=_beat, name="pdf-heartbeat", daemon=True)
    t.start()
    try:
        fn()
    finally:
        stop.set()
        t.join(timeout=0.2)


class _RateLimiter:
    def __init__(self, min_interval_s: float) -> None:
        self._min = max(0.0, float(min_interval_s))
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self, stop_event: threading.Event) -> None:
        if self._min <= 0:
            return
        while True:
            if stop_event.is_set():
                return
            with self._lock:
                now = time.monotonic()
                wait_s = self._min - (now - self._last)
                if wait_s <= 0:
                    self._last = now
                    return
            time.sleep(min(wait_s, 0.1))


def _project_root() -> Path:
    # scripts/download_louisiana_laws.py -> project root
    return Path(__file__).resolve().parents[1]


def _toc_cache_path(cache_dir: Path, cat: CategoryConfig) -> Path:
    return cache_dir / f"{cat.key}.json"


def _load_toc_cache(
    cache_path: Path,
    *,
    ttl_days: float,
    logger: _Logger,
) -> Optional[list[Bundle]]:
    if ttl_days <= 0:
        return None
    if not cache_path.exists() or cache_path.stat().st_size <= 0:
        return None

    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warn(f"[toc] cache read failed ({cache_path}): {e}")
        return None

    if data.get("schema_version") != SCRIPT_VERSION:
        return None

    created_at = int(data.get("created_at_epoch") or 0)
    if created_at <= 0:
        return None
    age_s = int(time.time()) - created_at
    if age_s > int(ttl_days * 86400):
        return None

    bundles_raw = data.get("bundles")
    if not isinstance(bundles_raw, list) or not bundles_raw:
        return None

    category_key = str(data.get("category_key") or "")
    category_name = str(data.get("category_name") or "")
    source_toc_url = str(data.get("source_toc_url") or "")

    bundles: list[Bundle] = []
    for b in bundles_raw:
        if not isinstance(b, dict):
            continue
        bundle_name = str(b.get("bundle_name") or "")
        entries_raw = b.get("entries") or []
        if not bundle_name or not isinstance(entries_raw, list):
            continue
        entries: list[TocEntry] = []
        for i, e in enumerate(entries_raw, start=1):
            if not isinstance(e, dict):
                continue
            url = str(e.get("url") or "")
            if not url:
                continue
            citation = str(e.get("citation") or "")
            title = str(e.get("title") or "")
            order = int(e.get("order") or i)
            entries.append(TocEntry(order=order, citation=citation, title=title, url=url))
        if not entries:
            continue
        bundles.append(
            Bundle(
                category_key=category_key,
                category_name=category_name,
                bundle_name=bundle_name,
                source_toc_url=source_toc_url,
                entries=entries,
            )
        )

    if not bundles:
        return None

    logger.info(f"[toc] cache hit: {cache_path} age={_fmt_dur(age_s)} bundles={len(bundles)}")
    return bundles


def _save_toc_cache(
    cache_path: Path,
    *,
    cat: CategoryConfig,
    bundles: list[Bundle],
    logger: _Logger,
) -> None:
    now = int(time.time())
    obj = {
        "schema_version": SCRIPT_VERSION,
        "created_at_epoch": now,
        "category_key": cat.key,
        "category_name": cat.display_name,
        "source_toc_url": toc_url(cat.folder_id),
        "bundles": [
            {
                "bundle_name": b.bundle_name,
                "entries": [asdict(e) for e in b.entries],
            }
            for b in bundles
        ],
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(cache_path, obj)
    logger.info(f"[toc] cache write: {cache_path} bundles={len(bundles)}")


def toc_url(folder_id: int) -> str:
    return urljoin(BASE, f"Laws_Toc.aspx?folder={folder_id}&level=Parent")


def _extract_update_panel_html(full_html: str) -> str:
    soup = BeautifulSoup(full_html, "html.parser")
    panel = soup.find(id="ctl00_ctl00_PageBody_PageContent_UpdatePanelToc")
    if not panel:
        # Some pages (rare) may render a slightly different id; fall back to returning whole page.
        return full_html
    return panel.decode_contents()


def _parse_root_tab_text(panel_html: str) -> Optional[str]:
    """Returns tab text like 'Titles' or 'Articles' if present."""
    soup = BeautifulSoup(panel_html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "MenuSearch" in href and "Tab1" in href:
            txt = a.get_text(" ", strip=True)
            return txt or None
    return None


def _parse_root_nodes(panel_html: str) -> list[RootNode]:
    """Parse rows like TITLE 1 / General Provisions."""
    soup = BeautifulSoup(panel_html, "html.parser")
    nodes: list[RootNode] = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) != 2:
            continue
        a1 = tds[0].find("a", href=True, id=True)
        a2 = tds[1].find("a", href=True, id=True)
        if not a1 or not a2:
            continue
        href = a1["href"]
        if not href.startswith("javascript:__doPostBack"):
            continue
        if "ListViewTOC1" not in a1["id"]:
            continue
        number_text = a1.get_text(" ", strip=True)
        name_text = a2.get_text(" ", strip=True)
        if not number_text or not name_text:
            continue
        nodes.append(RootNode(number_text=number_text, name_text=name_text, click_id=a1["id"]))
    return nodes


def _parse_leaf_entries(panel_html: str) -> list[TocEntry]:
    """Parse rows where each row has two Law.aspx links: citation + title."""
    soup = BeautifulSoup(panel_html, "html.parser")
    entries: list[TocEntry] = []
    order = 0
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        a1 = tds[0].find("a", href=True)
        a2 = tds[1].find("a", href=True)
        if not a1 or not a2:
            continue
        href1 = a1["href"]
        href2 = a2["href"]
        if not (href1.startswith("Law.aspx?") and href2.startswith("Law.aspx?")):
            continue
        # Usually both tds point at the same doc id.
        if href1 != href2:
            continue
        citation = a1.get_text(" ", strip=True)
        title = a2.get_text(" ", strip=True)
        if not citation:
            continue
        order += 1
        entries.append(
            TocEntry(
                order=order,
                citation=citation,
                title=title,
                url=urljoin(BASE, href1),
            )
        )
    return entries


def _dedupe_entries(entries: list[TocEntry]) -> list[TocEntry]:
    seen: set[str] = set()
    out: list[TocEntry] = []
    for e in entries:
        doc_id = e.doc_id
        if not doc_id:
            continue
        if doc_id in seen:
            continue
        seen.add(doc_id)
        out.append(e)
    return [TocEntry(order=i, citation=e.citation, title=e.title, url=e.url) for i, e in enumerate(out, start=1)]


def _filter_revised_statutes_title_doc(root: RootNode, entries: list[TocEntry]) -> list[TocEntry]:
    """
    Revised Statutes TOC sometimes includes a non-section entry like "RS 3"
    that is effectively just the Title heading. For consistent PDFs that start
    at real sections (RS x:y), drop that entry.
    """
    m = re.match(r"^TITLE\s+(\d+)\b", root.number_text.strip(), re.IGNORECASE)
    if not m:
        return entries
    title_num = m.group(1)
    filtered = [
        e
        for e in entries
        if not re.fullmatch(rf"RS\s+{re.escape(title_num)}", e.citation.strip(), flags=re.IGNORECASE)
    ]
    # Re-number order after filtering.
    return _dedupe_entries(filtered)


def _bundle_name_default(root: RootNode) -> str:
    return f"{root.number_text} - {root.name_text}"


def _write_json(path: Path, obj: object) -> None:
    _atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2) + "\n")


def _download_static_pdf(session: requests.Session, url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0:
        return
    r = session.get(url, timeout=60)
    r.raise_for_status()
    _atomic_write_bytes(out_path, r.content)


def _extract_law_doc(session: requests.Session, url: str) -> dict[str, str]:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    citation = ""
    cit_el = soup.find(id="ctl00_PageBody_LabelName")
    if cit_el:
        citation = cit_el.get_text(" ", strip=True)

    doc_el = soup.find(id="ctl00_PageBody_LabelDocument")
    if not doc_el:
        raise RuntimeError("Could not locate law document content span (ctl00_PageBody_LabelDocument)")

    doc_html = doc_el.decode_contents()
    doc_text = "\n".join([ln.strip() for ln in doc_el.get_text("\n").splitlines() if ln.strip()])

    return {"citation": citation, "doc_html": doc_html, "doc_text": doc_text}


def _render_bundle_html(
    bundle: Bundle,
    bundle_dir: Path,
    *,
    toc_page_numbers: Optional[dict[str, int]] = None,
    include_markers: bool = False,
    out_filename: str = "bundle.html",
) -> Path:
    """Create an HTML bundle suitable for printing to PDF."""
    html_path = bundle_dir / out_filename
    sections_dir = bundle_dir / "sections"

    available: list[tuple[TocEntry, dict]] = []
    for e in bundle.entries:
        doc_id = e.doc_id
        if not doc_id:
            continue
        meta_path = sections_dir / f"{doc_id}.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        available.append((e, meta))

    # Stream-write to avoid large in-memory strings.
    with html_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("<!doctype html>\n")
        f.write("<html><head><meta charset=\"utf-8\" />\n")
        f.write(f"<title>{bundle.bundle_name}</title>\n")
        f.write("<style>\n")
        f.write(
            """
@page { margin: 0.75in; }
body { font-family: "Times New Roman", Times, serif; font-size: 12pt; color: #111; }
h1 { font-size: 20pt; margin: 0 0 0.2in 0; }
h2 { font-size: 14pt; margin: 0.25in 0 0.08in 0; }
.toc { margin: 0.2in 0 0.35in 0; page-break-after: always; }
.toc h2 { margin-top: 0; }
.toc a { color: inherit; text-decoration: none; }
.toc .row { display: flex; gap: 0.15in; line-height: 1.25; align-items: baseline; }
.toc .row .lhs { flex: 0 0 auto; min-width: 1.25in; }
.toc .row .rhs { flex: 0 1 auto; min-width: 0; }
.toc .row .dots { flex: 1 1 auto; border-bottom: 1px dotted #777; transform: translateY(-0.35em); }
.toc .row .pnum {
  flex: 0 0 auto;
  width: 0.65in;
  text-align: right;
  font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 10pt;
}
section.law { page-break-before: always; position: relative; }
.docid-marker { position: absolute; top: 0; right: 0; font-size: 1pt; color: #ffffff; }
.law .citation { font-weight: bold; }
.law .title { margin-top: 0.05in; font-style: italic; color: #222; }
.law .doc { margin-top: 0.15in; }
/* Normalize doc paragraphs a bit; preserve align attributes from source. */
.law .doc p { margin: 0 0 0.08in 0; }
"""
        )
        f.write("</style></head><body>\n")
        f.write(f"<h1>{bundle.bundle_name}</h1>\n")
        # Keep provenance in HTML comments (won't render into the PDF).
        f.write(f"<!-- Source TOC: {bundle.source_toc_url} -->\n")

        # TOC
        f.write("<div class=\"toc\">\n")
        f.write("<h2>Table of Contents</h2>\n")
        for e, _meta in available:
            doc_id = e.doc_id
            anchor = f"doc-{doc_id or e.order}"
            title = e.title or ""
            pnum = ""
            if toc_page_numbers and doc_id and doc_id in toc_page_numbers:
                pnum = str(toc_page_numbers[doc_id])
            f.write("<div class=\"row\">")
            f.write(f"<span class=\"lhs\"><a href=\"#{anchor}\">{e.citation}</a></span>")
            f.write(f"<span class=\"rhs\"><a href=\"#{anchor}\">{title}</a></span>")
            f.write("<span class=\"dots\"></span>")
            f.write(f"<span class=\"pnum\">{pnum}</span>")
            f.write("</div>\n")
        f.write("</div>\n")

        # Sections
        for e, meta in available:
            doc_id = e.doc_id
            anchor = f"doc-{doc_id or e.order}"
            f.write(f"<section class=\"law\" id=\"{anchor}\">\n")
            if include_markers and doc_id:
                # Used to map doc -> PDF page number.
                f.write(f"<div class=\"docid-marker\">DOCID:{doc_id}</div>\n")
            f.write(f"<div class=\"citation\">{meta.get('citation') or e.citation}</div>\n")
            if e.title:
                f.write(f"<div class=\"title\">{e.title}</div>\n")
            f.write("<div class=\"doc\">\n")
            f.write(meta.get("doc_html", ""))
            f.write("\n</div>\n</section>\n")

        f.write("</body></html>\n")

    return html_path


class _PdfRenderer:
    def __init__(self, *, browser_channel: str, headless: bool) -> None:
        self._p = sync_playwright().start()
        self._browser = self._p.chromium.launch(channel=browser_channel, headless=headless)

    def close(self) -> None:
        try:
            try:
                self._browser.close()
            except BaseException:
                # Common on Ctrl+C / driver teardown; safe to ignore.
                pass
        finally:
            try:
                self._p.stop()
            except BaseException:
                pass

    def render(
        self,
        html_path: Path,
        pdf_path: Path,
        *,
        logger: Optional["_Logger"] = None,
        phase_label: str = "render",
        progress_interval_s: float = 15.0,
    ) -> None:
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_pdf = pdf_path.with_suffix(pdf_path.suffix + ".tmp")
        page = self._browser.new_page()
        try:
            # Use file:// to keep relative links working (anchors, etc).
            if logger is not None:
                try:
                    html_mb = html_path.stat().st_size / (1024 * 1024)
                    logger.info(f"    [pdf] {phase_label}: html={html_path.name} size={html_mb:.1f}MB")
                except OSError:
                    logger.info(f"    [pdf] {phase_label}: html={html_path.name}")

            t0 = time.monotonic()
            _run_with_heartbeat(
                logger=logger,
                label=f"{phase_label}: loading html",
                interval_s=progress_interval_s,
                fn=lambda: page.goto(html_path.resolve().as_uri(), wait_until="load", timeout=120_000),
            )
            if logger is not None:
                logger.info(f"    [pdf] {phase_label}: html loaded in {_fmt_dur(time.monotonic() - t0)}")

            t1 = time.monotonic()
            _run_with_heartbeat(
                logger=logger,
                label=f"{phase_label}: printing pdf",
                interval_s=progress_interval_s,
                fn=lambda: page.pdf(
                    path=str(tmp_pdf),
                    format="Letter",
                    print_background=True,
                    display_header_footer=True,
                    header_template="<div></div>",
                    footer_template=(
                        "<div style='width:100%; font-size:9px; color:#444; "
                        "padding:0 0.6in; box-sizing:border-box; text-align:right;'>"
                        "<span class='pageNumber'></span>"
                        "</div>"
                    ),
                    margin={"top": "0.6in", "bottom": "0.6in", "left": "0.6in", "right": "0.6in"},
                ),
            )
            if logger is not None:
                logger.info(f"    [pdf] {phase_label}: pdf print done in {_fmt_dur(time.monotonic() - t1)}")
        finally:
            page.close()
        os.replace(tmp_pdf, pdf_path)
        if logger is not None:
            try:
                out_mb = pdf_path.stat().st_size / (1024 * 1024)
                logger.info(f"    [pdf] {phase_label}: wrote {pdf_path.name} ({out_mb:.1f}MB)")
            except OSError:
                pass


def _render_pdf_from_html(
    html_path: Path,
    pdf_path: Path,
    *,
    browser_channel: str,
    headless: bool,
    renderer: Optional[_PdfRenderer] = None,
    logger: Optional["_Logger"] = None,
    phase_label: str = "render",
    progress_interval_s: float = 15.0,
) -> None:
    if renderer is not None:
        renderer.render(
            html_path,
            pdf_path,
            logger=logger,
            phase_label=phase_label,
            progress_interval_s=progress_interval_s,
        )
        return
    # Fallback (slower): create a new browser per render.
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_pdf = pdf_path.with_suffix(pdf_path.suffix + ".tmp")
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=browser_channel, headless=headless)
        page = browser.new_page()
        try:
            if logger is not None:
                logger.info(f"    [pdf] {phase_label}: fallback renderer start")
            _run_with_heartbeat(
                logger=logger,
                label=f"{phase_label}: loading html (fallback)",
                interval_s=progress_interval_s,
                fn=lambda: page.goto(html_path.resolve().as_uri(), wait_until="load", timeout=120_000),
            )
            _run_with_heartbeat(
                logger=logger,
                label=f"{phase_label}: printing pdf (fallback)",
                interval_s=progress_interval_s,
                fn=lambda: page.pdf(
                    path=str(tmp_pdf),
                    format="Letter",
                    print_background=True,
                    display_header_footer=True,
                    header_template="<div></div>",
                    footer_template=(
                        "<div style='width:100%; font-size:9px; color:#444; "
                        "padding:0 0.6in; box-sizing:border-box; text-align:right;'>"
                        "<span class='pageNumber'></span>"
                        "</div>"
                    ),
                    margin={"top": "0.6in", "bottom": "0.6in", "left": "0.6in", "right": "0.6in"},
                ),
            )
        finally:
            page.close()
            browser.close()
        os.replace(tmp_pdf, pdf_path)


def _load_cached_toc_page_numbers(
    toc_path: Path,
    *,
    expected_doc_ids: Iterable[str],
    html_sha256: str,
) -> Optional[dict[str, int]]:
    if not toc_path.exists() or toc_path.stat().st_size <= 0:
        return None
    try:
        obj = json.loads(toc_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    if int(obj.get("schema_version") or 0) != 1:
        return None
    if str(obj.get("html_sha256") or "") != html_sha256:
        return None
    raw_map = obj.get("page_map")
    if not isinstance(raw_map, dict):
        return None
    page_map: dict[str, int] = {}
    for k, v in raw_map.items():
        if not isinstance(k, str):
            return None
        try:
            page_num = int(v)
        except Exception:
            return None
        if page_num <= 0:
            return None
        page_map[k] = page_num
    expected = {d for d in expected_doc_ids if d}
    if not expected:
        return {}
    if not expected.issubset(set(page_map.keys())):
        return None
    return {doc_id: page_map[doc_id] for doc_id in expected}


def _save_toc_page_numbers(
    toc_path: Path,
    *,
    page_map: dict[str, int],
    doc_ids: Iterable[str],
    html_sha256: str,
    scan_backend: str,
) -> None:
    expected = sorted({d for d in doc_ids if d})
    _write_json(
        toc_path,
        {
            "schema_version": 1,
            "created_at_epoch": int(time.time()),
            "html_sha256": html_sha256,
            "scan_backend": scan_backend,
            "doc_ids": expected,
            "page_map": {doc_id: int(page_map[doc_id]) for doc_id in expected if doc_id in page_map},
        },
    )


def _compute_doc_start_pages_from_pdf_pymupdf(
    pdf_path: Path,
    *,
    doc_ids: Iterable[str],
    logger: Optional["_Logger"] = None,
    progress_interval_s: float = 10.0,
) -> dict[str, int]:
    try:
        import fitz  # PyMuPDF
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Missing dependency: pymupdf (pip install pymupdf)") from e

    wanted = {d for d in doc_ids if d}
    if not wanted:
        return {}

    marker_re = re.compile(r"DOCID:(\d+)")
    scan_start = time.monotonic()
    found: dict[str, int] = {}
    scanned_pages = 0
    last_progress = scan_start
    progress_every = max(1.0, float(progress_interval_s))

    with fitz.open(str(pdf_path)) as pdf_doc:
        total_pages = int(pdf_doc.page_count)
        if logger is not None:
            logger.info(f"    [pdf] pass1: scan init pages={total_pages} targets={len(wanted)} backend=pymupdf")

        for page_idx in range(total_pages):
            scanned_pages = page_idx + 1
            text = pdf_doc.load_page(page_idx).get_text("text") or ""
            for m in marker_re.finditer(text):
                doc_id = m.group(1)
                if doc_id in wanted and doc_id not in found:
                    found[doc_id] = scanned_pages

            now = time.monotonic()
            if logger is not None and (now - last_progress) >= progress_every:
                rate = scanned_pages / max(0.001, now - scan_start)
                remaining_pages = max(0, total_pages - scanned_pages)
                eta_s = remaining_pages / max(0.001, rate)
                logger.info(
                    f"    [pdf] pass1: scan progress {scanned_pages}/{total_pages} "
                    f"({scanned_pages*100/max(1, total_pages):.1f}%) "
                    f"found={len(found)}/{len(wanted)} rate={rate:.1f}/s eta={_fmt_dur(eta_s)}"
                )
                last_progress = now

            if len(found) == len(wanted):
                break

    missing = sorted(wanted - set(found.keys()))
    if missing:
        raise RuntimeError(
            "Could not determine page numbers for some docs. "
            f"Missing markers for doc_id(s): {missing[:20]}{'...' if len(missing) > 20 else ''}"
        )

    if logger is not None:
        elapsed = time.monotonic() - scan_start
        rate = scanned_pages / max(0.001, elapsed)
        logger.info(
            f"    [pdf] pass1: scan done pages={scanned_pages}/{total_pages} "
            f"found={len(found)}/{len(wanted)} in {_fmt_dur(elapsed)} rate={rate:.1f}/s backend=pymupdf"
        )

    return found


def _compute_doc_start_pages_from_pdf_pypdf(
    pdf_path: Path,
    *,
    doc_ids: Iterable[str],
    logger: Optional["_Logger"] = None,
    progress_interval_s: float = 10.0,
) -> dict[str, int]:
    """
    Scan the rendered PDF and find each section's start page using the embedded
    DOCID:<id> markers (added by _render_bundle_html with include_markers=True).
    """
    try:
        from pypdf import PdfReader
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Missing dependency: pypdf (pip install pypdf)") from e

    wanted = {d for d in doc_ids if d}
    if not wanted:
        return {}

    scan_start = time.monotonic()
    reader = PdfReader(str(pdf_path), strict=False)
    total_pages = len(reader.pages)
    if logger is not None:
        logger.info(f"    [pdf] pass1: scan init pages={total_pages} targets={len(wanted)}")

    fallback_reader = None
    try:
        from PyPDF2 import PdfReader as PyPdf2Reader  # type: ignore

        fallback_reader = PyPdf2Reader(str(pdf_path), strict=False)
    except Exception:
        fallback_reader = None
    marker_re = re.compile(r"DOCID:(\d+)")
    found: dict[str, int] = {}
    scanned_pages = 0
    extract_errors = 0
    fallback_attempts = 0
    last_progress = scan_start
    progress_every = max(1.0, float(progress_interval_s))

    for page_num, page in enumerate(reader.pages, start=1):
        scanned_pages = page_num
        # Defensive: some PDFs can still trigger extractor edge-cases.
        try:
            text = page.extract_text() or ""
        except Exception:
            extract_errors += 1
            if fallback_reader is not None:
                try:
                    fallback_attempts += 1
                    text = fallback_reader.pages[page_num - 1].extract_text() or ""
                except Exception:
                    text = ""
            else:
                text = ""
        for m in marker_re.finditer(text):
            doc_id = m.group(1)
            if doc_id in wanted and doc_id not in found:
                found[doc_id] = page_num

        now = time.monotonic()
        if logger is not None and (now - last_progress) >= progress_every:
            rate = scanned_pages / max(0.001, now - scan_start)
            remaining_pages = max(0, total_pages - scanned_pages)
            eta_s = remaining_pages / max(0.001, rate)
            logger.info(
                f"    [pdf] pass1: scan progress {scanned_pages}/{total_pages} "
                f"({scanned_pages*100/max(1, total_pages):.1f}%) "
                f"found={len(found)}/{len(wanted)} rate={rate:.1f}/s eta={_fmt_dur(eta_s)}"
            )
            last_progress = now

        if len(found) == len(wanted):
            break

    missing = sorted(wanted - set(found.keys()))
    if missing:
        raise RuntimeError(
            "Could not determine page numbers for some docs. "
            f"Missing markers for doc_id(s): {missing[:20]}{'...' if len(missing) > 20 else ''}"
        )

    if logger is not None:
        elapsed = time.monotonic() - scan_start
        rate = scanned_pages / max(0.001, elapsed)
        logger.info(
            f"    [pdf] pass1: scan done pages={scanned_pages}/{total_pages} "
            f"found={len(found)}/{len(wanted)} in {_fmt_dur(elapsed)} "
            f"rate={rate:.1f}/s fallback_attempts={fallback_attempts} extract_errors={extract_errors} backend=pypdf"
        )

    return found


def _compute_doc_start_pages_from_pdf(
    pdf_path: Path,
    *,
    doc_ids: Iterable[str],
    logger: Optional["_Logger"] = None,
    progress_interval_s: float = 10.0,
    scan_backend: str = "auto",
) -> tuple[dict[str, int], str]:
    backend = (scan_backend or "auto").strip().lower()
    if backend not in {"auto", "pymupdf", "pypdf"}:
        raise ValueError(f"Unsupported pdf scan backend: {scan_backend}")

    if backend in {"auto", "pymupdf"}:
        try:
            return (
                _compute_doc_start_pages_from_pdf_pymupdf(
                    pdf_path,
                    doc_ids=doc_ids,
                    logger=logger,
                    progress_interval_s=progress_interval_s,
                ),
                "pymupdf",
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if backend == "pymupdf":
                raise
            if logger is not None:
                logger.warn(f"    [pdf] pass1: pymupdf scan unavailable ({e}); falling back to pypdf")

    return (
        _compute_doc_start_pages_from_pdf_pypdf(
            pdf_path,
            doc_ids=doc_ids,
            logger=logger,
            progress_interval_s=progress_interval_s,
        ),
        "pypdf",
    )


def _crawl_category_flat(
    session: requests.Session,
    cat: CategoryConfig,
    *,
    max_sections: Optional[int],
) -> Bundle:
    url = toc_url(cat.folder_id)
    r = session.get(url, timeout=60)
    r.raise_for_status()
    panel_html = _extract_update_panel_html(r.text)
    entries = _parse_leaf_entries(panel_html)
    entries = _dedupe_entries(entries)
    if max_sections is not None:
        entries = entries[:max_sections]
    if not entries:
        raise RuntimeError(f"Category {cat.display_name} looked flat but no Law.aspx entries were parsed")
    return Bundle(
        category_key=cat.key,
        category_name=cat.display_name,
        bundle_name=cat.display_name,
        source_toc_url=url,
        entries=entries,
    )


def _crawl_category_hierarchical(
    session: requests.Session,
    cat: CategoryConfig,
    *,
    max_bundles: Optional[int],
    max_sections: Optional[int],
    browser_channel: str,
    headless: bool,
) -> list[Bundle]:
    url = toc_url(cat.folder_id)
    r = session.get(url, timeout=60)
    r.raise_for_status()
    panel_html = _extract_update_panel_html(r.text)
    root_tab = _parse_root_tab_text(panel_html) or "Titles"
    nodes = _parse_root_nodes(panel_html)
    if not nodes:
        raise RuntimeError(f"Could not parse root nodes for {cat.display_name} from {url}")
    if max_bundles is not None:
        nodes = nodes[:max_bundles]

    bundles: list[Bundle] = []

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(channel=browser_channel, headless=headless)
        page: Page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        page.wait_for_selector("#ctl00_ctl00_PageBody_PageContent_UpdatePanelToc", timeout=120_000)

        for idx, node in enumerate(nodes, start=1):
            # Click node by DOM id (more reliable than text selectors).
            click_sel = f"#{node.click_id}"
            page.click(click_sel, timeout=60_000)
            # Wait for leaf links to show up in the update panel.
            page.wait_for_selector(
                "#ctl00_ctl00_PageBody_PageContent_UpdatePanelToc a[href^='Law.aspx']",
                timeout=120_000,
            )
            node_panel_html = page.locator("#ctl00_ctl00_PageBody_PageContent_UpdatePanelToc").inner_html()
            entries = _parse_leaf_entries(node_panel_html)
            if cat.key == "revised-statutes":
                entries = _filter_revised_statutes_title_doc(node, entries)
            else:
                entries = _dedupe_entries(entries)
            if max_sections is not None:
                entries = entries[:max_sections]
            if not entries:
                # Try one more time after a short delay (UpdatePanel race).
                page.wait_for_timeout(800)
                node_panel_html = page.locator("#ctl00_ctl00_PageBody_PageContent_UpdatePanelToc").inner_html()
                entries = _parse_leaf_entries(node_panel_html)
                if cat.key == "revised-statutes":
                    entries = _filter_revised_statutes_title_doc(node, entries)
                else:
                    entries = _dedupe_entries(entries)
                if max_sections is not None:
                    entries = entries[:max_sections]
            if not entries:
                raise RuntimeError(f"After clicking {node.number_text} there were no Law.aspx entries parsed")

            # Keep bundle naming consistent across Titles.
            bundle_name = _bundle_name_default(node)

            bundles.append(
                Bundle(
                    category_key=cat.key,
                    category_name=cat.display_name,
                    bundle_name=bundle_name,
                    source_toc_url=url,
                    entries=entries,
                )
            )

            # Return to the root list (Titles/Articles) for next node.
            try:
                page.click(f"text={root_tab}", timeout=30_000)
                page.wait_for_selector(click_sel, timeout=120_000)
            except Exception:
                # Fallback: reload the page.
                page.goto(url, wait_until="domcontentloaded", timeout=120_000)
                page.wait_for_selector(click_sel, timeout=120_000)

        browser.close()

    return bundles


def _crawl_category(
    session: requests.Session,
    cat: CategoryConfig,
    *,
    max_bundles: Optional[int],
    max_sections: Optional[int],
    browser_channel: str,
    headless: bool,
) -> list[Bundle]:
    # Decide flat vs hierarchical by looking for Law.aspx links on the Parent TOC page.
    url = toc_url(cat.folder_id)
    r = session.get(url, timeout=60)
    r.raise_for_status()
    panel_html = _extract_update_panel_html(r.text)
    if "Law.aspx?d=" in panel_html:
        return [_crawl_category_flat(session, cat, max_sections=max_sections)]
    return _crawl_category_hierarchical(
        session,
        cat,
        max_bundles=max_bundles,
        max_sections=max_sections,
        browser_channel=browser_channel,
        headless=headless,
    )


def _download_bundle_sections(
    bundle: Bundle,
    bundle_dir: Path,
    *,
    trust_env: bool,
    resume: bool,
    delay_s: float,
    workers: int,
    timeout_s: float,
    retries: int,
    progress_interval_s: float,
    logger: _Logger,
    stop_event: threading.Event,
) -> dict[str, int]:
    sections_dir = bundle_dir / "sections"
    sections_dir.mkdir(parents=True, exist_ok=True)

    def _is_valid_meta(meta_path: Path) -> bool:
        if not meta_path.exists() or meta_path.stat().st_size <= 0:
            return False
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            return False
        # Backward-compatible: older runs may not have doc_text; treat as valid if
        # we have at least one representation.
        has_html = isinstance(meta.get("doc_html"), str) and bool(meta.get("doc_html"))
        has_text = isinstance(meta.get("doc_text"), str) and bool(meta.get("doc_text"))
        return isinstance(meta.get("doc_id"), str) and bool(meta.get("doc_id")) and (has_html or has_text)

    todo: list[TocEntry] = []
    skipped = 0
    expected_total = 0
    for e in bundle.entries:
        doc_id = e.doc_id
        if not doc_id:
            continue
        expected_total += 1
        meta_path = sections_dir / f"{doc_id}.json"
        if resume and _is_valid_meta(meta_path):
            skipped += 1
            continue
        todo.append(e)

    total = len(todo)
    if total == 0:
        logger.info(f"    [download] up-to-date (all {skipped} already downloaded)")
        return {"expected": expected_total, "todo": 0, "skipped": skipped, "ok": 0, "err": 0}

    workers = max(1, int(workers))
    rate = _RateLimiter(delay_s)
    errors_path = bundle_dir / "download_errors.jsonl"

    # Thread-local request sessions (requests.Session isn't thread-safe).
    tls = threading.local()

    def _get_session() -> requests.Session:
        sess = getattr(tls, "session", None)
        if sess is None:
            sess = _new_requests_session(trust_env=trust_env)
            tls.session = sess
        return sess

    def _download_one(e: TocEntry) -> tuple[str, str, str]:
        """
        Returns (status, doc_id, message)
          status: ok | skipped | error | cancelled
        """
        doc_id = e.doc_id
        if not doc_id:
            return ("skipped", "", "missing doc_id")
        if stop_event.is_set():
            return ("cancelled", doc_id, "cancelled")

        meta_path = sections_dir / f"{doc_id}.json"
        txt_path = sections_dir / f"{doc_id}.txt"
        html_path = sections_dir / f"{doc_id}.html"
        if resume and _is_valid_meta(meta_path):
            return ("skipped", doc_id, "already downloaded")

        sess = _get_session()

        last_exc: Exception | None = None
        for attempt in range(max(0, int(retries)) + 1):
            if stop_event.is_set():
                return ("cancelled", doc_id, "cancelled")
            try:
                rate.wait(stop_event)
                r = sess.get(e.url, timeout=(10, float(timeout_s)))
                if r.status_code == 429:
                    # Back off a bit and retry.
                    retry_after = r.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else 5.0
                    time.sleep(sleep_s)
                    continue
                r.raise_for_status()
                # Parse the doc.
                soup = BeautifulSoup(r.text, "html.parser")
                citation = ""
                cit_el = soup.find(id="ctl00_PageBody_LabelName")
                if cit_el:
                    citation = cit_el.get_text(" ", strip=True)
                doc_el = soup.find(id="ctl00_PageBody_LabelDocument")
                if not doc_el:
                    raise RuntimeError("Could not locate law document content span (ctl00_PageBody_LabelDocument)")
                doc_html = doc_el.decode_contents()
                doc_text = "\n".join([ln.strip() for ln in doc_el.get_text("\n").splitlines() if ln.strip()])
                if not doc_text:
                    raise RuntimeError("Empty extracted doc_text")

                citation = citation or e.citation

                # Write artifacts (atomic). Meta JSON is written last so its presence
                # is a reliable "this doc is complete" marker for --resume.
                txt_lines: list[str] = [citation]
                if e.title:
                    txt_lines.append(e.title)
                txt_lines.append(e.url)
                txt_lines.append("")
                txt_lines.append(doc_text)
                _atomic_write_text(txt_path, "\n".join(txt_lines).strip() + "\n")

                title = f"{citation} - {e.title}".strip(" -")
                _atomic_write_text(
                    html_path,
                    (
                        "<!doctype html>\n"
                        "<html><head><meta charset=\"utf-8\" />\n"
                        f"<title>{title}</title>\n"
                        "<style>\n"
                        "body{font-family:'Times New Roman',Times,serif;font-size:12pt;margin:0.75in;}\n"
                        "h1{font-size:16pt;margin:0 0 0.1in 0;}\n"
                        ".src{font-size:10pt;color:#444;margin-bottom:0.2in;}\n"
                        "p{margin:0 0 0.08in 0;}\n"
                        "</style></head><body>\n"
                        f"<h1>{title}</h1>\n"
                        f"<div class=\"src\"><a href=\"{e.url}\">{e.url}</a></div>\n"
                        f"{doc_html}\n"
                        "</body></html>\n"
                    ),
                )

                meta = {
                    "doc_id": doc_id,
                    "url": e.url,
                    "citation": citation,
                    "title": e.title,
                    "downloaded_at_epoch": int(time.time()),
                    "doc_html": doc_html,
                    "doc_text": doc_text,
                }
                _write_json(meta_path, meta)

                return ("ok", doc_id, "")
            except Exception as exc:
                last_exc = exc if isinstance(exc, Exception) else Exception(str(exc))
                # Quick retry on transient errors.
                if attempt < retries:
                    time.sleep(min(2.0 * (2**attempt), 10.0))
                    continue
                break

        return ("error", doc_id, f"{type(last_exc).__name__ if last_exc else 'Error'}: {last_exc}")

    logger.info(
        f"    [download] todo {total}, skipped {skipped}, workers {workers}, delay {delay_s}s, timeout {timeout_s}s"
    )

    ok = 0
    err = 0
    start = time.monotonic()
    last_log = start

    # Parallel downloads.
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_download_one, e) for e in todo]
        try:
            for fut in concurrent.futures.as_completed(futures):
                status, doc_id, msg = fut.result()
                if status == "ok":
                    ok += 1
                elif status == "error":
                    err += 1
                    _append_text(errors_path, json.dumps({"doc_id": doc_id, "error": msg}, ensure_ascii=False) + "\n")
                    logger.warn(f"    [download] error doc_id={doc_id}: {msg}")
                elif status == "cancelled":
                    # Stop early if cancellation propagates.
                    stop_event.set()
                    break

                now = time.monotonic()
                if now - last_log >= max(0.5, float(progress_interval_s)):
                    done = ok + err
                    logger.info(
                        f"    [download] {done}/{total} ({done*100/total:.1f}%) ok={ok} err={err} rate={_fmt_rate(done, now-start)}"
                    )
                    last_log = now
        except KeyboardInterrupt:
            stop_event.set()
            logger.warn("    [download] Ctrl+C received; stopping after in-flight requests finish...")
            # Cancel queued work so we only wait for in-flight requests.
            for f in futures:
                f.cancel()
            raise

    elapsed = time.monotonic() - start
    logger.info(f"    [download] done ok={ok} err={err} total={total} in {_fmt_dur(elapsed)}")
    return {"expected": expected_total, "todo": total, "skipped": skipped, "ok": ok, "err": err}


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--categories",
        nargs="+",
        default=["revised-statutes"],
        help="One or more category keys, or 'all'. Default: revised-statutes",
    )
    parser.add_argument("--out", default="out", help="Output directory (default: out)")
    parser.add_argument("--max-bundles", type=int, default=None, help="Limit bundles per hierarchical category")
    parser.add_argument("--max-sections", type=int, default=None, help="Limit sections per bundle (testing)")
    parser.add_argument(
        "--bundle-regex",
        default=None,
        help="Only process bundles whose name matches this regex (case-insensitive)",
    )
    parser.add_argument(
        "--toc-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Cache the parsed TOC on disk to speed up resume runs (default: true)",
    )
    parser.add_argument(
        "--toc-cache-dir",
        default=".toc-cache",
        help="Directory for TOC cache files (default: .toc-cache)",
    )
    parser.add_argument(
        "--toc-cache-ttl-days",
        type=float,
        default=7.0,
        help="Reuse TOC cache for this many days (default: 7.0)",
    )
    parser.add_argument(
        "--refresh-toc",
        action="store_true",
        help="Ignore any cached TOC and re-scrape it",
    )
    parser.add_argument("--skip-pdf", action="store_true", help="Skip PDF generation")
    parser.add_argument(
        "--toc-page-numbers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include real page numbers in bundle PDF table of contents (default: true)",
    )
    parser.add_argument(
        "--keep-draft-pdf",
        action="store_true",
        help="Keep intermediate draft PDFs used to calculate page numbers",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip already-downloaded docs (default: true)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Parallel downloads per bundle (default: 16). Use 1 to disable concurrency.",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="Per-request timeout in seconds (default: 30)")
    parser.add_argument("--retries", type=int, default=2, help="Retry count for transient errors (default: 2)")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Global minimum interval between HTTP requests in seconds (default: 0.0)",
    )
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=2.0,
        help="Progress log interval in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--pdf-progress-interval",
        type=float,
        default=10.0,
        help="Progress/liveness log interval for long PDF render/scan steps (default: 10.0)",
    )
    parser.add_argument(
        "--pdf-scan-backend",
        choices=["auto", "pymupdf", "pypdf"],
        default="auto",
        help="Backend for pass1 page-number scan (default: auto; prefers pymupdf, falls back to pypdf)",
    )
    parser.add_argument("--trust-env", action="store_true", help="Trust HTTP(S)_PROXY env vars for requests")
    parser.add_argument("--browser-channel", default="msedge", help="Playwright chromium channel (default: msedge)")
    parser.add_argument("--headful", action="store_true", help="Run browser headful (debugging)")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Continue to next bundle on PDF errors (default: true)",
    )
    args = parser.parse_args(argv)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    resume = bool(args.resume)
    headless = not args.headful
    logger = _Logger(verbose=bool(args.verbose))
    stop_event = threading.Event()
    cache_dir_arg = Path(str(args.toc_cache_dir))
    toc_cache_dir = cache_dir_arg if cache_dir_arg.is_absolute() else (_project_root() / cache_dir_arg)
    logger.info(
        "config: "
        f"resume={resume} workers={int(args.workers)} delay={float(args.delay)}s "
        f"timeout={float(args.timeout)}s retries={int(args.retries)} "
        f"pdf={'off' if args.skip_pdf else 'on'} toc_pages={'on' if args.toc_page_numbers else 'off'} "
        f"pdf_progress={float(args.pdf_progress_interval)}s pdf_scan={str(args.pdf_scan_backend)}"
    )
    if args.toc_cache:
        logger.info(
            f"config: toc_cache=on ttl_days={float(args.toc_cache_ttl_days)} dir={toc_cache_dir} refresh={bool(args.refresh_toc)}"
        )
    else:
        logger.info("config: toc_cache=off")

    categories: list[CategoryConfig] = [
        CategoryConfig("revised-statutes", "Revised Statutes", 75),
        CategoryConfig("louisiana-constitution", "Louisiana Constitution", 72),
        CategoryConfig("constitution-ancillaries", "Constitution Ancillaries", 66),
        CategoryConfig("childrens-code", "Children's Code", 71),
        CategoryConfig("civil-code", "Civil Code", 67),
        CategoryConfig("code-of-civil-procedure", "Code of Civil Procedure", 68),
        CategoryConfig("code-of-criminal-procedure", "Code of Criminal Procedure", 69),
        CategoryConfig("code-of-evidence", "Code of Evidence", 70),
        CategoryConfig("house-rules", "House Rules", 73),
        CategoryConfig("senate-rules", "Senate Rules", 211),
        CategoryConfig("joint-rules", "Joint Rules", 74),
    ]
    by_key = {c.key: c for c in categories}

    requested = [c.lower() for c in args.categories]
    if "all" in requested:
        selected = categories
    else:
        unknown = [c for c in requested if c not in by_key]
        if unknown:
            raise SystemExit(f"Unknown category key(s): {unknown}")
        selected = [by_key[c] for c in requested]

    session = _new_requests_session(trust_env=args.trust_env)

    # Static PDF from the LawsContents page.
    static_pdf_url = "https://legis.la.gov/LegisDocs/ConstAmend/Constitution_Amendments_Table.pdf"
    static_pdf_out = out_dir / "Amendments to the LA Constitution of 1974" / "Constitution_Amendments_Table.pdf"
    try:
        logger.info(f"[static-pdf] download {static_pdf_url}")
        _download_static_pdf(session, static_pdf_url, static_pdf_out)
        if static_pdf_out.exists():
            logger.info(f"[static-pdf] ok {static_pdf_out} ({static_pdf_out.stat().st_size} bytes)")
    except Exception as e:
        logger.warn(f"static PDF download failed: {static_pdf_url} ({e})")

    try:
        for cat in selected:
            if stop_event.is_set():
                break

            logger.info(f"[toc] {cat.display_name}")
            bundles: list[Bundle]
            cache_path = _toc_cache_path(toc_cache_dir, cat)
            bundles_from_cache = None
            if args.toc_cache and not args.refresh_toc:
                bundles_from_cache = _load_toc_cache(
                    cache_path,
                    ttl_days=float(args.toc_cache_ttl_days),
                    logger=logger,
                )
            if bundles_from_cache is not None:
                bundles = bundles_from_cache
            else:
                if args.toc_cache:
                    logger.info(f"[toc] cache miss: {cache_path} (crawling site)")
                bundles = _crawl_category(
                    session,
                    cat,
                    max_bundles=args.max_bundles,
                    max_sections=args.max_sections,
                    browser_channel=args.browser_channel,
                    headless=headless,
                )
                # Only write cache for full crawls (no limits/filters).
                if (
                    args.toc_cache
                    and not args.refresh_toc
                    and args.max_bundles is None
                    and args.max_sections is None
                    and not args.bundle_regex
                ):
                    _save_toc_cache(cache_path, cat=cat, bundles=bundles, logger=logger)
            logger.info(f"  [toc] bundles: {len(bundles)}")
            if args.bundle_regex:
                try:
                    bundle_pat = re.compile(str(args.bundle_regex), re.IGNORECASE)
                except re.error as e:
                    raise SystemExit(f"Invalid --bundle-regex: {e}") from e
                bundles = [b for b in bundles if bundle_pat.search(b.bundle_name)]
                logger.info(f"  [toc] bundles after filter: {len(bundles)}")

            cat_dir = out_dir / safe_name(cat.display_name)
            cat_dir.mkdir(parents=True, exist_ok=True)

            # If the category is a single flat bundle (bundle name == category name),
            # avoid a redundant nested folder like out/Civil Code/Civil Code/.
            single_flat = len(bundles) == 1 and bundles[0].bundle_name == cat.display_name

            pdf_renderer: Optional[_PdfRenderer] = None
            try:
                for b in bundles:
                    if stop_event.is_set():
                        break

                    b_dir = cat_dir if single_flat else (cat_dir / safe_name(b.bundle_name))
                    b_dir.mkdir(parents=True, exist_ok=True)

                    manifest_path = b_dir / "bundle.json"
                    if not manifest_path.exists() or not resume:
                        _write_json(
                            manifest_path,
                            {
                                "category_key": b.category_key,
                                "category_name": b.category_name,
                                "bundle_name": b.bundle_name,
                                "source_toc_url": b.source_toc_url,
                                "entries": [asdict(e) for e in b.entries],
                            },
                        )

                    downloaded_marker = b_dir / "bundle.downloaded.json"
                    pdf_marker = b_dir / "bundle.pdf.json"
                    pdf_path = b_dir / f"{safe_name(b.bundle_name)}.pdf"

                    # Fast resume: if a bundle is already fully finished, skip it.
                    if resume and not args.skip_pdf:
                        if pdf_path.exists() and pdf_path.stat().st_size > 0:
                            if not pdf_marker.exists():
                                _write_json(
                                    pdf_marker,
                                    {
                                        "bundle_name": b.bundle_name,
                                        "pdf": str(pdf_path),
                                        "completed_at_epoch": int(time.time()),
                                        "note": "marker created from existing pdf",
                                    },
                                )
                            logger.info(f"  [bundle] skip done (pdf exists) {b.bundle_name}")
                            continue
                    if resume and args.skip_pdf:
                        if downloaded_marker.exists():
                            logger.info(f"  [bundle] skip done (downloaded) {b.bundle_name}")
                            continue

                    logger.info(f"  [bundle] {b.bundle_name} ({len(b.entries)} docs)")
                    # Download sections (unless we already completed this bundle earlier).
                    download_stats = None
                    if not (resume and downloaded_marker.exists()):
                        download_stats = _download_bundle_sections(
                            b,
                            b_dir,
                            trust_env=bool(args.trust_env),
                            resume=resume,
                            delay_s=float(args.delay),
                            workers=int(args.workers),
                            timeout_s=float(args.timeout),
                            retries=int(args.retries),
                            progress_interval_s=float(args.progress_interval),
                            logger=logger,
                            stop_event=stop_event,
                        )

                        expected_total = int(download_stats["expected"])
                        if (
                            download_stats["err"] == 0
                            and (download_stats["ok"] + download_stats["skipped"]) == expected_total
                        ):
                            _write_json(
                                downloaded_marker,
                                {
                                    "bundle_name": b.bundle_name,
                                    "docs": expected_total,
                                    "ok": download_stats["ok"],
                                    "skipped": download_stats["skipped"],
                                    "err": download_stats["err"],
                                    "completed_at_epoch": int(time.time()),
                                },
                            )
                    else:
                        logger.info("    [download] skip (bundle already downloaded)")

                    if args.skip_pdf or stop_event.is_set():
                        continue

                    if resume and pdf_path.exists() and pdf_path.stat().st_size > 0:
                        if not pdf_marker.exists():
                            _write_json(
                                pdf_marker,
                                {
                                    "bundle_name": b.bundle_name,
                                    "pdf": str(pdf_path),
                                    "completed_at_epoch": int(time.time()),
                                    "note": "marker created from existing pdf",
                                },
                            )
                        logger.info(f"  [pdf] skip (exists) {pdf_path}")
                        continue

                    # Lazy-init renderer only when needed.
                    if pdf_renderer is None:
                        logger.info("  [pdf] init renderer")
                        pdf_renderer = _PdfRenderer(browser_channel=args.browser_channel, headless=headless)

                    try:
                        logger.info(f"  [pdf] {pdf_path}")

                        if args.toc_page_numbers:
                            draft_html = _render_bundle_html(
                                b,
                                b_dir,
                                toc_page_numbers=None,
                                include_markers=True,
                                out_filename="bundle.draft.html",
                            )
                            draft_html_sha256 = hashlib.sha256(draft_html.read_bytes()).hexdigest()
                            sections_dir = b_dir / "sections"
                            doc_ids = [
                                e.doc_id
                                for e in b.entries
                                if e.doc_id and (sections_dir / f"{e.doc_id}.json").exists()
                            ]
                            toc_page_map_path = b_dir / "toc_page_numbers.json"
                            page_map = _load_cached_toc_page_numbers(
                                toc_page_map_path,
                                expected_doc_ids=doc_ids,
                                html_sha256=draft_html_sha256,
                            )

                            if page_map is not None:
                                logger.info(
                                    f"    [pdf] pass1: reuse cached page numbers "
                                    f"({len(page_map)}/{len(doc_ids)}) from {toc_page_map_path.name}"
                                )
                            else:
                                logger.info("    [pdf] pass1: no valid cached page numbers; computing")
                            draft_pdf = b_dir / f"{safe_name(b.bundle_name)}.draft.pdf"
                            if page_map is None:
                                logger.info("    [pdf] pass1: render draft PDF")
                                _render_pdf_from_html(
                                    draft_html,
                                    draft_pdf,
                                    browser_channel=args.browser_channel,
                                    headless=headless,
                                    renderer=pdf_renderer,
                                    logger=logger,
                                    phase_label="pass1 draft render",
                                    progress_interval_s=float(args.pdf_progress_interval),
                                )

                                logger.info("    [pdf] pass1: scan page numbers")
                                page_map, scan_backend = _compute_doc_start_pages_from_pdf(
                                    draft_pdf,
                                    doc_ids=doc_ids,
                                    logger=logger,
                                    progress_interval_s=float(args.pdf_progress_interval),
                                    scan_backend=str(args.pdf_scan_backend),
                                )
                                _save_toc_page_numbers(
                                    toc_page_map_path,
                                    page_map=page_map,
                                    doc_ids=doc_ids,
                                    html_sha256=draft_html_sha256,
                                    scan_backend=scan_backend,
                                )
                                logger.info(
                                    f"    [pdf] pass1: cached page numbers with backend={scan_backend} "
                                    f"file={toc_page_map_path.name}"
                                )

                            final_html = _render_bundle_html(
                                b,
                                b_dir,
                                toc_page_numbers=page_map,
                                include_markers=False,
                                out_filename="bundle.html",
                            )
                            logger.info("    [pdf] pass2: render final PDF")
                            _render_pdf_from_html(
                                final_html,
                                pdf_path,
                                browser_channel=args.browser_channel,
                                headless=headless,
                                renderer=pdf_renderer,
                                logger=logger,
                                phase_label="pass2 final render",
                                progress_interval_s=float(args.pdf_progress_interval),
                            )

                            if not args.keep_draft_pdf:
                                try:
                                    draft_pdf.unlink()
                                except OSError:
                                    pass
                        else:
                            html_path = _render_bundle_html(b, b_dir, toc_page_numbers=None, include_markers=False)
                            _render_pdf_from_html(
                                html_path,
                                pdf_path,
                                browser_channel=args.browser_channel,
                                headless=headless,
                                renderer=pdf_renderer,
                                logger=logger,
                                phase_label="single-pass render",
                                progress_interval_s=float(args.pdf_progress_interval),
                            )

                        # Mark bundle PDF complete for fast resume.
                        _write_json(
                            pdf_marker,
                            {
                                "bundle_name": b.bundle_name,
                                "pdf": str(pdf_path),
                                "completed_at_epoch": int(time.time()),
                            },
                        )
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        logger.warn(f"  [pdf] failed for bundle '{b.bundle_name}': {e}")
                        _atomic_write_text(
                            b_dir / "pdf_error.txt",
                            f"{_ts()} PDF generation failed\n\n{type(e).__name__}: {e}\n",
                        )
                        # Reset the renderer in case it got into a bad state.
                        try:
                            if pdf_renderer is not None:
                                pdf_renderer.close()
                        finally:
                            pdf_renderer = None
                        if not args.continue_on_error:
                            raise
                        continue
            finally:
                if pdf_renderer is not None:
                    logger.info("  [pdf] closing renderer")
                    try:
                        pdf_renderer.close()
                    except BaseException as e:
                        logger.debug(f"[pdf] close failed (ignored): {e}")
    except KeyboardInterrupt:
        stop_event.set()
        logger.warn("Ctrl+C received. Stopping cleanly; re-run with the same command to resume.")
        return 130

    logger.info("[done]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

